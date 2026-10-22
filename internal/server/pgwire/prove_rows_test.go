package pgwire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/statetree"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func queryRowsProof(ctx context.Context, conn *pgx.Conn, sql string) (statetree.RowsProof, error) {
	var digest, rows, groups, proof string
	if err := conn.QueryRow(ctx, sql).Scan(&digest, &rows, &proof, &groups); err != nil {
		return statetree.RowsProof{}, err
	}
	var p statetree.RowsProof
	err := json.Unmarshal([]byte(`{"digest":`+digest+`,"rows":`+rows+`,"groups":`+groups+`,"proof":`+proof+`}`), &p)
	return p, err
}

func proveRowsSQL(keys string, lsn uint64) string {
	sql := "SELECT * FROM asql_admin.prove_rows WHERE keys = '" + strings.ReplaceAll(keys, "'", "''") + "'"
	if lsn > 0 {
		sql += fmt.Sprintf(" AND lsn = %d", lsn)
	}
	return sql
}

// TestProveRows: a lineage across artifacts and their composite-key edges
// is proven in one batch against the signed state root, at the latest
// digest and at an older one that shows the values of then.
func TestProveRows(t *testing.T) {
	config, keys := signedLedgerConfig(t)
	if err := ledgerdigest.GenerateStateKeyFile(ledgerdigest.DefaultStateKeyPath(config.LedgerKeyFile)); err != nil {
		t.Fatal(err)
	}
	server, pgwireAddr, _, cleanup := startAdminSmokeServer(t, config)
	defer cleanup()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	tx := func(sqls ...string) {
		t.Helper()
		for _, sql := range append(append([]string{"BEGIN DOMAIN lab"}, sqls...), "COMMIT") {
			if _, err := admin.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}
	tx("CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT) VERIFIABLE",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE",
		"CREATE TABLE runs (n INT PRIMARY KEY, ok BOOL) VERIFIABLE",
		"CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)")
	tx("INSERT INTO artifacts (id, kind) VALUES ('aa', 'source'), ('bb', 'extraction'), ('zz', 'unpublished')",
		"INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('bb', 'aa', 1), ('zz', 'aa', 1)",
		"INSERT INTO runs (n, ok) VALUES (7, true)",
		"INSERT INTO notes (id, body) VALUES (1, 'x')")
	early := waitForDigestAtHead(t, server)
	tx("UPDATE artifacts SET kind = 'source-v2' WHERE id = 'aa'")
	late := waitForDigestAtHead(t, server)

	const lineage = `[{"domain": "lab", "table": "artifacts", "pk": ["aa"]},
		{"domain": "lab", "table": "artifacts", "pk": ["bb"]},
		{"domain": "lab", "table": "artifact_parents", "pk": ["bb", "aa"]},
		{"domain": "lab", "table": "runs", "pk": [7]}]`
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })

		for lsn, want := range map[uint64]string{0: "source-v2", early.LSN: "source"} {
			p, err := queryRowsProof(ctx, conn, proveRowsSQL(lineage, lsn))
			if err != nil {
				t.Fatalf("%s at LSN %d: %v", mode, lsn, err)
			}
			if err := p.Verify(keys); err != nil {
				t.Fatalf("%s at LSN %d: %v", mode, lsn, err)
			}
			wantLSN := late.LSN
			if lsn != 0 {
				wantLSN = lsn
			}
			if p.Digest.LSN != wantLSN || len(p.Rows) != 4 {
				t.Fatalf("%s: digest LSN %d with %d rows, want %d and 4", mode, p.Digest.LSN, len(p.Rows), wantLSN)
			}
			if got := p.Rows[0].Row["kind"].StringValue; got != want {
				t.Fatalf("%s at LSN %d: aa's kind is %q, want %q", mode, lsn, got, want)
			}
			edge := p.Rows[2]
			if strings.Join(edge.PrimaryKeyColumns, ",") != "child_id,parent_id" || edge.Row["ordinal"].NumberValue != 1 {
				t.Fatalf("%s: edge %+v", mode, edge)
			}
			// Nothing about rows outside the lineage travels with it. Only
			// the rows carry values: the rest is hashes, salts and the
			// signature, whose base64 may spell anything.
			data, _ := json.Marshal(p.Rows)
			if strings.Contains(string(data), `"zz"`) || strings.Contains(string(data), "unpublished") {
				t.Fatalf("%s: the proof mentions a row it does not prove: %s", mode, data)
			}
		}

		for sql, want := range map[string]string{
			"SELECT * FROM asql_admin.prove_rows": "keys",
			proveRowsSQL(`[]`, 0):                 "empty",
			proveRowsSQL(`[{"domain": "lab", "table": "artifacts", "pk": ["nope"]}]`, 0):                                                      "no row",
			proveRowsSQL(`[{"domain": "lab", "table": "notes", "pk": [1]}]`, 0):                                                               "not a VERIFIABLE table",
			proveRowsSQL(`[{"domain": "lab", "table": "artifact_parents", "pk": ["bb"]}]`, 0):                                                 "2-column primary key",
			proveRowsSQL(`[{"domain": "lab", "table": "artifacts", "pk": ["aa"]}, {"domain": "lab", "table": "artifacts", "pk": ["aa"]}]`, 0): "twice",
			proveRowsSQL(`[{"domain": "lab", "table": "artifacts", "pk": ["aa"]}]`, early.LSN-1):                                              "no digest was signed",
		} {
			_, err := queryRowsProof(ctx, conn, sql)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "22023" || !strings.Contains(pgErr.Message, want) {
				t.Fatalf("%s: %s: %v, want 22023 with %q", mode, sql, err, want)
			}
		}
	}
}

func TestProveRowsNeedsAStateKey(t *testing.T) {
	config, _ := signedLedgerConfig(t)
	_, pgwireAddr, _, cleanup := startAdminSmokeServer(t, config)
	defer cleanup()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	_, err = queryRowsProof(ctx, conn, proveRowsSQL(`[{"domain": "lab", "table": "t", "pk": [1]}]`, 0))
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55000" {
		t.Fatalf("without a state key: %v, want 55000", err)
	}
}
