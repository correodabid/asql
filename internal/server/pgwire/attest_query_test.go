package pgwire

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// queryAttestation runs an asql_admin.attest_query and rebuilds the
// attested result from its row, whose columns are named as the JSON fields.
func queryAttestation(ctx context.Context, conn *pgx.Conn, sql string) (ledgerdigest.AttestedResult, error) {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return ledgerdigest.AttestedResult{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return ledgerdigest.AttestedResult{}, err
		}
		return ledgerdigest.AttestedResult{}, errors.New("no attestation row")
	}
	values, err := rows.Values()
	if err != nil {
		return ledgerdigest.AttestedResult{}, err
	}
	fields := map[string]json.RawMessage{}
	var encodedRows json.RawMessage
	for i, field := range rows.FieldDescriptions() {
		var raw json.RawMessage
		switch v := values[i].(type) {
		case string:
			if field.Name == "domains" || field.Name == "columns" || field.Name == "rows" {
				raw = json.RawMessage(v)
			} else {
				raw, _ = json.Marshal(v)
			}
		default:
			raw, _ = json.Marshal(v)
		}
		if field.Name == "rows" {
			encodedRows = raw
			continue
		}
		fields[field.Name] = raw
	}
	attestation, _ := json.Marshal(fields)
	var result ledgerdigest.AttestedResult
	if err := json.Unmarshal([]byte(`{"attestation":`+string(attestation)+`,"rows":`+string(encodedRows)+`}`), &result); err != nil {
		return ledgerdigest.AttestedResult{}, err
	}
	return result, rows.Err()
}

func TestAttestQuery(t *testing.T) {
	config, keys := signedLedgerConfig(t)
	server, pgwireAddr, _, cleanup := startAdminSmokeServer(t, config)
	defer cleanup()
	ctx := context.Background()
	execLedgerWrites(t, pgwireAddr, 1, 3)
	early := server.engine.HeadLSN()
	execLedgerWrites(t, pgwireAddr, 4, 5)
	head := server.engine.HeadLSN()

	const query = "SELECT id, result AS outcome FROM lab.results WHERE id >= 2 ORDER BY id"
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })

		for lsn, wantRows := range map[uint64]int{head: 4, early: 2} {
			sql := "SELECT * FROM asql_admin.attest_query WHERE sql = '" + strings.ReplaceAll(query, "'", "''") + "'"
			if lsn != head {
				sql += " AND lsn = " + strconv.FormatUint(lsn, 10)
			}
			got, err := queryAttestation(ctx, conn, sql)
			if err != nil {
				t.Fatalf("%s at LSN %d: %v", mode, lsn, err)
			}
			a := got.Attestation
			if err := a.Verify(keys); err != nil {
				t.Fatalf("%s at LSN %d: %v", mode, lsn, err)
			}
			if err := a.CheckRows(got.Rows); err != nil {
				t.Fatalf("%s at LSN %d: %v", mode, lsn, err)
			}
			chain, err := server.walStore.ChainAt(lsn)
			if err != nil {
				t.Fatal(err)
			}
			if a.LSN != lsn || a.Chain != chain || a.SQL != query || a.NodeID != "node-a" || a.EngineVersion == "" {
				t.Fatalf("%s at LSN %d: attestation %+v", mode, lsn, a)
			}
			if len(got.Rows) != wantRows || strings.Join(a.Columns, ",") != "id,outcome" {
				t.Fatalf("%s at LSN %d: %d rows, columns %v; want %d and id,outcome", mode, lsn, len(got.Rows), a.Columns, wantRows)
			}
			if got.Rows[0][0].NumberValue != 2 || got.Rows[0][1].StringValue != "WITHIN_SPEC" {
				t.Fatalf("%s at LSN %d: first row %v", mode, lsn, got.Rows[0])
			}
		}

		for sql, want := range map[string]string{
			"SELECT * FROM asql_admin.attest_query":                                                          "WHERE sql",
			"SELECT * FROM asql_admin.attest_query WHERE sql = 'DELETE FROM lab.results'":                    "only a SELECT",
			"SELECT * FROM asql_admin.attest_query WHERE sql = 'SELECT * FROM lab.results WHERE id = $1'":    "placeholders",
			"SELECT * FROM asql_admin.attest_query WHERE sql = 'SELECT * FROM lab.results FOR HISTORY'":      "FOR HISTORY",
			"SELECT * FROM asql_admin.attest_query WHERE sql = 'SELECT * FROM asql_admin.engine_stats'":      "virtual tables",
			"SELECT * FROM asql_admin.attest_query WHERE sql = 'SELECT * FROM lab.results AS OF LSN 3'":      "AS OF",
			"SELECT * FROM asql_admin.attest_query WHERE sql = 'SELECT * FROM lab.results' AND lsn = 999999": "not committed",
		} {
			if _, err := queryAttestation(ctx, conn, sql); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("%s: %s: error %v, want %q", mode, sql, err, want)
			}
		}
	}
}

func TestAttestQueryNeedsSignedLedgerAndTheQueryPrivileges(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := queryAttestation(ctx, conn, "SELECT * FROM asql_admin.attest_query WHERE sql = 'SELECT 1'"); err == nil || !strings.Contains(err.Error(), "ledger mode signed") {
		t.Fatalf("without ledger=signed: %v", err)
	}

	config, _ := signedLedgerConfig(t)
	server, pgwireAddr, _, cleanup := startAdminSmokeServer(t, config)
	defer cleanup()
	for _, step := range []func() error{
		func() error { return server.engine.BootstrapAdminPrincipal(ctx, "admin", "admin-pass") },
		func() error { return server.engine.CreateUser(ctx, "analyst", "analyst-pass") },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	admin, err := pgx.Connect(ctx, "postgres://admin:admin-pass@"+pgwireAddr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	for _, sql := range []string{"BEGIN DOMAIN lab", "CREATE TABLE results (id INT PRIMARY KEY, result TEXT)", "COMMIT",
		"BEGIN DOMAIN lab", "INSERT INTO results (id, result) VALUES (1, 'ok')", "COMMIT"} {
		if _, err := admin.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	early := server.engine.HeadLSN()
	if _, err := admin.Exec(ctx, "BEGIN DOMAIN lab"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO results (id, result) VALUES (2, 'ok')"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}

	analyst, err := pgx.Connect(ctx, "postgres://analyst:analyst-pass@"+pgwireAddr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer analyst.Close(ctx)
	const attest = "SELECT * FROM asql_admin.attest_query WHERE sql = 'SELECT id FROM lab.results'"
	if _, err := queryAttestation(ctx, analyst, attest); err != nil {
		t.Fatalf("analyst at the head: %v", err)
	}
	var pgErr *pgconn.PgError
	if _, err := queryAttestation(ctx, analyst, attest+" AND lsn = "+strconv.FormatUint(early, 10)); !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("analyst at an old LSN without SELECT_HISTORY: %v, want 42501", err)
	}
	if err := server.engine.GrantPrivilege(ctx, "analyst", executor.PrincipalPrivilegeSelectHistory); err != nil {
		t.Fatal(err)
	}
	if got, err := queryAttestation(ctx, analyst, attest+" AND lsn = "+strconv.FormatUint(early, 10)); err != nil || len(got.Rows) != 1 {
		t.Fatalf("analyst at an old LSN with SELECT_HISTORY: %d rows, err %v; want 1", len(got.Rows), err)
	}
}
