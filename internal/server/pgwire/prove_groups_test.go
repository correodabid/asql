package pgwire

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/storage/ledgerdigest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestProveCompleteGroups: with artifact_parents GROUP (child_id)
// REFERENCES artifacts (id), a row proof can show all the parents of an
// artifact, none for a source, at the head or at an older digest; the
// state root kept incrementally always equals one built from scratch.
func TestProveCompleteGroups(t *testing.T) {
	config, keys := signedLedgerConfig(t)
	if err := ledgerdigest.GenerateStateKeyFile(ledgerdigest.DefaultStateKeyPath(config.LedgerKeyFile)); err != nil {
		t.Fatal(err)
	}
	server, pgwireAddr, _, cleanup := startAdminSmokeServer(t, config)
	defer cleanup()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tx := func(sqls ...string) {
		t.Helper()
		for _, sql := range append(append([]string{"BEGIN DOMAIN lab"}, sqls...), "COMMIT") {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}
	checkpoint := func(step string) ledgerdigest.Digest {
		t.Helper()
		d := waitForDigestAtHead(t, server)
		view, err := server.engine.VerifiableViewAt(ctx, d.LSN)
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := buildStateTree(*server.ledger.stateKey, view)
		if err != nil {
			t.Fatal(err)
		}
		if d.Version != ledgerdigest.Version3 || d.StateRoot != fresh.Root() {
			t.Fatalf("%s: the signed state root differs from one built from scratch", step)
		}
		return d
	}

	tx("CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT) VERIFIABLE",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) VERIFIABLE")
	tx("INSERT INTO artifacts (id, kind) VALUES ('aa', 'source'), ('bb', 'run'), ('cc', 'source'), ('zz', 'unpublished')",
		"INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('bb', 'aa', 1), ('bb', 'cc', 2), ('zz', 'aa', 1)")
	before := checkpoint("before the group")
	tx("ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)")
	if d := checkpoint("group declared"); d.StateRoot == before.StateRoot {
		t.Fatal("declaring the group added no group leaves")
	}
	tx("INSERT INTO artifacts (id, kind) VALUES ('dd', 'source')")
	checkpoint("an artifact without parents")
	twoParents := checkpoint("again")

	prove := func(keysJSON string, lsn uint64) (string, error) {
		t.Helper()
		p, err := queryRowsProof(ctx, conn, proveRowsSQL(keysJSON, lsn))
		if err != nil {
			return "", err
		}
		if err := p.Verify(keys); err != nil {
			t.Fatalf("%s: %v", keysJSON, err)
		}
		var cited []string
		for _, r := range p.Rows {
			cited = append(cited, r.Table+":"+r.Row[r.PrimaryKeyColumns[0]].StringValue+"<-"+r.Row["parent_id"].StringValue)
		}
		for _, g := range p.Groups {
			cited = append(cited, "complete:"+g.Values[0].StringValue)
		}
		data, _ := json.Marshal(p.Rows)
		if strings.Contains(string(data), `"zz"`) {
			t.Fatalf("%s: the proof shows zz", keysJSON)
		}
		return strings.Join(cited, " "), nil
	}

	// bb and all its parents; the server adds the edges.
	got, err := prove(`[{"domain": "lab", "table": "artifacts", "pk": ["bb"]},
		{"domain": "lab", "table": "artifact_parents", "group": ["bb"]}]`, 0)
	if err != nil || got != "artifacts:bb<- artifact_parents:bb<-aa artifact_parents:bb<-cc complete:bb" && got != "artifacts:bb<- artifact_parents:bb<-cc artifact_parents:bb<-aa complete:bb" {
		t.Fatalf("bb's parents: %q, %v", got, err)
	}
	// A source: its group is empty.
	if got, err := prove(`[{"domain": "lab", "table": "artifact_parents", "group": ["dd"]}]`, 0); err != nil || got != "complete:dd" {
		t.Fatalf("dd's parents: %q, %v", got, err)
	}

	tx("DELETE FROM artifact_parents WHERE child_id = 'bb' AND parent_id = 'cc'")
	checkpoint("an edge deleted")
	if got, err := prove(`[{"domain": "lab", "table": "artifact_parents", "group": ["bb"]}]`, 0); err != nil || got != "artifact_parents:bb<-aa complete:bb" {
		t.Fatalf("bb's parents after the delete: %q, %v", got, err)
	}
	// At the older digest, both parents.
	if got, err := prove(`[{"domain": "lab", "table": "artifact_parents", "group": ["bb"]}]`, twoParents.LSN); err != nil || strings.Count(got, "artifact_parents:bb") != 2 {
		t.Fatalf("bb's parents at LSN %d: %q, %v", twoParents.LSN, got, err)
	}
	tx("DELETE FROM artifacts WHERE id = 'dd'")
	checkpoint("an artifact deleted")

	for sql, want := range map[string]string{
		proveRowsSQL(`[{"domain": "lab", "table": "artifact_parents", "group": ["dd"]}]`, 0):                                                                  "has no group",
		proveRowsSQL(`[{"domain": "lab", "table": "artifacts", "group": ["aa"]}]`, 0):                                                                         "no verifiable group",
		proveRowsSQL(`[{"domain": "lab", "table": "artifact_parents", "group": ["bb"], "pk": ["bb"]}]`, 0):                                                    "not both",
		proveRowsSQL(`[{"domain": "lab", "table": "artifact_parents", "group": ["bb"]}, {"domain": "lab", "table": "artifact_parents", "group": ["bb"]}]`, 0): "twice",
	} {
		_, err := queryRowsProof(ctx, conn, sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "22023" || !strings.Contains(pgErr.Message, want) {
			t.Fatalf("%s: %v, want 22023 with %q", sql, err, want)
		}
	}
}
