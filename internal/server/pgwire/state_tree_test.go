package pgwire

import (
	"context"
	"testing"

	"github.com/correodabid/asql/internal/storage/ledgerdigest"

	"github.com/jackc/pgx/v5"
)

// TestStateTreeFollowsTheState: with a state key, every digest is version 3
// and signs the root of the VERIFIABLE tables' rows at its LSN. The root the
// node keeps up incrementally equals one built from scratch from the state,
// through updates, deletes, schema changes that rewrite rows without new
// versions, a dropped table and a table made verifiable later; changes to
// other tables leave it alone.
func TestStateTreeFollowsTheState(t *testing.T) {
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
	if server.ledger.stateTree == nil {
		t.Fatal("no state tree with a state key")
	}

	tx := func(sqls ...string) {
		t.Helper()
		for _, sql := range append(append([]string{"BEGIN DOMAIN lab"}, sqls...), "COMMIT") {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}
	var signed []ledgerdigest.Digest
	// checkpoint waits for the digest of the head and checks its state
	// root against a tree built from scratch from the state at its LSN.
	checkpoint := func(step string) ledgerdigest.Digest {
		t.Helper()
		d := waitForDigestAtHead(t, server)
		if err := d.Verify(keys); err != nil || d.Version != ledgerdigest.Version3 {
			t.Fatalf("%s: digest version %d, %v", step, d.Version, err)
		}
		view, err := server.engine.VerifiableViewAt(ctx, d.LSN)
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := buildStateTree(*server.ledger.stateKey, view)
		if err != nil {
			t.Fatal(err)
		}
		if d.StateRoot != fresh.Root() {
			t.Fatalf("%s: the signed state root differs from the state's (%d rows)", step, fresh.Len())
		}
		signed = append(signed, d)
		return d
	}

	tx("CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT) VERIFIABLE",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE",
		"CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)")
	tx("INSERT INTO artifacts (id, kind) VALUES ('aa', 'source'), ('bb', 'extraction'), ('cc', 'run')",
		"INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('bb', 'aa', 1), ('cc', 'bb', 1)")
	first := checkpoint("inserts")

	tx("INSERT INTO notes (id, body) VALUES (1, 'scratch')")
	if d := checkpoint("a change outside the verifiable tables"); d.StateRoot != first.StateRoot {
		t.Fatal("a change to a table that is not verifiable changed the state root")
	}
	tx("UPDATE artifacts SET kind = 'source-v2' WHERE id = 'aa'", "DELETE FROM artifacts WHERE id = 'cc'")
	checkpoint("update and delete")
	tx("ALTER TABLE artifacts ADD COLUMN sha TEXT DEFAULT 'none'")
	checkpoint("add column with a default")
	tx("ALTER TABLE artifacts RENAME COLUMN kind TO role")
	checkpoint("rename column")
	tx("ALTER TABLE notes SET VERIFIABLE")
	checkpoint("a table made verifiable")
	tx("CREATE TABLE scratch (id INT PRIMARY KEY) VERIFIABLE", "INSERT INTO scratch (id) VALUES (1)")
	before := checkpoint("another table")
	tx("DROP TABLE scratch")
	if d := checkpoint("drop table"); d.StateRoot == before.StateRoot {
		t.Fatal("dropping a verifiable table left its rows in the tree")
	}

	// An older checkpoint's tree, kept or rebuilt, has the root signed then.
	for _, d := range []ledgerdigest.Digest{signed[0], signed[len(signed)-2]} {
		tree, _, err := server.ledger.stateTree.TreeAt(ctx, d.LSN)
		if err != nil || tree.Root() != d.StateRoot {
			t.Fatalf("tree at LSN %d: %v, root differs: %v", d.LSN, err, tree.Root() != d.StateRoot)
		}
	}
	server.ledger.stateTree.mu.Lock()
	server.ledger.stateTree.recent = nil
	server.ledger.stateTree.mu.Unlock()
	if tree, _, err := server.ledger.stateTree.TreeAt(ctx, signed[0].LSN); err != nil || tree.Root() != signed[0].StateRoot {
		t.Fatalf("the rebuilt tree at LSN %d: %v", signed[0].LSN, err)
	}
}
