package executor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/domains"
	"github.com/correodabid/asql/internal/storage/wal"
)

func mustExecVFK(t *testing.T, ctx context.Context, engine *Engine, session *Session, sql string) {
	t.Helper()
	if _, err := engine.Execute(ctx, session, sql); err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
}

// TestVFKCrossDomainJoinAfterReplay: a JOIN through a cross-domain VERSIONED
// FOREIGN KEY reads the referenced row as of the captured LSN, from the
// referenced domain itself, live and after the WAL is replayed.
func TestVFKCrossDomainJoinAfterReplay(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vfk-replay.wal")
	const join = "SELECT o.id, u.name FROM billing.orders o JOIN identity.users u ON o.user_id = u.id"

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN identity",
		"CREATE TABLE identity.users (id TEXT PRIMARY KEY, name TEXT)",
		"INSERT INTO identity.users (id, name) VALUES ('u1', 'Alice'), ('u2', 'Bob')",
		"COMMIT",
		"BEGIN CROSS DOMAIN billing, identity",
		`CREATE TABLE billing.orders (id TEXT PRIMARY KEY, user_id TEXT, user_lsn INT,
			VERSIONED FOREIGN KEY (user_id) REFERENCES identity.users(id) AS OF user_lsn)`,
		"INSERT INTO billing.orders (id, user_id) VALUES ('o1', 'u1')",
		"COMMIT",
		"BEGIN DOMAIN identity",
		"UPDATE identity.users SET name = 'Alicia' WHERE id = 'u1'",
		"COMMIT",
	} {
		mustExecVFK(t, ctx, engine, session, sql)
	}
	check := func(label string, engine *Engine) {
		t.Helper()
		result, err := engine.Query(ctx, join, []string{"billing"})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["u.name"].StringValue != "Alice" {
			t.Fatalf("%s: joined rows = %v, want o1 with Alice (the version it captured)", label, result.Rows)
		}
	}
	check("live", engine)
	engine.WaitPendingSnapshots()
	_ = store.Close()

	store, err = wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	replayed, err := New(ctx, store, "")
	if err != nil {
		t.Fatal(err)
	}
	check("after replay", replayed)
}

// TestDropLegacyProjectionTables: snapshots from earlier versions hold VFK
// projection tables, which are dropped on load. A user table with the same
// prefix is in the catalog and stays.
func TestDropLegacyProjectionTables(t *testing.T) {
	catalog := domains.NewCatalog()
	catalog.EnsureDomain("billing")
	catalog.RegisterTable("billing", "orders")
	catalog.RegisterTable("billing", "__proj__mine")
	snapshots := []engineSnapshot{{catalog: catalog, state: engineState{domains: map[string]*domainState{
		"billing": {tables: map[string]*tableState{
			"orders":                  {},
			"__proj__mine":            {},
			"__proj__identity__users": {},
		}},
	}}}}
	dropLegacyProjectionTables(snapshots)
	tables := snapshots[0].state.domains["billing"].tables
	if _, ok := tables["__proj__identity__users"]; ok {
		t.Fatal("legacy projection table kept")
	}
	if len(tables) != 2 {
		t.Fatalf("tables = %v, want orders and __proj__mine", tables)
	}
}
