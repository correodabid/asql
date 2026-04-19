package executor

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/storage/wal"
)

// mustExecProj is a helper that fatals on execution errors.
func mustExecProj(t *testing.T, ctx context.Context, engine *Engine, session *Session, sql string) {
	t.Helper()
	if _, err := engine.Execute(ctx, session, sql); err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
}

// TestVFKProjectionBasic verifies that when a table with a VERSIONED FOREIGN KEY
// is created in a subscriber domain, rows inserted into the source domain are
// automatically projected so JOIN queries can resolve them locally without a
// cross-domain hop.
func TestVFKProjectionBasic(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "proj-basic.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()

	// ── identity domain: create users table and insert 2 rows ──────────────
	mustExecProj(t, ctx, engine, session, "BEGIN DOMAIN identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE identity.users (
		id   TEXT PRIMARY KEY,
		name TEXT,
		email TEXT
	)`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name, email) VALUES ('u1', 'Alice', 'alice@example.com')`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name, email) VALUES ('u2', 'Bob', 'bob@example.com')`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	// ── billing domain: create orders with VFK referencing identity.users ───
	mustExecProj(t, ctx, engine, session, "BEGIN CROSS DOMAIN billing, identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE billing.orders (
		id       TEXT PRIMARY KEY,
		user_id  TEXT,
		user_lsn INT,
		amount   INT,
		VERSIONED FOREIGN KEY (user_id) REFERENCES identity.users(id) AS OF user_lsn
	)`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO billing.orders (id, user_id, amount) VALUES ('o1', 'u1', 100)`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO billing.orders (id, user_id, amount) VALUES ('o2', 'u2', 200)`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	// ── verify projection table is present in billing ───────────────────────
	state := engine.readState.Load()
	billingDS, ok := state.domains["billing"]
	if !ok {
		t.Fatal("billing domain not found")
	}
	projName := projectionTableName("identity", "users")
	projTable, ok := billingDS.tables[projName]
	if !ok {
		t.Fatalf("projection table %q not found in billing domain", projName)
	}
	if !projTable.isProjection {
		t.Error("projection table should have isProjection=true")
	}
	if len(projTable.rows) != 2 {
		t.Errorf("projection table: expected 2 rows, got %d", len(projTable.rows))
	}

	// ── verify __proj__ tables are hidden from schema introspection ──────────
	snap := engine.SchemaSnapshot([]string{"billing"})
	for _, domain := range snap.Domains {
		for _, tbl := range domain.Tables {
			if strings.HasPrefix(tbl.Name, "__proj__") {
				t.Errorf("schema snapshot should not expose projection table %q", tbl.Name)
			}
		}
	}

	// ── verify JOIN resolves via local projection ────────────────────────────
	result, err := engine.TimeTravelQueryAsOfLSN(
		ctx,
		"SELECT o.id, u.name, o.amount FROM billing.orders o JOIN identity.users u ON o.user_id = u.id ORDER BY o.id",
		[]string{"billing"},
		^uint64(0),
	)
	if err != nil {
		t.Fatalf("cross-domain JOIN: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 JOIN rows, got %d", len(result.Rows))
	}
	// Row 0: o1 + Alice
	if result.Rows[0]["u.name"].StringValue != "Alice" {
		t.Errorf("row 0 name: want Alice, got %v", result.Rows[0]["u.name"])
	}
	if result.Rows[1]["u.name"].StringValue != "Bob" {
		t.Errorf("row 1 name: want Bob, got %v", result.Rows[1]["u.name"])
	}
}

// TestVFKProjectionFanoutUpdate verifies that UPDATE on the source domain
// propagates to the subscriber's projection table.
func TestVFKProjectionFanoutUpdate(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "proj-update.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()

	mustExecProj(t, ctx, engine, session, "BEGIN DOMAIN identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE identity.users (id TEXT PRIMARY KEY, name TEXT)`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name) VALUES ('u1', 'Alice')`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	mustExecProj(t, ctx, engine, session, "BEGIN CROSS DOMAIN billing, identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE billing.orders (
		id TEXT PRIMARY KEY, user_id TEXT, user_lsn INT,
		VERSIONED FOREIGN KEY (user_id) REFERENCES identity.users(id) AS OF user_lsn
	)`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO billing.orders (id, user_id) VALUES ('o1', 'u1')`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	// Update user name in identity domain.
	mustExecProj(t, ctx, engine, session, "BEGIN DOMAIN identity")
	mustExecProj(t, ctx, engine, session, `UPDATE identity.users SET name = 'Alicia' WHERE id = 'u1'`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	// Projection should reflect the updated name.
	state := engine.readState.Load()
	projTable := state.domains["billing"].tables[projectionTableName("identity", "users")]
	if projTable == nil || len(projTable.rows) == 0 {
		t.Fatal("projection table missing or empty after update")
	}
	row := rowToMap(projTable, projTable.rows[0])
	if row["name"].StringValue != "Alicia" {
		t.Errorf("projection update: expected name=Alicia, got %v", row["name"].StringValue)
	}
}

// TestVFKProjectionFanoutDelete verifies that DELETE on the source domain
// removes the corresponding row from the subscriber's projection table.
func TestVFKProjectionFanoutDelete(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "proj-delete.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()

	mustExecProj(t, ctx, engine, session, "BEGIN DOMAIN identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE identity.users (id TEXT PRIMARY KEY, name TEXT)`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name) VALUES ('u1', 'Alice')`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name) VALUES ('u2', 'Bob')`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	mustExecProj(t, ctx, engine, session, "BEGIN CROSS DOMAIN billing, identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE billing.orders (
		id TEXT PRIMARY KEY, user_id TEXT, user_lsn INT,
		VERSIONED FOREIGN KEY (user_id) REFERENCES identity.users(id) AS OF user_lsn
	)`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	// Delete one user from identity.
	mustExecProj(t, ctx, engine, session, "BEGIN DOMAIN identity")
	mustExecProj(t, ctx, engine, session, `DELETE FROM identity.users WHERE id = 'u2'`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	// Projection should have only 1 row.
	state := engine.readState.Load()
	projTable := state.domains["billing"].tables[projectionTableName("identity", "users")]
	if projTable == nil {
		t.Fatal("projection table missing after delete")
	}
	if len(projTable.rows) != 1 {
		t.Errorf("projection after delete: expected 1 row, got %d", len(projTable.rows))
	}
	if rowToMap(projTable, projTable.rows[0])["name"].StringValue != "Alice" {
		t.Errorf("remaining row should be Alice, got %v", rowToMap(projTable, projTable.rows[0])["name"].StringValue)
	}
}

// TestVFKProjectionFanoutBatchesSourceMutations verifies that when multiple
// source-table mutations happen in one commit, the subscriber projection is
// rebuilt from the final committed state.
func TestVFKProjectionFanoutBatchesSourceMutations(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "proj-batch.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()

	mustExecProj(t, ctx, engine, session, "BEGIN DOMAIN identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE identity.users (id TEXT PRIMARY KEY, name TEXT)`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name) VALUES ('u1', 'Alice')`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name) VALUES ('u2', 'Bob')`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	mustExecProj(t, ctx, engine, session, "BEGIN CROSS DOMAIN billing, identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE billing.orders (
		id TEXT PRIMARY KEY, user_id TEXT, user_lsn INT,
		VERSIONED FOREIGN KEY (user_id) REFERENCES identity.users(id) AS OF user_lsn
	)`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	mustExecProj(t, ctx, engine, session, "BEGIN DOMAIN identity")
	mustExecProj(t, ctx, engine, session, `UPDATE identity.users SET name = 'Alicia' WHERE id = 'u1'`)
	mustExecProj(t, ctx, engine, session, `DELETE FROM identity.users WHERE id = 'u2'`)
	mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name) VALUES ('u3', 'Cara')`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	state := engine.readState.Load()
	projTable := state.domains["billing"].tables[projectionTableName("identity", "users")]
	if projTable == nil {
		t.Fatal("projection table missing after batched source mutations")
	}
	if len(projTable.rows) != 2 {
		t.Fatalf("projection after batched mutations: expected 2 rows, got %d", len(projTable.rows))
	}

	gotByID := make(map[string]string, len(projTable.rows))
	for _, row := range projTable.rows {
		mapped := rowToMap(projTable, row)
		gotByID[mapped["id"].StringValue] = mapped["name"].StringValue
	}
	if len(gotByID) != 2 {
		t.Fatalf("projection id map: expected 2 rows, got %d", len(gotByID))
	}
	if gotByID["u1"] != "Alicia" {
		t.Fatalf("projection row u1: got %q want Alicia", gotByID["u1"])
	}
	if _, exists := gotByID["u2"]; exists {
		t.Fatalf("projection should not contain deleted row u2")
	}
	if gotByID["u3"] != "Cara" {
		t.Fatalf("projection row u3: got %q want Cara", gotByID["u3"])
	}
}

// TestVFKProjectionReplayIdempotent verifies that projections are correctly
// reconstructed when the engine is restarted and replays the WAL from scratch.
func TestVFKProjectionReplayIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "proj-replay.wal")

	// ── first engine run ─────────────────────────────────────────────────────
	{
		store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
		if err != nil {
			t.Fatalf("new store (run 1): %v", err)
		}
		engine, err := New(ctx, store, "")
		if err != nil {
			t.Fatalf("new engine (run 1): %v", err)
		}
		session := engine.NewSession()

		mustExecProj(t, ctx, engine, session, "BEGIN DOMAIN identity")
		mustExecProj(t, ctx, engine, session, `CREATE TABLE identity.users (id TEXT PRIMARY KEY, name TEXT)`)
		mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name) VALUES ('u1', 'Alice')`)
		mustExecProj(t, ctx, engine, session, `INSERT INTO identity.users (id, name) VALUES ('u2', 'Bob')`)
		mustExecProj(t, ctx, engine, session, "COMMIT")

		mustExecProj(t, ctx, engine, session, "BEGIN CROSS DOMAIN billing, identity")
		mustExecProj(t, ctx, engine, session, `CREATE TABLE billing.orders (
			id TEXT PRIMARY KEY, user_id TEXT, user_lsn INT,
			VERSIONED FOREIGN KEY (user_id) REFERENCES identity.users(id) AS OF user_lsn
		)`)
		mustExecProj(t, ctx, engine, session, `INSERT INTO billing.orders (id, user_id) VALUES ('o1', 'u1')`)
		mustExecProj(t, ctx, engine, session, "COMMIT")

		engine.WaitPendingSnapshots()
		_ = store.Close()
	}

	// ── second engine run: replay the same WAL ────────────────────────────────
	store2, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store (run 2): %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	engine2, err := New(ctx, store2, "")
	if err != nil {
		t.Fatalf("new engine (run 2): %v", err)
	}

	state := engine2.readState.Load()

	// Projection table must survive replay.
	billingDS, ok := state.domains["billing"]
	if !ok {
		t.Fatal("billing domain missing after replay")
	}
	projTable, ok := billingDS.tables[projectionTableName("identity", "users")]
	if !ok {
		t.Fatal("projection table missing after replay")
	}
	if len(projTable.rows) != 2 {
		t.Errorf("projection rows after replay: expected 2, got %d", len(projTable.rows))
	}

	// JOIN via projection should still work.
	session2 := engine2.NewSession()
	result, err := engine2.TimeTravelQueryAsOfLSN(
		ctx,
		"SELECT o.id, u.name FROM billing.orders o JOIN identity.users u ON o.user_id = u.id",
		[]string{"billing"},
		^uint64(0),
	)
	if err != nil {
		t.Fatalf("JOIN after replay: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 joined row, got %d", len(result.Rows))
	}
	_ = session2
}

// TestVFKProjectionSchemaHidden verifies that __proj__* tables never appear
// in SchemaSnapshot output regardless of domain filter.
func TestVFKProjectionSchemaHidden(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "proj-schema.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()

	mustExecProj(t, ctx, engine, session, "BEGIN DOMAIN identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE identity.users (id TEXT PRIMARY KEY, name TEXT)`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	mustExecProj(t, ctx, engine, session, "BEGIN CROSS DOMAIN billing, identity")
	mustExecProj(t, ctx, engine, session, `CREATE TABLE billing.invoices (
		id TEXT PRIMARY KEY, user_id TEXT, user_lsn INT,
		VERSIONED FOREIGN KEY (user_id) REFERENCES identity.users(id) AS OF user_lsn
	)`)
	mustExecProj(t, ctx, engine, session, "COMMIT")

	snap := engine.SchemaSnapshot(nil) // all domains
	for _, domain := range snap.Domains {
		for _, tbl := range domain.Tables {
			if strings.HasPrefix(tbl.Name, "__proj__") {
				t.Errorf("domain %q: schema snapshot exposed internal table %q", domain.Name, tbl.Name)
			}
		}
	}

	// The real table must still appear.
	found := false
	for _, domain := range snap.Domains {
		if domain.Name == "billing" {
			for _, tbl := range domain.Tables {
				if tbl.Name == "invoices" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("billing.invoices should appear in schema snapshot")
	}
}
