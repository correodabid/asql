package executor

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/correodabid/asql/internal/storage/wal"
)

func mustExecIntrospection(t *testing.T, ctx context.Context, engine *Engine, session *Session, sql string) {
	t.Helper()
	if _, err := engine.Execute(ctx, session, sql); err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
}

func TestRowLSNReturnsCurrentRowHeadLSN(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "row-lsn.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()

	for _, sql := range []string{
		"BEGIN DOMAIN test",
		"CREATE TABLE items (id INT PRIMARY KEY, status TEXT)",
		"INSERT INTO items (id, status) VALUES (1, 'draft')",
		"COMMIT",
		"BEGIN DOMAIN test",
		"UPDATE items SET status = 'published' WHERE id = 1",
		"COMMIT",
	} {
		mustExecIntrospection(t, ctx, engine, session, sql)
	}

	rowLSN, ok, err := engine.RowLSN("test.items", "1")
	if err != nil {
		t.Fatalf("RowLSN(): %v", err)
	}
	if !ok || rowLSN == 0 {
		t.Fatalf("expected positive row LSN, got ok=%v lsn=%d", ok, rowLSN)
	}

	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT _lsn FROM items WHERE id = 1", []string{"test"}, ^uint64(0))
	if err != nil {
		t.Fatalf("SELECT _lsn: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if got := uint64(result.Rows[0]["_lsn"].NumberValue); got != rowLSN {
		t.Fatalf("expected RowLSN()=%d to match SELECT _lsn=%d", rowLSN, got)
	}

	missing, ok, err := engine.RowLSN("test.items", "999")
	if err != nil {
		t.Fatalf("RowLSN() missing row: %v", err)
	}
	if ok || missing != 0 {
		t.Fatalf("expected missing row to return ok=false, got ok=%v lsn=%d", ok, missing)
	}
}

func TestEntityVersionAndHeadLSNReturnLatestVisibleState(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "entity-version.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()

	for _, sql := range []string{
		"BEGIN DOMAIN test",
		"CREATE TABLE items (id INT PRIMARY KEY, status TEXT)",
		"CREATE ENTITY item_aggregate (ROOT items)",
		"COMMIT",
		"BEGIN DOMAIN test",
		"INSERT INTO items (id, status) VALUES (1, 'draft')",
		"COMMIT",
		"BEGIN DOMAIN test",
		"UPDATE items SET status = 'published' WHERE id = 1",
		"COMMIT",
	} {
		mustExecIntrospection(t, ctx, engine, session, sql)
	}

	version, ok, err := engine.EntityVersion("test", "item_aggregate", "1")
	if err != nil {
		t.Fatalf("EntityVersion(): %v", err)
	}
	if !ok || version != 2 {
		t.Fatalf("expected latest entity version 2, got ok=%v version=%d", ok, version)
	}

	headLSN, ok, err := engine.EntityHeadLSN("test", "item_aggregate", "1")
	if err != nil {
		t.Fatalf("EntityHeadLSN(): %v", err)
	}
	if !ok || headLSN == 0 {
		t.Fatalf("expected positive entity head LSN, got ok=%v lsn=%d", ok, headLSN)
	}

	rowLSN, ok, err := engine.RowLSN("test.items", "1")
	if err != nil {
		t.Fatalf("RowLSN(): %v", err)
	}
	if !ok || rowLSN != headLSN {
		t.Fatalf("expected entity head LSN %d to match root row LSN %d (ok=%v)", headLSN, rowLSN, ok)
	}

	versionOneLSN, ok, err := engine.EntityVersionLSN("test", "item_aggregate", "1", 1)
	if err != nil {
		t.Fatalf("EntityVersionLSN(version 1): %v", err)
	}
	if !ok || versionOneLSN == 0 {
		t.Fatalf("expected positive version 1 commit LSN, got ok=%v lsn=%d", ok, versionOneLSN)
	}
	if versionOneLSN >= headLSN {
		t.Fatalf("expected version 1 commit LSN %d to be older than latest head LSN %d", versionOneLSN, headLSN)
	}

	versionTwoLSN, ok, err := engine.EntityVersionLSN("test", "item_aggregate", "1", 2)
	if err != nil {
		t.Fatalf("EntityVersionLSN(version 2): %v", err)
	}
	if !ok || versionTwoLSN != headLSN {
		t.Fatalf("expected version 2 commit LSN %d to match head LSN %d (ok=%v)", versionTwoLSN, headLSN, ok)
	}

	missingVersionLSN, ok, err := engine.EntityVersionLSN("test", "item_aggregate", "1", 99)
	if err != nil {
		t.Fatalf("EntityVersionLSN(missing version): %v", err)
	}
	if ok || missingVersionLSN != 0 {
		t.Fatalf("expected missing version lookup to return ok=false, got ok=%v lsn=%d", ok, missingVersionLSN)
	}
}

func TestResolveReferenceMatchesCurrentVFKCaptureSemantics(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "resolve-reference.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()

	for _, sql := range []string{
		"BEGIN DOMAIN test",
		"CREATE TABLE items (id INT PRIMARY KEY, status TEXT)",
		"CREATE TABLE item_steps (id INT PRIMARY KEY, item_id INT REFERENCES items(id), label TEXT)",
		"CREATE TABLE audit_entries (id INT PRIMARY KEY, note TEXT)",
		"CREATE ENTITY item_aggregate (ROOT items, INCLUDES item_steps)",
		"COMMIT",
		"BEGIN DOMAIN test",
		"INSERT INTO items (id, status) VALUES (1, 'draft')",
		"INSERT INTO audit_entries (id, note) VALUES (7, 'created')",
		"COMMIT",
		"BEGIN DOMAIN test",
		"UPDATE items SET status = 'published' WHERE id = 1",
		"COMMIT",
	} {
		mustExecIntrospection(t, ctx, engine, session, sql)
	}

	resolvedEntity, ok, err := engine.ResolveReference("test.items", "1")
	if err != nil {
		t.Fatalf("ResolveReference() entity root: %v", err)
	}
	if !ok || resolvedEntity != 2 {
		t.Fatalf("expected resolve_reference() to return entity version 2, got ok=%v value=%d", ok, resolvedEntity)
	}

	auditRowLSN, ok, err := engine.RowLSN("test.audit_entries", "7")
	if err != nil {
		t.Fatalf("RowLSN() audit row: %v", err)
	}
	if !ok || auditRowLSN == 0 {
		t.Fatalf("expected positive audit row LSN, got ok=%v lsn=%d", ok, auditRowLSN)
	}

	resolvedAudit, ok, err := engine.ResolveReference("test.audit_entries", "7")
	if err != nil {
		t.Fatalf("ResolveReference() audit row: %v", err)
	}
	if !ok || resolvedAudit != auditRowLSN {
		t.Fatalf("expected resolve_reference()=%d to match row_lsn()=%d for non-entity table", resolvedAudit, auditRowLSN)
	}

	missing, ok, err := engine.ResolveReference("test.items", "999")
	if err != nil {
		t.Fatalf("ResolveReference() missing row: %v", err)
	}
	if ok || missing != 0 {
		t.Fatalf("expected missing resolve_reference() to return ok=false, got ok=%v value=%d", ok, missing)
	}

	for _, sql := range []string{
		"BEGIN DOMAIN test",
		"INSERT INTO item_steps (id, item_id, label) VALUES (11, 1, 'mix')",
		"COMMIT",
	} {
		mustExecIntrospection(t, ctx, engine, session, sql)
	}

	if _, _, err := engine.ResolveReference("test.item_steps", "11"); err == nil {
		t.Fatal("expected resolve_reference() on child entity table to fail")
	}
}

func TestEntityChangesReturnsDeterministicEntityBacklog(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "entity-changes.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()

	for _, sql := range []string{
		"BEGIN DOMAIN history",
		"CREATE TABLE items (id INT PRIMARY KEY, status TEXT)",
		"CREATE TABLE item_steps (id INT PRIMARY KEY, item_id INT REFERENCES items(id), label TEXT)",
		"CREATE ENTITY item_aggregate (ROOT items, INCLUDES item_steps)",
		"COMMIT",
		"BEGIN DOMAIN history",
		"INSERT INTO items (id, status) VALUES (1, 'draft')",
		"COMMIT",
		"BEGIN DOMAIN history",
		"INSERT INTO item_steps (id, item_id, label) VALUES (10, 1, 'mix')",
		"COMMIT",
		"BEGIN DOMAIN history",
		"UPDATE items SET status = 'published' WHERE id = 1",
		"COMMIT",
	} {
		mustExecIntrospection(t, ctx, engine, session, sql)
	}

	events, err := engine.EntityChanges(ctx, EntityChangesRequest{Domain: "history", Entity: "item_aggregate"})
	if err != nil {
		t.Fatalf("EntityChanges(): %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 entity change events, got %d", len(events))
	}
	if events[0].Timestamp == 0 || events[1].Timestamp == 0 || events[2].Timestamp == 0 {
		t.Fatalf("expected non-zero timestamps, got %#v", events)
	}

	if events[0].RootPK != "1" || events[0].Version != 1 || !reflect.DeepEqual(events[0].Tables, []string{"items"}) {
		t.Fatalf("unexpected first event: %#v", events[0])
	}
	if events[1].RootPK != "1" || events[1].Version != 2 || !reflect.DeepEqual(events[1].Tables, []string{"item_steps"}) {
		t.Fatalf("unexpected second event: %#v", events[1])
	}
	if events[2].RootPK != "1" || events[2].Version != 3 || !reflect.DeepEqual(events[2].Tables, []string{"items"}) {
		t.Fatalf("unexpected third event: %#v", events[2])
	}
	if events[0].CommitLSN >= events[1].CommitLSN || events[1].CommitLSN >= events[2].CommitLSN {
		t.Fatalf("expected ascending commit LSNs, got %#v", events)
	}

	fromSecond, err := engine.EntityChanges(ctx, EntityChangesRequest{Domain: "history", Entity: "item_aggregate", FromLSN: events[1].CommitLSN})
	if err != nil {
		t.Fatalf("EntityChanges(from second): %v", err)
	}
	if len(fromSecond) != 2 {
		t.Fatalf("expected 2 entity change events from second commit, got %d", len(fromSecond))
	}

	rootOnly, err := engine.EntityChanges(ctx, EntityChangesRequest{Domain: "history", Entity: "item_aggregate", RootPK: "1", Limit: 1})
	if err != nil {
		t.Fatalf("EntityChanges(root filter): %v", err)
	}
	if len(rootOnly) != 1 || rootOnly[0].Version != 1 {
		t.Fatalf("expected limited root-filtered first version, got %#v", rootOnly)
	}
}
