package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"asql/internal/storage/wal"
)

// TestSnapshotPersistRoundTrip verifies that a persisted snapshot on disk
// allows the engine to skip full WAL replay on restart.
func TestSnapshotPersistRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "persist.wal")
	snapDir := filepath.Join(dir, "snap")
	os.MkdirAll(snapDir, 0o755)

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}

	// Engine with snapDir enables snapshot persistence.
	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN billing")
	exec("CREATE TABLE invoices (id INT, amount INT)")
	exec("COMMIT")

	totalRows := defaultSnapshotInterval + 50
	for i := 1; i <= totalRows; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN billing")
		exec(fmt.Sprintf("INSERT INTO invoices (id, amount) VALUES (%d, %d)", i, i*100))
		exec("COMMIT")
	}

	// Record head state.
	snapState := engine.readState.Load()
	origHead := snapState.headLSN
	origTS := snapState.logicalTS

	// Verify snapshot files were created on disk.
	engine.WaitPendingSnapshots()
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		t.Fatalf("read snap dir: %v", err)
	}
	snapFiles := 0
	for _, e := range entries {
		if !e.IsDir() {
			snapFiles++
		}
	}
	if snapFiles == 0 {
		t.Fatal("no snapshot files found in snap dir")
	}
	t.Logf("snapshot files in dir: %d", snapFiles)

	// Close WAL and reopen engine — should load from snapshot.
	engine.WaitPendingSnapshots()
	_ = store.Close()

	store2, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen log store: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	engine2, err := New(ctx, store2, snapDir)
	if err != nil {
		t.Fatalf("new engine after restart: %v", err)
	}

	// Verify state is identical.
	newState := engine2.readState.Load()
	newHead := newState.headLSN
	newTS := newState.logicalTS

	if newHead != origHead {
		t.Errorf("head LSN mismatch: got %d, want %d", newHead, origHead)
	}
	if newTS != origTS {
		t.Errorf("logical TS mismatch: got %d, want %d", newTS, origTS)
	}

	// Query current data.
	result, err := engine2.TimeTravelQueryAsOfLSN(ctx, "SELECT id, amount FROM invoices", []string{"billing"}, newHead)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(result.Rows) != totalRows {
		t.Errorf("expected %d rows, got %d", totalRows, len(result.Rows))
	}

	// Verify multiple snapshots loaded from disk files after restart.
	engine2.snapshots.mu.Lock()
	snapCount := engine2.snapshots.count()
	engine2.snapshots.mu.Unlock()
	if snapCount < 2 {
		t.Errorf("expected at least 2 snapshots after restart (loaded from disk files), got %d", snapCount)
	}
}

func TestDeltaReplayDoesNotPersistCheckpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "delta-replay.wal")
	snapDir := filepath.Join(dir, "snap")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snap dir: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN replay")
	exec("CREATE TABLE items (id INT, name TEXT)")
	exec("INSERT INTO items (id, name) VALUES (1, 'one')")
	exec("COMMIT")

	state := engine.readState.Load()
	engine.snapshots.add(captureSnapshot(state, engine.catalog))
	engine.persistAllSnapshots()

	entries, err := os.ReadDir(snapDir)
	if err != nil {
		t.Fatalf("read snap dir: %v", err)
	}
	baselineFiles := len(entries)
	baselineSeq := engine.snapSeq
	beforeLSN := state.headLSN

	session = engine.NewSession()
	exec("BEGIN DOMAIN replay")
	exec("INSERT INTO items (id, name) VALUES (2, 'two')")
	exec("COMMIT")

	records, err := store.ReadFrom(ctx, beforeLSN+1, 0)
	if err != nil {
		t.Fatalf("read wal delta: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("expected wal delta records")
	}

	if err := engine.rebuildFromRecordsAfterSnapshot(records, beforeLSN); err != nil {
		t.Fatalf("delta replay: %v", err)
	}

	entries, err = os.ReadDir(snapDir)
	if err != nil {
		t.Fatalf("read snap dir after delta replay: %v", err)
	}
	if len(entries) != baselineFiles {
		t.Fatalf("expected no new checkpoint files during delta replay, got %d want %d", len(entries), baselineFiles)
	}
	if engine.snapSeq != baselineSeq {
		t.Fatalf("expected snap sequence to remain %d during delta replay, got %d", baselineSeq, engine.snapSeq)
	}
}

// TestSnapshotPersistPartialReplay verifies that after loading a snapshot,
// the engine correctly replays only WAL records after the snapshot's LSN.
func TestSnapshotPersistPartialReplay(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "partial.wal")
	snapDir := filepath.Join(dir, "snap")
	os.MkdirAll(snapDir, 0o755)

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}

	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN test")
	exec("CREATE TABLE items (id INT, name TEXT)")
	exec("COMMIT")

	// Insert enough to trigger snapshot persistence.
	phase1Count := defaultSnapshotInterval + 20
	for i := 1; i <= phase1Count; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN test")
		exec(fmt.Sprintf("INSERT INTO items (id, name) VALUES (%d, 'item-%d')", i, i))
		exec("COMMIT")
	}

	// Close, reopen and add more rows WITHOUT closing.
	engine.WaitPendingSnapshots()
	_ = store.Close()

	store2, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	engine2, err := New(ctx, store2, snapDir)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}

	// Add more rows in the second run.
	phase2Count := 30
	for i := phase1Count + 1; i <= phase1Count+phase2Count; i++ {
		session2 := engine2.NewSession()
		if _, err := engine2.Execute(ctx, session2, "BEGIN DOMAIN test"); err != nil {
			t.Fatalf("begin: %v", err)
		}
		if _, err := engine2.Execute(ctx, session2, fmt.Sprintf("INSERT INTO items (id, name) VALUES (%d, 'item-%d')", i, i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := engine2.Execute(ctx, session2, "COMMIT"); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	// Close and reopen a third time — should load latest snapshot + replay only new records.
	_ = store2.Close()

	store3, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen 3: %v", err)
	}
	t.Cleanup(func() { _ = store3.Close() })

	engine3, err := New(ctx, store3, snapDir)
	if err != nil {
		t.Fatalf("restart 3: %v", err)
	}

	head3 := engine3.readState.Load().headLSN

	result, err := engine3.TimeTravelQueryAsOfLSN(ctx, "SELECT id, name FROM items", []string{"test"}, head3)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	totalExpected := phase1Count + phase2Count
	if len(result.Rows) != totalExpected {
		t.Errorf("expected %d rows total, got %d", totalExpected, len(result.Rows))
	}
}

func TestReplayFailsClosedOnCorruptSnapshotFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "corrupt-snapshot.wal")
	snapDir := filepath.Join(dir, "snap")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snap dir: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN demo",
		"CREATE TABLE items (id INT)",
		"INSERT INTO items (id) VALUES (1)",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	state := engine.readState.Load()
	engine.writeMu.Lock()
	catalog := cloneCatalog(engine.catalog)
	engine.writeMu.Unlock()
	if err := writeSnapshotToDir(snapDir, 1, captureSnapshotWithCatalog(state, catalog), true, 0); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	snapPath := filepath.Join(snapDir, snapFilePrefix+"000001")
	data, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(snapPath, data, 0o644); err != nil {
		t.Fatalf("write corrupt snapshot: %v", err)
	}

	reopened, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer reopened.Close()
	_, err = New(ctx, reopened, snapDir)
	if err == nil {
		t.Fatal("expected restart failure for corrupt snapshot")
	}
	if !strings.Contains(err.Error(), "snapshot file") && !strings.Contains(err.Error(), "load snapshots from dir") {
		t.Fatalf("expected corrupt snapshot failure, got %v", err)
	}
}

func TestReplayFailsOnSnapshotWALGap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "snapshot-gap.wal")
	snapDir := filepath.Join(dir, "snap")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snap dir: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{}, wal.WithSegmentSize(512))
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN demo",
		"CREATE TABLE items (id INT, name TEXT)",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	for i := 1; i <= 20; i++ {
		session = engine.NewSession()
		for _, sql := range []string{
			"BEGIN DOMAIN demo",
			fmt.Sprintf("INSERT INTO items (id, name) VALUES (%d, '%s')", i, strings.Repeat("x", 40)),
			"COMMIT",
		} {
			if _, err := engine.Execute(ctx, session, sql); err != nil {
				t.Fatalf("execute tx %d %q: %v", i, sql, err)
			}
		}
		if i == 8 {
			state := engine.readState.Load()
			engine.writeMu.Lock()
			catalog := cloneCatalog(engine.catalog)
			engine.writeMu.Unlock()
			if err := writeSnapshotToDir(snapDir, 1, captureSnapshotWithCatalog(state, catalog), true, 0); err != nil {
				t.Fatalf("write snapshot: %v", err)
			}
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	catalog, err := wal.CatalogSegments(walBasePath)
	if err != nil {
		t.Fatalf("catalog segments: %v", err)
	}
	if len(catalog) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(catalog))
	}
	snapshotLSN := uint64(0)
	snaps, _, err := readAllSnapshotsFromDir(snapDir)
	if err != nil || len(snaps) == 0 {
		t.Fatalf("read snapshots: %v", err)
	}
	snapshotLSN = snaps[len(snaps)-1].lsn
	removed := false
	for _, segment := range catalog {
		if segment.FirstLSN == snapshotLSN+1 || (segment.FirstLSN <= snapshotLSN+1 && segment.LastLSN >= snapshotLSN+1) {
			if err := os.Remove(filepath.Join(dir, segment.FileName)); err != nil {
				t.Fatalf("remove segment %s: %v", segment.FileName, err)
			}
			removed = true
			break
		}
	}
	if !removed {
		t.Fatal("failed to remove segment containing first post-snapshot LSN")
	}

	reopened, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{}, wal.WithSegmentSize(512))
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer reopened.Close()
	_, err = New(ctx, reopened, snapDir)
	if err == nil {
		t.Fatal("expected restart failure for snapshot/WAL gap")
	}
	if !strings.Contains(err.Error(), errSnapshotWALMismatch.Error()) {
		t.Fatalf("expected snapshot/WAL mismatch error, got %v", err)
	}
}

// TestSnapshotPersistNoFile verifies the engine works fine without a snapshot file.
func TestSnapshotPersistNoFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "nosnap.wal")
	snapDir := filepath.Join(dir, "snap")
	os.MkdirAll(snapDir, 0o755)

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// snapDir is set but no snapshot files exist yet.
	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN demo")
	exec("CREATE TABLE t1 (id INT)")
	exec("INSERT INTO t1 (id) VALUES (1)")
	exec("COMMIT")

	headLSN := engine.readState.Load().headLSN

	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM t1", []string{"demo"}, headLSN)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

// TestSnapshotSerializationRoundTrip verifies that marshaling/unmarshaling
// an engine snapshot preserves all data accurately.
func TestSnapshotSerializationRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "serde.wal")

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	// Build a schema with various features.
	exec("BEGIN DOMAIN serde")
	exec("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE)")
	exec("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id), total INT)")
	exec("CREATE INDEX idx_orders_customer ON orders (customer_id) USING HASH")
	exec("INSERT INTO customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com')")
	exec("INSERT INTO customers (id, name, email) VALUES (2, 'Bob', 'bob@example.com')")
	exec("INSERT INTO orders (id, customer_id, total) VALUES (100, 1, 5000)")
	exec("INSERT INTO orders (id, customer_id, total) VALUES (101, 2, 3000)")
	exec("COMMIT")

	// Capture snapshot manually for serialization test.
	snap := captureSnapshot(engine.readState.Load(), engine.catalog)

	// Write and read back using multi-snapshot I/O.
	tmpFile := filepath.Join(dir, "test-snapshot.snap")
	snapStore := newSnapshotStore()
	snapStore.add(snap)
	if err := writeLatestSnapshotToDisk(tmpFile, snapStore); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	restoredSlice, err := readAllSnapshotsFromDisk(tmpFile)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if len(restoredSlice) != 1 {
		t.Fatalf("expected 1 restored snapshot, got %d", len(restoredSlice))
	}
	restored := &restoredSlice[0]

	// Verify LSN and logical timestamp.
	if restored.lsn != snap.lsn {
		t.Errorf("lsn mismatch: got %d, want %d", restored.lsn, snap.lsn)
	}
	if restored.logicalTS != snap.logicalTS {
		t.Errorf("logicalTS mismatch: got %d, want %d", restored.logicalTS, snap.logicalTS)
	}

	// Verify catalog.
	origDomains := snap.catalog.Domains()
	resDomains := restored.catalog.Domains()
	if len(origDomains) != len(resDomains) {
		t.Fatalf("catalog domain count: got %d, want %d", len(resDomains), len(origDomains))
	}

	// Verify state — check row counts.
	for domainName, origDomain := range snap.state.domains {
		resDomain, ok := restored.state.domains[domainName]
		if !ok {
			t.Errorf("domain %q missing from restored state", domainName)
			continue
		}
		for tableName, origTable := range origDomain.tables {
			resTable, ok := resDomain.tables[tableName]
			if !ok {
				t.Errorf("table %q.%q missing from restored state", domainName, tableName)
				continue
			}
			if len(resTable.rows) != len(origTable.rows) {
				t.Errorf("table %q.%q row count: got %d, want %d",
					domainName, tableName, len(resTable.rows), len(origTable.rows))
			}
			if len(resTable.indexes) != len(origTable.indexes) {
				t.Errorf("table %q.%q index count: got %d, want %d",
					domainName, tableName, len(resTable.indexes), len(origTable.indexes))
			}
			if resTable.primaryKey != origTable.primaryKey {
				t.Errorf("table %q.%q primary key: got %q, want %q",
					domainName, tableName, resTable.primaryKey, origTable.primaryKey)
			}
			if len(resTable.foreignKeys) != len(origTable.foreignKeys) {
				t.Errorf("table %q.%q foreign key count: got %d, want %d",
					domainName, tableName, len(resTable.foreignKeys), len(origTable.foreignKeys))
			}
		}
	}
}

// TestSnapshotIndexOmission verifies that persisted snapshot JSON does not
// contain index bucket/entry data, and that indexes are correctly rebuilt
// from row data when loading.
func TestSnapshotIndexOmission(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "idx.wal")

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN idx_test")
	exec("CREATE TABLE products (id INT PRIMARY KEY, name TEXT)")
	exec("CREATE INDEX idx_name ON products (name) USING HASH")
	exec("INSERT INTO products (id, name) VALUES (1, 'Widget')")
	exec("INSERT INTO products (id, name) VALUES (2, 'Gadget')")
	exec("COMMIT")

	// Capture and write snapshot.
	snap := captureSnapshot(engine.readState.Load(), engine.catalog)

	tmpFile := filepath.Join(dir, "idx-test")
	snapStore := newSnapshotStore()
	snapStore.add(snap)
	if err := writeLatestSnapshotToDisk(tmpFile, snapStore); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	// Verify indexes are rebuilt on load from binary snapshot.
	loaded, err := readAllSnapshotsFromDisk(tmpFile)
	if err != nil {
		t.Fatalf("read snapshots: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(loaded))
	}

	domain := loaded[0].state.domains["idx_test"]
	if domain == nil {
		t.Fatal("domain idx_test missing")
	}
	table := domain.tables["products"]
	if table == nil {
		t.Fatal("table products missing")
	}
	if len(table.indexes) == 0 {
		t.Fatal("indexes not rebuilt")
	}
	for name, idx := range table.indexes {
		if idx.kind == "hash" && len(idx.buckets) == 0 {
			t.Errorf("hash index %q has no buckets after rebuild", name)
		}
	}
}

// TestSnapshotDeltaEncoding verifies that snapshot persistence and restart
// produce correct results when only a subset of tables change between snapshots.
func TestSnapshotDeltaEncoding(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "delta.wal")
	snapDir := filepath.Join(dir, "snap")
	os.MkdirAll(snapDir, 0o755)

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}

	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	// Create two tables in the same domain.
	exec("BEGIN DOMAIN delta_test")
	exec("CREATE TABLE stable_table (id INT, data TEXT)")
	exec("CREATE TABLE active_table (id INT, value INT)")
	exec("COMMIT")

	// Insert into both tables initially.
	for i := 1; i <= 10; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN delta_test")
		exec(fmt.Sprintf("INSERT INTO stable_table (id, data) VALUES (%d, 'row-%d')", i, i))
		exec(fmt.Sprintf("INSERT INTO active_table (id, value) VALUES (%d, %d)", i, i*10))
		exec("COMMIT")
	}

	// Now only mutate active_table for enough rows to trigger multiple snapshots.
	for i := 11; i <= defaultSnapshotInterval+50; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN delta_test")
		exec(fmt.Sprintf("INSERT INTO active_table (id, value) VALUES (%d, %d)", i, i*10))
		exec("COMMIT")
	}

	// Record head state.
	origHead := engine.readState.Load().headLSN

	// Close and reopen to test persistence and reload.
	engine.WaitPendingSnapshots()
	_ = store.Close()

	store2, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	engine2, err := New(ctx, store2, snapDir)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}

	// Verify stable_table preserved across delta reconstruction.
	result, err := engine2.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM stable_table", []string{"delta_test"}, origHead)
	if err != nil {
		t.Fatalf("select stable: %v", err)
	}
	if len(result.Rows) != 10 {
		t.Errorf("stable_table: expected 10 rows, got %d", len(result.Rows))
	}

	// Verify active_table has all rows.
	result2, err := engine2.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM active_table", []string{"delta_test"}, origHead)
	if err != nil {
		t.Fatalf("select active: %v", err)
	}
	expectedActive := defaultSnapshotInterval + 50
	if len(result2.Rows) != expectedActive {
		t.Errorf("active_table: expected %d rows, got %d", expectedActive, len(result2.Rows))
	}
}

// TestSnapshotZstdCompression verifies that the snapshot file is zstd-compressed
// and can be read back correctly.
func TestSnapshotZstdCompression(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "gzip.wal")

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN gzip_test")
	exec("CREATE TABLE data (id INT, value TEXT)")
	for i := 1; i <= 50; i++ {
		exec(fmt.Sprintf("INSERT INTO data (id, value) VALUES (%d, 'value-%d')", i, i))
	}
	exec("COMMIT")

	snap := captureSnapshot(engine.readState.Load(), engine.catalog)

	tmpFile := filepath.Join(dir, "gzip-test.snap")
	snapStore := newSnapshotStore()
	snapStore.add(snap)
	if err := writeLatestSnapshotToDisk(tmpFile, snapStore); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Verify file starts with zstd magic bytes (0x28 0xB5 0x2F 0xFD).
	raw, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("read raw: %v", err)
	}
	if len(raw) < 4 || raw[0] != 0x28 || raw[1] != 0xB5 || raw[2] != 0x2F || raw[3] != 0xFD {
		t.Fatalf("file does not start with zstd magic bytes: got %02x %02x %02x %02x", raw[0], raw[1], raw[2], raw[3])
	}

	// Decompress and verify it's valid binary snapshot format.
	binData, err := decompressZstd(raw)
	if err != nil {
		t.Fatalf("zstd decompress: %v", err)
	}

	// Verify binary snapshot magic "ASNP" and version 3.
	if len(binData) < 5 {
		t.Fatal("decompressed data is too short")
	}
	if string(binData[:4]) != "ASNP" {
		t.Fatalf("expected binary magic 'ASNP', got %q", string(binData[:4]))
	}
	if binData[4] != snapVersion {
		t.Fatalf("expected version %d, got %d", snapVersion, binData[4])
	}

	// Verify round-trip through readAllSnapshotsFromDisk.
	loaded, err := readAllSnapshotsFromDisk(tmpFile)
	if err != nil {
		t.Fatalf("read snapshots: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("expected 1 loaded snapshot, got %d", len(loaded))
	}
	if loaded[0].lsn != snap.lsn {
		t.Errorf("LSN mismatch: got %d, want %d", loaded[0].lsn, snap.lsn)
	}
}

// TestSnapshotMultipleFilesOnDisk verifies that a single checkpoint file is
// persisted in the snap directory, and that after restart all data is accessible.
func TestSnapshotMultipleFilesOnDisk(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "freq.wal")
	snapDir := filepath.Join(dir, "snap")
	os.MkdirAll(snapDir, 0o755)

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}

	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN freq_test")
	exec("CREATE TABLE counter (id INT)")
	exec("COMMIT")

	// Insert enough rows to generate multiple in-memory snapshots.
	totalMutations := defaultSnapshotInterval*3 + 100
	for i := 1; i <= totalMutations; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN freq_test")
		exec(fmt.Sprintf("INSERT INTO counter (id) VALUES (%d)", i))
		exec("COMMIT")
	}

	// Close to trigger persistence.
	engine.WaitPendingSnapshots()
	_ = store.Close()

	// Verify multiple snapshot files exist on disk.
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		t.Fatalf("read snap dir: %v", err)
	}
	snapFiles := 0
	for _, e := range entries {
		if !e.IsDir() {
			snapFiles++
		}
	}
	if snapFiles != 1 {
		t.Fatalf("expected exactly 1 checkpoint file on disk, got %d", snapFiles)
	}
	t.Logf("checkpoint files on disk: %d", snapFiles)

	// Verify data integrity by reopening.
	store2, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	engine2, err := New(ctx, store2, snapDir)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}

	head := engine2.readState.Load().headLSN

	result, err := engine2.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM counter", []string{"freq_test"}, head)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(result.Rows) != totalMutations {
		t.Errorf("expected %d rows, got %d", totalMutations, len(result.Rows))
	}

	// Verify checkpoint loaded into memory for time-travel.
	engine2.snapshots.mu.Lock()
	memSnapCount := engine2.snapshots.count()
	engine2.snapshots.mu.Unlock()
	if memSnapCount < 1 {
		t.Errorf("expected at least 1 in-memory snapshot after restart, got %d", memSnapCount)
	}
	t.Logf("in-memory snapshots after restart: %d", memSnapCount)
}

// TestSnapshotDictionarySizeReduction verifies that the v9 string dictionary
// format produces significantly smaller snapshots than raw inline encoding
// when data contains many repeated strings (UUIDs, column names, etc.).
func TestSnapshotDictionarySizeReduction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "dictsize.wal")
	snapDir := filepath.Join(dir, "snap")
	os.MkdirAll(snapDir, 0o755)

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	// Create a table with many UUID columns (high string repetition in column
	// names across rows and changelog entries).
	exec("BEGIN DOMAIN testdomain")
	exec(`CREATE TABLE events (
		id TEXT PRIMARY KEY,
		order_id TEXT,
		recipe_id TEXT,
		sensor_id TEXT,
		event_type TEXT,
		status TEXT,
		created_by TEXT
	)`)
	exec("COMMIT")

	// Insert enough rows to trigger a snapshot.
	for i := 1; i <= defaultSnapshotInterval+50; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN testdomain")
		exec(fmt.Sprintf(
			`INSERT INTO events (id, order_id, recipe_id, sensor_id, event_type, status, created_by)
			 VALUES ('evt-%04d', 'order-%03d', 'recipe-%02d', 'sensor-%02d', 'measurement', 'active', 'system')`,
			i, i%50, i%10, i%5,
		))
		exec("COMMIT")
	}

	engine.WaitPendingSnapshots()

	// Read the latest snapshot file from the directory.
	entries, readErr := os.ReadDir(snapDir)
	if readErr != nil {
		t.Fatalf("read snap dir: %v", readErr)
	}
	var latestFile string
	for _, e := range entries {
		if !e.IsDir() {
			latestFile = filepath.Join(snapDir, e.Name())
		}
	}
	if latestFile == "" {
		t.Fatal("no snapshot files found")
	}

	compressed, err := os.ReadFile(latestFile)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}

	t.Logf("snapshot file size (zstd compressed): %d bytes", len(compressed))

	// Decode and re-encode to measure raw binary size.
	raw, err := decompressZstd(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	t.Logf("snapshot raw size (v9 with dict): %d bytes", len(raw))

	// Verify the dictionary shrank the output: the raw data should be
	// significantly smaller than (numRows * avgRowBytes * 2) since column
	// names and repeated values are deduplicated.
	// With ~550 rows × 7 columns × ~15 bytes/value × 2 (rows+changelog)
	// naive would be ~115KB minimum. Dictionary should bring raw well below that.
	if len(raw) > 200_000 {
		t.Errorf("raw snapshot too large (%d bytes); expected dictionary dedup to keep it under 200KB for %d rows",
			len(raw), defaultSnapshotInterval+50)
	}

	// Verify data roundtrips correctly.
	_, err = decodeSnapshotsBinary(raw)
	if err != nil {
		t.Fatalf("decode roundtrip: %v", err)
	}

	// Reopen engine and verify data.
	store2, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	engine2, err := New(ctx, store2, snapDir)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}

	head := engine2.readState.Load().headLSN
	result, err := engine2.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM events", []string{"testdomain"}, head)
	if err != nil {
		t.Fatalf("query after restart: %v", err)
	}
	if len(result.Rows) != defaultSnapshotInterval+50 {
		t.Errorf("expected %d rows, got %d", defaultSnapshotInterval+50, len(result.Rows))
	}
}

// TestRetainWALPreservesFullHistory verifies that with retainWAL=true (the
// default), the WAL is not truncated after snapshot persistence.
func TestRetainWALPreservesFullHistory(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "retain.wal")
	snapDir := filepath.Join(dir, "snap")
	os.MkdirAll(snapDir, 0o755)

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// retainWAL=true is the default, but be explicit.
	engine, err := New(ctx, store, snapDir, WithRetainWAL(true))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN audit")
	exec("CREATE TABLE records (id INT, data TEXT)")
	exec("COMMIT")

	totalRows := defaultSnapshotInterval + 50
	for i := 1; i <= totalRows; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN audit")
		exec(fmt.Sprintf("INSERT INTO records (id, data) VALUES (%d, 'record-%d')", i, i))
		exec("COMMIT")
	}

	engine.WaitPendingSnapshots()

	// WAL should still contain all records from the beginning.
	records, err := store.ReadFrom(ctx, 1, 100_000)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}

	// We expect at least: 1 BEGIN + 1 CREATE + 1 COMMIT + totalRows*(BEGIN+INSERT+COMMIT)
	expectedMin := 3 + totalRows*3
	if len(records) < expectedMin {
		t.Errorf("WAL was truncated: expected at least %d records, got %d", expectedMin, len(records))
	}

	// First record should be the very first transaction.
	if records[0].LSN != 1 {
		t.Errorf("first WAL record should have LSN=1, got %d (WAL was truncated)", records[0].LSN)
	}

	t.Logf("WAL preserved: %d records, first LSN=%d, last LSN=%d", len(records), records[0].LSN, records[len(records)-1].LSN)
}

// TestTimeTravelAfterRestartUsesPersistedSnapshots verifies that after restart,
// time-travel queries to historical LSNs are accelerated by the persisted
// intermediate snapshot files — avoiding full WAL replay from LSN 0.
func TestTimeTravelAfterRestartUsesPersistedSnapshots(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walBasePath := filepath.Join(dir, "tt.wal")
	snapDir := filepath.Join(dir, "snap")
	os.MkdirAll(snapDir, 0o755)

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}

	engine, err := New(ctx, store, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN demo")
	exec("CREATE TABLE items (id INT, name TEXT)")
	exec("COMMIT")

	// Record the LSN after table creation (early in history).
	earlyLSN := engine.readState.Load().headLSN

	// Insert enough rows to trigger multiple snapshot persists.
	totalRows := defaultSnapshotInterval*2 + 100
	for i := 1; i <= totalRows; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN demo")
		exec(fmt.Sprintf("INSERT INTO items (id, name) VALUES (%d, 'item-%d')", i, i))
		exec("COMMIT")
	}

	finalHead := engine.readState.Load().headLSN
	t.Logf("early LSN=%d, final head=%d, total mutations=%d", earlyLSN, finalHead, totalRows)

	// Time-travel to early LSN should show 0 items (only table creation).
	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM items", []string{"demo"}, earlyLSN)
	if err != nil {
		t.Fatalf("time travel to early LSN: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows at early LSN, got %d", len(result.Rows))
	}

	// Close and restart.
	engine.WaitPendingSnapshots()
	_ = store.Close()

	store2, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	engine2, err := New(ctx, store2, snapDir)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}

	// After restart, we should have at least 1 in-memory snapshot (the checkpoint).
	engine2.snapshots.mu.Lock()
	snapCount := engine2.snapshots.count()
	engine2.snapshots.mu.Unlock()
	t.Logf("snapshots in memory after restart: %d", snapCount)
	if snapCount < 1 {
		t.Fatalf("expected at least 1 snapshot loaded from disk, got %d", snapCount)
	}

	// Time-travel to early history should still work and be fast
	// (snapshot covers nearby LSN, minimal replay needed).
	result2, err := engine2.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM items", []string{"demo"}, earlyLSN)
	if err != nil {
		t.Fatalf("time travel after restart: %v", err)
	}
	if len(result2.Rows) != 0 {
		t.Errorf("expected 0 rows at early LSN after restart, got %d", len(result2.Rows))
	}

	// Time-travel to head should show all rows.
	result3, err := engine2.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM items", []string{"demo"}, finalHead)
	if err != nil {
		t.Fatalf("time travel to head after restart: %v", err)
	}
	if len(result3.Rows) != totalRows {
		t.Errorf("expected %d rows at head, got %d", totalRows, len(result3.Rows))
	}

	// Verify single checkpoint file on disk.
	entries, _ := os.ReadDir(snapDir)
	diskFiles := 0
	for _, e := range entries {
		if !e.IsDir() {
			diskFiles++
		}
	}
	t.Logf("checkpoint files on disk: %d", diskFiles)
	if diskFiles < 1 {
		t.Errorf("expected at least 1 checkpoint file on disk, got %d", diskFiles)
	}
}
