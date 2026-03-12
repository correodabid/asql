package executor

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"asql/internal/engine/ports"
	"asql/internal/storage/wal"
)

type countingSegmentedStore struct {
	inner     *wal.SegmentedLogStore
	readCount atomic.Int64
}

func (store *countingSegmentedStore) Append(ctx context.Context, record ports.WALRecord) (uint64, error) {
	return store.inner.Append(ctx, record)
}

func (store *countingSegmentedStore) ReadFrom(ctx context.Context, fromLSN uint64, limit int) ([]ports.WALRecord, error) {
	store.readCount.Add(1)
	return store.inner.ReadFrom(ctx, fromLSN, limit)
}

func (store *countingSegmentedStore) AppendBatchNoSync(ctx context.Context, records []ports.WALRecord) ([]uint64, error) {
	return store.inner.AppendBatchNoSync(ctx, records)
}

func (store *countingSegmentedStore) Sync() error {
	return store.inner.Sync()
}

func (store *countingSegmentedStore) Close() error {
	return store.inner.Close()
}

func TestTimestampIndexPersistsAcrossRestartAndSnapshotReplay(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "engine.wal")
	snapDir := filepath.Join(baseDir, "snaps")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snap dir: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	countingStore := &countingSegmentedStore{inner: store}
	engine, err := New(ctx, countingStore, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
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
	snap := captureSnapshotWithCatalog(state, catalog)
	if err := writeSnapshotToDir(snapDir, 1, snap, true, 0); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	session = engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	if err := countingStore.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopenedStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen log store: %v", err)
	}
	countingReopened := &countingSegmentedStore{inner: reopenedStore}
	restarted, err := New(ctx, countingReopened, snapDir)
	if err != nil {
		t.Fatalf("restart engine: %v", err)
	}
	countingReopened.readCount.Store(0)

	resolvedLSN, err := restarted.LSNForTimestamp(ctx, 6)
	if err != nil {
		t.Fatalf("resolve lsn for ts=6: %v", err)
	}
	if resolvedLSN != 6 {
		t.Fatalf("unexpected resolved lsn: got %d want 6", resolvedLSN)
	}
	if countingReopened.readCount.Load() != 0 {
		t.Fatalf("expected persisted timestamp index lookup without WAL scan, got %d reads", countingReopened.readCount.Load())
	}

	result, err := restarted.TimeTravelQueryAsOfTimestamp(ctx, "SELECT id, email FROM users ORDER BY id ASC", []string{"accounts"}, 4)
	if err != nil {
		t.Fatalf("time travel by timestamp: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("unexpected historical row count: got %d want 1", len(result.Rows))
	}
	if got := restarted.RowCount("accounts", "users"); got != 2 {
		t.Fatalf("unexpected current row count after restart: got %d want 2", got)
	}

	if err := countingReopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}

func TestTimestampIndexSupportsLargeWALHistoriesAcrossRestart(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "large-history.wal")
	snapDir := filepath.Join(baseDir, "snaps")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snap dir: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	countingStore := &countingSegmentedStore{inner: store}
	engine, err := New(ctx, countingStore, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	for i := 1; i <= 240; i++ {
		session = engine.NewSession()
		for _, sql := range []string{
			"BEGIN DOMAIN accounts",
			"INSERT INTO users (id, email) VALUES (" + itoa(i) + ", 'user" + itoa(i) + "@asql.dev')",
			"COMMIT",
		} {
			if _, err := engine.Execute(ctx, session, sql); err != nil {
				t.Fatalf("execute tx %d %q: %v", i, sql, err)
			}
		}
	}

	records, err := countingStore.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatalf("read wal records: %v", err)
	}
	target := records[len(records)/2]

	if err := countingStore.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopenedStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen log store: %v", err)
	}
	countingReopened := &countingSegmentedStore{inner: reopenedStore}
	restarted, err := New(ctx, countingReopened, snapDir)
	if err != nil {
		t.Fatalf("restart engine: %v", err)
	}
	countingReopened.readCount.Store(0)

	resolvedLSN, err := restarted.LSNForTimestamp(ctx, target.Timestamp)
	if err != nil {
		t.Fatalf("resolve lsn for target ts=%d: %v", target.Timestamp, err)
	}
	if resolvedLSN != target.LSN {
		t.Fatalf("unexpected resolved lsn: got %d want %d", resolvedLSN, target.LSN)
	}
	if countingReopened.readCount.Load() != 0 {
		t.Fatalf("expected large-history lookup to avoid WAL scan, got %d reads", countingReopened.readCount.Load())
	}

	if err := countingReopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}

func TestTimestampIndexCatchesUpFromStalePersistedFileOnRestart(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "stale-index.wal")
	snapDir := filepath.Join(baseDir, "snaps")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snap dir: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	countingStore := &countingSegmentedStore{inner: store}
	engine, err := New(ctx, countingStore, snapDir)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	session = engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	records, err := countingStore.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatalf("read wal records: %v", err)
	}
	staleEntries := timestampEntriesFromRecords(records[:4])
	if err := writeTimestampIndexFile(engine.timestampIndex.filePath, staleEntries); err != nil {
		t.Fatalf("write stale timestamp index: %v", err)
	}
	if err := countingStore.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopenedStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen log store: %v", err)
	}
	countingReopened := &countingSegmentedStore{inner: reopenedStore}
	restarted, err := New(ctx, countingReopened, snapDir)
	if err != nil {
		t.Fatalf("restart engine: %v", err)
	}
	countingReopened.readCount.Store(0)

	if got := restarted.timestampIndex.LastLSN(); got != records[len(records)-1].LSN {
		t.Fatalf("expected timestamp index catch-up to latest lsn %d, got %d", records[len(records)-1].LSN, got)
	}
	resolvedLSN, err := restarted.LSNForTimestamp(ctx, records[len(records)-1].Timestamp)
	if err != nil {
		t.Fatalf("resolve latest timestamp: %v", err)
	}
	if resolvedLSN != records[len(records)-1].LSN {
		t.Fatalf("unexpected resolved latest lsn: got %d want %d", resolvedLSN, records[len(records)-1].LSN)
	}
	if countingReopened.readCount.Load() != 0 {
		t.Fatalf("expected caught-up timestamp lookup without extra WAL scan, got %d reads", countingReopened.readCount.Load())
	}

	if err := countingReopened.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}