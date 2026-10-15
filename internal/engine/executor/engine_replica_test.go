package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/storage/wal"
)

// replicaNode is an engine fed with WAL records produced elsewhere, the way a
// Raft follower is: records are appended to its WAL, then applied once
// committed.
type replicaNode struct {
	dir     string
	store   *wal.SegmentedLogStore
	engine  *Engine
	snapDir string
}

func openReplicaNode(t *testing.T, dir string, opts ...EngineOption) *replicaNode {
	t.Helper()
	snapDir := filepath.Join(dir, "snaps")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := wal.NewSegmentedLogStore(filepath.Join(dir, "engine.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	engine, err := New(context.Background(), store, snapDir, opts...)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	return &replicaNode{dir: dir, store: store, engine: engine, snapDir: snapDir}
}

func (n *replicaNode) close(t *testing.T) {
	t.Helper()
	n.engine.WaitPendingSnapshots()
	if err := n.store.Close(); err != nil {
		t.Fatal(err)
	}
}

// replicate appends records to the WAL and applies them as committed.
func (n *replicaNode) replicate(t *testing.T, records []ports.WALRecord) {
	t.Helper()
	ctx := context.Background()
	if err := n.store.AppendReplicatedBatch(ctx, records); err != nil {
		t.Fatalf("append replicated: %v", err)
	}
	if err := n.engine.ApplyCommittedRecords(ctx, records[len(records)-1].LSN, records); err != nil {
		t.Fatalf("apply committed: %v", err)
	}
}

// sourceEngine produces WAL records by executing SQL, like a leader whose
// log starts with history.
func sourceEngine(t *testing.T, history ...ports.WALRecord) (*Engine, *wal.SegmentedLogStore) {
	t.Helper()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "source.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if len(history) > 0 {
		if err := store.AppendReplicatedBatch(context.Background(), history); err != nil {
			t.Fatal(err)
		}
	}
	engine, err := New(context.Background(), store, "")
	if err != nil {
		t.Fatal(err)
	}
	return engine, store
}

func execAll(t *testing.T, engine *Engine, statements ...string) {
	t.Helper()
	session := engine.NewSession()
	for _, sql := range statements {
		if _, err := engine.Execute(context.Background(), session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
}

func walRecords(t *testing.T, store *wal.SegmentedLogStore, from uint64) []ports.WALRecord {
	t.Helper()
	records, err := store.ReadFrom(context.Background(), from, 0)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func insertTx(id int) []string {
	return []string{
		"BEGIN DOMAIN accounts",
		fmt.Sprintf("INSERT INTO users (id, email) VALUES (%d, 'u%d@asql.dev')", id, id),
		"COMMIT",
	}
}

func TestFollowerApplyMaintainsTimestampIndex(t *testing.T) {
	leader, leaderStore := sourceEngine(t)
	execAll(t, leader, "BEGIN DOMAIN accounts", "CREATE TABLE users (id INT, email TEXT)", "COMMIT")
	execAll(t, leader, insertTx(1)...)
	execAll(t, leader, insertTx(2)...)
	records := walRecords(t, leaderStore, 1)

	dir := t.TempDir()
	replica := openReplicaNode(t, dir)
	replica.replicate(t, records)

	last := records[len(records)-1]
	if got, _ := replica.engine.LSNForTimestamp(context.Background(), last.Timestamp); got != last.LSN {
		t.Fatalf("follower resolved ts=%d to LSN %d, want %d", last.Timestamp, got, last.LSN)
	}
	replica.close(t)

	replica = openReplicaNode(t, dir)
	defer replica.close(t)
	if got, _ := replica.engine.LSNForTimestamp(context.Background(), last.Timestamp); got != last.LSN {
		t.Fatalf("after restart resolved ts=%d to LSN %d, want %d", last.Timestamp, got, last.LSN)
	}
}
