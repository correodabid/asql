package executor

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
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

// writeSnapshot persists the replica's current state as a disk checkpoint.
func (n *replicaNode) writeSnapshot(t *testing.T) {
	t.Helper()
	state := n.engine.readState.Load()
	n.engine.writeMu.Lock()
	catalog := cloneCatalog(n.engine.catalog)
	n.engine.snapSeq++
	seq := n.engine.snapSeq
	n.engine.writeMu.Unlock()
	if err := writeSnapshotToDir(n.snapDir, seq, captureSnapshotWithCatalog(state, catalog), true, 0); err != nil {
		t.Fatalf("write snapshot: %v", err)
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

func userIDs(t *testing.T, engine *Engine) []string {
	t.Helper()
	result, err := engine.Query(context.Background(), "SELECT id FROM users ORDER BY id ASC", []string{"accounts"})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		ids = append(ids, strconv.FormatInt(row["id"].NumberValue, 10))
	}
	return ids
}

// divergedHistories returns a shared prefix, the suffix an old leader
// replicated, and the suffix the new leader replaced it with.
func divergedHistories(t *testing.T) (prefix, stale, replacement []ports.WALRecord) {
	t.Helper()
	oldLeader, oldStore := sourceEngine(t)
	execAll(t, oldLeader, "BEGIN DOMAIN accounts", "CREATE TABLE users (id INT, email TEXT)", "COMMIT")
	execAll(t, oldLeader, insertTx(1)...)
	prefix = walRecords(t, oldStore, 1)
	execAll(t, oldLeader, insertTx(2)...)
	execAll(t, oldLeader, insertTx(3)...)
	stale = walRecords(t, oldStore, uint64(len(prefix))+1)

	newLeader, newStore := sourceEngine(t, prefix...)
	for id := 10; id < 14; id++ {
		execAll(t, newLeader, insertTx(id)...)
	}
	replacement = walRecords(t, newStore, uint64(len(prefix))+1)
	return prefix, stale, replacement
}

// rollBack replays what Raft does when a new leader replaces entries the
// replica had counted as committed: truncate the WAL, then roll the engine
// back.
func (n *replicaNode) rollBack(t *testing.T, afterLSN uint64) {
	t.Helper()
	ctx := context.Background()
	if err := n.store.TruncateAfter(ctx, afterLSN); err != nil {
		t.Fatal(err)
	}
	if err := n.engine.RollbackTo(ctx, afterLSN); err != nil {
		t.Fatal(err)
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

func TestRollbackToTrimsTimestampIndex(t *testing.T) {
	prefix, stale, replacement := divergedHistories(t)
	dir := t.TempDir()
	replica := openReplicaNode(t, dir)
	replica.replicate(t, prefix)
	// With a checkpoint below the cut, rollback replays only the WAL delta
	// and keeps the persisted index instead of rebuilding it.
	replica.writeSnapshot(t)
	replica.replicate(t, stale)

	cut := prefix[len(prefix)-1].LSN
	replica.rollBack(t, cut)
	if got, _ := replica.engine.LSNForTimestamp(context.Background(), math.MaxUint64); got != cut {
		t.Fatalf("after rollback the newest timestamp resolves to LSN %d, want %d", got, cut)
	}

	// The replacement entries reuse the discarded LSNs; they must be indexed
	// with their own timestamps.
	for i := range replacement {
		replacement[i].Timestamp += 1000
	}
	replica.replicate(t, replacement)
	for _, r := range []ports.WALRecord{replacement[0], replacement[len(replacement)-1]} {
		if got, _ := replica.engine.LSNForTimestamp(context.Background(), r.Timestamp); got != r.LSN {
			t.Fatalf("replacement ts=%d resolves to LSN %d, want %d", r.Timestamp, got, r.LSN)
		}
	}
	replica.close(t)

	replica = openReplicaNode(t, dir)
	defer replica.close(t)
	last := replacement[len(replacement)-1]
	if got, _ := replica.engine.LSNForTimestamp(context.Background(), last.Timestamp); got != last.LSN {
		t.Fatalf("after restart replacement ts=%d resolves to LSN %d, want %d", last.Timestamp, got, last.LSN)
	}
	if got, _ := replica.engine.LSNForTimestamp(context.Background(), stale[len(stale)-1].Timestamp); got > cut {
		t.Fatalf("after restart a discarded timestamp resolves to LSN %d, beyond the cut %d", got, cut)
	}
}

func TestRollbackToDiscardsSnapshotsBeyondTheCut(t *testing.T) {
	prefix, stale, replacement := divergedHistories(t)
	if replacement[len(replacement)-1].LSN <= stale[len(stale)-1].LSN {
		t.Fatal("test needs a replacement history longer than the discarded one")
	}
	dir := t.TempDir()
	replica := openReplicaNode(t, dir)
	replica.replicate(t, prefix)
	replica.writeSnapshot(t)
	replica.replicate(t, stale)
	// A checkpoint of state that is about to be discarded.
	replica.writeSnapshot(t)

	replica.rollBack(t, prefix[len(prefix)-1].LSN)
	replica.replicate(t, replacement)
	want := []string{"1", "10", "11", "12", "13"}
	if got := userIDs(t, replica.engine); !equalStrings(got, want) {
		t.Fatalf("rows after rollback = %v, want %v", got, want)
	}

	// Crash without a shutdown checkpoint, then restart.
	if err := replica.store.Close(); err != nil {
		t.Fatal(err)
	}
	replica = openReplicaNode(t, dir)
	defer replica.close(t)
	if got := userIDs(t, replica.engine); !equalStrings(got, want) {
		t.Fatalf("rows after restart = %v, want %v", got, want)
	}
	anchors, err := SnapshotLedgerAnchors(replica.snapDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range anchors {
		if a.LSN > prefix[len(prefix)-1].LSN && a.LSN <= stale[len(stale)-1].LSN {
			t.Fatalf("snapshot of discarded state still on disk: %+v", a)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
