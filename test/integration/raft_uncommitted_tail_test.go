package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/cluster/raft"
	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/platform/clock"
	"github.com/correodabid/asql/internal/storage/wal"
)

type idleRaftTransport struct{}

func (idleRaftTransport) RequestVote(context.Context, string, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{}, nil
}

func (idleRaftTransport) AppendEntries(context.Context, string, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, nil
}

// walRecordsFor runs statements in a scratch engine and returns its WAL.
func walRecordsFor(t *testing.T, txs ...[]string) []ports.WALRecord {
	t.Helper()
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "scratch.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	eng, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, tx := range txs {
		s := eng.NewSession()
		for _, sql := range tx {
			if _, err := eng.Execute(ctx, s, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}
	eng.WaitPendingSnapshots()
	records, err := store.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func idsIn(t *testing.T, eng *executor.Engine) []int64 {
	t.Helper()
	res, err := eng.Query(context.Background(), "SELECT id FROM results ORDER BY id", []string{"lab"})
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, row := range res.Rows {
		ids = append(ids, row["id"].NumberValue)
	}
	return ids
}

// TestRestartWithUncommittedRaftTailConverges covers a node that restarts
// with WAL entries a leader of an old term wrote but never replicated, then
// rejoins a cluster whose new leader committed different entries at the same
// LSNs.
func TestRestartWithUncommittedRaftTailConverges(t *testing.T) {
	// persisted: the node saved its commit index (normal operation).
	// legacy:    no saved commit index (a node from before it was persisted).
	for _, persisted := range []bool{true, false} {
		name := map[bool]string{true: "persisted-commit-index", false: "legacy-no-commit-index"}[persisted]
		t.Run(name, func(t *testing.T) { runUncommittedTailScenario(t, persisted) })
	}
}

func runUncommittedTailScenario(t *testing.T, persisted bool) {
	ctx := context.Background()
	base := [][]string{
		{"BEGIN DOMAIN lab", "CREATE TABLE results (id INT, result TEXT)", "COMMIT"},
		{"BEGIN DOMAIN lab", "INSERT INTO results (id, result) VALUES (1, 'committed')", "COMMIT"},
	}
	committed := walRecordsFor(t, base...)
	stale := walRecordsFor(t, append(base, []string{"BEGIN DOMAIN lab", "INSERT INTO results (id, result) VALUES (99, 'never committed')", "COMMIT"})...)[len(committed):]
	newer := walRecordsFor(t, append(base, []string{"BEGIN DOMAIN lab", "INSERT INTO results (id, result) VALUES (7, 'committed by new leader')", "COMMIT"})...)[len(committed):]
	lastCommitted := uint64(len(committed))

	// The node's WAL: committed history (term 1), then a tail written in term 2
	// by this node while it was leader, never replicated.
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "node.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := range committed {
		committed[i].Term = 1
	}
	for i := range stale {
		stale[i].Term = 2
	}
	if err := store.AppendReplicatedBatch(ctx, append(append([]ports.WALRecord(nil), committed...), stale...)); err != nil {
		t.Fatal(err)
	}
	storage := raft.NewMemStorage()
	var engineOpts []executor.EngineOption
	if persisted {
		if err := storage.(raft.CommitIndexStore).SaveCommitIndex(ctx, lastCommitted); err != nil {
			t.Fatal(err)
		}
		engineOpts = append(engineOpts, executor.WithReplayLimit(lastCommitted))
	}

	// Restart, wired as the pgwire server wires it.
	eng, err := executor.New(ctx, store, "", engineOpts...)
	if err != nil {
		t.Fatal(err)
	}
	afterRestart := fmt.Sprint(idsIn(t, eng))
	t.Logf("after restart, before rejoining: ids=%s", afterRestart)
	if persisted && afterRestart != "[1]" {
		t.Fatalf("restarted node serves uncommitted entries: ids=%s, want [1]", afterRestart)
	}

	rlog, err := raft.NewWALLog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	node, err := raft.NewRaftNode(ctx, raft.Config{
		NodeID:             "n1",
		Peers:              []raft.Peer{{NodeID: "n2", RaftAddr: "n2"}, {NodeID: "n3", RaftAddr: "n3"}},
		Storage:            storage,
		Log:                rlog,
		Transport:          idleRaftTransport{},
		Clock:              clock.Realtime{},
		ElectionMinTimeout: time.Minute,
		ElectionMaxTimeout: 2 * time.Minute,
		HeartbeatInterval:  time.Second,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnEntriesCommitted: func(ctx context.Context, commitIndex uint64, records []ports.WALRecord) {
			if err := eng.ApplyCommittedRecords(ctx, commitIndex, records); err != nil {
				t.Errorf("apply committed: %v", err)
			}
		},
		OnCommitIndexLowered: eng.RollbackTo,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The new leader (term 3) never had the term-2 tail and has committed
	// other entries at those LSNs.
	entries := make([]raft.RaftEntry, len(newer))
	for i, r := range newer {
		entries[i] = raft.RaftEntry{Index: lastCommitted + uint64(i) + 1, Term: 3, TxID: r.TxID, Type: r.Type, Payload: r.Payload}
	}
	resp, err := node.HandleAppendEntries(ctx, raft.AppendEntriesRequest{
		Term:         3,
		LeaderID:     "n2",
		PrevLogIndex: lastCommitted,
		PrevLogTerm:  1,
		Entries:      entries,
		LeaderCommit: lastCommitted + uint64(len(entries)),
	})
	if err != nil || !resp.Success {
		t.Fatalf("append entries: resp=%+v err=%v", resp, err)
	}

	// OnEntriesCommitted runs asynchronously.
	deadline := time.Now().Add(5 * time.Second)
	got := ""
	for time.Now().Before(deadline) {
		if got = fmt.Sprint(idsIn(t, eng)); got == "[1 7]" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("engine after rejoin: ids=%s (cluster state is [1 7])", got)
	if got != "[1 7]" {
		t.Fatalf("follower engine diverged from the committed log: ids=%s, want [1 7]", got)
	}
}
