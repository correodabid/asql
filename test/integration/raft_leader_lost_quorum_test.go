package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/cluster/raft"
	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/platform/clock"
	"github.com/correodabid/asql/internal/storage/wal"
)

// switchablePeers simulates followers that grant votes and accept every
// entry while up, and are unreachable while down.
type switchablePeers struct {
	down atomic.Bool
	// hold, when non-nil, blocks every AppendEntries until it is closed.
	hold atomic.Pointer[chan struct{}]
}

var errPeerDown = errors.New("peer unreachable")

func (p *switchablePeers) RequestVote(_ context.Context, _ string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	if p.down.Load() {
		return raft.RequestVoteResponse{}, errPeerDown
	}
	return raft.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (p *switchablePeers) AppendEntries(ctx context.Context, _ string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if h := p.hold.Load(); h != nil {
		select {
		case <-*h:
		case <-ctx.Done():
			return raft.AppendEntriesResponse{}, ctx.Err()
		}
	}
	if p.down.Load() {
		return raft.AppendEntriesResponse{}, errPeerDown
	}
	last := req.PrevLogIndex
	if n := len(req.Entries); n > 0 {
		last = req.Entries[n-1].Index
	}
	return raft.AppendEntriesResponse{Term: req.Term, Success: true, LastIndex: last}, nil
}

// raftCommitter adapts RaftNode to the engine, as the pgwire server does.
type raftCommitter struct{ node *raft.RaftNode }

func (c raftCommitter) Apply(ctx context.Context, typ, txID string, payload []byte) (uint64, error) {
	e, err := c.node.Apply(ctx, typ, txID, payload)
	return e.Index, err
}

func (c raftCommitter) WaitLeaderReady(ctx context.Context) error {
	return c.node.WaitLeaderReady(ctx)
}

func (c raftCommitter) ApplyBatch(ctx context.Context, records []ports.RaftRecord) ([]uint64, error) {
	batch := make([]raft.BatchRecord, len(records))
	for i, r := range records {
		batch[i] = raft.BatchRecord{Type: r.Type, TxID: r.TxID, Payload: r.Payload}
	}
	entries, err := c.node.ApplyBatch(ctx, batch)
	if err != nil {
		return nil, err
	}
	lsns := make([]uint64, len(entries))
	for i, e := range entries {
		lsns[i] = e.Index
	}
	return lsns, nil
}

func execTx(eng *executor.Engine, sql string) error {
	ctx := context.Background()
	s := eng.NewSession()
	for _, stmt := range []string{"BEGIN DOMAIN lab", sql, "COMMIT"} {
		if _, err := eng.Execute(ctx, s, stmt); err != nil {
			return err
		}
	}
	return nil
}

// A write that arrives while a newly elected leader is still committing its
// no-op must wait for it without holding the engine's write lock, which the
// leader's catch-up needs. It used to deadlock every write on the node.
func TestWriteDuringLeadershipEstablishmentDoesNotDeadlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "leader.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	eng, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatal(err)
	}
	rlog, err := raft.NewWALLog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	peers := &switchablePeers{}
	hold := make(chan struct{})
	peers.hold.Store(&hold) // the no-op cannot reach a quorum yet
	node, err := raft.NewRaftNode(ctx, raft.Config{
		NodeID:             "n1",
		Peers:              []raft.Peer{{NodeID: "n2", RaftAddr: "n2"}, {NodeID: "n3", RaftAddr: "n3"}},
		Storage:            raft.NewMemStorage(),
		Log:                rlog,
		Transport:          peers,
		Clock:              clock.Realtime{},
		ElectionMinTimeout: 30 * time.Millisecond,
		ElectionMaxTimeout: 60 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnEntriesCommitted: func(ctx context.Context, commitIndex uint64, records []ports.WALRecord) {
			_ = eng.ApplyCommittedRecords(ctx, commitIndex, records)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	eng.SetRaftCommitter(raftCommitter{node: node})
	go func() { _ = node.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("no leadership")
		}
		time.Sleep(5 * time.Millisecond)
	}

	done := make(chan error, 1)
	go func() { done <- execTx(eng, "CREATE TABLE results (id INT, result TEXT)") }()
	time.Sleep(100 * time.Millisecond) // the write is now waiting for readiness
	peers.hold.Store(nil)
	close(hold)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write after leadership was established: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write deadlocked while the leader was establishing leadership")
	}
	if err := execTx(eng, "INSERT INTO results (id, result) VALUES (1, 'a')"); err != nil {
		t.Fatalf("next write: %v", err)
	}
}
