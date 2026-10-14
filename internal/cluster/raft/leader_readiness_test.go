package raft

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/platform/clock"
)

type failingTransport struct{}

func (failingTransport) RequestVote(context.Context, string, RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, errors.New("unreachable")
}

func (failingTransport) AppendEntries(context.Context, string, AppendEntriesRequest) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{}, errors.New("unreachable")
}

func newReadinessLeader(t *testing.T, transport Transport) *RaftNode {
	t.Helper()
	_, log := newWatermarkLog(t, 3, 0)
	n, err := NewRaftNode(context.Background(), Config{
		NodeID:    "n1",
		Peers:     []Peer{{NodeID: "n2", RaftAddr: "n2"}, {NodeID: "n3", RaftAddr: "n3"}},
		Storage:   NewMemStorage(),
		Log:       log,
		Transport: transport,
		Clock:     clock.Realtime{},
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.state = stateLeader
	n.currentTerm = 1
	n.leaderID = "n1"
	n.nextIndex = map[string]uint64{"n2": 4, "n3": 4}
	n.matchIndex = map[string]uint64{"n1": 3, "n2": 0, "n3": 0}
	n.mu.Unlock()
	return n
}

func TestApplyFailsFastUntilLeaderReady(t *testing.T) {
	n := newReadinessLeader(t, failingTransport{})
	readyCh := make(chan struct{})
	n.mu.Lock()
	n.leaderReadyCh = readyCh
	n.mu.Unlock()

	if _, err := n.ApplyBatch(context.Background(), []BatchRecord{{Type: "BEGIN", TxID: "t"}}); !errors.Is(err, ErrLeaderNotReady) {
		t.Fatalf("ApplyBatch before ready: %v, want ErrLeaderNotReady", err)
	}
	if _, err := n.Apply(context.Background(), "BEGIN", "t", nil); !errors.Is(err, ErrLeaderNotReady) {
		t.Fatalf("Apply before ready: %v, want ErrLeaderNotReady", err)
	}

	waited := make(chan error, 1)
	go func() { waited <- n.WaitLeaderReady(context.Background()) }()
	select {
	case err := <-waited:
		t.Fatalf("WaitLeaderReady returned before ready: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(readyCh)
	if err := <-waited; err != nil {
		t.Fatalf("WaitLeaderReady after ready: %v", err)
	}
}

func TestWaitLeaderReadyReportsLostLeadership(t *testing.T) {
	n := newReadinessLeader(t, failingTransport{})
	readyCh := make(chan struct{})
	n.mu.Lock()
	n.leaderReadyCh = readyCh
	n.mu.Unlock()
	waited := make(chan error, 1)
	go func() { waited <- n.WaitLeaderReady(context.Background()) }()
	n.mu.Lock()
	n.becomeFollowerLocked(2)
	n.mu.Unlock()
	close(readyCh) // establishLeadership closes it when leadership is lost
	if err := <-waited; !errors.Is(err, ErrNotLeader) {
		t.Fatalf("WaitLeaderReady after losing leadership: %v, want ErrNotLeader", err)
	}
}
