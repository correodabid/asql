package raft

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"asql/internal/engine/ports"
	"asql/internal/platform/clock"
)

type heartbeatTestWALStore struct {
	records []ports.WALRecord
	lastLSN uint64
}

func (m *heartbeatTestWALStore) Append(_ context.Context, r ports.WALRecord) (uint64, error) {
	m.lastLSN++
	r.LSN = m.lastLSN
	m.records = append(m.records, r)
	return m.lastLSN, nil
}

func (m *heartbeatTestWALStore) AppendReplicated(_ context.Context, r ports.WALRecord) error {
	if r.LSN == 0 || r.LSN != m.lastLSN+1 {
		return fmt.Errorf("out of order lsn: got=%d expected=%d", r.LSN, m.lastLSN+1)
	}
	m.records = append(m.records, r)
	m.lastLSN = r.LSN
	return nil
}

func (m *heartbeatTestWALStore) TruncateBefore(_ context.Context, beforeLSN uint64) error {
	kept := m.records[:0]
	for _, r := range m.records {
		if r.LSN >= beforeLSN {
			kept = append(kept, r)
		}
	}
	m.records = kept
	if len(kept) == 0 {
		m.lastLSN = beforeLSN - 1
	}
	return nil
}

func (m *heartbeatTestWALStore) ReadFrom(_ context.Context, fromLSN uint64, limit int) ([]ports.WALRecord, error) {
	out := make([]ports.WALRecord, 0)
	for _, r := range m.records {
		if r.LSN < fromLSN {
			continue
		}
		out = append(out, r)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (m *heartbeatTestWALStore) LastLSN() uint64 {
	return m.lastLSN
}

type captureTransport struct {
	appendCh chan AppendEntriesRequest
}

type delayedPeerTransport struct {
	responses map[string]func(AppendEntriesRequest) AppendEntriesResponse
}

func (t *captureTransport) RequestVote(_ context.Context, _ string, _ RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, nil
}

func (t *captureTransport) AppendEntries(_ context.Context, _ string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.appendCh <- req
	lastIndex := req.PrevLogIndex
	if len(req.Entries) > 0 {
		lastIndex = req.Entries[len(req.Entries)-1].Index
	}
	return AppendEntriesResponse{Term: req.Term, Success: true, LastIndex: lastIndex}, nil
}

func (t *delayedPeerTransport) RequestVote(_ context.Context, _ string, _ RequestVoteRequest) (RequestVoteResponse, error) {
	return RequestVoteResponse{}, nil
}

func (t *delayedPeerTransport) AppendEntries(_ context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
		if responder, ok := t.responses[addr]; ok {
			return responder(req), nil
		}
		lastIndex := req.PrevLogIndex
		if len(req.Entries) > 0 {
			lastIndex = req.Entries[len(req.Entries)-1].Index
		}
		return AppendEntriesResponse{Term: req.Term, Success: true, LastIndex: lastIndex}, nil
}

func newHeartbeatLeaderNode(t *testing.T, transport Transport) *RaftNode {
	t.Helper()
	store := &heartbeatTestWALStore{}
	for i := 0; i < 10; i++ {
		_, err := store.Append(context.Background(), ports.WALRecord{Term: 1, Type: "MUTATION", TxID: fmt.Sprintf("t%d", i+1)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	log, err := NewWALLog(context.Background(), store)
	if err != nil {
		t.Fatalf("NewWALLog: %v", err)
	}
	n, err := NewRaftNode(context.Background(), Config{
		NodeID:             "n1",
		Peers:              []Peer{{NodeID: "n2", RaftAddr: "n2"}},
		Storage:            NewMemStorage(),
		Log:                log,
		Transport:          transport,
		Clock:              clock.Realtime{},
		ElectionMinTimeout: 10 * time.Second,
		ElectionMaxTimeout: 20 * time.Second,
		HeartbeatInterval:  10 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(testWriter{t}, nil)),
	})
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	n.mu.Lock()
	n.state = stateLeader
	n.currentTerm = 1
	n.leaderID = "n1"
	n.commitIndex = 10
	n.nextIndex = map[string]uint64{"n2": 1}
	n.matchIndex = map[string]uint64{"n1": 10, "n2": 0}
	n.mu.Unlock()
	return n
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

func TestSendHeartbeatsPiggybacksWhenIdle(t *testing.T) {
	transport := &captureTransport{appendCh: make(chan AppendEntriesRequest, 1)}
	n := newHeartbeatLeaderNode(t, transport)

	n.sendHeartbeats(context.Background())

	select {
	case req := <-transport.appendCh:
		if len(req.Entries) == 0 {
			t.Fatalf("expected heartbeat piggyback entries when replication is idle")
		}
		if len(req.Entries) != 10 {
			t.Fatalf("entries=%d want 10", len(req.Entries))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for AppendEntries")
	}
}

func TestSendHeartbeatsSkipsDuringActiveReplication(t *testing.T) {
	transport := &captureTransport{appendCh: make(chan AppendEntriesRequest, 1)}
	n := newHeartbeatLeaderNode(t, transport)

	n.mu.Lock()
	n.replicationBusy = true
	n.mu.Unlock()

	n.sendHeartbeats(context.Background())

	select {
	case req := <-transport.appendCh:
		t.Fatalf("unexpected AppendEntries during active replication: %+v", req)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestBroadcastAndCommitCapsEntriesPerRPC(t *testing.T) {
	transport := &captureTransport{appendCh: make(chan AppendEntriesRequest, 8)}
	n := newHeartbeatLeaderNode(t, transport)

	store := &heartbeatTestWALStore{}
	for i := 0; i < 300; i++ {
		_, err := store.Append(context.Background(), ports.WALRecord{Term: 1, Type: "MUTATION", TxID: fmt.Sprintf("bulk-%d", i+1)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	log, err := NewWALLog(context.Background(), store)
	if err != nil {
		t.Fatalf("NewWALLog: %v", err)
	}

	n.mu.Lock()
	n.cfg.Log = log
	n.commitIndex = 0
	n.nextIndex["n2"] = 1
	n.matchIndex["n1"] = 300
	n.matchIndex["n2"] = 0
	n.mu.Unlock()

	if err := n.broadcastAndCommit(context.Background(), 300); err != nil {
		t.Fatalf("broadcastAndCommit: %v", err)
	}

	select {
	case req := <-transport.appendCh:
		if len(req.Entries) != 128 {
			t.Fatalf("entries=%d want 128", len(req.Entries))
		}
		if req.Entries[0].Index != 1 {
			t.Fatalf("first index=%d want 1", req.Entries[0].Index)
		}
		if req.Entries[len(req.Entries)-1].Index != 128 {
			t.Fatalf("last index=%d want 128", req.Entries[len(req.Entries)-1].Index)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for AppendEntries")
	}
}

func TestBroadcastAndCommitRetriesUntilFollowerReachesTarget(t *testing.T) {
	transport := &captureTransport{appendCh: make(chan AppendEntriesRequest, 8)}
	n := newHeartbeatLeaderNode(t, transport)

	store := &heartbeatTestWALStore{}
	for i := 0; i < 300; i++ {
		_, err := store.Append(context.Background(), ports.WALRecord{Term: 1, Type: "MUTATION", TxID: fmt.Sprintf("bulk-%d", i+1)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	log, err := NewWALLog(context.Background(), store)
	if err != nil {
		t.Fatalf("NewWALLog: %v", err)
	}

	n.mu.Lock()
	n.cfg.Log = log
	n.commitIndex = 0
	n.nextIndex["n2"] = 1
	n.matchIndex["n1"] = 300
	n.matchIndex["n2"] = 0
	n.mu.Unlock()

	if err := n.broadcastAndCommit(context.Background(), 300); err != nil {
		t.Fatalf("broadcastAndCommit: %v", err)
	}

	requests := make([]AppendEntriesRequest, 0, 3)
	deadline := time.After(time.Second)
	for len(requests) < 3 {
		select {
		case req := <-transport.appendCh:
			requests = append(requests, req)
		case <-deadline:
			t.Fatalf("timed out waiting for AppendEntries, got %d requests", len(requests))
		}
	}

	if len(requests[0].Entries) != 128 || len(requests[1].Entries) != 128 || len(requests[2].Entries) != 44 {
		t.Fatalf("unexpected request sizes: %d, %d, %d", len(requests[0].Entries), len(requests[1].Entries), len(requests[2].Entries))
	}
	if requests[0].Entries[0].Index != 1 || requests[0].Entries[len(requests[0].Entries)-1].Index != 128 {
		t.Fatalf("unexpected first request range: %d..%d", requests[0].Entries[0].Index, requests[0].Entries[len(requests[0].Entries)-1].Index)
	}
	if requests[1].Entries[0].Index != 129 || requests[1].Entries[len(requests[1].Entries)-1].Index != 256 {
		t.Fatalf("unexpected second request range: %d..%d", requests[1].Entries[0].Index, requests[1].Entries[len(requests[1].Entries)-1].Index)
	}
	if requests[2].Entries[0].Index != 257 || requests[2].Entries[len(requests[2].Entries)-1].Index != 300 {
		t.Fatalf("unexpected third request range: %d..%d", requests[2].Entries[0].Index, requests[2].Entries[len(requests[2].Entries)-1].Index)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.matchIndex["n2"] != 300 {
		t.Fatalf("matchIndex=%d want 300", n.matchIndex["n2"])
	}
	if n.commitIndex != 300 {
		t.Fatalf("commitIndex=%d want 300", n.commitIndex)
	}
}

func TestBroadcastAndCommitDrainsLatePeerSuccess(t *testing.T) {
	transport := &delayedPeerTransport{responses: map[string]func(AppendEntriesRequest) AppendEntriesResponse{
		"n2": func(req AppendEntriesRequest) AppendEntriesResponse {
			lastIndex := req.PrevLogIndex
			if len(req.Entries) > 0 {
				lastIndex = req.Entries[len(req.Entries)-1].Index
			}
			return AppendEntriesResponse{Term: req.Term, Success: true, LastIndex: lastIndex}
		},
		"n3": func(req AppendEntriesRequest) AppendEntriesResponse {
			time.Sleep(25 * time.Millisecond)
			lastIndex := req.PrevLogIndex
			if len(req.Entries) > 0 {
				lastIndex = req.Entries[len(req.Entries)-1].Index
			}
			return AppendEntriesResponse{Term: req.Term, Success: true, LastIndex: lastIndex}
		},
	}}

	store := &heartbeatTestWALStore{}
	for i := 0; i < 20; i++ {
		_, err := store.Append(context.Background(), ports.WALRecord{Term: 1, Type: "MUTATION", TxID: fmt.Sprintf("t%d", i+1)})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	log, err := NewWALLog(context.Background(), store)
	if err != nil {
		t.Fatalf("NewWALLog: %v", err)
	}

	n, err := NewRaftNode(context.Background(), Config{
		NodeID:             "n1",
		Peers:              []Peer{{NodeID: "n2", RaftAddr: "n2"}, {NodeID: "n3", RaftAddr: "n3"}},
		Storage:            NewMemStorage(),
		Log:                log,
		Transport:          transport,
		Clock:              clock.Realtime{},
		ElectionMinTimeout: 10 * time.Second,
		ElectionMaxTimeout: 20 * time.Second,
		HeartbeatInterval:  10 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(testWriter{t}, nil)),
	})
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}

	n.mu.Lock()
	n.state = stateLeader
	n.currentTerm = 1
	n.leaderID = "n1"
	n.commitIndex = 0
	n.nextIndex = map[string]uint64{"n2": 1, "n3": 1}
	n.matchIndex = map[string]uint64{"n1": 20, "n2": 0, "n3": 0}
	n.mu.Unlock()

	if err := n.broadcastAndCommit(context.Background(), 20); err != nil {
		t.Fatalf("broadcastAndCommit: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		n.mu.Lock()
		next := n.nextIndex["n3"]
		match := n.matchIndex["n3"]
		n.mu.Unlock()
		if next == 21 && match == 20 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("late peer progress not applied: nextIndex=%d matchIndex=%d", n.nextIndex["n3"], n.matchIndex["n3"])
}