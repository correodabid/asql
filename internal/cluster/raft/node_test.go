package raft_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"asql/internal/cluster/raft"
	"asql/internal/engine/ports"
	"asql/internal/platform/clock"
)

// ──────────────────────────────────────────────────────────────────────────────
// In-memory WAL store (walAppender) for tests
// ──────────────────────────────────────────────────────────────────────────────

type memWALStore struct {
	mu      sync.Mutex
	records []ports.WALRecord
	lastLSN uint64
}

func (m *memWALStore) Append(_ context.Context, r ports.WALRecord) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastLSN++
	r.LSN = m.lastLSN
	m.records = append(m.records, r)
	return m.lastLSN, nil
}

func (m *memWALStore) AppendReplicated(_ context.Context, r ports.WALRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r.LSN == 0 || r.LSN != m.lastLSN+1 {
		return fmt.Errorf("out of order lsn: got=%d expected=%d", r.LSN, m.lastLSN+1)
	}
	m.records = append(m.records, r)
	m.lastLSN = r.LSN
	return nil
}

func (m *memWALStore) TruncateBefore(_ context.Context, beforeLSN uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
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

func (m *memWALStore) ReadFrom(_ context.Context, fromLSN uint64, limit int) ([]ports.WALRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ports.WALRecord
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

func (m *memWALStore) LastLSN() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastLSN
}

// ──────────────────────────────────────────────────────────────────────────────
// In-memory transport — routes RPCs directly to node.Handle* methods
// ──────────────────────────────────────────────────────────────────────────────

// memTransport routes RPCs by nodeID used as address.
type memTransport struct {
	mu    sync.RWMutex
	nodes map[string]*raft.RaftNode
	// dropped is an optional set of node IDs to which calls fail (simulate partition).
	dropped map[string]bool
	// blockedLinks simulates directional partitions between sender and target.
	blockedLinks map[string]map[string]bool
}

func newMemTransport() *memTransport {
	return &memTransport{
		nodes:        make(map[string]*raft.RaftNode),
		dropped:      make(map[string]bool),
		blockedLinks: make(map[string]map[string]bool),
	}
}

func (t *memTransport) register(nodeID string, n *raft.RaftNode) {
	t.mu.Lock()
	t.nodes[nodeID] = n
	t.mu.Unlock()
}

// drop simulates a network partition: calls to nodeID return an error.
func (t *memTransport) drop(nodeID string) {
	t.mu.Lock()
	t.dropped[nodeID] = true
	t.mu.Unlock()
}

// restore lifts a simulated partition.
func (t *memTransport) restore(nodeID string) {
	t.mu.Lock()
	delete(t.dropped, nodeID)
	t.mu.Unlock()
}

func (t *memTransport) dropLink(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.blockedLinks[from] == nil {
		t.blockedLinks[from] = make(map[string]bool)
	}
	t.blockedLinks[from][to] = true
}

func (t *memTransport) restoreLink(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if links := t.blockedLinks[from]; links != nil {
		delete(links, to)
		if len(links) == 0 {
			delete(t.blockedLinks, from)
		}
	}
}

func (t *memTransport) isBlocked(from, to string) bool {
	if links := t.blockedLinks[from]; links != nil {
		return links[to]
	}
	return false
}

func (t *memTransport) RequestVote(ctx context.Context, addr string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	t.mu.RLock()
	n, ok := t.nodes[addr]
	dropped := t.dropped[addr]
	blocked := t.isBlocked(req.CandidateID, addr)
	t.mu.RUnlock()
	if !ok || dropped || blocked {
		return raft.RequestVoteResponse{}, fmt.Errorf("unreachable: %s", addr)
	}
	return n.HandleRequestVote(ctx, req)
}

func (t *memTransport) AppendEntries(ctx context.Context, addr string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	t.mu.RLock()
	n, ok := t.nodes[addr]
	dropped := t.dropped[addr]
	blocked := t.isBlocked(req.LeaderID, addr)
	t.mu.RUnlock()
	if !ok || dropped || blocked {
		return raft.AppendEntriesResponse{}, fmt.Errorf("unreachable: %s", addr)
	}
	return n.HandleAppendEntries(ctx, req)
}

// ──────────────────────────────────────────────────────────────────────────────
// Test helpers
// ──────────────────────────────────────────────────────────────────────────────

// makeNode builds a RaftNode for nodeID with the given peers, sharing transport and logger.
func makeNode(t *testing.T, nodeID string, peers []raft.Peer, transport raft.Transport) *raft.RaftNode {
	t.Helper()
	store := &memWALStore{}
	return makeNodeWithStore(t, nodeID, peers, transport, store)
}

func makeNodeWithStore(t *testing.T, nodeID string, peers []raft.Peer, transport raft.Transport, store *memWALStore) *raft.RaftNode {
	t.Helper()
	log, err := raft.NewWALLog(context.Background(), store)
	if err != nil {
		t.Fatalf("NewWALLog %s: %v", nodeID, err)
	}
	node, err := raft.NewRaftNode(context.Background(), raft.Config{
		NodeID:             nodeID,
		Peers:              peers,
		Storage:            raft.NewMemStorage(),
		Log:                log,
		Transport:          transport,
		Clock:              clock.Realtime{},
		ElectionMinTimeout: 30 * time.Millisecond,
		ElectionMaxTimeout: 60 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
		Logger:             newTestLogger(t),
	})
	if err != nil {
		t.Fatalf("NewRaftNode %s: %v", nodeID, err)
	}
	return node
}

type testNodeRuntime struct {
	id    string
	node  *raft.RaftNode
	store *memWALStore
}

func makeNodeRuntime(t *testing.T, nodeID string, peers []raft.Peer, transport *memTransport) *testNodeRuntime {
	t.Helper()
	store := &memWALStore{}
	node := makeNodeWithStore(t, nodeID, peers, transport, store)
	transport.register(nodeID, node)
	return &testNodeRuntime{id: nodeID, node: node, store: store}
}

func startRuntime(parent context.Context, runtime *testNodeRuntime) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	go runtime.node.Run(ctx) //nolint:errcheck
	return cancel
}

func waitStoreLSN(t *testing.T, store *memWALStore, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := store.LastLSN(); got >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("store lastLSN=%d want >= %d after %v", store.LastLSN(), want, timeout)
}

func nodePointers(runtimes []*testNodeRuntime) []*raft.RaftNode {
	nodes := make([]*raft.RaftNode, len(runtimes))
	for i, runtime := range runtimes {
		nodes[i] = runtime.node
	}
	return nodes
}

func findRuntime(runtimes []*testNodeRuntime, node *raft.RaftNode) *testNodeRuntime {
	for _, runtime := range runtimes {
		if runtime.node == node {
			return runtime
		}
	}
	return nil
}

func peersForNode(ids []string, self string) []raft.Peer {
	peers := make([]raft.Peer, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, raft.Peer{NodeID: id, RaftAddr: id})
		}
	}
	return peers
}

func applyWrites(t *testing.T, ctx context.Context, leader *raft.RaftNode, prefix string, count int) uint64 {
	t.Helper()
	var last uint64
	for i := 0; i < count; i++ {
		entry, err := leader.Apply(ctx, "COMMIT", fmt.Sprintf("%s-%02d", prefix, i), []byte(`{"op":"write"}`))
		if err != nil {
			t.Fatalf("apply %s-%02d: %v", prefix, i, err)
		}
		last = entry.Index
	}
	return last
}

func partitionNodeLinks(transport *memTransport, nodeID string, peers []string) {
	for _, peer := range peers {
		transport.dropLink(nodeID, peer)
		transport.dropLink(peer, nodeID)
	}
}

func restoreNodeLinks(transport *memTransport, nodeID string, peers []string) {
	for _, peer := range peers {
		transport.restoreLink(nodeID, peer)
		transport.restoreLink(peer, nodeID)
	}
}

// waitRole polls until n.Role() == want or timeout.
func waitRole(t *testing.T, n *raft.RaftNode, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.Role() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node role: got %q want %q after %v", n.Role(), want, timeout)
}

// waitLeaderAmong polls until exactly one of the nodes is leader.
func waitLeaderAmong(t *testing.T, nodes []*raft.RaftNode, timeout time.Duration) *raft.RaftNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	roles := make([]string, len(nodes))
	for i, n := range nodes {
		roles[i] = n.Role()
	}
	t.Fatalf("no leader elected after %v; roles=%v", timeout, roles)
	return nil
}

// newTestLogger returns a *slog.Logger writing to stderr at WARN level.
func newTestLogger(_ *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

// TestSingleNodeElectsLeader: a single-node cluster (no peers) must
// elect itself as soon as the election timeout fires.
func TestSingleNodeElectsLeader(t *testing.T) {
	transport := newMemTransport()
	n := makeNode(t, "n1", nil, transport)
	transport.register("n1", n)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go n.Run(ctx) //nolint:errcheck

	waitRole(t, n, "leader", 500*time.Millisecond)
}

// TestThreeNodeElection: start 3 nodes; exactly one must become leader.
func TestThreeNodeElection(t *testing.T) {
	transport := newMemTransport()

	ids := []string{"n1", "n2", "n3"}
	nodes := make([]*raft.RaftNode, len(ids))
	for i, id := range ids {
		peers := make([]raft.Peer, 0, len(ids)-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, raft.Peer{NodeID: pid, RaftAddr: pid})
			}
		}
		nodes[i] = makeNode(t, id, peers, transport)
		transport.register(id, nodes[i])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, n := range nodes {
		n := n
		go n.Run(ctx) //nolint:errcheck
	}

	leader := waitLeaderAmong(t, nodes, time.Second)
	t.Logf("leader elected: %s term=%d", leader.LeaderID(), leader.CurrentTerm())

	// Verify no split-brain: at most one leader.
	leaderCount := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("split brain: %d nodes think they are leader", leaderCount)
	}
}

// TestTermRejection: a node with a higher term must reject stale RequestVote.
func TestTermRejection(t *testing.T) {
	transport := newMemTransport()
	n := makeNode(t, "n1", nil, transport)
	transport.register("n1", n)

	ctx := context.Background()

	// Advance n1 to term 5 by calling HandleRequestVote from a high-term peer.
	resp, err := n.HandleRequestVote(ctx, raft.RequestVoteRequest{
		Term:        5,
		CandidateID: "n2",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// n1 should update to term 5 and grant the vote (log is empty so candidate is OK).
	if resp.Term != 5 {
		t.Fatalf("expected resp.Term=5, got %d", resp.Term)
	}
	if !resp.VoteGranted {
		t.Fatalf("expected vote granted for empty-log candidate")
	}

	// Now a stale term=3 request must be rejected.
	resp2, err := n.HandleRequestVote(ctx, raft.RequestVoteRequest{
		Term:        3,
		CandidateID: "n3",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp2.VoteGranted {
		t.Fatalf("stale term should not be granted a vote")
	}
	if resp2.Term != 5 {
		t.Fatalf("response must carry current term=5, got %d", resp2.Term)
	}
}

// TestVoteNotGrantedTwice: a node must not grant two votes in the same term.
func TestVoteNotGrantedTwice(t *testing.T) {
	transport := newMemTransport()
	n := makeNode(t, "n1", nil, transport)

	ctx := context.Background()

	// First vote for n2, term 1.
	resp, err := n.HandleRequestVote(ctx, raft.RequestVoteRequest{
		Term: 1, CandidateID: "n2",
	})
	if err != nil || !resp.VoteGranted {
		t.Fatalf("first vote should be granted: err=%v granted=%v", err, resp.VoteGranted)
	}

	// Second vote request same term from n3 must be denied.
	resp2, err := n.HandleRequestVote(ctx, raft.RequestVoteRequest{
		Term: 1, CandidateID: "n3",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp2.VoteGranted {
		t.Fatalf("second candidate in same term must not be granted a vote")
	}
}

// TestAppendEntriesHeartbeat: a follower must reset its election timer
// when a valid heartbeat AppendEntries arrives.
func TestAppendEntriesHeartbeat(t *testing.T) {
	transport := newMemTransport()
	n := makeNode(t, "n1", []raft.Peer{{NodeID: "n2", RaftAddr: "n2"}}, transport)

	ctx := context.Background()

	// Announce ourselves as leader of term 1.
	resp, err := n.HandleAppendEntries(ctx, raft.AppendEntriesRequest{
		Term:     1,
		LeaderID: "n2",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("heartbeat AppendEntries (no entries, prevLogIndex=0) must succeed")
	}
	if n.LeaderID() != "n2" {
		t.Fatalf("leaderID should be n2 after AppendEntries, got %q", n.LeaderID())
	}
	if n.Role() != "follower" {
		t.Fatalf("role should remain follower after heartbeat, got %q", n.Role())
	}
}

// TestAppendEntriesLogMismatch: AppendEntries with mismatched prevLogTerm must fail.
func TestAppendEntriesLogMismatch(t *testing.T) {
	transport := newMemTransport()
	store := &memWALStore{}
	log, _ := raft.NewWALLog(context.Background(), store)

	// Pre-populate the log with one entry at term 1.
	_, _ = store.Append(context.Background(), ports.WALRecord{Term: 1, Type: "BEGIN", TxID: "t1"})
	// Reinitialise log to pick up the pre-populated record.
	log, _ = raft.NewWALLog(context.Background(), store)

	n, err := raft.NewRaftNode(context.Background(), raft.Config{
		NodeID:             "n1",
		Peers:              []raft.Peer{{NodeID: "n2", RaftAddr: "n2"}},
		Storage:            raft.NewMemStorage(),
		Log:                log,
		Transport:          transport,
		Clock:              clock.Realtime{},
		ElectionMinTimeout: 10 * time.Second, // long timeout so it doesn't elect during test
		ElectionMaxTimeout: 20 * time.Second,
		HeartbeatInterval:  10 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}

	ctx := context.Background()

	// Tell n1 the leader is n2 at term 2.
	_, _ = n.HandleAppendEntries(ctx, raft.AppendEntriesRequest{Term: 2, LeaderID: "n2"})

	// Now send AppendEntries claiming prevLogIndex=1 prevLogTerm=99 (mismatch).
	resp, err := n.HandleAppendEntries(ctx, raft.AppendEntriesRequest{
		Term:         2,
		LeaderID:     "n2",
		PrevLogIndex: 1,
		PrevLogTerm:  99, // wrong term
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Success {
		t.Fatalf("AppendEntries with mismatched prevLogTerm must fail")
	}
}

// TestAppendEntriesOverlappingPrefix verifies that a follower accepts
// overlapping AppendEntries batches that resend already-appended entries with
// matching terms, appending only the missing suffix.
func TestAppendEntriesOverlappingPrefix(t *testing.T) {
	transport := newMemTransport()
	store := &memWALStore{}

	// Pre-populate follower log with entries 1..3, all at term 1.
	for i := 0; i < 3; i++ {
		_, _ = store.Append(context.Background(), ports.WALRecord{Term: 1, Type: "MUTATION", TxID: fmt.Sprintf("t%d", i+1)})
	}
	log, err := raft.NewWALLog(context.Background(), store)
	if err != nil {
		t.Fatalf("NewWALLog: %v", err)
	}

	n, err := raft.NewRaftNode(context.Background(), raft.Config{
		NodeID:             "n1",
		Peers:              []raft.Peer{{NodeID: "n2", RaftAddr: "n2"}},
		Storage:            raft.NewMemStorage(),
		Log:                log,
		Transport:          transport,
		Clock:              clock.Realtime{},
		ElectionMinTimeout: 10 * time.Second,
		ElectionMaxTimeout: 20 * time.Second,
		HeartbeatInterval:  10 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}

	ctx := context.Background()
	resp, err := n.HandleAppendEntries(ctx, raft.AppendEntriesRequest{
		Term:         2,
		LeaderID:     "n2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []raft.RaftEntry{
			{Index: 2, Term: 1, TxID: "t2", Type: "MUTATION", Payload: []byte("two")},
			{Index: 3, Term: 1, TxID: "t3", Type: "MUTATION", Payload: []byte("three")},
			{Index: 4, Term: 2, TxID: "t4", Type: "COMMIT", Payload: []byte("four")},
		},
		LeaderCommit: 4,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("AppendEntries with overlapping matching prefix must succeed")
	}
	if got := store.LastLSN(); got != 4 {
		t.Fatalf("lastLSN=%d want 4", got)
	}
	records, err := store.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if len(records) != 4 {
		t.Fatalf("record count=%d want 4", len(records))
	}
	for i, rec := range records {
		want := uint64(i + 1)
		if rec.LSN != want {
			t.Fatalf("record[%d].LSN=%d want %d", i, rec.LSN, want)
		}
	}
}

// TestLeaderApplyAndReplication: a 3-node cluster elects a leader, then Apply
// successfully replicates a log entry to a majority.
func TestLeaderApplyAndReplication(t *testing.T) {
	transport := newMemTransport()

	ids := []string{"n1", "n2", "n3"}
	nodes := make([]*raft.RaftNode, len(ids))
	for i, id := range ids {
		peers := make([]raft.Peer, 0, len(ids)-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, raft.Peer{NodeID: pid, RaftAddr: pid})
			}
		}
		nodes[i] = makeNode(t, id, peers, transport)
		transport.register(id, nodes[i])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, n := range nodes {
		n := n
		go n.Run(ctx) //nolint:errcheck
	}

	leader := waitLeaderAmong(t, nodes, time.Second)

	// Apply a transaction commit entry.
	entry, err := leader.Apply(ctx, "COMMIT", "tx-001", []byte(`{"table":"users"}`))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if entry.Index == 0 {
		t.Fatalf("Apply returned zero index")
	}
	if entry.Term != leader.CurrentTerm() {
		t.Fatalf("entry term=%d want currentTerm=%d", entry.Term, leader.CurrentTerm())
	}
}

// TestLeaderApplyFailsOnNonLeader: Apply on a follower must return ErrNotLeader.
func TestLeaderApplyFailsOnNonLeader(t *testing.T) {
	transport := newMemTransport()

	ids := []string{"n1", "n2", "n3"}
	nodes := make([]*raft.RaftNode, len(ids))
	for i, id := range ids {
		peers := make([]raft.Peer, 0, len(ids)-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, raft.Peer{NodeID: pid, RaftAddr: pid})
			}
		}
		nodes[i] = makeNode(t, id, peers, transport)
		transport.register(id, nodes[i])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, n := range nodes {
		n := n
		go n.Run(ctx) //nolint:errcheck
	}

	waitLeaderAmong(t, nodes, time.Second)

	// Find a follower and try Apply.
	var follower *raft.RaftNode
	for _, n := range nodes {
		if !n.IsLeader() {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Skip("all nodes are leader (single-node mode?)")
	}

	_, err := follower.Apply(ctx, "COMMIT", "tx-002", nil)
	if err == nil {
		t.Fatal("Apply on follower should return an error")
	}
}

// TestDeadNodeCannotBecomeLeader: when 2 out of 3 nodes are alive, the dead
// node must never be elected — it can't collect a quorum of votes.
// We simulate a crash by cancelling the leader's goroutine context (stopping
// its heartbeat loop) and dropping its transport entry so it can't participate.
func TestDeadNodeCannotBecomeLeader(t *testing.T) {
	transport := newMemTransport()

	ids := []string{"n1", "n2", "n3"}
	nodes := make([]*raft.RaftNode, len(ids))
	// Per-node cancel so we can kill individual nodes.
	cancelFuncs := make([]context.CancelFunc, len(ids))

	for i, id := range ids {
		peers := make([]raft.Peer, 0, len(ids)-1)
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, raft.Peer{NodeID: pid, RaftAddr: pid})
			}
		}
		nodes[i] = makeNode(t, id, peers, transport)
		transport.register(id, nodes[i])
	}

	outerCtx, outerCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer outerCancel()

	for i, n := range nodes {
		nodeCtx, nodeCancel := context.WithCancel(outerCtx)
		cancelFuncs[i] = nodeCancel
		n := n
		go n.Run(nodeCtx) //nolint:errcheck
	}

	// Wait for initial leader.
	leader := waitLeaderAmong(t, nodes, 2*time.Second)
	leaderID := leader.LeaderID()
	t.Logf("initial leader: %s", leaderID)

	// Find the index of the dead leader and cancel its goroutine (real crash).
	deadIdx := -1
	for i, id := range ids {
		if id == leaderID {
			deadIdx = i
			break
		}
	}
	if deadIdx < 0 {
		t.Fatalf("could not find leader %s in ids", leaderID)
	}
	cancelFuncs[deadIdx]()           // stop the leader's Run loop
	transport.drop(leaderID)         // also block any stray in-flight RPCs to it

	// Collect surviving nodes.
	var surviving []*raft.RaftNode
	for i, n := range nodes {
		if ids[i] != leaderID {
			surviving = append(surviving, n)
		}
	}

	// The two survivors must elect one of themselves as the new leader.
	newLeader := waitLeaderAmong(t, surviving, 3*time.Second)
	if newLeader.LeaderID() == leaderID {
		t.Fatalf("dead node %s was re-elected — split brain!", leaderID)
	}
	t.Logf("new leader after crash: %s (dead was: %s)", newLeader.LeaderID(), leaderID)
}

func TestFollowerPartitionCatchUpAfterHealing(t *testing.T) {
	transport := newMemTransport()
	ids := []string{"n1", "n2", "n3"}
	runtimes := make([]*testNodeRuntime, 0, len(ids))
	for _, id := range ids {
		runtimes = append(runtimes, makeNodeRuntime(t, id, peersForNode(ids, id), transport))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for _, runtime := range runtimes {
		_ = startRuntime(ctx, runtime)
	}

	leader := waitLeaderAmong(t, nodePointers(runtimes), 2*time.Second)
	leaderRuntime := findRuntime(runtimes, leader)
	if leaderRuntime == nil {
		t.Fatal("expected leader runtime")
	}

	var lagging *testNodeRuntime
	var quorumFollower *testNodeRuntime
	for _, runtime := range runtimes {
		if runtime.id == leaderRuntime.id {
			continue
		}
		if lagging == nil {
			lagging = runtime
		} else {
			quorumFollower = runtime
		}
	}
	if lagging == nil || quorumFollower == nil {
		t.Fatal("expected two follower runtimes")
	}

	partitionNodeLinks(transport, lagging.id, []string{leaderRuntime.id, quorumFollower.id})
	defer restoreNodeLinks(transport, lagging.id, []string{leaderRuntime.id, quorumFollower.id})

	targetLSN := applyWrites(t, ctx, leader, "partition", 12)
	waitStoreLSN(t, quorumFollower.store, targetLSN, 2*time.Second)
	if got := lagging.store.LastLSN(); got >= targetLSN {
		t.Fatalf("expected lagging follower to stay behind during partition: got=%d target=%d", got, targetLSN)
	}

	restoreNodeLinks(transport, lagging.id, []string{leaderRuntime.id, quorumFollower.id})
	waitStoreLSN(t, lagging.store, targetLSN, 3*time.Second)
	if lagging.node.IsLeader() {
		t.Fatal("expected healed lagging node to remain follower")
	}
}

func TestLeaderCrashDuringSustainedWritesPromotesNewLeaderAndContinues(t *testing.T) {
	transport := newMemTransport()
	ids := []string{"n1", "n2", "n3"}
	runtimes := make([]*testNodeRuntime, 0, len(ids))
	for _, id := range ids {
		runtimes = append(runtimes, makeNodeRuntime(t, id, peersForNode(ids, id), transport))
	}

	ctx, outerCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer outerCancel()
	cancels := make(map[string]context.CancelFunc, len(runtimes))
	for _, runtime := range runtimes {
		cancels[runtime.id] = startRuntime(ctx, runtime)
	}

	leader := waitLeaderAmong(t, nodePointers(runtimes), 2*time.Second)
	leaderRuntime := findRuntime(runtimes, leader)
	if leaderRuntime == nil {
		t.Fatal("expected leader runtime")
	}

	targetLSN := applyWrites(t, ctx, leader, "before-crash", 10)

	cancels[leaderRuntime.id]()
	transport.drop(leaderRuntime.id)
	defer transport.restore(leaderRuntime.id)

	survivors := make([]*testNodeRuntime, 0, len(runtimes)-1)
	for _, runtime := range runtimes {
		if runtime.id != leaderRuntime.id {
			survivors = append(survivors, runtime)
		}
	}
	newLeader := waitLeaderAmong(t, nodePointers(survivors), 3*time.Second)
	if newLeader == leader {
		t.Fatal("expected a new leader after crash")
	}
	targetLSN = applyWrites(t, ctx, newLeader, "after-crash", 10)
	for _, runtime := range survivors {
		waitStoreLSN(t, runtime.store, targetLSN, 2*time.Second)
	}
	if got := leaderRuntime.store.LastLSN(); got >= targetLSN {
		t.Fatalf("expected crashed leader to stop before final LSN: got=%d final=%d", got, targetLSN)
	}
}

func TestStaleLeaderRecoversAfterPartitionHealing(t *testing.T) {
	transport := newMemTransport()
	ids := []string{"n1", "n2", "n3"}
	runtimes := make([]*testNodeRuntime, 0, len(ids))
	for _, id := range ids {
		runtimes = append(runtimes, makeNodeRuntime(t, id, peersForNode(ids, id), transport))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cancels := make(map[string]context.CancelFunc, len(runtimes))
	for _, runtime := range runtimes {
		cancels[runtime.id] = startRuntime(ctx, runtime)
	}

	leader := waitLeaderAmong(t, nodePointers(runtimes), 2*time.Second)
	staleLeader := findRuntime(runtimes, leader)
	if staleLeader == nil {
		t.Fatal("expected stale leader runtime")
	}

	var survivors []*testNodeRuntime
	var survivorIDs []string
	for _, runtime := range runtimes {
		if runtime.id != staleLeader.id {
			survivors = append(survivors, runtime)
			survivorIDs = append(survivorIDs, runtime.id)
		}
	}
	partitionNodeLinks(transport, staleLeader.id, survivorIDs)
	defer restoreNodeLinks(transport, staleLeader.id, survivorIDs)

	newLeader := waitLeaderAmong(t, nodePointers(survivors), 3*time.Second)
	if findRuntime(survivors, newLeader) == nil {
		t.Fatal("expected survivor leader after partition")
	}
	if !staleLeader.node.IsLeader() {
		t.Fatal("expected isolated node to remain stale leader until healing")
	}

	targetLSN := applyWrites(t, ctx, newLeader, "healed", 8)
	cancels[staleLeader.id]()
	restoreNodeLinks(transport, staleLeader.id, survivorIDs)
	cancels[staleLeader.id] = startRuntime(ctx, staleLeader)
	targetLSN = applyWrites(t, ctx, newLeader, "post-heal", 2)
	waitStoreLSN(t, staleLeader.store, targetLSN, 3*time.Second)
	waitRole(t, staleLeader.node, "follower", 3*time.Second)
	if staleLeader.node.IsLeader() {
		t.Fatal("expected healed stale leader to step down to follower")
	}
}

func TestRollingRestartsUnderSustainedWrites(t *testing.T) {
	transport := newMemTransport()
	ids := []string{"n1", "n2", "n3"}
	runtimes := make([]*testNodeRuntime, 0, len(ids))
	for _, id := range ids {
		runtimes = append(runtimes, makeNodeRuntime(t, id, peersForNode(ids, id), transport))
	}

	ctx, outerCancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer outerCancel()
	cancels := make(map[string]context.CancelFunc, len(runtimes))
	for _, runtime := range runtimes {
		cancels[runtime.id] = startRuntime(ctx, runtime)
	}

	targetLSN := applyWrites(t, ctx, waitLeaderAmong(t, nodePointers(runtimes), 2*time.Second), "warmup", 5)

	for _, runtime := range runtimes {
		currentLeader := waitLeaderAmong(t, nodePointers(runtimes), 2*time.Second)
		wasLeader := runtime.node == currentLeader

		cancels[runtime.id]()
		transport.drop(runtime.id)

		active := make([]*testNodeRuntime, 0, len(runtimes)-1)
		for _, candidate := range runtimes {
			if candidate.id != runtime.id {
				active = append(active, candidate)
			}
		}

		if wasLeader {
			currentLeader = waitLeaderAmong(t, nodePointers(active), 3*time.Second)
		}
		targetLSN = applyWrites(t, ctx, currentLeader, "rolling-"+runtime.id, 4)

		transport.restore(runtime.id)
		cancels[runtime.id] = startRuntime(ctx, runtime)
		waitStoreLSN(t, runtime.store, targetLSN, 4*time.Second)
	}

	finalLeader := waitLeaderAmong(t, nodePointers(runtimes), 3*time.Second)
	targetLSN = applyWrites(t, ctx, finalLeader, "final", 4)
	for _, runtime := range runtimes {
		waitStoreLSN(t, runtime.store, targetLSN, 4*time.Second)
	}
}
