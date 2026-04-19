package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
)

// ──────────────────────────────────────────────────────────────────────────────
// Peer  —  a remote cluster member
// ──────────────────────────────────────────────────────────────────────────────

// Peer describes one member of the Raft consensus group.
type Peer struct {
	// NodeID is the stable logical identifier for the node (e.g. "node-a").
	NodeID string
	// RaftAddr is the gRPC address for Raft RPCs (host:port).
	RaftAddr string
}

// ──────────────────────────────────────────────────────────────────────────────
// Config
// ──────────────────────────────────────────────────────────────────────────────

// Config holds all inputs required to create a RaftNode.
type Config struct {
	// NodeID is the unique identifier of this node within the group.
	NodeID string
	// Peers lists all OTHER nodes in the group (not including self).
	Peers []Peer

	// Storage persists currentTerm + votedFor across restarts.
	Storage Storage
	// Log provides durable log operations, backed by the WAL store.
	Log Log
	// Transport sends RequestVote and AppendEntries RPCs to peers.
	Transport Transport
	// Clock provides wall-clock time; inject a mock in tests.
	Clock ports.Clock

	// ElectionMinTimeout is the lower bound of the random election timer (default 500ms).
	ElectionMinTimeout time.Duration
	// ElectionMaxTimeout is the upper bound of the random election timer (default 1000ms).
	ElectionMaxTimeout time.Duration
	// HeartbeatInterval is how often the leader sends heartbeats (default 50ms).
	HeartbeatInterval time.Duration

	// OnEntriesCommitted is called after HandleAppendEntries advances the
	// commit index on a follower node. Implementations typically trigger an
	// incremental replay of new WAL entries into the engine's in-memory
	// state. When available, the committed records are provided directly so
	// followers can apply the delta without rereading the WAL. The callback
	// runs in a separate goroutine to avoid blocking the RPC. May be nil.
	OnEntriesCommitted func(ctx context.Context, commitIndex uint64, records []ports.WALRecord)

	Logger *slog.Logger
}

func (c *Config) electionMin() time.Duration {
	if c.ElectionMinTimeout > 0 {
		return c.ElectionMinTimeout
	}
	// Production default: use a wider timeout window than the test harness.
	// Under heavy replicated write load, follower append + catch-up can add
	// hundreds of milliseconds of jitter; overly aggressive elections cause
	// needless leader churn and benchmark instability.
	return 1500 * time.Millisecond
}

func (c *Config) electionMax() time.Duration {
	if c.ElectionMaxTimeout > 0 {
		return c.ElectionMaxTimeout
	}
	return 3000 * time.Millisecond
}

func (c *Config) heartbeat() time.Duration {
	if c.HeartbeatInterval > 0 {
		return c.HeartbeatInterval
	}
	return 50 * time.Millisecond
}

// ──────────────────────────────────────────────────────────────────────────────
// nodeState enum
// ──────────────────────────────────────────────────────────────────────────────

type nodeState uint8

const (
	stateFollower  nodeState = iota
	stateCandidate           // running an election
	stateLeader              // won the election, sending heartbeats
)

func (s nodeState) String() string {
	switch s {
	case stateFollower:
		return "follower"
	case stateCandidate:
		return "candidate"
	case stateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// RaftNode
// ──────────────────────────────────────────────────────────────────────────────

// RaftNode implements a single-group Raft consensus node.
//
// Thread safety: all exported methods are safe for concurrent use.
// The internal state machine is protected by a single mutex.  The Run loop
// polls at 10 ms resolution; RPCs lock the mutex briefly and return.
type RaftNode struct {
	cfg Config

	// ── persistent state (on stable storage) ─────────────────────────────────
	mu          sync.Mutex
	currentTerm uint64
	votedFor    string // "" = none

	// ── volatile state ────────────────────────────────────────────────────────
	state    nodeState
	leaderID string

	commitIndex uint64 // highest log index known to be committed
	lastApplied uint64 // highest log index applied (== commitIndex for this engine)

	// ── leader volatile state (reset after each election win) ─────────────────
	nextIndex     map[string]uint64 // peer nodeID → next log index to send
	matchIndex    map[string]uint64 // peer nodeID → highest index known replicated on peer
	leaderReadyCh chan struct{}     // closed after engine catch-up; Apply blocks until closed

	// ── timer ─────────────────────────────────────────────────────────────────
	electionDeadline time.Time
	lastHeartbeat    time.Time // leader: last time we sent heartbeats

	// ── config shortcuts ──────────────────────────────────────────────────────
	electionMin  time.Duration
	electionMax  time.Duration
	heartbeatInt time.Duration

	replicateMu     sync.Mutex
	replicationBusy bool
}

// NewRaftNode creates a RaftNode from config and restores persisted term state.
// Call Run(ctx) in a goroutine to start the consensus loop.
func NewRaftNode(ctx context.Context, cfg Config) (*RaftNode, error) {
	n := &RaftNode{
		cfg:          cfg,
		state:        stateFollower,
		electionMin:  cfg.electionMin(),
		electionMax:  cfg.electionMax(),
		heartbeatInt: cfg.heartbeat(),
	}

	// Restore persisted term + vote.
	term, voted, err := cfg.Storage.LoadState(ctx)
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}
	n.currentTerm = term
	n.votedFor = voted

	// Restore commitIndex from the WAL last index.
	// We assume any record that made it to the WAL was committed (single-node
	// safety; the distributed invariant is enforced by quorum append going forward).
	n.commitIndex = cfg.Log.LastIndex()
	n.lastApplied = n.commitIndex

	n.resetElectionDeadline()
	return n, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Public observable methods
// ──────────────────────────────────────────────────────────────────────────────

// Role returns "follower", "candidate", or "leader".
func (n *RaftNode) Role() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.String()
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *RaftNode) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == stateLeader
}

// LeaderID returns the nodeID of the current known leader, or "" if unknown.
// Followers learn the leaderID from AppendEntries RPCs (§5.1).
func (n *RaftNode) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// LeaderGRPCAddr returns the gRPC/Raft address of the current cluster leader
// so that WAL replication streams can connect to the right node. Returns ""
// when the leader is unknown or when this node is itself the leader (callers
// should not replicate from themselves).
func (n *RaftNode) LeaderGRPCAddr() string {
	n.mu.Lock()
	leaderID := n.leaderID
	n.mu.Unlock()
	if leaderID == "" || leaderID == n.cfg.NodeID {
		return ""
	}
	for _, p := range n.cfg.Peers {
		if p.NodeID == leaderID {
			return p.RaftAddr
		}
	}
	return ""
}

// CurrentTerm returns the node's current Raft term.
func (n *RaftNode) CurrentTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// ──────────────────────────────────────────────────────────────────────────────
// Apply — leader write path
// ──────────────────────────────────────────────────────────────────────────────

// Apply proposes a new log entry.  Only the leader may call this; followers
// return ErrNotLeader so callers can redirect to the current leader.
//
// Apply blocks until a quorum of nodes has durably stored the entry or the
// context is cancelled.  It returns the committed Entry on success.
func (n *RaftNode) Apply(ctx context.Context, typ, txID string, payload []byte) (Entry, error) {
	n.mu.Lock()
	if n.state != stateLeader {
		n.mu.Unlock()
		return Entry{}, ErrNotLeader
	}
	readyCh := n.leaderReadyCh
	term := n.currentTerm
	n.mu.Unlock()

	// Block until leader engine catch-up is complete.
	if readyCh != nil {
		select {
		case <-readyCh:
		case <-ctx.Done():
			return Entry{}, ctx.Err()
		}
		n.mu.Lock()
		if n.state != stateLeader {
			n.mu.Unlock()
			return Entry{}, ErrNotLeader
		}
		n.mu.Unlock()
	}

	// Append to our own log first.
	entry, err := n.cfg.Log.AppendLeader(ctx, term, typ, txID, payload)
	if err != nil {
		return Entry{}, fmt.Errorf("raft: append log: %w", err)
	}

	// Update own matchIndex — check state again: a concurrent step-down
	// (e.g. heartbeat response from a higher-term leader) may have called
	// becomeFollowerLocked which nils the maps.
	n.mu.Lock()
	if n.state != stateLeader {
		n.mu.Unlock()
		return Entry{}, ErrNotLeader
	}
	n.matchIndex[n.cfg.NodeID] = entry.Index
	n.mu.Unlock()

	// Fan out AppendEntries to all followers and wait for quorum.
	if err := n.broadcastAndCommit(ctx, entry.Index); err != nil {
		return Entry{}, err
	}

	return entry, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// ApplyBatch — leader batched write path
// ──────────────────────────────────────────────────────────────────────────────

// BatchRecord is a single entry to be proposed as part of a batch via ApplyBatch.
type BatchRecord struct {
	Type    string
	TxID    string
	Payload []byte
}

// ApplyBatch proposes multiple log entries in a single Raft round-trip.
// All entries are appended locally first, then a single broadcastAndCommit is
// issued for the highest index, amortising the network latency of the quorum
// write across all records in the batch.
//
// Only the leader may call this; ErrNotLeader is returned on any other node.
// The method blocks until quorum durability is established for the last entry.
func (n *RaftNode) ApplyBatch(ctx context.Context, records []BatchRecord) ([]Entry, error) {
	if len(records) == 0 {
		return nil, nil
	}

	n.mu.Lock()
	if n.state != stateLeader {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	readyCh := n.leaderReadyCh
	term := n.currentTerm
	n.mu.Unlock()

	// Block until leader engine catch-up is complete.
	if readyCh != nil {
		select {
		case <-readyCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		n.mu.Lock()
		if n.state != stateLeader {
			n.mu.Unlock()
			return nil, ErrNotLeader
		}
		n.mu.Unlock()
	}

	entries, err := n.cfg.Log.AppendLeaderBatch(ctx, term, records)
	if err != nil {
		return nil, fmt.Errorf("raft: append batch: %w", err)
	}

	lastIndex := entries[len(entries)-1].Index

	// Guard: concurrent step-down nils the maps — check state before writing.
	n.mu.Lock()
	if n.state != stateLeader {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	n.matchIndex[n.cfg.NodeID] = lastIndex
	n.mu.Unlock()

	// Single broadcastAndCommit for the entire batch — this is the key
	// optimisation: N local appends + 1 network round-trip instead of N.
	if err := n.broadcastAndCommit(ctx, lastIndex); err != nil {
		return nil, err
	}

	return entries, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// RPC handlers — called by the gRPC server
// ──────────────────────────────────────────────────────────────────────────────

// HandleRequestVote processes a RequestVote RPC from a candidate (§5.2, §5.4).
func (n *RaftNode) HandleRequestVote(ctx context.Context, req RequestVoteRequest) (RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If the request carries a higher term, update ours and step down.
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term)
		if err := n.persistLocked(ctx); err != nil {
			return RequestVoteResponse{}, err
		}
	}

	resp := RequestVoteResponse{Term: n.currentTerm}

	// Deny if request is stale.
	if req.Term < n.currentTerm {
		return resp, nil
	}

	// Deny if we already voted for someone else this term.
	if n.votedFor != "" && n.votedFor != req.CandidateID {
		return resp, nil
	}

	// Deny if candidate's log is behind ours (§5.4.1 — log completeness check).
	lastIdx := n.cfg.Log.LastIndex()
	lastTerm := n.cfg.Log.LastTerm()
	candidateOK := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)
	if !candidateOK {
		return resp, nil
	}

	// Grant vote.
	n.votedFor = req.CandidateID
	n.resetElectionDeadline()
	if err := n.persistLocked(ctx); err != nil {
		return RequestVoteResponse{}, err
	}
	resp.VoteGranted = true
	n.cfg.Logger.Info("raft.vote_granted",
		slog.String("node", n.cfg.NodeID),
		slog.String("candidate", req.CandidateID),
		slog.Uint64("term", req.Term))
	return resp, nil
}

// HandleAppendEntries processes an AppendEntries RPC from the leader (§5.3).
func (n *RaftNode) HandleAppendEntries(ctx context.Context, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	stepdownDur := time.Duration(0)
	prevCheckDur := time.Duration(0)
	overlapDur := time.Duration(0)
	truncateDur := time.Duration(0)
	appendDur := time.Duration(0)
	commitAdvanceDur := time.Duration(0)

	resp := AppendEntriesResponse{Term: n.currentTerm}

	// Stale leader; tell it our term.
	if req.Term < n.currentTerm {
		return resp, nil
	}

	// Valid leader: update term if needed, step down to follower.
	if req.Term > n.currentTerm || n.state != stateFollower {
		stepdownStart := time.Now()
		n.becomeFollowerLocked(req.Term)
		if err := n.persistLocked(ctx); err != nil {
			return AppendEntriesResponse{}, err
		}
		stepdownDur += time.Since(stepdownStart)
	}
	n.leaderID = req.LeaderID
	n.resetElectionDeadline()
	resp.Term = n.currentTerm

	// Consistency check: our log must contain prevLogIndex with prevLogTerm.
	if req.PrevLogIndex > 0 {
		prevCheckStart := time.Now()
		prevTerm, err := n.cfg.Log.Term(req.PrevLogIndex)
		prevCheckDur += time.Since(prevCheckStart)
		if err != nil || prevTerm != req.PrevLogTerm {
			// Conflict: send back info to help leader back up quickly.
			if err == nil && prevTerm != req.PrevLogTerm {
				resp.ConflictTerm = prevTerm
				// Find first index in conflictTerm.
				resp.ConflictIndex = req.PrevLogIndex
			} else {
				// Index out of range; tell leader our last index.
				resp.ConflictIndex = n.cfg.Log.LastIndex() + 1
			}
			return resp, nil
		}
	}

	// Apply new entries.
	if len(req.Entries) > 0 {
		entries := make([]Entry, len(req.Entries))
		for i, re := range req.Entries {
			entries[i] = Entry(re)
		}

		// Skip any overlapping prefix we already have with the same term.
		// This is required for valid Raft retries / overlapping AppendEntries:
		// a follower may receive entries it has already appended, and must not
		// treat them as out-of-order WAL appends.
		overlapStart := time.Now()
		appendFrom := 0
		conflictIndex := uint64(0)
		lastIndex := n.cfg.Log.LastIndex()
		if matcher, ok := n.cfg.Log.(overlapMatcher); ok {
			var matchErr error
			appendFrom, conflictIndex, matchErr = matcher.MatchEntriesPrefix(entries)
			if matchErr != nil {
				return AppendEntriesResponse{}, fmt.Errorf("raft: match entries prefix: %w", matchErr)
			}
		} else {
			for appendFrom < len(entries) && entries[appendFrom].Index <= lastIndex {
				existingTerm, err := n.cfg.Log.Term(entries[appendFrom].Index)
				if err != nil {
					break
				}
				if existingTerm != entries[appendFrom].Term {
					conflictIndex = entries[appendFrom].Index
					break
				}
				appendFrom++
			}
		}
		if conflictIndex > 0 {
			n.mu.Unlock()
			truncateStart := time.Now()
			_ = n.cfg.Log.TruncateAfter(ctx, conflictIndex-1)
			truncateDur += time.Since(truncateStart)
			n.mu.Lock()
			n.resetElectionDeadline() // refresh after I/O to prevent spurious elections
		}
		overlapDur += time.Since(overlapStart)

		entries = entries[appendFrom:]
		if len(entries) > 0 {
			n.mu.Unlock()
			appendStart := time.Now()
			if err := n.cfg.Log.AppendFollower(ctx, entries); err != nil {
				appendDur += time.Since(appendStart)
				n.mu.Lock()
				return AppendEntriesResponse{}, fmt.Errorf("raft: append follower: %w", err)
			}
			appendDur += time.Since(appendStart)
			n.mu.Lock()
			n.resetElectionDeadline() // refresh after I/O to prevent spurious elections
		}
	}

	// Advance commitIndex.
	commitAdvanceStart := time.Now()
	lastNew := n.cfg.Log.LastIndex()
	oldCommit := n.commitIndex
	if req.LeaderCommit > n.commitIndex {
		if req.LeaderCommit < lastNew {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNew
		}
	}
	commitAdvanceDur += time.Since(commitAdvanceStart)

	// Notify engine of newly committed entries so followers keep their
	// in-memory state up to date. Runs in a goroutine to avoid blocking
	// the RPC; the engine's writeMu serialises with the commit queue.
	if n.commitIndex > oldCommit && n.cfg.OnEntriesCommitted != nil {
		ci := n.commitIndex
		fromLSN := oldCommit + 1
		committed := committedWALRecordsFromAppend(req.Entries, oldCommit+1, ci)
		go func(commitIndex, fromLSN uint64, committed []ports.WALRecord) {
			if uint64(len(committed)) != commitIndex-fromLSN+1 {
				// Heartbeats that only advance LeaderCommit often arrive without the
				// full committed span in req.Entries. Reload any missing records in
				// the background so the RPC path stays bounded while followers still
				// receive an exact incremental delta instead of falling back to a
				// full CatchUp(commitIndex) on every commit advance.
				entries, err := n.cfg.Log.Entries(context.Background(), fromLSN, commitIndex+1)
				if err != nil {
					if n.cfg.Logger != nil {
						n.cfg.Logger.Warn("raft committed log reload failed",
							"from_lsn", fromLSN,
							"to_lsn", commitIndex,
							"error", err)
					}
					committed = nil
				} else {
					committed = walRecordsFromEntries(entries)
					if uint64(len(committed)) != commitIndex-fromLSN+1 {
						committed = nil
					}
				}
			}
			n.cfg.OnEntriesCommitted(context.Background(), commitIndex, committed)
		}(ci, fromLSN, committed)
	}

	resp.Success = true
	resp.LastIndex = lastNew
	return resp, nil
}

func walRecordsFromEntries(entries []Entry) []ports.WALRecord {
	if len(entries) == 0 {
		return nil
	}
	records := make([]ports.WALRecord, len(entries))
	for i, e := range entries {
		records[i] = ports.WALRecord{
			LSN:     e.Index,
			Term:    e.Term,
			TxID:    e.TxID,
			Type:    e.Type,
			Payload: e.Payload,
		}
	}
	return records
}

func committedWALRecordsFromAppend(entries []RaftEntry, fromLSN, toLSN uint64) []ports.WALRecord {
	if len(entries) == 0 || fromLSN > toLSN {
		return nil
	}
	records := make([]ports.WALRecord, 0, min(len(entries), int(toLSN-fromLSN+1)))
	for _, e := range entries {
		if e.Index < fromLSN || e.Index > toLSN {
			continue
		}
		records = append(records, ports.WALRecord{
			LSN:     e.Index,
			Term:    e.Term,
			TxID:    e.TxID,
			Type:    e.Type,
			Payload: e.Payload,
		})
	}
	return records
}

// ──────────────────────────────────────────────────────────────────────────────
// Run — main consensus loop
// ──────────────────────────────────────────────────────────────────────────────

// Run drives the Raft state machine until ctx is cancelled.
// It should be started in a dedicated goroutine.
func (n *RaftNode) Run(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n.tick(ctx)
		}
	}
}

// tick is called every 10ms by the Run loop.
func (n *RaftNode) tick(ctx context.Context) {
	n.mu.Lock()
	state := n.state
	now := n.cfg.Clock.Now()
	deadline := n.electionDeadline
	lastHB := n.lastHeartbeat
	n.mu.Unlock()

	switch state {
	case stateFollower, stateCandidate:
		if now.After(deadline) {
			n.startElection(ctx)
		}
	case stateLeader:
		if now.Sub(lastHB) >= n.heartbeatInt {
			n.sendHeartbeats(ctx)
		}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Election
// ──────────────────────────────────────────────────────────────────────────────

// startElection transitions this node to candidate, increments the term,
// votes for itself, and sends RequestVote to all peers.
func (n *RaftNode) startElection(ctx context.Context) {
	n.mu.Lock()
	n.currentTerm++
	n.state = stateCandidate
	n.votedFor = n.cfg.NodeID
	n.leaderID = ""
	term := n.currentTerm
	lastIdx := n.cfg.Log.LastIndex()
	lastTerm := n.cfg.Log.LastTerm()
	n.resetElectionDeadline()
	peers := n.cfg.Peers
	_ = n.persistLocked(ctx)
	n.mu.Unlock()

	n.cfg.Logger.Info("raft.election_started",
		slog.String("node", n.cfg.NodeID),
		slog.Uint64("term", term))

	quorum := (len(peers)+1)/2 + 1 // e.g. 3 nodes → quorum=2
	votes := 1                     // vote for self

	type voteResult struct {
		resp RequestVoteResponse
		err  error
	}
	ch := make(chan voteResult, len(peers))

	for _, p := range peers {
		p := p
		go func() {
			// Per-RPC timeout prevents blocking the tick loop when a
			// peer is unreachable (avoids TCP-level timeouts of 30s+).
			rpcCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			resp, err := n.cfg.Transport.RequestVote(rpcCtx, p.RaftAddr, RequestVoteRequest{
				Term:         term,
				CandidateID:  n.cfg.NodeID,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			ch <- voteResult{resp, err}
		}()
	}

	for i := 0; i < len(peers); i++ {
		res := <-ch
		if res.err != nil {
			continue
		}
		n.mu.Lock()
		// If we see a higher term we must step down immediately.
		if res.resp.Term > n.currentTerm {
			n.becomeFollowerLocked(res.resp.Term)
			_ = n.persistLocked(ctx)
			n.mu.Unlock()
			return
		}
		if res.resp.VoteGranted {
			votes++
		}
		curState := n.state
		n.mu.Unlock()

		if votes >= quorum && curState == stateCandidate {
			if readyCh := n.becomeLeader(ctx, term); readyCh != nil {
				// Synchronously catch up engine state before unblocking writes.
				if n.cfg.OnEntriesCommitted != nil {
					n.mu.Lock()
					ci := n.commitIndex
					n.mu.Unlock()
					n.cfg.OnEntriesCommitted(ctx, ci, nil)
				}
				close(readyCh)
			}
			return
		}
	}

	// Single-node cluster: no peers to poll, but self-vote already satisfies quorum.
	if votes >= quorum {
		n.mu.Lock()
		curState := n.state
		n.mu.Unlock()
		if curState == stateCandidate {
			if readyCh := n.becomeLeader(ctx, term); readyCh != nil {
				if n.cfg.OnEntriesCommitted != nil {
					n.mu.Lock()
					ci := n.commitIndex
					n.mu.Unlock()
					n.cfg.OnEntriesCommitted(ctx, ci, nil)
				}
				close(readyCh)
			}
		}
	}

	// Reset deadline after a failed election so we don't immediately retry
	// (avoids livelock when multiple candidates fire simultaneously).
	n.mu.Lock()
	if n.state == stateCandidate {
		n.resetElectionDeadline()
	}
	n.mu.Unlock()
}

// becomeLeader transitions this node to leader for term.
// Returns a non-nil ready channel if the transition succeeded; the caller
// MUST close it after engine catch-up is complete to unblock Apply.
// Returns nil if the transition was not possible (wrong state/term).
func (n *RaftNode) becomeLeader(ctx context.Context, term uint64) chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Guard: only become leader if still candidate in the same term.
	if n.state != stateCandidate || n.currentTerm != term {
		return nil
	}

	n.state = stateLeader
	n.leaderID = n.cfg.NodeID
	n.lastHeartbeat = time.Time{} // force immediate heartbeat

	// Create a ready channel that Apply will block on until engine catch-up
	// is complete.  The caller (startElection) closes it after CatchUp.
	readyCh := make(chan struct{})
	n.leaderReadyCh = readyCh

	// Initialize leader volatile state (§5.3).
	lastIdx := n.cfg.Log.LastIndex()
	n.nextIndex = make(map[string]uint64, len(n.cfg.Peers))
	n.matchIndex = make(map[string]uint64, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		n.nextIndex[p.NodeID] = lastIdx + 1
		n.matchIndex[p.NodeID] = 0
	}
	n.matchIndex[n.cfg.NodeID] = lastIdx

	n.cfg.Logger.Info("raft.became_leader",
		slog.String("node", n.cfg.NodeID),
		slog.Uint64("term", term),
		slog.Uint64("last_index", lastIdx))
	return readyCh
}

// ──────────────────────────────────────────────────────────────────────────────
// Heartbeats / log replication
// ──────────────────────────────────────────────────────────────────────────────

// sendHeartbeats sends AppendEntries to all peers.  When a follower is behind
// (nextIndex ≤ lastIndex), the heartbeat piggy-backs up to maxEntriesPerHB
// missing entries so followers catch up between explicit Apply rounds.
func (n *RaftNode) sendHeartbeats(ctx context.Context) {
	const maxEntriesPerHB = 64

	n.mu.Lock()
	if n.state != stateLeader {
		n.mu.Unlock()
		return
	}
	n.lastHeartbeat = n.cfg.Clock.Now()
	term := n.currentTerm
	commitIdx := n.commitIndex
	peers := n.cfg.Peers
	replicationBusy := n.replicationBusy
	if replicationBusy {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	lastIdx := n.cfg.Log.LastIndex()

	for _, p := range peers {
		p := p
		n.mu.Lock()
		if n.state != stateLeader {
			n.mu.Unlock()
			return
		}
		ni := n.nextIndex[p.NodeID]
		n.mu.Unlock()
		go func() {
			rpcCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()

			var rEntries []RaftEntry
			var prevIdx, prevTerm uint64

			if ni <= lastIdx {
				// Follower is behind — piggyback entries on the heartbeat.
				upper := lastIdx + 1
				if upper-ni > maxEntriesPerHB {
					upper = ni + maxEntriesPerHB
				}
				ents, err := n.cfg.Log.Entries(rpcCtx, ni, upper)
				if err == nil && len(ents) > 0 {
					rEntries = make([]RaftEntry, len(ents))
					for i, e := range ents {
						rEntries[i] = RaftEntry(e)
					}
				}
				if ni > 1 {
					prevIdx = ni - 1
					prevTerm, _ = n.cfg.Log.Term(prevIdx)
				}
			} else {
				// Follower is up to date — pure heartbeat.
				prevIdx = lastIdx
				if lastIdx > 0 {
					prevTerm, _ = n.cfg.Log.Term(lastIdx)
				}
			}

			req := AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.cfg.NodeID,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTerm,
				Entries:      rEntries,
				LeaderCommit: commitIdx,
			}
			resp, err := n.cfg.Transport.AppendEntries(rpcCtx, p.RaftAddr, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term)
				_ = n.persistLocked(ctx)
				return
			}
			if n.state != stateLeader {
				return
			}
			if resp.Success {
				if resp.LastIndex > n.matchIndex[p.NodeID] {
					n.matchIndex[p.NodeID] = resp.LastIndex
					n.nextIndex[p.NodeID] = resp.LastIndex + 1
				}
				n.advanceCommitIndexLocked(term)
			} else {
				// Back up nextIndex using conflict info from the follower.
				if resp.ConflictIndex > 0 && resp.ConflictIndex < n.nextIndex[p.NodeID] {
					n.nextIndex[p.NodeID] = resp.ConflictIndex
				} else if n.nextIndex[p.NodeID] > 1 {
					n.nextIndex[p.NodeID]--
				}
			}
		}()
	}
}

// broadcastAndCommit sends AppendEntries with the entries each follower is
// missing, and blocks until a quorum (including self) has acknowledged
// index newIndex.  Called by Apply after appending to the local log.
func (n *RaftNode) broadcastAndCommit(ctx context.Context, newIndex uint64) error {
	const maxEntriesPerBroadcast = 128

	n.replicateMu.Lock()
	defer n.replicateMu.Unlock()

	n.mu.Lock()
	if n.state != stateLeader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.commitIndex >= newIndex {
		n.mu.Unlock()
		return nil
	}
	if lastIdx := n.cfg.Log.LastIndex(); lastIdx > newIndex {
		newIndex = lastIdx
	}
	n.replicationBusy = true
	term := n.currentTerm
	commitIdx := n.commitIndex
	peers := n.cfg.Peers
	peerStates := make([]struct {
		peer      Peer
		nextIndex uint64
	}, 0, len(peers))
	for _, p := range peers {
		peerStates = append(peerStates, struct {
			peer      Peer
			nextIndex uint64
		}{
			peer:      p,
			nextIndex: n.nextIndex[p.NodeID],
		})
	}
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		n.replicationBusy = false
		n.mu.Unlock()
	}()

	quorum := (len(peers)+1)/2 + 1

	type peerResult struct {
		peer     Peer
		peerID   string
		resp     AppendEntriesResponse
		err      error
		readDur  time.Duration
		buildDur time.Duration
		rpcDur   time.Duration
		entries  int
	}
	ch := make(chan peerResult, len(peers))
	launchPeerReplication := func(parentCtx context.Context, p Peer, ni uint64, resultCh chan<- peerResult) {
		go func() {
			// Per-RPC timeout prevents a slow/down peer from blocking the
			// entire commit path indefinitely.
			rpcCtx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
			defer cancel()

			// Load the entries [ni, upper) that this peer is missing.
			upper := newIndex + 1
			if upper-ni > maxEntriesPerBroadcast {
				upper = ni + maxEntriesPerBroadcast
			}
			readStart := time.Now()
			entries, err := n.cfg.Log.Entries(rpcCtx, ni, upper)
			readDur := time.Since(readStart)
			if err != nil {
				resultCh <- peerResult{peer: p, peerID: p.NodeID, resp: AppendEntriesResponse{}, err: err, readDur: readDur}
				return
			}
			buildStart := time.Now()
			var prevIdx uint64
			var prevTerm uint64
			if ni > 1 {
				prevIdx = ni - 1
				prevTerm, _ = n.cfg.Log.Term(prevIdx)
			}
			rEntries := make([]RaftEntry, len(entries))
			for i, e := range entries {
				rEntries[i] = RaftEntry(e)
			}
			req := AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.cfg.NodeID,
				PrevLogIndex: prevIdx,
				PrevLogTerm:  prevTerm,
				Entries:      rEntries,
				LeaderCommit: commitIdx,
			}
			buildDur := time.Since(buildStart)
			rpcStart := time.Now()
			resp, err := n.cfg.Transport.AppendEntries(rpcCtx, p.RaftAddr, req)
			resultCh <- peerResult{peer: p, peerID: p.NodeID, resp: resp, err: err, readDur: readDur, buildDur: buildDur, rpcDur: time.Since(rpcStart), entries: len(entries)}
		}()
	}
	processPeerResult := func(persistCtx context.Context, res peerResult) (acked bool, steppedDown bool, retry bool, retryFrom uint64) {
		n.mu.Lock()
		defer n.mu.Unlock()

		// If a concurrent step-down (e.g. heartbeat from higher-term leader)
		// called becomeFollowerLocked, the maps are nil — bail out safely.
		if n.state != stateLeader {
			return false, true, false, 0
		}
		if res.err != nil {
			n.cfg.Logger.Warn("raft.broadcast peer error",
				slog.String("peer", res.peerID),
				slog.String("error", res.err.Error()))
			return false, false, false, 0
		}
		if res.resp.Term > n.currentTerm {
			n.becomeFollowerLocked(res.resp.Term)
			_ = n.persistLocked(persistCtx)
			return false, true, false, 0
		}
		if res.resp.Success {
			if res.resp.LastIndex > n.matchIndex[res.peerID] {
				n.matchIndex[res.peerID] = res.resp.LastIndex
			}
			n.nextIndex[res.peerID] = res.resp.LastIndex + 1
			if res.resp.LastIndex >= newIndex {
				return true, false, false, 0
			}
			return false, false, true, n.nextIndex[res.peerID]
		}

		n.cfg.Logger.Warn("raft.broadcast peer rejected",
			slog.String("peer", res.peerID),
			slog.Uint64("conflict_term", res.resp.ConflictTerm),
			slog.Uint64("conflict_index", res.resp.ConflictIndex))
		// Use ConflictIndex to skip back by term boundary, not one-by-one.
		if res.resp.ConflictIndex > 0 && res.resp.ConflictIndex < n.nextIndex[res.peerID] {
			n.nextIndex[res.peerID] = res.resp.ConflictIndex
		} else if n.nextIndex[res.peerID] > 1 {
			n.nextIndex[res.peerID]--
		}
		return false, false, true, n.nextIndex[res.peerID]
	}

	for _, ps := range peerStates {
		launchPeerReplication(ctx, ps.peer, ps.nextIndex, ch)
	}

	acks := 1 // self
	var totalReadDur time.Duration
	var totalBuildDur time.Duration
	var totalRPCDur time.Duration
	var totalPeerEntries int
	var maxReadDur time.Duration
	var maxBuildDur time.Duration
	var maxRPCDur time.Duration
	pending := len(peers)
	for pending > 0 {
		res := <-ch
		totalReadDur += res.readDur
		totalBuildDur += res.buildDur
		totalRPCDur += res.rpcDur
		totalPeerEntries += res.entries
		if res.readDur > maxReadDur {
			maxReadDur = res.readDur
		}
		if res.buildDur > maxBuildDur {
			maxBuildDur = res.buildDur
		}
		if res.rpcDur > maxRPCDur {
			maxRPCDur = res.rpcDur
		}
		acked, steppedDown, retry, retryFrom := processPeerResult(ctx, res)
		if steppedDown {
			return ErrNotLeader
		}
		if acked {
			acks++
		}
		if retry {
			launchPeerReplication(ctx, res.peer, retryFrom, ch)
			continue
		}
		pending--

		if acks >= quorum {
			remaining := pending
			if remaining > 0 {
				go func(pending int) {
					bgCtx := context.Background()
					for j := 0; j < pending; j++ {
						res := <-ch
						_, _, _, _ = processPeerResult(bgCtx, res)
					}
				}(remaining)
			}
			n.mu.Lock()
			n.advanceCommitIndexLocked(term)
			n.mu.Unlock()
			return nil
		}
	}

	n.cfg.Logger.Warn("raft.quorum_unreachable",
		slog.Int("acks", acks),
		slog.Int("quorum", quorum),
		slog.Int("peers", len(peers)))
	return ErrQuorumUnreachable
}

// advanceCommitIndexLocked finds the highest index that a majority has matched
// and updates n.commitIndex.  Must be called with n.mu held.
func (n *RaftNode) advanceCommitIndexLocked(term uint64) {
	matches := make([]uint64, 0, len(n.cfg.Peers)+1)
	matches = append(matches, n.cfg.Log.LastIndex()) // self
	for _, peer := range n.cfg.Peers {
		matches = append(matches, n.matchIndex[peer.NodeID])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })

	quorum := len(matches)/2 + 1
	for i := len(matches) - 1; i >= 0; {
		candidate := matches[i]
		groupStart := i
		for groupStart > 0 && matches[groupStart-1] == candidate {
			groupStart--
		}
		if candidate > n.commitIndex && len(matches)-groupStart >= quorum {
			entryTerm, err := n.cfg.Log.Term(candidate)
			if err == nil && entryTerm == term {
				n.commitIndex = candidate
				return
			}
		}
		i = groupStart - 1
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

// becomeFollowerLocked transitions to follower and clears votedFor for the new term.
// MUST be called with n.mu held.
func (n *RaftNode) becomeFollowerLocked(term uint64) {
	n.state = stateFollower
	n.currentTerm = term
	n.votedFor = ""
	n.nextIndex = nil
	n.matchIndex = nil
	n.replicationBusy = false
	n.resetElectionDeadline()
	n.cfg.Logger.Info("raft.became_follower",
		slog.String("node", n.cfg.NodeID),
		slog.Uint64("term", term))
}

// persistLocked writes currentTerm + votedFor to durable storage.
// MUST be called with n.mu held.
func (n *RaftNode) persistLocked(ctx context.Context) error {
	term := n.currentTerm
	voted := n.votedFor
	n.mu.Unlock()
	err := n.cfg.Storage.SaveState(ctx, term, voted)
	n.mu.Lock()
	return err
}

// resetElectionDeadline picks a random election timeout in [min, max).
// SAFE to call with or without n.mu held (does not use shared state).
func (n *RaftNode) resetElectionDeadline() {
	spread := n.electionMax - n.electionMin
	jitter := time.Duration(rand.Int63n(int64(spread)))
	n.electionDeadline = n.cfg.Clock.Now().Add(n.electionMin + jitter)
}
