package raft

import "context"

// ──────────────────────────────────────────────────────────────────────────────
// RequestVote RPC (§5.2)
// ──────────────────────────────────────────────────────────────────────────────

// RequestVoteRequest is sent by a candidate to solicit votes.
type RequestVoteRequest struct {
	// Term is the candidate's current term.
	Term uint64 `json:"term"`
	// CandidateID is the node ID of the requesting candidate.
	CandidateID string `json:"candidate_id"`
	// LastLogIndex is the index of the candidate's last log entry (§5.4).
	LastLogIndex uint64 `json:"last_log_index"`
	// LastLogTerm is the term of the candidate's last log entry (§5.4).
	LastLogTerm uint64 `json:"last_log_term"`
}

// RequestVoteResponse is returned by the voter.
type RequestVoteResponse struct {
	// Term is the voter's current term.  If greater than the candidate's term,
	// the candidate must step down to follower.
	Term uint64 `json:"term"`
	// VoteGranted is true when the vote was granted.
	VoteGranted bool `json:"vote_granted"`
}

// ──────────────────────────────────────────────────────────────────────────────
// AppendEntries RPC (§5.3)
// ──────────────────────────────────────────────────────────────────────────────

// RaftEntry is a single log entry transmitted via AppendEntries.
type RaftEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	TxID    string `json:"tx_id"`
	Type    string `json:"type"`
	Payload []byte `json:"payload,omitempty"`
}

// AppendEntriesRequest is sent by the leader to replicate entries and as heartbeats.
type AppendEntriesRequest struct {
	// Term is the leader's current term.
	Term uint64 `json:"term"`
	// LeaderID identifies the leader so followers can redirect clients.
	LeaderID string `json:"leader_id"`
	// PrevLogIndex is the index of the log entry immediately preceding the new entries.
	PrevLogIndex uint64 `json:"prev_log_index"`
	// PrevLogTerm is the term of PrevLogIndex.
	PrevLogTerm uint64 `json:"prev_log_term"`
	// Entries are the log entries to store (empty for heartbeats).
	Entries []RaftEntry `json:"entries,omitempty"`
	// LeaderCommit is the leader's commitIndex.
	LeaderCommit uint64 `json:"leader_commit"`
}

// AppendEntriesResponse is returned by the follower.
type AppendEntriesResponse struct {
	// Term is the follower's current term; leader steps down if Term > req.Term.
	Term uint64 `json:"term"`
	// Success is true when the follower matched PrevLogIndex and applied the entries.
	Success bool `json:"success"`
	// LastIndex is the follower's last log index after applying entries.
	// Used by the leader to advance nextIndex/matchIndex quickly.
	LastIndex uint64 `json:"last_index"`
	// ConflictTerm is the term of the conflicting entry (on failure), to allow
	// the leader to skip back by term boundary, not one-by-one.
	ConflictTerm uint64 `json:"conflict_term,omitempty"`
	// ConflictIndex is the first index in ConflictTerm (on failure).
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

// ──────────────────────────────────────────────────────────────────────────────
// Transport interface
// ──────────────────────────────────────────────────────────────────────────────

// Transport abstracts the network layer for Raft RPCs.
// An implementation backed by gRPC lives in internal/server/grpc/raft_transport.go.
// An in-memory implementation is used in unit tests.
type Transport interface {
	// RequestVote sends a RequestVote RPC to a peer at the given address.
	RequestVote(ctx context.Context, addr string, req RequestVoteRequest) (RequestVoteResponse, error)
	// AppendEntries sends an AppendEntries RPC (or heartbeat) to a peer.
	AppendEntries(ctx context.Context, addr string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
