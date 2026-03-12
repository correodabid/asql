// Package raft implements a single-group Raft consensus algorithm for ASQL.
//
// Design invariants:
//   - A single RaftNode drives one consensus group (one WAL domain).
//   - Log index = WAL LSN — no separate Raft log file; Raft entries ARE WAL records.
//   - Only RequestVote and AppendEntries RPCs are implemented (no cluster membership changes in v1).
//   - Election timeout: random 150–300 ms.  Heartbeat: 50 ms.
//   - All wall-clock usage is injected through ports.Clock for deterministic testing.
package raft

import "errors"

var (
	// ErrNotLeader is returned when a write operation is attempted on a follower.
	ErrNotLeader = errors.New("raft: not leader")

	// ErrStaleTerm is returned when an incoming RPC carries a smaller term than our current term.
	ErrStaleTerm = errors.New("raft: stale term")

	// ErrLogMismatch is returned when AppendEntries prevLogIndex/prevLogTerm do not match.
	ErrLogMismatch = errors.New("raft: log mismatch")

	// ErrAlreadyVoted is returned when a vote request for the same term is rejected
	// because we already voted for a different candidate.
	ErrAlreadyVoted = errors.New("raft: already voted for another candidate")

	// ErrCandidateLogBehind is returned when a vote is denied because the candidate's
	// log is less up-to-date than ours (§5.4.1).
	ErrCandidateLogBehind = errors.New("raft: candidate log is behind")

	// ErrQuorumUnreachable is returned when a leader cannot contact a quorum of peers.
	ErrQuorumUnreachable = errors.New("raft: quorum unreachable")

	// ErrIndexOutOfRange is returned when a log index is beyond the known last index.
	ErrIndexOutOfRange = errors.New("raft: log index out of range")
)
