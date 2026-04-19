package grpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	asqlv1 "github.com/correodabid/asql/api/proto/asql/v1"
	"github.com/correodabid/asql/internal/cluster/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// raftServiceHandler implements the generated protobuf Raft service by delegating
// to a *raft.RaftNode. It translates between protobuf wire types and raft
// package types.
type raftServiceHandler struct {
	asqlv1.UnimplementedRaftServiceServer
	node   *raft.RaftNode
	logger *slog.Logger
}

var appendEntriesHandlerPerf struct {
	mu           sync.Mutex
	samples      int64
	totalEntries int64
	totalConvert time.Duration
	totalHandle  time.Duration
	totalTotal   time.Duration
	maxTotal     time.Duration
}

// newRaftServiceHandler creates a RaftServiceServer backed by node.
func newRaftServiceHandler(node *raft.RaftNode, logger *slog.Logger) asqlv1.RaftServiceServer {
	return &raftServiceHandler{node: node, logger: logger}
}

// RequestVote handles an incoming RequestVote RPC from a candidate peer.
func (h *raftServiceHandler) RequestVote(ctx context.Context, req *asqlv1.RaftRequestVoteRequest) (*asqlv1.RaftRequestVoteResponse, error) {
	resp, err := h.node.HandleRequestVote(ctx, raft.RequestVoteRequest{
		Term:         req.GetTerm(),
		CandidateID:  req.GetCandidateId(),
		LastLogIndex: req.GetLastLogIndex(),
		LastLogTerm:  req.GetLastLogTerm(),
	})
	if err != nil {
		h.logger.Error("raft.handler.request_vote_error", slog.String("err", err.Error()))
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &asqlv1.RaftRequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

// AppendEntries handles an incoming AppendEntries RPC (or heartbeat) from the leader.
func (h *raftServiceHandler) AppendEntries(ctx context.Context, req *asqlv1.RaftAppendEntriesRequest) (*asqlv1.RaftAppendEntriesResponse, error) {
	started := time.Now()
	convertStart := time.Now()
	entries := make([]raft.RaftEntry, len(req.GetEntries()))
	for i, e := range req.GetEntries() {
		entries[i] = raft.RaftEntry{
			Index:   e.GetIndex(),
			Term:    e.GetTerm(),
			TxID:    e.GetTxId(),
			Type:    e.GetType(),
			Payload: e.GetPayload(),
		}
	}
	convertDur := time.Since(convertStart)
	handleStart := time.Now()
	resp, err := h.node.HandleAppendEntries(ctx, raft.AppendEntriesRequest{
		Term:         req.GetTerm(),
		LeaderID:     req.GetLeaderId(),
		PrevLogIndex: req.GetPrevLogIndex(),
		PrevLogTerm:  req.GetPrevLogTerm(),
		Entries:      entries,
		LeaderCommit: req.GetLeaderCommit(),
	})
	handleDur := time.Since(handleStart)
	recordAppendEntriesHandlerPerf(h.logger, len(entries), convertDur, handleDur, time.Since(started))
	if err != nil {
		h.logger.Error("raft.handler.append_entries_error", slog.String("err", err.Error()))
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &asqlv1.RaftAppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		LastIndex:     resp.LastIndex,
		ConflictTerm:  resp.ConflictTerm,
		ConflictIndex: resp.ConflictIndex,
	}, nil
}

func recordAppendEntriesHandlerPerf(logger *slog.Logger, entries int, convertDur, handleDur, totalDur time.Duration) {
	const appendEntriesSummaryEvery = 1000
	const appendEntriesSlowThreshold = 50 * time.Millisecond

	appendEntriesHandlerPerf.mu.Lock()
	defer appendEntriesHandlerPerf.mu.Unlock()

	p := &appendEntriesHandlerPerf
	p.samples++
	p.totalEntries += int64(entries)
	p.totalConvert += convertDur
	p.totalHandle += handleDur
	p.totalTotal += totalDur
	if totalDur > p.maxTotal {
		p.maxTotal = totalDur
	}

	if totalDur >= appendEntriesSlowThreshold {
		logger.Info("raft.handler.append_entries.slow",
			slog.Int("entries", entries),
			slog.Duration("convert", convertDur),
			slog.Duration("handle", handleDur),
			slog.Duration("total", totalDur),
		)
	}

	if p.samples%appendEntriesSummaryEvery == 0 {
		logger.Info("raft.handler.append_entries.summary",
			slog.Int64("samples", p.samples),
			slog.Int64("avg_entries", p.totalEntries/p.samples),
			slog.Duration("avg_convert", time.Duration(int64(p.totalConvert)/p.samples)),
			slog.Duration("avg_handle", time.Duration(int64(p.totalHandle)/p.samples)),
			slog.Duration("avg_total", time.Duration(int64(p.totalTotal)/p.samples)),
			slog.Duration("max_total", p.maxTotal),
		)
	}
}
