package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	asqlv1 "asql/api/proto/asql/v1"
	"asql/internal/cluster/raft"

	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCRaftTransport implements raft.Transport using gRPC connections.
// It maintains a pool of connections, one per peer address, and reuses them
// across repeated RequestVote and AppendEntries calls.
//
// Connection lifecycle: connections are established lazily on first use and
// remain open until Close() is called.
type GRPCRaftTransport struct {
	mu    sync.Mutex
	conns map[string]*grpcgo.ClientConn // addr → conn
}

var appendEntriesTransportPerf struct {
	mu           sync.Mutex
	samples      int64
	totalEntries int64
	totalGetConn time.Duration
	totalInvoke  time.Duration
	totalTotal   time.Duration
	maxTotal     time.Duration
	maxInvoke    time.Duration
}

// NewGRPCRaftTransport creates a Transport that sends Raft RPCs over gRPC.
func NewGRPCRaftTransport() *GRPCRaftTransport {
	return &GRPCRaftTransport{
		conns: make(map[string]*grpcgo.ClientConn),
	}
}

// Close releases all cached gRPC connections.
func (t *GRPCRaftTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		_ = conn.Close()
	}
	t.conns = make(map[string]*grpcgo.ClientConn)
}

// RequestVote sends a RequestVote RPC to the peer at addr.
func (t *GRPCRaftTransport) RequestVote(ctx context.Context, addr string, req raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	conn, err := t.getConn(addr)
	if err != nil {
		return raft.RequestVoteResponse{}, fmt.Errorf("raft transport: dial %s: %w", addr, err)
	}

	client := asqlv1.NewRaftServiceClient(conn)
	wireReq := &asqlv1.RaftRequestVoteRequest{
		Term:         req.Term,
		CandidateId:  req.CandidateID,
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
	}
	wireResp, err := client.RequestVote(ctx, wireReq)
	if err != nil {
		return raft.RequestVoteResponse{}, fmt.Errorf("raft transport: RequestVote %s: %w", addr, err)
	}

	return raft.RequestVoteResponse{
		Term:        wireResp.GetTerm(),
		VoteGranted: wireResp.GetVoteGranted(),
	}, nil
}

// AppendEntries sends an AppendEntries RPC to the peer at addr.
func (t *GRPCRaftTransport) AppendEntries(ctx context.Context, addr string, req raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	started := time.Now()
	getConnStart := time.Now()
	conn, err := t.getConn(addr)
	getConnDur := time.Since(getConnStart)
	if err != nil {
		return raft.AppendEntriesResponse{}, fmt.Errorf("raft transport: dial %s: %w", addr, err)
	}

	client := asqlv1.NewRaftServiceClient(conn)
	wireEntries := make([]*asqlv1.RaftEntry, len(req.Entries))
	for i, e := range req.Entries {
		wireEntries[i] = &asqlv1.RaftEntry{
			Index:   e.Index,
			Term:    e.Term,
			TxId:    e.TxID,
			Type:    e.Type,
			Payload: e.Payload,
		}
	}
	wireReq := &asqlv1.RaftAppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     req.LeaderID,
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      wireEntries,
		LeaderCommit: req.LeaderCommit,
	}
	invokeStart := time.Now()
	wireResp, err := client.AppendEntries(ctx, wireReq)
	if err != nil {
		recordAppendEntriesTransportPerf(len(req.Entries), getConnDur, time.Since(invokeStart), time.Since(started))
		return raft.AppendEntriesResponse{}, fmt.Errorf("raft transport: AppendEntries %s: %w", addr, err)
	}
	recordAppendEntriesTransportPerf(len(req.Entries), getConnDur, time.Since(invokeStart), time.Since(started))

	return raft.AppendEntriesResponse{
		Term:          wireResp.GetTerm(),
		Success:       wireResp.GetSuccess(),
		LastIndex:     wireResp.GetLastIndex(),
		ConflictTerm:  wireResp.GetConflictTerm(),
		ConflictIndex: wireResp.GetConflictIndex(),
	}, nil
}
func recordAppendEntriesTransportPerf(entries int, getConnDur, invokeDur, totalDur time.Duration) {
	appendEntriesTransportPerf.mu.Lock()
	defer appendEntriesTransportPerf.mu.Unlock()

	p := &appendEntriesTransportPerf
	p.samples++
	p.totalEntries += int64(entries)
	p.totalGetConn += getConnDur
	p.totalInvoke += invokeDur
	p.totalTotal += totalDur
	if totalDur > p.maxTotal {
		p.maxTotal = totalDur
	}
	if invokeDur > p.maxInvoke {
		p.maxInvoke = invokeDur
	}

	if totalDur >= 25*time.Millisecond {
		slog.Info("raft.transport.append_entries.slow",
			slog.Int("entries", entries),
			slog.Duration("get_conn", getConnDur),
			slog.Duration("invoke", invokeDur),
			slog.Duration("total", totalDur),
		)
	}

	if p.samples%25 == 0 {
		slog.Info("raft.transport.append_entries.summary",
			slog.Int64("samples", p.samples),
			slog.Int64("avg_entries", p.totalEntries/p.samples),
			slog.Duration("avg_get_conn", time.Duration(int64(p.totalGetConn)/p.samples)),
			slog.Duration("avg_invoke", time.Duration(int64(p.totalInvoke)/p.samples)),
			slog.Duration("avg_total", time.Duration(int64(p.totalTotal)/p.samples)),
			slog.Duration("max_total", p.maxTotal),
			slog.Duration("max_invoke", p.maxInvoke),
		)
	}
}

// getConn returns a cached connection to addr, creating one if needed.
func (t *GRPCRaftTransport) getConn(addr string) (*grpcgo.ClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if conn, ok := t.conns[addr]; ok {
		return conn, nil
	}
	conn, err := grpcgo.NewClient(addr,
		grpcgo.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	t.conns[addr] = conn
	return conn, nil
}
