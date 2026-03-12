package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	asqlv1 "asql/api/proto/asql/v1"
	"net"
	"strings"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/cluster/heartbeat"
	"asql/internal/cluster/raft"
	"asql/internal/engine/executor"
	"asql/internal/engine/ports"
	"asql/internal/platform/clock"
	"asql/internal/platform/datadir"
	"asql/internal/storage/audit"
	"asql/internal/storage/wal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const defaultLeadershipLeaseTTL = 5 * time.Second

var errLegacyClusterModeDisabled = errors.New("standalone gRPC cluster mode is disabled; use the pgwire + Raft runtime")

type Server struct {
	config           Config
	grpcServer       *grpc.Server
	walStore         *wal.SegmentedLogStore
	auditStore       *audit.Store
	engine           *executor.Engine
	heartbeatLoop    *heartbeat.Loop
	prober           *GRPCPeerProber              // closed in Stop()
	streamReplicator *PersistentStreamReplicator  // started in Run()
}

// NOTE: this standalone gRPC server still contains a legacy/transitional
// heartbeat-led cluster path. It is useful for compatibility and internal
// reuse, but it is not the primary production cluster runtime.
//
// Production clustering is driven by the pgwire server, which boots the Raft
// node and uses this package as a sidecar transport surface.

// ServeOnListener serves the gRPC server on a provided listener.
func (server *Server) ServeOnListener(listener net.Listener) error {
	if listener == nil {
		return errors.New("listener is required")
	}

	return server.grpcServer.Serve(listener)
}

// Stop gracefully stops gRPC and closes WAL resources.
func (server *Server) Stop() {
	if server.prober != nil {
		server.prober.Close()
	}
	if server.grpcServer != nil {
		server.grpcServer.GracefulStop()
	}

	if server.engine != nil {
		server.engine.WaitPendingSnapshots()
	}

	if server.walStore != nil {
		_ = server.walStore.Close()
	}

	if server.auditStore != nil {
		_ = server.auditStore.Close()
	}
}

func New(config Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid grpc server config: %w", err)
	}

	if config.NodeID != "" || len(config.Peers) > 0 || len(config.Groups) > 0 {
		return nil, errLegacyClusterModeDisabled
	}

	dd, err := datadir.New(config.DataDirPath)
	if err != nil {
		return nil, fmt.Errorf("initialize data directory: %w", err)
	}

	walStore, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		return nil, fmt.Errorf("initialize wal store: %w", err)
	}

	auditStore, err := audit.New(dd.AuditBasePath())
	if err != nil {
		_ = walStore.Close()
		return nil, fmt.Errorf("initialize audit store: %w", err)
	}

	engine, err := executor.New(context.Background(), walStore, dd.SnapDir(), executor.WithAuditStore(auditStore))
	if err != nil {
		_ = walStore.Close()
		_ = auditStore.Close()
		return nil, fmt.Errorf("initialize executor engine: %w", err)
	}

	leadershipManager, err := coordinator.NewLeadershipManager(clock.Realtime{}, defaultLeadershipLeaseTTL)
	if err != nil {
		_ = walStore.Close()
		_ = auditStore.Close()
		return nil, fmt.Errorf("initialize leadership manager: %w", err)
	}

	tlsConfig, err := loadServerTLSConfig(config)
	if err != nil {
		_ = walStore.Close()
		_ = auditStore.Close()
		return nil, fmt.Errorf("initialize tls config: %w", err)
	}

	serverOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(unaryAuthInterceptor(config.AuthToken)),
		grpc.StreamInterceptor(streamAuthInterceptor(config.AuthToken)),
		// Allow follower keepalive pings as frequent as every 5 s so the
		// client-side 30 s interval is always accepted.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     10 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 10 * time.Second,
			Time:                  30 * time.Second,
			Timeout:               10 * time.Second,
		}),
	}
	if tlsConfig != nil {
		serverOptions = append(serverOptions, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	grpcServer := grpc.NewServer(serverOptions...)
	registerASQLServiceServer(grpcServer, newService(engine, config.Logger, leadershipManager))
	registerReplicationServiceServer(grpcServer, newReplicationService(walStore, config.Logger))

	srv := &Server{
		config:     config,
		grpcServer: grpcServer,
		walStore:   walStore,
		auditStore: auditStore,
		engine:     engine,
	}

	// Set up heartbeat loop if cluster mode is configured.
	if config.NodeID != "" && len(config.Peers) > 0 {
		peers, parseErr := ParsePeers(config.Peers)
		if parseErr != nil {
			_ = walStore.Close()
			_ = auditStore.Close()
			return nil, fmt.Errorf("parse peers: %w", parseErr)
		}

		failoverCoord, failoverErr := coordinator.NewFailoverCoordinator(leadershipManager)
		if failoverErr != nil {
			_ = walStore.Close()
			_ = auditStore.Close()
			return nil, fmt.Errorf("initialize failover coordinator: %w", failoverErr)
		}

		prober := NewGRPCPeerProber(config.AuthToken, config.PeerProbeTimeout)
		srv.prober = prober
		srv.streamReplicator = NewPersistentStreamReplicator(walStore, config.AuthToken, config.Logger)
		srv.heartbeatLoop = heartbeat.New(
			heartbeat.Config{
				NodeID: config.NodeID,
				Peers:  peers,
				Groups: config.Groups,
			},
			clock.Realtime{},
			leadershipManager,
			failoverCoord,
			prober,
			walStore.LastLSN,
			config.Logger,
		)

		config.Logger.Info("cluster mode enabled",
			"node_id", config.NodeID,
			"peers", len(peers),
			"groups", config.Groups,
		)
	}

	return srv, nil
}

func (server *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", server.config.Address)
	if err != nil {
		return fmt.Errorf("listen %s: %w", server.config.Address, err)
	}

	server.config.Logger.Info("grpc server listening", "address", server.config.Address)

	// Start heartbeat loop and persistent WAL replication goroutine if configured.
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()
	if server.heartbeatLoop != nil {
		go server.heartbeatLoop.Run(heartbeatCtx)
	}
	if server.streamReplicator != nil && server.heartbeatLoop != nil && len(server.config.Groups) > 0 {
		go server.streamReplicator.Run(heartbeatCtx,
			NewHeartbeatLeaderSource(server.heartbeatLoop, server.config.Groups[0]),
			server.config.Groups)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		heartbeatCancel()
		server.grpcServer.GracefulStop()
		if server.engine != nil {
			server.engine.WaitPendingSnapshots()
		}
		if server.walStore != nil {
			_ = server.walStore.Close()
		}
		return nil
	case serveErr := <-errCh:
		heartbeatCancel()
		if serveErr == nil || errors.Is(serveErr, grpc.ErrServerStopped) {
			if server.engine != nil {
				server.engine.WaitPendingSnapshots()
			}
			if server.walStore != nil {
				_ = server.walStore.Close()
			}
			return nil
		}
		if server.engine != nil {
			server.engine.WaitPendingSnapshots()
		}
		if server.walStore != nil {
			_ = server.walStore.Close()
		}
		return fmt.Errorf("grpc serve failed: %w", serveErr)
	}
}

// ClusterLeader is the interface the PersistentStreamReplicator and write-
// routing components use to discover the current cluster leader without
// coupling to a specific consensus implementation (Raft or heartbeat-based).
type ClusterLeader interface {
	// IsLeader reports whether this node is the current cluster leader.
	IsLeader() bool
	// LeaderGRPCAddr returns the gRPC address of the current leader for WAL
	// replication. Returns "" when the leader is unknown or this node is leader.
	LeaderGRPCAddr() string
}

// NewHeartbeatLeaderSource wraps a heartbeat.Loop as a ClusterLeader for
// clusters that are not yet using Raft consensus.
func NewHeartbeatLeaderSource(loop *heartbeat.Loop, primaryGroup string) ClusterLeader {
	return &heartbeatLeaderAdapter{loop: loop, group: primaryGroup}
}

type heartbeatLeaderAdapter struct {
	loop  *heartbeat.Loop
	group string
}

func (h *heartbeatLeaderAdapter) IsLeader() bool {
	return h.loop.Role(h.group) == "leader"
}

func (h *heartbeatLeaderAdapter) LeaderGRPCAddr() string {
	return h.loop.LeaderAddress(h.group)
}

// PersistentStreamReplicator maintains a continuous WAL replication stream
// from the current cluster leader. It runs as a long-lived goroutine, opens
// a single streaming connection per leader, and reconnects with exponential
// backoff when the stream breaks or the leader changes.
//
// Replication latency is bounded by the server-side poll interval (20 ms)
// rather than the heartbeat tick interval (2 s).
type PersistentStreamReplicator struct {
	walStore  *wal.SegmentedLogStore
	authToken string
	logger    *slog.Logger
}

// NewPersistentStreamReplicator creates a PersistentStreamReplicator backed by
// the provided WAL store. authToken is forwarded as a bearer credential to the
// leader's gRPC replication endpoint.
func NewPersistentStreamReplicator(walStore *wal.SegmentedLogStore, authToken string, logger *slog.Logger) *PersistentStreamReplicator {
	return &PersistentStreamReplicator{walStore: walStore, authToken: authToken, logger: logger}
}

// Run loops forever, maintaining a streaming connection to the leader.
// It exits when ctx is cancelled. Call as a goroutine.
// leader is the ClusterLeader source; pass a *raft.RaftNode when Raft is
// active or NewHeartbeatLeaderSource(loop, group) for legacy heartbeat mode.
func (r *PersistentStreamReplicator) Run(ctx context.Context, leader ClusterLeader, groups []string) {
	if len(groups) == 0 {
		return
	}

	const (
		baseBackoff = 100 * time.Millisecond
		maxBackoff  = 30 * time.Second
		idlePoll    = 500 * time.Millisecond
	)
	backoff := baseBackoff

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Leaders do not replicate from themselves.
		if leader.IsLeader() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(idlePoll):
			}
			backoff = baseBackoff
			continue
		}

		leaderAddr := leader.LeaderGRPCAddr()
		if leaderAddr == "" {
			select {
			case <-ctx.Done():
				return
			case <-time.After(idlePoll):
			}
			continue
		}

		if err := r.stream(ctx, leaderAddr); err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Warn("replication.stream_lost",
				"leader", leaderAddr, "error", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}
		backoff = baseBackoff
	}
}

// stream opens a single persistent StreamWAL RPC to leaderAddr and applies
// records until the context is cancelled or the stream breaks.
func (r *PersistentStreamReplicator) stream(ctx context.Context, leaderAddr string) error {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
		// Send keepalive pings every 30 s; the server enforcement policy
		// accepts pings >= 5 s, so we stay well clear of ENHANCE_YOUR_CALM.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: false,
		}),
	}
	if r.authToken != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(tokenCredentials{token: r.authToken}))
	}
	//nolint:staticcheck
	conn, err := grpc.Dial(leaderAddr, opts...)
	if err != nil {
		return fmt.Errorf("dial leader %s: %w", leaderAddr, err)
	}
	defer conn.Close()

	// Limit=0 signals persistent streaming mode on the server side.
	fromLSN := r.walStore.LastLSN() + 1
	grpcStream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true},
		"/asql.v1.ReplicationService/StreamWAL",
		grpc.ForceCodec(jsonCodec{}))
	if err != nil {
		return fmt.Errorf("open replication stream: %w", err)
	}
	if err := grpcStream.SendMsg(&StreamWALRequest{FromLSN: fromLSN, Limit: 0}); err != nil {
		return fmt.Errorf("send replication request: %w", err)
	}
	if err := grpcStream.CloseSend(); err != nil {
		return fmt.Errorf("close send half: %w", err)
	}

	for {
		var resp StreamWALResponse
		if err := grpcStream.RecvMsg(&resp); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if err == io.EOF {
				return fmt.Errorf("leader closed stream")
			}
			return fmt.Errorf("receive: %w", err)
		}
		expected := r.walStore.LastLSN() + 1
		if resp.LSN != expected {
			return fmt.Errorf("replication divergence: got lsn=%d expected=%d", resp.LSN, expected)
		}
		if err := r.walStore.AppendReplicated(ctx, ports.WALRecord{
			LSN:       resp.LSN,
			Term:      resp.Term,
			TxID:      resp.TxID,
			Type:      resp.Type,
			Timestamp: resp.Timestamp,
			Payload:   resp.Payload,
		}); err != nil {
			return fmt.Errorf("apply replicated record lsn=%d: %w", resp.LSN, err)
		}
	}
}

// NewClusterGRPCServer creates a gRPC server with ASQL service and replication
// service handlers registered, using pre-built shared resources (walStore,
// engine, leadershipManager). The returned server is ready to Serve a
// net.Listener; the caller is responsible for GracefulStop().
//
// loop may be nil (standalone mode); when non-nil, the JoinCluster RPC will
// accept hot-join requests from new nodes at runtime.
//
// raftNode is optional; when non-nil, the Raft gRPC service is registered so
// peers can reach this node with RequestVote and AppendEntries RPCs.
//
// This is used by the pgwire server to expose cluster endpoints on a separate
// TCP port without duplicating resource creation logic.
func NewClusterGRPCServer(
	walStore *wal.SegmentedLogStore,
	engine *executor.Engine,
	leadershipManager *coordinator.LeadershipManager,
	localNodeID string,
	authToken string,
	logger *slog.Logger,
	loop *heartbeat.Loop,
	raftNode *raft.RaftNode,
) *grpc.Server {
	serverOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(unaryAuthInterceptor(authToken)),
		grpc.StreamInterceptor(streamAuthInterceptor(authToken)),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     10 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 10 * time.Second,
			Time:                  30 * time.Second,
			Timeout:               10 * time.Second,
		}),
	}
	srv := grpc.NewServer(serverOptions...)
	svc := newService(engine, logger, leadershipManager)
	svc.authority = newRaftClusterAuthority(localNodeID, raftNode, leadershipManager)
	if loop != nil {
		svc.peerRegistry = loop
	}
	registerASQLServiceServer(srv, svc)
	registerReplicationServiceServer(srv, newReplicationService(walStore, logger))
	if raftNode != nil {
		asqlv1.RegisterRaftServiceServer(srv, newRaftServiceHandler(raftNode, logger))
	}
	return srv
}

// ParsePeers converts "nodeID@host:port" strings into heartbeat.Peer values.
func ParsePeers(raw []string) ([]heartbeat.Peer, error) {
	peers := make([]heartbeat.Peer, 0, len(raw))
	for _, entry := range raw {
		parts := strings.SplitN(entry, "@", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid peer format %q: expected nodeID@host:port", entry)
		}
		peers = append(peers, heartbeat.Peer{
			NodeID:  parts[0],
			Address: parts[1],
		})
	}
	return peers, nil
}
