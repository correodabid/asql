package pgwire

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/correodabid/asql/internal/cluster/coordinator"
	"github.com/correodabid/asql/internal/cluster/heartbeat"
	"github.com/correodabid/asql/internal/cluster/raft"
	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/platform/clock"
	"github.com/correodabid/asql/internal/platform/datadir"
	grpcserver "github.com/correodabid/asql/internal/server/grpc"
	"github.com/correodabid/asql/internal/storage/audit"
	"github.com/correodabid/asql/internal/storage/wal"
	api "github.com/correodabid/asql/pkg/adminapi"

	"github.com/jackc/pgx/v5/pgproto3"
)

const maxLSN uint64 = ^uint64(0)

const maxRecentSecurityAuditEvents = 128

var explainResultColumns = []string{"operation", "domain", "table", "plan_shape", "access_plan"}

func stripExplainSQLPrefix(sql string) (string, bool) {
	trimmed := strings.TrimSpace(sql)
	found := false
	for len(trimmed) >= len("EXPLAIN") && strings.EqualFold(trimmed[:len("EXPLAIN")], "EXPLAIN") {
		if len(trimmed) > len("EXPLAIN") {
			next := trimmed[len("EXPLAIN")]
			if next != ' ' && next != '\t' && next != '\n' && next != '\r' {
				break
			}
		}
		found = true
		trimmed = strings.TrimSpace(trimmed[len("EXPLAIN"):])
	}
	return trimmed, found
}

func (server *Server) auditSuccess(operation string, attrs ...slog.Attr) {
	if server == nil || server.config.Logger == nil {
		return
	}
	server.appendSecurityAuditEvent(operation, "success", "", attrs...)
	args := make([]any, 0, 3+len(attrs))
	args = append(args,
		slog.String("event", "audit"),
		slog.String("status", "success"),
		slog.String("operation", operation),
	)
	for _, attr := range attrs {
		args = append(args, attr)
	}
	server.config.Logger.Info("audit_event", args...)
}

func (server *Server) auditFailure(operation, reason string, attrs ...slog.Attr) {
	if server == nil || server.config.Logger == nil {
		return
	}
	server.appendSecurityAuditEvent(operation, "failure", reason, attrs...)
	args := make([]any, 0, 4+len(attrs))
	args = append(args,
		slog.String("event", "audit"),
		slog.String("status", "failure"),
		slog.String("operation", operation),
		slog.String("reason", reason),
	)
	for _, attr := range attrs {
		args = append(args, attr)
	}
	server.config.Logger.Warn("audit_event", args...)
}

func normalizeAuditPrincipal(user string) string {
	return strings.ToLower(strings.TrimSpace(user))
}

type historicalReadAuditDetail struct {
	queryKind             string
	targetKind            string
	targetLSN             uint64
	targetTimestampMicros uint64
}

func principalPrivilegeStrings(privileges []executor.PrincipalPrivilege) []string {
	if len(privileges) == 0 {
		return nil
	}
	result := make([]string, 0, len(privileges))
	for _, privilege := range privileges {
		result = append(result, string(privilege))
	}
	return result
}

func principalHasPrivilege(privileges []executor.PrincipalPrivilege, target executor.PrincipalPrivilege) bool {
	for _, privilege := range privileges {
		if privilege == target {
			return true
		}
	}
	return false
}

func (server *Server) historicalReadAuditAttrs(principal string, detail historicalReadAuditDetail) []slog.Attr {
	attrs := []slog.Attr{
		slog.String("principal", normalizeAuditPrincipal(principal)),
		slog.String("query_kind", detail.queryKind),
		slog.String("privilege", string(executor.PrincipalPrivilegeSelectHistory)),
		slog.String("grant_state_scope", "current"),
		slog.String("historical_target_kind", detail.targetKind),
	}
	switch detail.targetKind {
	case "lsn":
		attrs = append(attrs, slog.Uint64("historical_target_lsn", detail.targetLSN))
	case "timestamp":
		attrs = append(attrs,
			slog.Uint64("historical_target_timestamp_micros", detail.targetTimestampMicros),
			slog.String("historical_target_timestamp_utc", time.UnixMicro(int64(detail.targetTimestampMicros)).UTC().Format(time.RFC3339Nano)),
		)
	}
	if !server.engine.HasPrincipalCatalog() {
		return attrs
	}
	info, ok := server.engine.Principal(principal)
	attrs = append(attrs, slog.Bool("principal_catalog_entry", ok))
	if !ok {
		return attrs
	}
	attrs = append(attrs,
		slog.String("principal_kind", string(info.Kind)),
		slog.Bool("principal_enabled", info.Enabled),
		slog.Any("principal_direct_roles", append([]string(nil), info.Roles...)),
		slog.Any("principal_effective_roles", append([]string(nil), info.EffectiveRoles...)),
		slog.Any("principal_direct_privileges", principalPrivilegeStrings(info.Privileges)),
		slog.Any("principal_effective_privileges", principalPrivilegeStrings(info.EffectivePrivileges)),
		slog.Bool("principal_has_select_history", principalHasPrivilege(info.EffectivePrivileges, executor.PrincipalPrivilegeSelectHistory)),
	)
	return attrs
}

// raftCommitterAdapter bridges *raft.RaftNode to the ports.RaftCommitter
// interface expected by the executor.  When the node is not the leader, Apply
// returns raft.ErrNotLeader, which propagates as a commit error and causes the
// pgwire handler to return a serialisation-failure code to the client.
type raftCommitterAdapter struct{ node *raft.RaftNode }

func (a raftCommitterAdapter) Apply(ctx context.Context, typ, txID string, payload []byte) (uint64, error) {
	entry, err := a.node.Apply(ctx, typ, txID, payload)
	if err != nil {
		return 0, err
	}
	return entry.Index, nil
}

func (a raftCommitterAdapter) ApplyBatch(ctx context.Context, records []ports.RaftRecord) ([]uint64, error) {
	batch := make([]raft.BatchRecord, len(records))
	for i, r := range records {
		batch[i] = raft.BatchRecord{Type: r.Type, TxID: r.TxID, Payload: r.Payload}
	}
	entries, err := a.node.ApplyBatch(ctx, batch)
	if err != nil {
		return nil, err
	}
	lsns := make([]uint64, len(entries))
	for i, e := range entries {
		lsns[i] = e.Index
	}
	return lsns, nil
}

// clusterSidecar is a minimal interface covering the gRPC server methods used
// by the pgwire server to manage the production cluster communication sidecar.
// In production, clustering is driven by the pgwire + Raft runtime.
type clusterSidecar interface {
	Serve(net.Listener) error
	GracefulStop()
}

type backendCancelKey struct {
	processID uint32
	secretKey uint32
}

type Server struct {
	config     Config
	engine     *executor.Engine
	walStore   *wal.SegmentedLogStore
	auditStore *audit.Store
	leadership *coordinator.LeadershipManager

	// cluster machinery — nil when operating in standalone mode. When set, this
	// server is running the production cluster path (pgwire + Raft).
	sidecar          clusterSidecar
	heartbeatLoop    *heartbeat.Loop
	raftNode         *raft.RaftNode
	raftTransport    *grpcserver.GRPCRaftTransport
	prober           *grpcserver.GRPCPeerProber
	streamReplicator *grpcserver.PersistentStreamReplicator
	clusterCancel    context.CancelFunc // cancels heartbeat + replicator goroutines

	listener            net.Listener
	adminMu             sync.RWMutex // guards adminListener and adminServer
	adminListener       net.Listener
	adminServer         *http.Server
	metrics             *runtimeMetrics
	securityAuditMu     sync.Mutex
	securityAuditEvents []api.SecurityAuditEvent
	closeCh             chan struct{}
	closeMux            sync.Once
	waitConn            sync.WaitGroup
	cancelMu            sync.Mutex
	cancelTargets       map[backendCancelKey]*connState
	nextBackendID       uint32
}

func slogValueToAny(value slog.Value) any {
	value = value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		return value.String()
	case slog.KindInt64:
		return value.Int64()
	case slog.KindUint64:
		return value.Uint64()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindBool:
		return value.Bool()
	case slog.KindDuration:
		return value.Duration().String()
	case slog.KindTime:
		return value.Time().UTC().Format(time.RFC3339Nano)
	case slog.KindGroup:
		group := value.Group()
		if len(group) == 0 {
			return map[string]any{}
		}
		result := make(map[string]any, len(group))
		for _, attr := range group {
			result[attr.Key] = slogValueToAny(attr.Value)
		}
		return result
	case slog.KindAny:
		return value.Any()
	default:
		return value.Any()
	}
}

func (server *Server) appendSecurityAuditEvent(operation, status, reason string, attrs ...slog.Attr) {
	if server == nil {
		return
	}
	event := api.SecurityAuditEvent{
		TimestampUTC: time.Now().UTC().Format(time.RFC3339Nano),
		Operation:    operation,
		Status:       status,
		Reason:       reason,
	}
	if len(attrs) > 0 {
		event.Attributes = make(map[string]any, len(attrs))
		for _, attr := range attrs {
			event.Attributes[attr.Key] = slogValueToAny(attr.Value)
		}
	}
	server.securityAuditMu.Lock()
	defer server.securityAuditMu.Unlock()
	server.securityAuditEvents = append(server.securityAuditEvents, event)
	if len(server.securityAuditEvents) > maxRecentSecurityAuditEvents {
		server.securityAuditEvents = append([]api.SecurityAuditEvent(nil), server.securityAuditEvents[len(server.securityAuditEvents)-maxRecentSecurityAuditEvents:]...)
	}
}

func (server *Server) RecentSecurityAuditEvents(limit int) []api.SecurityAuditEvent {
	if server == nil {
		return nil
	}
	server.securityAuditMu.Lock()
	defer server.securityAuditMu.Unlock()
	if len(server.securityAuditEvents) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(server.securityAuditEvents) {
		limit = len(server.securityAuditEvents)
	}
	start := len(server.securityAuditEvents) - limit
	result := make([]api.SecurityAuditEvent, limit)
	copy(result, server.securityAuditEvents[start:])
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

func New(config Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pgwire config: %w", err)
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

	const defaultLeadershipLeaseTTL = 5 * time.Second
	leadershipManager, err := coordinator.NewLeadershipManager(clock.Realtime{}, defaultLeadershipLeaseTTL)
	if err != nil {
		_ = walStore.Close()
		_ = auditStore.Close()
		return nil, fmt.Errorf("initialize leadership manager: %w", err)
	}

	srv := &Server{
		config:        config,
		engine:        engine,
		walStore:      walStore,
		auditStore:    auditStore,
		leadership:    leadershipManager,
		metrics:       newRuntimeMetrics(config.NodeID),
		closeCh:       make(chan struct{}),
		cancelTargets: make(map[backendCancelKey]*connState),
	}

	// Wire full cluster machinery when NodeID + ClusterGRPCAddr are set and
	// either static peers OR a join address is provided.
	if config.NodeID != "" && config.ClusterGRPCAddr != "" && (len(config.Peers) > 0 || config.JoinAddr != "") {
		peers, parseErr := grpcserver.ParsePeers(config.Peers)
		if parseErr != nil {
			_ = walStore.Close()
			_ = auditStore.Close()
			return nil, fmt.Errorf("parse cluster peers: %w", parseErr)
		}

		failoverCoord, failoverErr := coordinator.NewFailoverCoordinatorWithObserver(leadershipManager, srv.metrics)
		if failoverErr != nil {
			_ = walStore.Close()
			_ = auditStore.Close()
			return nil, fmt.Errorf("initialize failover coordinator: %w", failoverErr)
		}

		prober := grpcserver.NewGRPCPeerProber(config.AuthToken, config.PeerProbeTimeout)
		replicator := grpcserver.NewPersistentStreamReplicator(walStore, config.AuthToken, config.Logger)
		loop := heartbeat.New(
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

		// ── Raft consensus node ──────────────────────────────────────────────
		raftTransport := grpcserver.NewGRPCRaftTransport()
		raftPeers := make([]raft.Peer, len(peers))
		for i, p := range peers {
			raftPeers[i] = raft.Peer{NodeID: p.NodeID, RaftAddr: p.Address}
		}
		raftLog, raftLogErr := raft.NewWALLog(context.Background(), walStore)
		if raftLogErr != nil {
			_ = walStore.Close()
			_ = auditStore.Close()
			return nil, fmt.Errorf("initialize raft log: %w", raftLogErr)
		}
		commitCatchUp := newCommitCatchUpCoalescer(config.Logger, engine.ApplyCommittedRecords)
		raftStorage := raft.NewFileStorage(dd.MetaDir() + "/raft-state.json")
		raftNode, raftNodeErr := raft.NewRaftNode(context.Background(), raft.Config{
			NodeID:    config.NodeID,
			Peers:     raftPeers,
			Storage:   raftStorage,
			Log:       raftLog,
			Transport: raftTransport,
			Clock:     clock.Realtime{},
			OnEntriesCommitted: func(ctx context.Context, commitIndex uint64, records []ports.WALRecord) {
				commitCatchUp.handle(ctx, commitIndex, records)
			},
			Logger: config.Logger,
		})
		if raftNodeErr != nil {
			_ = walStore.Close()
			_ = auditStore.Close()
			return nil, fmt.Errorf("initialize raft node: %w", raftNodeErr)
		}

		srv.sidecar = grpcserver.NewClusterGRPCServer(walStore, engine, leadershipManager, config.NodeID, config.AuthToken, config.Logger, loop, raftNode)
		// Wire Raft committer: every commit in the engine now blocks until quorum.
		engine.SetRaftCommitter(raftCommitterAdapter{node: raftNode})
		srv.heartbeatLoop = loop
		// Self-register this node's pgwire address in the heartbeat peer list so
		// gossip propagates it to all cluster members.  Without this, static
		// peers (configured via -peers) never learn each other's pgwire address
		// and SHOW asql_cluster_leader returns "" on followers.
		loop.AddPeer(heartbeat.Peer{
			NodeID:        config.NodeID,
			Address:       config.ClusterGRPCAddr,
			PgwireAddress: normalizeListenAddr(config.Address),
		})
		// Delegate heartbeat loop role determination to Raft.  This ensures
		// followers always run tickFollower (receiving gossip with peer pgwire
		// addresses) regardless of what the heartbeat lease table says.
		raftNodeRef := raftNode
		loop.SetRaftRoleSource(func() string {
			if raftNodeRef.IsLeader() {
				return "leader"
			}
			return "follower"
		})
		srv.raftNode = raftNode
		srv.raftTransport = raftTransport
		srv.prober = prober
		srv.streamReplicator = replicator

		config.Logger.Info("cluster mode enabled",
			"node_id", config.NodeID,
			"grpc_addr", config.ClusterGRPCAddr,
			"peers", len(peers),
			"groups", config.Groups,
		)
	} else if config.NodeID != "" {
		config.Logger.Warn("pgwire: NodeID set but ClusterGRPCAddr or Peers missing — running standalone",
			"node_id", config.NodeID,
			"cluster_grpc_addr", config.ClusterGRPCAddr,
			"peers", len(config.Peers),
		)
	}

	return srv, nil
}

func (server *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", server.config.Address)
	if err != nil {
		return fmt.Errorf("listen %s: %w", server.config.Address, err)
	}
	return server.ServeOnListener(ctx, listener)
}

func (server *Server) ServeOnListener(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return errors.New("listener is required")
	}
	server.listener = listener

	server.config.Logger.Info("pgwire server listening", "address", listener.Addr().String())
	if err := server.startAdminHTTP(); err != nil {
		return err
	}

	// Start cluster goroutines when in cluster mode.
	//
	// IMPORTANT: the gRPC sidecar listener MUST be started BEFORE the Raft
	// node so that peers can reach us for RequestVote/AppendEntries RPCs
	// during the very first election.  If the Raft tick loop starts first,
	// it fires an election (~200ms) while peer gRPC ports are still closed,
	// causing every vote request to get "connection refused".  All three
	// nodes then keep incrementing terms in a loop without ever reaching
	// quorum (= the "Waiting for cluster quorum... timeout" bug).
	if server.heartbeatLoop != nil {
		clusterCtx, cancel := context.WithCancel(ctx)
		server.clusterCancel = cancel

		// ── Step 1: bring up the gRPC sidecar so peers can reach us. ──
		if server.sidecar != nil {
			clusterListener, listenErr := net.Listen("tcp", server.config.ClusterGRPCAddr)
			if listenErr != nil {
				cancel()
				return fmt.Errorf("cluster grpc listen %s: %w", server.config.ClusterGRPCAddr, listenErr)
			}
			go func() {
				if err := server.sidecar.Serve(clusterListener); err != nil {
					server.config.Logger.Warn("cluster grpc sidecar exited", "error", err)
				}
			}()
			server.config.Logger.Info("cluster grpc sidecar listening", "address", server.config.ClusterGRPCAddr)
		}

		// ── Step 2: start heartbeat + Raft now that the RPC port is open. ──
		go server.heartbeatLoop.Run(clusterCtx)
		if server.raftNode != nil {
			go func() {
				if err := server.raftNode.Run(clusterCtx); err != nil && clusterCtx.Err() == nil {
					server.config.Logger.Warn("raft node exited", "error", err)
				}
			}()
		}
		if server.streamReplicator != nil {
			if server.raftNode != nil {
				// Raft handles all WAL replication via AppendEntries; the
				// persistent stream replicator would race with Raft's
				// AppendFollower and cause "out-of-order replicated lsn" errors.
				server.config.Logger.Info("stream replicator skipped (Raft active)")
			} else if len(server.config.Groups) > 0 {
				// Fallback: heartbeat-based leader detection (no Raft).
				go server.streamReplicator.Run(clusterCtx,
					grpcserver.NewHeartbeatLeaderSource(server.heartbeatLoop, server.config.Groups[0]),
					server.config.Groups)
			}
		}

		// ── Step 3: hot join after everything is running. ──
		if server.sidecar != nil && server.config.JoinAddr != "" {
			go server.runJoin(clusterCtx)
		}
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.serve(listener)
	}()

	select {
	case <-ctx.Done():
		server.Stop()
		return nil
	case err := <-errCh:
		server.Stop()
		if err == nil || errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	}
}

func (server *Server) Stop() {
	server.closeMux.Do(func() {
		close(server.closeCh)
		server.adminMu.RLock()
		adminServer := server.adminServer
		adminListener := server.adminListener
		server.adminMu.RUnlock()
		if adminServer != nil {
			_ = adminServer.Close()
		}
		if adminListener != nil {
			_ = adminListener.Close()
		}
		if server.listener != nil {
			_ = server.listener.Close()
		}
		// Stop cluster goroutines before closing storage.
		if server.clusterCancel != nil {
			server.clusterCancel()
		}
		if server.prober != nil {
			server.prober.Close()
		}
		if server.raftTransport != nil {
			server.raftTransport.Close()
		}
		if server.sidecar != nil {
			server.sidecar.GracefulStop()
		}
		server.waitConn.Wait()
		// Flush the final in-memory snapshot to disk and wait for any
		// in-flight async checkpoint goroutines before closing the WAL.
		// Without this, every graceful restart replays the full WAL delta.
		server.engine.WaitPendingSnapshots()
		if server.auditStore != nil {
			_ = server.auditStore.Close()
		}
		if server.walStore != nil {
			_ = server.walStore.Close()
		}
	})
}

// runJoin announces this node to the seed peer (config.JoinAddr) and fans out
// the join notification to every peer the seed returns. After a brief delay
// (to let the sidecar TCP port become accept-ready) it:
//  1. Calls JoinCluster on the seed — gets the current leader + known peers.
//  2. Adds all returned peers to the local heartbeat loop.
//  3. Calls JoinCluster on each returned peer (so they all know about this node).
func (server *Server) runJoin(ctx context.Context) {
	// Brief pause so the sidecar listener has accepted the first connection.
	select {
	case <-ctx.Done():
		return
	case <-time.After(200 * time.Millisecond):
	}

	if server.prober == nil || server.heartbeatLoop == nil {
		server.config.Logger.Warn("hot join skipped: cluster not initialised")
		return
	}

	myAddr := server.config.ClusterGRPCAddr
	req := &grpcserver.JoinClusterRequest{
		NodeID:        server.config.NodeID,
		Address:       myAddr,
		PgwireAddress: normalizeListenAddr(server.config.Address),
		Groups:        server.config.Groups,
	}

	resp, err := server.prober.JoinCluster(ctx, server.config.JoinAddr, req)
	if err != nil {
		server.config.Logger.Warn("hot join: JoinCluster on seed failed",
			"seed", server.config.JoinAddr, "error", err)
		return
	}

	server.config.Logger.Info("hot join: accepted by seed",
		"seed", server.config.JoinAddr,
		"leader_id", resp.LeaderID,
		"known_peers", len(resp.KnownPeers),
	)

	// Add all returned peers to our heartbeat loop.
	for _, p := range resp.KnownPeers {
		peer, ok := grpcserver.PeerFromInfo(p)
		if !ok {
			continue
		}
		server.heartbeatLoop.AddPeer(peer)
	}

	// Fan out: inform every returned peer about this new node (best effort).
	for _, p := range resp.KnownPeers {
		if p.Address == "" || p.Address == server.config.JoinAddr {
			continue // already done above or empty
		}
		addr := p.Address
		go func() {
			if _, ferr := server.prober.JoinCluster(ctx, addr, req); ferr != nil {
				server.config.Logger.Warn("hot join: fanout failed",
					"peer", addr, "error", ferr)
			}
		}()
	}
}

func (server *Server) serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-server.closeCh:
				return nil
			default:
			}
			return fmt.Errorf("accept pgwire connection: %w", err)
		}

		server.waitConn.Add(1)
		go func(connection net.Conn) {
			defer server.waitConn.Done()
			defer connection.Close()
			defer func() {
				if r := recover(); r != nil {
					server.config.Logger.Error("pgwire connection panic",
						"panic", fmt.Sprintf("%v", r),
						"stack", string(debug.Stack()),
					)
				}
			}()
			if err := server.handleConnection(connection); err != nil && !errors.Is(err, io.EOF) {
				server.config.Logger.Warn("pgwire connection failed", "error", err.Error())
			}
		}(conn)
	}
}

func (server *Server) handleConnection(conn net.Conn) error {
	backend := pgproto3.NewBackend(bufio.NewReader(conn), conn)
	backendKey := server.allocateBackendKey()
	state := &connState{
		session:   server.engine.NewSession(),
		prepared:  make(map[string]preparedStmt),
		portals:   make(map[string]portal),
		logger:    server.config.Logger,
		processID: backendKey.processID,
		secretKey: backendKey.secretKey,
	}

	handled, err := server.performStartupHandshake(backend, conn, backendKey, state)
	if err != nil {
		return err
	}
	if handled {
		return nil
	}

	server.registerCancelableConnection(backendKey, state)
	defer server.unregisterCancelableConnection(backendKey)
	for {
		message, err := backend.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("receive frontend message: %w", err)
		}

		switch typed := message.(type) {
		case *pgproto3.Query:
			if err := server.handleSimpleQuery(backend, state, typed.String); err != nil {
				return err
			}
		case *pgproto3.CopyData:
			if err := server.handleCopyData(backend, state, typed); err != nil {
				return err
			}
		case *pgproto3.CopyDone:
			if err := server.handleCopyDone(backend, state); err != nil {
				return err
			}
		case *pgproto3.CopyFail:
			if err := server.handleCopyFail(backend, state, typed); err != nil {
				return err
			}
		case *pgproto3.Parse:
			server.handleParse(backend, state, typed)
		case *pgproto3.Bind:
			server.handleBind(backend, state, typed)
		case *pgproto3.Describe:
			server.handleDescribe(backend, state, typed)
		case *pgproto3.Execute:
			if err := server.handleExtendedExecute(backend, state, typed); err != nil {
				return err
			}
		case *pgproto3.Sync:
			state.errorPending = false
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus(state.session)})
			_ = backend.Flush()
		case *pgproto3.Flush:
			_ = backend.Flush()
		case *pgproto3.Close:
			server.handleCloseMessage(backend, state, typed)
		case *pgproto3.Terminate:
			return nil
		default:
			if err := sendErrorAndReadyCode(backend, fmt.Sprintf("unsupported frontend message %T", typed), "XX000", state.session); err != nil {
				return err
			}
		}
	}
}

// performStartupHandshake completes the pgwire authentication handshake and
// sends initial ParameterStatus messages including cluster topology information
// (leader address, peer list, this node's role) so SDK clients can route
// writes to the leader without any extra round-trip.
func (server *Server) performStartupHandshake(backend *pgproto3.Backend, conn net.Conn, backendKey backendCancelKey, state *connState) (bool, error) {
	for {
		startupMessage, err := backend.ReceiveStartupMessage()
		if err != nil {
			return false, fmt.Errorf("receive startup message: %w", err)
		}

		switch typed := startupMessage.(type) {
		case *pgproto3.SSLRequest:
			if _, err := conn.Write([]byte("N")); err != nil {
				return false, fmt.Errorf("write ssl rejection: %w", err)
			}
			continue
		case *pgproto3.CancelRequest:
			server.cancelRequest(uint32(typed.ProcessID), secretKeyToUint32(typed.SecretKey))
			return true, nil
		case *pgproto3.StartupMessage:
			user := typed.Parameters["user"]
			requiresPassword := server.config.AuthToken != "" || server.engine.HasPrincipalCatalog()
			authMethod := "none"
			if server.engine.HasPrincipalCatalog() {
				authMethod = "durable_principal"
			} else if server.config.AuthToken != "" {
				authMethod = "shared_token"
			}
			if requiresPassword {
				backend.Send(&pgproto3.AuthenticationCleartextPassword{})
				if err := backend.Flush(); err != nil {
					return true, fmt.Errorf("send password challenge: %w", err)
				}

				message, err := backend.Receive()
				if err != nil {
					return true, fmt.Errorf("receive password message: %w", err)
				}
				passwordMessage, ok := message.(*pgproto3.PasswordMessage)
				if !ok {
					server.auditFailure("auth.login", "protocol_violation",
						slog.String("principal", normalizeAuditPrincipal(user)),
						slog.String("auth_method", authMethod),
					)
					return true, sendMessages(backend, &pgproto3.ErrorResponse{
						Severity: "FATAL",
						Code:     "08P01",
						Message:  fmt.Sprintf("expected password message, got %T", message),
					})
				}
				if server.engine.HasPrincipalCatalog() {
					if _, err := server.engine.AuthenticatePrincipal(user, passwordMessage.Password); err != nil {
						server.auditFailure("auth.login", "authentication_failed",
							slog.String("principal", normalizeAuditPrincipal(user)),
							slog.String("auth_method", authMethod),
						)
						return true, sendMessages(backend, &pgproto3.ErrorResponse{
							Severity: "FATAL",
							Code:     "28P01",
							Message:  fmt.Sprintf("password authentication failed for user %q", user),
						})
					}
					state.session.SetPrincipal(user)
				} else {
					if passwordMessage.Password != server.config.AuthToken {
						server.auditFailure("auth.login", "authentication_failed",
							slog.String("principal", normalizeAuditPrincipal(user)),
							slog.String("auth_method", authMethod),
						)
						return true, sendMessages(backend, &pgproto3.ErrorResponse{
							Severity: "FATAL",
							Code:     "28P01",
							Message:  fmt.Sprintf("password authentication failed for user %q", user),
						})
					}
					state.session.SetPrincipal(user)
				}
			} else {
				state.session.SetPrincipal(user)
			}
			server.auditSuccess("auth.login",
				slog.String("principal", normalizeAuditPrincipal(user)),
				slog.String("auth_method", authMethod),
			)

			msgs := make([]pgproto3.BackendMessage, 0, 12)
			msgs = append(msgs,
				&pgproto3.AuthenticationOk{},
				&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0-asql-spike"},
				&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"},
				&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"},
			)
			for _, ps := range server.clusterParameterStatuses() {
				msgs = append(msgs, ps)
			}
			msgs = append(msgs,
				&pgproto3.BackendKeyData{ProcessID: backendKey.processID, SecretKey: uint32ToSecretKey(backendKey.secretKey)},
				&pgproto3.ReadyForQuery{TxStatus: txStatus(nil)},
			)
			return false, sendMessages(backend, msgs...)
		default:
			return false, fmt.Errorf("unsupported startup message %T", startupMessage)
		}
	}
}

func (server *Server) allocateBackendKey() backendCancelKey {
	processID := atomic.AddUint32(&server.nextBackendID, 1)
	return backendCancelKey{processID: processID, secretKey: processID ^ 0x5A17C0DE}
}

// uint32ToSecretKey encodes a uint32 secret key as a 4-byte big-endian slice,
// matching the pgproto3 v5.9+ BackendKeyData.SecretKey []byte representation.
func uint32ToSecretKey(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// secretKeyToUint32 decodes a pgproto3 v5.9+ SecretKey []byte back to uint32.
// Fewer than 4 bytes are zero-padded on the left.
func secretKeyToUint32(b []byte) uint32 {
	if len(b) >= 4 {
		return binary.BigEndian.Uint32(b[len(b)-4:])
	}
	var buf [4]byte
	copy(buf[4-len(b):], b)
	return binary.BigEndian.Uint32(buf[:])
}

// AdminHTTPAddress returns the bound admin HTTP address when the optional
// admin listener is running.
func (server *Server) AdminHTTPAddress() string {
	if server == nil {
		return ""
	}
	server.adminMu.RLock()
	listener := server.adminListener
	server.adminMu.RUnlock()
	if listener == nil {
		return ""
	}
	return listener.Addr().String()
}

func (server *Server) registerCancelableConnection(key backendCancelKey, state *connState) {
	server.cancelMu.Lock()
	defer server.cancelMu.Unlock()
	server.cancelTargets[key] = state
}

func (server *Server) unregisterCancelableConnection(key backendCancelKey) {
	server.cancelMu.Lock()
	defer server.cancelMu.Unlock()
	delete(server.cancelTargets, key)
}

func (server *Server) cancelRequest(processID, secretKey uint32) {
	server.cancelMu.Lock()
	target := server.cancelTargets[backendCancelKey{processID: processID, secretKey: secretKey}]
	server.cancelMu.Unlock()
	if target != nil {
		target.cancelCurrentQuery()
	}
}

// clusterParameterStatuses returns pgwire ParameterStatus messages carrying
// topology information: node ID, role, current leader pgwire address, and the
// full peer list. Clients (e.g. the Go SDK) read these on every new connection
// to determine where to route writes after a failover.
func (server *Server) clusterParameterStatuses() []*pgproto3.ParameterStatus {
	nodeID := server.config.NodeID
	if nodeID == "" {
		return nil // standalone mode — no cluster params
	}

	role := "unknown"
	leaderAddr := ""
	peerAddrs := []string{normalizeListenAddr(server.config.Address)}

	if server.raftNode != nil {
		// Raft is authoritative for leader/follower role and leader identity.
		role = server.raftNode.Role()
		if role == "" {
			role = "unknown"
		}
		if server.raftNode.IsLeader() {
			leaderAddr = normalizeListenAddr(server.config.Address)
		} else {
			leaderAddr = server.leaderPgwireAddr()
		}
		if server.heartbeatLoop != nil {
			for _, p := range server.heartbeatLoop.Peers() {
				if p.PgwireAddress != "" {
					peerAddrs = append(peerAddrs, p.PgwireAddress)
				}
			}
		}
	}

	return []*pgproto3.ParameterStatus{
		{Name: "asql_node_id", Value: nodeID},
		{Name: "asql_node_role", Value: role},
		{Name: "asql_cluster_leader", Value: leaderAddr},
		{Name: "asql_cluster_peers", Value: strings.Join(peerAddrs, ",")},
	}
}

// leaderPgwireAddr returns the pgwire address of the current cluster leader
// for client redirect hints. In the production cluster runtime, Raft is the
// only authority for leader identity; heartbeat peer gossip is used only as a
// metadata source for pgwire address resolution.
func (server *Server) leaderPgwireAddr() string {
	if server.raftNode != nil {
		leaderID := server.raftNode.LeaderID()
		if leaderID == "" {
			return ""
		}
		if leaderID == server.config.NodeID {
			return normalizeListenAddr(server.config.Address)
		}
		if server.heartbeatLoop != nil {
			for _, p := range server.heartbeatLoop.Peers() {
				if p.NodeID == leaderID && p.PgwireAddress != "" {
					return p.PgwireAddress
				}
			}
		}
		// Fallback: derive PgwireAddress from the Raft leader's gRPC address.
		// When gossip hasn't propagated yet, compute the pgwire address by
		// replacing the gRPC port with the corresponding pgwire port offset
		// (gRPC 6XXX → pgwire 5XXX, a 1000-port offset convention used by
		// the dev cluster).  This is a best-effort heuristic that unblocks
		// follower redirects during the first seconds after cluster startup.
		if leaderGRPC := server.raftNode.LeaderGRPCAddr(); leaderGRPC != "" {
			if derived := derivePgwireFromGRPC(leaderGRPC); derived != "" {
				return derived
			}
		}
		return ""
	}
	return ""
}

// isFollower reports whether this node should reject write statements.
// When Raft is active it is the authoritative source: any node that is not
// the elected Raft leader (including candidates mid-election) must not accept
// writes. Returns false in standalone (single-node) mode.
func (server *Server) isFollower() bool {
	if server.raftNode != nil {
		return !server.raftNode.IsLeader()
	}
	return false
}

// isWriteStatement returns true for SQL statements that mutate state and must
// therefore be routed to the cluster leader. Read-only statements (SELECT),
// transaction control (BEGIN, COMMIT, ROLLBACK), and admin queries are allowed
// on follower nodes.
func isWriteStatement(sql string) bool {
	s := strings.ToLower(strings.TrimSpace(sql))
	// Strip leading block comments so "/* hint */ INSERT ..." is caught.
	for strings.HasPrefix(s, "/*") {
		end := strings.Index(s, "*/")
		if end < 0 {
			break
		}
		s = strings.TrimSpace(s[end+2:])
	}
	for _, kw := range []string{"insert ", "update ", "delete ", "create ", "drop ", "alter ", "truncate "} {
		if strings.HasPrefix(s, kw) {
			return true
		}
	}
	return false
}

// parseShowStatement checks whether sql is a bare SHOW command and returns the
// parameter name in lower-case.  Strips a trailing semicolon if present.
// e.g. "SHOW asql_node_role;" → ("asql_node_role", true)
func parseShowStatement(sql string) (string, bool) {
	s := strings.TrimSuffix(strings.TrimSpace(sql), ";")
	s = strings.TrimSpace(s)
	upper := strings.ToUpper(s)
	if !strings.HasPrefix(upper, "SHOW ") {
		return "", false
	}
	param := strings.TrimSpace(s[5:])
	if param == "" || strings.ContainsAny(param, " \t\n") {
		return "", false
	}
	return strings.ToLower(param), true
}

// showParamResult resolves a SHOW parameter and returns an executor.Result
// with a single row.  Used by the extended query path where we must NOT send
// RowDescription or ReadyForQuery (those come from Describe and Sync).
func (server *Server) showParamResult(session *executor.Session, param string) (executor.Result, []string) {
	value, ok := server.resolveShowParam(session, param)
	if !ok {
		return executor.Result{Status: "error"}, nil
	}
	row := map[string]ast.Literal{
		param: {Kind: ast.LiteralString, StringValue: value},
	}
	return executor.Result{
		Status: "SHOW",
		Rows:   []map[string]ast.Literal{row},
	}, []string{param}
}

// resolveShowParam returns the value for a SHOW parameter. Returns ("", false)
// for unrecognized parameters.
func (server *Server) resolveShowParam(_ *executor.Session, param string) (string, bool) {
	switch param {
	case "asql_node_id":
		return server.config.NodeID, true
	case "asql_node_role":
		if server.raftNode != nil {
			v := server.raftNode.Role()
			if v == "" {
				v = "unknown"
			}
			return v, true
		}
		return "standalone", true
	case "asql_cluster_leader":
		if server.raftNode != nil {
			if server.raftNode.IsLeader() {
				return normalizeListenAddr(server.config.Address), true
			}
			return server.leaderPgwireAddr(), true
		}
		return "", true
	case "asql_cluster_peers":
		peers := []string{normalizeListenAddr(server.config.Address)}
		if server.heartbeatLoop != nil {
			for _, p := range server.heartbeatLoop.Peers() {
				if p.PgwireAddress != "" {
					peers = append(peers, p.PgwireAddress)
				}
			}
		}
		return strings.Join(peers, ","), true
	case "asql_raft_term":
		if server.raftNode != nil {
			return fmt.Sprintf("%d", server.raftNode.CurrentTerm()), true
		}
		return "0", true
	case "asql_raft_leader_id":
		if server.raftNode != nil {
			return server.raftNode.LeaderID(), true
		}
		return "", true
	case "server_version":
		return "16.0-asql-spike", true
	case "server_version_num":
		return "160000", true
	case "client_encoding":
		return "UTF8", true
	case "standard_conforming_strings":
		return "on", true
	case "search_path":
		return "\"$user\", public", true
	case "timezone":
		return "UTC", true
	case "datestyle":
		return "ISO, MDY", true
	case "intervalstyle":
		return "postgres", true
	case "integer_datetimes":
		return "on", true
	case "is_superuser":
		return "on", true
	case "session_authorization":
		return "asql", true
	case "max_identifier_length":
		return "63", true
	case "transaction_isolation", "default_transaction_isolation":
		return "read committed", true
	case "bytea_output":
		return "hex", true
	case "lc_collate", "lc_ctype", "lc_messages", "lc_monetary", "lc_numeric", "lc_time":
		return "en_US.UTF-8", true
	default:
		if strings.HasPrefix(param, "asql_") {
			return "", false
		}
		return "", true
	}
}

// handleShowParam responds to a SHOW <param> command using the server's live
// runtime state.  Supports all four asql_* cluster params plus common
// PostgreSQL compatibility parameters. Unknown non-asql parameters fall back
// to an empty string to keep mainstream tool probes moving.
func (server *Server) handleShowParam(backend *pgproto3.Backend, session *executor.Session, param string) error {
	value, ok := server.resolveShowParam(session, param)
	if !ok {
		return sendErrorAndReadyCode(backend, fmt.Sprintf("unrecognized configuration parameter %q", param), "42704", session)
	}

	backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{
		Name:         []byte(param),
		DataTypeOID:  25, // text
		DataTypeSize: -1,
		TypeModifier: -1,
	}}})
	backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(value)}})
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SHOW")})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus(session)})
	return backend.Flush()
}

func (server *Server) handleSimpleQuery(backend *pgproto3.Backend, state *connState, sql string) error {
	session := state.session
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return sendMessages(backend, &pgproto3.EmptyQueryResponse{}, &pgproto3.ReadyForQuery{TxStatus: txStatus(session)})
	}

	// Intercept SHOW <param> before it reaches the SQL engine.
	// The engine has no SHOW handler; these are pgwire-level runtime parameters.
	if paramName, ok := parseShowStatement(trimmed); ok {
		return server.handleShowParam(backend, session, paramName)
	}

	// Intercept known catalog/introspection queries (pg_catalog, current_setting,
	// pg_is_in_recovery, etc.) so they never reach the SQL engine.
	{
		ctx, finish := state.beginQuery()
		if err := server.authorizeCatalogQuery(session.Principal(), trimmed, true); err != nil {
			finish()
			return sendErrorAndReady(backend, err, session)
		}
		if intercepted, ok := server.interceptCatalog(ctx, trimmed, session.ActiveDomains(), session.Principal()); ok {
			finish()
			return sendInterceptedResult(backend, session, intercepted)
		}
		finish()
	}

	// Follower redirect: reject DML/DDL when this node is not the leader.
	// The leader's pgwire address is embedded in the error hint so SDK clients
	// and ASQL-aware apps can transparently reconnect without extra discovery.
	if server.isFollower() && isWriteStatement(trimmed) {
		return sendFollowerRedirectError(backend, server.leaderPgwireAddr(), session)
	}

	if handled, err := server.handleCopyQuery(backend, state, trimmed); handled {
		return err
	}

	if tailStmt, ok, err := parseTailEntityChangesStatement(trimmed); ok && tailStmt.Follow {
		if err != nil {
			return sendErrorAndReady(backend, err, session)
		}
		ctx, finish := state.beginQuery()
		defer finish()
		return server.handleTailEntityChangesFollowQuery(ctx, backend, session, tailStmt)
	}

	ctx, finish := state.beginQuery()
	defer finish()

	result, columns, err := server.executeSQL(ctx, session, trimmed)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return sendErrorAndReadyCode(backend, "query canceled", "57014", session)
		}
		return sendErrorAndReady(backend, err, session)
	}

	if len(columns) > 0 {
		columnTypeOIDs := inferColumnTypeOIDs(columns, result.Rows)
		fields := make([]pgproto3.FieldDescription, 0, len(columns))
		for _, column := range columns {
			fields = append(fields, pgproto3.FieldDescription{
				Name:                 []byte(column),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          columnTypeOIDs[column],
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			})
		}

		backend.Send(&pgproto3.RowDescription{Fields: fields})

		for _, row := range result.Rows {
			runPgwireStreamHook()
			if err := ctx.Err(); err != nil {
				return sendErrorAndReadyCode(backend, "query canceled", "57014", session)
			}
			values := make([][]byte, 0, len(columns))
			for _, column := range columns {
				literal, exists := row[column]
				if !exists || literal.Kind == ast.LiteralNull {
					values = append(values, nil)
					continue
				}
				switch literal.Kind {
				case ast.LiteralNumber:
					values = append(values, []byte(strconv.FormatInt(literal.NumberValue, 10)))
				case ast.LiteralString:
					values = append(values, []byte(literal.StringValue))
				case ast.LiteralBoolean:
					if literal.BoolValue {
						values = append(values, []byte("t"))
					} else {
						values = append(values, []byte("f"))
					}
				case ast.LiteralFloat:
					values = append(values, []byte(strconv.FormatFloat(literal.FloatValue, 'g', -1, 64)))
				case ast.LiteralTimestamp:
					t := time.UnixMicro(literal.NumberValue).UTC()
					values = append(values, []byte(t.Format("2006-01-02 15:04:05")))
				case ast.LiteralJSON:
					values = append(values, []byte(literal.StringValue))
				default:
					values = append(values, []byte(""))
				}
			}

			backend.Send(&pgproto3.DataRow{Values: values})
		}
	}

	tag := commandTag(result)
	if err := ctx.Err(); err != nil {
		return sendErrorAndReadyCode(backend, "query canceled", "57014", session)
	}
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus(session)})
	return backend.Flush()
}

func (server *Server) authorizeHistoricalRead(session *executor.Session, detail historicalReadAuditDetail) error {
	principal := ""
	if session != nil {
		principal = session.Principal()
	}
	return server.authorizeHistoricalReadPrincipal(principal, detail, true)
}

func (server *Server) authorizeHistoricalReadPrincipal(principal string, detail historicalReadAuditDetail, emitAudit bool) error {
	attrs := server.historicalReadAuditAttrs(principal, detail)
	if err := server.engine.AuthorizeHistoricalRead(principal); err != nil {
		if emitAudit && server.engine.HasPrincipalCatalog() {
			server.auditFailure("authz.historical_read", "privilege_denied", attrs...)
		}
		return err
	}
	if emitAudit && server.engine.HasPrincipalCatalog() {
		server.auditSuccess("authz.historical_read", attrs...)
	}
	return nil
}

// sendInterceptedResult serialises a catalog-intercepted result set as
// RowDescription + DataRow* + CommandComplete + ReadyForQuery, exactly like
// the normal simple-query path.  This avoids duplicating the marshal logic.
func sendInterceptedResult(backend *pgproto3.Backend, session *executor.Session, ir interceptResult) error {
	columns := ir.columns
	rows := ir.result.Rows

	if len(columns) > 0 {
		columnTypeOIDs := inferColumnTypeOIDs(columns, rows)
		fields := make([]pgproto3.FieldDescription, 0, len(columns))
		for _, col := range columns {
			fields = append(fields, pgproto3.FieldDescription{
				Name:                 []byte(col),
				TableOID:             0,
				TableAttributeNumber: 0,
				DataTypeOID:          columnTypeOIDs[col],
				DataTypeSize:         -1,
				TypeModifier:         -1,
				Format:               0,
			})
		}
		backend.Send(&pgproto3.RowDescription{Fields: fields})
	}
	if len(columns) > 0 && len(rows) > 0 {
		for _, row := range rows {
			values := make([][]byte, 0, len(columns))
			for _, col := range columns {
				literal, exists := row[col]
				if !exists || literal.Kind == ast.LiteralNull {
					values = append(values, nil)
					continue
				}
				switch literal.Kind {
				case ast.LiteralNumber:
					values = append(values, []byte(strconv.FormatInt(literal.NumberValue, 10)))
				case ast.LiteralString:
					values = append(values, []byte(literal.StringValue))
				case ast.LiteralBoolean:
					if literal.BoolValue {
						values = append(values, []byte("t"))
					} else {
						values = append(values, []byte("f"))
					}
				case ast.LiteralFloat:
					values = append(values, []byte(strconv.FormatFloat(literal.FloatValue, 'g', -1, 64)))
				case ast.LiteralTimestamp:
					t := time.UnixMicro(literal.NumberValue).UTC()
					values = append(values, []byte(t.Format("2006-01-02 15:04:05")))
				case ast.LiteralJSON:
					values = append(values, []byte(literal.StringValue))
				default:
					values = append(values, []byte(""))
				}
			}
			backend.Send(&pgproto3.DataRow{Values: values})
		}
	}

	rowCount := len(rows)
	tag := fmt.Sprintf("SELECT %d", rowCount)
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus(session)})
	return backend.Flush()
}

func (server *Server) executeSQL(ctx context.Context, session *executor.Session, sql string) (executor.Result, []string, error) {
	// Extract and strip any /* as-of-lsn: N */ or /* as-of-ts: N */ comment
	// appended by the client before handing SQL to the parser.  The comment is
	// not valid SQL syntax and will cause parse failures even for trivially
	// correct statements.  We also drop any trailing semicolon left behind after
	// comment removal (e.g. "SELECT ... LIMIT 100; /* as-of-lsn: N */").
	asOfKind, asOfValue, stripped := extractAsOf(sql)
	if _, isExplain := stripExplainSQLPrefix(stripped); isExplain {
		result, err := server.engine.ExplainAsPrincipal(stripped, session.ActiveDomains(), session.Principal())
		if err != nil {
			return executor.Result{}, nil, err
		}
		return result, append([]string(nil), explainResultColumns...), nil
	}

	if tailStmt, ok, err := parseTailEntityChangesStatement(stripped); ok {
		if err != nil {
			return executor.Result{}, nil, err
		}
		if tailStmt.Follow {
			return executor.Result{}, nil, fmt.Errorf("TAIL ENTITY CHANGES FOLLOW requires the pgwire streaming execution path")
		}
		if err := server.authorizeHistoricalRead(session, historicalReadAuditDetail{queryKind: "entity_changes", targetKind: "history_stream"}); err != nil {
			return executor.Result{}, nil, err
		}
		events, err := server.engine.EntityChanges(ctx, executor.EntityChangesRequest{
			Domain:  tailStmt.Domain,
			Entity:  tailStmt.Entity,
			RootPK:  tailStmt.RootPK,
			FromLSN: tailStmt.FromLSN,
			ToLSN:   tailStmt.ToLSN,
			Limit:   tailStmt.Limit,
		})
		if err != nil {
			return executor.Result{}, nil, err
		}
		rows := make([]map[string]ast.Literal, 0, len(events))
		for _, event := range events {
			row, rowErr := entityChangeEventRow(event)
			if rowErr != nil {
				return executor.Result{}, nil, rowErr
			}
			rows = append(rows, row)
		}
		return executor.Result{Status: "TAIL ENTITY CHANGES", Rows: rows}, append([]string(nil), tailEntityChangesColumns...), nil
	}

	statement, parseErr := parser.Parse(stripped)
	if parseErr == nil {
		if selectStatement, isSelect := statement.(ast.SelectStatement); isSelect {
			domains := session.ActiveDomains()
			if _, err := server.engine.AuthorizeSQL(session.Principal(), stripped, domains); err != nil {
				return executor.Result{}, nil, err
			}
			// Inline `AS OF LSN N` / `AS OF TIMESTAMP '...'` in the SQL
			// takes priority over the legacy /* as-of-... */ comment
			// hint; it carries the user's explicit intent.
			effKind := asOfKind
			effValue := asOfValue
			if selectStatement.AsOfLSN != nil {
				effKind, effValue = "lsn", *selectStatement.AsOfLSN
			} else if selectStatement.AsOfTimestampMicros != nil {
				effKind, effValue = "ts", uint64(*selectStatement.AsOfTimestampMicros)
			}
			var result executor.Result
			var err error
			if selectStatement.ForHistory {
				if err = server.authorizeHistoricalRead(session, historicalReadAuditDetail{queryKind: "for_history", targetKind: "history_stream"}); err != nil {
					return executor.Result{}, nil, err
				}
				result, err = server.engine.RowHistory(ctx, stripped, domains)
			} else {
				switch effKind {
				case "ts":
					if err = server.authorizeHistoricalRead(session, historicalReadAuditDetail{queryKind: "as_of_timestamp", targetKind: "timestamp", targetTimestampMicros: effValue}); err != nil {
						return executor.Result{}, nil, err
					}
					result, err = server.engine.TimeTravelQueryAsOfTimestamp(ctx, stripped, domains, effValue)
				default:
					targetLSN := maxLSN
					if effKind == "lsn" {
						if err = server.authorizeHistoricalRead(session, historicalReadAuditDetail{queryKind: "as_of_lsn", targetKind: "lsn", targetLSN: effValue}); err != nil {
							return executor.Result{}, nil, err
						}
						targetLSN = effValue
					}
					result, err = server.engine.TimeTravelQueryAsOfLSN(ctx, stripped, domains, targetLSN)
				}
			}
			if err != nil {
				return executor.Result{}, nil, err
			}
			columns := deriveColumns(statement, result.Rows)
			return result, columns, nil
		}
	}

	result, err := server.engine.Execute(ctx, session, stripped)
	if err != nil {
		return executor.Result{}, nil, err
	}

	// For the currently supported INSERT ... RETURNING path, the executor
	// populates result.Rows. Derive column names from the parsed statement's
	// ReturningColumns list so that the pgwire handler sends a proper
	// RowDescription + DataRow sequence.
	if len(result.Rows) > 0 {
		var cols []string
		if parseErr == nil {
			if ins, ok := statement.(ast.InsertStatement); ok && len(ins.ReturningColumns) > 0 {
				cols = ins.ReturningColumns
			}
		}
		if len(cols) == 0 {
			// Fallback: derive from the first row's keys (sorted for determinism).
			cols = deriveColumns(nil, result.Rows)
		}
		return result, cols, nil
	}

	return result, nil, nil
}

func (server *Server) handleTailEntityChangesFollowQuery(ctx context.Context, backend *pgproto3.Backend, session *executor.Session, stmt tailEntityChangesStatement) error {
	if err := server.authorizeHistoricalRead(session, historicalReadAuditDetail{queryKind: "entity_changes_follow", targetKind: "history_stream"}); err != nil {
		return sendErrorAndReady(backend, err, session)
	}
	fields := tailEntityChangesFields()
	backend.Send(&pgproto3.RowDescription{Fields: fields})
	if err := backend.Flush(); err != nil {
		return err
	}

	nextFrom := stmt.FromLSN
	remaining := stmt.Limit
	totalRows := 0
	for {
		ch := server.walStore.Subscribe()
		batchLimit := 0
		if remaining > 0 {
			batchLimit = remaining
		}
		events, err := server.engine.EntityChanges(ctx, executor.EntityChangesRequest{
			Domain:  stmt.Domain,
			Entity:  stmt.Entity,
			RootPK:  stmt.RootPK,
			FromLSN: nextFrom,
			ToLSN:   stmt.ToLSN,
			Limit:   batchLimit,
		})
		if err != nil {
			return sendErrorAndReady(backend, err, session)
		}
		if len(events) > 0 {
			for _, event := range events {
				runPgwireStreamHook()
				if err := ctx.Err(); err != nil {
					return sendErrorAndReadyCode(backend, "query canceled", "57014", session)
				}
				values, valueErr := entityChangeEventValues(event)
				if valueErr != nil {
					return sendErrorAndReady(backend, valueErr, session)
				}
				backend.Send(&pgproto3.DataRow{Values: values})
				totalRows++
			}
			if err := backend.Flush(); err != nil {
				return err
			}
			if remaining > 0 {
				remaining -= len(events)
				if remaining <= 0 {
					break
				}
			}
			nextFrom = events[len(events)-1].CommitLSN + 1
			if stmt.ToLSN > 0 && nextFrom > stmt.ToLSN {
				break
			}
			continue
		}
		if !stmt.Follow || (stmt.ToLSN > 0 && nextFrom > stmt.ToLSN) {
			break
		}
		if err := server.waitForTailEntityChangesWake(ctx, ch); err != nil {
			if errors.Is(err, context.Canceled) {
				return sendErrorAndReadyCode(backend, "query canceled", "57014", session)
			}
			return sendErrorAndReady(backend, err, session)
		}
	}
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", totalRows))})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus(session)})
	return backend.Flush()
}

func (server *Server) waitForTailEntityChangesWake(ctx context.Context, ch <-chan struct{}) error {
	if server.engine.HeadLSN() < server.walStore.LastLSN() {
		timer := time.NewTimer(2 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-server.closeCh:
			return fmt.Errorf("server shutting down")
		case <-timer.C:
			return nil
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-server.closeCh:
		return fmt.Errorf("server shutting down")
	case <-ch:
		return nil
	}
}

func deriveColumns(statement ast.Statement, rows []map[string]ast.Literal) []string {
	if statement != nil {
		if selectStatement, ok := statement.(ast.SelectStatement); ok {
			columns := make([]string, 0, len(selectStatement.Columns))
			for _, column := range selectStatement.Columns {
				canonical := strings.TrimSpace(strings.ToLower(column))
				if canonical == "" || canonical == "*" || isQualifiedStarProjection(canonical) {
					continue
				}
				columns = append(columns, canonical)
			}
			if len(columns) > 0 {
				return columns
			}
		}
	}
	if len(rows) == 0 {
		return []string{"?column?"}
	}
	fallback := make([]string, 0, len(rows[0]))
	for key := range rows[0] {
		fallback = append(fallback, key)
	}
	sortColumns(fallback)
	return fallback
}

func isQualifiedStarProjection(column string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(column))
	return len(trimmed) > 2 && strings.HasSuffix(trimmed, ".*") && strings.TrimSpace(strings.TrimSuffix(trimmed, ".*")) != ""
}

func inferColumnTypeOIDs(columns []string, rows []map[string]ast.Literal) map[string]uint32 {
	const (
		oidBool      uint32 = 16
		oidText      uint32 = 25
		oidInt8      uint32 = 20
		oidJSON      uint32 = 114
		oidFloat8    uint32 = 701
		oidTimestamp uint32 = 1114
	)

	typeOIDs := make(map[string]uint32, len(columns))
	for _, column := range columns {
		typeOIDs[column] = oidText
	}

	for _, row := range rows {
		for _, column := range columns {
			if typeOIDs[column] != oidText {
				continue
			}

			literal, exists := row[column]
			if !exists || literal.Kind == ast.LiteralNull {
				continue
			}

			switch literal.Kind {
			case ast.LiteralNumber:
				typeOIDs[column] = oidInt8
			case ast.LiteralBoolean:
				typeOIDs[column] = oidBool
			case ast.LiteralFloat:
				typeOIDs[column] = oidFloat8
			case ast.LiteralTimestamp:
				typeOIDs[column] = oidTimestamp
			case ast.LiteralJSON:
				typeOIDs[column] = oidJSON
			case ast.LiteralString:
				typeOIDs[column] = oidText
			default:
				typeOIDs[column] = oidText
			}
		}
	}

	return typeOIDs
}

func commandTag(result executor.Result) string {
	status := strings.ToUpper(strings.TrimSpace(result.Status))
	switch status {
	case "OK":
		return "OK"
	case "COMMIT":
		return "COMMIT"
	case "ROLLBACK":
		return "ROLLBACK"
	case "QUEUED":
		return "QUEUE"
	case "BEGIN":
		return "BEGIN"
	}

	if len(result.Rows) > 0 {
		return fmt.Sprintf("SELECT %d", len(result.Rows))
	}

	if status == "" {
		return "OK"
	}
	return status
}

func sendErrorAndReady(backend *pgproto3.Backend, err error, session *executor.Session) error {
	return sendErrorAndReadyCode(backend, err.Error(), mapErrorToSQLState(err), session)
}

func sendErrorAndReadyCode(backend *pgproto3.Backend, message, code string, session *executor.Session) error {
	return sendMessages(
		backend,
		&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     code,
			Message:  message,
		},
		&pgproto3.ReadyForQuery{TxStatus: txStatus(session)},
	)
}

func sendMessages(backend *pgproto3.Backend, messages ...pgproto3.BackendMessage) error {
	for _, message := range messages {
		backend.Send(message)
	}
	return backend.Flush()
}

func txStatus(session *executor.Session) byte {
	if session != nil && session.InTransaction() {
		return 'T'
	}
	return 'I'
}

// derivePgwireFromGRPC derives a pgwire address from a gRPC address using
// the port-offset convention (gRPC 6XXX → pgwire 5XXX, i.e. −1000).
// Returns "" when the conversion fails or yields an invalid port.
// This is a best-effort heuristic for the first seconds after cluster boot
// before heartbeat gossip has propagated PgwireAddress entries.
func derivePgwireFromGRPC(grpcAddr string) string {
	host, portStr, err := net.SplitHostPort(grpcAddr)
	if err != nil {
		return ""
	}
	grpcPort, err := strconv.Atoi(portStr)
	if err != nil || grpcPort <= 1000 {
		return ""
	}
	pgwirePort := grpcPort - 1000
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("%s:%d", host, pgwirePort)
}

// normalizeListenAddr converts a bind address like ":5436" to a routable
// address like "127.0.0.1:5436". Full addresses ("1.2.3.4:5436") are returned
// unchanged. Used when advertising pgwire address to cluster peers.
func normalizeListenAddr(addr string) string {
	if addr == "" {
		return ""
	}
	// net.SplitHostPort handles [::]:port and host:port correctly.
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return "127.0.0.1:" + port
	}
	return addr
}
