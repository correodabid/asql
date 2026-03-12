package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/cluster/heartbeat"
	"asql/internal/engine/executor"
	"asql/internal/engine/parser/ast"
	"asql/internal/platform/sysinfo"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
)

type jsonCodec struct{}

func (jsonCodec) Name() string {
	return "json"
}

func (jsonCodec) Marshal(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (jsonCodec) Unmarshal(data []byte, value interface{}) error {
	return json.Unmarshal(data, value)
}

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

// auditBufSize is the capacity of the async audit log channel.
// A large buffer absorbs bursts without blocking the hot commit path.
const auditBufSize = 4096

type auditEntry struct {
	level slog.Level
	msg   string
	args  []any
}

// PeerRegistry is the port for dynamic peer registration (hot join).
// Implemented by *heartbeat.Loop.
type PeerRegistry interface {
	AddPeer(peer heartbeat.Peer)
	Peers() []heartbeat.Peer
}

type service struct {
	engine       *executor.Engine
	logger       *slog.Logger
	leadership   *coordinator.LeadershipManager
	authority    clusterAuthority
	peerRegistry PeerRegistry // nil in standalone mode
	routing      *readRoutingStats
	mu           sync.Mutex
	sessions     map[string]*executor.Session
	sessionSeen  map[string]time.Time
	cleanupClose chan struct{}
	auditCh      chan auditEntry
}

// sessionIdleTimeout is the maximum time a session can be idle before cleanup.
const sessionIdleTimeout = 10 * time.Minute

func newService(engine *executor.Engine, logger *slog.Logger, leadership *coordinator.LeadershipManager) *service {
	s := &service{
		engine:       engine,
		logger:       logger,
		leadership:   leadership,
		routing:      newReadRoutingStats(),
		sessions:     make(map[string]*executor.Session),
		sessionSeen:  make(map[string]time.Time),
		cleanupClose: make(chan struct{}),
	}
	s.startAuditWriter()
	go s.sessionCleanupLoop()
	return s
}

func (service *service) sessionCleanupLoop() {
	ticker := time.NewTicker(sessionIdleTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			service.mu.Lock()
			now := time.Now()
			var expired []struct {
				txID    string
				session *executor.Session
				idle    time.Duration
			}
			for txID, lastSeen := range service.sessionSeen {
				if now.Sub(lastSeen) > sessionIdleTimeout {
					if sess, ok := service.sessions[txID]; ok {
						expired = append(expired, struct {
							txID    string
							session *executor.Session
							idle    time.Duration
						}{txID, sess, now.Sub(lastSeen)})
					}
					delete(service.sessions, txID)
					delete(service.sessionSeen, txID)
				}
			}
			service.mu.Unlock()

			// Rollback expired sessions outside the service lock to properly
			// decrement the engine's activeTransactions counter.
			for _, e := range expired {
				_, _ = service.engine.Execute(context.Background(), e.session, "ROLLBACK")
				if service.logger != nil {
					service.logger.Warn("session expired due to inactivity", "tx_id", e.txID, "idle", e.idle.String())
				}
			}
		case <-service.cleanupClose:
			return
		}
	}
}

// maxSQLLength is the maximum allowed length for a SQL statement (1 MB).
const maxSQLLength = 1 << 20

func (service *service) Execute(ctx context.Context, request *ExecuteRequest) (*ExecuteResponse, error) {
	if strings.TrimSpace(request.SQL) == "" {
		service.auditFailure("tx.execute", "sql is required", slog.String("tx_id", request.TxID))
		return nil, status.Error(codes.InvalidArgument, "sql is required")
	}
	if len(request.SQL) > maxSQLLength {
		service.auditFailure("tx.execute", "sql exceeds maximum length", slog.String("tx_id", request.TxID), slog.Int("length", len(request.SQL)))
		return nil, status.Errorf(codes.InvalidArgument, "sql exceeds maximum length (%d bytes)", maxSQLLength)
	}

	session, err := service.findSession(request.TxID)
	if err != nil {
		service.auditFailure("tx.execute", err.Error(), slog.String("tx_id", request.TxID))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	result, err := service.engine.Execute(ctx, session, request.SQL)
	if err != nil {
		service.auditFailure("tx.execute", err.Error(), slog.String("tx_id", request.TxID), slog.String("sql", request.SQL))
		return nil, mapError(err)
	}

	service.auditSuccess("tx.execute", slog.String("tx_id", request.TxID), slog.String("sql", request.SQL), slog.String("tx_status", result.Status), slog.Int("rows", len(result.Rows)))

	return &ExecuteResponse{Status: result.Status, TxID: request.TxID, Rows: normalizeRows(result.Rows), RowsAffected: len(result.Rows)}, nil
}

func (service *service) ExecuteBatch(ctx context.Context, request *ExecuteBatchRequest) (*ExecuteBatchResponse, error) {
	if strings.TrimSpace(request.TxID) == "" || len(request.Statements) == 0 {
		service.auditFailure("tx.execute_batch", "tx_id and statements are required", slog.String("tx_id", request.TxID))
		return nil, status.Error(codes.InvalidArgument, "tx_id and statements are required")
	}

	session, err := service.findSession(request.TxID)
	if err != nil {
		service.auditFailure("tx.execute_batch", err.Error(), slog.String("tx_id", request.TxID))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	for i, stmt := range request.Statements {
		if len(stmt) > maxSQLLength {
			service.auditFailure("tx.execute_batch", "statement exceeds maximum length", slog.String("tx_id", request.TxID), slog.Int("index", i))
			return nil, status.Errorf(codes.InvalidArgument, "statement %d exceeds maximum length (%d bytes)", i, maxSQLLength)
		}
		if _, err := service.engine.Execute(ctx, session, stmt); err != nil {
			service.auditFailure("tx.execute_batch", err.Error(), slog.String("tx_id", request.TxID), slog.Int("index", i), slog.String("sql", stmt))
			return nil, mapError(err)
		}
	}

	service.auditSuccess("tx.execute_batch", slog.String("tx_id", request.TxID), slog.Int("executed", len(request.Statements)))
	return &ExecuteBatchResponse{Status: "OK", Executed: len(request.Statements)}, nil
}

func (service *service) BeginTx(ctx context.Context, request *BeginTxRequest) (*BeginTxResponse, error) {
	beginSQL, err := buildBeginSQL(request.Mode, request.Domains)
	if err != nil {
		service.auditFailure("tx.begin", err.Error(), slog.String("mode", request.Mode), slog.Any("domains", request.Domains))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	session := service.engine.NewSession()
	result, err := service.engine.Execute(ctx, session, beginSQL)
	if err != nil {
		service.auditFailure("tx.begin", err.Error(), slog.String("mode", request.Mode), slog.Any("domains", request.Domains))
		return nil, mapError(err)
	}

	if result.TxID == "" {
		service.auditFailure("tx.begin", "engine did not return tx id", slog.String("mode", request.Mode), slog.Any("domains", request.Domains))
		return nil, status.Error(codes.Internal, "engine did not return tx id")
	}

	service.mu.Lock()
	service.sessions[result.TxID] = session
	service.sessionSeen[result.TxID] = time.Now()
	service.mu.Unlock()

	service.auditSuccess("tx.begin", slog.String("tx_id", result.TxID), slog.String("mode", request.Mode), slog.Any("domains", request.Domains))

	return &BeginTxResponse{TxID: result.TxID}, nil
}

func (service *service) CommitTx(ctx context.Context, request *CommitTxRequest) (*CommitTxResponse, error) {
	session, err := service.findSession(request.TxID)
	if err != nil {
		service.auditFailure("tx.commit", err.Error(), slog.String("tx_id", request.TxID))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := service.validateFencingForCommit(session, request); err != nil {
		service.auditFailure("tx.commit", err.Error(), slog.String("tx_id", request.TxID))
		return nil, err
	}

	result, err := service.engine.Execute(ctx, session, "COMMIT")
	if err != nil {
		service.auditFailure("tx.commit", err.Error(), slog.String("tx_id", request.TxID))
		// Rollback the session to release the activeTransactions counter.
		// Without this, failed commits leave phantom active transactions.
		_, _ = service.engine.Execute(ctx, session, "ROLLBACK")
		service.mu.Lock()
		delete(service.sessions, request.TxID)
		delete(service.sessionSeen, request.TxID)
		service.mu.Unlock()
		return nil, mapError(err)
	}

	service.mu.Lock()
	delete(service.sessions, request.TxID)
	delete(service.sessionSeen, request.TxID)
	service.mu.Unlock()

	service.auditSuccess("tx.commit", slog.String("tx_id", request.TxID), slog.String("tx_status", result.Status))

	return &CommitTxResponse{Status: result.Status, CommitLSN: result.CommitLSN}, nil
}

func (service *service) validateFencingForCommit(session *executor.Session, request *CommitTxRequest) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "commit request is required")
	}

	hasGroup := strings.TrimSpace(request.Group) != ""
	hasNode := strings.TrimSpace(request.NodeID) != ""
	hasToken := strings.TrimSpace(request.FencingToken) != ""

	if !hasGroup && !hasNode && !hasToken {
		return nil
	}

	if !(hasNode && hasToken) {
		return status.Error(codes.InvalidArgument, "node_id and fencing_token are required when fencing is enabled")
	}

	group := strings.ToLower(strings.TrimSpace(request.Group))
	if group == "" {
		domains := session.ActiveDomains()
		if len(domains) == 0 {
			return status.Error(codes.FailedPrecondition, "cannot infer group from active transaction")
		}
		group = strings.Join(domains, ",")
	}

	nodeID := strings.TrimSpace(request.NodeID)
	fencingToken := strings.TrimSpace(request.FencingToken)
	canAcceptWrite := false
	if service.authority != nil {
		canAcceptWrite = service.authority.CanAcceptWrite(group, nodeID, fencingToken)
	} else if service.leadership != nil {
		canAcceptWrite = service.leadership.CanAcceptWrite(group, nodeID, fencingToken)
	} else {
		return status.Error(codes.FailedPrecondition, "leadership manager is not configured")
	}
	if !canAcceptWrite {
		service.auditFailure("ha.fencing_rejection", "write rejected by fencing token check", slog.String("group", group), slog.String("node_id", nodeID))
		return status.Errorf(codes.PermissionDenied, "write rejected by fencing token check for group %s", group)
	}

	return nil
}

func (service *service) RollbackTx(ctx context.Context, request *RollbackTxRequest) (*RollbackTxResponse, error) {
	session, err := service.findSession(request.TxID)
	if err != nil {
		service.auditFailure("tx.rollback", err.Error(), slog.String("tx_id", request.TxID))
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	result, err := service.engine.Execute(ctx, session, "ROLLBACK")
	if err != nil {
		service.auditFailure("tx.rollback", err.Error(), slog.String("tx_id", request.TxID))
		return nil, mapError(err)
	}

	service.mu.Lock()
	delete(service.sessions, request.TxID)
	delete(service.sessionSeen, request.TxID)
	service.mu.Unlock()

	service.auditSuccess("tx.rollback", slog.String("tx_id", request.TxID), slog.String("tx_status", result.Status))

	return &RollbackTxResponse{Status: result.Status}, nil
}

func (service *service) ReplayToLSN(ctx context.Context, request *ReplayToLSNRequest) (*ReplayToLSNResponse, error) {
	if err := service.engine.ReplayToLSN(ctx, request.LSN); err != nil {
		service.auditFailure("admin.replay_to_lsn", err.Error(), slog.Uint64("lsn", request.LSN))
		return nil, mapError(err)
	}

	service.auditSuccess("admin.replay_to_lsn", slog.Uint64("lsn", request.LSN))

	return &ReplayToLSNResponse{AppliedLSN: request.LSN}, nil
}

func (service *service) TimeTravelQuery(ctx context.Context, request *TimeTravelQueryRequest) (*TimeTravelQueryResponse, error) {
	if strings.TrimSpace(request.SQL) == "" {
		service.auditFailure("admin.time_travel_query", "sql is required")
		return nil, status.Error(codes.InvalidArgument, "sql is required")
	}

	var (
		result executor.Result
		err    error
	)

	if request.LSN > 0 {
		result, err = service.engine.TimeTravelQueryAsOfLSN(ctx, request.SQL, request.Domains, request.LSN)
	} else {
		result, err = service.engine.TimeTravelQueryAsOfTimestamp(ctx, request.SQL, request.Domains, request.LogicalTimestamp)
	}
	if err != nil {
		service.auditFailure("admin.time_travel_query", err.Error(), slog.String("sql", request.SQL), slog.Uint64("lsn", request.LSN), slog.Uint64("logical_timestamp", request.LogicalTimestamp), slog.Any("domains", request.Domains))
		return nil, mapError(err)
	}

	service.auditSuccess("admin.time_travel_query", slog.String("sql", request.SQL), slog.Uint64("lsn", request.LSN), slog.Uint64("logical_timestamp", request.LogicalTimestamp), slog.Any("domains", request.Domains), slog.Int("rows", len(result.Rows)))

	return &TimeTravelQueryResponse{Status: result.Status, Rows: normalizeRows(result.Rows)}, nil
}

func (service *service) RowHistory(ctx context.Context, request *RowHistoryRequest) (*RowHistoryResponse, error) {
	if strings.TrimSpace(request.SQL) == "" {
		service.auditFailure("admin.row_history", "sql is required")
		return nil, status.Error(codes.InvalidArgument, "sql is required")
	}

	result, err := service.engine.RowHistory(ctx, request.SQL, request.Domains)
	if err != nil {
		service.auditFailure("admin.row_history", err.Error(), slog.String("sql", request.SQL), slog.Any("domains", request.Domains))
		return nil, mapError(err)
	}

	service.auditSuccess("admin.row_history", slog.String("sql", request.SQL), slog.Any("domains", request.Domains), slog.Int("rows", len(result.Rows)))

	return &RowHistoryResponse{Status: result.Status, Rows: normalizeRows(result.Rows)}, nil
}

func (service *service) EntityVersionHistory(ctx context.Context, request *EntityVersionHistoryRequest) (*EntityVersionHistoryResponse, error) {
	if strings.TrimSpace(request.Domain) == "" {
		service.auditFailure("admin.entity_version_history", "domain is required")
		return nil, status.Error(codes.InvalidArgument, "domain is required")
	}
	if strings.TrimSpace(request.EntityName) == "" {
		service.auditFailure("admin.entity_version_history", "entity_name is required")
		return nil, status.Error(codes.InvalidArgument, "entity_name is required")
	}

	entries, err := service.engine.EntityVersionHistory(ctx, request.Domain, request.EntityName, request.RootPK)
	if err != nil {
		service.auditFailure("admin.entity_version_history", err.Error(), slog.String("domain", request.Domain), slog.String("entity", request.EntityName), slog.String("root_pk", request.RootPK))
		return nil, mapError(err)
	}

	versions := make([]EntityVersionHistoryEntry, len(entries))
	for i, e := range entries {
		tables := make([]string, len(e.Tables))
		copy(tables, e.Tables)
		versions[i] = EntityVersionHistoryEntry{
			Version:   e.Version,
			CommitLSN: e.CommitLSN,
			Tables:    tables,
		}
	}

	service.auditSuccess("admin.entity_version_history", slog.String("domain", request.Domain), slog.String("entity", request.EntityName), slog.String("root_pk", request.RootPK), slog.Int("versions", len(versions)))

	return &EntityVersionHistoryResponse{
		Status:   "OK",
		Entity:   request.EntityName,
		RootPK:   request.RootPK,
		Versions: versions,
	}, nil
}

func (service *service) ExplainQuery(_ context.Context, request *ExplainQueryRequest) (*ExplainQueryResponse, error) {
	if strings.TrimSpace(request.SQL) == "" {
		service.auditFailure("admin.explain_query", "sql is required")
		return nil, status.Error(codes.InvalidArgument, "sql is required")
	}

	result, err := service.engine.Explain(request.SQL, request.Domains)
	if err != nil {
		service.auditFailure("admin.explain_query", err.Error(), slog.String("sql", request.SQL), slog.Any("domains", request.Domains))
		return nil, mapError(err)
	}

	service.auditSuccess("admin.explain_query", slog.String("sql", request.SQL), slog.Any("domains", request.Domains), slog.Int("rows", len(result.Rows)))

	return &ExplainQueryResponse{Status: result.Status, Rows: normalizeRows(result.Rows)}, nil
}

func (service *service) ScanStrategyStats(_ context.Context, _ *ScanStrategyStatsRequest) (*ScanStrategyStatsResponse, error) {
	counts := service.engine.ScanStrategyCounts()
	service.auditSuccess("admin.scan_strategy_stats", slog.Int("strategies", len(counts)))
	return &ScanStrategyStatsResponse{Counts: counts}, nil
}

func (service *service) EngineStats(_ context.Context, _ *EngineStatsRequest) (*EngineStatsResponse, error) {
	snap := service.engine.PerfStats()
	service.auditSuccess("admin.engine_stats")
	return &EngineStatsResponse{
		TotalCommits:               snap.TotalCommits,
		TotalReads:                 snap.TotalReads,
		TotalRollbacks:             snap.TotalRollbacks,
		TotalBegins:                snap.TotalBegins,
		TotalCrossDomainBegins:     snap.TotalCrossDomainBegins,
		TotalTimeTravelQueries:     snap.TotalTimeTravelQueries,
		TotalSnapshots:             snap.TotalSnapshots,
		TotalReplays:               snap.TotalReplays,
		TotalFsyncErrors:           snap.TotalFsyncErrors,
		TotalAuditErrors:           snap.TotalAuditErrors,
		ActiveTransactions:         snap.ActiveTransactions,
		CrossDomainBeginAvgDomains: snap.CrossDomainBeginAvgDomains,
		CrossDomainBeginMaxDomains: snap.CrossDomainBeginMaxDomains,
		CommitLatencyP50:           snap.CommitLatencyP50,
		CommitLatencyP95:           snap.CommitLatencyP95,
		CommitLatencyP99:           snap.CommitLatencyP99,
		FsyncLatencyP50:            snap.FsyncLatencyP50,
		FsyncLatencyP95:            snap.FsyncLatencyP95,
		FsyncLatencyP99:            snap.FsyncLatencyP99,
		ReadLatencyP50:             snap.ReadLatencyP50,
		ReadLatencyP95:             snap.ReadLatencyP95,
		ReadLatencyP99:             snap.ReadLatencyP99,
		TimeTravelLatencyP50:       snap.TimeTravelLatencyP50,
		TimeTravelLatencyP95:       snap.TimeTravelLatencyP95,
		TimeTravelLatencyP99:       snap.TimeTravelLatencyP99,
		ReplayDurationMS:           snap.ReplayDurationMs,
		SnapshotDurationMS:         snap.SnapshotDurationMs,
		CommitThroughput:           snap.CommitThroughput,
		ReadThroughput:             snap.ReadThroughput,
		WALFileSize:                snap.WALFileSize,
		SnapshotFileSize:           snap.SnapshotFileSize,
		AuditFileSize:              snap.AuditFileSize,
		System:                     collectSystemInfo(),
	}, nil
}

func (service *service) EvaluateReadRoute(_ context.Context, request *EvaluateReadRouteRequest) (*EvaluateReadRouteResponse, error) {
	consistency := normalizeReadConsistency(request.Consistency)
	decision := decideReadRoute(readRouteInput{
		Consistency:         consistency,
		LeaderLSN:           request.LeaderLSN,
		FollowerLSN:         request.FollowerLSN,
		HasFollower:         request.HasFollower,
		FollowerUnavailable: request.FollowerUnavailable,
		MaxLag:              request.MaxLag,
	})

	if service.routing != nil {
		service.routing.record(readRoutingMetricInput{
			Consistency: consistency,
			Decision:    decision,
			HasFollower: request.HasFollower,
			MaxLag:      request.MaxLag,
			LeaderLSN:   request.LeaderLSN,
			FollowerLSN: request.FollowerLSN,
		})
	}

	service.auditSuccess(
		"admin.evaluate_read_route",
		slog.String("mode", string(consistency)),
		slog.String("route", string(decision.Route)),
		slog.Uint64("leader_lsn", request.LeaderLSN),
		slog.Uint64("follower_lsn", request.FollowerLSN),
		slog.Uint64("lag", decision.Lag),
		slog.String("fallback_reason", decision.FallbackReason),
	)

	return &EvaluateReadRouteResponse{
		Mode:           string(consistency),
		Route:          string(decision.Route),
		LeaderLSN:      request.LeaderLSN,
		FollowerLSN:    request.FollowerLSN,
		Lag:            decision.Lag,
		FallbackReason: decision.FallbackReason,
	}, nil
}

func (service *service) ReadRoutingStats(_ context.Context, _ *ReadRoutingStatsRequest) (*ReadRoutingStatsResponse, error) {
	counts := map[string]uint64{}
	if service.routing != nil {
		counts = service.routing.snapshot()
	}
	service.auditSuccess("admin.read_routing_stats", slog.Int("counters", len(counts)))
	return &ReadRoutingStatsResponse{Counts: counts}, nil
}

func collectSystemInfo() *SystemInfoResponse {
	s := sysinfo.Collect()
	return &SystemInfoResponse{
		Hostname:        s.Hostname,
		OS:              s.OS,
		Arch:            s.Arch,
		NumCPU:          s.NumCPU,
		PID:             s.PID,
		GoVersion:       s.GoVersion,
		NumGoroutine:    s.NumGoroutine,
		UptimeMS:        s.UptimeMS,
		HeapAllocBytes:  s.HeapAllocBytes,
		HeapSysBytes:    s.HeapSysBytes,
		HeapInuseBytes:  s.HeapInuseBytes,
		HeapObjects:     s.HeapObjects,
		StackInuseBytes: s.StackInuseBytes,
		TotalAllocBytes: s.TotalAllocBytes,
		SysBytes:        s.SysBytes,
		GCCycles:        s.GCCycles,
		LastGCPauseNS:   s.LastGCPauseNS,
		GCPauseTotalNS:  s.GCPauseTotalNS,
		GCCPUFraction:   s.GCCPUFraction,
	}
}

func (service *service) LeadershipState(_ context.Context, request *LeadershipStateRequest) (*LeadershipStateResponse, error) {
	group := strings.ToLower(strings.TrimSpace(request.Group))
	if group == "" {
		service.auditFailure("admin.leadership_state", "group is required")
		return nil, status.Error(codes.InvalidArgument, "group is required")
	}

	if service.leadership == nil {
		service.auditFailure("admin.leadership_state", "leadership manager is not configured", slog.String("group", group))
		return nil, status.Error(codes.FailedPrecondition, "leadership manager is not configured")
	}

	state, exists, leaseActive := service.leadership.SnapshotWithLeaseStatus(group)
	if !exists {
		service.auditFailure("admin.leadership_state", "group not found", slog.String("group", group))
		return nil, status.Errorf(codes.NotFound, "leadership state not found for group %s", group)
	}

	service.auditSuccess(
		"admin.leadership_state",
		slog.String("group", state.Group),
		slog.String("leader_id", state.LeaderID),
		slog.Uint64("term", state.Term),
		slog.Bool("lease_active", leaseActive),
	)

	// Gossip: include known peers so heartbeat probers can propagate
	// PgwireAddress to statically-configured nodes over time.
	var peers []PeerInfo
	if service.peerRegistry != nil {
		known := service.peerRegistry.Peers()
		peers = make([]PeerInfo, len(known))
		for i, p := range known {
			peers[i] = PeerInfo{NodeID: p.NodeID, Address: p.Address, PgwireAddress: p.PgwireAddress}
		}
	}

	return &LeadershipStateResponse{
		Group:              state.Group,
		Term:               state.Term,
		LeaderID:           state.LeaderID,
		FencingToken:       state.FencingToken,
		LeaseExpiresAtUnix: state.LeaseExpiresAt.Unix(),
		LeaseActive:        leaseActive,
		LastLeaderLSN:      state.LastLeaderLSN,
		Peers:              peers,
	}, nil
}

func (service *service) SchemaSnapshot(_ context.Context, request *SchemaSnapshotRequest) (*SchemaSnapshotResponse, error) {
	filter := []string{}
	if request != nil {
		filter = request.Domains
	}

	snapshot := service.engine.SchemaSnapshot(filter)
	domains := make([]SchemaSnapshotDomain, 0, len(snapshot.Domains))
	for _, domain := range snapshot.Domains {
		tables := make([]SchemaSnapshotTable, 0, len(domain.Tables))
		for _, table := range domain.Tables {
			columns := make([]SchemaSnapshotColumn, 0, len(table.Columns))
			for _, column := range table.Columns {
				columns = append(columns, SchemaSnapshotColumn{
					Name:             column.Name,
					Type:             column.Type,
					PrimaryKey:       column.PrimaryKey,
					Unique:           column.Unique,
					ReferencesTable:  column.ReferencesTable,
					ReferencesColumn: column.ReferencesColumn,
					DefaultValue:     formatDefaultValue(column.DefaultValue),
				})
			}
			indexes := make([]SchemaSnapshotIndex, 0, len(table.Indexes))
			for _, idx := range table.Indexes {
				cols := make([]string, len(idx.Columns))
				copy(cols, idx.Columns)
				indexes = append(indexes, SchemaSnapshotIndex{
					Name:    idx.Name,
					Columns: cols,
					Method:  idx.Method,
				})
			}
			var vfks []SchemaSnapshotVersionedFK
			for _, vfk := range table.VersionedForeignKeys {
				vfks = append(vfks, SchemaSnapshotVersionedFK{
					Column:           vfk.Column,
					LSNColumn:        vfk.LSNColumn,
					ReferencesDomain: vfk.ReferencesDomain,
					ReferencesTable:  vfk.ReferencesTable,
					ReferencesColumn: vfk.ReferencesColumn,
				})
			}
			tables = append(tables, SchemaSnapshotTable{Name: table.Name, Columns: columns, Indexes: indexes, VersionedForeignKeys: vfks})
		}
		var entities []SchemaSnapshotEntity
		for _, entity := range domain.Entities {
			tables := make([]string, len(entity.Tables))
			copy(tables, entity.Tables)
			entities = append(entities, SchemaSnapshotEntity{
				Name:      entity.Name,
				RootTable: entity.RootTable,
				Tables:    tables,
			})
		}
		domains = append(domains, SchemaSnapshotDomain{Name: domain.Name, Tables: tables, Entities: entities})
	}

	service.auditSuccess("admin.schema_snapshot", slog.Int("domains", len(domains)))
	return &SchemaSnapshotResponse{Status: "SNAPSHOT", Domains: domains}, nil
}

func (service *service) Query(ctx context.Context, request *QueryRequest) (*QueryResponse, error) {
	if strings.TrimSpace(request.SQL) == "" {
		service.auditFailure("query.execute", "sql is required")
		return nil, status.Error(codes.InvalidArgument, "sql is required")
	}
	if len(request.SQL) > maxSQLLength {
		service.auditFailure("query.execute", "sql exceeds maximum length", slog.Int("length", len(request.SQL)))
		return nil, status.Errorf(codes.InvalidArgument, "sql exceeds maximum length (%d bytes)", maxSQLLength)
	}

	result, err := service.engine.Query(ctx, request.SQL, request.Domains)
	if err != nil {
		service.auditFailure("query.execute", err.Error(), slog.String("sql", request.SQL), slog.Any("domains", request.Domains))
		return nil, mapError(err)
	}

	service.auditSuccess("query.execute", slog.String("sql", request.SQL), slog.Any("domains", request.Domains), slog.Int("rows", len(result.Rows)))
	return &QueryResponse{Status: result.Status, Rows: normalizeRows(result.Rows)}, nil
}

func (service *service) TimelineCommits(ctx context.Context, request *TimelineCommitsRequest) (*TimelineCommitsResponse, error) {
	fromLSN := request.FromLSN
	if fromLSN == 0 {
		fromLSN = 1
	}
	limit := request.Limit
	if limit <= 0 {
		limit = 500 // sensible default
	}

	commits, err := service.engine.TimelineCommits(ctx, fromLSN, request.ToLSN, request.Domain, limit)
	if err != nil {
		service.auditFailure("admin.timeline_commits", err.Error())
		return nil, mapError(err)
	}

	entries := make([]TimelineCommitEntry, len(commits))
	for i, c := range commits {
		tables := make([]TimelineCommitMutationEntry, len(c.Tables))
		for j, t := range c.Tables {
			tables[j] = TimelineCommitMutationEntry{Domain: t.Domain, Table: t.Table, Operation: t.Operation}
		}
		entries[i] = TimelineCommitEntry{LSN: c.LSN, TxID: c.TxID, Timestamp: c.Timestamp, Tables: tables}
	}

	service.auditSuccess("admin.timeline_commits", slog.Int("commits", len(entries)))
	return &TimelineCommitsResponse{Commits: entries}, nil
}

// JoinCluster registers a new peer into the running cluster (hot join).
// The joining node calls this on any existing peer (seed) to announce itself.
// The seed adds the new peer to its heartbeat loop and returns the current
// leader identity plus the complete list of known peers so the new node can
// fan out the join announcement to every cluster member.
func (service *service) JoinCluster(_ context.Context, request *JoinClusterRequest) (*JoinClusterResponse, error) {
	if request.NodeID == "" || request.Address == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and address are required")
	}

	if service.peerRegistry == nil {
		return nil, status.Error(codes.FailedPrecondition, "this node is not running in cluster mode")
	}

	// Capture known peers BEFORE adding the new one so the response reflects
	// the peers the new node still needs to notify.
	existing := service.peerRegistry.Peers()

	// Add the joining node to this node's heartbeat peer list.
	service.peerRegistry.AddPeer(heartbeat.Peer{
		NodeID:        request.NodeID,
		Address:       request.Address,
		PgwireAddress: request.PgwireAddress,
	})

	// Resolve the current leader from the production authority when available.
	var leaderID, leaderAddr string
	if service.authority != nil {
		leaderID = service.authority.LeaderID(request.Groups)
	} else if service.leadership != nil {
		for _, group := range request.Groups {
			state, exists, active := service.leadership.SnapshotWithLeaseStatus(group)
			if exists && active {
				leaderID = state.LeaderID
				break
			}
		}
	}
	if leaderID != "" {
		for _, p := range existing {
			if p.NodeID == leaderID {
				leaderAddr = p.Address
				break
			}
		}
	}

	knownPeers := make([]PeerInfo, len(existing))
	for i, p := range existing {
		knownPeers[i] = PeerInfo{NodeID: p.NodeID, Address: p.Address, PgwireAddress: p.PgwireAddress}
	}

	service.auditSuccess(
		"cluster.join",
		slog.String("joining_node", request.NodeID),
		slog.String("joining_addr", request.Address),
		slog.String("leader_id", leaderID),
	)

	return &JoinClusterResponse{
		Accepted:      true,
		LeaderID:      leaderID,
		LeaderAddress: leaderAddr,
		KnownPeers:    knownPeers,
	}, nil
}

// startAuditWriter launches a background goroutine that drains the audit
// channel and writes entries via slog. This decouples I/O from the hot path.
func (service *service) startAuditWriter() {
	if service.auditCh != nil {
		return
	}
	service.auditCh = make(chan auditEntry, auditBufSize)
	go func() {
		for entry := range service.auditCh {
			if service.logger == nil {
				continue
			}
			switch entry.level {
			case slog.LevelWarn:
				service.logger.Warn(entry.msg, entry.args...)
			default:
				service.logger.Info(entry.msg, entry.args...)
			}
		}
	}()
}

// stopAuditWriter drains and closes the audit channel.
func (service *service) stopAuditWriter() {
	if service.auditCh != nil {
		close(service.auditCh)
	}
}

func (service *service) auditSuccess(operation string, attrs ...slog.Attr) {
	if service.auditCh == nil {
		return
	}

	args := make([]any, 0, 3+len(attrs))
	args = append(args,
		slog.String("event", "audit"),
		slog.String("status", "success"),
		slog.String("operation", operation),
	)
	for _, attr := range attrs {
		args = append(args, attr)
	}

	select {
	case service.auditCh <- auditEntry{level: slog.LevelInfo, msg: "audit_event", args: args}:
	default:
		// Drop audit entry if channel is full — never block the hot path.
	}
}

func (service *service) auditFailure(operation, reason string, attrs ...slog.Attr) {
	if service.auditCh == nil {
		return
	}

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

	select {
	case service.auditCh <- auditEntry{level: slog.LevelWarn, msg: "audit_event", args: args}:
	default:
	}
}

func (service *service) findSession(txID string) (*executor.Session, error) {
	trimmed := strings.TrimSpace(txID)
	if trimmed == "" {
		return nil, errors.New("tx_id is required")
	}

	service.mu.Lock()
	defer service.mu.Unlock()

	session, exists := service.sessions[trimmed]
	if !exists {
		return nil, fmt.Errorf("unknown tx_id %s", trimmed)
	}

	service.sessionSeen[trimmed] = time.Now()
	return session, nil
}

func buildBeginSQL(mode string, domains []string) (string, error) {
	canonical := make([]string, 0, len(domains))
	for _, domain := range domains {
		trimmed := strings.ToLower(strings.TrimSpace(domain))
		if trimmed == "" {
			continue
		}
		canonical = append(canonical, trimmed)
	}

	inferred := strings.ToLower(strings.TrimSpace(mode))
	if inferred == "" {
		if len(canonical) <= 1 {
			inferred = "domain"
		} else {
			inferred = "cross"
		}
	}

	switch inferred {
	case "domain":
		if len(canonical) != 1 {
			return "", errors.New("domain mode requires exactly one domain")
		}
		return "BEGIN DOMAIN " + canonical[0], nil
	case "cross", "cross-domain", "cross_domain":
		if len(canonical) < 2 {
			return "", errors.New("cross-domain mode requires at least two domains")
		}
		return "BEGIN CROSS DOMAIN " + strings.Join(canonical, ", "), nil
	default:
		return "", fmt.Errorf("unsupported begin mode %q", mode)
	}
}

func normalizeRows(rows []map[string]ast.Literal) []map[string]interface{} {
	normalized := make([]map[string]interface{}, 0, len(rows))
	for _, row := range rows {
		converted := make(map[string]interface{}, len(row))
		for key, value := range row {
			switch value.Kind {
			case ast.LiteralString:
				converted[key] = value.StringValue
			case ast.LiteralNumber:
				converted[key] = value.NumberValue
			case ast.LiteralBoolean:
				converted[key] = value.BoolValue
			case ast.LiteralFloat:
				converted[key] = value.FloatValue
			case ast.LiteralTimestamp:
				converted[key] = time.UnixMicro(value.NumberValue).UTC().Format(time.RFC3339)
			case ast.LiteralJSON:
				var parsed interface{}
				if err := json.Unmarshal([]byte(value.StringValue), &parsed); err == nil {
					converted[key] = parsed
				} else {
					converted[key] = value.StringValue
				}
			case ast.LiteralNull:
				converted[key] = nil
			default:
				converted[key] = nil
			}
		}
		normalized = append(normalized, converted)
	}

	return normalized
}

func formatDefaultValue(def *ast.DefaultExpr) string {
	if def == nil {
		return ""
	}
	switch def.Kind {
	case ast.DefaultAutoIncrement:
		return "AUTOINCREMENT"
	case ast.DefaultUUIDv7:
		return "UUID_V7"
	case ast.DefaultLiteral:
		switch def.Value.Kind {
		case ast.LiteralNull:
			return "NULL"
		case ast.LiteralString:
			return "'" + def.Value.StringValue + "'"
		case ast.LiteralNumber:
			return fmt.Sprintf("%d", def.Value.NumberValue)
		case ast.LiteralBoolean:
			if def.Value.BoolValue {
				return "TRUE"
			}
			return "FALSE"
		case ast.LiteralFloat:
			return fmt.Sprintf("%g", def.Value.FloatValue)
		default:
			return ""
		}
	default:
		return ""
	}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}

	msg := err.Error()

	// Parse errors → InvalidArgument
	if strings.Contains(msg, "parse") || strings.Contains(msg, "syntax") {
		return status.Error(codes.InvalidArgument, msg)
	}

	// Not found errors → NotFound
	if strings.Contains(msg, "not found") || strings.Contains(msg, "does not exist") {
		return status.Error(codes.NotFound, msg)
	}

	// Write conflicts → Aborted (retriable)
	if strings.Contains(msg, "write conflict") {
		return status.Error(codes.Aborted, msg)
	}

	// Constraint violations, already exists → FailedPrecondition
	if strings.Contains(msg, "constraint") || strings.Contains(msg, "already exists") {
		return status.Error(codes.FailedPrecondition, msg)
	}

	// Transaction state errors → FailedPrecondition
	if strings.Contains(msg, "transaction") || strings.Contains(msg, "session") || strings.Contains(msg, "domain is required") {
		return status.Error(codes.FailedPrecondition, msg)
	}

	return status.Error(codes.Internal, msg)
}
