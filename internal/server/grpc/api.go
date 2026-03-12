package grpc

import (
	"context"

	grpcgo "google.golang.org/grpc"
)

type ExecuteRequest struct {
	TxID string `json:"tx_id,omitempty"`
	SQL  string `json:"sql"`
}

type ExecuteResponse struct {
	TxID         string                   `json:"tx_id,omitempty"`
	Status       string                   `json:"status"`
	Rows         []map[string]interface{} `json:"rows,omitempty"`
	RowsAffected int                      `json:"rows_affected,omitempty"`
}

type BeginTxRequest struct {
	Mode    string   `json:"mode,omitempty"`
	Domains []string `json:"domains"`
}

type BeginTxResponse struct {
	TxID string `json:"tx_id"`
}

type CommitTxRequest struct {
	TxID         string `json:"tx_id"`
	Group        string `json:"group,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	FencingToken string `json:"fencing_token,omitempty"`
}

type CommitTxResponse struct {
	Status    string `json:"status"`
	CommitLSN uint64 `json:"commit_lsn"`
}

type RollbackTxRequest struct {
	TxID string `json:"tx_id"`
}

type RollbackTxResponse struct {
	Status string `json:"status"`
}

type ReplayToLSNRequest struct {
	LSN uint64 `json:"lsn"`
}

type ReplayToLSNResponse struct {
	AppliedLSN uint64 `json:"applied_lsn"`
}

type TimeTravelQueryRequest struct {
	SQL              string   `json:"sql"`
	Domains          []string `json:"domains,omitempty"`
	LSN              uint64   `json:"lsn,omitempty"`
	LogicalTimestamp uint64   `json:"logical_timestamp,omitempty"`
}

type TimeTravelQueryResponse struct {
	Status string                   `json:"status"`
	Rows   []map[string]interface{} `json:"rows,omitempty"`
}

type ExplainQueryRequest struct {
	SQL     string   `json:"sql"`
	Domains []string `json:"domains,omitempty"`
}

type ExplainQueryResponse struct {
	Status string                   `json:"status"`
	Rows   []map[string]interface{} `json:"rows,omitempty"`
}

type ScanStrategyStatsRequest struct{}

type ScanStrategyStatsResponse struct {
	Counts map[string]uint64 `json:"counts,omitempty"`
}

type EvaluateReadRouteRequest struct {
	Consistency         string `json:"consistency,omitempty"`
	LeaderLSN           uint64 `json:"leader_lsn,omitempty"`
	FollowerLSN         uint64 `json:"follower_lsn,omitempty"`
	HasFollower         bool   `json:"has_follower,omitempty"`
	FollowerUnavailable bool   `json:"follower_unavailable,omitempty"`
	MaxLag              uint64 `json:"max_lag,omitempty"`
}

type EvaluateReadRouteResponse struct {
	Mode           string `json:"mode"`
	Route          string `json:"route"`
	LeaderLSN      uint64 `json:"leader_lsn"`
	FollowerLSN    uint64 `json:"follower_lsn"`
	Lag            uint64 `json:"lag"`
	FallbackReason string `json:"fallback_reason,omitempty"`
}

type ReadRoutingStatsRequest struct{}

type ReadRoutingStatsResponse struct {
	Counts map[string]uint64 `json:"counts,omitempty"`
}

type LeadershipStateRequest struct {
	Group string `json:"group"`
}

type LeadershipStateResponse struct {
	Group              string     `json:"group"`
	Term               uint64     `json:"term"`
	LeaderID           string     `json:"leader_id"`
	FencingToken       string     `json:"fencing_token,omitempty"`
	LeaseExpiresAtUnix int64      `json:"lease_expires_at_unix"`
	LeaseActive        bool       `json:"lease_active"`
	LastLeaderLSN      uint64     `json:"last_leader_lsn"`
	// Peers carries the node's known peer list for address gossip.
	Peers []PeerInfo `json:"peers,omitempty"`
}

type SchemaSnapshotRequest struct {
	Domains []string `json:"domains,omitempty"`
}

type SchemaSnapshotColumn struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	PrimaryKey       bool   `json:"primary_key,omitempty"`
	Unique           bool   `json:"unique,omitempty"`
	ReferencesTable  string `json:"references_table,omitempty"`
	ReferencesColumn string `json:"references_column,omitempty"`
	DefaultValue     string `json:"default_value,omitempty"`
}

type SchemaSnapshotIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Method  string   `json:"method"`
}

type SchemaSnapshotVersionedFK struct {
	Column           string `json:"column"`
	LSNColumn        string `json:"lsn_column"`
	ReferencesDomain string `json:"references_domain"`
	ReferencesTable  string `json:"references_table"`
	ReferencesColumn string `json:"references_column"`
}

type SchemaSnapshotTable struct {
	Name                 string                      `json:"name"`
	Columns              []SchemaSnapshotColumn      `json:"columns"`
	Indexes              []SchemaSnapshotIndex       `json:"indexes,omitempty"`
	VersionedForeignKeys []SchemaSnapshotVersionedFK `json:"versioned_foreign_keys,omitempty"`
}

type SchemaSnapshotDomain struct {
	Name     string                 `json:"name"`
	Tables   []SchemaSnapshotTable  `json:"tables"`
	Entities []SchemaSnapshotEntity `json:"entities,omitempty"`
}

type SchemaSnapshotEntity struct {
	Name      string   `json:"name"`
	RootTable string   `json:"root_table"`
	Tables    []string `json:"tables"`
}

type SchemaSnapshotResponse struct {
	Status  string                 `json:"status"`
	Domains []SchemaSnapshotDomain `json:"domains"`
}

type RowHistoryRequest struct {
	SQL     string   `json:"sql"`
	Domains []string `json:"domains,omitempty"`
}

type RowHistoryResponse struct {
	Status string                   `json:"status"`
	Rows   []map[string]interface{} `json:"rows,omitempty"`
}

type EntityVersionHistoryRequest struct {
	Domain     string `json:"domain"`
	EntityName string `json:"entity_name"`
	RootPK     string `json:"root_pk"`
}

type EntityVersionHistoryEntry struct {
	Version   uint64   `json:"version"`
	CommitLSN uint64   `json:"commit_lsn"`
	Tables    []string `json:"tables"`
}

type EntityVersionHistoryResponse struct {
	Status   string                      `json:"status"`
	Entity   string                      `json:"entity"`
	RootPK   string                      `json:"root_pk"`
	Versions []EntityVersionHistoryEntry `json:"versions"`
}

type EngineStatsRequest struct{}

type QueryRequest struct {
	SQL     string   `json:"sql"`
	Domains []string `json:"domains,omitempty"`
}

type QueryResponse struct {
	Status string                   `json:"status"`
	Rows   []map[string]interface{} `json:"rows,omitempty"`
}

// SystemInfoResponse holds OS and Go runtime health metrics.
type SystemInfoResponse struct {
	Hostname        string  `json:"hostname"`
	OS              string  `json:"os"`
	Arch            string  `json:"arch"`
	NumCPU          int     `json:"num_cpu"`
	PID             int     `json:"pid"`
	GoVersion       string  `json:"go_version"`
	NumGoroutine    int     `json:"num_goroutine"`
	UptimeMS        int64   `json:"uptime_ms"`
	HeapAllocBytes  uint64  `json:"heap_alloc_bytes"`
	HeapSysBytes    uint64  `json:"heap_sys_bytes"`
	HeapInuseBytes  uint64  `json:"heap_inuse_bytes"`
	HeapObjects     uint64  `json:"heap_objects"`
	StackInuseBytes uint64  `json:"stack_inuse_bytes"`
	TotalAllocBytes uint64  `json:"total_alloc_bytes"`
	SysBytes        uint64  `json:"sys_bytes"`
	GCCycles        uint32  `json:"gc_cycles"`
	LastGCPauseNS   uint64  `json:"last_gc_pause_ns"`
	GCPauseTotalNS  uint64  `json:"gc_pause_total_ns"`
	GCCPUFraction   float64 `json:"gc_cpu_fraction"`
}

type EngineStatsResponse struct {
	TotalCommits           uint64              `json:"total_commits"`
	TotalReads             uint64              `json:"total_reads"`
	TotalRollbacks         uint64              `json:"total_rollbacks"`
	TotalBegins            uint64              `json:"total_begins"`
	TotalTimeTravelQueries uint64              `json:"total_time_travel_queries"`
	TotalSnapshots         uint64              `json:"total_snapshots"`
	TotalReplays           uint64              `json:"total_replays"`
	TotalFsyncErrors       uint64              `json:"total_fsync_errors"`
	TotalAuditErrors       uint64              `json:"total_audit_errors"`
	ActiveTransactions     int64               `json:"active_transactions"`
	CommitLatencyP50       float64             `json:"commit_latency_p50_ms"`
	CommitLatencyP95       float64             `json:"commit_latency_p95_ms"`
	CommitLatencyP99       float64             `json:"commit_latency_p99_ms"`
	FsyncLatencyP50        float64             `json:"fsync_latency_p50_ms"`
	FsyncLatencyP95        float64             `json:"fsync_latency_p95_ms"`
	FsyncLatencyP99        float64             `json:"fsync_latency_p99_ms"`
	ReadLatencyP50         float64             `json:"read_latency_p50_ms"`
	ReadLatencyP95         float64             `json:"read_latency_p95_ms"`
	ReadLatencyP99         float64             `json:"read_latency_p99_ms"`
	TimeTravelLatencyP50   float64             `json:"time_travel_latency_p50_ms"`
	TimeTravelLatencyP95   float64             `json:"time_travel_latency_p95_ms"`
	TimeTravelLatencyP99   float64             `json:"time_travel_latency_p99_ms"`
	ReplayDurationMS       float64             `json:"replay_duration_ms"`
	SnapshotDurationMS     float64             `json:"snapshot_duration_ms"`
	CommitThroughput       float64             `json:"commit_throughput_per_sec"`
	ReadThroughput         float64             `json:"read_throughput_per_sec"`
	WALFileSize            int64               `json:"wal_file_size_bytes"`
	SnapshotFileSize       int64               `json:"snapshot_file_size_bytes"`
	AuditFileSize          int64               `json:"audit_file_size_bytes"`
	System                 *SystemInfoResponse `json:"system,omitempty"`
}

type ExecuteBatchRequest struct {
	TxID       string   `json:"tx_id"`
	Statements []string `json:"statements"`
}

type ExecuteBatchResponse struct {
	Status   string `json:"status"`
	Executed int    `json:"executed"`
}

// TimelineCommitsRequest requests commit summaries for the timeline UI.
type TimelineCommitsRequest struct {
	FromLSN uint64 `json:"from_lsn,omitempty"`
	ToLSN   uint64 `json:"to_lsn,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	Domain  string `json:"domain,omitempty"`
}

// TimelineCommitsResponse returns aggregated commit summaries.
type TimelineCommitsResponse struct {
	Commits []TimelineCommitEntry `json:"commits"`
}

// TimelineCommitEntry is a single commit summary for the timeline.
type TimelineCommitEntry struct {
	LSN       uint64                        `json:"lsn"`
	TxID      string                        `json:"tx_id"`
	Timestamp uint64                        `json:"timestamp"`
	Tables    []TimelineCommitMutationEntry `json:"tables"`
}

// TimelineCommitMutationEntry describes one mutation within a commit.
type TimelineCommitMutationEntry struct {
	Domain    string `json:"domain"`
	Table     string `json:"table"`
	Operation string `json:"operation"`
}

// PeerInfo carries a peer's identity and gRPC address.
type PeerInfo struct {
	NodeID        string `json:"node_id"`
	Address       string `json:"address"`        // gRPC address
	PgwireAddress string `json:"pgwire_address"` // pgwire SQL address; may be empty for static peers
}

// JoinClusterRequest is sent by a new node to announce itself to an existing peer.
type JoinClusterRequest struct {
	NodeID        string   `json:"node_id"`                // unique node identifier of the joining node
	Address       string   `json:"address"`               // cluster gRPC address the joining node is listening on
	PgwireAddress string   `json:"pgwire_address"`        // pgwire SQL address for Studio / client connections
	Groups        []string `json:"groups,omitempty"`      // domain groups the joining node participates in
}

// JoinClusterResponse is returned by the seed peer after accepting a join.
type JoinClusterResponse struct {
	Accepted      bool       `json:"accepted"`
	LeaderID      string     `json:"leader_id,omitempty"`      // current leader node ID (if known)
	LeaderAddress string     `json:"leader_address,omitempty"` // cluster gRPC address of the leader
	KnownPeers    []PeerInfo `json:"known_peers,omitempty"`    // all peers the seed knows about
}

type ASQLServiceServer interface {
	Execute(context.Context, *ExecuteRequest) (*ExecuteResponse, error)
	ExecuteBatch(context.Context, *ExecuteBatchRequest) (*ExecuteBatchResponse, error)
	BeginTx(context.Context, *BeginTxRequest) (*BeginTxResponse, error)
	CommitTx(context.Context, *CommitTxRequest) (*CommitTxResponse, error)
	RollbackTx(context.Context, *RollbackTxRequest) (*RollbackTxResponse, error)
	ReplayToLSN(context.Context, *ReplayToLSNRequest) (*ReplayToLSNResponse, error)
	TimeTravelQuery(context.Context, *TimeTravelQueryRequest) (*TimeTravelQueryResponse, error)
	ExplainQuery(context.Context, *ExplainQueryRequest) (*ExplainQueryResponse, error)
	ScanStrategyStats(context.Context, *ScanStrategyStatsRequest) (*ScanStrategyStatsResponse, error)
	EvaluateReadRoute(context.Context, *EvaluateReadRouteRequest) (*EvaluateReadRouteResponse, error)
	ReadRoutingStats(context.Context, *ReadRoutingStatsRequest) (*ReadRoutingStatsResponse, error)
	LeadershipState(context.Context, *LeadershipStateRequest) (*LeadershipStateResponse, error)
	SchemaSnapshot(context.Context, *SchemaSnapshotRequest) (*SchemaSnapshotResponse, error)
	RowHistory(context.Context, *RowHistoryRequest) (*RowHistoryResponse, error)
	EntityVersionHistory(context.Context, *EntityVersionHistoryRequest) (*EntityVersionHistoryResponse, error)
	EngineStats(context.Context, *EngineStatsRequest) (*EngineStatsResponse, error)
	Query(context.Context, *QueryRequest) (*QueryResponse, error)
	TimelineCommits(context.Context, *TimelineCommitsRequest) (*TimelineCommitsResponse, error)
	JoinCluster(context.Context, *JoinClusterRequest) (*JoinClusterResponse, error)
}

func registerASQLServiceServer(server *grpcgo.Server, implementation ASQLServiceServer) {
	server.RegisterService(&grpcgo.ServiceDesc{
		ServiceName: "asql.v1.ASQLService",
		HandlerType: (*ASQLServiceServer)(nil),
		Methods: []grpcgo.MethodDesc{
			{MethodName: "Execute", Handler: executeHandler},
			{MethodName: "ExecuteBatch", Handler: executeBatchHandler},
			{MethodName: "BeginTx", Handler: beginTxHandler},
			{MethodName: "CommitTx", Handler: commitTxHandler},
			{MethodName: "RollbackTx", Handler: rollbackTxHandler},
			{MethodName: "ReplayToLSN", Handler: replayToLSNHandler},
			{MethodName: "TimeTravelQuery", Handler: timeTravelQueryHandler},
			{MethodName: "ExplainQuery", Handler: explainQueryHandler},
			{MethodName: "ScanStrategyStats", Handler: scanStrategyStatsHandler},
			{MethodName: "EvaluateReadRoute", Handler: evaluateReadRouteHandler},
			{MethodName: "ReadRoutingStats", Handler: readRoutingStatsHandler},
			{MethodName: "LeadershipState", Handler: leadershipStateHandler},
			{MethodName: "SchemaSnapshot", Handler: schemaSnapshotHandler},
			{MethodName: "RowHistory", Handler: rowHistoryHandler},
			{MethodName: "EntityVersionHistory", Handler: entityVersionHistoryHandler},
			{MethodName: "EngineStats", Handler: engineStatsHandler},
			{MethodName: "Query", Handler: queryHandler},
			{MethodName: "TimelineCommits", Handler: timelineCommitsHandler},
			{MethodName: "JoinCluster", Handler: joinClusterHandler},
		},
	}, implementation)
}

func executeHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(ExecuteRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).Execute(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/Execute"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).Execute(innerCtx, innerReq.(*ExecuteRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func executeBatchHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(ExecuteBatchRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).ExecuteBatch(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/ExecuteBatch"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).ExecuteBatch(innerCtx, innerReq.(*ExecuteBatchRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func beginTxHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(BeginTxRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).BeginTx(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/BeginTx"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).BeginTx(innerCtx, innerReq.(*BeginTxRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func commitTxHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(CommitTxRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).CommitTx(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/CommitTx"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).CommitTx(innerCtx, innerReq.(*CommitTxRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func rollbackTxHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(RollbackTxRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).RollbackTx(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/RollbackTx"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).RollbackTx(innerCtx, innerReq.(*RollbackTxRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func replayToLSNHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(ReplayToLSNRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).ReplayToLSN(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/ReplayToLSN"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).ReplayToLSN(innerCtx, innerReq.(*ReplayToLSNRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func timeTravelQueryHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(TimeTravelQueryRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).TimeTravelQuery(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/TimeTravelQuery"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).TimeTravelQuery(innerCtx, innerReq.(*TimeTravelQueryRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func explainQueryHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(ExplainQueryRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).ExplainQuery(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/ExplainQuery"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).ExplainQuery(innerCtx, innerReq.(*ExplainQueryRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func scanStrategyStatsHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(ScanStrategyStatsRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).ScanStrategyStats(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/ScanStrategyStats"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).ScanStrategyStats(innerCtx, innerReq.(*ScanStrategyStatsRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func evaluateReadRouteHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(EvaluateReadRouteRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).EvaluateReadRoute(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/EvaluateReadRoute"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).EvaluateReadRoute(innerCtx, innerReq.(*EvaluateReadRouteRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func readRoutingStatsHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(ReadRoutingStatsRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).ReadRoutingStats(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/ReadRoutingStats"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).ReadRoutingStats(innerCtx, innerReq.(*ReadRoutingStatsRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func leadershipStateHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(LeadershipStateRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).LeadershipState(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/LeadershipState"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).LeadershipState(innerCtx, innerReq.(*LeadershipStateRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func schemaSnapshotHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(SchemaSnapshotRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).SchemaSnapshot(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/SchemaSnapshot"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).SchemaSnapshot(innerCtx, innerReq.(*SchemaSnapshotRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func rowHistoryHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(RowHistoryRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).RowHistory(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/RowHistory"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).RowHistory(innerCtx, innerReq.(*RowHistoryRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func entityVersionHistoryHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(EntityVersionHistoryRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).EntityVersionHistory(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/EntityVersionHistory"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).EntityVersionHistory(innerCtx, innerReq.(*EntityVersionHistoryRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func engineStatsHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(EngineStatsRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).EngineStats(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/EngineStats"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).EngineStats(innerCtx, innerReq.(*EngineStatsRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func queryHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(QueryRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).Query(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/Query"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).Query(innerCtx, innerReq.(*QueryRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func joinClusterHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(JoinClusterRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).JoinCluster(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/JoinCluster"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).JoinCluster(innerCtx, innerReq.(*JoinClusterRequest))
	}

	return interceptor(ctx, request, info, handler)
}

func timelineCommitsHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(TimelineCommitsRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ASQLServiceServer).TimelineCommits(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ASQLService/TimelineCommits"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ASQLServiceServer).TimelineCommits(innerCtx, innerReq.(*TimelineCommitsRequest))
	}

	return interceptor(ctx, request, info, handler)
}
