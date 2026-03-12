package httpapi

import (
	"encoding/json"
	"fmt"

	"asql/internal/engine/parser/ast"
)

// ─── Transaction request/response types ─────────────────────────────────────

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

type ExecuteBatchRequest struct {
	TxID       string   `json:"tx_id"`
	Statements []string `json:"statements"`
}

type ExecuteBatchResponse struct {
	Status   string `json:"status"`
	Executed int    `json:"executed"`
}

// ─── Query types ────────────────────────────────────────────────────────────

type QueryRequest struct {
	SQL     string   `json:"sql"`
	Domains []string `json:"domains,omitempty"`
}

type QueryResponse struct {
	Status string                   `json:"status"`
	Rows   []map[string]interface{} `json:"rows,omitempty"`
}

// ─── Time-travel types ──────────────────────────────────────────────────────

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

// ─── Explain/plan types ─────────────────────────────────────────────────────

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

// ─── Admin / observability types ────────────────────────────────────────────

type ReplayToLSNRequest struct {
	LSN uint64 `json:"lsn"`
}

type ReplayToLSNResponse struct {
	AppliedLSN uint64 `json:"applied_lsn"`
}

type EngineStatsRequest struct{}

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
	TotalCommits               uint64              `json:"total_commits"`
	TotalReads                 uint64              `json:"total_reads"`
	TotalRollbacks             uint64              `json:"total_rollbacks"`
	TotalBegins                uint64              `json:"total_begins"`
	TotalCrossDomainBegins     uint64              `json:"total_cross_domain_begins"`
	TotalTimeTravelQueries     uint64              `json:"total_time_travel_queries"`
	TotalSnapshots             uint64              `json:"total_snapshots"`
	TotalReplays               uint64              `json:"total_replays"`
	TotalFsyncErrors           uint64              `json:"total_fsync_errors"`
	TotalAuditErrors           uint64              `json:"total_audit_errors"`
	ActiveTransactions         int64               `json:"active_transactions"`
	CrossDomainBeginAvgDomains float64             `json:"cross_domain_begin_avg_domains"`
	CrossDomainBeginMaxDomains uint64              `json:"cross_domain_begin_max_domains"`
	CommitLatencyP50           float64             `json:"commit_latency_p50_ms"`
	CommitLatencyP95           float64             `json:"commit_latency_p95_ms"`
	CommitLatencyP99           float64             `json:"commit_latency_p99_ms"`
	FsyncLatencyP50            float64             `json:"fsync_latency_p50_ms"`
	FsyncLatencyP95            float64             `json:"fsync_latency_p95_ms"`
	FsyncLatencyP99            float64             `json:"fsync_latency_p99_ms"`
	ReadLatencyP50             float64             `json:"read_latency_p50_ms"`
	ReadLatencyP95             float64             `json:"read_latency_p95_ms"`
	ReadLatencyP99             float64             `json:"read_latency_p99_ms"`
	TimeTravelLatencyP50       float64             `json:"time_travel_latency_p50_ms"`
	TimeTravelLatencyP95       float64             `json:"time_travel_latency_p95_ms"`
	TimeTravelLatencyP99       float64             `json:"time_travel_latency_p99_ms"`
	ReplayDurationMS           float64             `json:"replay_duration_ms"`
	SnapshotDurationMS         float64             `json:"snapshot_duration_ms"`
	CommitThroughput           float64             `json:"commit_throughput_per_sec"`
	ReadThroughput             float64             `json:"read_throughput_per_sec"`
	WALFileSize                int64               `json:"wal_file_size_bytes"`
	SnapshotFileSize           int64               `json:"snapshot_file_size_bytes"`
	AuditFileSize              int64               `json:"audit_file_size_bytes"`
	System                     *SystemInfoResponse `json:"system,omitempty"`
}

// ─── Read routing types ─────────────────────────────────────────────────────

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

// ─── Cluster / leadership types ─────────────────────────────────────────────

type LeadershipStateRequest struct {
	Group string `json:"group"`
}

type LeadershipStateResponse struct {
	Group              string `json:"group"`
	Term               uint64 `json:"term"`
	LeaderID           string `json:"leader_id"`
	FencingToken       string `json:"fencing_token,omitempty"`
	LeaseExpiresAtUnix int64  `json:"lease_expires_at_unix"`
	LeaseActive        bool   `json:"lease_active"`
	LastLeaderLSN      uint64 `json:"last_leader_lsn"`
}

// ─── Schema snapshot types ──────────────────────────────────────────────────

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

// ─── Timeline types ─────────────────────────────────────────────────────────

type TimelineCommitsRequest struct {
	FromLSN uint64 `json:"from_lsn,omitempty"`
	ToLSN   uint64 `json:"to_lsn,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	Domain  string `json:"domain,omitempty"`
}

type TimelineCommitsResponse struct {
	Commits []TimelineCommitEntry `json:"commits"`
}

type TimelineCommitEntry struct {
	LSN       uint64                        `json:"lsn"`
	TxID      string                        `json:"tx_id"`
	Timestamp uint64                        `json:"timestamp"`
	Tables    []TimelineCommitMutationEntry `json:"tables"`
}

type TimelineCommitMutationEntry struct {
	Domain    string `json:"domain"`
	Table     string `json:"table"`
	Operation string `json:"operation"`
}

// ─── Timeline Events types ─────────────────────────────────────────────────

type TimelineEventsResponse struct {
	MemorySnapshots []TimelineSnapshotPoint `json:"memory_snapshots"`
	DiskSnapshots   []TimelineSnapshotPoint `json:"disk_snapshots"`
}

type TimelineSnapshotPoint struct {
	LSN uint64 `json:"lsn"`
}

// ─── Replication types ──────────────────────────────────────────────────────

type LastLSNResponse struct {
	LSN uint64 `json:"lsn"`
}

// ─── Helpers ────────────────────────────────────────────────────────────────

// normalizeRows converts ast.Literal maps to generic interface{} maps for
// JSON serialization.
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
				converted[key] = value.NumberValue // keep as microsecond epoch
			case ast.LiteralJSON:
				var parsed interface{}
				if jsonErr := json.Unmarshal([]byte(value.StringValue), &parsed); jsonErr == nil {
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

// formatDefaultValue renders a DefaultExpr for JSON output.
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
