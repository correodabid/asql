package studioapp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"asql/pkg/fixtures"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	api "asql/pkg/adminapi"
)

// engineInvoker abstracts pgwire engine calls used by schema_apply.go and the studio handlers.
type engineInvoker interface {
	BeginTx(ctx context.Context, req *api.BeginTxRequest) (*api.BeginTxResponse, error)
	Execute(ctx context.Context, req *api.ExecuteRequest) (*api.ExecuteResponse, error)
	ExecuteBatch(ctx context.Context, req *api.ExecuteBatchRequest) (*api.ExecuteBatchResponse, error)
	CommitTx(ctx context.Context, req *api.CommitTxRequest) (*api.CommitTxResponse, error)
	RollbackTx(ctx context.Context, req *api.RollbackTxRequest) (*api.RollbackTxResponse, error)
	SchemaSnapshot(ctx context.Context, req *api.SchemaSnapshotRequest) (*api.SchemaSnapshotResponse, error)
}

// engineClient talks to the ASQL engine over the PostgreSQL wire protocol (pgwire).
//
// All connections are acquired from the shared pool – this guarantees that if
// any pool operation succeeds, dedicated-connection operations (time-travel,
// transactions, explain, …) will also succeed. Using pgx.Connect() directly
// bypasses the pool and can fail with "connection refused" even when the pool
// already holds live connections (e.g. after a server restart).
type engineClient struct {
	addr     string
	nodeID   string // resolved from cluster_members; empty until first sync
	password string
	pool     *pgxpool.Pool

	mu      sync.Mutex
	txConns map[string]*pgxpool.Conn
}

func newEngineClient(addr, authToken string) *engineClient {
	// Strip http(s):// if the user accidentally passes an HTTP endpoint.
	addr = strings.TrimPrefix(strings.TrimPrefix(addr, "https://"), "http://")
	if idx := strings.Index(addr, "/"); idx != -1 {
		addr = addr[:idx]
	}

	connStr := buildConnStr(addr, authToken)
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		cfg = &pgxpool.Config{}
	}
	cfg.MaxConns = 10
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, _ := pgxpool.NewWithConfig(context.Background(), cfg)

	return &engineClient{
		addr:     addr,
		password: authToken,
		pool:     pool,
		txConns:  make(map[string]*pgxpool.Conn),
	}
}

func buildConnStr(addr, password string) string {
	if password != "" {
		return fmt.Sprintf("postgres://asql:%s@%s/asql?sslmode=disable", password, addr)
	}
	return fmt.Sprintf("postgres://asql@%s/asql?sslmode=disable", addr)
}

// ── Transaction-scoped connection management ──────────────────────────────────

// acquireConn borrows a connection from the pool. The caller must call
// conn.Release() when done (or conn.Conn().Close(ctx) is NOT used).
func (c *engineClient) acquireConn(ctx context.Context) (*pgxpool.Conn, error) {
	return c.pool.Acquire(ctx)
}

func (c *engineClient) storeTxConn(txID string, conn *pgxpool.Conn) {
	c.mu.Lock()
	c.txConns[txID] = conn
	c.mu.Unlock()
}

// peekTxConn returns the connection for txID without removing it
// (used by Execute/ExecuteBatch which keep the tx open).
func (c *engineClient) peekTxConn(txID string) (*pgxpool.Conn, bool) {
	c.mu.Lock()
	conn, ok := c.txConns[txID]
	c.mu.Unlock()
	return conn, ok
}

// takeTxConn removes and returns the connection for txID
// (used by Commit/Rollback which end the tx).
func (c *engineClient) takeTxConn(txID string) (*pgxpool.Conn, bool) {
	c.mu.Lock()
	conn, ok := c.txConns[txID]
	if ok {
		delete(c.txConns, txID)
	}
	c.mu.Unlock()
	return conn, ok
}

// ── Query helpers ─────────────────────────────────────────────────────────────

// pgxRowsToMaps converts pgx rows to the generic map format used by API responses.
// It reads raw wire bytes (RawValues) rather than decoded Go values (Values) to avoid
// pgx type-decoder errors when the server sends an unusual byte sequence for a typed
// column (e.g. OID 1114 TIMESTAMP receiving an empty or RFC3339-formatted byte slice).
// Column values are decoded as UTF-8 strings; NULL wire values become nil.
func pgxRowsToMaps(rows pgx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()
	fds := rows.FieldDescriptions()
	var result []map[string]interface{}
	for rows.Next() {
		raw := rows.RawValues()
		row := make(map[string]interface{}, len(fds))
		for i, fd := range fds {
			if i >= len(raw) || raw[i] == nil {
				row[string(fd.Name)] = nil
				continue
			}
			row[string(fd.Name)] = string(raw[i])
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// randomID generates a short random hex string suitable for use as a tx_id.
func randomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ── Per-method handlers ───────────────────────────────────────────────────────

func (c *engineClient) BeginTx(ctx context.Context, req *api.BeginTxRequest) (*api.BeginTxResponse, error) {
	conn, err := c.acquireConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open tx connection: %w", err)
	}

	mode := strings.ToLower(strings.TrimSpace(req.Mode))
	var beginSQL string
	switch mode {
	case "cross", "cross_domain":
		beginSQL = "BEGIN CROSS DOMAIN " + strings.Join(req.Domains, ", ")
	default:
		if len(req.Domains) == 1 {
			beginSQL = "BEGIN DOMAIN " + req.Domains[0]
		} else {
			beginSQL = "BEGIN CROSS DOMAIN " + strings.Join(req.Domains, ", ")
		}
	}

	if _, execErr := conn.Exec(ctx, beginSQL); execErr != nil {
		conn.Release()
		return nil, fmt.Errorf("begin transaction: %w", execErr)
	}

	txID := randomID()
	c.storeTxConn(txID, conn)
	return &api.BeginTxResponse{TxID: txID}, nil
}

func (c *engineClient) Execute(ctx context.Context, req *api.ExecuteRequest) (*api.ExecuteResponse, error) {
	conn, ok := c.peekTxConn(req.TxID)
	if !ok {
		return nil, fmt.Errorf("transaction %q not found or already closed", req.TxID)
	}
	rows, err := conn.Query(ctx, req.SQL)
	if err != nil {
		return nil, fmt.Errorf("execute sql: %w", err)
	}
	result, err := pgxRowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("collect rows: %w", err)
	}
	return &api.ExecuteResponse{Status: "OK", TxID: req.TxID, Rows: result}, nil
}

func (c *engineClient) ExecuteBatch(ctx context.Context, req *api.ExecuteBatchRequest) (*api.ExecuteBatchResponse, error) {
	conn, ok := c.peekTxConn(req.TxID)
	if !ok {
		return nil, fmt.Errorf("transaction %q not found or already closed", req.TxID)
	}
	for _, stmt := range req.Statements {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("execute batch statement %q: %w", stmt, err)
		}
	}
	return &api.ExecuteBatchResponse{Status: "OK", Executed: len(req.Statements)}, nil
}

func (c *engineClient) CommitTx(ctx context.Context, req *api.CommitTxRequest) (*api.CommitTxResponse, error) {
	conn, ok := c.takeTxConn(req.TxID)
	if !ok {
		return nil, fmt.Errorf("transaction %q not found or already closed", req.TxID)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	var lsn int64
	_ = conn.QueryRow(ctx, "SELECT last_lsn FROM asql_admin.replication_status").Scan(&lsn)
	return &api.CommitTxResponse{Status: "OK", CommitLSN: uint64(lsn)}, nil
}

func (c *engineClient) RollbackTx(ctx context.Context, req *api.RollbackTxRequest) (*api.RollbackTxResponse, error) {
	conn, ok := c.takeTxConn(req.TxID)
	if !ok {
		return &api.RollbackTxResponse{Status: "OK"}, nil
	}
	defer conn.Release()
	_, _ = conn.Exec(ctx, "ROLLBACK")
	return &api.RollbackTxResponse{Status: "OK"}, nil
}

// queryWithDomains acquires a connection from the pool, sets domain context
// with BEGIN DOMAIN / BEGIN CROSS DOMAIN, runs sql, then issues ROLLBACK and
// releases the connection back to the pool. Used for read-only user-table
// queries (time-travel, explain, etc.) that need a session to resolve table names.
func (c *engineClient) queryWithDomains(ctx context.Context, domains []string, sql string) ([]map[string]interface{}, error) {
	conn, err := c.acquireConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	defer conn.Release()

	if len(domains) > 0 {
		var beginSQL string
		if len(domains) == 1 {
			beginSQL = "BEGIN DOMAIN " + domains[0]
		} else {
			beginSQL = "BEGIN CROSS DOMAIN " + strings.Join(domains, ", ")
		}
		if _, err := conn.Exec(ctx, beginSQL); err != nil {
			return nil, fmt.Errorf("set domain context: %w", err)
		}
		defer func() { _, _ = conn.Exec(ctx, "ROLLBACK") }()
	}

	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	return pgxRowsToMaps(rows)
}

func (c *engineClient) TimeTravelQuery(ctx context.Context, req *api.TimeTravelQueryRequest) (*api.TimeTravelQueryResponse, error) {
	sql := req.SQL
	if req.LSN != 0 {
		sql = fmt.Sprintf("%s /* as-of-lsn: %d */", sql, req.LSN)
	} else if req.LogicalTimestamp != 0 {
		sql = fmt.Sprintf("%s /* as-of-ts: %d */", sql, req.LogicalTimestamp)
	}
	result, err := c.queryWithDomains(ctx, req.Domains, sql)
	if err != nil {
		return nil, fmt.Errorf("time travel query: %w", err)
	}
	return &api.TimeTravelQueryResponse{Status: "OK", Rows: result}, nil
}

func (c *engineClient) RowHistory(ctx context.Context, req *api.RowHistoryRequest) (*api.RowHistoryResponse, error) {
	query := fmt.Sprintf("SELECT * FROM asql_admin.row_history WHERE sql = '%s'", pgEscape(req.SQL))
	result, err := c.queryWithDomains(ctx, req.Domains, query)
	if err != nil {
		return nil, fmt.Errorf("row history: %w", err)
	}
	return &api.RowHistoryResponse{Status: "OK", Rows: result}, nil
}

func (c *engineClient) EntityVersionHistory(ctx context.Context, req *api.EntityVersionHistoryRequest) (*api.EntityVersionHistoryResponse, error) {
	query := fmt.Sprintf(
		"SELECT * FROM asql_admin.entity_version_history WHERE domain = '%s' AND entity = '%s' AND root_pk = '%s'",
		pgEscape(req.Domain), pgEscape(req.EntityName), pgEscape(req.RootPK),
	)
	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("entity version history: %w", err)
	}
	resultRows, err := pgxRowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("collect entity version history: %w", err)
	}
	entries := make([]api.EntityVersionHistoryEntry, 0, len(resultRows))
	for _, row := range resultRows {
		var entry api.EntityVersionHistoryEntry
		entry.Version = toUint64(row["version"])
		entry.CommitLSN = toUint64(row["commit_lsn"])
		if s := toString(row["tables"]); s != "" {
			entry.Tables = strings.Split(s, ",")
		}
		entries = append(entries, entry)
	}
	return &api.EntityVersionHistoryResponse{
		Status: "OK", Entity: req.EntityName, RootPK: req.RootPK, Versions: entries,
	}, nil
}

func (c *engineClient) ExplainQuery(ctx context.Context, req *api.ExplainQueryRequest) (*api.ExplainQueryResponse, error) {
	result, err := c.queryWithDomains(ctx, req.Domains, "EXPLAIN "+req.SQL)
	if err != nil {
		return nil, fmt.Errorf("explain: %w", err)
	}
	return &api.ExplainQueryResponse{Status: "OK", Rows: result}, nil
}

func (c *engineClient) SchemaSnapshot(ctx context.Context, req *api.SchemaSnapshotRequest) (*api.SchemaSnapshotResponse, error) {
	query := "SELECT snapshot FROM asql_admin.schema_snapshot"
	if len(req.Domains) > 0 {
		quoted := make([]string, len(req.Domains))
		for i, d := range req.Domains {
			quoted[i] = "'" + pgEscape(d) + "'"
		}
		query += fmt.Sprintf(" WHERE domain IN (%s)", strings.Join(quoted, ","))
	}
	var snapshotJSON string
	if err := c.pool.QueryRow(ctx, query).Scan(&snapshotJSON); err != nil {
		return nil, fmt.Errorf("schema snapshot: %w", err)
	}
	var resp api.SchemaSnapshotResponse
	if err := json.Unmarshal([]byte(snapshotJSON), &resp); err != nil {
		return nil, fmt.Errorf("decode schema snapshot: %w", err)
	}
	return &resp, nil
}

func (c *engineClient) TimelineCommits(ctx context.Context, req *api.TimelineCommitsRequest) (*api.TimelineCommitsResponse, error) {
	var parts []string
	if req.FromLSN != 0 {
		parts = append(parts, fmt.Sprintf("from_lsn >= %d", req.FromLSN))
	}
	if req.ToLSN != 0 {
		parts = append(parts, fmt.Sprintf("to_lsn <= %d", req.ToLSN))
	}
	if req.Domain != "" {
		parts = append(parts, fmt.Sprintf("domain = '%s'", pgEscape(req.Domain)))
	}
	query := "SELECT * FROM asql_admin.timeline_commits"
	if len(parts) > 0 {
		query += " WHERE " + strings.Join(parts, " AND ")
	}
	if req.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", req.Limit)
	}
	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("timeline commits: %w", err)
	}
	resultRows, err := pgxRowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("collect timeline commits: %w", err)
	}
	commits := make([]api.TimelineCommitEntry, 0, len(resultRows))
	for _, row := range resultRows {
		commits = append(commits, api.TimelineCommitEntry{
			LSN:       toUint64(row["lsn"]),
			TxID:      toString(row["tx_id"]),
			Timestamp: toUint64(row["timestamp"]),
		})
	}
	return &api.TimelineCommitsResponse{Commits: commits}, nil
}

func (c *engineClient) ScanStrategyStats(ctx context.Context) (*api.ScanStrategyStatsResponse, error) {
	rows, err := c.pool.Query(ctx, "SELECT strategy, count FROM asql_admin.scan_strategy_stats")
	if err != nil {
		return nil, fmt.Errorf("scan strategy stats: %w", err)
	}
	resultRows, err := pgxRowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("collect scan strategy stats: %w", err)
	}
	counts := make(map[string]uint64, len(resultRows))
	for _, row := range resultRows {
		counts[toString(row["strategy"])] = toUint64(row["count"])
	}
	return &api.ScanStrategyStatsResponse{Counts: counts}, nil
}

func (c *engineClient) EngineStats(ctx context.Context) (*api.EngineStatsResponse, error) {
	rows, err := c.pool.Query(ctx, "SELECT * FROM asql_admin.engine_stats")
	if err != nil {
		return nil, fmt.Errorf("engine stats: %w", err)
	}
	resultRows, err := pgxRowsToMaps(rows)
	if err != nil || len(resultRows) == 0 {
		return &api.EngineStatsResponse{}, nil
	}
	row := resultRows[0]
	return &api.EngineStatsResponse{
		TotalCommits:               toUint64(row["total_commits"]),
		TotalReads:                 toUint64(row["total_reads"]),
		TotalRollbacks:             toUint64(row["total_rollbacks"]),
		TotalBegins:                toUint64(row["total_begins"]),
		TotalCrossDomainBegins:     toUint64(row["total_cross_domain_begins"]),
		TotalTimeTravelQueries:     toUint64(row["total_time_travel_queries"]),
		TotalSnapshots:             toUint64(row["total_snapshots"]),
		TotalReplays:               toUint64(row["total_replays"]),
		TotalFsyncErrors:           toUint64(row["total_fsync_errors"]),
		TotalAuditErrors:           toUint64(row["total_audit_errors"]),
		ActiveTransactions:         int64(toUint64(row["active_transactions"])),
		CrossDomainBeginAvgDomains: toFloat64(row["cross_domain_begin_avg_domains"]),
		CrossDomainBeginMaxDomains: toUint64(row["cross_domain_begin_max_domains"]),
		CommitLatencyP50:           toFloat64(row["commit_latency_p50_ms"]),
		CommitLatencyP95:           toFloat64(row["commit_latency_p95_ms"]),
		CommitLatencyP99:           toFloat64(row["commit_latency_p99_ms"]),
		FsyncLatencyP50:            toFloat64(row["fsync_latency_p50_ms"]),
		FsyncLatencyP95:            toFloat64(row["fsync_latency_p95_ms"]),
		FsyncLatencyP99:            toFloat64(row["fsync_latency_p99_ms"]),
		ReadLatencyP50:             toFloat64(row["read_latency_p50_ms"]),
		ReadLatencyP95:             toFloat64(row["read_latency_p95_ms"]),
		ReadLatencyP99:             toFloat64(row["read_latency_p99_ms"]),
		TimeTravelLatencyP50:       toFloat64(row["time_travel_latency_p50_ms"]),
		TimeTravelLatencyP95:       toFloat64(row["time_travel_latency_p95_ms"]),
		TimeTravelLatencyP99:       toFloat64(row["time_travel_latency_p99_ms"]),
		ReplayDurationMS:           toFloat64(row["replay_duration_ms"]),
		SnapshotDurationMS:         toFloat64(row["snapshot_duration_ms"]),
		CommitThroughput:           toFloat64(row["commit_throughput_per_sec"]),
		ReadThroughput:             toFloat64(row["read_throughput_per_sec"]),
		WALFileSize:                int64(toUint64(row["wal_file_size_bytes"])),
		SnapshotFileSize:           int64(toUint64(row["snapshot_file_size_bytes"])),
		AuditFileSize:              int64(toUint64(row["audit_file_size_bytes"])),
	}, nil
}

func (c *engineClient) LeadershipState(ctx context.Context, req *api.LeadershipStateRequest) (*api.LeadershipStateResponse, error) {
	query := "SELECT * FROM asql_admin.leadership_state"
	if req.Group != "" {
		query += fmt.Sprintf(" WHERE group_name = '%s'", pgEscape(req.Group))
	}
	rows, err := c.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("leadership state: %w", err)
	}
	resultRows, err := pgxRowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("collect leadership state: %w", err)
	}
	if len(resultRows) == 0 {
		return &api.LeadershipStateResponse{}, nil
	}
	row := resultRows[0]
	return &api.LeadershipStateResponse{
		Group:        toString(row["group_name"]),
		Term:         toUint64(row["term"]),
		LeaderID:     toString(row["leader_id"]),
		FencingToken: toString(row["fencing_token"]),
		LeaseActive:  toBool(row["lease_active"]),
	}, nil
}

func (c *engineClient) LastLSN(ctx context.Context) (*api.LastLSNResponse, error) {
	var lsn int64
	if err := c.pool.QueryRow(ctx, "SELECT last_lsn FROM asql_admin.replication_status").Scan(&lsn); err != nil {
		return nil, fmt.Errorf("last lsn: %w", err)
	}
	return &api.LastLSNResponse{LSN: uint64(lsn)}, nil
}

// ClusterMember describes a node returned by asql_admin.cluster_members.
type ClusterMember struct {
	NodeID        string
	GRPCAddress   string
	PgwireAddress string
	IsSelf        bool
}

// ClusterMembers returns all known cluster nodes (self + peers) as reported
// by this engine's asql_admin.cluster_members virtual table.
func (c *engineClient) ClusterMembers(ctx context.Context) ([]ClusterMember, error) {
	rows, err := c.pool.Query(ctx, "SELECT * FROM asql_admin.cluster_members")
	if err != nil {
		return nil, fmt.Errorf("cluster members: %w", err)
	}
	resultRows, err := pgxRowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("collect cluster members: %w", err)
	}
	members := make([]ClusterMember, 0, len(resultRows))
	for _, row := range resultRows {
		members = append(members, ClusterMember{
			NodeID:        toString(row["node_id"]),
			GRPCAddress:   toString(row["grpc_address"]),
			PgwireAddress: toString(row["pgwire_address"]),
			IsSelf:        toBool(row["is_self"]),
		})
	}
	return members, nil
}

// leadershipStateResult holds one row from asql_admin.leadership_state.
type leadershipStateResult struct {
	GroupName   string
	Term        uint64
	LeaderID    string
	LeaseActive bool
	LastLSN     uint64
}

// LeadershipStates returns the current leadership state for every group known
// to this engine. Used by Studio to identify the current leader after failover.
func (c *engineClient) LeadershipStates(ctx context.Context) ([]leadershipStateResult, error) {
	rows, err := c.pool.Query(ctx, "SELECT * FROM asql_admin.leadership_state")
	if err != nil {
		return nil, fmt.Errorf("leadership states: %w", err)
	}
	resultRows, err := pgxRowsToMaps(rows)
	if err != nil {
		return nil, fmt.Errorf("collect leadership states: %w", err)
	}
	states := make([]leadershipStateResult, 0, len(resultRows))
	for _, row := range resultRows {
		states = append(states, leadershipStateResult{
			GroupName:   toString(row["group_name"]),
			Term:        toUint64(row["term"]),
			LeaderID:    toString(row["leader_id"]),
			LeaseActive: toBool(row["lease_active"]),
			LastLSN:     toUint64(row["last_leader_lsn"]),
		})
	}
	return states, nil
}

// nodeRoleResult is the Raft-authoritative role of this engine node.
type nodeRoleResult struct {
	Role          string // "leader", "follower", "candidate", "standalone", "unknown"
	LeaderAddr    string // pgwire address of the current leader ("" if unknown or self)
	ClusterLeader string // same as LeaderAddr, kept for clarity at call site
	RaftTerm      uint64 // current Raft term
	RaftLeaderID  string // node ID of the Raft leader ("" if unknown)
	NodeID        string // this node's ID
}

// NodeRole queries the Raft-authoritative role of this node via SHOW commands.
// Unlike LeadershipStates (which reads the heartbeat lease table), NodeRole
// reflects the actual Raft state machine: whoever won the last election.
func (c *engineClient) NodeRole(ctx context.Context) (nodeRoleResult, error) {
	showParams := []string{
		"asql_node_role",
		"asql_cluster_leader",
		"asql_raft_term",
		"asql_raft_leader_id",
		"asql_node_id",
	}
	values := make(map[string]string, len(showParams))
	for _, param := range showParams {
		row := c.pool.QueryRow(ctx, "SHOW "+param)
		var val string
		if err := row.Scan(&val); err == nil {
			values[param] = val
		}
	}
	return nodeRoleResult{
		Role:          values["asql_node_role"],
		LeaderAddr:    values["asql_cluster_leader"],
		ClusterLeader: values["asql_cluster_leader"],
		RaftTerm:      parseUint64(values["asql_raft_term"]),
		RaftLeaderID:  values["asql_raft_leader_id"],
		NodeID:        values["asql_node_id"],
	}, nil
}

type temporalLookupResult struct {
	CurrentLSN          *uint64
	RowLSN              *uint64
	ResolveReference    *uint64
	ResolveReferenceErr string
	EntityVersion       *uint64
	EntityHeadLSN       *uint64
	EntityVersionLSN    *uint64
}

func (c *engineClient) TemporalLookup(ctx context.Context, req temporalLookupRequest) (*temporalLookupResult, error) {
	if strings.TrimSpace(req.Domain) == "" {
		return nil, fmt.Errorf("domain is required")
	}
	if strings.TrimSpace(req.TableName) == "" {
		return nil, fmt.Errorf("table_name is required")
	}
	if strings.TrimSpace(req.PrimaryKey) == "" {
		return nil, fmt.Errorf("primary_key is required")
	}

	tableRef := pgEscape(strings.TrimSpace(req.Domain) + "." + strings.TrimSpace(req.TableName))
	pk := pgEscape(strings.TrimSpace(req.PrimaryKey))
	result := &temporalLookupResult{}

	currentLSN, err := c.queryOptionalUint64(ctx, "SELECT current_lsn()")
	if err != nil {
		return nil, err
	}
	result.CurrentLSN = currentLSN

	rowLSN, err := c.queryOptionalUint64(ctx, fmt.Sprintf("SELECT row_lsn('%s', '%s')", tableRef, pk))
	if err != nil {
		return nil, err
	}
	result.RowLSN = rowLSN

	resolveReference, err := c.queryOptionalUint64(ctx, fmt.Sprintf("SELECT resolve_reference('%s', '%s')", tableRef, pk))
	if err != nil {
		result.ResolveReferenceErr = err.Error()
	} else {
		result.ResolveReference = resolveReference
	}

	if strings.TrimSpace(req.EntityName) != "" && strings.TrimSpace(req.EntityRootPK) != "" {
		domain := pgEscape(strings.TrimSpace(req.Domain))
		entity := pgEscape(strings.TrimSpace(req.EntityName))
		rootPK := pgEscape(strings.TrimSpace(req.EntityRootPK))

		entityVersion, err := c.queryOptionalUint64(ctx, fmt.Sprintf("SELECT entity_version('%s', '%s', '%s')", domain, entity, rootPK))
		if err != nil {
			return nil, err
		}
		result.EntityVersion = entityVersion

		entityHeadLSN, err := c.queryOptionalUint64(ctx, fmt.Sprintf("SELECT entity_head_lsn('%s', '%s', '%s')", domain, entity, rootPK))
		if err != nil {
			return nil, err
		}
		result.EntityHeadLSN = entityHeadLSN

		if entityVersion != nil {
			entityVersionLSN, err := c.queryOptionalUint64(ctx, fmt.Sprintf("SELECT entity_version_lsn('%s', '%s', '%s', %d)", domain, entity, rootPK, *entityVersion))
			if err != nil {
				return nil, err
			}
			result.EntityVersionLSN = entityVersionLSN
		}
	}

	return result, nil
}

func (c *engineClient) ApplyFixture(ctx context.Context, fixture *fixtures.File) error {
	if fixture == nil {
		return fmt.Errorf("fixture is required")
	}
	conn, err := c.acquireConn(ctx)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer conn.Release()
	if err := fixtures.Apply(ctx, fixture, poolConnExecutor{conn: conn}); err != nil {
		return fmt.Errorf("apply fixture: %w", err)
	}
	return nil
}

func (c *engineClient) ExportFixture(ctx context.Context, options fixtures.ExportOptions) (*fixtures.File, error) {
	conn, err := c.acquireConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	defer conn.Release()
	fixture, err := fixtures.ExportFromPGWire(ctx, conn.Conn(), options)
	if err != nil {
		return nil, fmt.Errorf("export fixture: %w", err)
	}
	return fixture, nil
}

type poolConnExecutor struct {
	conn *pgxpool.Conn
}

func (e poolConnExecutor) Exec(ctx context.Context, sql string) error {
	if e.conn == nil {
		return fmt.Errorf("connection is required")
	}
	_, err := e.conn.Exec(ctx, sql)
	return err
}

func (c *engineClient) queryOptionalUint64(ctx context.Context, sql string) (*uint64, error) {
	rows, err := c.pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	resultRows, err := pgxRowsToMaps(rows)
	if err != nil {
		return nil, err
	}
	if len(resultRows) == 0 || len(resultRows[0]) == 0 {
		return nil, nil
	}
	for _, value := range resultRows[0] {
		if value == nil || strings.TrimSpace(toString(value)) == "" {
			return nil, nil
		}
		parsed := toUint64(value)
		return &parsed, nil
	}
	return nil, nil
}

func parseUint64(s string) uint64 {
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

// ── Conversion helpers ────────────────────────────────────────────────────────

// pgEscape escapes SQL single-quote chars for safe string embedding.
func pgEscape(s string) string { return strings.ReplaceAll(s, "'", "''") }

func toUint64(v interface{}) uint64 {
	switch x := v.(type) {
	case int64:
		if x >= 0 {
			return uint64(x)
		}
	case uint64:
		return x
	case int32:
		return uint64(x)
	case float64:
		return uint64(x)
	case float32:
		return uint64(x)
	case string:
		if n, err := strconv.ParseUint(x, 10, 64); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return uint64(f)
		}
	}
	return 0
}

func toFloat64(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case string:
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return f
		}
	}
	return 0
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func toBool(v interface{}) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return x == "t" || x == "true" || x == "1"
	}
	return false
}
