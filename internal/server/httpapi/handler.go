package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/correodabid/asql/internal/cluster/coordinator"
	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/platform/sysinfo"
)

// auditBufSize is the capacity of the async audit log channel.
const auditBufSize = 4096

const (
	principalHeader = "asql-principal"
	passwordHeader  = "asql-password"
)

type auditEntry struct {
	level slog.Level
	msg   string
	args  []any
}

// maxSQLLength is the maximum allowed length for a SQL statement (1 MB).
const maxSQLLength = 1 << 20

// sessionIdleTimeout is the maximum time a session can be idle before cleanup.
const sessionIdleTimeout = 10 * time.Minute

// service holds the shared state for all HTTP handlers.
type service struct {
	engine     *executor.Engine
	logStore   ports.LogStore
	logger     *slog.Logger
	leadership *coordinator.LeadershipManager
	authToken  string
	routing    *readRoutingStats

	mu           sync.Mutex
	sessions     map[string]*executor.Session
	sessionSeen  map[string]time.Time
	cleanupClose chan struct{}
	auditCh      chan auditEntry
}

func newService(engine *executor.Engine, logStore ports.LogStore, logger *slog.Logger, leadership *coordinator.LeadershipManager, authToken string) *service {
	s := &service{
		engine:       engine,
		logStore:     logStore,
		logger:       logger,
		leadership:   leadership,
		authToken:    authToken,
		routing:      newReadRoutingStats(),
		sessions:     make(map[string]*executor.Session),
		sessionSeen:  make(map[string]time.Time),
		cleanupClose: make(chan struct{}),
	}
	s.startAuditWriter()
	go s.sessionCleanupLoop()
	return s
}

// RegisterRoutes wires all API routes into the provided mux.
func (svc *service) RegisterRoutes(mux *http.ServeMux) {
	// Transaction lifecycle
	mux.HandleFunc("/api/v1/begin", svc.withAuth(svc.handleBeginTx))
	mux.HandleFunc("/api/v1/execute", svc.withAuth(svc.handleExecute))
	mux.HandleFunc("/api/v1/execute-batch", svc.withAuth(svc.handleExecuteBatch))
	mux.HandleFunc("/api/v1/commit", svc.withAuth(svc.handleCommitTx))
	mux.HandleFunc("/api/v1/rollback", svc.withAuth(svc.handleRollbackTx))

	// Read-only queries
	mux.HandleFunc("/api/v1/query", svc.withAuth(svc.handleQuery))

	// Time-travel & history
	mux.HandleFunc("/api/v1/time-travel", svc.withAuth(svc.handleTimeTravelQuery))
	mux.HandleFunc("/api/v1/row-history", svc.withAuth(svc.handleRowHistory))
	mux.HandleFunc("/api/v1/entity-version-history", svc.withAuth(svc.handleEntityVersionHistory))

	// Explain / plan
	mux.HandleFunc("/api/v1/explain", svc.withAuth(svc.handleExplainQuery))
	mux.HandleFunc("/api/v1/scan-strategy-stats", svc.withAuth(svc.handleScanStrategyStats))

	// Schema
	mux.HandleFunc("/api/v1/schema-snapshot", svc.withAuth(svc.handleSchemaSnapshot))

	// Admin
	mux.HandleFunc("/api/v1/replay-to-lsn", svc.withAuth(svc.handleReplayToLSN))
	mux.HandleFunc("/api/v1/engine-stats", svc.withAuth(svc.handleEngineStats))
	mux.HandleFunc("/api/v1/timeline-commits", svc.withAuth(svc.handleTimelineCommits))
	mux.HandleFunc("/api/v1/timeline-events", svc.withAuth(svc.handleTimelineEvents))

	// Cluster / leadership
	mux.HandleFunc("/api/v1/leadership-state", svc.withAuth(svc.handleLeadershipState))
	mux.HandleFunc("/api/v1/evaluate-read-route", svc.withAuth(svc.handleEvaluateReadRoute))
	mux.HandleFunc("/api/v1/read-routing-stats", svc.withAuth(svc.handleReadRoutingStats))

	// Replication
	mux.HandleFunc("/api/v1/last-lsn", svc.withAuth(svc.handleLastLSN))

	// Health check (no auth)
	mux.HandleFunc("/api/v1/health", svc.handleHealth)
}

// ─── Auth middleware ────────────────────────────────────────────────────────

func (svc *service) withAuth(next http.HandlerFunc) http.HandlerFunc {
	if svc.authToken == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			writeError(w, http.StatusUnauthorized, "authorization header is required")
			return
		}
		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(auth, bearerPrefix) {
			writeError(w, http.StatusUnauthorized, "authorization header must use Bearer token")
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(auth, bearerPrefix))
		if token != svc.authToken {
			writeError(w, http.StatusUnauthorized, "invalid bearer token")
			return
		}
		next(w, r)
	}
}

// ─── JSON helpers ───────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func decodeBody(r *http.Request, v interface{}) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}
	if len(body) == 0 {
		return nil // allow empty body for GET-like POSTs
	}
	return json.Unmarshal(body, v)
}

// ─── Health ─────────────────────────────────────────────────────────────────

func (svc *service) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ─── BeginTx ────────────────────────────────────────────────────────────────

func (svc *service) handleBeginTx(w http.ResponseWriter, r *http.Request) {
	var req BeginTxRequest
	if err := decodeBody(r, &req); err != nil {
		svc.auditFailure("tx.begin", err.Error())
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	beginSQL, err := buildBeginSQL(req.Mode, req.Domains)
	if err != nil {
		svc.auditFailure("tx.begin", err.Error(), slog.String("mode", req.Mode), slog.Any("domains", req.Domains))
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	principal, err := svc.authenticatedPrincipal(r)
	if err != nil {
		svc.auditFailure("tx.begin", err.Error(), slog.String("mode", req.Mode), slog.Any("domains", req.Domains))
		writeSecurityError(w, err)
		return
	}

	session := svc.engine.NewSession()
	session.SetPrincipal(principal)
	result, err := svc.engine.Execute(r.Context(), session, beginSQL)
	if err != nil {
		svc.auditFailure("tx.begin", err.Error(), slog.String("mode", req.Mode), slog.Any("domains", req.Domains))
		writeMapError(w, err)
		return
	}

	if result.TxID == "" {
		svc.auditFailure("tx.begin", "engine did not return tx id", slog.String("mode", req.Mode), slog.Any("domains", req.Domains))
		writeError(w, http.StatusInternalServerError, "engine did not return tx id")
		return
	}

	svc.mu.Lock()
	svc.sessions[result.TxID] = session
	svc.sessionSeen[result.TxID] = time.Now()
	svc.mu.Unlock()

	svc.auditSuccess("tx.begin", slog.String("tx_id", result.TxID), slog.String("mode", req.Mode), slog.Any("domains", req.Domains))
	writeJSON(w, http.StatusOK, &BeginTxResponse{TxID: result.TxID})
}

// ─── Execute ────────────────────────────────────────────────────────────────

func (svc *service) handleExecute(w http.ResponseWriter, r *http.Request) {
	var req ExecuteRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		svc.auditFailure("tx.execute", "sql is required", slog.String("tx_id", req.TxID))
		writeError(w, http.StatusBadRequest, "sql is required")
		return
	}
	if len(req.SQL) > maxSQLLength {
		svc.auditFailure("tx.execute", "sql exceeds maximum length", slog.String("tx_id", req.TxID), slog.Int("length", len(req.SQL)))
		writeError(w, http.StatusBadRequest, fmt.Sprintf("sql exceeds maximum length (%d bytes)", maxSQLLength))
		return
	}

	session, err := svc.findSession(req.TxID)
	if err != nil {
		svc.auditFailure("tx.execute", err.Error(), slog.String("tx_id", req.TxID))
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := svc.engine.Execute(r.Context(), session, req.SQL)
	if err != nil {
		svc.auditFailure("tx.execute", err.Error(), slog.String("tx_id", req.TxID), slog.String("sql", req.SQL))
		writeMapError(w, err)
		return
	}

	svc.auditSuccess("tx.execute", slog.String("tx_id", req.TxID), slog.String("sql", req.SQL), slog.String("tx_status", result.Status), slog.Int("rows", len(result.Rows)))
	writeJSON(w, http.StatusOK, &ExecuteResponse{
		Status:       result.Status,
		TxID:         req.TxID,
		Rows:         normalizeRows(result.Rows),
		RowsAffected: len(result.Rows),
	})
}

// ─── ExecuteBatch ───────────────────────────────────────────────────────────

func (svc *service) handleExecuteBatch(w http.ResponseWriter, r *http.Request) {
	var req ExecuteBatchRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if strings.TrimSpace(req.TxID) == "" || len(req.Statements) == 0 {
		svc.auditFailure("tx.execute_batch", "tx_id and statements are required", slog.String("tx_id", req.TxID))
		writeError(w, http.StatusBadRequest, "tx_id and statements are required")
		return
	}

	session, err := svc.findSession(req.TxID)
	if err != nil {
		svc.auditFailure("tx.execute_batch", err.Error(), slog.String("tx_id", req.TxID))
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	for i, stmt := range req.Statements {
		if len(stmt) > maxSQLLength {
			svc.auditFailure("tx.execute_batch", "statement exceeds maximum length", slog.String("tx_id", req.TxID), slog.Int("index", i))
			writeError(w, http.StatusBadRequest, fmt.Sprintf("statement %d exceeds maximum length (%d bytes)", i, maxSQLLength))
			return
		}
		if _, execErr := svc.engine.Execute(r.Context(), session, stmt); execErr != nil {
			svc.auditFailure("tx.execute_batch", execErr.Error(), slog.String("tx_id", req.TxID), slog.Int("index", i), slog.String("sql", stmt))
			writeMapError(w, execErr)
			return
		}
	}

	svc.auditSuccess("tx.execute_batch", slog.String("tx_id", req.TxID), slog.Int("executed", len(req.Statements)))
	writeJSON(w, http.StatusOK, &ExecuteBatchResponse{Status: "OK", Executed: len(req.Statements)})
}

// ─── CommitTx ───────────────────────────────────────────────────────────────

func (svc *service) handleCommitTx(w http.ResponseWriter, r *http.Request) {
	var req CommitTxRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	session, err := svc.findSession(req.TxID)
	if err != nil {
		svc.auditFailure("tx.commit", err.Error(), slog.String("tx_id", req.TxID))
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if fencErr := svc.validateFencingForCommit(session, &req); fencErr != nil {
		svc.auditFailure("tx.commit", fencErr.Error(), slog.String("tx_id", req.TxID))
		writeError(w, http.StatusForbidden, fencErr.Error())
		return
	}

	result, err := svc.engine.Execute(r.Context(), session, "COMMIT")
	if err != nil {
		svc.auditFailure("tx.commit", err.Error(), slog.String("tx_id", req.TxID))
		// Rollback to release activeTransactions counter.
		_, _ = svc.engine.Execute(r.Context(), session, "ROLLBACK")
		svc.mu.Lock()
		delete(svc.sessions, req.TxID)
		delete(svc.sessionSeen, req.TxID)
		svc.mu.Unlock()
		writeMapError(w, err)
		return
	}

	svc.mu.Lock()
	delete(svc.sessions, req.TxID)
	delete(svc.sessionSeen, req.TxID)
	svc.mu.Unlock()

	svc.auditSuccess("tx.commit", slog.String("tx_id", req.TxID), slog.String("tx_status", result.Status))
	writeJSON(w, http.StatusOK, &CommitTxResponse{Status: result.Status, CommitLSN: result.CommitLSN})
}

// ─── RollbackTx ─────────────────────────────────────────────────────────────

func (svc *service) handleRollbackTx(w http.ResponseWriter, r *http.Request) {
	var req RollbackTxRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	session, err := svc.findSession(req.TxID)
	if err != nil {
		svc.auditFailure("tx.rollback", err.Error(), slog.String("tx_id", req.TxID))
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	result, err := svc.engine.Execute(r.Context(), session, "ROLLBACK")
	if err != nil {
		svc.auditFailure("tx.rollback", err.Error(), slog.String("tx_id", req.TxID))
		writeMapError(w, err)
		return
	}

	svc.mu.Lock()
	delete(svc.sessions, req.TxID)
	delete(svc.sessionSeen, req.TxID)
	svc.mu.Unlock()

	svc.auditSuccess("tx.rollback", slog.String("tx_id", req.TxID), slog.String("tx_status", result.Status))
	writeJSON(w, http.StatusOK, &RollbackTxResponse{Status: result.Status})
}

// ─── Query (read-only, auto-session) ────────────────────────────────────────

func (svc *service) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		svc.auditFailure("query.execute", "sql is required")
		writeError(w, http.StatusBadRequest, "sql is required")
		return
	}
	if len(req.SQL) > maxSQLLength {
		svc.auditFailure("query.execute", "sql exceeds maximum length", slog.Int("length", len(req.SQL)))
		writeError(w, http.StatusBadRequest, fmt.Sprintf("sql exceeds maximum length (%d bytes)", maxSQLLength))
		return
	}

	principal, err := svc.authenticatedPrincipal(r)
	if err != nil {
		svc.auditFailure("query.execute", err.Error(), slog.String("sql", req.SQL), slog.Any("domains", req.Domains))
		writeSecurityError(w, err)
		return
	}

	result, err := svc.engine.QueryAsPrincipal(r.Context(), req.SQL, req.Domains, principal)
	if err != nil {
		svc.auditFailure("query.execute", err.Error(), slog.String("sql", req.SQL), slog.Any("domains", req.Domains))
		writeMapError(w, err)
		return
	}

	svc.auditSuccess("query.execute", slog.String("sql", req.SQL), slog.Any("domains", req.Domains), slog.Int("rows", len(result.Rows)))
	writeJSON(w, http.StatusOK, &QueryResponse{Status: result.Status, Rows: normalizeRows(result.Rows)})
}

// ─── TimeTravelQuery ────────────────────────────────────────────────────────

func (svc *service) handleTimeTravelQuery(w http.ResponseWriter, r *http.Request) {
	var req TimeTravelQueryRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		svc.auditFailure("admin.time_travel_query", "sql is required")
		writeError(w, http.StatusBadRequest, "sql is required")
		return
	}

	principal, err := svc.authenticatedPrincipal(r)
	if err != nil {
		svc.auditFailure("admin.time_travel_query", err.Error(), slog.String("sql", req.SQL), slog.Uint64("lsn", req.LSN), slog.Uint64("logical_timestamp", req.LogicalTimestamp), slog.Any("domains", req.Domains))
		writeSecurityError(w, err)
		return
	}

	var (
		result executor.Result
	)

	if req.LSN > 0 {
		result, err = svc.engine.TimeTravelQueryAsOfLSNAsPrincipal(r.Context(), req.SQL, req.Domains, req.LSN, principal)
	} else {
		result, err = svc.engine.TimeTravelQueryAsOfTimestampAsPrincipal(r.Context(), req.SQL, req.Domains, req.LogicalTimestamp, principal)
	}
	if err != nil {
		svc.auditFailure("admin.time_travel_query", err.Error(), slog.String("sql", req.SQL), slog.Uint64("lsn", req.LSN), slog.Uint64("logical_timestamp", req.LogicalTimestamp), slog.Any("domains", req.Domains))
		writeMapError(w, err)
		return
	}

	svc.auditSuccess("admin.time_travel_query", slog.String("sql", req.SQL), slog.Uint64("lsn", req.LSN), slog.Uint64("logical_timestamp", req.LogicalTimestamp), slog.Any("domains", req.Domains), slog.Int("rows", len(result.Rows)))
	writeJSON(w, http.StatusOK, &TimeTravelQueryResponse{Status: result.Status, Rows: normalizeRows(result.Rows)})
}

// ─── RowHistory ─────────────────────────────────────────────────────────────

func (svc *service) handleRowHistory(w http.ResponseWriter, r *http.Request) {
	var req RowHistoryRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		svc.auditFailure("admin.row_history", "sql is required")
		writeError(w, http.StatusBadRequest, "sql is required")
		return
	}

	principal, err := svc.authenticatedPrincipal(r)
	if err != nil {
		svc.auditFailure("admin.row_history", err.Error(), slog.String("sql", req.SQL), slog.Any("domains", req.Domains))
		writeSecurityError(w, err)
		return
	}

	result, err := svc.engine.RowHistoryAsPrincipal(r.Context(), req.SQL, req.Domains, principal)
	if err != nil {
		svc.auditFailure("admin.row_history", err.Error(), slog.String("sql", req.SQL), slog.Any("domains", req.Domains))
		writeMapError(w, err)
		return
	}

	svc.auditSuccess("admin.row_history", slog.String("sql", req.SQL), slog.Any("domains", req.Domains), slog.Int("rows", len(result.Rows)))
	writeJSON(w, http.StatusOK, &RowHistoryResponse{Status: result.Status, Rows: normalizeRows(result.Rows)})
}

// ─── EntityVersionHistory ───────────────────────────────────────────────────

func (svc *service) handleEntityVersionHistory(w http.ResponseWriter, r *http.Request) {
	var req EntityVersionHistoryRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if strings.TrimSpace(req.Domain) == "" {
		svc.auditFailure("admin.entity_version_history", "domain is required")
		writeError(w, http.StatusBadRequest, "domain is required")
		return
	}
	if strings.TrimSpace(req.EntityName) == "" {
		svc.auditFailure("admin.entity_version_history", "entity_name is required")
		writeError(w, http.StatusBadRequest, "entity_name is required")
		return
	}

	principal, err := svc.authenticatedPrincipal(r)
	if err != nil {
		svc.auditFailure("admin.entity_version_history", err.Error(), slog.String("domain", req.Domain), slog.String("entity", req.EntityName), slog.String("root_pk", req.RootPK))
		writeSecurityError(w, err)
		return
	}

	entries, err := svc.engine.EntityVersionHistoryAsPrincipal(r.Context(), req.Domain, req.EntityName, req.RootPK, principal)
	if err != nil {
		svc.auditFailure("admin.entity_version_history", err.Error(), slog.String("domain", req.Domain), slog.String("entity", req.EntityName), slog.String("root_pk", req.RootPK))
		writeMapError(w, err)
		return
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

	svc.auditSuccess("admin.entity_version_history", slog.String("domain", req.Domain), slog.String("entity", req.EntityName), slog.String("root_pk", req.RootPK), slog.Int("versions", len(versions)))
	writeJSON(w, http.StatusOK, &EntityVersionHistoryResponse{
		Status:   "OK",
		Entity:   req.EntityName,
		RootPK:   req.RootPK,
		Versions: versions,
	})
}

// ─── ExplainQuery ───────────────────────────────────────────────────────────

func (svc *service) handleExplainQuery(w http.ResponseWriter, r *http.Request) {
	var req ExplainQueryRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if strings.TrimSpace(req.SQL) == "" {
		svc.auditFailure("admin.explain_query", "sql is required")
		writeError(w, http.StatusBadRequest, "sql is required")
		return
	}

	principal, err := svc.authenticatedPrincipal(r)
	if err != nil {
		svc.auditFailure("admin.explain_query", err.Error(), slog.String("sql", req.SQL), slog.Any("domains", req.Domains))
		writeSecurityError(w, err)
		return
	}

	result, err := svc.engine.ExplainAsPrincipal(req.SQL, req.Domains, principal)
	if err != nil {
		svc.auditFailure("admin.explain_query", err.Error(), slog.String("sql", req.SQL), slog.Any("domains", req.Domains))
		writeMapError(w, err)
		return
	}

	svc.auditSuccess("admin.explain_query", slog.String("sql", req.SQL), slog.Any("domains", req.Domains), slog.Int("rows", len(result.Rows)))
	writeJSON(w, http.StatusOK, &ExplainQueryResponse{Status: result.Status, Rows: normalizeRows(result.Rows)})
}

// ─── ScanStrategyStats ──────────────────────────────────────────────────────

func (svc *service) handleScanStrategyStats(w http.ResponseWriter, r *http.Request) {
	if err := svc.authorizeAdminCapability(r, "scan_strategy_stats"); err != nil {
		svc.auditFailure("admin.scan_strategy_stats", err.Error())
		writeSecurityError(w, err)
		return
	}

	counts := svc.engine.ScanStrategyCounts()
	svc.auditSuccess("admin.scan_strategy_stats", slog.Int("strategies", len(counts)))
	writeJSON(w, http.StatusOK, &ScanStrategyStatsResponse{Counts: counts})
}

// ─── SchemaSnapshot ─────────────────────────────────────────────────────────

func (svc *service) handleSchemaSnapshot(w http.ResponseWriter, r *http.Request) {
	if err := svc.authorizeAdminCapability(r, "schema_snapshot"); err != nil {
		svc.auditFailure("admin.schema_snapshot", err.Error())
		writeSecurityError(w, err)
		return
	}

	var req SchemaSnapshotRequest
	_ = decodeBody(r, &req) // ignore errors for empty body → returns all

	snapshot := svc.engine.SchemaSnapshot(req.Domains)
	domains := make([]SchemaSnapshotDomain, 0, len(snapshot.Domains))
	for _, domain := range snapshot.Domains {
		tables := make([]SchemaSnapshotTable, 0, len(domain.Tables))
		for _, table := range domain.Tables {
			columns := make([]SchemaSnapshotColumn, 0, len(table.Columns))
			for _, col := range table.Columns {
				columns = append(columns, SchemaSnapshotColumn{
					Name:             col.Name,
					Type:             col.Type,
					PrimaryKey:       col.PrimaryKey,
					Unique:           col.Unique,
					ReferencesTable:  col.ReferencesTable,
					ReferencesColumn: col.ReferencesColumn,
					DefaultValue:     formatDefaultValue(col.DefaultValue),
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
			entityTables := make([]string, len(entity.Tables))
			copy(entityTables, entity.Tables)
			entities = append(entities, SchemaSnapshotEntity{
				Name:      entity.Name,
				RootTable: entity.RootTable,
				Tables:    entityTables,
			})
		}
		domains = append(domains, SchemaSnapshotDomain{Name: domain.Name, Tables: tables, Entities: entities})
	}

	svc.auditSuccess("admin.schema_snapshot", slog.Int("domains", len(domains)))
	writeJSON(w, http.StatusOK, &SchemaSnapshotResponse{Status: "SNAPSHOT", Domains: domains})
}

// ─── ReplayToLSN ────────────────────────────────────────────────────────────

func (svc *service) handleReplayToLSN(w http.ResponseWriter, r *http.Request) {
	var req ReplayToLSNRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	principal, err := svc.authenticatedPrincipal(r)
	if err != nil {
		svc.auditFailure("admin.replay_to_lsn", err.Error(), slog.Uint64("lsn", req.LSN))
		writeSecurityError(w, err)
		return
	}

	if err := svc.engine.ReplayToLSNAsPrincipal(r.Context(), req.LSN, principal); err != nil {
		svc.auditFailure("admin.replay_to_lsn", err.Error(), slog.Uint64("lsn", req.LSN))
		writeMapError(w, err)
		return
	}

	svc.auditSuccess("admin.replay_to_lsn", slog.Uint64("lsn", req.LSN))
	writeJSON(w, http.StatusOK, &ReplayToLSNResponse{AppliedLSN: req.LSN})
}

// ─── EngineStats ────────────────────────────────────────────────────────────

func (svc *service) handleEngineStats(w http.ResponseWriter, r *http.Request) {
	if err := svc.authorizeAdminCapability(r, "engine_stats"); err != nil {
		svc.auditFailure("admin.engine_stats", err.Error())
		writeSecurityError(w, err)
		return
	}

	snap := svc.engine.PerfStats()
	svc.auditSuccess("admin.engine_stats")
	writeJSON(w, http.StatusOK, &EngineStatsResponse{
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
	})
}

// ─── TimelineCommits ────────────────────────────────────────────────────────

func (svc *service) handleTimelineCommits(w http.ResponseWriter, r *http.Request) {
	if err := svc.authorizeAdminCapability(r, "timeline_commits"); err != nil {
		svc.auditFailure("admin.timeline_commits", err.Error())
		writeSecurityError(w, err)
		return
	}

	var req TimelineCommitsRequest
	_ = decodeBody(r, &req)

	fromLSN := req.FromLSN
	if fromLSN == 0 {
		fromLSN = 1
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 500
	}

	commits, err := svc.engine.TimelineCommits(r.Context(), fromLSN, req.ToLSN, req.Domain, limit)
	if err != nil {
		svc.auditFailure("admin.timeline_commits", err.Error())
		writeMapError(w, err)
		return
	}

	entries := make([]TimelineCommitEntry, len(commits))
	for i, c := range commits {
		tables := make([]TimelineCommitMutationEntry, len(c.Tables))
		for j, t := range c.Tables {
			tables[j] = TimelineCommitMutationEntry{Domain: t.Domain, Table: t.Table, Operation: t.Operation}
		}
		entries[i] = TimelineCommitEntry{LSN: c.LSN, TxID: c.TxID, Timestamp: c.Timestamp, Tables: tables}
	}

	svc.auditSuccess("admin.timeline_commits", slog.Int("commits", len(entries)))
	writeJSON(w, http.StatusOK, &TimelineCommitsResponse{Commits: entries})
}

// ─── TimelineEvents ─────────────────────────────────────────────────────────

func (svc *service) handleTimelineEvents(w http.ResponseWriter, r *http.Request) {
	if err := svc.authorizeAdminCapability(r, "timeline_events"); err != nil {
		svc.auditFailure("admin.timeline_events", err.Error())
		writeSecurityError(w, err)
		return
	}

	memLSNs, diskLSNs := svc.engine.ListSnapshotPoints()

	memPoints := make([]TimelineSnapshotPoint, len(memLSNs))
	for i, lsn := range memLSNs {
		memPoints[i] = TimelineSnapshotPoint{LSN: lsn}
	}
	diskPoints := make([]TimelineSnapshotPoint, len(diskLSNs))
	for i, lsn := range diskLSNs {
		diskPoints[i] = TimelineSnapshotPoint{LSN: lsn}
	}

	svc.auditSuccess("admin.timeline_events",
		slog.Int("memory_snapshots", len(memPoints)),
		slog.Int("disk_snapshots", len(diskPoints)),
	)
	writeJSON(w, http.StatusOK, &TimelineEventsResponse{
		MemorySnapshots: memPoints,
		DiskSnapshots:   diskPoints,
	})
}

// ─── LeadershipState ────────────────────────────────────────────────────────

func (svc *service) handleLeadershipState(w http.ResponseWriter, r *http.Request) {
	if err := svc.authorizeAdminCapability(r, "leadership_state"); err != nil {
		svc.auditFailure("admin.leadership_state", err.Error())
		writeSecurityError(w, err)
		return
	}

	var req LeadershipStateRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	group := strings.ToLower(strings.TrimSpace(req.Group))
	if group == "" {
		svc.auditFailure("admin.leadership_state", "group is required")
		writeError(w, http.StatusBadRequest, "group is required")
		return
	}

	if svc.leadership == nil {
		svc.auditFailure("admin.leadership_state", "leadership manager is not configured", slog.String("group", group))
		writeError(w, http.StatusPreconditionFailed, "leadership manager is not configured")
		return
	}

	state, exists, leaseActive := svc.leadership.SnapshotWithLeaseStatus(group)
	if !exists {
		svc.auditFailure("admin.leadership_state", "group not found", slog.String("group", group))
		writeError(w, http.StatusNotFound, fmt.Sprintf("leadership state not found for group %s", group))
		return
	}

	svc.auditSuccess("admin.leadership_state", slog.String("group", state.Group), slog.String("leader_id", state.LeaderID), slog.Uint64("term", state.Term), slog.Bool("lease_active", leaseActive))
	writeJSON(w, http.StatusOK, &LeadershipStateResponse{
		Group:              state.Group,
		Term:               state.Term,
		LeaderID:           state.LeaderID,
		FencingToken:       state.FencingToken,
		LeaseExpiresAtUnix: state.LeaseExpiresAt.Unix(),
		LeaseActive:        leaseActive,
		LastLeaderLSN:      state.LastLeaderLSN,
	})
}

// ─── EvaluateReadRoute ──────────────────────────────────────────────────────

func (svc *service) handleEvaluateReadRoute(w http.ResponseWriter, r *http.Request) {
	if err := svc.authorizeAdminCapability(r, "evaluate_read_route"); err != nil {
		svc.auditFailure("admin.evaluate_read_route", err.Error())
		writeSecurityError(w, err)
		return
	}

	var req EvaluateReadRouteRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	consistency := normalizeReadConsistency(req.Consistency)
	decision := decideReadRoute(readRouteInput{
		Consistency:         consistency,
		LeaderLSN:           req.LeaderLSN,
		FollowerLSN:         req.FollowerLSN,
		HasFollower:         req.HasFollower,
		FollowerUnavailable: req.FollowerUnavailable,
		MaxLag:              req.MaxLag,
	})

	if svc.routing != nil {
		svc.routing.record(readRoutingMetricInput{
			Consistency: consistency,
			Decision:    decision,
			HasFollower: req.HasFollower,
			MaxLag:      req.MaxLag,
			LeaderLSN:   req.LeaderLSN,
			FollowerLSN: req.FollowerLSN,
		})
	}

	svc.auditSuccess("admin.evaluate_read_route",
		slog.String("mode", string(consistency)),
		slog.String("route", string(decision.Route)),
		slog.Uint64("leader_lsn", req.LeaderLSN),
		slog.Uint64("follower_lsn", req.FollowerLSN),
		slog.Uint64("lag", decision.Lag),
		slog.String("fallback_reason", decision.FallbackReason),
	)

	writeJSON(w, http.StatusOK, &EvaluateReadRouteResponse{
		Mode:           string(consistency),
		Route:          string(decision.Route),
		LeaderLSN:      req.LeaderLSN,
		FollowerLSN:    req.FollowerLSN,
		Lag:            decision.Lag,
		FallbackReason: decision.FallbackReason,
	})
}

// ─── ReadRoutingStats ───────────────────────────────────────────────────────

func (svc *service) handleReadRoutingStats(w http.ResponseWriter, r *http.Request) {
	if err := svc.authorizeAdminCapability(r, "read_routing_stats"); err != nil {
		svc.auditFailure("admin.read_routing_stats", err.Error())
		writeSecurityError(w, err)
		return
	}

	counts := map[string]uint64{}
	if svc.routing != nil {
		counts = svc.routing.snapshot()
	}
	svc.auditSuccess("admin.read_routing_stats", slog.Int("counters", len(counts)))
	writeJSON(w, http.StatusOK, &ReadRoutingStatsResponse{Counts: counts})
}

// ─── LastLSN (replication) ──────────────────────────────────────────────────

func (svc *service) handleLastLSN(w http.ResponseWriter, r *http.Request) {
	if err := svc.authorizeAdminCapability(r, "replication_last_lsn"); err != nil {
		svc.auditFailure("admin.replication_last_lsn", err.Error())
		writeSecurityError(w, err)
		return
	}

	withLastLSN, ok := svc.logStore.(interface{ LastLSN() uint64 })
	if !ok {
		writeError(w, http.StatusPreconditionFailed, "log store does not expose last lsn")
		return
	}

	lsn := withLastLSN.LastLSN()
	svc.auditSuccess("admin.replication_last_lsn", slog.Uint64("lsn", lsn))
	writeJSON(w, http.StatusOK, &LastLSNResponse{LSN: lsn})
}

// ─── Fencing validation ─────────────────────────────────────────────────────

func (svc *service) validateFencingForCommit(session *executor.Session, request *CommitTxRequest) error {
	if request == nil {
		return errors.New("commit request is required")
	}

	hasGroup := strings.TrimSpace(request.Group) != ""
	hasNode := strings.TrimSpace(request.NodeID) != ""
	hasToken := strings.TrimSpace(request.FencingToken) != ""

	if !hasGroup && !hasNode && !hasToken {
		return nil
	}

	if !(hasNode && hasToken) {
		return errors.New("node_id and fencing_token are required when fencing is enabled")
	}

	if svc.leadership == nil {
		return errors.New("leadership manager is not configured")
	}

	group := strings.ToLower(strings.TrimSpace(request.Group))
	if group == "" {
		domains := session.ActiveDomains()
		if len(domains) == 0 {
			return errors.New("cannot infer group from active transaction")
		}
		group = strings.Join(domains, ",")
	}

	nodeID := strings.TrimSpace(request.NodeID)
	fencingToken := strings.TrimSpace(request.FencingToken)
	if !svc.leadership.CanAcceptWrite(group, nodeID, fencingToken) {
		svc.auditFailure("ha.fencing_rejection", "write rejected by fencing token check", slog.String("group", group), slog.String("node_id", nodeID))
		return fmt.Errorf("write rejected by fencing token check for group %s", group)
	}

	return nil
}

// ─── Session management ─────────────────────────────────────────────────────

func (svc *service) findSession(txID string) (*executor.Session, error) {
	trimmed := strings.TrimSpace(txID)
	if trimmed == "" {
		return nil, errors.New("tx_id is required")
	}

	svc.mu.Lock()
	defer svc.mu.Unlock()

	session, exists := svc.sessions[trimmed]
	if !exists {
		return nil, fmt.Errorf("unknown tx_id %s", trimmed)
	}

	svc.sessionSeen[trimmed] = time.Now()
	return session, nil
}

func (svc *service) authenticatedPrincipal(r *http.Request) (string, error) {
	if svc.engine == nil || !svc.engine.HasPrincipalCatalog() {
		return "", nil
	}

	principal := strings.TrimSpace(r.Header.Get(principalHeader))
	if principal == "" {
		return "", errors.New("asql-principal header is required")
	}

	password := strings.TrimSpace(r.Header.Get(passwordHeader))
	if password == "" {
		return "", errors.New("asql-password header is required")
	}

	info, err := svc.engine.AuthenticatePrincipal(principal, password)
	if err != nil {
		return "", err
	}

	return info.Name, nil
}

func (svc *service) authorizeAdminCapability(r *http.Request, capability string) error {
	principal, err := svc.authenticatedPrincipal(r)
	if err != nil {
		return err
	}
	return svc.engine.AuthorizePrincipalPrivilege(principal, executor.PrincipalPrivilegeAdmin, capability)
}

func (svc *service) sessionCleanupLoop() {
	ticker := time.NewTicker(sessionIdleTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			svc.mu.Lock()
			now := time.Now()
			var expired []struct {
				txID    string
				session *executor.Session
				idle    time.Duration
			}
			for txID, lastSeen := range svc.sessionSeen {
				if now.Sub(lastSeen) > sessionIdleTimeout {
					if sess, ok := svc.sessions[txID]; ok {
						expired = append(expired, struct {
							txID    string
							session *executor.Session
							idle    time.Duration
						}{txID, sess, now.Sub(lastSeen)})
					}
					delete(svc.sessions, txID)
					delete(svc.sessionSeen, txID)
				}
			}
			svc.mu.Unlock()

			for _, e := range expired {
				_, _ = svc.engine.Execute(context.Background(), e.session, "ROLLBACK")
				if svc.logger != nil {
					svc.logger.Warn("session expired due to inactivity", "tx_id", e.txID, "idle", e.idle.String())
				}
			}
		case <-svc.cleanupClose:
			return
		}
	}
}

// ─── buildBeginSQL ──────────────────────────────────────────────────────────

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
		return "", fmt.Errorf("unsupported begin mode %q", inferred)
	}
}

// ─── Error mapping ──────────────────────────────────────────────────────────

// writeMapError translates engine errors to appropriate HTTP status codes.
func writeMapError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}

	msg := err.Error()

	// Parse errors → 400
	if strings.Contains(msg, "parse") || strings.Contains(msg, "syntax") {
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	// Authentication failures → 401
	if strings.Contains(msg, "header is required") || strings.Contains(msg, "authentication failed") || strings.Contains(msg, "principal is disabled") {
		writeError(w, http.StatusUnauthorized, msg)
		return
	}

	// Authorization failures → 403
	if strings.Contains(msg, "permission denied") || strings.Contains(msg, "privilege required") {
		writeError(w, http.StatusForbidden, msg)
		return
	}

	// Not found → 404
	if strings.Contains(msg, "not found") || strings.Contains(msg, "does not exist") {
		writeError(w, http.StatusNotFound, msg)
		return
	}

	// Write conflicts → 409
	if strings.Contains(msg, "write conflict") {
		writeError(w, http.StatusConflict, msg)
		return
	}

	// Constraint violations, already exists → 422
	if strings.Contains(msg, "constraint") || strings.Contains(msg, "already exists") {
		writeError(w, http.StatusUnprocessableEntity, msg)
		return
	}

	// Transaction state errors → 412
	if strings.Contains(msg, "transaction") || strings.Contains(msg, "session") || strings.Contains(msg, "domain is required") {
		writeError(w, http.StatusPreconditionFailed, msg)
		return
	}

	writeError(w, http.StatusInternalServerError, msg)
}

func writeSecurityError(w http.ResponseWriter, err error) {
	writeMapError(w, err)
}

// ─── Audit system ───────────────────────────────────────────────────────────

func (svc *service) startAuditWriter() {
	if svc.auditCh != nil {
		return
	}
	svc.auditCh = make(chan auditEntry, auditBufSize)
	go func() {
		for entry := range svc.auditCh {
			if svc.logger == nil {
				continue
			}
			switch entry.level {
			case slog.LevelWarn:
				svc.logger.Warn(entry.msg, entry.args...)
			default:
				svc.logger.Info(entry.msg, entry.args...)
			}
		}
	}()
}

func (svc *service) stopAuditWriter() {
	if svc.auditCh != nil {
		close(svc.auditCh)
	}
}

func (svc *service) auditSuccess(operation string, attrs ...slog.Attr) {
	if svc.auditCh == nil {
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
	case svc.auditCh <- auditEntry{level: slog.LevelInfo, msg: "audit_event", args: args}:
	default:
	}
}

func (svc *service) auditFailure(operation, reason string, attrs ...slog.Attr) {
	if svc.auditCh == nil {
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
	case svc.auditCh <- auditEntry{level: slog.LevelWarn, msg: "audit_event", args: args}:
	default:
	}
}

// ─── System info ────────────────────────────────────────────────────────────

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
