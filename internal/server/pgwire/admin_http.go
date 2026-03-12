package pgwire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/engine/executor"
)

const maxFailoverHistoryEntries = 64

type runtimeMetrics struct {
	nodeID string

	mu                 sync.Mutex
	failoverPromotions map[string]uint64
	failoverHistory     []coordinator.FailoverTransition
}

func newRuntimeMetrics(nodeID string) *runtimeMetrics {
	return &runtimeMetrics{
		nodeID:             nodeID,
		failoverPromotions: map[string]uint64{},
	}
}

func (metrics *runtimeMetrics) OnFailoverTransition(transition coordinator.FailoverTransition) {
	if metrics == nil || transition.Phase != coordinator.FailoverPhasePromotedLeader || transition.Group == "" {
		return
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	metrics.failoverPromotions[transition.Group] = metrics.failoverPromotions[transition.Group] + 1
	metrics.failoverHistory = append(metrics.failoverHistory, transition)
	if len(metrics.failoverHistory) > maxFailoverHistoryEntries {
		metrics.failoverHistory = append([]coordinator.FailoverTransition(nil), metrics.failoverHistory[len(metrics.failoverHistory)-maxFailoverHistoryEntries:]...)
	}
}

func (metrics *runtimeMetrics) snapshotFailovers() map[string]uint64 {
	if metrics == nil {
		return map[string]uint64{}
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	out := make(map[string]uint64, len(metrics.failoverPromotions))
	for group, count := range metrics.failoverPromotions {
		out[group] = count
	}
	return out
}

func (metrics *runtimeMetrics) snapshotFailoverHistory() []coordinator.FailoverTransition {
	if metrics == nil {
		return nil
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()
	out := make([]coordinator.FailoverTransition, len(metrics.failoverHistory))
	copy(out, metrics.failoverHistory)
	return out
}

type adminHealthResponse struct {
	Status         string   `json:"status"`
	Ready          bool     `json:"ready"`
	Live           bool     `json:"live"`
	ClusterMode    bool     `json:"cluster_mode"`
	NodeID         string   `json:"node_id,omitempty"`
	RaftRole       string   `json:"raft_role,omitempty"`
	LeaderID       string   `json:"leader_id,omitempty"`
	CurrentTerm    uint64   `json:"current_term,omitempty"`
	LastDurableLSN uint64   `json:"last_durable_lsn"`
	Reasons        []string `json:"reasons,omitempty"`
}

type adminLeadershipState struct {
	Group          string `json:"group_name"`
	Term           uint64 `json:"term"`
	LeaderID       string `json:"leader_id"`
	FencingToken   string `json:"fencing_token"`
	LeaseExpiresAt string `json:"lease_expires_at"`
	LastSafeLSN    uint64 `json:"last_safe_lsn"`
	LeaseActive    bool   `json:"lease_active"`
	LocalRole      string `json:"local_role,omitempty"`
}

type adminFailoverTransition struct {
	Phase  string `json:"phase"`
	Group  string `json:"group_name"`
	Term   uint64 `json:"term"`
	NodeID string `json:"node_id"`
}

func (server *Server) startAdminHTTP() error {
	if server == nil || server.config.AdminHTTPAddr == "" {
		return nil
	}

	listener, err := net.Listen("tcp", server.config.AdminHTTPAddr)
	if err != nil {
		return fmt.Errorf("admin http listen %s: %w", server.config.AdminHTTPAddr, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", server.handleMetrics)
	mux.HandleFunc("/livez", server.handleLivez)
	mux.HandleFunc("/readyz", server.handleReadyz)
	mux.HandleFunc("/api/v1/health", server.handleAdminHealth)
	mux.HandleFunc("/api/v1/leadership-state", server.handleAdminLeadershipState)
	mux.HandleFunc("/api/v1/last-lsn", server.handleAdminLastLSN)
	mux.HandleFunc("/api/v1/failover-history", server.handleAdminFailoverHistory)
	mux.HandleFunc("/api/v1/snapshot-catalog", server.handleAdminSnapshotCatalog)
	mux.HandleFunc("/api/v1/wal-retention", server.handleAdminWALRetention)

	server.adminListener = listener
	server.adminServer = &http.Server{Handler: mux}

	go func() {
		if err := server.adminServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			server.config.Logger.Warn("admin http server exited", "error", err, "address", listener.Addr().String())
		}
	}()

	server.config.Logger.Info("admin http server listening", "address", listener.Addr().String())
	return nil
}

func (server *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write(server.renderPrometheusMetrics())
}

func (server *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	status := server.liveStatus()
	code := http.StatusOK
	if !status.Live {
		code = http.StatusServiceUnavailable
	}
	writeAdminJSON(w, code, status)
}

func (server *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	status := server.readyStatus()
	code := http.StatusOK
	if !status.Ready {
		code = http.StatusServiceUnavailable
	}
	writeAdminJSON(w, code, status)
}

func (server *Server) handleAdminHealth(w http.ResponseWriter, _ *http.Request) {
	status := server.readyStatus()
	code := http.StatusOK
	if !status.Ready {
		code = http.StatusServiceUnavailable
	}
	writeAdminJSON(w, code, status)
}

func (server *Server) handleAdminLeadershipState(w http.ResponseWriter, _ *http.Request) {
	writeAdminJSON(w, http.StatusOK, map[string]any{"groups": server.leadershipStates()})
}

func (server *Server) handleAdminLastLSN(w http.ResponseWriter, _ *http.Request) {
	writeAdminJSON(w, http.StatusOK, map[string]uint64{"last_lsn": server.lastDurableLSN()})
}

func (server *Server) handleAdminFailoverHistory(w http.ResponseWriter, _ *http.Request) {
	history := server.metrics.snapshotFailoverHistory()
	transitions := make([]adminFailoverTransition, 0, len(history))
	for _, transition := range history {
		transitions = append(transitions, adminFailoverTransition{
			Phase:  string(transition.Phase),
			Group:  transition.Group,
			Term:   transition.Term,
			NodeID: transition.NodeID,
		})
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{"transitions": transitions})
}

func (server *Server) handleAdminSnapshotCatalog(w http.ResponseWriter, _ *http.Request) {
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	catalog, err := server.engine.SnapshotCatalog()
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusOK, map[string]any{"snapshots": catalog})
}

func (server *Server) handleAdminWALRetention(w http.ResponseWriter, _ *http.Request) {
	if server == nil || strings.TrimSpace(server.config.DataDirPath) == "" {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "data directory unavailable"})
		return
	}
	state, err := executor.InspectDataDirWALRetention(server.config.DataDirPath)
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusOK, state)
}

func (server *Server) liveStatus() adminHealthResponse {
	live := server != nil && server.engine != nil && server.walStore != nil
	if server != nil {
		select {
		case <-server.closeCh:
			live = false
		default:
		}
	}

	status := adminHealthResponse{
		Status:         "live",
		Ready:          false,
		Live:           live,
		ClusterMode:    server != nil && server.raftNode != nil,
		LastDurableLSN: server.lastDurableLSN(),
	}
	if server != nil {
		status.NodeID = server.config.NodeID
	}
	if !live {
		status.Status = "not_live"
		status.Reasons = []string{"server stopping or core stores unavailable"}
	}
	return status
}

func (server *Server) readyStatus() adminHealthResponse {
	status := server.liveStatus()
	status.Status = "ready"
	status.Ready = true
	status.Reasons = nil

	if !status.Live {
		status.Status = "not_ready"
		status.Ready = false
		status.Reasons = append(status.Reasons, "server not live")
		return status
	}

	if server != nil && server.raftNode != nil {
		status.RaftRole = server.raftNode.Role()
		status.LeaderID = server.raftNode.LeaderID()
		status.CurrentTerm = server.raftNode.CurrentTerm()
		if !server.raftNode.IsLeader() && status.LeaderID == "" {
			status.Status = "not_ready"
			status.Ready = false
			status.Reasons = append(status.Reasons, "raft leader unknown")
		}
	}

	return status
}

func (server *Server) leadershipStates() []adminLeadershipState {
	if server == nil || server.leadership == nil {
		return nil
	}

	groups := server.leadership.Groups()
	out := make([]adminLeadershipState, 0, len(groups))
	for _, group := range groups {
		state, exists, leaseActive := server.leadership.SnapshotWithLeaseStatus(group)
		if !exists {
			continue
		}
		localRole := "follower"
		if server.config.NodeID != "" && state.LeaderID == server.config.NodeID {
			localRole = "leader"
		}
		out = append(out, adminLeadershipState{
			Group:          state.Group,
			Term:           state.Term,
			LeaderID:       state.LeaderID,
			FencingToken:   state.FencingToken,
			LeaseExpiresAt: state.LeaseExpiresAt.UTC().Format(time.RFC3339),
			LastSafeLSN:    state.LastLeaderLSN,
			LeaseActive:    leaseActive,
			LocalRole:      localRole,
		})
	}
	return out
}

func (server *Server) lastDurableLSN() uint64 {
	if server == nil || server.walStore == nil {
		return 0
	}
	return server.walStore.LastLSN()
}

func (server *Server) renderPrometheusMetrics() []byte {
	var buf bytes.Buffer
	perf := server.engine.PerfStats()
	ready := server.readyStatus()
	failovers := server.metrics.snapshotFailovers()
	leadership := server.leadershipStates()
	sort.Slice(leadership, func(i, j int) bool { return leadership[i].Group < leadership[j].Group })

	writeMetricHelp(&buf, "asql_process_live", "Whether the ASQL pgwire runtime is live (1) or stopping/unavailable (0).", "gauge")
	writeMetricValue(&buf, "asql_process_live", nil, boolFloat(ready.Live))
	writeMetricHelp(&buf, "asql_process_ready", "Whether the ASQL pgwire runtime is ready to serve traffic (1) or not ready (0).", "gauge")
	writeMetricValue(&buf, "asql_process_ready", nil, boolFloat(ready.Ready))

	writeMetricHelp(&buf, "asql_wal_last_durable_lsn", "Last durable WAL LSN observed by this node.", "gauge")
	writeMetricValue(&buf, "asql_wal_last_durable_lsn", nil, float64(server.lastDurableLSN()))
	writeMetricHelp(&buf, "asql_audit_log_size_bytes", "On-disk size of the append-only audit log.", "gauge")
	writeMetricValue(&buf, "asql_audit_log_size_bytes", nil, float64(perf.AuditFileSize))

	writeMetricHelp(&buf, "asql_engine_commits_total", "Total committed transactions processed by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_commits_total", nil, float64(perf.TotalCommits))
	writeMetricHelp(&buf, "asql_engine_reads_total", "Total read queries processed by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_reads_total", nil, float64(perf.TotalReads))
	writeMetricHelp(&buf, "asql_engine_time_travel_queries_total", "Total time-travel queries processed by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_time_travel_queries_total", nil, float64(perf.TotalTimeTravelQueries))
	writeMetricHelp(&buf, "asql_engine_active_transactions", "Current active transaction count.", "gauge")
	writeMetricValue(&buf, "asql_engine_active_transactions", nil, float64(perf.ActiveTransactions))

	writeQuantiles(&buf, "asql_engine_commit_latency_seconds", "Commit latency percentiles in seconds.", perf.CommitLatencyP50, perf.CommitLatencyP95, perf.CommitLatencyP99)
	writeQuantiles(&buf, "asql_engine_commit_queue_wait_seconds", "Commit queue wait percentiles in seconds.", perf.CommitQueueWaitP50, perf.CommitQueueWaitP95, perf.CommitQueueWaitP99)
	writeQuantiles(&buf, "asql_engine_commit_write_hold_seconds", "Commit write-lock hold percentiles in seconds.", perf.CommitWriteHoldP50, perf.CommitWriteHoldP95, perf.CommitWriteHoldP99)
	writeQuantiles(&buf, "asql_engine_commit_apply_seconds", "Commit apply percentiles in seconds.", perf.CommitApplyP50, perf.CommitApplyP95, perf.CommitApplyP99)
	writeQuantiles(&buf, "asql_engine_read_latency_seconds", "Read latency percentiles in seconds.", perf.ReadLatencyP50, perf.ReadLatencyP95, perf.ReadLatencyP99)
	writeQuantiles(&buf, "asql_engine_time_travel_latency_seconds", "Time-travel query latency percentiles in seconds.", perf.TimeTravelLatencyP50, perf.TimeTravelLatencyP95, perf.TimeTravelLatencyP99)
	writeQuantiles(&buf, "asql_engine_fsync_latency_seconds", "WAL fsync latency percentiles in seconds.", perf.FsyncLatencyP50, perf.FsyncLatencyP95, perf.FsyncLatencyP99)

	writeMetricHelp(&buf, "asql_engine_commit_throughput_per_second", "Rolling commit throughput over the recent sampling window.", "gauge")
	writeMetricValue(&buf, "asql_engine_commit_throughput_per_second", nil, perf.CommitThroughput)
	writeMetricHelp(&buf, "asql_engine_read_throughput_per_second", "Rolling read throughput over the recent sampling window.", "gauge")
	writeMetricValue(&buf, "asql_engine_read_throughput_per_second", nil, perf.ReadThroughput)
	writeMetricHelp(&buf, "asql_engine_fsync_errors_total", "Total WAL fsync errors observed by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_fsync_errors_total", nil, float64(perf.TotalFsyncErrors))
	writeMetricHelp(&buf, "asql_engine_replays_total", "Total replay operations performed by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_replays_total", nil, float64(perf.TotalReplays))
	writeMetricHelp(&buf, "asql_engine_replay_duration_seconds", "Most recent replay duration in seconds.", "gauge")
	writeMetricValue(&buf, "asql_engine_replay_duration_seconds", nil, perf.ReplayDurationMs/1000.0)
	writeMetricHelp(&buf, "asql_engine_snapshots_total", "Total persisted snapshot operations completed by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_snapshots_total", nil, float64(perf.TotalSnapshots))
	writeMetricHelp(&buf, "asql_engine_snapshot_duration_seconds", "Most recent snapshot persistence duration in seconds.", "gauge")
	writeMetricValue(&buf, "asql_engine_snapshot_duration_seconds", nil, perf.SnapshotDurationMs/1000.0)
	writeMetricHelp(&buf, "asql_engine_audit_errors_total", "Total asynchronous audit append errors observed by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_audit_errors_total", nil, float64(perf.TotalAuditErrors))
	writeMetricHelp(&buf, "asql_engine_wal_file_size_bytes", "On-disk WAL size in bytes.", "gauge")
	writeMetricValue(&buf, "asql_engine_wal_file_size_bytes", nil, float64(perf.WALFileSize))
	writeMetricHelp(&buf, "asql_engine_snapshot_file_size_bytes", "On-disk snapshot size in bytes.", "gauge")
	writeMetricValue(&buf, "asql_engine_snapshot_file_size_bytes", nil, float64(perf.SnapshotFileSize))

	writeMetricHelp(&buf, "asql_cluster_failovers_total", "Total serialized leader promotions observed by this node.", "counter")
	groups := make([]string, 0, len(failovers))
	for group := range failovers {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	for _, group := range groups {
		writeMetricValue(&buf, "asql_cluster_failovers_total", map[string]string{"group": group}, float64(failovers[group]))
	}

	writeMetricHelp(&buf, "asql_cluster_current_leader_info", "Current leader per group as observed by the local leadership manager.", "gauge")
	writeMetricHelp(&buf, "asql_cluster_leader_term", "Current leadership term per group.", "gauge")
	writeMetricHelp(&buf, "asql_cluster_last_safe_lsn", "Last safe leader LSN per group for operator decisions.", "gauge")
	writeMetricHelp(&buf, "asql_cluster_leader_lease_active", "Whether the current group lease is active (1) or expired (0).", "gauge")
	writeMetricHelp(&buf, "asql_cluster_replication_lag_lsn", "Observed local replication lag behind the group leader in LSN units.", "gauge")
	for _, state := range leadership {
		labels := map[string]string{
			"group":         state.Group,
			"leader_id":     state.LeaderID,
			"local_node_id": server.config.NodeID,
		}
		replicationLag := 0.0
		if state.LastSafeLSN > server.lastDurableLSN() {
			replicationLag = float64(state.LastSafeLSN - server.lastDurableLSN())
		}
		writeMetricValue(&buf, "asql_cluster_current_leader_info", labels, 1)
		writeMetricValue(&buf, "asql_cluster_leader_term", map[string]string{"group": state.Group, "leader_id": state.LeaderID}, float64(state.Term))
		writeMetricValue(&buf, "asql_cluster_last_safe_lsn", map[string]string{"group": state.Group, "leader_id": state.LeaderID}, float64(state.LastSafeLSN))
		writeMetricValue(&buf, "asql_cluster_leader_lease_active", map[string]string{"group": state.Group, "leader_id": state.LeaderID}, boolFloat(state.LeaseActive))
		writeMetricValue(&buf, "asql_cluster_replication_lag_lsn", map[string]string{"group": state.Group, "leader_id": state.LeaderID, "local_node_id": server.config.NodeID}, replicationLag)
	}

	if server.raftNode != nil {
		writeMetricHelp(&buf, "asql_cluster_raft_term", "Current local Raft term.", "gauge")
		writeMetricValue(&buf, "asql_cluster_raft_term", map[string]string{"node_id": server.config.NodeID}, float64(server.raftNode.CurrentTerm()))
		writeMetricHelp(&buf, "asql_cluster_raft_role_info", "Current local Raft role.", "gauge")
		writeMetricValue(&buf, "asql_cluster_raft_role_info", map[string]string{"node_id": server.config.NodeID, "role": server.raftNode.Role()}, 1)
	}

	return buf.Bytes()
}

func writeAdminJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeMetricHelp(buf *bytes.Buffer, name, help, typ string) {
	fmt.Fprintf(buf, "# HELP %s %s\n", name, help)
	fmt.Fprintf(buf, "# TYPE %s %s\n", name, typ)
}

func writeMetricValue(buf *bytes.Buffer, name string, labels map[string]string, value float64) {
	buf.WriteString(name)
	if len(labels) > 0 {
		keys := make([]string, 0, len(labels))
		for key := range labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(key)
			buf.WriteString("=\"")
			buf.WriteString(prometheusEscape(labels[key]))
			buf.WriteByte('"')
		}
		buf.WriteByte('}')
	}
	buf.WriteByte(' ')
	buf.WriteString(strconv.FormatFloat(value, 'f', -1, 64))
	buf.WriteByte('\n')
}

func writeQuantiles(buf *bytes.Buffer, name, help string, p50, p95, p99 float64) {
	writeMetricHelp(buf, name, help, "gauge")
	writeMetricValue(buf, name, map[string]string{"quantile": "0.50"}, millisecondsToSeconds(p50))
	writeMetricValue(buf, name, map[string]string{"quantile": "0.95"}, millisecondsToSeconds(p95))
	writeMetricValue(buf, name, map[string]string{"quantile": "0.99"}, millisecondsToSeconds(p99))
}

func millisecondsToSeconds(ms float64) float64 {
	return ms / 1000.0
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func prometheusEscape(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"")
	return replacer.Replace(value)
}
