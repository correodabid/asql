package pgwire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/engine/executor"
	api "asql/pkg/adminapi"
)

const maxFailoverHistoryEntries = 64

type adminScope string

const (
	adminScopeRead  adminScope = "read"
	adminScopeWrite adminScope = "write"
)

type runtimeMetrics struct {
	nodeID string

	mu                 sync.Mutex
	failoverPromotions map[string]uint64
	failoverHistory    []coordinator.FailoverTransition
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
	mux.HandleFunc("/api/v1/health", server.withAdminAuth(adminScopeRead, server.handleAdminHealth))
	mux.HandleFunc("/api/v1/leadership-state", server.withAdminAuth(adminScopeRead, server.handleAdminLeadershipState))
	mux.HandleFunc("/api/v1/last-lsn", server.withAdminAuth(adminScopeRead, server.handleAdminLastLSN))
	mux.HandleFunc("/api/v1/failover-history", server.withAdminAuth(adminScopeRead, server.handleAdminFailoverHistory))
	mux.HandleFunc("/api/v1/snapshot-catalog", server.withAdminAuth(adminScopeRead, server.handleAdminSnapshotCatalog))
	mux.HandleFunc("/api/v1/wal-retention", server.withAdminAuth(adminScopeRead, server.handleAdminWALRetention))
	mux.HandleFunc("/api/v1/security/principals", server.withAdminAuth(adminScopeRead, server.handleAdminListPrincipals))
	mux.HandleFunc("/api/v1/security/bootstrap-admin", server.withAdminAuth(adminScopeWrite, server.handleAdminBootstrapPrincipal))
	mux.HandleFunc("/api/v1/security/users", server.withAdminAuth(adminScopeWrite, server.handleAdminCreateUser))
	mux.HandleFunc("/api/v1/security/roles", server.withAdminAuth(adminScopeWrite, server.handleAdminCreateRole))
	mux.HandleFunc("/api/v1/security/privileges/grant", server.withAdminAuth(adminScopeWrite, server.handleAdminGrantPrivilege))
	mux.HandleFunc("/api/v1/security/privileges/revoke", server.withAdminAuth(adminScopeWrite, server.handleAdminRevokePrivilege))
	mux.HandleFunc("/api/v1/security/roles/grant", server.withAdminAuth(adminScopeWrite, server.handleAdminGrantRole))
	mux.HandleFunc("/api/v1/security/roles/revoke", server.withAdminAuth(adminScopeWrite, server.handleAdminRevokeRole))
	mux.HandleFunc("/api/v1/security/passwords/set", server.withAdminAuth(adminScopeWrite, server.handleAdminSetPrincipalPassword))
	mux.HandleFunc("/api/v1/security/principals/disable", server.withAdminAuth(adminScopeWrite, server.handleAdminDisablePrincipal))
	mux.HandleFunc("/api/v1/security/principals/enable", server.withAdminAuth(adminScopeWrite, server.handleAdminEnablePrincipal))
	mux.HandleFunc("/api/v1/recovery/backup-create", server.withAdminAuth(adminScopeWrite, server.handleAdminRecoveryCreateBackup))
	mux.HandleFunc("/api/v1/recovery/backup-manifest", server.withAdminAuth(adminScopeRead, server.handleAdminRecoveryBackupManifest))
	mux.HandleFunc("/api/v1/recovery/backup-verify", server.withAdminAuth(adminScopeRead, server.handleAdminRecoveryVerifyBackup))
	mux.HandleFunc("/api/v1/recovery/restore-lsn", server.withAdminAuth(adminScopeWrite, server.handleAdminRecoveryRestoreLSN))
	mux.HandleFunc("/api/v1/recovery/restore-timestamp", server.withAdminAuth(adminScopeWrite, server.handleAdminRecoveryRestoreTimestamp))
	mux.HandleFunc("/api/v1/recovery/snapshot-catalog", server.withAdminAuth(adminScopeRead, server.handleAdminRecoverySnapshotCatalog))
	mux.HandleFunc("/api/v1/recovery/wal-retention", server.withAdminAuth(adminScopeRead, server.handleAdminRecoveryWALRetention))

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

func (server *Server) withAdminAuth(scope adminScope, next http.HandlerFunc) http.HandlerFunc {
	if next == nil {
		return func(w http.ResponseWriter, _ *http.Request) {
			writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": "admin handler unavailable"})
		}
	}
	token := server.adminToken(scope)
	if token == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if !validateAdminAuthorization(r, token) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="asql-admin"`)
			writeAdminJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin authorization is required"})
			return
		}
		next(w, r)
	}
}

func (server *Server) adminToken(scope adminScope) string {
	if server == nil {
		return ""
	}
	readToken := strings.TrimSpace(server.config.AdminReadToken)
	writeToken := strings.TrimSpace(server.config.AdminWriteToken)
	sharedToken := strings.TrimSpace(server.config.AuthToken)
	if readToken == "" {
		if sharedToken != "" {
			readToken = sharedToken
		} else {
			readToken = writeToken
		}
	}
	if writeToken == "" {
		if sharedToken != "" {
			writeToken = sharedToken
		} else {
			writeToken = readToken
		}
	}
	if scope == adminScopeWrite {
		return writeToken
	}
	return readToken
}

func validateAdminAuthorization(r *http.Request, expected string) bool {
	if strings.TrimSpace(expected) == "" {
		return true
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if auth == "" {
		return false
	}
	const bearerPrefix = "Bearer "
	if !strings.HasPrefix(auth, bearerPrefix) {
		return false
	}
	provided := strings.TrimSpace(auth[len(bearerPrefix):])
	return provided == expected
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

func (server *Server) handleAdminListPrincipals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	writeAdminJSON(w, http.StatusOK, api.ListPrincipalsResponse{Principals: toAdminPrincipalRecords(server.engine.ListPrincipals())})
}

func (server *Server) handleAdminBootstrapPrincipal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.BootstrapAdminPrincipalRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	if err := server.engine.BootstrapAdminPrincipal(r.Context(), req.Principal, req.Password); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) handleAdminCreateUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.CreateUserRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	if err := server.engine.CreateUser(r.Context(), req.Principal, req.Password); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) handleAdminCreateRole(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.CreateRoleRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	if err := server.engine.CreateRole(r.Context(), req.Principal); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) handleAdminGrantPrivilege(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.GrantPrivilegeRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	privilege, err := executor.ParsePrincipalPrivilege(req.Privilege)
	if err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := server.engine.GrantPrivilege(r.Context(), req.Principal, privilege); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) handleAdminGrantRole(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.GrantRoleRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	if err := server.engine.GrantRole(r.Context(), req.Principal, req.Role); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) handleAdminRevokePrivilege(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.RevokePrivilegeRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	privilege, err := executor.ParsePrincipalPrivilege(req.Privilege)
	if err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := server.engine.RevokePrivilege(r.Context(), req.Principal, privilege); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) handleAdminRevokeRole(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.RevokeRoleRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	if err := server.engine.RevokeRole(r.Context(), req.Principal, req.Role); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) handleAdminSetPrincipalPassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.SetPasswordRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	if err := server.engine.SetPrincipalPassword(r.Context(), req.Principal, req.Password); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) handleAdminDisablePrincipal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.DisablePrincipalRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	if err := server.engine.DisablePrincipal(r.Context(), req.Principal); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) handleAdminEnablePrincipal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.EnablePrincipalRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	if err := server.engine.EnablePrincipal(r.Context(), req.Principal); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	server.writeSecurityMutationResponse(w, http.StatusOK, strings.TrimSpace(req.Principal))
}

func (server *Server) writeSecurityMutationResponse(w http.ResponseWriter, statusCode int, principal string) {
	if server == nil || server.engine == nil {
		writeAdminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "engine unavailable"})
		return
	}
	info, ok := server.engine.Principal(principal)
	if !ok {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": "principal state unavailable after mutation"})
		return
	}
	writeAdminJSON(w, statusCode, api.SecurityMutationResponse{
		Status:    "ok",
		Principal: toAdminPrincipalRecord(info),
	})
}

func (server *Server) handleAdminRecoveryCreateBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.RecoveryCreateBackupRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	dataDir := server.recoveryDataDir(req.DataDir)
	if dataDir == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "data directory is required"})
		return
	}
	if strings.TrimSpace(req.BackupDir) == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "backup directory is required"})
		return
	}
	manifest, err := executor.CreateBaseBackup(dataDir, strings.TrimSpace(req.BackupDir))
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusOK, toAdminBaseBackupManifest(manifest))
}

func (server *Server) handleAdminRecoveryBackupManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.RecoveryBackupManifestRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.BackupDir) == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "backup directory is required"})
		return
	}
	manifest, err := executor.LoadBaseBackupManifest(strings.TrimSpace(req.BackupDir))
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusOK, toAdminBaseBackupManifest(manifest))
}

func (server *Server) handleAdminRecoveryVerifyBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.RecoveryVerifyBackupRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.BackupDir) == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "backup directory is required"})
		return
	}
	manifest, err := executor.VerifyBaseBackup(strings.TrimSpace(req.BackupDir))
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusOK, api.RecoveryVerifyBackupResponse{Status: "OK", Manifest: toAdminBaseBackupManifest(manifest)})
}

func (server *Server) handleAdminRecoveryRestoreLSN(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.RecoveryRestoreLSNRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.BackupDir) == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "backup directory is required"})
		return
	}
	if strings.TrimSpace(req.TargetDataDir) == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "target data directory is required"})
		return
	}
	if req.LSN == 0 {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "lsn must be greater than zero"})
		return
	}
	result, err := executor.RestoreBaseBackupToLSN(r.Context(), strings.TrimSpace(req.BackupDir), strings.TrimSpace(req.TargetDataDir), req.LSN)
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusOK, api.RestoreResult{AppliedLSN: result.AppliedLSN, AppliedTimestamp: result.AppliedTimestamp})
}

func (server *Server) handleAdminRecoveryRestoreTimestamp(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.RecoveryRestoreTimestampRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	if strings.TrimSpace(req.BackupDir) == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "backup directory is required"})
		return
	}
	if strings.TrimSpace(req.TargetDataDir) == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "target data directory is required"})
		return
	}
	if req.LogicalTimestamp == 0 {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "logical timestamp must be greater than zero"})
		return
	}
	result, err := executor.RestoreBaseBackupToTimestamp(r.Context(), strings.TrimSpace(req.BackupDir), strings.TrimSpace(req.TargetDataDir), req.LogicalTimestamp)
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusOK, api.RestoreResult{AppliedLSN: result.AppliedLSN, AppliedTimestamp: result.AppliedTimestamp})
}

func (server *Server) handleAdminRecoverySnapshotCatalog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.RecoverySnapshotCatalogRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	dataDir := server.recoveryDataDir(req.DataDir)
	if dataDir == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "data directory is required"})
		return
	}
	entries, err := executor.InspectDataDirSnapshotCatalog(dataDir)
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusOK, api.RecoverySnapshotCatalogResponse{Snapshots: toAdminSnapshotCatalog(entries)})
}

func (server *Server) handleAdminRecoveryWALRetention(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req api.RecoveryWALRetentionRequest
	if !decodeAdminJSON(w, r, &req) {
		return
	}
	dataDir := server.recoveryDataDir(req.DataDir)
	if dataDir == "" {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "data directory is required"})
		return
	}
	state, err := executor.InspectDataDirWALRetention(dataDir)
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeAdminJSON(w, http.StatusOK, toAdminWALRetentionState(state))
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
	writeMetricHelp(&buf, "asql_engine_begins_total", "Total transactions opened by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_begins_total", nil, float64(perf.TotalBegins))
	writeMetricHelp(&buf, "asql_engine_cross_domain_begins_total", "Total cross-domain transactions opened by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_cross_domain_begins_total", nil, float64(perf.TotalCrossDomainBegins))
	writeMetricHelp(&buf, "asql_engine_reads_total", "Total read queries processed by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_reads_total", nil, float64(perf.TotalReads))
	writeMetricHelp(&buf, "asql_engine_time_travel_queries_total", "Total time-travel queries processed by the engine.", "counter")
	writeMetricValue(&buf, "asql_engine_time_travel_queries_total", nil, float64(perf.TotalTimeTravelQueries))
	writeMetricHelp(&buf, "asql_engine_active_transactions", "Current active transaction count.", "gauge")
	writeMetricValue(&buf, "asql_engine_active_transactions", nil, float64(perf.ActiveTransactions))
	writeMetricHelp(&buf, "asql_engine_cross_domain_begin_domains_avg", "Average domain fanout for cross-domain transaction openings.", "gauge")
	writeMetricValue(&buf, "asql_engine_cross_domain_begin_domains_avg", nil, perf.CrossDomainBeginAvgDomains)
	writeMetricHelp(&buf, "asql_engine_cross_domain_begin_domains_max", "Maximum domain fanout observed for a cross-domain transaction opening.", "gauge")
	writeMetricValue(&buf, "asql_engine_cross_domain_begin_domains_max", nil, float64(perf.CrossDomainBeginMaxDomains))

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

func decodeAdminJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("decode request: %v", err)})
		return false
	}
	var extra struct{}
	if err := decoder.Decode(&extra); err != io.EOF {
		writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "request body must contain a single JSON object"})
		return false
	}
	return true
}

func (server *Server) recoveryDataDir(requested string) string {
	if trimmed := strings.TrimSpace(requested); trimmed != "" {
		return trimmed
	}
	if server == nil {
		return ""
	}
	return strings.TrimSpace(server.config.DataDirPath)
}

func toAdminBaseBackupManifest(manifest executor.BaseBackupManifest) api.BaseBackupManifest {
	out := api.BaseBackupManifest{
		Version:       manifest.Version,
		HeadLSN:       manifest.HeadLSN,
		HeadTimestamp: manifest.HeadTimestamp,
		Snapshots:     make([]api.SnapshotBackupMetadata, 0, len(manifest.Snapshots)),
		WALSegments:   make([]api.WALSegmentBackupMetadata, 0, len(manifest.WALSegments)),
	}
	for _, snapshot := range manifest.Snapshots {
		out.Snapshots = append(out.Snapshots, api.SnapshotBackupMetadata{
			BackupFileMetadata: api.BackupFileMetadata{
				RelativePath: snapshot.RelativePath,
				Bytes:        snapshot.Bytes,
				SHA256:       snapshot.SHA256,
			},
			Sequence:  snapshot.Sequence,
			LSN:       snapshot.LSN,
			LogicalTS: snapshot.LogicalTS,
		})
	}
	for _, segment := range manifest.WALSegments {
		out.WALSegments = append(out.WALSegments, api.WALSegmentBackupMetadata{
			BackupFileMetadata: api.BackupFileMetadata{
				RelativePath: segment.RelativePath,
				Bytes:        segment.Bytes,
				SHA256:       segment.SHA256,
			},
			SeqNum:      segment.SeqNum,
			FirstLSN:    segment.FirstLSN,
			LastLSN:     segment.LastLSN,
			RecordCount: segment.RecordCount,
		})
	}
	if manifest.TimestampIndex != nil {
		out.TimestampIndex = &api.BackupFileMetadata{
			RelativePath: manifest.TimestampIndex.RelativePath,
			Bytes:        manifest.TimestampIndex.Bytes,
			SHA256:       manifest.TimestampIndex.SHA256,
		}
	}
	return out
}

func toAdminSnapshotCatalog(entries []executor.SnapshotCatalogEntry) []api.SnapshotCatalogEntry {
	out := make([]api.SnapshotCatalogEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, api.SnapshotCatalogEntry{
			FileName:  entry.FileName,
			Sequence:  entry.Sequence,
			LSN:       entry.LSN,
			LogicalTS: entry.LogicalTS,
			Bytes:     entry.Bytes,
			IsFull:    entry.IsFull,
		})
	}
	return out
}

func toAdminWALRetentionState(state executor.WALRetentionState) api.WALRetentionState {
	out := api.WALRetentionState{
		DataDir:             state.DataDir,
		RetainWAL:           state.RetainWAL,
		HeadLSN:             state.HeadLSN,
		OldestRetainedLSN:   state.OldestRetainedLSN,
		LastRetainedLSN:     state.LastRetainedLSN,
		SegmentCount:        state.SegmentCount,
		DiskSnapshotCount:   state.DiskSnapshotCount,
		MemorySnapshotCount: state.MemorySnapshotCount,
		MaxDiskSnapshots:    state.MaxDiskSnapshots,
		Segments:            make([]api.WALSegmentCatalogEntry, 0, len(state.Segments)),
	}
	for _, segment := range state.Segments {
		out.Segments = append(out.Segments, api.WALSegmentCatalogEntry{
			FileName:    segment.FileName,
			SeqNum:      segment.SeqNum,
			FirstLSN:    segment.FirstLSN,
			LastLSN:     segment.LastLSN,
			RecordCount: segment.RecordCount,
			Bytes:       segment.Bytes,
			Sealed:      segment.Sealed,
		})
	}
	return out
}

func toAdminPrincipalRecords(principals []executor.PrincipalInfo) []api.PrincipalRecord {
	out := make([]api.PrincipalRecord, 0, len(principals))
	for _, principal := range principals {
		out = append(out, *toAdminPrincipalRecord(principal))
	}
	return out
}

func toAdminPrincipalRecord(principal executor.PrincipalInfo) *api.PrincipalRecord {
	roles := append([]string(nil), principal.Roles...)
	privileges := append([]executor.PrincipalPrivilege(nil), principal.Privileges...)
	return &api.PrincipalRecord{
		Name:                principal.Name,
		Kind:                principal.Kind,
		Enabled:             principal.Enabled,
		Roles:               roles,
		EffectiveRoles:      append([]string(nil), principal.EffectiveRoles...),
		Privileges:          privileges,
		EffectivePrivileges: append([]executor.PrincipalPrivilege(nil), principal.EffectivePrivileges...),
	}
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
