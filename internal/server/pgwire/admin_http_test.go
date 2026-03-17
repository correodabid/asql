package pgwire

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/cluster/raft"
	"asql/internal/engine/executor"
	api "asql/pkg/adminapi"

	"github.com/jackc/pgx/v5"
)

func startAdminSmokeServer(t *testing.T, config Config) (*Server, string, string, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	server, err := New(config)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for test: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeOnListener(ctx, listener)
	}()

	var adminAddr string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if server.adminListener != nil {
			adminAddr = server.adminListener.Addr().String()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if adminAddr == "" {
		cancel()
		server.Stop()
		t.Fatal("timeout waiting for admin http listener")
	}

	cleanup := func() {
		cancel()
		server.Stop()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("pgwire server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire server shutdown")
		}
	}

	return server, listener.Addr().String(), adminAddr, cleanup
}

func TestRuntimeAndAdminHTTPSmokeFlow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, pgwireAddr, adminAddr, cleanup := startAdminSmokeServer(t, Config{
		Address:         "127.0.0.1:0",
		AdminHTTPAddr:   "127.0.0.1:0",
		DataDirPath:     filepath.Join(t.TempDir(), "data"),
		Logger:          logger,
		AdminReadToken:  "read-secret",
		AdminWriteToken: "write-secret",
	})
	defer cleanup()

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT PRIMARY KEY, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	client := &http.Client{Timeout: 2 * time.Second}

	assertStatus := func(path string, want int) []byte {
		t.Helper()
		resp, err := client.Get("http://" + adminAddr + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read %s response: %v", path, err)
		}
		if resp.StatusCode != want {
			t.Fatalf("unexpected %s status: got %d want %d body=%s", path, resp.StatusCode, want, string(body))
		}
		return body
	}

	livezBody := assertStatus("/livez", http.StatusOK)
	if !strings.Contains(string(livezBody), `"live":true`) {
		t.Fatalf("unexpected livez payload: %s", string(livezBody))
	}

	readyzBody := assertStatus("/readyz", http.StatusOK)
	if !strings.Contains(string(readyzBody), `"ready":true`) {
		t.Fatalf("unexpected readyz payload: %s", string(readyzBody))
	}

	metricsBody := assertStatus("/metrics", http.StatusOK)
	for _, fragment := range []string{
		"asql_engine_begins_total",
		"asql_engine_commits_total 1",
		"asql_engine_fsync_errors_total 0",
	} {
		if !strings.Contains(string(metricsBody), fragment) {
			t.Fatalf("metrics body missing fragment %q\n%s", fragment, string(metricsBody))
		}
	}

	req, err := http.NewRequest(http.MethodGet, "http://"+adminAddr+"/api/v1/last-lsn", nil)
	if err != nil {
		t.Fatalf("new authorized request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer read-secret")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("authorized last-lsn request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected last-lsn status: got %d want %d body=%s", resp.StatusCode, http.StatusOK, string(body))
	}
	var lastLSN map[string]uint64
	if err := json.NewDecoder(resp.Body).Decode(&lastLSN); err != nil {
		t.Fatalf("decode last-lsn response: %v", err)
	}
	if lastLSN["last_lsn"] == 0 {
		t.Fatalf("expected durable lsn after commit, got %+v", lastLSN)
	}

	req, err = http.NewRequest(http.MethodGet, "http://"+adminAddr+"/api/v1/last-lsn", nil)
	if err != nil {
		t.Fatalf("new unauthorized request: %v", err)
	}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("unauthorized last-lsn request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected unauthorized status: got %d want %d body=%s", resp.StatusCode, http.StatusUnauthorized, string(body))
	}
}

func TestAdminMetricsExposeFailoverLeaderAndSafeLSN(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      logger,
		NodeID:      "node-a",
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(server.Stop)

	state, err := server.leadership.TryAcquireLeadership("orders", "node-a", 42)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}
	_, err = server.leadership.RenewLeadership("orders", "node-a", state.FencingToken, 99)
	if err != nil {
		t.Fatalf("renew leadership: %v", err)
	}
	server.metrics.OnFailoverTransition(coordinator.FailoverTransition{
		Phase:  coordinator.FailoverPhasePromotedLeader,
		Group:  "orders",
		Term:   2,
		NodeID: "node-a",
	})

	for _, sql := range []string{
		"BEGIN CROSS DOMAIN billing, risk;",
		"ROLLBACK;",
		"BEGIN CROSS DOMAIN billing, ledger, risk;",
		"ROLLBACK;",
	} {
		if _, err := server.engine.Execute(context.Background(), server.engine.NewSession(), sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	res := httptest.NewRecorder()
	server.handleMetrics(res, req)

	body := res.Body.String()
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", res.Code, http.StatusOK)
	}
	for _, fragment := range []string{
		`asql_cluster_failovers_total{group="orders"} 1`,
		`asql_cluster_current_leader_info{group="orders",leader_id="node-a",local_node_id="node-a"} 1`,
		`asql_cluster_last_safe_lsn{group="orders",leader_id="node-a"} 99`,
		`asql_engine_begins_total 2`,
		`asql_engine_cross_domain_begins_total 2`,
		`asql_engine_cross_domain_begin_domains_avg 2.5`,
		`asql_engine_cross_domain_begin_domains_max 3`,
		`asql_engine_fsync_errors_total 0`,
		`asql_engine_audit_errors_total 0`,
		`asql_audit_log_size_bytes`,
	} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("metrics body missing fragment %q\n%s", fragment, body)
		}
	}
}

func TestAdminHTTPSecurityPrincipalManagementFlow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, _, adminAddr, cleanup := startAdminSmokeServer(t, Config{
		Address:         "127.0.0.1:0",
		AdminHTTPAddr:   "127.0.0.1:0",
		DataDirPath:     filepath.Join(t.TempDir(), "data"),
		Logger:          logger,
		AdminReadToken:  "read-secret",
		AdminWriteToken: "write-secret",
	})
	defer cleanup()

	client := &http.Client{Timeout: 2 * time.Second}
	doJSON := func(method, path, token string, payload any, out any) int {
		t.Helper()
		var body io.Reader
		if payload != nil {
			data, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("marshal %s payload: %v", path, err)
			}
			body = bytes.NewReader(data)
		}
		req, err := http.NewRequest(method, "http://"+adminAddr+path, body)
		if err != nil {
			t.Fatalf("new %s request: %v", path, err)
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("do %s request: %v", path, err)
		}
		defer resp.Body.Close()
		if out != nil {
			if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
				t.Fatalf("decode %s response: %v", path, err)
			}
		}
		return resp.StatusCode
	}

	var bootstrap api.SecurityMutationResponse
	status := doJSON(http.MethodPost, "/api/v1/security/bootstrap-admin", "write-secret", api.BootstrapAdminPrincipalRequest{
		Principal: "admin",
		Password:  "secret-pass",
	}, &bootstrap)
	if status != http.StatusOK {
		t.Fatalf("unexpected bootstrap status: got %d want %d", status, http.StatusOK)
	}
	if bootstrap.Principal == nil || bootstrap.Principal.Name != "admin" || bootstrap.Principal.Kind != executor.PrincipalKindUser {
		t.Fatalf("unexpected bootstrap response: %+v", bootstrap)
	}

	var roleResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/roles", "write-secret", api.CreateRoleRequest{Principal: "history_readers"}, &roleResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected create role status: got %d want %d", status, http.StatusOK)
	}

	var privilegeResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/privileges/grant", "write-secret", api.GrantPrivilegeRequest{
		Principal: "history_readers",
		Privilege: "SELECT_HISTORY",
	}, &privilegeResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected grant privilege status: got %d want %d", status, http.StatusOK)
	}

	var userResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/users", "write-secret", api.CreateUserRequest{
		Principal: "analyst",
		Password:  "analyst-pass",
	}, &userResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected create user status: got %d want %d", status, http.StatusOK)
	}

	var grantRoleResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/roles/grant", "write-secret", api.GrantRoleRequest{
		Principal: "analyst",
		Role:      "history_readers",
	}, &grantRoleResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected grant role status: got %d want %d", status, http.StatusOK)
	}

	var list api.ListPrincipalsResponse
	status = doJSON(http.MethodGet, "/api/v1/security/principals", "read-secret", nil, &list)
	if status != http.StatusOK {
		t.Fatalf("unexpected list principals status: got %d want %d", status, http.StatusOK)
	}
	if len(list.Principals) != 3 {
		t.Fatalf("unexpected principal count: got %d want 3 (%+v)", len(list.Principals), list.Principals)
	}
	for _, principal := range list.Principals {
		if principal.Name == "analyst" && len(principal.EffectivePrivileges) == 0 {
			t.Fatalf("expected analyst effective privileges in list response: %+v", principal)
		}
	}

	analyst, ok := server.engine.Principal("analyst")
	if !ok {
		t.Fatal("expected analyst principal in engine state")
	}
	if !server.engine.HasPrincipalPrivilege("analyst", executor.PrincipalPrivilegeSelectHistory) {
		t.Fatalf("expected analyst to inherit SELECT_HISTORY, got %+v", analyst)
	}

	var revokeResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/privileges/revoke", "write-secret", api.RevokePrivilegeRequest{
		Principal: "history_readers",
		Privilege: "SELECT_HISTORY",
	}, &revokeResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected revoke privilege status: got %d want %d", status, http.StatusOK)
	}
	if server.engine.HasPrincipalPrivilege("analyst", executor.PrincipalPrivilegeSelectHistory) {
		t.Fatal("expected analyst to lose SELECT_HISTORY after revoke")
	}

	var restorePrivilegeResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/privileges/grant", "write-secret", api.GrantPrivilegeRequest{
		Principal: "history_readers",
		Privilege: "SELECT_HISTORY",
	}, &restorePrivilegeResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected restore privilege status: got %d want %d", status, http.StatusOK)
	}

	var restoreGrantRoleResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/roles/grant", "write-secret", api.GrantRoleRequest{
		Principal: "analyst",
		Role:      "history_readers",
	}, &restoreGrantRoleResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected restore grant role status: got %d want %d", status, http.StatusOK)
	}

	var revokeRoleResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/roles/revoke", "write-secret", api.RevokeRoleRequest{
		Principal: "analyst",
		Role:      "history_readers",
	}, &revokeRoleResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected revoke role status: got %d want %d", status, http.StatusOK)
	}
	if server.engine.HasPrincipalPrivilege("analyst", executor.PrincipalPrivilegeSelectHistory) {
		t.Fatal("expected analyst to lose SELECT_HISTORY after role revoke")
	}

	var setPasswordResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/passwords/set", "write-secret", api.SetPasswordRequest{
		Principal: "analyst",
		Password:  "rotated-pass",
	}, &setPasswordResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected set password status: got %d want %d", status, http.StatusOK)
	}
	if _, err := server.engine.AuthenticatePrincipal("analyst", "analyst-pass"); err == nil {
		t.Fatal("expected old analyst password to fail after admin password rotation")
	}
	if _, err := server.engine.AuthenticatePrincipal("analyst", "rotated-pass"); err != nil {
		t.Fatalf("expected rotated analyst password to authenticate: %v", err)
	}

	var disableResp api.SecurityMutationResponse
	status = doJSON(http.MethodPost, "/api/v1/security/principals/disable", "write-secret", api.DisablePrincipalRequest{
		Principal: "analyst",
	}, &disableResp)
	if status != http.StatusOK {
		t.Fatalf("unexpected disable principal status: got %d want %d", status, http.StatusOK)
	}
	if disableResp.Principal == nil || disableResp.Principal.Enabled {
		t.Fatalf("expected disabled principal response, got %+v", disableResp)
	}
}

func TestAdminReadyzAndLeadershipEndpoints(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      logger,
		NodeID:      "node-a",
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(server.Stop)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	res := httptest.NewRecorder()
	server.handleReadyz(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected readyz status: got %d want %d", res.Code, http.StatusOK)
	}
	var ready adminHealthResponse
	if err := json.Unmarshal(res.Body.Bytes(), &ready); err != nil {
		t.Fatalf("unmarshal readyz: %v", err)
	}
	if !ready.Ready || !ready.Live {
		t.Fatalf("expected standalone server to be ready/live, got %+v", ready)
	}

	server.raftNode = &raft.RaftNode{}
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	res = httptest.NewRecorder()
	server.handleReadyz(res, req)
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("unexpected clustered readyz status: got %d want %d", res.Code, http.StatusServiceUnavailable)
	}

	state, err := server.leadership.TryAcquireLeadership("billing", "node-a", 7)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}
	_, err = server.leadership.RenewLeadership("billing", "node-a", state.FencingToken, 11)
	if err != nil {
		t.Fatalf("renew leadership: %v", err)
	}
	server.metrics.OnFailoverTransition(coordinator.FailoverTransition{
		Phase:  coordinator.FailoverPhasePromotedLeader,
		Group:  "billing",
		Term:   7,
		NodeID: "node-a",
	})

	req = httptest.NewRequest(http.MethodGet, "/api/v1/leadership-state", nil)
	res = httptest.NewRecorder()
	server.handleAdminLeadershipState(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected leadership status: got %d want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), `"group_name":"billing"`) || !strings.Contains(res.Body.String(), `"last_safe_lsn":11`) {
		t.Fatalf("unexpected leadership payload: %s", res.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/last-lsn", nil)
	res = httptest.NewRecorder()
	server.handleAdminLastLSN(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected last-lsn status: got %d want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), `"last_lsn":0`) {
		t.Fatalf("unexpected last-lsn payload: %s", res.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/failover-history", nil)
	res = httptest.NewRecorder()
	server.handleAdminFailoverHistory(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected failover-history status: got %d want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), `"phase":"promoted_leader"`) {
		t.Fatalf("unexpected failover-history payload: %s", res.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/snapshot-catalog", nil)
	res = httptest.NewRecorder()
	server.handleAdminSnapshotCatalog(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected snapshot-catalog status: got %d want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), `"snapshots"`) {
		t.Fatalf("unexpected snapshot-catalog payload: %s", res.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/wal-retention", nil)
	res = httptest.NewRecorder()
	server.handleAdminWALRetention(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected wal-retention status: got %d want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), `"segment_count"`) {
		t.Fatalf("unexpected wal-retention payload: %s", res.Body.String())
	}
}

func TestAdminRecoveryBackupEndpoints(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      logger,
		NodeID:      "node-a",
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(server.Stop)

	backupDir := filepath.Join(t.TempDir(), "backup")
	createBody, err := json.Marshal(api.RecoveryCreateBackupRequest{BackupDir: backupDir})
	if err != nil {
		t.Fatalf("marshal backup-create request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recovery/backup-create", bytes.NewReader(createBody))
	res := httptest.NewRecorder()
	server.handleAdminRecoveryCreateBackup(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected backup-create status: got %d want %d body=%s", res.Code, http.StatusOK, res.Body.String())
	}
	var manifest api.BaseBackupManifest
	if err := json.Unmarshal(res.Body.Bytes(), &manifest); err != nil {
		t.Fatalf("unmarshal backup-create response: %v", err)
	}
	if manifest.Version == 0 {
		t.Fatalf("expected backup manifest version, got %+v", manifest)
	}

	manifestBody, err := json.Marshal(api.RecoveryBackupManifestRequest{BackupDir: backupDir})
	if err != nil {
		t.Fatalf("marshal backup-manifest request: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/recovery/backup-manifest", bytes.NewReader(manifestBody))
	res = httptest.NewRecorder()
	server.handleAdminRecoveryBackupManifest(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected backup-manifest status: got %d want %d body=%s", res.Code, http.StatusOK, res.Body.String())
	}

	verifyBody, err := json.Marshal(api.RecoveryVerifyBackupRequest{BackupDir: backupDir})
	if err != nil {
		t.Fatalf("marshal backup-verify request: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/recovery/backup-verify", bytes.NewReader(verifyBody))
	res = httptest.NewRecorder()
	server.handleAdminRecoveryVerifyBackup(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected backup-verify status: got %d want %d body=%s", res.Code, http.StatusOK, res.Body.String())
	}
	var verifyResp api.RecoveryVerifyBackupResponse
	if err := json.Unmarshal(res.Body.Bytes(), &verifyResp); err != nil {
		t.Fatalf("unmarshal backup-verify response: %v", err)
	}
	if verifyResp.Status != "OK" {
		t.Fatalf("expected verify status OK, got %+v", verifyResp)
	}
	if verifyResp.Manifest.Version != manifest.Version {
		t.Fatalf("expected matching manifest versions, got create=%d verify=%d", manifest.Version, verifyResp.Manifest.Version)
	}
}

func TestAdminAPIAuthScopes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := New(Config{
		Address:         "127.0.0.1:0",
		DataDirPath:     filepath.Join(t.TempDir(), "data"),
		Logger:          logger,
		AdminReadToken:  "read-secret",
		AdminWriteToken: "write-secret",
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(server.Stop)

	readHandler := server.withAdminAuth(adminScopeRead, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	writeHandler := server.withAdminAuth(adminScopeWrite, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/last-lsn", nil)
	res := httptest.NewRecorder()
	readHandler(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized read request, got %d", res.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/last-lsn", nil)
	req.Header.Set("Authorization", "Bearer read-secret")
	res = httptest.NewRecorder()
	readHandler(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("expected read token to pass, got %d", res.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/recovery/restore-lsn", nil)
	req.Header.Set("Authorization", "Bearer read-secret")
	res = httptest.NewRecorder()
	writeHandler(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected read token to fail write scope, got %d", res.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/recovery/restore-lsn", nil)
	req.Header.Set("Authorization", "Bearer write-secret")
	res = httptest.NewRecorder()
	writeHandler(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("expected write token to pass, got %d", res.Code)
	}
}

func TestAdminAPIAuthFallsBackToSharedToken(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      logger,
		AuthToken:   "shared-secret",
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(server.Stop)

	handler := server.withAdminAuth(adminScopeWrite, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recovery/backup-create", nil)
	req.Header.Set("Authorization", "Bearer shared-secret")
	res := httptest.NewRecorder()
	handler(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("expected shared auth token to authorize admin write path, got %d", res.Code)
	}
}

func TestAdminRecoveryInspectionAndValidationEndpoints(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := filepath.Join(t.TempDir(), "data")
	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: dataDir,
		Logger:      logger,
		NodeID:      "node-a",
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(server.Stop)

	snapshotBody, err := json.Marshal(api.RecoverySnapshotCatalogRequest{})
	if err != nil {
		t.Fatalf("marshal snapshot-catalog request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recovery/snapshot-catalog", bytes.NewReader(snapshotBody))
	res := httptest.NewRecorder()
	server.handleAdminRecoverySnapshotCatalog(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected recovery snapshot-catalog status: got %d want %d body=%s", res.Code, http.StatusOK, res.Body.String())
	}

	retentionBody, err := json.Marshal(api.RecoveryWALRetentionRequest{})
	if err != nil {
		t.Fatalf("marshal wal-retention request: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/recovery/wal-retention", bytes.NewReader(retentionBody))
	res = httptest.NewRecorder()
	server.handleAdminRecoveryWALRetention(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected recovery wal-retention status: got %d want %d body=%s", res.Code, http.StatusOK, res.Body.String())
	}
	var retention api.WALRetentionState
	if err := json.Unmarshal(res.Body.Bytes(), &retention); err != nil {
		t.Fatalf("unmarshal wal-retention response: %v", err)
	}
	if retention.DataDir != dataDir {
		t.Fatalf("expected wal-retention data dir %q, got %q", dataDir, retention.DataDir)
	}

	restoreBody, err := json.Marshal(api.RecoveryRestoreLSNRequest{BackupDir: filepath.Join(t.TempDir(), "backup"), TargetDataDir: filepath.Join(t.TempDir(), "target")})
	if err != nil {
		t.Fatalf("marshal restore-lsn request: %v", err)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/recovery/restore-lsn", bytes.NewReader(restoreBody))
	res = httptest.NewRecorder()
	server.handleAdminRecoveryRestoreLSN(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("unexpected restore-lsn validation status: got %d want %d body=%s", res.Code, http.StatusBadRequest, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "lsn must be greater than zero") {
		t.Fatalf("unexpected restore-lsn validation payload: %s", res.Body.String())
	}
}
