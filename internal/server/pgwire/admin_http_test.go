package pgwire

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"asql/internal/cluster/coordinator"
	"asql/internal/cluster/raft"
	api "asql/pkg/adminapi"
)

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
