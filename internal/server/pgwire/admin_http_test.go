package pgwire

import (
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
