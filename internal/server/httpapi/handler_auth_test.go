package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/wal"
)

func TestHTTPAPIPrincipalAuthorization(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()

	walStore, err := wal.NewSegmentedLogStore(filepath.Join(baseDir, "http-auth.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}

	engine, err := executor.New(ctx, walStore, filepath.Join(baseDir, "snaps"))
	if err != nil {
		_ = walStore.Close()
		t.Fatalf("new executor engine: %v", err)
	}

	svc := newService(engine, walStore, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, "")
	mux := http.NewServeMux()
	svc.RegisterRoutes(mux)
	server := httptest.NewServer(mux)

	t.Cleanup(func() {
		server.Close()
		close(svc.cleanupClose)
		svc.stopAuditWriter()
		engine.WaitPendingSnapshots()
		_ = walStore.Close()
	})

	if err := engine.BootstrapAdminPrincipal(ctx, "admin", "admin-secret"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}
	if err := engine.CreateUser(ctx, "analyst", "analyst-secret"); err != nil {
		t.Fatalf("create analyst principal: %v", err)
	}
	if err := engine.CreateUser(ctx, "historian", "historian-secret"); err != nil {
		t.Fatalf("create historian principal: %v", err)
	}
	if err := engine.GrantPrivilege(ctx, "historian", executor.PrincipalPrivilegeSelectHistory); err != nil {
		t.Fatalf("grant historian select_history: %v", err)
	}

	status, _, body := doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/begin", "", "", &BeginTxRequest{Mode: "domain", Domains: []string{"accounts"}}, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("begin without durable principal headers status = %d, want %d body=%s", status, http.StatusUnauthorized, body)
	}

	var adminBegin BeginTxResponse
	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/begin", "admin", "admin-secret", &BeginTxRequest{Mode: "domain", Domains: []string{"accounts"}}, &adminBegin)
	if status != http.StatusOK {
		t.Fatalf("admin begin status = %d, want %d body=%s", status, http.StatusOK, body)
	}

	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/execute", "admin", "admin-secret", &ExecuteRequest{TxID: adminBegin.TxID, SQL: "CREATE TABLE users (id INT, email TEXT)"}, nil)
	if status != http.StatusOK {
		t.Fatalf("admin create table status = %d, want %d body=%s", status, http.StatusOK, body)
	}
	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/execute", "admin", "admin-secret", &ExecuteRequest{TxID: adminBegin.TxID, SQL: "INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')"}, nil)
	if status != http.StatusOK {
		t.Fatalf("admin insert status = %d, want %d body=%s", status, http.StatusOK, body)
	}

	var adminCommit CommitTxResponse
	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/commit", "admin", "admin-secret", &CommitTxRequest{TxID: adminBegin.TxID}, &adminCommit)
	if status != http.StatusOK {
		t.Fatalf("admin commit status = %d, want %d body=%s", status, http.StatusOK, body)
	}

	var queryResp QueryResponse
	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/query", "analyst", "analyst-secret", &QueryRequest{SQL: "SELECT id, email FROM users", Domains: []string{"accounts"}}, &queryResp)
	if status != http.StatusOK {
		t.Fatalf("analyst query status = %d, want %d body=%s", status, http.StatusOK, body)
	}
	if len(queryResp.Rows) != 1 {
		t.Fatalf("analyst query rows = %d, want 1", len(queryResp.Rows))
	}

	var analystBegin BeginTxResponse
	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/begin", "analyst", "analyst-secret", &BeginTxRequest{Mode: "domain", Domains: []string{"accounts"}}, &analystBegin)
	if status != http.StatusOK {
		t.Fatalf("analyst begin status = %d, want %d body=%s", status, http.StatusOK, body)
	}

	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/execute", "analyst", "analyst-secret", &ExecuteRequest{TxID: analystBegin.TxID, SQL: "INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')"}, nil)
	if status != http.StatusForbidden {
		t.Fatalf("analyst insert status = %d, want %d body=%s", status, http.StatusForbidden, body)
	}
	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/rollback", "analyst", "analyst-secret", &RollbackTxRequest{TxID: analystBegin.TxID}, nil)
	if status != http.StatusOK {
		t.Fatalf("analyst rollback status = %d, want %d body=%s", status, http.StatusOK, body)
	}

	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/time-travel", "analyst", "analyst-secret", &TimeTravelQueryRequest{SQL: "SELECT id, email FROM users", Domains: []string{"accounts"}, LSN: adminCommit.CommitLSN}, nil)
	if status != http.StatusForbidden {
		t.Fatalf("analyst time-travel status = %d, want %d body=%s", status, http.StatusForbidden, body)
	}

	var historyResp TimeTravelQueryResponse
	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/time-travel", "historian", "historian-secret", &TimeTravelQueryRequest{SQL: "SELECT id, email FROM users", Domains: []string{"accounts"}, LSN: adminCommit.CommitLSN}, &historyResp)
	if status != http.StatusOK {
		t.Fatalf("historian time-travel status = %d, want %d body=%s", status, http.StatusOK, body)
	}
	if len(historyResp.Rows) != 1 {
		t.Fatalf("historian time-travel rows = %d, want 1", len(historyResp.Rows))
	}

	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/schema-snapshot", "analyst", "analyst-secret", &SchemaSnapshotRequest{Domains: []string{"accounts"}}, nil)
	if status != http.StatusForbidden {
		t.Fatalf("analyst schema snapshot status = %d, want %d body=%s", status, http.StatusForbidden, body)
	}
	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/schema-snapshot", "admin", "admin-secret", &SchemaSnapshotRequest{Domains: []string{"accounts"}}, nil)
	if status != http.StatusOK {
		t.Fatalf("admin schema snapshot status = %d, want %d body=%s", status, http.StatusOK, body)
	}

	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/replay-to-lsn", "analyst", "analyst-secret", &ReplayToLSNRequest{LSN: adminCommit.CommitLSN}, nil)
	if status != http.StatusForbidden {
		t.Fatalf("analyst replay status = %d, want %d body=%s", status, http.StatusForbidden, body)
	}
	status, _, body = doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/replay-to-lsn", "admin", "admin-secret", &ReplayToLSNRequest{LSN: adminCommit.CommitLSN}, nil)
	if status != http.StatusOK {
		t.Fatalf("admin replay status = %d, want %d body=%s", status, http.StatusOK, body)
	}

	status, _, body = doJSONRequest(t, server.URL, http.MethodGet, "/api/v1/last-lsn", "analyst", "analyst-secret", nil, nil)
	if status != http.StatusForbidden {
		t.Fatalf("analyst last-lsn status = %d, want %d body=%s", status, http.StatusForbidden, body)
	}
	status, _, body = doJSONRequest(t, server.URL, http.MethodGet, "/api/v1/last-lsn", "admin", "admin-secret", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("admin last-lsn status = %d, want %d body=%s", status, http.StatusOK, body)
	}
}

func doJSONRequest(t *testing.T, baseURL, method, path, principal, password string, payload any, out any) (int, http.Header, string) {
	t.Helper()

	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload for %s: %v", path, err)
		}
		body = bytes.NewReader(raw)
	}

	req, err := http.NewRequest(method, baseURL+path, body)
	if err != nil {
		t.Fatalf("new request %s: %v", path, err)
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if principal != "" {
		req.Header.Set(principalHeader, principal)
	}
	if password != "" {
		req.Header.Set(passwordHeader, password)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request %s: %v", path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body %s: %v", path, err)
	}
	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			t.Fatalf("unmarshal response %s: %v body=%s", path, err, string(respBody))
		}
	}

	return resp.StatusCode, resp.Header.Clone(), string(respBody)
}
