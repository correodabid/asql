package studioapp

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pgwireserver "asql/internal/server/pgwire"
	api "asql/pkg/adminapi"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestConnectionInfoAndSwitchConnection(t *testing.T) {
	addrOne := startStudioPGWireServer(t, "studio-pass")
	addrTwo := startStudioPGWireServer(t, "studio-pass")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	app := newApp(
		newEngineClient(addrOne, "studio-pass"),
		addrOne,
		nil,
		"",
		nil,
		nil,
		nil,
		nil,
		"admin-secret",
		filepath.Join(t.TempDir(), "data"),
		logger,
	)
	t.Cleanup(func() {
		app.peersMu.RLock()
		defer app.peersMu.RUnlock()
		closeEngineClients(app.engine, app.followerEngine)
	})

	info, err := app.ConnectionInfo()
	if err != nil {
		t.Fatalf("ConnectionInfo: %v", err)
	}
	if got := info["pgwire_endpoint"]; got != addrOne {
		t.Fatalf("unexpected initial pgwire endpoint: got %v want %q", got, addrOne)
	}
	if got := info["auth_token_configured"]; got != true {
		t.Fatalf("expected auth token to be reported as configured, got %v", got)
	}

	recoveryDir := filepath.Join(t.TempDir(), "recovery")
	resp, err := app.SwitchConnection(connectionSwitchRequest{
		PgwireEndpoint: addrTwo,
		AdminEndpoints: []string{"127.0.0.1:9090", "127.0.0.1:9091"},
		DataDir:        recoveryDir,
	})
	if err != nil {
		t.Fatalf("SwitchConnection: %v", err)
	}
	if got := resp["status"]; got != "ok" {
		t.Fatalf("unexpected switch status: %v", got)
	}

	connection, ok := resp["connection"].(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected switch payload: %+v", resp)
	}
	if got := connection["pgwire_endpoint"]; got != addrTwo {
		t.Fatalf("unexpected switched endpoint: got %v want %q", got, addrTwo)
	}
	if got := connection["data_dir"]; got != recoveryDir {
		t.Fatalf("unexpected switched data dir: got %v want %q", got, recoveryDir)
	}

	app.peersMu.RLock()
	defer app.peersMu.RUnlock()
	if app.engine == nil || app.engine.addr != addrTwo {
		t.Fatalf("expected active engine to point at %q, got %+v", addrTwo, app.engine)
	}
	if app.engine.password != "studio-pass" {
		t.Fatalf("expected pgwire token to be reused, got %q", app.engine.password)
	}
	if len(app.adminEndpoints) != 2 {
		t.Fatalf("expected updated admin endpoints, got %+v", app.adminEndpoints)
	}
	if app.dataDir != recoveryDir {
		t.Fatalf("expected updated data dir, got %q", app.dataDir)
	}
}

func TestTransactionCallsStayOnLeaderClient(t *testing.T) {
	followerAddr := startStudioPGWireServer(t, "studio-pass")
	leaderAddr := startStudioPGWireServer(t, "studio-pass")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	follower := newEngineClient(followerAddr, "studio-pass")
	leader := newEngineClient(leaderAddr, "studio-pass")
	app := newApp(
		follower,
		followerAddr,
		nil,
		"",
		[]*engineClient{follower, leader},
		[]string{followerAddr, leaderAddr},
		nil,
		nil,
		"admin-secret",
		filepath.Join(t.TempDir(), "data"),
		logger,
	)
	app.leaderClient = leader
	t.Cleanup(func() {
		closeEngineClients(follower, leader)
	})

	beginResp, err := app.Begin(beginRequest{Domains: []string{"default"}})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	txID, _ := beginResp["tx_id"].(string)
	if txID == "" {
		t.Fatalf("expected tx_id in begin response, got %+v", beginResp)
	}
	if got := app.lookupTxClient(txID); got != leader {
		t.Fatalf("expected tx client to be pinned to leader, got %+v", got)
	}

	execResp, err := app.Execute(executeRequest{TxID: txID, SQL: "SHOW asql_node_role"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rows, _ := execResp["rows"].([]map[string]interface{})
	if len(rows) == 0 {
		// structToMap converts slices through reflection; tolerate []interface{} too.
		genericRows, ok := execResp["rows"].([]interface{})
		if !ok || len(genericRows) == 0 {
			t.Fatalf("expected query rows, got %+v", execResp)
		}
	}

	if _, err := app.Commit(txRequest{TxID: txID}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := app.lookupTxClient(txID); got != nil {
		t.Fatalf("expected tx client to be cleared after commit, got %+v", got)
	}
}

func TestLeaderWriteInvokerUsesLeaderClient(t *testing.T) {
	followerAddr := startStudioPGWireServer(t, "studio-pass")
	leaderAddr := startStudioPGWireServer(t, "studio-pass")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	follower := newEngineClient(followerAddr, "studio-pass")
	leader := newEngineClient(leaderAddr, "studio-pass")
	app := newApp(
		follower,
		followerAddr,
		nil,
		"",
		[]*engineClient{follower, leader},
		[]string{followerAddr, leaderAddr},
		nil,
		nil,
		"admin-secret",
		filepath.Join(t.TempDir(), "data"),
		logger,
	)
	app.leaderClient = leader
	t.Cleanup(func() {
		closeEngineClients(follower, leader)
	})

	invoker := app.newLeaderWriteInvoker()
	beginResp, err := invoker.BeginTx(context.Background(), &api.BeginTxRequest{Mode: "domain", Domains: []string{"default"}})
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if beginResp.TxID == "" {
		t.Fatalf("expected tx id from invoker begin")
	}
	if _, err := invoker.Execute(context.Background(), &api.ExecuteRequest{TxID: beginResp.TxID, SQL: "SHOW asql_node_role"}); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := invoker.CommitTx(context.Background(), &api.CommitTxRequest{TxID: beginResp.TxID}); err != nil {
		t.Fatalf("CommitTx: %v", err)
	}
	if _, err := invoker.Execute(context.Background(), &api.ExecuteRequest{TxID: beginResp.TxID, SQL: "SHOW asql_node_role"}); err == nil {
		t.Fatal("expected tx to be cleared after commit")
	}
}

func TestTxLeaderChangeErrorExplainsRestart(t *testing.T) {
	err := txLeaderChangeError("tx-123", &pgconn.PgError{
		Code:    "25006",
		Message: "not the leader: redirect writes to 127.0.0.1:5433",
		Hint:    "asql_leader=127.0.0.1:5433",
	})
	if err == nil {
		t.Fatal("expected rewritten transaction error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "start a new transaction") {
		t.Fatalf("expected restart guidance, got %q", msg)
	}
	if !strings.Contains(msg, "127.0.0.1:5433") {
		t.Fatalf("expected leader address in message, got %q", msg)
	}
	if !strings.Contains(msg, "tx-123") {
		t.Fatalf("expected tx id in message, got %q", msg)
	}
}

func startStudioPGWireServer(t *testing.T, authToken string) string {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := pgwireserver.New(pgwireserver.Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      logger,
		AuthToken:   authToken,
	})
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen pgwire test server: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeOnListener(ctx, listener)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("pgwire server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire test server shutdown")
		}
	})

	return listener.Addr().String()
}
