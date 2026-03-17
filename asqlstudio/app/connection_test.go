package studioapp

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	pgwireserver "asql/internal/server/pgwire"
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
