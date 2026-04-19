// Package servertest provides ephemeral ASQL server fixtures for tests.
//
// External clients and tooling that drive a running ASQL instance can use
// StartForTesting to obtain a single-node server bound to a random port,
// with a throw-away data directory that is cleaned up automatically when
// the test completes. Every call returns an independent server, so tests
// can spin up multiple nodes for routing/failover scenarios.
package servertest

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	pgwireserver "github.com/correodabid/asql/internal/server/pgwire"
)

// Options configures a test server.
type Options struct {
	// AuthToken, if non-empty, is required by every pgwire connection.
	AuthToken string
	// DataDir overrides the auto-created data directory under t.TempDir().
	DataDir string
	// Logger overrides the default no-op logger.
	Logger *slog.Logger
}

// Server is a running ASQL pgwire server bound to a local port.
type Server struct {
	// Addr is the listener address ("host:port") accepting pgwire connections.
	Addr string
}

// StartForTesting launches a single-node ASQL server bound to a random local
// port. The caller must pass a *testing.T so the server is shut down and its
// data directory removed via t.Cleanup.
func StartForTesting(t *testing.T, opts Options) *Server {
	t.Helper()

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	dataDir := opts.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(t.TempDir(), "data")
	}

	ctx, cancel := context.WithCancel(context.Background())
	server, err := pgwireserver.New(pgwireserver.Config{
		Address:     "127.0.0.1:0",
		DataDirPath: dataDir,
		Logger:      logger,
		AuthToken:   opts.AuthToken,
	})
	if err != nil {
		cancel()
		t.Fatalf("servertest: new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("servertest: listen: %v", err)
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
				t.Fatalf("servertest: server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("servertest: timeout waiting for server shutdown")
		}
	})

	return &Server{Addr: listener.Addr().String()}
}
