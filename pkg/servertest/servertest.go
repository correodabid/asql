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
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/pkg/embed"
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
	// DSN is a pgx/libpq connection string for the server.
	DSN string
}

// StartForTesting launches a single-node ASQL server bound to a random local
// port. The caller must pass a *testing.T so the server is shut down and its
// data directory removed via t.Cleanup. It is pkg/embed with a temporary
// data directory.
func StartForTesting(t *testing.T, opts Options) *Server {
	t.Helper()

	dataDir := opts.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(t.TempDir(), "data")
	}
	srv, err := embed.Start(context.Background(), embed.Options{
		DataDir:   dataDir,
		AuthToken: opts.AuthToken,
		Logger:    opts.Logger,
	})
	if err != nil {
		t.Fatalf("servertest: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("servertest: %v", err)
		}
	})
	return &Server{Addr: srv.Addr(), DSN: srv.DSN()}
}
