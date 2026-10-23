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
	"crypto/ed25519"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/storage/ledgerdigest"
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
	// SignedLedger starts the server in ledger mode signed (ADR 0005, 0008)
	// with a fresh signing key and state key, so ledger_digest,
	// prove_consistency, prove_rows and attest_query answer. Server holds
	// the public key to verify what it signs. Digests are signed every
	// second as in production; Server.SignLedgerDigest signs one at once.
	SignedLedger bool
	// NodeID names the node in what it signs (node_id). Empty leaves it
	// empty.
	NodeID string
}

// Server is a running ASQL pgwire server bound to a local port.
type Server struct {
	// Addr is the listener address ("host:port") accepting pgwire connections.
	Addr string
	// DSN is a pgx/libpq connection string for the server.
	DSN string
	// With Options.SignedLedger: the public key that verifies the server's
	// digests and attestations, its key_id, and the PEM file that holds it,
	// which asqlctl takes as -pubkey.
	LedgerPublicKey     ed25519.PublicKey
	LedgerKeyID         string
	LedgerPublicKeyFile string

	srv *embed.Server
}

// SignLedgerDigest signs the committed head now, without waiting for the
// digest cadence. When it returns, the latest digest covers the head; it
// reports false when one already did (the cadence may have got there
// first). Committing, then signing, gives a digest at a known LSN; doing
// it twice gives two digests to prove consistency between. It fails the
// test without Options.SignedLedger.
func (s *Server) SignLedgerDigest(t testing.TB) bool {
	t.Helper()
	signed, err := s.srv.SignLedgerDigest(context.Background())
	if err != nil {
		t.Fatalf("servertest: sign ledger digest: %v", err)
	}
	return signed
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
	embedOpts := embed.Options{
		DataDir:   dataDir,
		AuthToken: opts.AuthToken,
		Logger:    opts.Logger,
		NodeID:    opts.NodeID,
	}
	var pub ed25519.PublicKey
	var pubPath string
	if opts.SignedLedger {
		// The keys live outside the data directory, as in production, and
		// are made as asqlctl ledger-keygen makes them.
		keyDir := t.TempDir()
		keyPath := filepath.Join(keyDir, "ledger.key")
		pubPath = filepath.Join(keyDir, "ledger.pub")
		var err error
		if pub, err = ledgerdigest.GenerateKeyFiles(keyPath, pubPath); err != nil {
			t.Fatalf("servertest: ledger key: %v", err)
		}
		if err := ledgerdigest.GenerateStateKeyFile(ledgerdigest.DefaultStateKeyPath(keyPath)); err != nil {
			t.Fatalf("servertest: ledger state key: %v", err)
		}
		embedOpts.LedgerMode = "signed"
		embedOpts.LedgerKeyFile = keyPath
	}
	srv, err := embed.Start(context.Background(), embedOpts)
	if err != nil {
		t.Fatalf("servertest: %v", err)
	}
	t.Cleanup(func() {
		if err := srv.Close(); err != nil {
			t.Errorf("servertest: %v", err)
		}
	})
	server := &Server{Addr: srv.Addr(), DSN: srv.DSN(), srv: srv}
	if pub != nil {
		server.LedgerPublicKey, server.LedgerKeyID, server.LedgerPublicKeyFile = pub, ledgerdigest.KeyID(pub), pubPath
	}
	return server
}
