// Package embed runs an ASQL server inside the calling process.
//
// The server is the same one asqld runs: clients reach it over pgwire, with
// the same SQL, transactions and errors, so code written against a separate
// asqld works unchanged. Use it for single-process deployments, and in tests
// through pkg/servertest, which wraps it with a temporary data directory.
//
//	srv, err := embed.Start(ctx, embed.Options{DataDir: "/var/lib/myapp/asql"})
//	if err != nil { ... }
//	defer srv.Close()
//	conn, err := pgx.Connect(ctx, srv.DSN())
package embed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"time"

	pgwireserver "github.com/correodabid/asql/internal/server/pgwire"
	"github.com/correodabid/asql/internal/storage/wal"
)

// ErrDataDirInUse is returned by Start when another server, embedded or
// not, in this process or another, already has DataDir open. Two servers on
// one directory would write diverging histories without an error.
var ErrDataDirInUse = wal.ErrDataDirInUse

// Options configures an embedded server.
type Options struct {
	// DataDir is the data directory. It is created if missing, and reopening
	// it later recovers the same database. Required.
	DataDir string
	// Address is the pgwire listen address. Empty means a random port on
	// 127.0.0.1, which keeps the server private to this machine.
	Address string
	// AuthToken, if set, is the password every pgwire connection must give.
	AuthToken string
	// LedgerMode is the WAL ledger mode: "chain" (the default when empty),
	// "signed" (needs LedgerKeyFile) or "off". See ADR 0005.
	LedgerMode string
	// LedgerKeyFile is the Ed25519 private key for LedgerMode "signed".
	LedgerKeyFile string
	// LedgerStateKeyFile is the state key for LedgerMode "signed" (ADR 0008).
	// Empty means LedgerKeyFile + ".state", which may be missing.
	LedgerStateKeyFile string
	// LedgerDigestInterval and LedgerDigestEveryLSNs set how often
	// LedgerMode "signed" signs the committed head: at most every interval,
	// or sooner once that many LSNs were committed. Zero means the defaults
	// (1s and 10,000). Server.SignLedgerDigest signs one at once.
	LedgerDigestInterval  time.Duration
	LedgerDigestEveryLSNs uint64
	// NodeID names the node in the digests and attestations it signs
	// (node_id). Empty leaves it empty; the signing key's key_id still
	// tells nodes apart. It does not enable cluster mode: the server logs
	// that it runs standalone, and lists itself in cluster_members.
	NodeID string
	// Compliance refuses to start unless the ledger vouches for the whole WAL.
	Compliance bool
	// Logger receives the server's logs. Nil discards them.
	Logger *slog.Logger
}

// ShutdownTimeout bounds how long Close waits for the server to finish
// serving after its listener is closed.
const ShutdownTimeout = 10 * time.Second

// Server is a running embedded server.
type Server struct {
	server    *pgwireserver.Server
	addr      string
	authToken string
	cancel    context.CancelFunc
	served    chan error
	closeOnce sync.Once
	closeErr  error
}

// Start opens the data directory, replays it and starts serving pgwire. It
// returns once the server accepts connections.
func Start(ctx context.Context, opts Options) (*Server, error) {
	if opts.DataDir == "" {
		return nil, errors.New("embed: DataDir is required")
	}
	address := opts.Address
	if address == "" {
		address = "127.0.0.1:0"
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("embed: listen %s: %w", address, err)
	}
	server, err := pgwireserver.New(pgwireserver.Config{
		Address:               listener.Addr().String(),
		DataDirPath:           opts.DataDir,
		Logger:                logger,
		AuthToken:             opts.AuthToken,
		LedgerMode:            opts.LedgerMode,
		LedgerKeyFile:         opts.LedgerKeyFile,
		LedgerStateKeyFile:    opts.LedgerStateKeyFile,
		LedgerDigestInterval:  opts.LedgerDigestInterval,
		LedgerDigestEveryLSNs: opts.LedgerDigestEveryLSNs,
		NodeID:                opts.NodeID,
		Compliance:            opts.Compliance,
	})
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("embed: %w", err)
	}

	serveCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &Server{
		server:    server,
		addr:      listener.Addr().String(),
		authToken: opts.AuthToken,
		cancel:    cancel,
		served:    make(chan error, 1),
	}
	go func() { s.served <- server.ServeOnListener(serveCtx, listener) }()
	return s, nil
}

// ErrLedgerNotSigning reports SignLedgerDigest on a server whose
// LedgerMode is not "signed".
var ErrLedgerNotSigning = pgwireserver.ErrLedgerNotSigning

// SignLedgerDigest signs the committed head now, without waiting for the
// digest cadence. When it returns, the latest digest covers the head; it
// reports false when one already did (the cadence may have got there
// first). A commit is in the head once its COMMIT returns, so a test can
// commit, sign, and read the digest from asql_admin.ledger_digest at a
// known LSN.
func (s *Server) SignLedgerDigest(ctx context.Context) (bool, error) {
	return s.server.SignLedgerDigest(ctx)
}

// Addr is the "host:port" the server listens on.
func (s *Server) Addr() string { return s.addr }

// DSN is a pgx/libpq connection string for the server.
func (s *Server) DSN() string {
	user := url.User("asql")
	if s.authToken != "" {
		user = url.UserPassword("asql", s.authToken)
	}
	return (&url.URL{Scheme: "postgres", User: user, Host: s.addr, Path: "/asql", RawQuery: "sslmode=disable"}).String()
}

// Close stops serving and closes the data directory, after writing a final
// checkpoint so the next Start replays little. Open client connections are
// closed. It is safe to call more than once.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.server.Stop()
		select {
		case err := <-s.served:
			if err != nil && !errors.Is(err, context.Canceled) {
				s.closeErr = fmt.Errorf("embed: serve: %w", err)
			}
		case <-time.After(ShutdownTimeout):
			s.closeErr = errors.New("embed: timed out waiting for the server to stop")
		}
	})
	return s.closeErr
}
