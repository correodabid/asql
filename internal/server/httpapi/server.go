package httpapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/engine/executor"
	"asql/internal/platform/clock"
	"asql/internal/platform/datadir"
	"asql/internal/storage/wal"
)

const defaultLeadershipLeaseTTL = 5 * time.Second

// Config holds all parameters needed to start the HTTP API server.
type Config struct {
	Address         string
	DataDirPath     string // path to .asql/ data directory
	Logger          *slog.Logger
	AuthToken       string
	TLSCertPath     string
	TLSKeyPath      string
	TLSClientCAPath string

	// Cluster fields — optional.
	NodeID string
	Peers  []string
	Groups []string
}

// Validate ensures required fields are set.
func (c Config) Validate() error {
	if c.Address == "" {
		return errors.New("address is required")
	}
	if c.DataDirPath == "" {
		return errors.New("data directory path is required")
	}
	if c.Logger == nil {
		return errors.New("logger is required")
	}

	mtlsInputs := 0
	if c.TLSCertPath != "" {
		mtlsInputs++
	}
	if c.TLSKeyPath != "" {
		mtlsInputs++
	}
	if c.TLSClientCAPath != "" {
		mtlsInputs++
	}
	if mtlsInputs > 0 && mtlsInputs < 3 {
		return errors.New("mtls requires tls cert path, tls key path, and tls client ca path")
	}

	return nil
}

// Server is the HTTP API server wrapping the ASQL engine.
type Server struct {
	config     Config
	httpServer *http.Server
	walStore   *wal.SegmentedLogStore
	engine     *executor.Engine
	svc        *service
}

// New creates a new HTTP API server with WAL, engine, and all routes registered.
func New(config Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid httpapi server config: %w", err)
	}

	dd, err := datadir.New(config.DataDirPath)
	if err != nil {
		return nil, fmt.Errorf("initialize data directory: %w", err)
	}

	walStore, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		return nil, fmt.Errorf("initialize wal store: %w", err)
	}

	engine, err := executor.New(context.Background(), walStore, dd.SnapDir())
	if err != nil {
		_ = walStore.Close()
		return nil, fmt.Errorf("initialize executor engine: %w", err)
	}

	leadershipManager, err := coordinator.NewLeadershipManager(clock.Realtime{}, defaultLeadershipLeaseTTL)
	if err != nil {
		_ = walStore.Close()
		return nil, fmt.Errorf("initialize leadership manager: %w", err)
	}

	svc := newService(engine, walStore, config.Logger, leadershipManager, config.AuthToken)

	mux := http.NewServeMux()
	svc.RegisterRoutes(mux)

	tlsConfig, err := loadServerTLSConfig(config)
	if err != nil {
		_ = walStore.Close()
		return nil, fmt.Errorf("initialize tls config: %w", err)
	}

	httpServer := &http.Server{
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute, // large for batch/time-travel queries
		IdleTimeout:  2 * time.Minute,
		TLSConfig:    tlsConfig,
	}

	return &Server{
		config:     config,
		httpServer: httpServer,
		walStore:   walStore,
		engine:     engine,
		svc:        svc,
	}, nil
}

// Run starts the HTTP server and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.config.Address, err)
	}

	s.config.Logger.Info("http api server listening", "address", s.config.Address)

	errCh := make(chan error, 1)
	go func() {
		if s.httpServer.TLSConfig != nil {
			errCh <- s.httpServer.ServeTLS(listener, "", "")
		} else {
			errCh <- s.httpServer.Serve(listener)
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutdownCtx)
		s.cleanup()
		return nil
	case serveErr := <-errCh:
		s.cleanup()
		if serveErr == nil || errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("http serve failed: %w", serveErr)
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	if s.httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutdownCtx)
	}
	s.cleanup()
}

// Engine returns the executor.Engine owned by this server.
// It can be passed to pgwire.Config.Engine to share the same engine instance.
func (s *Server) Engine() *executor.Engine {
	return s.engine
}

// WALStore returns the wal.SegmentedLogStore owned by this server.
// It can be passed to pgwire.Config.WALStore alongside Engine().
func (s *Server) WALStore() *wal.SegmentedLogStore {
	return s.walStore
}

func (s *Server) cleanup() {
	if s.svc != nil {
		close(s.svc.cleanupClose)
		s.svc.stopAuditWriter()
	}
	if s.engine != nil {
		s.engine.WaitPendingSnapshots()
	}
	if s.walStore != nil {
		_ = s.walStore.Close()
	}
}

// loadServerTLSConfig creates a TLS config from the provided cert/key/ca paths.
func loadServerTLSConfig(config Config) (*tls.Config, error) {
	if config.TLSCertPath == "" && config.TLSKeyPath == "" && config.TLSClientCAPath == "" {
		return nil, nil
	}

	certificate, err := tls.LoadX509KeyPair(config.TLSCertPath, config.TLSKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}

	caPEM, err := os.ReadFile(config.TLSClientCAPath)
	if err != nil {
		return nil, fmt.Errorf("read client ca cert: %w", err)
	}

	clientCAPool := x509.NewCertPool()
	if !clientCAPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse client ca cert: invalid PEM")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    clientCAPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}, nil
}
