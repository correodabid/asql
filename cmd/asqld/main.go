package main

import (
	"bufio"
	"context"
	"flag"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	pgwireserver "github.com/correodabid/asql/internal/server/pgwire"
)

func main() {
	// Reduce GC frequency: the engine holds many live objects (row maps,
	// overlays, snapshots). Default GOGC=100 causes the GC to fire too
	// often and spend up to 30% of CPU scanning pointers. GOGC=400 lets
	// the heap grow 4x before triggering collection, trading memory for
	// throughput. Can be overridden via GOGC env var.
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(400)
	}

	addr := flag.String("addr", ":5433", "pgwire listen address")
	dataDir := flag.String("data-dir", ".asql", "data directory path")
	authToken := flag.String("auth-token", "", "optional pgwire password and cluster/admin bearer token")
	adminReadToken := flag.String("admin-read-token", "", "optional bearer token for read-only admin API endpoints; falls back to -auth-token")
	adminWriteToken := flag.String("admin-write-token", "", "optional bearer token for mutating admin API endpoints; falls back to -auth-token")
	nodeID := flag.String("node-id", "", "unique node identifier for cluster mode (e.g. node-a)")
	peers := flag.String("peers", "", "comma-separated peer list in nodeID@host:port format (cluster gRPC ports)")
	groups := flag.String("groups", "", "comma-separated domain groups for heartbeat monitoring")
	grpcAddr := flag.String("grpc-addr", "", "cluster gRPC listen address (e.g. :6433); enables cluster mode when set with -node-id and -peers")
	joinAddr := flag.String("join", "", "gRPC address of an existing cluster peer to join at startup (hot join); omit -peers when using -join")
	probeTimeout := flag.Duration("probe-timeout", 0, "per-RPC timeout for heartbeat probes (0 = default 1s)")
	adminAddr := flag.String("admin-addr", "", "admin HTTP listen address for /metrics, /readyz, /livez (e.g. :9090)")
	pprofAddr := flag.String("pprof-addr", "", "pprof HTTP listen address (e.g. :6060)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Buffered slog output: wrap stdout in a bufio.Writer and flush
	// periodically. Unbuffered slog was spending ~14% CPU on syscall.Write
	// due to one write(2) per log line. A 64 KiB buffer batches writes and
	// a 250 ms flush ticker keeps latency bounded.
	bufOut := bufio.NewWriterSize(os.Stdout, 64*1024)
	logFlushDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = bufOut.Flush()
			case <-logFlushDone:
				_ = bufOut.Flush()
				return
			}
		}
	}()
	logger = slog.New(slog.NewTextHandler(bufOut, nil))
	logger.Info("asqld starting", "addr", *addr, "data_dir", *dataDir)

	// Optional pprof server for profiling.
	if *pprofAddr != "" {
		go func() {
			logger.Info("pprof server starting", "addr", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				logger.Error("pprof server failed", slog.String("error", err.Error()))
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config := pgwireserver.Config{
		Address:          *addr,
		AdminHTTPAddr:    *adminAddr,
		DataDirPath:      *dataDir,
		Logger:           logger,
		AuthToken:        *authToken,
		AdminReadToken:   *adminReadToken,
		AdminWriteToken:  *adminWriteToken,
		NodeID:           *nodeID,
		ClusterGRPCAddr:  *grpcAddr,
		PeerProbeTimeout: *probeTimeout,
		JoinAddr:         *joinAddr,
	}

	if *peers != "" {
		config.Peers = splitCSV(*peers)
	}
	if *groups != "" {
		config.Groups = splitCSV(*groups)
	}

	server, err := pgwireserver.New(config)
	if err != nil {
		logger.Error("failed to initialize server", slog.String("error", err.Error()))
		close(logFlushDone)
		os.Exit(1)
	}

	if err := server.Run(ctx); err != nil {
		logger.Error("server terminated with error", slog.String("error", err.Error()))
		close(logFlushDone)
		os.Exit(1)
	}
	close(logFlushDone)
}

// splitCSV splits a comma-separated string and trims whitespace.
func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
