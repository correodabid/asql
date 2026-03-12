package pgwire

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func BenchmarkPGWireWriteCommit(b *testing.B) {
	ctx := context.Background()
	conn, cleanup := newBenchmarkPGWireConn(b, ctx)
	defer cleanup()

	mustExecPGWireBenchmark(b, ctx, conn, "BEGIN DOMAIN bench")
	mustExecPGWireBenchmark(b, ctx, conn, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
	mustExecPGWireBenchmark(b, ctx, conn, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExecPGWireBenchmark(b, ctx, conn, "BEGIN DOMAIN bench")
		mustExecPGWireBenchmark(b, ctx, conn, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", i))
		mustExecPGWireBenchmark(b, ctx, conn, "COMMIT")
	}
}

func BenchmarkPGWireWriteCommitReturningUUID(b *testing.B) {
	ctx := context.Background()
	conn, cleanup := newBenchmarkPGWireConn(b, ctx)
	defer cleanup()

	mustExecPGWireBenchmark(b, ctx, conn, "BEGIN DOMAIN bench")
	mustExecPGWireBenchmark(b, ctx, conn, "CREATE TABLE entries (id TEXT PRIMARY KEY DEFAULT UUID_V7, payload TEXT)")
	mustExecPGWireBenchmark(b, ctx, conn, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExecPGWireBenchmark(b, ctx, conn, "BEGIN DOMAIN bench")
		var id string
		if err := conn.QueryRow(ctx, fmt.Sprintf("INSERT INTO entries (payload) VALUES ('payload-%d') RETURNING id", i)).Scan(&id); err != nil {
			b.Fatalf("insert returning id: %v", err)
		}
		if id == "" {
			b.Fatal("expected returned id")
		}
		mustExecPGWireBenchmark(b, ctx, conn, "COMMIT")
	}
}

func BenchmarkPGWireWriteCommitBulk10(b *testing.B) {
	ctx := context.Background()
	conn, cleanup := newBenchmarkPGWireConn(b, ctx)
	defer cleanup()

	mustExecPGWireBenchmark(b, ctx, conn, "BEGIN DOMAIN bench")
	mustExecPGWireBenchmark(b, ctx, conn, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
	mustExecPGWireBenchmark(b, ctx, conn, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExecPGWireBenchmark(b, ctx, conn, "BEGIN DOMAIN bench")
		for j := 0; j < 10; j++ {
			mustExecPGWireBenchmark(b, ctx, conn, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", i*10+j))
		}
		mustExecPGWireBenchmark(b, ctx, conn, "COMMIT")
	}
}

func newBenchmarkPGWireConn(b *testing.B, ctx context.Context) (*pgx.Conn, func()) {
	b.Helper()

	dataDir := filepath.Join(b.TempDir(), "pgwire-bench-data")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: dataDir,
		Logger:      logger,
	})
	if err != nil {
		b.Fatalf("new pgwire server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		server.Stop()
		b.Fatalf("listen: %v", err)
	}

	serveCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeOnListener(serveCtx, listener)
	}()

	connStr := "postgres://asql@" + listener.Addr().String() + "/asql?sslmode=disable&default_query_exec_mode=simple_protocol"
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		cancel()
		_ = listener.Close()
		server.Stop()
		b.Fatalf("connect pgx: %v", err)
	}

	cleanup := func() {
		_ = conn.Close(ctx)
		cancel()
		_ = listener.Close()
		server.Stop()
		select {
		case err := <-errCh:
			if err != nil {
				b.Fatalf("pgwire server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			b.Fatal("timeout waiting for pgwire server shutdown")
		}
	}

	return conn, cleanup
}

func mustExecPGWireBenchmark(b *testing.B, ctx context.Context, conn *pgx.Conn, sql string) {
	b.Helper()
	if _, err := conn.Exec(ctx, sql); err != nil {
		b.Fatalf("exec %q: %v", sql, err)
	}
}