package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	auditstore "github.com/correodabid/asql/internal/storage/audit"
	"github.com/correodabid/asql/internal/storage/wal"
)

// newAuditedBenchmarkEngine wires an audit store, as the servers do, so row
// history is written on every commit.
func newAuditedBenchmarkEngine(b *testing.B) (*Engine, func()) {
	b.Helper()
	dir := b.TempDir()
	store, err := wal.NewSegmentedLogStore(filepath.Join(dir, "bench.wal"), wal.AlwaysSync{}, wal.WithLedgerMode(benchmarkLedgerMode(b)))
	if err != nil {
		b.Fatal(err)
	}
	auditPath := filepath.Join(dir, "audit", "audit")
	if err := os.MkdirAll(filepath.Dir(auditPath), 0o755); err != nil {
		b.Fatal(err)
	}
	audit, err := auditstore.New(auditPath)
	if err != nil {
		b.Fatal(err)
	}
	engine, err := New(context.Background(), store, "", WithAuditStore(audit))
	if err != nil {
		b.Fatal(err)
	}
	return engine, func() {
		// Shutdown waits for background snapshots; keep it out of the timing.
		b.StopTimer()
		engine.WaitPendingSnapshots()
		_ = audit.Close()
		_ = store.Close()
	}
}

func seedAuditedBenchmark(b *testing.B, ctx context.Context, engine *Engine, rows int) {
	b.Helper()
	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT PRIMARY KEY, qty INT, payload TEXT)")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")
	for i := 0; i < rows; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("INSERT INTO entries (id, qty, payload) VALUES (%d, 0, 'seed')", i))
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}
}

// BenchmarkEngineWriteCommitAudited: one INSERT per commit with row history.
func BenchmarkEngineWriteCommitAudited(b *testing.B) {
	ctx := context.Background()
	engine, closeAll := newAuditedBenchmarkEngine(b)
	defer closeAll()
	seedAuditedBenchmark(b, ctx, engine, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("INSERT INTO entries (id, qty, payload) VALUES (%d, 0, 'payload')", i))
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}
}

// BenchmarkEngineUpdateCommitAudited: one UPDATE per commit over 1,000 rows
// with row history.
func BenchmarkEngineUpdateCommitAudited(b *testing.B) {
	ctx := context.Background()
	engine, closeAll := newAuditedBenchmarkEngine(b)
	defer closeAll()
	seedAuditedBenchmark(b, ctx, engine, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("UPDATE entries SET qty = %d WHERE id = %d", i, i%1000))
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}
}

// BenchmarkRowHistoryAudited: FOR HISTORY of one row in a store holding
// 2,000 commits.
func BenchmarkRowHistoryAudited(b *testing.B) {
	ctx := context.Background()
	engine, closeAll := newAuditedBenchmarkEngine(b)
	defer closeAll()
	seedAuditedBenchmark(b, ctx, engine, 1000)
	for i := 0; i < 1000; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("UPDATE entries SET qty = %d WHERE id = %d", i, i%10))
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := engine.RowHistory(ctx, "SELECT * FROM entries FOR HISTORY WHERE id = 3", []string{"bench"})
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Rows) == 0 {
			b.Fatal("empty history")
		}
	}
}
