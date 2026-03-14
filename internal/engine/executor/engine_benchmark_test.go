package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"asql/internal/storage/wal"
)

func BenchmarkEngineWriteCommit(b *testing.B) {
	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT, payload TEXT)")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", i))
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

// BenchmarkEngineWriteCommitPreSeeded inserts single rows into a table that
// already has 10K rows. This isolates the per-INSERT commit cost at a
// realistic table size, revealing O(N) vs O(1) scaling behavior.
func BenchmarkEngineWriteCommitPreSeeded(b *testing.B) {
	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
	for i := 0; i < 10000; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'seed')", i))
	}
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", 10000+i))
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

// BenchmarkEngineWriteScaling tests INSERT cost at different pre-seed sizes
// to detect O(N) scaling in the commit path.
func BenchmarkEngineWriteScaling(b *testing.B) {
	for _, size := range []int{10_000, 50_000, 100_000, 200_000} {
		b.Run(fmt.Sprintf("rows_%dk", size/1000), func(b *testing.B) {
			ctx := context.Background()
			store, engine := newBenchmarkEngine(b)

			session := engine.NewSession()
			mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
			mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
			for i := 0; i < size; i++ {
				mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'seed')", i))
			}
			mustExecBenchmark(b, ctx, engine, session, "COMMIT")

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx := engine.NewSession()
				mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
				mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", size+i))
				mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
			}

			engine.WaitPendingSnapshots()
			_ = store.Close()
		})
	}
}

func BenchmarkEngineWriteCommitAlwaysSync(b *testing.B) {
	ctx := context.Background()

	path := filepath.Join(b.TempDir(), "bench-sync.wal")
	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("new file log store: %v", err)
	}
	engine, err := New(ctx, store, "")
	if err != nil {
		_ = store.Close()
		b.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT, payload TEXT)")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", i))
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineWriteCommitBulk10(b *testing.B) {
	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT, payload TEXT)")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		for j := 0; j < 10; j++ {
			mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", i*10+j))
		}
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineWriteCommitReturningUUID(b *testing.B) {
	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id TEXT PRIMARY KEY DEFAULT UUID_V7, payload TEXT)")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		result, err := engine.Execute(ctx, tx, fmt.Sprintf("INSERT INTO entries (payload) VALUES ('payload-%d') RETURNING id", i))
		if err != nil {
			b.Fatalf("insert returning id: %v", err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["id"].StringValue == "" {
			b.Fatalf("unexpected RETURNING rows: %+v", result.Rows)
		}
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineWriteCommitBulk10ReturningUUID(b *testing.B) {
	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id TEXT PRIMARY KEY DEFAULT UUID_V7, payload TEXT)")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		for j := 0; j < 10; j++ {
			result, err := engine.Execute(ctx, tx, fmt.Sprintf("INSERT INTO entries (payload) VALUES ('payload-%d-%d') RETURNING id", i, j))
			if err != nil {
				b.Fatalf("insert returning id: %v", err)
			}
			if len(result.Rows) != 1 || result.Rows[0]["id"].StringValue == "" {
				b.Fatalf("unexpected RETURNING rows: %+v", result.Rows)
			}
		}
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReadAsOfLSN(b *testing.B) {
	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	seed := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, seed, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, seed, "CREATE TABLE entries (id INT, payload TEXT)")
	for i := 0; i < 1000; i++ {
		mustExecBenchmark(b, ctx, engine, seed, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", i))
	}
	mustExecBenchmark(b, ctx, engine, seed, "COMMIT")

	targetLSN := store.LastLSN()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, payload FROM entries", []string{"bench"}, targetLSN)
		if err != nil {
			b.Fatalf("time travel query: %v", err)
		}
		if len(result.Rows) != 1000 {
			b.Fatalf("unexpected row count: got %d want 1000", len(result.Rows))
		}
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReplayToLSN(b *testing.B) {
	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	seed := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, seed, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, seed, "CREATE TABLE entries (id INT, payload TEXT)")
	for i := 0; i < 1000; i++ {
		mustExecBenchmark(b, ctx, engine, seed, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", i))
	}
	mustExecBenchmark(b, ctx, engine, seed, "COMMIT")

	targetLSN := store.LastLSN()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := engine.ReplayToLSN(ctx, targetLSN); err != nil {
			b.Fatalf("replay to lsn: %v", err)
		}
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineRestartReplayOnly(b *testing.B) {
	benchmarkEngineRestartLoad(b, false)
}

func BenchmarkEngineRestartFromPersistedSnapshot(b *testing.B) {
	benchmarkEngineRestartLoad(b, true)
}

func BenchmarkEngineReadPersistedSnapshotsFromDir(b *testing.B) {
	_, snapDir, expectedHeadLSN := prepareRestartBenchmarkFixture(b, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshots, maxSeq, err := readAllSnapshotsFromDir(snapDir)
		if err != nil {
			b.Fatalf("read persisted snapshots from dir: %v", err)
		}
		if maxSeq == 0 {
			b.Fatal("expected persisted snapshot sequence")
		}
		if len(snapshots) == 0 {
			b.Fatal("expected persisted snapshots")
		}
		if snapshots[len(snapshots)-1].lsn != expectedHeadLSN {
			b.Fatalf("unexpected latest snapshot lsn: got %d want %d", snapshots[len(snapshots)-1].lsn, expectedHeadLSN)
		}
	}
	b.StopTimer()
}

func BenchmarkEngineReplayFromPersistedSnapshots(b *testing.B) {
	ctx := context.Background()
	walPath, snapDir, expectedHeadLSN := prepareRestartBenchmarkFixture(b, true)

	fixtureStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("reopen snapshot benchmark wal store: %v", err)
	}
	defer fixtureStore.Close()

	snapshots, _, err := readAllSnapshotsFromDir(snapDir)
	if err != nil {
		b.Fatalf("read persisted snapshots for replay benchmark: %v", err)
	}
	if len(snapshots) == 0 {
		b.Fatal("expected persisted snapshots for replay benchmark")
	}

	latest := snapshots[len(snapshots)-1]
	deltaRecords, err := fixtureStore.ReadFrom(ctx, latest.lsn+1, 0)
	if err != nil {
		b.Fatalf("read wal delta after persisted snapshots: %v", err)
	}

	store, engine := newBenchmarkEngine(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := engine.replayFromSnapshots(snapshots, deltaRecords); err != nil {
			b.Fatalf("replay from persisted snapshots: %v", err)
		}
		if engine.readState.Load().headLSN != expectedHeadLSN {
			b.Fatalf("unexpected replayed head lsn: got %d want %d", engine.readState.Load().headLSN, expectedHeadLSN)
		}
	}
	b.StopTimer()

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReadIndexedRangeBTree(b *testing.B) {
	ctx := context.Background()
	store, engine, targetLSN := prepareIndexedReadBenchmarkFixture(b)

	query := "SELECT id, payload FROM entries WHERE id >= 9900 ORDER BY id ASC LIMIT 100"
	baselineCounts := engine.ScanStrategyCounts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
		if err != nil {
			b.Fatalf("indexed range query: %v", err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("unexpected indexed range row count: got %d want 100", len(result.Rows))
		}
	}
	b.StopTimer()
	reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyBTreeOrder))

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReadIndexOnlyOrderBTree(b *testing.B) {
	ctx := context.Background()
	store, engine, targetLSN := prepareIndexedReadBenchmarkFixture(b)

	query := "SELECT email FROM entries ORDER BY email ASC LIMIT 100"
	baselineCounts := engine.ScanStrategyCounts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
		if err != nil {
			b.Fatalf("index-only query: %v", err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("unexpected index-only row count: got %d want 100", len(result.Rows))
		}
	}
	b.StopTimer()
	reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyBTreeIOScan))

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReadIndexOnlyOrderOffsetBTree(b *testing.B) {
	ctx := context.Background()
	store, engine, targetLSN := prepareIndexedReadBenchmarkFixture(b)

	query := "SELECT email FROM entries ORDER BY email ASC LIMIT 100 OFFSET 5000"
	baselineCounts := engine.ScanStrategyCounts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
		if err != nil {
			b.Fatalf("index-only offset query: %v", err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("unexpected index-only offset row count: got %d want 100", len(result.Rows))
		}
	}
	b.StopTimer()
	reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyBTreeIOScan))

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReadIndexOnlySelectiveCoveredBTree(b *testing.B) {
	ctx := context.Background()
	store, engine, targetLSN := prepareIndexedReadBenchmarkFixture(b)

	query := "SELECT email FROM entries WHERE email >= 'user-09900@asql.dev' ORDER BY email ASC LIMIT 100"
	baselineCounts := engine.ScanStrategyCounts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
		if err != nil {
			b.Fatalf("covered selective index-only query: %v", err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("unexpected covered selective row count: got %d want 100", len(result.Rows))
		}
	}
	b.StopTimer()
	reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyBTreeIOScan))

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReadSelectiveNonCoveredBTree(b *testing.B) {
	ctx := context.Background()
	store, engine, targetLSN := prepareIndexedReadBenchmarkFixture(b)

	query := "SELECT email, payload FROM entries WHERE email >= 'user-09900@asql.dev' ORDER BY email ASC LIMIT 100"
	baselineCounts := engine.ScanStrategyCounts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
		if err != nil {
			b.Fatalf("non-covered selective query: %v", err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("unexpected non-covered selective row count: got %d want 100", len(result.Rows))
		}
	}
	b.StopTimer()
	reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyBTreeOrder))

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReadCompositeCoveredIndexOnlyBTree(b *testing.B) {
	ctx := context.Background()
	store, engine, targetLSN := prepareCompositeIndexedReadBenchmarkFixture(b)

	query := "SELECT email, id FROM entries ORDER BY email ASC, id ASC LIMIT 100"
	baselineCounts := engine.ScanStrategyCounts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
		if err != nil {
			b.Fatalf("composite covered index-only query: %v", err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("unexpected composite covered row count: got %d want 100", len(result.Rows))
		}
	}
	b.StopTimer()
	reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyBTreeIOScan))

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReadCompositeNonCoveredBTree(b *testing.B) {
	ctx := context.Background()
	store, engine, targetLSN := prepareCompositeIndexedReadBenchmarkFixture(b)

	query := "SELECT email, id, payload FROM entries ORDER BY email ASC, id ASC LIMIT 100"
	baselineCounts := engine.ScanStrategyCounts()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
		if err != nil {
			b.Fatalf("composite non-covered query: %v", err)
		}
		if len(result.Rows) != 100 {
			b.Fatalf("unexpected composite non-covered row count: got %d want 100", len(result.Rows))
		}
	}
	b.StopTimer()
	reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyBTreeOrder))

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func benchmarkEngineRestartLoad(b *testing.B, withPersistedSnapshot bool) {
	ctx := context.Background()
	walPath, snapDir, expectedHeadLSN := prepareRestartBenchmarkFixture(b, withPersistedSnapshot)
	runRoot := filepath.Join(b.TempDir(), "restart-runs")
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		b.Fatalf("mkdir restart benchmark run root: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runWalPath, runSnapDir := cloneRestartBenchmarkFixture(b, runRoot, i, walPath, snapDir)
		b.StartTimer()

		store, err := wal.NewSegmentedLogStore(runWalPath, wal.AlwaysSync{})
		if err != nil {
			b.Fatalf("reopen wal store: %v", err)
		}

		engine, err := New(ctx, store, runSnapDir)
		if err != nil {
			_ = store.Close()
			b.Fatalf("restart engine: %v", err)
		}

		headLSN := engine.readState.Load().headLSN
		if headLSN != expectedHeadLSN {
			engine.WaitPendingSnapshots()
			_ = store.Close()
			b.Fatalf("unexpected head LSN after restart: got %d want %d", headLSN, expectedHeadLSN)
		}

		result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, payload FROM entries WHERE id = 1", []string{"bench"}, headLSN)
		if err != nil {
			engine.WaitPendingSnapshots()
			_ = store.Close()
			b.Fatalf("validate restarted state: %v", err)
		}
		if len(result.Rows) != 1 {
			engine.WaitPendingSnapshots()
			_ = store.Close()
			b.Fatalf("unexpected validation row count: got %d want 1", len(result.Rows))
		}

		b.StopTimer()
		engine.WaitPendingSnapshots()
		_ = store.Close()
		_ = os.RemoveAll(filepath.Dir(runWalPath))
		b.StartTimer()
	}
	b.StopTimer()
}

func cloneRestartBenchmarkFixture(b *testing.B, runRoot string, run int, walPath string, snapDir string) (string, string) {
	b.Helper()

	runDir := filepath.Join(runRoot, fmt.Sprintf("run-%06d", run))
	if err := os.RemoveAll(runDir); err != nil {
		b.Fatalf("reset restart benchmark run dir: %v", err)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		b.Fatalf("mkdir restart benchmark run dir: %v", err)
	}

	runWalPath := filepath.Join(runDir, filepath.Base(walPath))
	if err := copyWALFixture(walPath, runWalPath); err != nil {
		b.Fatalf("copy restart benchmark wal fixture: %v", err)
	}

	if snapDir == "" {
		return runWalPath, ""
	}

	runSnapDir := filepath.Join(runDir, filepath.Base(snapDir))
	if err := copyDir(snapDir, runSnapDir); err != nil {
		b.Fatalf("copy restart benchmark snapshot fixture: %v", err)
	}
	return runWalPath, runSnapDir
}

func copyWALFixture(srcBasePath string, dstBasePath string) error {
	srcDir := filepath.Dir(srcBasePath)
	srcBase := filepath.Base(srcBasePath)
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return fmt.Errorf("read wal fixture dir %s: %w", srcDir, err)
	}

	copied := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), srcBase+".") {
			continue
		}
		suffix := strings.TrimPrefix(entry.Name(), srcBase)
		srcPath := filepath.Join(srcDir, entry.Name())
		dstPath := dstBasePath + suffix
		if err := copyFile(srcPath, dstPath); err != nil {
			return err
		}
		copied = true
	}
	if !copied {
		return fmt.Errorf("no wal segment files found for %s", srcBasePath)
	}
	return nil
}

func copyDir(srcDir string, dstDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", srcDir, err)
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return fmt.Errorf("mkdir dir %s: %w", dstDir, err)
	}
	for _, entry := range entries {
		srcPath := filepath.Join(srcDir, entry.Name())
		dstPath := filepath.Join(dstDir, entry.Name())
		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
			continue
		}
		if err := copyFile(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(srcPath string, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", srcPath, err)
	}
	defer src.Close()

	info, err := src.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", srcPath, err)
	}

	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return fmt.Errorf("create %s: %w", dstPath, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("copy %s to %s: %w", srcPath, dstPath, err)
	}
	return nil
}

func prepareRestartBenchmarkFixture(b *testing.B, withPersistedSnapshot bool) (walPath string, snapDir string, expectedHeadLSN uint64) {
	b.Helper()

	ctx := context.Background()
	dir := b.TempDir()
	walPath = filepath.Join(dir, "restart-bench.wal")
	if withPersistedSnapshot {
		snapDir = filepath.Join(dir, "snapshots")
		if err := os.MkdirAll(snapDir, 0o755); err != nil {
			b.Fatalf("mkdir snapshot dir: %v", err)
		}
	}

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("new wal store: %v", err)
	}

	engine, err := New(ctx, store, snapDir)
	if err != nil {
		_ = store.Close()
		b.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
	for i := 1; i <= defaultSnapshotInterval+50; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
	}
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	engine.WaitPendingSnapshots()
	expectedHeadLSN = store.LastLSN()

	if withPersistedSnapshot {
		entries, err := os.ReadDir(snapDir)
		if err != nil {
			_ = store.Close()
			b.Fatalf("read snapshot dir: %v", err)
		}
		hasSnapshotFile := false
		for _, entry := range entries {
			if !entry.IsDir() {
				hasSnapshotFile = true
				break
			}
		}
		if !hasSnapshotFile {
			_ = store.Close()
			b.Fatal("expected persisted snapshot fixture files")
		}
	}

	if err := store.Close(); err != nil {
		b.Fatalf("close seeded wal store: %v", err)
	}

	return walPath, snapDir, expectedHeadLSN
}

func prepareIndexedReadBenchmarkFixture(b *testing.B) (*wal.SegmentedLogStore, *Engine, uint64) {
	b.Helper()

	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT, payload TEXT, email TEXT)")
	for i := 0; i < 10000; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload, email) VALUES (%d, 'payload-%05d', 'user-%05d@asql.dev')", i, i, i))
	}
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX idx_entries_id_btree ON entries (id) USING BTREE")
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX idx_entries_email_btree ON entries (email) USING BTREE")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	targetLSN := store.LastLSN()
	return store, engine, targetLSN
}

func prepareCompositeIndexedReadBenchmarkFixture(b *testing.B) (*wal.SegmentedLogStore, *Engine, uint64) {
	b.Helper()

	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT, payload TEXT, email TEXT)")
	for i := 0; i < 10000; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload, email) VALUES (%d, 'payload-%05d', 'user-%05d@asql.dev')", i, i, i%1000))
	}
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX idx_entries_email_id_btree ON entries (email, id) USING BTREE")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	targetLSN := store.LastLSN()
	return store, engine, targetLSN
}

func reportScanStrategyDelta(b *testing.B, engine *Engine, baselineCounts map[string]uint64, strategy string) {
	b.Helper()

	counts := engine.ScanStrategyCounts()
	delta := counts[strategy] - baselineCounts[strategy]
	if delta == 0 {
		b.Fatalf("expected scan strategy %q to be exercised, got counts=%+v", strategy, counts)
	}
	b.ReportMetric(float64(delta), strategy+"-count")
}

func newBenchmarkEngine(b *testing.B) (*wal.SegmentedLogStore, *Engine) {
	b.Helper()

	path := filepath.Join(b.TempDir(), "bench.wal")
	store, err := wal.NewSegmentedLogStore(path, wal.EveryN{N: 256})
	if err != nil {
		b.Fatalf("new file log store: %v", err)
	}

	engine, err := New(context.Background(), store, "")
	if err != nil {
		_ = store.Close()
		b.Fatalf("new engine: %v", err)
	}

	return store, engine
}

func mustExecBenchmark(b *testing.B, ctx context.Context, engine *Engine, session *Session, sql string) {
	b.Helper()

	if _, err := engine.Execute(ctx, session, sql); err != nil {
		b.Fatalf("execute %q: %v", sql, err)
	}
}

// BenchmarkEngineWriteCommitConcurrent measures concurrent write throughput
// with multiple goroutines doing INSERT+COMMIT in parallel. This demonstrates
// the benefit of group commit: N concurrent commits share a single fsync
// instead of serializing N fsyncs. Uses retry-on-conflict since writes to the
// same table use optimistic concurrency control.
func BenchmarkEngineWriteCommitConcurrent(b *testing.B) {
	ctx := context.Background()

	path := filepath.Join(b.TempDir(), "bench-concurrent.wal")
	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("new file log store: %v", err)
	}

	engine, err := New(ctx, store, "")
	if err != nil {
		_ = store.Close()
		b.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT, payload TEXT)")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			sql := fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", i)
			for {
				tx := engine.NewSession()
				if _, err := engine.Execute(ctx, tx, "BEGIN DOMAIN bench"); err != nil {
					b.Fatalf("begin: %v", err)
				}
				if _, err := engine.Execute(ctx, tx, sql); err != nil {
					b.Fatalf("insert: %v", err)
				}
				_, err := engine.Execute(ctx, tx, "COMMIT")
				if err == nil {
					break // success
				}
				// Retry on write conflict.
				if strings.Contains(err.Error(), "write conflict") {
					continue
				}
				b.Fatalf("commit: %v", err)
			}
		}
	})

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

// BenchmarkEngineWriteCommitPreSeededBTree inserts single rows into a table
// that already has 10K rows and a btree index. This isolates the btree
// overlay COW cost and verifies it scales as O(1) amortized.
func BenchmarkEngineWriteCommitPreSeededBTree(b *testing.B) {
	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
	for i := 0; i < 10000; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'seed')", i))
	}
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX idx_entries_payload ON entries (payload) USING BTREE")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", 10000+i))
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()
}
