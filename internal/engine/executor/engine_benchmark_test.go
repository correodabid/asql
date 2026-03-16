package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
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
			benchmarkEngineWriteScalingAtSize(b, size)
		})
	}
}

// BenchmarkEngineWriteScalingGuardrail keeps a fixed size ladder focused on
// the regression question “does single-row INSERT latency stay effectively
// flat as the seeded table grows from 10k to 1m rows?”.
func BenchmarkEngineWriteScalingGuardrail(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{name: "rows_10k", size: 10_000},
		{name: "rows_100k", size: 100_000},
		{name: "rows_1m", size: 1_000_000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkEngineWriteScalingAtSize(b, tc.size)
		})
	}
}

func benchmarkEngineWriteScalingAtSize(b *testing.B, size int) {
	ctx := context.Background()
	store, engine := newBenchmarkEngineWithSegmentSize(b, 256*1024*1024)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
	for i := 0; i < size; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'seed')", i))
	}
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	engine.snapshotWg.Wait()
	warmupTx := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, warmupTx, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, warmupTx, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'warmup')", size))
	mustExecBenchmark(b, ctx, engine, warmupTx, "COMMIT")
	engine.snapshotWg.Wait()
	prevGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(prevGC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		mustExecBenchmark(b, ctx, engine, tx, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload')", size+1+i))
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}

	b.StopTimer()
	engine.WaitPendingSnapshots()
	_ = store.Close()
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

	b.StopTimer()
	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineReadHistoricalAsOfLSNScaling(b *testing.B) {
	ctx := context.Background()

	for _, totalRows := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("rows_%d", totalRows), func(b *testing.B) {
			store, engine, targetLSN := prepareHistoricalReadBenchmarkFixture(b, totalRows)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, payload FROM entries WHERE id = 1", []string{"bench"}, targetLSN)
				if err != nil {
					b.Fatalf("historical time travel query: %v", err)
				}
				if len(result.Rows) != 1 {
					b.Fatalf("unexpected historical row count: got %d want 1", len(result.Rows))
				}
			}

			b.StopTimer()
			engine.WaitPendingSnapshots()
			_ = store.Close()
		})
	}
}

func BenchmarkEngineLSNForTimestampAfterRestart(b *testing.B) {
	ctx := context.Background()
	baseDir := b.TempDir()
	walPath := filepath.Join(baseDir, "timestamp-bench.wal")
	snapDir := filepath.Join(baseDir, "snaps")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		b.Fatalf("mkdir snap dir: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("new log store: %v", err)
	}
	engine, err := New(ctx, store, snapDir)
	if err != nil {
		_ = store.Close()
		b.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
	for i := 0; i < 1000; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
	}
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	records, err := store.ReadFrom(ctx, 1, 0)
	if err != nil {
		engine.WaitPendingSnapshots()
		_ = store.Close()
		b.Fatalf("read wal records: %v", err)
	}
	target := records[len(records)/2]

	engine.WaitPendingSnapshots()
	if err := store.Close(); err != nil {
		b.Fatalf("close store: %v", err)
	}

	reopenedStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("reopen log store: %v", err)
	}
	restarted, err := New(ctx, reopenedStore, snapDir)
	if err != nil {
		_ = reopenedStore.Close()
		b.Fatalf("restart engine: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resolvedLSN, err := restarted.LSNForTimestamp(ctx, target.Timestamp)
		if err != nil {
			b.Fatalf("resolve lsn for timestamp: %v", err)
		}
		if resolvedLSN != target.LSN {
			b.Fatalf("unexpected resolved lsn: got %d want %d", resolvedLSN, target.LSN)
		}
	}

	b.StopTimer()
	restarted.WaitPendingSnapshots()
	_ = reopenedStore.Close()
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
	if _, _, err := engine.readAllReplayRecords(ctx); err != nil {
		b.Fatalf("warm replay records: %v", err)
	}
	if err := engine.ReplayToLSN(ctx, targetLSN); err != nil {
		b.Fatalf("prime replay to lsn: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := engine.ReplayToLSN(ctx, targetLSN); err != nil {
			b.Fatalf("replay to lsn: %v", err)
		}
	}

	b.StopTimer()
	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func BenchmarkEngineRestartReplayOnly(b *testing.B) {
	benchmarkEngineRestartLoad(b, false)
}

func BenchmarkEngineRestartFromPersistedSnapshot(b *testing.B) {
	benchmarkEngineRestartLoad(b, true)
}

func BenchmarkEngineRestartReplayTailSweep(b *testing.B) {
	for _, tailInserts := range []int{0, 50, 100, 500, 1000, 5000, 10000} {
		b.Run(fmt.Sprintf("replay_only_tail_%d", tailInserts), func(b *testing.B) {
			benchmarkEngineRestartTailLoad(b, tailInserts, false)
		})
		b.Run(fmt.Sprintf("persisted_snapshot_tail_%d", tailInserts), func(b *testing.B) {
			benchmarkEngineRestartTailLoad(b, tailInserts, true)
		})
	}
}

func BenchmarkEngineRestartSnapshotCadenceSweep(b *testing.B) {
	const replayTail = 500
	for _, baseRows := range []int{defaultSnapshotInterval, snapshotIntervalMedium, snapshotIntervalHigh} {
		totalRows := baseRows + replayTail
		b.Run(fmt.Sprintf("replay_only_total_%d", totalRows), func(b *testing.B) {
			benchmarkEngineRestartReplayOnlyRowsLoad(b, totalRows)
		})
		b.Run(fmt.Sprintf("persisted_snapshot_total_%d_tail_%d", totalRows, replayTail), func(b *testing.B) {
			benchmarkEngineRestartCustomTailLoad(b, baseRows, replayTail, true)
		})
	}
}

func BenchmarkEngineRestartWorkloadSweep(b *testing.B) {
	for _, workload := range []restartWorkloadKind{
		restartWorkloadInsertHeavy,
		restartWorkloadUpdateHeavy,
		restartWorkloadDeleteHeavy,
	} {
		b.Run(string(workload), func(b *testing.B) {
			b.Run("replay_only", func(b *testing.B) {
				benchmarkEngineRestartWorkloadLoad(b, workload, false)
			})
			b.Run("persisted_snapshot", func(b *testing.B) {
				benchmarkEngineRestartWorkloadLoad(b, workload, true)
			})
		})
	}
}

func BenchmarkEngineRestartWorkloadCadenceSweep(b *testing.B) {
	const replayTail = 500
	for _, workload := range []restartWorkloadKind{
		restartWorkloadInsertHeavy,
		restartWorkloadUpdateHeavy,
		restartWorkloadDeleteHeavy,
	} {
		b.Run(string(workload), func(b *testing.B) {
			for _, baseMutations := range []int{defaultSnapshotInterval, snapshotIntervalMedium} {
				totalMutations := restartWorkloadTotalMutations(workload, baseMutations, replayTail)
				b.Run(fmt.Sprintf("replay_only_total_%d", totalMutations), func(b *testing.B) {
					benchmarkEngineRestartWorkloadCustomLoad(b, workload, baseMutations, replayTail, false)
				})
				b.Run(fmt.Sprintf("persisted_snapshot_total_%d_tail_%d", totalMutations, replayTail), func(b *testing.B) {
					benchmarkEngineRestartWorkloadCustomLoad(b, workload, baseMutations, replayTail, true)
				})
			}
		})
	}
}

func BenchmarkEngineRestartNaturalWorkloadCadenceSweep(b *testing.B) {
	const replayTail = 500
	for _, workload := range []restartWorkloadKind{
		restartWorkloadInsertHeavy,
		restartWorkloadUpdateHeavy,
		restartWorkloadDeleteHeavy,
	} {
		b.Run(string(workload), func(b *testing.B) {
			for _, baseMutations := range []int{defaultSnapshotInterval, snapshotIntervalMedium} {
				totalMutations := restartWorkloadTotalMutations(workload, baseMutations, replayTail)
				b.Run(fmt.Sprintf("replay_only_total_%d", totalMutations), func(b *testing.B) {
					benchmarkEngineRestartNaturalWorkloadLoad(b, workload, baseMutations, replayTail, false)
				})
				b.Run(fmt.Sprintf("policy_persisted_total_%d_tail_%d", totalMutations, replayTail), func(b *testing.B) {
					benchmarkEngineRestartNaturalWorkloadLoad(b, workload, baseMutations, replayTail, true)
				})
			}
		})
	}
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

func BenchmarkEngineDecompressPersistedSnapshotFiles(b *testing.B) {
	compressedFiles, _ := loadPersistedSnapshotFixtureFiles(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, compressed := range compressedFiles {
			data := compressed
			if isZstd(data) {
				var err error
				data, err = decompressZstd(data)
				if err != nil {
					b.Fatalf("decompress persisted snapshot file: %v", err)
				}
			}
			if len(data) == 0 {
				b.Fatal("expected decompressed snapshot payload")
			}
		}
	}
	b.StopTimer()
}

func BenchmarkEngineReadPersistedSnapshotFilesOnly(b *testing.B) {
	snapDir, names := persistedSnapshotFixtureDir(b)
	expectedFiles := len(names)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, err := os.ReadDir(snapDir)
		if err != nil {
			b.Fatalf("read persisted snapshot dir: %v", err)
		}
		readCount := 0
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasPrefix(entry.Name(), snapFilePrefix) {
				continue
			}
			data, err := os.ReadFile(filepath.Join(snapDir, entry.Name()))
			if err != nil {
				b.Fatalf("read persisted snapshot file: %v", err)
			}
			if len(data) == 0 {
				b.Fatal("expected persisted snapshot file bytes")
			}
			readCount++
		}
		if readCount != expectedFiles {
			b.Fatalf("unexpected snapshot file count: got %d want %d", readCount, expectedFiles)
		}
	}
	b.StopTimer()
}

func BenchmarkEngineDecodePersistedSnapshotFiles(b *testing.B) {
	compressedFiles, _ := loadPersistedSnapshotFixtureFiles(b)
	decompressedFiles := decompressSnapshotFixtureFiles(b, compressedFiles)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, data := range decompressedFiles {
			entries, err := decodeSnapshotFileBinaryRaw(data)
			if err != nil {
				b.Fatalf("decode persisted snapshot file: %v", err)
			}
			if len(entries) == 0 {
				b.Fatal("expected decoded snapshot entries")
			}
		}
	}
	b.StopTimer()
}

func BenchmarkEngineMaterializePersistedSnapshots(b *testing.B) {
	compressedFiles, expectedHeadLSN := loadPersistedSnapshotFixtureFiles(b)
	decompressedFiles := decompressSnapshotFixtureFiles(b, compressedFiles)
	rawFiles := decodeSnapshotFixtureFiles(b, decompressedFiles)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshots := materializeSnapshotFixtureFiles(rawFiles)
		if len(snapshots) == 0 {
			b.Fatal("expected materialized snapshots")
		}
		if snapshots[len(snapshots)-1].lsn != expectedHeadLSN {
			b.Fatalf("unexpected materialized latest snapshot lsn: got %d want %d", snapshots[len(snapshots)-1].lsn, expectedHeadLSN)
		}
	}
	b.StopTimer()
}

func BenchmarkEngineMergePersistedSnapshotDeltas(b *testing.B) {
	compressedFiles, _ := loadPersistedSnapshotFixtureFiles(b)
	decompressedFiles := decompressSnapshotFixtureFiles(b, compressedFiles)
	rawFiles := decodeSnapshotFixtureFiles(b, decompressedFiles)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mergedCount := 0
		var accumulated map[string]*persistedDomain
		for _, fileEntries := range rawFiles {
			for _, entry := range fileEntries {
				if entry.isFull || accumulated == nil {
					accumulated = entry.domains
				} else {
					accumulated = applyDeltaBinary(accumulated, entry.domains, entry.catalog)
				}
				mergedCount++
			}
		}
		if mergedCount == 0 || accumulated == nil {
			b.Fatal("expected merged persisted snapshot state")
		}
	}
	b.StopTimer()
}

func BenchmarkEngineDecodePersistedSnapshotFilesIndexed(b *testing.B) {
	compressedFiles, _ := loadIndexedPersistedSnapshotFixtureFiles(b)
	decompressedFiles := decompressSnapshotFixtureFiles(b, compressedFiles)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, data := range decompressedFiles {
			entries, err := decodeSnapshotFileBinaryRaw(data)
			if err != nil {
				b.Fatalf("decode indexed persisted snapshot file: %v", err)
			}
			if len(entries) == 0 {
				b.Fatal("expected decoded indexed snapshot entries")
			}
		}
	}
	b.StopTimer()
}

func BenchmarkEngineDecodeFullSnapshotDirect(b *testing.B) {
	compressedFiles, _ := loadPersistedSnapshotFixtureFiles(b)
	decompressedFiles := decompressSnapshotFixtureFiles(b, compressedFiles)
	data := decompressedFiles[0]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshots, err := decodeSnapshotsBinary(data)
		if err != nil {
			b.Fatalf("decode full snapshot direct: %v", err)
		}
		if len(snapshots) == 0 {
			b.Fatal("expected directly decoded full snapshot")
		}
	}
	b.StopTimer()
}

func BenchmarkEngineDecodeFullSnapshotDirectIndexed(b *testing.B) {
	compressedFiles, _ := loadIndexedPersistedSnapshotFixtureFiles(b)
	decompressedFiles := decompressSnapshotFixtureFiles(b, compressedFiles)
	data := decompressedFiles[0]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshots, err := decodeSnapshotsBinary(data)
		if err != nil {
			b.Fatalf("decode indexed full snapshot direct: %v", err)
		}
		if len(snapshots) == 0 {
			b.Fatal("expected directly decoded indexed full snapshot")
		}
	}
	b.StopTimer()
}

func BenchmarkEngineReadPersistedSnapshotsFromDirIndexed(b *testing.B) {
	_, snapDir, expectedHeadLSN := prepareIndexedSnapshotBenchmarkFixture(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshots, maxSeq, err := readAllSnapshotsFromDir(snapDir)
		if err != nil {
			b.Fatalf("read indexed persisted snapshots from dir: %v", err)
		}
		if maxSeq == 0 {
			b.Fatal("expected indexed persisted snapshot sequence")
		}
		if len(snapshots) == 0 {
			b.Fatal("expected indexed persisted snapshots")
		}
		if snapshots[len(snapshots)-1].lsn != expectedHeadLSN {
			b.Fatalf("unexpected indexed latest snapshot lsn: got %d want %d", snapshots[len(snapshots)-1].lsn, expectedHeadLSN)
		}
	}
	b.StopTimer()
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

func BenchmarkEngineReadEntityRelatedJoinScaling(b *testing.B) {
	ctx := context.Background()
	const linesPerOrder = 5

	for _, indexChildFK := range []bool{true, false} {
		variant := "child_fk_unindexed"
		expectedStrategy := string(scanStrategyJoinLeftIx)
		if indexChildFK {
			variant = "child_fk_indexed"
			expectedStrategy = string(scanStrategyJoinRightIx)
		}

		b.Run(variant, func(b *testing.B) {
			for _, orderCount := range []int{1000, 10000, 25000} {
				b.Run(fmt.Sprintf("orders_%d", orderCount), func(b *testing.B) {
					store, engine, targetLSN, targetOrderID := prepareEntityRelatedReadBenchmarkFixture(b, orderCount, linesPerOrder, indexChildFK)

					query := fmt.Sprintf(
						"SELECT o.id, o.status, l.id, l.sku, l.qty FROM orders o JOIN order_lines l ON o.id = l.order_id WHERE o.id = %d ORDER BY l.id ASC",
						targetOrderID,
					)
					baselineCounts := engine.ScanStrategyCounts()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
						if err != nil {
							b.Fatalf("entity-related join query: %v", err)
						}
						if len(result.Rows) != linesPerOrder {
							b.Fatalf("unexpected entity-related row count: got %d want %d", len(result.Rows), linesPerOrder)
						}
					}
					b.StopTimer()
					reportScanStrategyDelta(b, engine, baselineCounts, expectedStrategy)

					engine.WaitPendingSnapshots()
					_ = store.Close()
				})
			}
		})
	}
}

func BenchmarkEngineReadEntityRelatedJoinRightFilterScaling(b *testing.B) {
	ctx := context.Background()
	const linesPerOrder = 5
	const targetSKU = "sku-03"

	for _, indexChildFK := range []bool{true, false} {
		variant := "child_fk_unindexed"
		expectedStrategy := string(scanStrategyJoinLeftIx)
		if indexChildFK {
			variant = "child_fk_indexed"
			expectedStrategy = string(scanStrategyJoinRightIx)
		}

		b.Run(variant, func(b *testing.B) {
			for _, orderCount := range []int{1000, 10000} {
				b.Run(fmt.Sprintf("orders_%d", orderCount), func(b *testing.B) {
					store, engine, targetLSN, targetOrderID := prepareEntityRelatedReadBenchmarkFixture(b, orderCount, linesPerOrder, indexChildFK)

					query := fmt.Sprintf(
						"SELECT o.id, o.status, l.id, l.sku, l.qty FROM orders o JOIN order_lines l ON o.id = l.order_id WHERE o.id = %d AND l.sku = '%s' ORDER BY l.id ASC",
						targetOrderID,
						targetSKU,
					)
					baselineCounts := engine.ScanStrategyCounts()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
						if err != nil {
							b.Fatalf("entity-related join right-filter query: %v", err)
						}
						if len(result.Rows) != 1 {
							b.Fatalf("unexpected entity-related right-filter row count: got %d want 1", len(result.Rows))
						}
					}
					b.StopTimer()
					reportScanStrategyDelta(b, engine, baselineCounts, expectedStrategy)

					engine.WaitPendingSnapshots()
					_ = store.Close()
				})
			}
		})
	}
}

func BenchmarkEngineReadIndexedBooleanPredicates(b *testing.B) {
	ctx := context.Background()
	store, engine, targetLSN := prepareBooleanIndexedReadBenchmarkFixture(b)

	b.Run("and_hash_lookup", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE id = 5000 AND status = 'common'"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("AND indexed query: %v", err)
			}
			if len(result.Rows) != 1 {
				b.Fatalf("unexpected AND row count: got %d want 1", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyHashLookup))
	})

	b.Run("and_full_scan", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE payload = 'payload-05000' AND bucket = 'common'"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("AND full-scan query: %v", err)
			}
			if len(result.Rows) != 1 {
				b.Fatalf("unexpected AND full-scan row count: got %d want 1", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyFullScan))
	})

	b.Run("or_index_union", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE id = 5000 OR id = 7500"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("OR indexed query: %v", err)
			}
			if len(result.Rows) != 2 {
				b.Fatalf("unexpected OR row count: got %d want 2", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyIndexUnion))
	})

	b.Run("in_hash_lookup", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE id IN (5000, 7500)"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("IN indexed query: %v", err)
			}
			if len(result.Rows) != 2 {
				b.Fatalf("unexpected IN row count: got %d want 2", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyHashLookup))
	})

	b.Run("in_full_scan", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE payload IN ('payload-05000', 'payload-07500')"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("IN full-scan query: %v", err)
			}
			if len(result.Rows) != 2 {
				b.Fatalf("unexpected IN full-scan row count: got %d want 2", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyFullScan))
	})

	b.Run("or_full_scan", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE payload = 'payload-05000' OR payload = 'payload-07500'"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("OR full-scan query: %v", err)
			}
			if len(result.Rows) != 2 {
				b.Fatalf("unexpected OR full-scan row count: got %d want 2", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyFullScan))
	})

	b.Run("not_index_complement", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE NOT status = 'common'"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("NOT indexed query: %v", err)
			}
			if len(result.Rows) != 100 {
				b.Fatalf("unexpected NOT row count: got %d want 100", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyIndexNot))
	})

	b.Run("not_full_scan", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE NOT bucket = 'common'"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("NOT full-scan query: %v", err)
			}
			if len(result.Rows) != 100 {
				b.Fatalf("unexpected NOT full-scan row count: got %d want 100", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyFullScan))
	})

	b.Run("not_in_index_complement", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE status NOT IN ('common')"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("NOT IN indexed query: %v", err)
			}
			if len(result.Rows) != 100 {
				b.Fatalf("unexpected NOT IN row count: got %d want 100", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyIndexNot))
	})

	b.Run("not_in_full_scan", func(b *testing.B) {
		query := "SELECT id, status FROM entries WHERE bucket NOT IN ('common')"
		baselineCounts := engine.ScanStrategyCounts()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, query, []string{"bench"}, targetLSN)
			if err != nil {
				b.Fatalf("NOT IN full-scan query: %v", err)
			}
			if len(result.Rows) != 100 {
				b.Fatalf("unexpected NOT IN full-scan row count: got %d want 100", len(result.Rows))
			}
		}
		b.StopTimer()
		reportScanStrategyDelta(b, engine, baselineCounts, string(scanStrategyFullScan))
	})

	engine.WaitPendingSnapshots()
	_ = store.Close()
}

func benchmarkEngineRestartLoad(b *testing.B, withPersistedSnapshot bool) {
	ctx := context.Background()
	walPath, snapDir, expectedHeadLSN := prepareRestartBenchmarkFixture(b, withPersistedSnapshot)
	benchmarkEngineRestartFixtureLoad(b, ctx, walPath, snapDir, expectedHeadLSN)
}

func benchmarkEngineRestartTailLoad(b *testing.B, tailInserts int, withPersistedSnapshot bool) {
	benchmarkEngineRestartCustomTailLoad(b, defaultSnapshotInterval, tailInserts, withPersistedSnapshot)
}

func benchmarkEngineRestartReplayOnlyRowsLoad(b *testing.B, totalRows int) {
	ctx := context.Background()
	walPath, snapDir, expectedHeadLSN := prepareRestartReplayOnlyFixture(b, totalRows)
	benchmarkEngineRestartFixtureLoad(b, ctx, walPath, snapDir, expectedHeadLSN)
}

func benchmarkEngineRestartCustomTailLoad(b *testing.B, baseRows int, tailInserts int, withPersistedSnapshot bool) {
	ctx := context.Background()
	walPath, snapDir, expectedHeadLSN := prepareRestartTailBenchmarkFixtureWithBase(b, baseRows, tailInserts, withPersistedSnapshot)
	benchmarkEngineRestartFixtureLoad(b, ctx, walPath, snapDir, expectedHeadLSN)
}

func benchmarkEngineRestartWorkloadLoad(b *testing.B, workload restartWorkloadKind, withPersistedSnapshot bool) {
	ctx := context.Background()
	walPath, snapDir, expectedHeadLSN := prepareRestartWorkloadFixture(b, workload, withPersistedSnapshot)
	benchmarkEngineRestartFixtureLoad(b, ctx, walPath, snapDir, expectedHeadLSN)
}

func benchmarkEngineRestartWorkloadCustomLoad(b *testing.B, workload restartWorkloadKind, baseMutations int, tailMutations int, withPersistedSnapshot bool) {
	ctx := context.Background()
	walPath, snapDir, expectedHeadLSN := prepareRestartWorkloadFixtureWithBase(b, workload, baseMutations, tailMutations, withPersistedSnapshot)
	benchmarkEngineRestartFixtureLoad(b, ctx, walPath, snapDir, expectedHeadLSN)
}

func benchmarkEngineRestartNaturalWorkloadLoad(b *testing.B, workload restartWorkloadKind, baseMutations int, tailMutations int, withPersistedSnapshot bool) {
	ctx := context.Background()
	walPath, snapDir, expectedHeadLSN := prepareRestartNaturalWorkloadFixture(b, workload, baseMutations, tailMutations, withPersistedSnapshot)
	benchmarkEngineRestartFixtureLoad(b, ctx, walPath, snapDir, expectedHeadLSN)
}

func benchmarkEngineRestartFixtureLoad(b *testing.B, ctx context.Context, walPath string, snapDir string, expectedHeadLSN uint64) {
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

func loadPersistedSnapshotFixtureFiles(b *testing.B) ([][]byte, uint64) {
	b.Helper()

	snapDir, names := persistedSnapshotFixtureDir(b)
	expectedHeadLSN := persistedSnapshotFixtureHeadLSN(b, snapDir)
	return loadSnapshotFixtureFilesFromDir(b, snapDir, names), expectedHeadLSN
}

func loadIndexedPersistedSnapshotFixtureFiles(b *testing.B) ([][]byte, uint64) {
	b.Helper()

	_, snapDir, expectedHeadLSN := prepareIndexedSnapshotBenchmarkFixture(b)
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		b.Fatalf("read indexed persisted snapshot fixture dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), snapFilePrefix) {
			continue
		}
		names = append(names, entry.Name())
	}
	if len(names) == 0 {
		b.Fatal("expected indexed persisted snapshot fixture files")
	}
	sort.Strings(names)
	return loadSnapshotFixtureFilesFromDir(b, snapDir, names), expectedHeadLSN
}

func loadSnapshotFixtureFilesFromDir(b *testing.B, snapDir string, names []string) [][]byte {
	b.Helper()

	files := make([][]byte, 0, len(names))
	for _, name := range names {
		data, err := os.ReadFile(filepath.Join(snapDir, name))
		if err != nil {
			b.Fatalf("read persisted snapshot fixture file %s: %v", name, err)
		}
		files = append(files, data)
	}
	return files
}

func persistedSnapshotFixtureDir(b *testing.B) (string, []string) {
	b.Helper()

	_, snapDir, _ := prepareRestartBenchmarkFixture(b, true)
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		b.Fatalf("read persisted snapshot fixture dir: %v", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), snapFilePrefix) {
			continue
		}
		names = append(names, entry.Name())
	}
	if len(names) == 0 {
		b.Fatal("expected persisted snapshot fixture files")
	}
	sort.Strings(names)
	return snapDir, names
}

func persistedSnapshotFixtureHeadLSN(b *testing.B, snapDir string) uint64 {
	b.Helper()

	walPath := filepath.Join(filepath.Dir(snapDir), "restart-bench.wal")
	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("open persisted snapshot fixture wal store: %v", err)
	}
	defer store.Close()

	return store.LastLSN()
}

func prepareIndexedSnapshotBenchmarkFixture(b *testing.B) (walPath string, snapDir string, expectedHeadLSN uint64) {
	b.Helper()

	ctx := context.Background()
	dir := b.TempDir()
	walPath = filepath.Join(dir, "indexed-restart-bench.wal")
	snapDir = filepath.Join(dir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		b.Fatalf("mkdir indexed snapshot dir: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("new indexed wal store: %v", err)
	}

	engine, err := New(ctx, store, snapDir)
	if err != nil {
		_ = store.Close()
		b.Fatalf("new indexed engine: %v", err)
	}

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT, email TEXT)")
	for i := 1; i <= defaultSnapshotInterval+50; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload, email) VALUES (%d, 'payload-%d', 'user-%04d@asql.dev')", i, i, i))
	}
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX idx_entries_email_btree ON entries (email) USING BTREE")
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX idx_entries_payload_hash ON entries (payload) USING HASH")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	engine.WaitPendingSnapshots()
	expectedHeadLSN = store.LastLSN()

	if err := store.Close(); err != nil {
		b.Fatalf("close indexed seeded wal store: %v", err)
	}

	return walPath, snapDir, expectedHeadLSN
}

func decompressSnapshotFixtureFiles(b *testing.B, compressedFiles [][]byte) [][]byte {
	b.Helper()

	decompressedFiles := make([][]byte, len(compressedFiles))
	for i, compressed := range compressedFiles {
		data := compressed
		if isZstd(data) {
			var err error
			data, err = decompressZstd(data)
			if err != nil {
				b.Fatalf("decompress persisted snapshot fixture file: %v", err)
			}
		}
		decompressedFiles[i] = data
	}
	return decompressedFiles
}

func decodeSnapshotFixtureFiles(b *testing.B, decompressedFiles [][]byte) [][]rawSnapshotFileEntry {
	b.Helper()

	rawFiles := make([][]rawSnapshotFileEntry, len(decompressedFiles))
	for i, data := range decompressedFiles {
		entries, err := decodeSnapshotFileBinaryRaw(data)
		if err != nil {
			b.Fatalf("decode persisted snapshot fixture file: %v", err)
		}
		if len(entries) == 0 {
			b.Fatal("expected raw snapshot entries")
		}
		rawFiles[i] = entries
	}
	return rawFiles
}

func materializeSnapshotFixtureFiles(rawFiles [][]rawSnapshotFileEntry) []engineSnapshot {
	var accumulated map[string]*persistedDomain
	result := make([]engineSnapshot, 0)
	for _, fileEntries := range rawFiles {
		for _, entry := range fileEntries {
			if entry.isFull || accumulated == nil {
				accumulated = entry.domains
			} else {
				accumulated = applyDeltaBinary(accumulated, entry.domains, entry.catalog)
			}

			snap := marshalableToSnapshot(persistedSnapshot{
				LSN:       entry.lsn,
				LogicalTS: entry.logicalTS,
				Catalog:   entry.catalog,
				Domains:   accumulated,
			})
			rebuildAllIndexes(&snap)
			result = append(result, snap)
		}
	}
	return result
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

func prepareRestartTailBenchmarkFixture(b *testing.B, tailInserts int, withPersistedSnapshot bool) (walPath string, snapDir string, expectedHeadLSN uint64) {
	return prepareRestartTailBenchmarkFixtureWithBase(b, defaultSnapshotInterval, tailInserts, withPersistedSnapshot)
}

type restartWorkloadKind string

const (
	restartWorkloadInsertHeavy restartWorkloadKind = "insert_heavy"
	restartWorkloadUpdateHeavy restartWorkloadKind = "update_heavy"
	restartWorkloadDeleteHeavy restartWorkloadKind = "delete_heavy"
)

func prepareRestartWorkloadFixture(b *testing.B, workload restartWorkloadKind, withPersistedSnapshot bool) (walPath string, snapDir string, expectedHeadLSN uint64) {
	return prepareRestartWorkloadFixtureWithBase(b, workload, defaultSnapshotInterval, defaultSnapshotInterval, withPersistedSnapshot)
}

func prepareRestartNaturalWorkloadFixture(b *testing.B, workload restartWorkloadKind, baseMutations int, tailMutations int, withPersistedSnapshot bool) (walPath string, snapDir string, expectedHeadLSN uint64) {
	b.Helper()
	if baseMutations <= 0 {
		b.Fatalf("base mutations must be positive: %d", baseMutations)
	}
	if tailMutations < 0 {
		b.Fatalf("tail mutations must be non-negative: %d", tailMutations)
	}

	ctx := context.Background()
	dir := b.TempDir()
	walPath = filepath.Join(dir, fmt.Sprintf("restart-natural-%s-bench.wal", workload))
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
	applyRestartWorkloadSeed(b, ctx, engine, session, workload, baseMutations, tailMutations)
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	session = engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	applyRestartWorkloadTail(b, ctx, engine, session, workload, baseMutations, tailMutations)
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	waitInFlightSnapshotsWithoutFlush(engine)
	expectedHeadLSN = store.LastLSN()
	if err := store.Close(); err != nil {
		b.Fatalf("close natural workload wal store: %v", err)
	}

	if !withPersistedSnapshot {
		return walPath, "", expectedHeadLSN
	}
	return walPath, snapDir, expectedHeadLSN
}

func waitInFlightSnapshotsWithoutFlush(engine *Engine) {
	if engine.commitQ != nil {
		engine.commitQ.stop()
		engine.commitQ = nil
	}
	if engine.groupSync != nil {
		engine.groupSync.stop()
		engine.groupSync = nil
	}
	engine.snapshotWg.Wait()
}

func prepareRestartWorkloadFixtureWithBase(b *testing.B, workload restartWorkloadKind, baseMutations int, tailMutations int, withPersistedSnapshot bool) (walPath string, snapDir string, expectedHeadLSN uint64) {
	b.Helper()
	if baseMutations <= 0 {
		b.Fatalf("base mutations must be positive: %d", baseMutations)
	}
	if tailMutations < 0 {
		b.Fatalf("tail mutations must be non-negative: %d", tailMutations)
	}

	ctx := context.Background()
	dir := b.TempDir()
	walPath = filepath.Join(dir, fmt.Sprintf("restart-%s-bench.wal", workload))
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
	applyRestartWorkloadSeed(b, ctx, engine, session, workload, baseMutations, tailMutations)
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	if withPersistedSnapshot {
		engine.WaitPendingSnapshots()
		expectedHeadLSN = store.LastLSN()
		if err := store.Close(); err != nil {
			b.Fatalf("close workload baseline wal store: %v", err)
		}

		entries, err := os.ReadDir(snapDir)
		if err != nil {
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
			b.Fatal("expected persisted snapshot fixture files")
		}

		if tailMutations == 0 {
			return walPath, snapDir, expectedHeadLSN
		}

		store, err = wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
		if err != nil {
			b.Fatalf("reopen workload wal store for tail: %v", err)
		}
		engine, err = New(ctx, store, "")
		if err != nil {
			_ = store.Close()
			b.Fatalf("reopen workload engine for tail: %v", err)
		}
		session = engine.NewSession()
	} else if tailMutations == 0 {
		engine.WaitPendingSnapshots()
		expectedHeadLSN = store.LastLSN()
		if err := store.Close(); err != nil {
			b.Fatalf("close workload replay-only wal store: %v", err)
		}
		return walPath, "", expectedHeadLSN
	}

	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	applyRestartWorkloadTail(b, ctx, engine, session, workload, baseMutations, tailMutations)
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	engine.WaitPendingSnapshots()
	expectedHeadLSN = store.LastLSN()
	if err := store.Close(); err != nil {
		b.Fatalf("close workload wal store: %v", err)
	}

	if !withPersistedSnapshot {
		return walPath, "", expectedHeadLSN
	}
	return walPath, snapDir, expectedHeadLSN
}

func applyRestartWorkloadSeed(b *testing.B, ctx context.Context, engine *Engine, session *Session, workload restartWorkloadKind, baseMutations int, tailMutations int) {
	b.Helper()

	switch workload {
	case restartWorkloadInsertHeavy, restartWorkloadUpdateHeavy:
		for i := 1; i <= baseMutations; i++ {
			mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
		}
	case restartWorkloadDeleteHeavy:
		for i := 1; i <= baseMutations+tailMutations; i++ {
			mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
		}
	default:
		b.Fatalf("unsupported restart workload: %s", workload)
	}
}

func applyRestartWorkloadTail(b *testing.B, ctx context.Context, engine *Engine, session *Session, workload restartWorkloadKind, baseMutations int, tailMutations int) {
	b.Helper()

	switch workload {
	case restartWorkloadInsertHeavy:
		for i := baseMutations + 1; i <= baseMutations+tailMutations; i++ {
			mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
		}
	case restartWorkloadUpdateHeavy:
		const workingSet = 100
		for i := 1; i <= tailMutations; i++ {
			targetID := ((i - 1) % workingSet) + 1
			mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("UPDATE entries SET payload = 'payload-updated-%d' WHERE id = %d", i, targetID))
		}
	case restartWorkloadDeleteHeavy:
		for i := baseMutations + tailMutations; i > baseMutations; i-- {
			mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("DELETE FROM entries WHERE id = %d", i))
		}
	default:
		b.Fatalf("unsupported restart workload: %s", workload)
	}
}

func restartWorkloadTotalMutations(workload restartWorkloadKind, baseMutations int, tailMutations int) int {
	switch workload {
	case restartWorkloadInsertHeavy, restartWorkloadUpdateHeavy:
		return baseMutations + tailMutations
	case restartWorkloadDeleteHeavy:
		return baseMutations + (2 * tailMutations)
	default:
		return baseMutations + tailMutations
	}
}

func prepareRestartTailBenchmarkFixtureWithBase(b *testing.B, baseRows int, tailInserts int, withPersistedSnapshot bool) (walPath string, snapDir string, expectedHeadLSN uint64) {
	b.Helper()

	if baseRows <= 0 {
		b.Fatalf("base rows must be positive: %d", baseRows)
	}
	if tailInserts < 0 {
		b.Fatalf("tail inserts must be non-negative: %d", tailInserts)
	}
	if !withPersistedSnapshot {
		return prepareRestartReplayOnlyFixture(b, baseRows+tailInserts)
	}

	ctx := context.Background()
	dir := b.TempDir()
	walPath = filepath.Join(dir, "restart-tail-bench.wal")
	snapDir = filepath.Join(dir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		b.Fatalf("mkdir snapshot dir: %v", err)
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
	for i := 1; i <= baseRows; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
	}
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	engine.WaitPendingSnapshots()
	expectedHeadLSN = store.LastLSN()
	if err := store.Close(); err != nil {
		b.Fatalf("close baseline wal store: %v", err)
	}

	entries, err := os.ReadDir(snapDir)
	if err != nil {
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
		b.Fatal("expected persisted snapshot fixture files")
	}

	if tailInserts == 0 {
		return walPath, snapDir, expectedHeadLSN
	}

	store, err = wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("reopen wal store for tail: %v", err)
	}
	engine, err = New(ctx, store, "")
	if err != nil {
		_ = store.Close()
		b.Fatalf("reopen engine for tail: %v", err)
	}

	session = engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	for i := baseRows + 1; i <= baseRows+tailInserts; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
	}
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	engine.WaitPendingSnapshots()
	expectedHeadLSN = store.LastLSN()
	if err := store.Close(); err != nil {
		b.Fatalf("close tail wal store: %v", err)
	}

	return walPath, snapDir, expectedHeadLSN
}

func prepareRestartReplayOnlyFixture(b *testing.B, totalRows int) (walPath string, snapDir string, expectedHeadLSN uint64) {
	b.Helper()

	ctx := context.Background()
	dir := b.TempDir()
	walPath = filepath.Join(dir, "restart-replay-only-bench.wal")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("new wal store: %v", err)
	}

	engine, err := New(ctx, store, "")
	if err != nil {
		_ = store.Close()
		b.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
	for i := 1; i <= totalRows; i++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
	}
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	engine.WaitPendingSnapshots()
	expectedHeadLSN = store.LastLSN()
	if err := store.Close(); err != nil {
		b.Fatalf("close replay-only wal store: %v", err)
	}

	return walPath, "", expectedHeadLSN
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

func prepareBooleanIndexedReadBenchmarkFixture(b *testing.B) (*wal.SegmentedLogStore, *Engine, uint64) {
	b.Helper()

	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE entries (id INT, status TEXT, bucket TEXT, payload TEXT)")
	for i := 0; i < 10000; i++ {
		status := "common"
		if i >= 9900 {
			status = "rare"
		}
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO entries (id, status, bucket, payload) VALUES (%d, '%s', '%s', 'payload-%05d')", i, status, status, i))
	}
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX idx_entries_id_hash ON entries (id) USING HASH")
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX idx_entries_status_hash ON entries (status) USING HASH")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	targetLSN := store.LastLSN()
	return store, engine, targetLSN
}

func prepareEntityRelatedReadBenchmarkFixture(b *testing.B, orderCount int, linesPerOrder int, indexChildFK bool) (*wal.SegmentedLogStore, *Engine, uint64, int) {
	b.Helper()

	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT)")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE order_lines (id INT PRIMARY KEY, order_id INT REFERENCES orders(id), sku TEXT, qty INT)")
	mustExecBenchmark(b, ctx, engine, session, "CREATE ENTITY order_aggregate (ROOT orders, INCLUDES order_lines)")

	lineID := 1
	for orderID := 1; orderID <= orderCount; orderID++ {
		mustExecBenchmark(b, ctx, engine, session, fmt.Sprintf("INSERT INTO orders (id, status) VALUES (%d, 'open')", orderID))
		for line := 0; line < linesPerOrder; line++ {
			mustExecBenchmark(
				b,
				ctx,
				engine,
				session,
				fmt.Sprintf(
					"INSERT INTO order_lines (id, order_id, sku, qty) VALUES (%d, %d, 'sku-%02d', %d)",
					lineID,
					orderID,
					line,
					line+1,
				),
			)
			lineID++
		}
	}

	if indexChildFK {
		mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX idx_order_lines_order_id_hash ON order_lines (order_id) USING HASH")
	}
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	targetLSN := store.LastLSN()
	targetOrderID := orderCount / 2
	if targetOrderID == 0 {
		targetOrderID = 1
	}
	return store, engine, targetLSN, targetOrderID
}

func prepareHistoricalReadBenchmarkFixture(b *testing.B, totalRows int) (*wal.SegmentedLogStore, *Engine, uint64) {
	b.Helper()

	ctx := context.Background()
	store, engine := newBenchmarkEngine(b)

	halfRows := totalRows / 2
	if halfRows == 0 {
		halfRows = 1
	}

	seed := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, seed, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, seed, "CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)")
	for i := 1; i <= halfRows; i++ {
		mustExecBenchmark(b, ctx, engine, seed, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
	}
	mustExecBenchmark(b, ctx, engine, seed, "COMMIT")
	targetLSN := store.LastLSN()

	advance := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, advance, "BEGIN DOMAIN bench")
	for i := halfRows + 1; i <= totalRows; i++ {
		mustExecBenchmark(b, ctx, engine, advance, fmt.Sprintf("INSERT INTO entries (id, payload) VALUES (%d, 'payload-%d')", i, i))
	}
	mustExecBenchmark(b, ctx, engine, advance, "COMMIT")

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

	return newBenchmarkEngineWithSegmentSize(b, wal.DefaultSegmentSize)
}

func newBenchmarkEngineWithSegmentSize(b *testing.B, segmentSize int64) (*wal.SegmentedLogStore, *Engine) {
	b.Helper()

	path := filepath.Join(b.TempDir(), "bench.wal")
	store, err := wal.NewSegmentedLogStore(path, wal.EveryN{N: 256}, wal.WithSegmentSize(segmentSize))
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
