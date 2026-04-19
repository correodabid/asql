package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/storage/wal"
)

func BenchmarkRawCommit(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	store, err := wal.NewSegmentedLogStore(dir+"/bench.wal", wal.AlwaysSync{})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	engine, err := New(ctx, store, dir)
	if err != nil {
		b.Fatal(err)
	}

	// Setup: create table
	session := engine.NewSession()
	if _, err := engine.Execute(ctx, session, "BEGIN DOMAIN bench"); err != nil {
		b.Fatal(err)
	}
	if _, err := engine.Execute(ctx, session, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)"); err != nil {
		b.Fatal(err)
	}
	if _, err := engine.Execute(ctx, session, "COMMIT"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := engine.NewSession()
		if _, err := engine.Execute(ctx, s, "BEGIN DOMAIN bench"); err != nil {
			b.Fatal(err)
		}
		sql := fmt.Sprintf("INSERT INTO items (id, name) VALUES (%d, 'item-%d')", i+1000, i)
		if _, err := engine.Execute(ctx, s, sql); err != nil {
			b.Fatal(err)
		}
		if _, err := engine.Execute(ctx, s, "COMMIT"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentCommit simulates production: 4 goroutines writing to the
// same domain with 9 INSERTs per transaction, retrying on write conflict.
// This reproduces the contention pattern seen with the seed scripts and
// measures real P50/P95/P99 latencies under writeMu serialization.
func BenchmarkConcurrentCommit(b *testing.B) {
	const workers = 4

	ctx := context.Background()
	dir := b.TempDir()
	store, err := wal.NewSegmentedLogStore(dir+"/bench-concurrent.wal", wal.AlwaysSync{})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	snapDir := filepath.Join(dir, "bench-concurrent-snapshots")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		b.Fatal(err)
	}

	engine, err := New(ctx, store, snapDir)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.WaitPendingSnapshots()

	// Setup: create schema with FKs and indexes (mimics seed_billing).
	session := engine.NewSession()
	if _, err := engine.Execute(ctx, session, "BEGIN DOMAIN bench"); err != nil {
		b.Fatal(err)
	}
	tables := []string{
		"CREATE TABLE recipes (id INT PRIMARY KEY, name TEXT, status TEXT)",
		"CREATE TABLE recipe_steps (id INT PRIMARY KEY, recipe_id INT REFERENCES recipes(id), step TEXT, duration INT)",
		"CREATE TABLE recipe_materials (id INT PRIMARY KEY, recipe_id INT REFERENCES recipes(id), material TEXT, quantity INT)",
		"CREATE INDEX idx_steps_recipe ON recipe_steps (recipe_id)",
		"CREATE INDEX idx_materials_recipe ON recipe_materials (recipe_id)",
	}
	for _, sql := range tables {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := engine.Execute(ctx, session, "COMMIT"); err != nil {
		b.Fatal(err)
	}

	// Distribute work: each worker gets b.N/workers commits.
	perWorker := b.N / workers
	if perWorker < 1 {
		perWorker = 1
	}

	var (
		latencies []time.Duration
		latMu     sync.Mutex
		retries   atomic.Int64
	)

	b.ResetTimer()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			base := workerID * perWorker * 1000

			for i := 0; i < perWorker; i++ {
				recipeID := base + i + 1000

				for attempt := 0; ; attempt++ {
					start := time.Now()
					s := engine.NewSession()
					if _, err := engine.Execute(ctx, s, "BEGIN DOMAIN bench"); err != nil {
						b.Error(err)
						return
					}

					sql := fmt.Sprintf("INSERT INTO recipes (id, name, status) VALUES (%d, 'recipe-%d', 'active')", recipeID, i)
					if _, err := engine.Execute(ctx, s, sql); err != nil {
						b.Error(err)
						return
					}
					for j := 0; j < 5; j++ {
						sql = fmt.Sprintf("INSERT INTO recipe_steps (id, recipe_id, step, duration) VALUES (%d, %d, 'step-%d', %d)", recipeID*100+j, recipeID, j, (j+1)*10)
						if _, err := engine.Execute(ctx, s, sql); err != nil {
							b.Error(err)
							return
						}
					}
					for j := 0; j < 3; j++ {
						sql = fmt.Sprintf("INSERT INTO recipe_materials (id, recipe_id, material, quantity) VALUES (%d, %d, 'material-%d', %d)", recipeID*100+50+j, recipeID, j, (j+1)*5)
						if _, err := engine.Execute(ctx, s, sql); err != nil {
							b.Error(err)
							return
						}
					}

					_, err := engine.Execute(ctx, s, "COMMIT")
					elapsed := time.Since(start)

					if err != nil {
						if strings.Contains(err.Error(), "write conflict") {
							retries.Add(1)
							engine.Execute(ctx, s, "ROLLBACK")
							continue
						}
						b.Error(err)
						return
					}

					latMu.Lock()
					latencies = append(latencies, elapsed)
					latMu.Unlock()
					break
				}
			}
		}(w)
	}
	wg.Wait()
	b.StopTimer()

	// Report latency percentiles.
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		p50 := latencies[len(latencies)*50/100]
		p95 := latencies[len(latencies)*95/100]
		p99 := latencies[len(latencies)*99/100]
		b.ReportMetric(float64(p50.Microseconds())/1000, "p50-ms")
		b.ReportMetric(float64(p95.Microseconds())/1000, "p95-ms")
		b.ReportMetric(float64(p99.Microseconds())/1000, "p99-ms")
		b.ReportMetric(float64(retries.Load()), "retries")
	}
}

// BenchmarkBatchCommit simulates the production workload pattern: each
// transaction contains many INSERT statements into multiple tables, causing
// the dataset to grow large over time. This exercises the snapshot capture
// path which was the root cause of high commit latency (O(total_rows) deep
// clone per snapshot).
func BenchmarkBatchCommit(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	store, err := wal.NewSegmentedLogStore(dir+"/bench-batch.wal", wal.AlwaysSync{})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	engine, err := New(ctx, store, dir)
	if err != nil {
		b.Fatal(err)
	}

	// Setup: create schema with multiple tables (mimics seed_billing)
	session := engine.NewSession()
	if _, err := engine.Execute(ctx, session, "BEGIN DOMAIN bench"); err != nil {
		b.Fatal(err)
	}
	tables := []string{
		"CREATE TABLE recipes (id INT PRIMARY KEY, name TEXT, status TEXT)",
		"CREATE TABLE recipe_steps (id INT PRIMARY KEY, recipe_id INT REFERENCES recipes(id), step TEXT, duration INT)",
		"CREATE TABLE recipe_materials (id INT PRIMARY KEY, recipe_id INT REFERENCES recipes(id), material TEXT, quantity INT)",
		"CREATE INDEX idx_steps_recipe ON recipe_steps (recipe_id)",
		"CREATE INDEX idx_materials_recipe ON recipe_materials (recipe_id)",
	}
	for _, sql := range tables {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := engine.Execute(ctx, session, "COMMIT"); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := engine.NewSession()
		if _, err := engine.Execute(ctx, s, "BEGIN DOMAIN bench"); err != nil {
			b.Fatal(err)
		}

		recipeID := i + 1000
		// Each transaction: 1 recipe + 5 steps + 3 materials = 9 INSERTs
		sql := fmt.Sprintf("INSERT INTO recipes (id, name, status) VALUES (%d, 'recipe-%d', 'active')", recipeID, i)
		if _, err := engine.Execute(ctx, s, sql); err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 5; j++ {
			sql = fmt.Sprintf("INSERT INTO recipe_steps (id, recipe_id, step, duration) VALUES (%d, %d, 'step-%d', %d)", recipeID*100+j, recipeID, j, (j+1)*10)
			if _, err := engine.Execute(ctx, s, sql); err != nil {
				b.Fatal(err)
			}
		}
		for j := 0; j < 3; j++ {
			sql = fmt.Sprintf("INSERT INTO recipe_materials (id, recipe_id, material, quantity) VALUES (%d, %d, 'material-%d', %d)", recipeID*100+50+j, recipeID, j, (j+1)*5)
			if _, err := engine.Execute(ctx, s, sql); err != nil {
				b.Fatal(err)
			}
		}

		if _, err := engine.Execute(ctx, s, "COMMIT"); err != nil {
			b.Fatal(err)
		}
	}
}
