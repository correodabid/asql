package integration

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/engine/executor"
	"asql/internal/storage/wal"
)

const failoverBenchmarkDomain = "orders"

type failoverRecoveryBenchmarkConfig struct {
	preFailoverRows  int
	postFailoverRows int
	batchSize        int
}

func BenchmarkFailoverCoordinatorPromotion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		clock := &integrationClock{now: time.Unix(100, 0).UTC()}
		leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
		if err != nil {
			b.Fatalf("new leadership manager: %v", err)
		}
		initial, err := leadership.TryAcquireLeadership("orders", "node-a", 100)
		if err != nil {
			b.Fatalf("acquire initial leader: %v", err)
		}
		clock.Advance(6 * time.Second)
		flow, err := coordinator.NewFailoverCoordinator(leadership)
		if err != nil {
			b.Fatalf("new failover coordinator: %v", err)
		}

		result, err := flow.Failover("orders", []coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: 100}}, 100)
		if err != nil {
			b.Fatalf("failover: %v", err)
		}
		if result.Promoted.LeaderID != "node-b" {
			b.Fatalf("unexpected promoted leader: %s", result.Promoted.LeaderID)
		}
		if result.Promoted.Term != initial.Term+1 {
			b.Fatalf("unexpected promoted term: got %d want %d", result.Promoted.Term, initial.Term+1)
		}
	}
}

func BenchmarkFailoverRecoveryReplay(b *testing.B) {
	ctx := context.Background()
	walPath, expectedLSN, expectedLastEmail := prepareFailoverRecoveryFixture(b, ctx, failoverRecoveryBenchmarkConfig{
		preFailoverRows:  1,
		postFailoverRows: 1,
		batchSize:        1,
	})

	benchmarkFailoverRecoveryReplayFixture(b, ctx, walPath, expectedLSN, 2, expectedLastEmail)
}

func BenchmarkFailoverRecoveryReplaySweep(b *testing.B) {
	ctx := context.Background()
	scenarios := []struct {
		name   string
		config failoverRecoveryBenchmarkConfig
	}{
		{
			name: "small_total_40",
			config: failoverRecoveryBenchmarkConfig{
				preFailoverRows:  32,
				postFailoverRows: 8,
				batchSize:        16,
			},
		},
		{
			name: "medium_total_640",
			config: failoverRecoveryBenchmarkConfig{
				preFailoverRows:  512,
				postFailoverRows: 128,
				batchSize:        64,
			},
		},
		{
			name: "large_total_4608",
			config: failoverRecoveryBenchmarkConfig{
				preFailoverRows:  4096,
				postFailoverRows: 512,
				batchSize:        128,
			},
		},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			totalRows := scenario.config.preFailoverRows + scenario.config.postFailoverRows
			walPath, expectedLSN, expectedLastEmail := prepareFailoverRecoveryFixture(b, ctx, scenario.config)
			benchmarkFailoverRecoveryReplayFixture(b, ctx, walPath, expectedLSN, totalRows, expectedLastEmail)
		})
	}
}

func benchmarkFailoverRecoveryReplayFixture(b *testing.B, ctx context.Context, walPath string, expectedLSN uint64, expectedRows int, expectedLastEmail string) {
	b.Helper()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
		if err != nil {
			b.Fatalf("reopen failover wal store: %v", err)
		}

		engine, err := executor.New(ctx, store, "")
		if err != nil {
			_ = store.Close()
			b.Fatalf("new replay engine: %v", err)
		}

		if got := store.LastLSN(); got != expectedLSN {
			engine.WaitPendingSnapshots()
			_ = store.Close()
			b.Fatalf("unexpected replay LSN: got %d want %d", got, expectedLSN)
		}

		result, err := engine.TimeTravelQueryAsOfLSN(ctx, fmt.Sprintf("SELECT email FROM users WHERE id = %d", expectedRows), []string{failoverBenchmarkDomain}, expectedLSN)
		if err != nil {
			engine.WaitPendingSnapshots()
			_ = store.Close()
			b.Fatalf("validate replayed failover state: %v", err)
		}
		if len(result.Rows) != 1 {
			engine.WaitPendingSnapshots()
			_ = store.Close()
			b.Fatalf("unexpected replayed row count: got %d want 1", len(result.Rows))
		}
		if got := result.Rows[0]["email"].StringValue; got != expectedLastEmail {
			engine.WaitPendingSnapshots()
			_ = store.Close()
			b.Fatalf("unexpected replayed trailing row: got %q want %q", got, expectedLastEmail)
		}

		b.StopTimer()
		engine.WaitPendingSnapshots()
		_ = store.Close()
		b.StartTimer()
	}
	b.StopTimer()
}

func prepareFailoverRecoveryFixture(b *testing.B, ctx context.Context, config failoverRecoveryBenchmarkConfig) (walPath string, expectedLSN uint64, expectedLastEmail string) {
	b.Helper()
	if config.preFailoverRows <= 0 {
		b.Fatal("preFailoverRows must be positive")
	}
	if config.postFailoverRows <= 0 {
		b.Fatal("postFailoverRows must be positive")
	}
	if config.batchSize <= 0 {
		b.Fatal("batchSize must be positive")
	}

	clock := &integrationClock{now: time.Unix(200, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		b.Fatalf("new leadership manager: %v", err)
	}
	state, err := leadership.TryAcquireLeadership("orders", "node-a", 0)
	if err != nil {
		b.Fatalf("acquire leader node-a: %v", err)
	}

	walPath = filepath.Join(b.TempDir(), "failover-recovery.wal")
	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		b.Fatalf("new failover wal store: %v", err)
	}

	engine, err := executor.New(ctx, store, "")
	if err != nil {
		_ = store.Close()
		b.Fatalf("new failover engine: %v", err)
	}

	execBenchmarkTransaction(b, ctx, engine,
		fmt.Sprintf("BEGIN DOMAIN %s", failoverBenchmarkDomain),
		"CREATE TABLE users (id INT PRIMARY KEY, email TEXT)",
		"COMMIT",
	)
	insertFailoverBenchmarkUsers(b, ctx, engine, 1, config.preFailoverRows, config.batchSize)

	if _, err := leadership.RenewLeadership(failoverBenchmarkDomain, "node-a", state.FencingToken, store.LastLSN()); err != nil {
		engine.WaitPendingSnapshots()
		_ = store.Close()
		b.Fatalf("renew leader node-a: %v", err)
	}

	clock.Advance(6 * time.Second)
	flow, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		engine.WaitPendingSnapshots()
		_ = store.Close()
		b.Fatalf("new failover coordinator: %v", err)
	}
	promoted, err := flow.Failover(failoverBenchmarkDomain, []coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: store.LastLSN()}}, store.LastLSN())
	if err != nil {
		engine.WaitPendingSnapshots()
		_ = store.Close()
		b.Fatalf("failover promote node-b: %v", err)
	}
	if promoted.Promoted.LeaderID != "node-b" {
		engine.WaitPendingSnapshots()
		_ = store.Close()
		b.Fatalf("unexpected promoted leader: %s", promoted.Promoted.LeaderID)
	}

	insertFailoverBenchmarkUsers(b, ctx, engine, config.preFailoverRows+1, config.postFailoverRows, config.batchSize)

	engine.WaitPendingSnapshots()
	expectedLSN = store.LastLSN()
	expectedLastEmail = failoverBenchmarkEmail(config.preFailoverRows + config.postFailoverRows)
	if err := store.Close(); err != nil {
		b.Fatalf("close failover wal store: %v", err)
	}

	return walPath, expectedLSN, expectedLastEmail
}

func execBenchmarkTransaction(b *testing.B, ctx context.Context, engine *executor.Engine, statements ...string) {
	b.Helper()

	session := engine.NewSession()
	for _, statement := range statements {
		if _, err := engine.Execute(ctx, session, statement); err != nil {
			b.Fatalf("execute %q: %v", statement, err)
		}
	}
}

func insertFailoverBenchmarkUsers(b *testing.B, ctx context.Context, engine *executor.Engine, startID int, count int, batchSize int) {
	b.Helper()
	for inserted := 0; inserted < count; inserted += batchSize {
		remaining := count - inserted
		currentBatchSize := batchSize
		if remaining < currentBatchSize {
			currentBatchSize = remaining
		}

		statements := make([]string, 0, currentBatchSize+2)
		statements = append(statements, fmt.Sprintf("BEGIN DOMAIN %s", failoverBenchmarkDomain))
		for offset := 0; offset < currentBatchSize; offset++ {
			id := startID + inserted + offset
			statements = append(statements, fmt.Sprintf("INSERT INTO users (id, email) VALUES (%d, '%s')", id, failoverBenchmarkEmail(id)))
		}
		statements = append(statements, "COMMIT")
		execBenchmarkTransaction(b, ctx, engine, statements...)
	}
}

func failoverBenchmarkEmail(id int) string {
	return fmt.Sprintf("user-%06d@asql.dev", id)
}
