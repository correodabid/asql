package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/engine/executor"
	"asql/internal/storage/wal"
)

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
	walPath, expectedLSN := prepareFailoverRecoveryFixture(b, ctx)

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

		result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users ORDER BY id ASC", []string{"orders"}, expectedLSN)
		if err != nil {
			engine.WaitPendingSnapshots()
			_ = store.Close()
			b.Fatalf("validate replayed failover state: %v", err)
		}
		if len(result.Rows) != 2 {
			engine.WaitPendingSnapshots()
			_ = store.Close()
			b.Fatalf("unexpected replayed row count: got %d want 2", len(result.Rows))
		}

		b.StopTimer()
		engine.WaitPendingSnapshots()
		_ = store.Close()
		b.StartTimer()
	}
	b.StopTimer()
}

func prepareFailoverRecoveryFixture(b *testing.B, ctx context.Context) (walPath string, expectedLSN uint64) {
	b.Helper()

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
		"BEGIN DOMAIN orders",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	)

	if _, err := leadership.RenewLeadership("orders", "node-a", state.FencingToken, store.LastLSN()); err != nil {
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
	promoted, err := flow.Failover("orders", []coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: store.LastLSN()}}, store.LastLSN())
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

	execBenchmarkTransaction(b, ctx, engine,
		"BEGIN DOMAIN orders",
		"INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')",
		"COMMIT",
	)

	engine.WaitPendingSnapshots()
	expectedLSN = store.LastLSN()
	if err := store.Close(); err != nil {
		b.Fatalf("close failover wal store: %v", err)
	}

	return walPath, expectedLSN
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
