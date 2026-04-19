package integration

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/wal"
)

func TestUpgradeSimulationCandidateReplaysPreviousWALAndPreservesHistoricalParity(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "upgrade-simulation.wal")

	var firstCommitLSN uint64

	{
		store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
		if err != nil {
			t.Fatalf("open previous-version wal store: %v", err)
		}

		engine, err := executor.New(ctx, store, "")
		if err != nil {
			_ = store.Close()
			t.Fatalf("open previous-version engine: %v", err)
		}

		session := engine.NewSession()
		for _, sql := range []string{
			"BEGIN DOMAIN accounts",
			"CREATE TABLE users (id INT, email TEXT)",
			"INSERT INTO users (id, email) VALUES (1, 'first@asql.dev')",
			"COMMIT",
		} {
			if _, err := engine.Execute(ctx, session, sql); err != nil {
				_ = store.Close()
				t.Fatalf("previous-version execute %q: %v", sql, err)
			}
		}
		firstCommitLSN = store.LastLSN()

		session = engine.NewSession()
		for _, sql := range []string{
			"BEGIN DOMAIN accounts",
			"INSERT INTO users (id, email) VALUES (2, 'second@asql.dev')",
			"COMMIT",
		} {
			if _, err := engine.Execute(ctx, session, sql); err != nil {
				_ = store.Close()
				t.Fatalf("previous-version execute %q: %v", sql, err)
			}
		}

		if err := store.Close(); err != nil {
			t.Fatalf("close previous-version store: %v", err)
		}
	}

	candidateStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("open candidate wal store: %v", err)
	}
	defer candidateStore.Close()

	candidateEngine, err := executor.New(ctx, candidateStore, "")
	if err != nil {
		t.Fatalf("open candidate engine: %v", err)
	}

	if got := candidateEngine.RowCount("accounts", "users"); got != 2 {
		t.Fatalf("unexpected candidate row count: got %d want 2", got)
	}

	historical, err := candidateEngine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users", []string{"accounts"}, firstCommitLSN)
	if err != nil {
		t.Fatalf("candidate historical query: %v", err)
	}
	if len(historical.Rows) != 1 {
		t.Fatalf("unexpected historical row count after candidate replay: got %d want 1", len(historical.Rows))
	}
	if got := historical.Rows[0]["email"].StringValue; got != "first@asql.dev" {
		t.Fatalf("unexpected historical email after candidate replay: got %q want %q", got, "first@asql.dev")
	}
}
