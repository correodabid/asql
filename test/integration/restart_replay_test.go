package integration

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/wal"
)

func TestRestartReplayRestoresState(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.wal")

	{
		store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
		if err != nil {
			t.Fatalf("new file log store: %v", err)
		}

		engine, err := executor.New(ctx, store, "")
		if err != nil {
			t.Fatalf("new engine: %v", err)
		}

		session := engine.NewSession()
		if _, err := engine.Execute(ctx, session, "BEGIN DOMAIN accounts"); err != nil {
			t.Fatalf("begin domain: %v", err)
		}

		if _, err := engine.Execute(ctx, session, "CREATE TABLE users (id INT, email TEXT)"); err != nil {
			t.Fatalf("create table: %v", err)
		}

		if _, err := engine.Execute(ctx, session, "INSERT INTO users (id, email) VALUES (1, 'a@b.com')"); err != nil {
			t.Fatalf("insert row: %v", err)
		}

		if _, err := engine.Execute(ctx, session, "COMMIT"); err != nil {
			t.Fatalf("commit tx: %v", err)
		}

		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	replayed, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new replayed engine: %v", err)
	}

	if got := replayed.RowCount("accounts", "users"); got != 1 {
		t.Fatalf("unexpected replayed row count: got %d want 1", got)
	}
}
