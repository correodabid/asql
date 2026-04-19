package integration

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/wal"
)

func TestReplayToLSNAndTimeTravelQueries(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "time-travel.wal")

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	first := engine.NewSession()
	if _, err := engine.Execute(ctx, first, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("begin domain tx1: %v", err)
	}
	if _, err := engine.Execute(ctx, first, "CREATE TABLE users (id INT, email TEXT)"); err != nil {
		t.Fatalf("create table tx1: %v", err)
	}
	if _, err := engine.Execute(ctx, first, "INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')"); err != nil {
		t.Fatalf("insert row 1 tx1: %v", err)
	}
	if _, err := engine.Execute(ctx, first, "COMMIT"); err != nil {
		t.Fatalf("commit tx1: %v", err)
	}

	second := engine.NewSession()
	if _, err := engine.Execute(ctx, second, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("begin domain tx2: %v", err)
	}
	if _, err := engine.Execute(ctx, second, "INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')"); err != nil {
		t.Fatalf("insert row 2 tx2: %v", err)
	}
	if _, err := engine.Execute(ctx, second, "COMMIT"); err != nil {
		t.Fatalf("commit tx2: %v", err)
	}

	if got := engine.RowCount("accounts", "users"); got != 2 {
		t.Fatalf("unexpected current row count: got %d want 2", got)
	}

	atFirstCommit, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users", []string{"accounts"}, 4)
	if err != nil {
		t.Fatalf("time travel lsn=4: %v", err)
	}
	if len(atFirstCommit.Rows) != 1 {
		t.Fatalf("unexpected rows at lsn 4: got %d want 1", len(atFirstCommit.Rows))
	}

	atSecondCommit, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users", []string{"accounts"}, 7)
	if err != nil {
		t.Fatalf("time travel lsn=7: %v", err)
	}
	if len(atSecondCommit.Rows) != 2 {
		t.Fatalf("unexpected rows at lsn 7: got %d want 2", len(atSecondCommit.Rows))
	}

	resolvedLSN, err := engine.LSNForTimestamp(ctx, 6)
	if err != nil {
		t.Fatalf("resolve lsn for ts=6: %v", err)
	}
	if resolvedLSN != 6 {
		t.Fatalf("unexpected resolved lsn for ts=6: got %d want 6", resolvedLSN)
	}

	atTimestampFour, err := engine.TimeTravelQueryAsOfTimestamp(ctx, "SELECT id, email FROM users", []string{"accounts"}, 4)
	if err != nil {
		t.Fatalf("time travel timestamp=4: %v", err)
	}
	if len(atTimestampFour.Rows) != 1 {
		t.Fatalf("unexpected rows at ts 4: got %d want 1", len(atTimestampFour.Rows))
	}

	if err := engine.ReplayToLSN(ctx, 4); err != nil {
		t.Fatalf("replay to lsn 4: %v", err)
	}

	if got := engine.RowCount("accounts", "users"); got != 1 {
		t.Fatalf("unexpected row count after replay-to-lsn: got %d want 1", got)
	}
}
