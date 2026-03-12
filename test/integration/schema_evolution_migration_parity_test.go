package integration

import (
	"context"
	"path/filepath"
	"testing"

	"asql/internal/engine/executor"
	"asql/internal/engine/parser/ast"
	"asql/internal/storage/wal"
)

func TestSchemaEvolutionMigrationReplayRestartParity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "schema-evolution-parity.wal")

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
		t.Fatalf("begin setup tx: %v", err)
	}
	if _, err := engine.Execute(ctx, session, "CREATE TABLE users (id INT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := engine.Execute(ctx, session, "INSERT INTO users (id) VALUES (1)"); err != nil {
		t.Fatalf("insert row 1: %v", err)
	}
	if _, err := engine.Execute(ctx, session, "COMMIT"); err != nil {
		t.Fatalf("commit setup tx: %v", err)
	}

	forward := []string{
		"ALTER TABLE users ADD COLUMN email TEXT",
		"UPDATE users SET email = 'one@asql.dev' WHERE id = 1",
		"INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')",
	}
	rollback := []string{
		"UPDATE users SET email = NULL WHERE id = 1",
		"DELETE FROM users WHERE id = 2",
	}

	report, err := engine.ValidateMigrationPlan("accounts", forward, rollback)
	if err != nil {
		t.Fatalf("validate migration plan: %v", err)
	}
	if !report.ForwardAccepted || !report.RollbackChecked {
		t.Fatalf("unexpected migration report: %+v", report)
	}
	if report.RollbackSafe {
		t.Fatalf("expected additive schema migration to be rollback-unsafe, got %+v", report)
	}
	if len(report.Issues) == 0 {
		t.Fatalf("expected rollback-safety issue for additive schema migration, got %+v", report)
	}

	migration := engine.NewSession()
	if _, err := engine.Execute(ctx, migration, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("begin migration tx: %v", err)
	}
	for _, statement := range forward {
		if _, err := engine.Execute(ctx, migration, statement); err != nil {
			t.Fatalf("queue migration statement %q: %v", statement, err)
		}
	}
	if _, err := engine.Execute(ctx, migration, "COMMIT"); err != nil {
		t.Fatalf("commit migration tx: %v", err)
	}

	finalLSN := store.LastLSN()
	baselineResult, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users ORDER BY id ASC", []string{"accounts"}, finalLSN)
	if err != nil {
		t.Fatalf("baseline query: %v", err)
	}
	if len(baselineResult.Rows) != 2 {
		t.Fatalf("unexpected baseline row count: got %d want 2", len(baselineResult.Rows))
	}

	if err := engine.ReplayToLSN(ctx, finalLSN); err != nil {
		t.Fatalf("replay to final lsn: %v", err)
	}

	replayedResult, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users ORDER BY id ASC", []string{"accounts"}, finalLSN)
	if err != nil {
		t.Fatalf("replayed query: %v", err)
	}
	assertRowParity(t, baselineResult.Rows, replayedResult.Rows)

	if err := store.Close(); err != nil {
		t.Fatalf("close store before restart: %v", err)
	}

	reopenedStore, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen file log store: %v", err)
	}
	t.Cleanup(func() { _ = reopenedStore.Close() })

	restartedEngine, err := executor.New(ctx, reopenedStore, "")
	if err != nil {
		t.Fatalf("new restarted engine: %v", err)
	}

	restartedResult, err := restartedEngine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users ORDER BY id ASC", []string{"accounts"}, reopenedStore.LastLSN())
	if err != nil {
		t.Fatalf("restarted query: %v", err)
	}
	assertRowParity(t, baselineResult.Rows, restartedResult.Rows)
}

func assertRowParity(t *testing.T, expected []map[string]ast.Literal, actual []map[string]ast.Literal) {
	t.Helper()

	if len(expected) != len(actual) {
		t.Fatalf("row count mismatch: expected=%d actual=%d", len(expected), len(actual))
	}

	for index := range expected {
		expectedRow := expected[index]
		actualRow := actual[index]

		expectedID, expectedOK := expectedRow["id"]
		actualID, actualOK := actualRow["id"]
		if !expectedOK || !actualOK {
			t.Fatalf("missing id column at row %d: expected=%+v actual=%+v", index, expectedRow, actualRow)
		}
		if expectedID.Kind != actualID.Kind || expectedID.NumberValue != actualID.NumberValue {
			t.Fatalf("id mismatch at row %d: expected=%+v actual=%+v", index, expectedID, actualID)
		}

		expectedEmail, expectedEmailOK := expectedRow["email"]
		actualEmail, actualEmailOK := actualRow["email"]
		if !expectedEmailOK || !actualEmailOK {
			t.Fatalf("missing email column at row %d: expected=%+v actual=%+v", index, expectedRow, actualRow)
		}
		if expectedEmail.Kind != actualEmail.Kind || expectedEmail.StringValue != actualEmail.StringValue {
			t.Fatalf("email mismatch at row %d: expected=%+v actual=%+v", index, expectedEmail, actualEmail)
		}
	}
}
