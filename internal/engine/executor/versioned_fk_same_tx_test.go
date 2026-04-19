package executor

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/wal"
)

func mustExecVFKSameTx(t *testing.T, ctx context.Context, engine *Engine, session *Session, sql string) {
	t.Helper()
	if _, err := engine.Execute(ctx, session, sql); err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
}

func TestVFKAutoCaptureSeesEarlierStatementsInSameTransactionForEntities(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "vfk-same-tx-entity.wal")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()

	for _, sql := range []string{
		"BEGIN DOMAIN recipes",
		"CREATE TABLE recipes.master_recipes (id INT PRIMARY KEY, name TEXT)",
		"CREATE ENTITY recipe_aggregate (ROOT master_recipes)",
		"COMMIT",
		"BEGIN CROSS DOMAIN execution, recipes",
		"CREATE TABLE execution.process_orders (id INT PRIMARY KEY, recipe_id INT, recipe_version INT, VERSIONED FOREIGN KEY (recipe_id) REFERENCES recipes.master_recipes(id) AS OF recipe_version)",
		"COMMIT",
		"BEGIN CROSS DOMAIN execution, recipes",
		"INSERT INTO recipes.master_recipes (id, name) VALUES (1, 'Recipe A')",
		"INSERT INTO execution.process_orders (id, recipe_id) VALUES (1, 1)",
		"COMMIT",
		"BEGIN CROSS DOMAIN execution, recipes",
		"UPDATE recipes.master_recipes SET name = 'Recipe A v2' WHERE id = 1",
		"INSERT INTO execution.process_orders (id, recipe_id) VALUES (2, 1)",
		"COMMIT",
	} {
		mustExecVFKSameTx(t, ctx, engine, session, sql)
	}

	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, recipe_version FROM process_orders ORDER BY id", []string{"execution"}, ^uint64(0))
	if err != nil {
		t.Fatalf("select process_orders: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 process_orders rows, got %d", len(result.Rows))
	}
	if got := result.Rows[0]["recipe_version"]; got.Kind != ast.LiteralNumber || got.NumberValue != 1 {
		t.Fatalf("expected first same-tx auto-capture to store version 1, got %+v", got)
	}
	if got := result.Rows[1]["recipe_version"]; got.Kind != ast.LiteralNumber || got.NumberValue != 2 {
		t.Fatalf("expected second same-tx auto-capture to store version 2, got %+v", got)
	}

	replayed, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("replay engine: %v", err)
	}
	replayedResult, err := replayed.TimeTravelQueryAsOfLSN(ctx, "SELECT id, recipe_version FROM process_orders ORDER BY id", []string{"execution"}, ^uint64(0))
	if err != nil {
		t.Fatalf("replay select process_orders: %v", err)
	}
	if len(replayedResult.Rows) != 2 {
		t.Fatalf("expected 2 replayed rows, got %d", len(replayedResult.Rows))
	}
	if replayedResult.Rows[0]["recipe_version"].NumberValue != 1 || replayedResult.Rows[1]["recipe_version"].NumberValue != 2 {
		t.Fatalf("unexpected replayed recipe versions: %+v", replayedResult.Rows)
	}
}

func TestVFKAutoCaptureSeesEarlierStatementsInSameTransactionForPlainRows(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "vfk-same-tx-plain.wal")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()

	for _, sql := range []string{
		"BEGIN DOMAIN identity",
		"CREATE TABLE identity.users (id TEXT PRIMARY KEY, name TEXT)",
		"COMMIT",
		"BEGIN CROSS DOMAIN billing, identity",
		"CREATE TABLE billing.orders (id TEXT PRIMARY KEY, user_id TEXT, user_lsn INT, amount INT, VERSIONED FOREIGN KEY (user_id) REFERENCES identity.users(id) AS OF user_lsn)",
		"COMMIT",
		"BEGIN CROSS DOMAIN billing, identity",
		"INSERT INTO identity.users (id, name) VALUES ('u1', 'Alice')",
		"INSERT INTO billing.orders (id, user_id, amount) VALUES ('o1', 'u1', 100)",
		"COMMIT",
	} {
		mustExecVFKSameTx(t, ctx, engine, session, sql)
	}

	parent, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, _lsn FROM users WHERE id = 'u1'", []string{"identity"}, ^uint64(0))
	if err != nil {
		t.Fatalf("select users: %v", err)
	}
	if len(parent.Rows) != 1 {
		t.Fatalf("expected 1 user row, got %d", len(parent.Rows))
	}
	userLSN := parent.Rows[0]["_lsn"].NumberValue
	if userLSN <= 0 {
		t.Fatalf("expected positive user _lsn, got %d", userLSN)
	}

	orders, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, user_lsn FROM orders WHERE id = 'o1'", []string{"billing"}, ^uint64(0))
	if err != nil {
		t.Fatalf("select orders: %v", err)
	}
	if len(orders.Rows) != 1 {
		t.Fatalf("expected 1 order row, got %d", len(orders.Rows))
	}
	if got := orders.Rows[0]["user_lsn"].NumberValue; got != userLSN {
		t.Fatalf("expected auto-captured user_lsn=%d, got %d", userLSN, got)
	}
}
