package executor

import (
	"context"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/wal"
)

type historyShapeSnapshot struct {
	Keys      []string
	Operation string
	ID        int64
	Name      string
}

func mustExecHistoryRegression(t *testing.T, ctx context.Context, engine *Engine, session *Session, sql string) {
	t.Helper()
	if _, err := engine.Execute(ctx, session, sql); err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
}

func sortedLiteralKeys(row map[string]ast.Literal) []string {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func snapshotHistoryRows(t *testing.T, rows []map[string]ast.Literal) []historyShapeSnapshot {
	t.Helper()
	snapshots := make([]historyShapeSnapshot, 0, len(rows))
	for i, row := range rows {
		commitLSN := row[HistoryCommitLSNColumnName]
		if commitLSN.NumberValue <= 0 {
			t.Fatalf("row %d has non-positive %s", i, HistoryCommitLSNColumnName)
		}
		snapshots = append(snapshots, historyShapeSnapshot{
			Keys:      sortedLiteralKeys(row),
			Operation: row[HistoryOperationColumnName].StringValue,
			ID:        row["id"].NumberValue,
			Name:      row["name"].StringValue,
		})
	}
	return snapshots
}

func TestRowHistoryRegressionStableShapeAndReplay(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "history-regression.wal")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()

	for _, sql := range []string{
		"BEGIN DOMAIN historyreg",
		"CREATE TABLE historyreg.items (id INT PRIMARY KEY, name TEXT)",
		"INSERT INTO historyreg.items (id, name) VALUES (1, 'Alice')",
		"COMMIT",
		"BEGIN DOMAIN historyreg",
		"UPDATE historyreg.items SET name = 'Bob' WHERE id = 1",
		"COMMIT",
		"BEGIN DOMAIN historyreg",
		"DELETE FROM historyreg.items WHERE id = 1",
		"COMMIT",
	} {
		mustExecHistoryRegression(t, ctx, engine, session, sql)
	}

	result, err := engine.RowHistory(ctx, "SELECT * FROM items FOR HISTORY WHERE id = 1", []string{"historyreg"})
	if err != nil {
		t.Fatalf("RowHistory(): %v", err)
	}

	want := []historyShapeSnapshot{
		{Keys: []string{HistoryCommitLSNColumnName, HistoryOperationColumnName, "id", "name"}, Operation: "INSERT", ID: 1, Name: "Alice"},
		{Keys: []string{HistoryCommitLSNColumnName, HistoryOperationColumnName, "id", "name"}, Operation: "UPDATE", ID: 1, Name: "Bob"},
		{Keys: []string{HistoryCommitLSNColumnName, HistoryOperationColumnName, "id", "name"}, Operation: "DELETE", ID: 1, Name: "Bob"},
	}
	got := snapshotHistoryRows(t, result.Rows)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected history regression snapshot:\n got: %#v\nwant: %#v", got, want)
	}

	engine.WaitPendingSnapshots()
	_ = store.Close()

	replayedStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer replayedStore.Close()

	replayed, err := New(ctx, replayedStore, "")
	if err != nil {
		t.Fatalf("replay engine: %v", err)
	}
	replayedResult, err := replayed.RowHistory(ctx, "SELECT * FROM items FOR HISTORY WHERE id = 1", []string{"historyreg"})
	if err != nil {
		t.Fatalf("replayed RowHistory(): %v", err)
	}
	gotReplay := snapshotHistoryRows(t, replayedResult.Rows)
	if !reflect.DeepEqual(gotReplay, want) {
		t.Fatalf("unexpected replayed history regression snapshot:\n got: %#v\nwant: %#v", gotReplay, want)
	}
}
