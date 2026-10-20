package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	auditstore "github.com/correodabid/asql/internal/storage/audit"
	"github.com/correodabid/asql/internal/storage/wal"
)

// historyTestEngine returns an engine with three versions of row 1 and one
// of row 2, with or without the audit store that servers wire.
func historyTestEngine(t *testing.T, audited bool) *Engine {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	store, err := wal.NewSegmentedLogStore(filepath.Join(dir, "wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	var opts []EngineOption
	if audited {
		if err := os.MkdirAll(filepath.Join(dir, "audit"), 0o755); err != nil {
			t.Fatal(err)
		}
		audit, err := auditstore.New(filepath.Join(dir, "audit", "audit"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = audit.Close() })
		opts = append(opts, WithAuditStore(audit))
	}
	engine, err := New(ctx, store, "", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.WaitPendingSnapshots)
	for _, tx := range [][]string{
		{"CREATE TABLE resolutions (id INT PRIMARY KEY, verdict TEXT, score INT)"},
		{"INSERT INTO resolutions (id, verdict, score) VALUES (1, 'open', 1)", "INSERT INTO resolutions (id, verdict, score) VALUES (2, 'open', 5)"},
		{"UPDATE resolutions SET verdict = 'review', score = 2 WHERE id = 1"},
		{"UPDATE resolutions SET verdict = 'closed', score = 3 WHERE id = 1"},
	} {
		session := engine.NewSession()
		for _, sql := range append(append([]string{"BEGIN DOMAIN audit"}, tx...), "COMMIT") {
			if _, err := engine.Execute(ctx, session, sql); err != nil {
				t.Fatalf("execute %q: %v", sql, err)
			}
		}
	}
	return engine
}

// TestRowHistoryRunsTheWholeSelect: FOR HISTORY applies the select list,
// aggregates, GROUP BY, ORDER BY and LIMIT to the history rows, and WHERE can
// test the history columns.
func TestRowHistoryRunsTheWholeSelect(t *testing.T) {
	num := func(n int64) ast.Literal { return ast.Literal{Kind: ast.LiteralNumber, NumberValue: n} }
	str := func(s string) ast.Literal { return ast.Literal{Kind: ast.LiteralString, StringValue: s} }
	cases := []struct {
		sql  string
		want []map[string]ast.Literal
	}{
		{"SELECT COUNT(*) AS versions FROM resolutions FOR HISTORY WHERE id = 1",
			[]map[string]ast.Literal{{"versions": num(3)}}},
		{"SELECT COUNT(*) AS n FROM resolutions FOR HISTORY WHERE id = 99",
			[]map[string]ast.Literal{{"n": num(0)}}},
		{"SELECT verdict, __operation FROM resolutions FOR HISTORY WHERE id = 1",
			[]map[string]ast.Literal{
				{"verdict": str("open"), "__operation": str("INSERT")},
				{"verdict": str("review"), "__operation": str("UPDATE")},
				{"verdict": str("closed"), "__operation": str("UPDATE")},
			}},
		{"SELECT verdict FROM resolutions FOR HISTORY WHERE id = 1 ORDER BY __commit_lsn DESC LIMIT 1",
			[]map[string]ast.Literal{{"verdict": str("closed")}}},
		{"SELECT id FROM resolutions FOR HISTORY WHERE __operation = 'UPDATE' AND score >= 3",
			[]map[string]ast.Literal{{"id": num(1)}}},
		{"SELECT __operation, COUNT(*) AS n FROM resolutions FOR HISTORY GROUP BY __operation ORDER BY __operation",
			[]map[string]ast.Literal{
				{"__operation": str("INSERT"), "n": num(2)},
				{"__operation": str("UPDATE"), "n": num(2)},
			}},
		{"SELECT MAX(score) AS top FROM resolutions FOR HISTORY WHERE id = 1",
			[]map[string]ast.Literal{{"top": num(3)}}},
	}
	for _, audited := range []bool{true, false} {
		engine := historyTestEngine(t, audited)
		for _, tc := range cases {
			res, err := engine.RowHistory(context.Background(), tc.sql, []string{"audit"})
			if err != nil {
				t.Fatalf("audited=%t %s: %v", audited, tc.sql, err)
			}
			if !sameRows(res.Rows, tc.want) {
				t.Fatalf("audited=%t %s:\n got  %v\n want %v", audited, tc.sql, res.Rows, tc.want)
			}
		}
	}
}

func sameRows(got, want []map[string]ast.Literal) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if len(got[i]) != len(want[i]) {
			return false
		}
		for column, w := range want[i] {
			g, ok := got[i][column]
			if !ok || compareLiterals(g, w) != 0 {
				return false
			}
		}
	}
	return true
}
