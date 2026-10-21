package executor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

// openLineageNode: d <- c <- b <- a and d <- x <- a (a diamond), and y <-> z
// (a cycle).
func openLineageNode(t *testing.T) *appendOnlyNode {
	t.Helper()
	n := openAppendOnlyNode(t, t.TempDir())
	n.mustTx(
		"CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT)",
		"CREATE TABLE edges (id INT PRIMARY KEY, child_id TEXT, parent_id TEXT)",
		"CREATE INDEX edges_by_child ON edges (child_id) USING HASH",
	)
	n.mustTx(
		"INSERT INTO artifacts (id, kind) VALUES ('a', 'source'), ('b', 'step'), ('c', 'step'), ('d', 'result'), ('x', 'step'), ('y', 'loop'), ('z', 'loop')",
		"INSERT INTO edges (id, child_id, parent_id) VALUES (1, 'd', 'c'), (2, 'c', 'b'), (3, 'b', 'a'), (4, 'd', 'x'), (5, 'x', 'a'), (6, 'y', 'z'), (7, 'z', 'y')",
	)
	return n
}

// column returns one column of the rows, sorted.
func column(rows []map[string]ast.Literal, name string) []string {
	var values []string
	for _, row := range rows {
		values = append(values, row[name].StringValue)
	}
	sort.Strings(values)
	return values
}

func TestRecursiveCTEWalksAGraph(t *testing.T) {
	n := openLineageNode(t)
	defer n.close()
	ctx := context.Background()
	query := func(sql string) []map[string]ast.Literal {
		t.Helper()
		res, err := n.engine.Query(ctx, sql, []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return res.Rows
	}
	// The CTE may come first in the recursive term's join or second, which
	// runs it from the CTE too.
	for _, join := range []string{"edges e JOIN lineage l ON e.child_id = l.id", "lineage l JOIN edges e ON e.child_id = l.id"} {
		rows := query("WITH RECURSIVE lineage(id) AS (SELECT parent_id FROM edges WHERE child_id = 'd' UNION SELECT e.parent_id FROM " + join + ") SELECT a.id, a.kind FROM lineage l JOIN artifacts a ON a.id = l.id")
		if got := strings.Join(column(rows, "a.id"), ","); got != "a,b,c,x" {
			t.Fatalf("%s: ancestors of d are %s, want a,b,c,x", join, got)
		}
		// UNION ALL keeps a, reached through b and through x.
		rows = query("WITH RECURSIVE lineage(id) AS (SELECT parent_id FROM edges WHERE child_id = 'd' UNION ALL SELECT e.parent_id FROM " + join + ") SELECT id FROM lineage")
		if got := strings.Join(column(rows, "id"), ","); got != "a,a,b,c,x" {
			t.Fatalf("%s: UNION ALL ancestors of d are %s, want a,a,b,c,x", join, got)
		}
	}

	// Descendants, named by the body's column, and aggregated.
	rows := query("WITH RECURSIVE kids AS (SELECT child_id FROM edges WHERE parent_id = 'a' UNION SELECT e.child_id FROM edges e JOIN kids k ON e.parent_id = k.child_id) SELECT COUNT(*) AS n FROM kids")
	if rows[0]["n"].NumberValue != 4 {
		t.Fatalf("descendants of a: %v, want 4", rows)
	}

	// UNION ends on a cycle: it adds no row twice.
	rows = query("WITH RECURSIVE loop AS (SELECT parent_id FROM edges WHERE child_id = 'y' UNION SELECT e.parent_id FROM edges e JOIN loop l ON e.child_id = l.parent_id) SELECT * FROM loop")
	if got := strings.Join(column(rows, "parent_id"), ","); got != "y,z" {
		t.Fatalf("cycle: %s, want y,z", got)
	}

	// Depth, counted by an expression in the select list. Each ancestor
	// appears once per depth it is reached at: a through b and through x.
	rows = query("WITH RECURSIVE lineage(id, depth) AS (SELECT parent_id, 1 FROM edges WHERE child_id = 'd' UNION SELECT e.parent_id, l.depth + 1 FROM lineage l JOIN edges e ON e.child_id = l.id) SELECT id, depth FROM lineage")
	var depths []string
	for _, row := range rows {
		depths = append(depths, fmt.Sprintf("%s%d", row["id"].StringValue, row["depth"].NumberValue))
	}
	sort.Strings(depths)
	if got := strings.Join(depths, ","); got != "a2,a3,b2,c1,x1" {
		t.Fatalf("lineage of d with depth: %s, want a2,a3,b2,c1,x1", got)
	}

	// A later CTE reads an earlier one.
	rows = query("WITH RECURSIVE lineage(id) AS (SELECT parent_id FROM edges WHERE child_id = 'd' UNION SELECT e.parent_id FROM edges e JOIN lineage l ON e.child_id = l.id), sources AS (SELECT a.id FROM lineage l JOIN artifacts a ON a.id = l.id WHERE a.kind = 'source') SELECT * FROM sources")
	if got := strings.Join(column(rows, "id"), ","); got != "a" {
		t.Fatalf("sources of d: %s, want a", got)
	}

	for sql, want := range map[string]string{
		"WITH RECURSIVE loop AS (SELECT parent_id FROM edges WHERE child_id = 'y' UNION ALL SELECT e.parent_id FROM edges e JOIN loop l ON e.child_id = l.parent_id) SELECT * FROM loop":               "did not finish",
		"WITH RECURSIVE lineage(id, kind) AS (SELECT parent_id FROM edges WHERE child_id = 'd' UNION SELECT e.parent_id FROM edges e JOIN lineage l ON e.child_id = l.id) SELECT * FROM lineage":       "column list names 2 columns",
		"WITH RECURSIVE lineage(id) AS (SELECT parent_id FROM edges WHERE child_id = 'd' UNION SELECT e.parent_id, e.child_id FROM edges e JOIN lineage l ON e.child_id = l.id) SELECT * FROM lineage": "recursive term returns 2 columns",
		"WITH missing AS (SELECT id FROM nowhere) SELECT * FROM missing": "table not found",
	} {
		if _, err := n.engine.Query(ctx, sql, []string{"app"}); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: error %v, want %q", sql, err, want)
		}
	}
}

// TestCTEColumnsDropTheirQualifier: a qualified column of a CTE's body is a
// column of the CTE without the qualifier, as in PostgreSQL.
func TestCTEColumnsDropTheirQualifier(t *testing.T) {
	n := openLineageNode(t)
	defer n.close()
	res, err := n.engine.Query(context.Background(), "WITH p AS (SELECT e.parent_id FROM edges e WHERE e.child_id = 'd') SELECT * FROM p", []string{"app"})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(column(res.Rows, "parent_id"), ","); got != "c,x" {
		t.Fatalf("rows %v, want parent_id c and x", res.Rows)
	}
}
