package executor

import (
	"context"
	"os"
	"sort"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

func sortedIDs(rows []map[string]ast.Literal) []int64 {
	var ids []int64
	for _, row := range rows {
		ids = append(ids, row["id"].NumberValue)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func requireIDs(t *testing.T, label string, got []int64, want ...int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: ids %v, want %v", label, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: ids %v, want %v", label, got, want)
		}
	}
}

// TestNotEqualOperators: != and <> compare, and like = leave out NULL;
// "b != 'z'" was read as the column "b !" and matched nothing.
func TestNotEqualOperators(t *testing.T) {
	ctx := context.Background()
	n := openAppendOnlyNode(t, t.TempDir())
	n.mustTx("CREATE TABLE jobs (id INT PRIMARY KEY, state TEXT, owner TEXT, prev TEXT)", "CREATE INDEX jobs_state ON jobs (state) USING HASH")
	n.mustTx("INSERT INTO jobs (id, state, owner, prev) VALUES (1, 'done', 'a', 'a'), (2, 'pending', 'b', 'a'), (3, 'pending', NULL, NULL), (4, NULL, 'c', 'c')")
	query := func(sql string) []int64 {
		t.Helper()
		res, err := n.engine.Query(ctx, sql, []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return sortedIDs(res.Rows)
	}
	requireIDs(t, "!=", query("SELECT id FROM jobs WHERE state != 'done'"), 2, 3)
	requireIDs(t, "<>", query("SELECT id FROM jobs WHERE state <> 'done'"), 2, 3)
	requireIDs(t, "NOT !=", query("SELECT id FROM jobs WHERE NOT (state != 'pending')"), 2, 3)
	requireIDs(t, "column != column", query("SELECT id FROM jobs WHERE owner != prev"), 2)
	requireIDs(t, "AND", query("SELECT id FROM jobs WHERE state != 'done' AND owner <> 'b'"))

	// Through the WAL, as binary predicates and, with LIKE, as SQL text.
	n.mustTx("UPDATE jobs SET owner = 'z' WHERE state != 'done'")
	n.mustTx("DELETE FROM jobs WHERE owner <> 'z' AND prev LIKE 'c%'")
	check := func(label string) {
		t.Helper()
		requireIDs(t, label+": updated", query("SELECT id FROM jobs WHERE owner = 'z'"), 2, 3)
		requireIDs(t, label+": kept", query("SELECT id FROM jobs"), 1, 2, 3)
	}
	check("live")
	n.close()
	if err := os.RemoveAll(n.snapDir); err != nil {
		t.Fatal(err)
	}
	n.open()
	defer n.close()
	check("replay of the WAL alone")
}
