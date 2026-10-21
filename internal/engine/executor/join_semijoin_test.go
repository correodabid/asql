package executor

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestSemiJoinKeepsRowsAndOrder: narrowing the base table of a join through
// its index gives the same rows, in the same order, as reading all of it.
func TestSemiJoinKeepsRowsAndOrder(t *testing.T) {
	ctx := context.Background()
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	n.mustTx(
		"CREATE TABLE big (id INT PRIMARY KEY, grp INT, label TEXT)",
		"CREATE INDEX big_grp ON big (grp) USING HASH",
		"CREATE TABLE small (id INT PRIMARY KEY, big_id INT, grp INT, tag TEXT)",
	)
	var rows []string
	// Inserted out of id order, so table order and id order differ.
	for i := 200; i >= 1; i-- {
		rows = append(rows, fmt.Sprintf("(%d, %d, 'l%d')", i, i%7, i))
	}
	n.mustTx("INSERT INTO big (id, grp, label) VALUES " + strings.Join(rows, ", "))
	n.mustTx("INSERT INTO small (id, big_id, grp, tag) VALUES (1, 150, 3, 'a'), (2, 7, 3, 'b'), (3, 150, NULL, 'c'), (4, 999, 5, 'd'), (5, NULL, 1, 'e')")

	for _, sql := range []string{
		"SELECT b.id, s.tag FROM big b JOIN small s ON b.id = s.big_id",
		"SELECT b.id, s.tag FROM big b JOIN small s ON s.big_id = b.id",
		// Many base rows per value, and duplicate values on the small side.
		"SELECT b.id, s.tag FROM big b JOIN small s ON b.grp = s.grp",
		"SELECT b.label, s.tag FROM big b INNER JOIN small s ON b.grp = s.grp WHERE s.tag != 'b'",
		"SELECT COUNT(*) AS n FROM big b JOIN small s ON b.grp = s.grp",
	} {
		semiJoinEnabled = false
		want, err := n.engine.Query(ctx, sql, []string{"app"})
		semiJoinEnabled = true
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		got, err := n.engine.Query(ctx, sql, []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if fmt.Sprint(got.Rows) != fmt.Sprint(want.Rows) {
			t.Fatalf("%s:\n got %v\nwant %v", sql, got.Rows, want.Rows)
		}
		if len(want.Rows) == 0 {
			t.Fatalf("%s: no rows; the test checks nothing", sql)
		}
	}
}
