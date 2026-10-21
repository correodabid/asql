package executor

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestJoinFiltersOnTheJoinedTable: a WHERE on the joined table gives the
// same rows whether the base table has an index on the join column or not.
// With one, the join filtered the joined rows with an evaluator that read
// !=, LIKE, NOT and other conditions as false, and dropped every row.
func TestJoinFiltersOnTheJoinedTable(t *testing.T) {
	ctx := context.Background()
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	n.mustTx(
		"CREATE TABLE indexed (id INT PRIMARY KEY, grp INT)",
		"CREATE INDEX indexed_grp ON indexed (grp) USING HASH",
		"CREATE TABLE plain (id INT PRIMARY KEY, grp INT)",
		"CREATE TABLE small (id INT PRIMARY KEY, grp INT, tag TEXT, score INT)",
	)
	var rows []string
	for i := 1; i <= 60; i++ {
		rows = append(rows, fmt.Sprintf("(%d, %d)", i, i%5))
	}
	n.mustTx("INSERT INTO indexed (id, grp) VALUES "+strings.Join(rows, ", "), "INSERT INTO plain (id, grp) VALUES "+strings.Join(rows, ", "))
	n.mustTx("INSERT INTO small (id, grp, tag, score) VALUES (1, 1, 'a', 10), (2, 1, 'b', 20), (3, 2, NULL, 30), (4, 3, 'ab', NULL)")

	for _, where := range []string{
		"s.tag != 'b'",
		"s.tag <> 'b'",
		"s.tag LIKE 'a%'",
		"NOT (s.tag = 'b')",
		"s.score BETWEEN 15 AND 30",
		"s.tag IS NULL",
		"s.tag = 'a' OR s.score > 25",
	} {
		count := func(table string) int {
			t.Helper()
			sql := fmt.Sprintf("SELECT b.id, s.tag FROM %s b JOIN small s ON b.grp = s.grp WHERE %s", table, where)
			res, err := n.engine.Query(ctx, sql, []string{"app"})
			if err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			return len(res.Rows)
		}
		want := count("plain")
		if want == 0 {
			t.Fatalf("%s: no rows over the table without an index; the case checks nothing", where)
		}
		if got := count("indexed"); got != want {
			t.Fatalf("WHERE %s: %d rows with an index on the join column, %d without", where, got, want)
		}
	}
}
