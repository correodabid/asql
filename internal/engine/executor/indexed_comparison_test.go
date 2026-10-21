package executor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// TestIndexedColumnsComparedWithNonLiterals (ASQL-12): an indexed column
// compared with a subquery or another column gives the rows the same query
// gives without the index. Read through the index, such a comparison looked
// up an empty value and matched nothing, in SELECT, UPDATE and DELETE.
func TestIndexedColumnsComparedWithNonLiterals(t *testing.T) {
	ctx := context.Background()
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	for _, table := range []string{"indexed", "plain"} {
		n.mustTx(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, question_id TEXT, alt TEXT, score INT)", table))
		n.mustTx(fmt.Sprintf("INSERT INTO %s (id, question_id, alt, score) VALUES ('a', 'q1', 'x', 10), ('b', 'q2', 'b', 20), ('c', 'q1', 'c', 30)", table))
	}
	n.mustTx("CREATE INDEX indexed_q ON indexed (question_id) USING HASH", "CREATE INDEX indexed_score ON indexed (score) USING BTREE")
	n.mustTx("CREATE TABLE refs (id INT PRIMARY KEY, target TEXT)")
	n.mustTx("INSERT INTO refs (id, target) VALUES (1, 'b'), (2, 'q1')")

	query := func(sql string) []int64 {
		t.Helper()
		res, err := n.engine.Query(ctx, sql, []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return []int64{int64(len(res.Rows))}
	}
	// id is the primary key, indexed in both tables.
	for where, want := range map[string]int64{
		"id = (SELECT target FROM refs WHERE id = 1)":               1,
		"question_id = (SELECT target FROM refs WHERE id = 2)":      2,
		"question_id = (SELECT question_id FROM %s WHERE id = 'a')": 2,
		"score < (SELECT score FROM %s WHERE id = 'c')":             2,
		"id = alt":           2,
		"question_id != alt": 3,
	} {
		for _, table := range []string{"plain", "indexed"} {
			sql := fmt.Sprintf("SELECT id FROM %s WHERE "+where, append([]any{table}, repeat(table, countVerbs(where))...)...)
			if got := query(sql)[0]; got != want {
				t.Fatalf("%s: %d rows, want %d", sql, got, want)
			}
		}
	}

	if got := query("SELECT id FROM indexed WHERE question_id = (SELECT target FROM refs WHERE id = 99)")[0]; got != 0 {
		t.Fatalf("subquery with no row: %d rows, want 0", got)
	}
	_, err := n.engine.Query(ctx, "SELECT id FROM indexed WHERE question_id = (SELECT target FROM refs)", []string{"app"})
	var sqlErr *sqlerr.SQLError
	if !errors.As(err, &sqlErr) || sqlErr.Code != "21000" {
		t.Fatalf("subquery with two rows: %v, want 21000", err)
	}

	// Writes find their rows through the index too.
	n.mustTx("UPDATE indexed SET score = 99 WHERE question_id = (SELECT target FROM refs WHERE id = 2)")
	if got := query("SELECT id FROM indexed WHERE score = 99")[0]; got != 2 {
		t.Fatalf("UPDATE through the index changed %d rows, want 2", got)
	}
	n.mustTx("DELETE FROM indexed WHERE id = alt")
	if got := query("SELECT id FROM indexed")[0]; got != 1 {
		t.Fatalf("DELETE through the index left %d rows, want 1", got)
	}
}

func repeat(s string, n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = s
	}
	return out
}

// countVerbs counts the %s in where, after the first, which names the table.
func countVerbs(where string) int {
	n := 0
	for i := 0; i+1 < len(where); i++ {
		if where[i] == '%' && where[i+1] == 's' {
			n++
		}
	}
	return n
}
