package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// TestSubqueriesInWhere: IN, NOT IN, EXISTS and scalar subqueries give the
// same answers whether they are run once per query (uncorrelated), inlined
// as a list of values, or per row (correlated); and they report their
// errors instead of reading as false.
func TestSubqueriesInWhere(t *testing.T) {
	ctx := context.Background()
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	n.mustTx(
		"CREATE TABLE items (id INT PRIMARY KEY, kind TEXT, owner INT)",
		"CREATE TABLE picks (id INT PRIMARY KEY, item_id INT, note TEXT)",
	)
	n.mustTx(
		"INSERT INTO items (id, kind, owner) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', NULL), (4, 'c', 10)",
		"INSERT INTO picks (id, item_id, note) VALUES (1, 1, 'x'), (2, 3, 'y'), (3, NULL, 'z')",
	)
	query := func(sql string) []int64 {
		t.Helper()
		res, err := n.engine.Query(ctx, sql, []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return sortedIDs(res.Rows)
	}
	cases := []struct {
		sql  string
		want []int64
	}{
		// Inlined as IN (1, 3).
		{"SELECT id FROM items WHERE id IN (SELECT item_id FROM picks WHERE note < 'z')", []int64{1, 3}},
		// The NULL item_id: IN stays per row (cached), NOT IN is never true.
		{"SELECT id FROM items WHERE id IN (SELECT item_id FROM picks)", []int64{1, 3}},
		{"SELECT id FROM items WHERE id NOT IN (SELECT item_id FROM picks)", nil},
		{"SELECT id FROM items WHERE id NOT IN (SELECT item_id FROM picks WHERE note < 'z')", []int64{2, 4}},
		{"SELECT id FROM items WHERE NOT (id IN (SELECT item_id FROM picks WHERE note < 'z'))", []int64{2, 4}},
		// The select list names the column, also qualified.
		{"SELECT id FROM items WHERE id IN (SELECT p.item_id FROM picks p WHERE p.note = 'x')", []int64{1}},
		// Uncorrelated EXISTS and scalar subqueries.
		{"SELECT id FROM items WHERE EXISTS (SELECT id FROM picks WHERE note = 'y')", []int64{1, 2, 3, 4}},
		{"SELECT id FROM items WHERE owner = (SELECT owner FROM items WHERE id = 2)", []int64{2}},
		// Correlated: per row.
		{"SELECT id FROM items WHERE EXISTS (SELECT id FROM picks WHERE item_id = items.id)", []int64{1, 3}},
		{"SELECT id FROM items WHERE id IN (SELECT item_id FROM picks WHERE item_id = items.id AND note = 'y')", []int64{3}},
	}
	for _, c := range cases {
		got := query(c.sql)
		if len(got) != len(c.want) {
			t.Fatalf("%s: %v, want %v", c.sql, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("%s: %v, want %v", c.sql, got, c.want)
			}
		}
	}

	for sql, code := range map[string]string{
		"SELECT id FROM items WHERE owner = (SELECT owner FROM items WHERE kind = 'a')": "21000",
		"SELECT id FROM items WHERE id IN (SELECT id, kind FROM items)":                 "42601",
		"SELECT id FROM items WHERE id IN (SELECT id FROM nowhere)":                     "42P01",
	} {
		_, err := n.engine.Query(ctx, sql, []string{"app"})
		var sqlErr *sqlerr.SQLError
		if !errors.As(err, &sqlErr) || sqlErr.Code != code {
			t.Fatalf("%s: %v, want %s", sql, err, code)
		}
	}
}
