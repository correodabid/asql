package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// TestSelectListExpressions: literals and arithmetic in the select list are
// computed, and an item that cannot be is an error. They used to be left
// out of the result without one.
func TestSelectListExpressions(t *testing.T) {
	ctx := context.Background()
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	n.mustTx("CREATE TABLE items (id INT PRIMARY KEY, qty INT, name TEXT)", "CREATE TABLE tags (item_id INT, tag TEXT, id INT PRIMARY KEY)")
	n.mustTx("INSERT INTO items (id, qty, name) VALUES (1, 5, 'a')", "INSERT INTO tags (id, item_id, tag) VALUES (1, 1, 'red')")
	n.mustTx("ALTER TABLE items ADD COLUMN note TEXT")

	row := func(sql string) map[string]ast.Literal {
		t.Helper()
		res, err := n.engine.Query(ctx, sql, []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("%s: %d rows", sql, len(res.Rows))
		}
		return res.Rows[0]
	}
	got := row("SELECT id AS x, qty + 1 AS q, qty * 2, 7 AS seven, 'lit' AS l, note FROM items")
	want := map[string]ast.Literal{
		"x":       {Kind: ast.LiteralNumber, NumberValue: 1},
		"q":       {Kind: ast.LiteralNumber, NumberValue: 6},
		"qty * 2": {Kind: ast.LiteralNumber, NumberValue: 10},
		"seven":   {Kind: ast.LiteralNumber, NumberValue: 7},
		"l":       {Kind: ast.LiteralString, StringValue: "lit"},
		// Added after the row was written.
		"note": {Kind: ast.LiteralNull},
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("%s = %+v, want %+v (row %v)", key, got[key], value, got)
		}
	}
	if got := row("SELECT i.id, t.tag, i.qty + 10 AS q FROM items i JOIN tags t ON t.item_id = i.id"); got["q"].NumberValue != 15 {
		t.Fatalf("join with an expression: %v", got)
	}

	for sql, code := range map[string]string{
		"SELECT id, nope FROM items":           "42703",
		"SELECT i.nope FROM items i":           "42703",
		"SELECT id, md5(name) AS h FROM items": "0A000",
	} {
		_, err := n.engine.Query(ctx, sql, []string{"app"})
		var sqlErr *sqlerr.SQLError
		if !errors.As(err, &sqlErr) || sqlErr.Code != code {
			t.Fatalf("%s: %v, want %s", sql, err, code)
		}
	}
}
