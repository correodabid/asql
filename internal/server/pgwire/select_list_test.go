package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestSelectListAliasesAndExpressions: over both protocols, a column is
// named by its alias and expressions are computed. Over the simple protocol
// "SELECT id AS x" returned a column named "id as x" holding NULL.
func TestSelectListAliasesAndExpressions(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		if mode == "simple_protocol" {
			for _, sql := range []string{"BEGIN DOMAIN lab", "CREATE TABLE t (id INT PRIMARY KEY, qty INT)", "COMMIT",
				"BEGIN DOMAIN lab", "INSERT INTO t (id, qty) VALUES (1, 5)", "COMMIT"} {
				if _, err := conn.Exec(ctx, sql); err != nil {
					t.Fatalf("%s: %v", sql, err)
				}
			}
		}
		rows, err := conn.Query(ctx, "SELECT id AS x, qty + 1 AS q, 'lit' AS l FROM lab.t")
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		var names []string
		for _, f := range rows.FieldDescriptions() {
			names = append(names, f.Name)
		}
		var x, q int64
		var l string
		if !rows.Next() {
			t.Fatalf("%s: no row: %v", mode, rows.Err())
		}
		if err := rows.Scan(&x, &q, &l); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		rows.Close()
		if len(names) != 3 || names[0] != "x" || names[1] != "q" || names[2] != "l" || x != 1 || q != 6 || l != "lit" {
			t.Fatalf("%s: columns %v, values %d %d %q; want x q l, 1 6 lit", mode, names, x, q, l)
		}
		_ = conn.Close(ctx)
	}
}
