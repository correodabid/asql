package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestHistoryAggregatesOverPgwire: aggregates and projections over FOR
// HISTORY reach clients with the right columns and values.
func TestHistoryAggregatesOverPgwire(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, sql := range []string{"BEGIN DOMAIN shop", "UPDATE items SET name = 'b' WHERE id = 1", "COMMIT"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		c, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close(context.Background()) })
		var versions int64
		if err := c.QueryRow(ctx, "SELECT COUNT(*) FROM shop.items FOR HISTORY WHERE id = 1").Scan(&versions); err != nil || versions != 2 {
			t.Fatalf("%s: count = %d, err %v; want 2", mode, versions, err)
		}
		var name, operation string
		if err := c.QueryRow(ctx, "SELECT name, __operation FROM shop.items FOR HISTORY WHERE id = 1 ORDER BY __commit_lsn DESC LIMIT 1").Scan(&name, &operation); err != nil || name != "b" || operation != "UPDATE" {
			t.Fatalf("%s: latest version = %q %q, err %v", mode, name, operation, err)
		}
	}
}

// TestAggregatesScanIntoIntegers: over the extended protocol, aggregates are
// described with the type the engine computes, so pgx scans them into Go
// integers instead of failing on a text column.
func TestAggregatesScanIntoIntegers(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, sql := range []string{"BEGIN DOMAIN shop", "INSERT INTO items (id, name) VALUES (5, 'e')", "COMMIT"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	for sql, want := range map[string]int64{
		"SELECT COUNT(*) FROM shop.items":                  2,
		"SELECT COUNT(DISTINCT name) AS n FROM shop.items": 2,
		"SELECT SUM(id) FROM shop.items":                   6,
		"SELECT MAX(id) AS top FROM shop.items":            5,
		"SELECT MIN(id) FROM shop.items":                   1,
		"SELECT AVG(id) FROM shop.items":                   3,
	} {
		var got int64
		if err := conn.QueryRow(ctx, sql).Scan(&got); err != nil || got != want {
			t.Fatalf("%s = %d, err %v; want %d", sql, got, err, want)
		}
	}
}
