package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestPostgresTypeNames: columns declared with PostgreSQL type names store
// and return values of the matching ASQL type over both protocols.
func TestPostgresTypeNames(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, sql := range []string{
		"BEGIN DOMAIN shop",
		"CREATE TABLE meta (id INTEGER PRIMARY KEY, n BIGINT, active BOOLEAN NOT NULL, cost REAL, temperature DOUBLE PRECISION, ratio FLOAT8)",
		"INSERT INTO meta (id, n, active, cost, temperature, ratio) VALUES (1, 9000000000, true, 0.25, 0.7, 1.5)",
		"COMMIT",
		"BEGIN DOMAIN shop",
		"ALTER TABLE meta ADD COLUMN score DOUBLE PRECISION",
		"COMMIT",
	} {
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
		var (
			id, n                    int64
			active                   bool
			cost, temperature, ratio float64
		)
		err = c.QueryRow(ctx, "SELECT id, n, active, cost, temperature, ratio FROM shop.meta WHERE active = true").
			Scan(&id, &n, &active, &cost, &temperature, &ratio)
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		if id != 1 || n != 9000000000 || !active || cost != 0.25 || temperature != 0.7 || ratio != 1.5 {
			t.Fatalf("%s: got %d %d %t %v %v %v", mode, id, n, active, cost, temperature, ratio)
		}

		// Parameters of these types round-trip too. Not over the simple
		// protocol: pgx sends its arguments as quoted strings there, and ASQL
		// stores a quoted string in an INT, BOOL or FLOAT column as text.
		if mode == "simple_protocol" {
			continue
		}
		id = 2
		for _, sql := range []string{"BEGIN DOMAIN shop"} {
			if _, err := c.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", mode, err)
			}
		}
		if _, err := c.Exec(ctx, "INSERT INTO meta (id, n, active, cost, temperature, ratio) VALUES ($1, $2, $3, $4, $5, $6)",
			id, int64(7), false, 1.25, 0.125, -2.5); err != nil {
			t.Fatalf("%s: insert with parameters: %v", mode, err)
		}
		if _, err := c.Exec(ctx, "COMMIT"); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		err = c.QueryRow(ctx, "SELECT active, cost, temperature, ratio FROM shop.meta WHERE id = $1", id).
			Scan(&active, &cost, &temperature, &ratio)
		if err != nil || active || cost != 1.25 || temperature != 0.125 || ratio != -2.5 {
			t.Fatalf("%s: parameters read back as %t %v %v %v, err %v", mode, active, cost, temperature, ratio, err)
		}
	}
}
