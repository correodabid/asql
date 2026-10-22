package pgwire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestRowHelpersOnTablesWithoutASingleColumnKey: the helpers that find a row
// by a single-column primary key fail with 0A000 on a table with a composite
// key or none, so a client can tell that from a missing row, which stays
// NULL.
func TestRowHelpersOnTablesWithoutASingleColumnKey(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, sql := range []string{"BEGIN DOMAIN shop",
		"CREATE TABLE single (id INT PRIMARY KEY, v TEXT)",
		"CREATE TABLE lines (order_id INT, line_no INT, v TEXT, PRIMARY KEY (order_id, line_no))",
		"CREATE TABLE loose (a INT, b TEXT)",
		"CREATE ENTITY line_agg (ROOT lines)",
		"COMMIT",
		"BEGIN DOMAIN shop",
		"INSERT INTO single (id, v) VALUES (1, 'a')",
		"INSERT INTO lines (order_id, line_no, v) VALUES (1, 2, 'x')",
		"INSERT INTO loose (a, b) VALUES (1, 'y')",
		"COMMIT"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	scalar := func(sql string) (*int64, error) {
		var v *int64
		err := conn.QueryRow(ctx, sql).Scan(&v)
		return v, err
	}
	if v, err := scalar("SELECT row_lsn('shop.single', '1')"); err != nil || v == nil {
		t.Fatalf("an existing row: %v, %v", v, err)
	}
	if v, err := scalar("SELECT row_lsn('shop.single', '999')"); err != nil || v != nil {
		t.Fatalf("a missing row: %v, %v; want NULL", v, err)
	}
	for sql, want := range map[string]string{
		"SELECT row_lsn('shop.lines', '1,2')":                   "composite primary key (order_id, line_no)",
		"SELECT resolve_reference('shop.lines', '1')":           "composite primary key",
		"SELECT row_lsn('shop.loose', '1')":                     "has no primary key",
		"SELECT entity_version('shop', 'line_agg', '1')":        "composite primary key",
		"SELECT entity_head_lsn('shop', 'line_agg', '1')":       "composite primary key",
		"SELECT entity_version_lsn('shop', 'line_agg', '1', 1)": "composite primary key",
	} {
		_, err := scalar(sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "0A000" || !strings.Contains(pgErr.Message, want) {
			t.Fatalf("%s: %v, want 0A000 with %q", sql, err, want)
		}
	}
	// Unknown tables and entities have no rows: still NULL.
	for _, sql := range []string{"SELECT row_lsn('shop.nope', '1')", "SELECT entity_version('shop', 'nope', '1')"} {
		if v, err := scalar(sql); err != nil || v != nil {
			t.Fatalf("%s: %v, %v; want NULL", sql, v, err)
		}
	}
}
