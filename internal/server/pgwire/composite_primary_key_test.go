package pgwire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestCompositePrimaryKeyOverPgwire: a repeated composite key reaches clients
// as SQLSTATE 23505, and the schema lists the key in key order.
func TestCompositePrimaryKeyOverPgwire(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, sql := range []string{
		"BEGIN DOMAIN lab", "CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT) APPEND ONLY", "COMMIT",
		"BEGIN DOMAIN lab", "INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c1', 'p1', 1), ('c1', 'p2', 2)", "COMMIT",
		"BEGIN DOMAIN lab", "ALTER TABLE artifact_parents ADD PRIMARY KEY (child_id, parent_id)", "COMMIT",
		"BEGIN DOMAIN lab", "INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c1', 'p1', 1) ON CONFLICT (child_id, parent_id) DO NOTHING", "COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	if _, err := conn.Exec(ctx, "BEGIN DOMAIN lab"); err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c1', 'p2', 3)")
	if _, cerr := conn.Exec(ctx, "COMMIT"); err == nil {
		err = cerr
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatalf("repeated composite key: %v, want SQLSTATE 23505", err)
	}
	_, _ = conn.Exec(ctx, "ROLLBACK")

	var snapshot string
	if err := conn.QueryRow(ctx, "SELECT snapshot FROM asql_admin.schema_snapshot").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(snapshot, `"primary_key":["child_id","parent_id"]`) {
		t.Fatalf("schema snapshot does not list the key: %s", snapshot)
	}
}
