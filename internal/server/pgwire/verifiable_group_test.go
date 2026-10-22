package pgwire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestVerifiableGroupOverPgwire: the schema reports a group, and an invalid
// one reaches the client as 42P16.
func TestVerifiableGroupOverPgwire(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, sql := range []string{
		"BEGIN DOMAIN lab",
		"CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT) VERIFIABLE",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE",
		"COMMIT",
		"BEGIN DOMAIN lab", "ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)", "COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	var snapshot string
	if err := conn.QueryRow(ctx, "SELECT snapshot FROM asql_admin.schema_snapshot").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(snapshot, `"verifiable_groups":[{"columns":["child_id"],"references":"artifacts","referenced_columns":["id"]}]`) {
		t.Fatalf("schema snapshot: %s", snapshot)
	}

	if _, err := conn.Exec(ctx, "BEGIN DOMAIN lab"); err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (ordinal) REFERENCES artifacts (id)")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42P16" {
		t.Fatalf("a group over an INT column for a TEXT key: %v, want 42P16", err)
	}
	_, _ = conn.Exec(ctx, "ROLLBACK")
}
