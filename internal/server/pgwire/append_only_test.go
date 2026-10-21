package pgwire

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestAppendOnlyOverPgwire: violations reach clients as SQLSTATE 42501 on the
// statement itself, and the schema reports the table as append-only.
func TestAppendOnlyOverPgwire(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, sql := range []string{
		"BEGIN DOMAIN shop", "ALTER TABLE items SET APPEND ONLY", "COMMIT",
		"BEGIN DOMAIN shop", "INSERT INTO items (id, name) VALUES (2, 'b')", "COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	if _, err := conn.Exec(ctx, "BEGIN DOMAIN shop"); err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(ctx, "UPDATE items SET name = 'forged' WHERE id = 1")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("UPDATE on an append-only table: %v, want SQLSTATE 42501", err)
	}
	if _, err := conn.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatal(err)
	}

	var snapshot string
	if err := conn.QueryRow(ctx, "SELECT snapshot FROM asql_admin.schema_snapshot").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(snapshot, `"append_only":true`) {
		t.Fatalf("schema snapshot does not mark the table append-only: %s", snapshot)
	}
}

// TestVerifiableOverPgwire: SET VERIFIABLE restricts nothing, and the schema
// reports it.
func TestVerifiableOverPgwire(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	for _, sql := range []string{
		"BEGIN DOMAIN shop", "ALTER TABLE items SET VERIFIABLE", "COMMIT",
		"BEGIN DOMAIN shop", "UPDATE items SET name = 'renamed' WHERE id = 1", "COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	var snapshot string
	if err := conn.QueryRow(ctx, "SELECT snapshot FROM asql_admin.schema_snapshot").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(snapshot, `"verifiable":true`) || strings.Contains(snapshot, `"append_only":true`) {
		t.Fatalf("schema snapshot: %s", snapshot)
	}
}
