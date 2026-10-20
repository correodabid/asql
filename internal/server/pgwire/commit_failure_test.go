package pgwire

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestFailedCommitEndsTheTransaction: as in PostgreSQL, a COMMIT that fails
// (write conflict, unique violation) leaves the connection idle, so a pooled
// connection is safe to reuse without an explicit ROLLBACK.
func TestFailedCommitEndsTheTransaction(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	connect := func() *pgx.Conn {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		return conn
	}
	exec := func(conn *pgx.Conn, sqls ...string) {
		t.Helper()
		for _, sql := range sqls {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("exec %q: %v", sql, err)
			}
		}
	}
	requireFailedCommitThenIdle := func(conn *pgx.Conn, code string) {
		t.Helper()
		_, err := conn.Exec(ctx, "COMMIT")
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code {
			t.Fatalf("COMMIT: error %v, want SQLSTATE %s", err, code)
		}
		if status := conn.PgConn().TxStatus(); status != 'I' {
			t.Fatalf("transaction status after a failed COMMIT = %q, want idle", status)
		}
		exec(conn, "BEGIN DOMAIN shop", "ROLLBACK")
	}

	a, b := connect(), connect()
	exec(a, "BEGIN DOMAIN shop", "UPDATE items SET name = 'from-a' WHERE id = 1")
	exec(b, "BEGIN DOMAIN shop", "UPDATE items SET name = 'from-b' WHERE id = 1", "COMMIT")
	requireFailedCommitThenIdle(a, "40001")

	exec(a, "BEGIN DOMAIN shop", "INSERT INTO items (id, name) VALUES (1, 'duplicate')")
	requireFailedCommitThenIdle(a, "23505")
}
