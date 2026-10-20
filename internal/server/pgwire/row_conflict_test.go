package pgwire

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestWorkersClaimingDifferentJobsDoNotConflict: the optimistic job-claim
// pattern over pgx. Claims of different jobs both commit; the later claim of
// the same job gets 40001.
func TestWorkersClaimingDifferentJobsDoNotConflict(t *testing.T) {
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
	exec := func(conn *pgx.Conn, sqls ...string) error {
		for _, sql := range sqls {
			if _, err := conn.Exec(ctx, sql); err != nil {
				return err
			}
		}
		return nil
	}
	setup := connect()
	if err := exec(setup, "BEGIN DOMAIN shop",
		"CREATE TABLE jobs (id INT PRIMARY KEY, state TEXT, owner TEXT)", "COMMIT",
		"BEGIN DOMAIN shop",
		"INSERT INTO jobs (id, state, owner) VALUES (1, 'pending', '')",
		"INSERT INTO jobs (id, state, owner) VALUES (2, 'pending', '')", "COMMIT"); err != nil {
		t.Fatal(err)
	}
	claim := func(conn *pgx.Conn, id, owner string) error {
		return exec(conn, "BEGIN DOMAIN shop",
			"UPDATE jobs SET state = 'running', owner = '"+owner+"' WHERE id = "+id+" AND state = 'pending'")
	}

	a, b, c := connect(), connect(), connect()
	for _, step := range []error{claim(a, "1", "a"), claim(b, "2", "b"), claim(c, "1", "c")} {
		if step != nil {
			t.Fatal(step)
		}
	}
	if err := exec(a, "COMMIT"); err != nil {
		t.Fatalf("claim of job 1: %v", err)
	}
	if err := exec(b, "COMMIT"); err != nil {
		t.Fatalf("claim of job 2 after job 1 committed: %v", err)
	}
	var pgErr *pgconn.PgError
	if err := exec(c, "COMMIT"); !errors.As(err, &pgErr) || pgErr.Code != "40001" {
		t.Fatalf("second claim of job 1: %v, want SQLSTATE 40001", err)
	}
}
