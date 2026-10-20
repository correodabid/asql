package pgwire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestSimpleProtocolArgumentsTakeTheColumnType: pgx over the simple protocol
// sends every argument as a quoted string. Like PostgreSQL, ASQL stores and
// compares them as the type of the column they meet.
func TestSimpleProtocolArgumentsTakeTheColumnType(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	connect := func(mode string) *pgx.Conn {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		return conn
	}
	simple := connect("simple_protocol")
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := simple.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s %v: %v", sql, args, err)
		}
	}
	at := time.Date(2024, 3, 15, 10, 30, 45, 123456000, time.UTC)
	later := at.Add(36 * time.Hour)

	exec("BEGIN DOMAIN shop")
	exec("CREATE TABLE jobs (id INT PRIMARY KEY, attempts INT, ready BOOL, weight FLOAT, run_at TIMESTAMP, note TEXT)")
	exec("INSERT INTO jobs (id, attempts, ready, weight, run_at, note) VALUES ($1, $2, $3, $4, $5, $6)", 1, 0, true, 0.5, at, "7")
	exec("INSERT INTO jobs (id, attempts, ready, weight, run_at, note) VALUES ($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12)",
		2, 0, false, 1.5, at, "two", 3, 0, true, 2.5, later, "three")
	exec("COMMIT")
	exec("BEGIN DOMAIN shop")
	exec("UPDATE jobs SET attempts = attempts + $1, run_at = $2 WHERE id = $3", 1, later, 1)
	// BETWEEN and LIKE keep the statement as SQL text in the WAL.
	exec("UPDATE jobs SET weight = $1 WHERE id BETWEEN $2 AND $3 AND note LIKE $4", 9.75, 2, 3, "t%")
	exec("INSERT INTO jobs (id, attempts, ready) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET attempts = $4", 3, 0, false, 5)
	exec("DELETE FROM jobs WHERE ready = $1 AND id IN ($2, $3)", false, 2, 4)
	exec("COMMIT")

	type job struct {
		attempts int64
		ready    bool
		weight   float64
		runAt    time.Time
		note     string
	}
	want := map[int64]job{
		1: {1, true, 0.5, later, "7"},
		3: {5, true, 9.75, later, "three"},
	}
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn := simple
		if mode != "simple_protocol" {
			conn = connect(mode)
		}
		for id, w := range want {
			var got job
			err := conn.QueryRow(ctx, "SELECT attempts, ready, weight, run_at, note FROM shop.jobs WHERE id = $1 AND run_at >= $2", id, at).
				Scan(&got.attempts, &got.ready, &got.weight, &got.runAt, &got.note)
			if err != nil {
				t.Fatalf("%s: job %d: %v", mode, id, err)
			}
			if got.attempts != w.attempts || got.ready != w.ready || got.weight != w.weight || !got.runAt.Equal(w.runAt) || got.note != w.note {
				t.Fatalf("%s: job %d is %+v, want %+v", mode, id, got, w)
			}
		}
		var count int64
		if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM shop.jobs WHERE id IN ($1, $2)", 1, 2).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s: count = %d, err %v; want 1", mode, count, err)
		}
	}

	var pgErr *pgconn.PgError
	exec("BEGIN DOMAIN shop")
	if _, err := simple.Exec(ctx, "INSERT INTO jobs (id, attempts) VALUES ($1, $2)", 5, "many"); !errors.As(err, &pgErr) || pgErr.Code != "22P02" {
		t.Fatalf("text in an INT column: %v, want 22P02", err)
	}
	exec("ROLLBACK")
	if _, err := simple.Exec(ctx, "SELECT note FROM shop.jobs WHERE run_at = $1", "soon"); !errors.As(err, &pgErr) || pgErr.Code != "22007" {
		t.Fatalf("text compared with a TIMESTAMP column: %v, want 22007", err)
	}
}
