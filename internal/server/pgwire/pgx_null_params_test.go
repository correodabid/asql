package pgwire

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestPgxNullParametersAndPing: details of pgx over the extended protocol
// that an integration hit (ASQL-8 and others).
func TestPgxNullParametersAndPing(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	// pgx's Ping sends a query that is only a comment.
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	for _, sql := range []string{"BEGIN DOMAIN lab", "CREATE TABLE runs (id INT PRIMARY KEY, cost FLOAT, label TEXT, score INT)", "COMMIT", "BEGIN DOMAIN lab"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	var noCost *float64
	var noLabel *string
	// A NULL parameter for a FLOAT column, and one for TEXT.
	if _, err := conn.Exec(ctx, "INSERT INTO runs (id, cost, label, score) VALUES ($1, $2, $3, $4)", 1, noCost, noLabel, 7); err != nil {
		t.Fatalf("NULL parameters: %v", err)
	}
	// A literal NULL before a placeholder: $2 is typed by its own column.
	if _, err := conn.Exec(ctx, "INSERT INTO runs (id, cost, label, score) VALUES ($1, NULL, 'x', $2)", 2, 9); err != nil {
		t.Fatalf("literal NULL before a placeholder: %v", err)
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		t.Fatal(err)
	}

	var cost *float64
	var label *string
	var score int64
	if err := conn.QueryRow(ctx, "SELECT cost, label, score FROM lab.runs WHERE id = 1").Scan(&cost, &label, &score); err != nil {
		t.Fatal(err)
	}
	if cost != nil || label != nil || score != 7 {
		t.Fatalf("row 1: cost %v, label %v, score %d; want NULL, NULL, 7", cost, label, score)
	}
	if err := conn.QueryRow(ctx, "SELECT score FROM lab.runs WHERE id = 2").Scan(&score); err != nil || score != 9 {
		t.Fatalf("row 2: score %d, err %v; want 9", score, err)
	}

	// MAX over a history column comes typed, not as text.
	var maxLSN int64
	if err := conn.QueryRow(ctx, "SELECT MAX(__commit_lsn) FROM lab.runs FOR HISTORY WHERE id = 1").Scan(&maxLSN); err != nil || maxLSN == 0 {
		t.Fatalf("MAX(__commit_lsn): %d, err %v", maxLSN, err)
	}
}
