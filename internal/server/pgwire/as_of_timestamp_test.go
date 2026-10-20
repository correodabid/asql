package pgwire

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestAsOfTimestampResolvesByCommitTime (ASQL-9): AS OF TIMESTAMP answers
// with the state at that date, by the wall-clock time each commit records.
// It used to compare the date with the WAL's logical counter, so any real
// date returned the present.
func TestAsOfTimestampResolvesByCommitTime(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	exec := func(sqls ...string) {
		t.Helper()
		for _, sql := range sqls {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}
	exec("BEGIN DOMAIN lab", "CREATE TABLE artifacts (id INT PRIMARY KEY)", "COMMIT",
		"BEGIN DOMAIN lab", "INSERT INTO artifacts (id) VALUES (1)", "COMMIT")
	time.Sleep(5 * time.Millisecond)
	between := time.Now().UTC()
	time.Sleep(5 * time.Millisecond)
	exec("BEGIN DOMAIN lab", "INSERT INTO artifacts (id) VALUES (2)", "COMMIT")

	count := func(date string) (int64, error) {
		var n int64
		err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM lab.artifacts AS OF TIMESTAMP '"+date+"'").Scan(&n)
		return n, err
	}
	// asql_admin.timeline_commits shows when each commit happened.
	var lastCommit time.Time
	rows, err := conn.Query(ctx, "SELECT lsn, commit_time FROM asql_admin.timeline_commits")
	if err != nil {
		t.Fatal(err)
	}
	var lastLSN int64
	for rows.Next() {
		var lsn int64
		var commitTime time.Time
		if err := rows.Scan(&lsn, &commitTime); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if lsn > lastLSN {
			lastLSN, lastCommit = lsn, commitTime
		}
	}
	rows.Close()
	if !lastCommit.After(between) || lastCommit.After(time.Now()) {
		t.Fatalf("last commit at %s, want between %s and now", lastCommit, between)
	}

	if _, err := count("2000-01-01 00:00:00"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("as of 2000: %v, want the table not to exist yet", err)
	}
	for date, want := range map[string]int64{
		between.Format("2006-01-02 15:04:05.999999"):         1,
		between.Format(time.RFC3339Nano):                     1,
		time.Now().UTC().Add(time.Hour).Format(time.RFC3339): 2,
	} {
		if n, err := count(date); err != nil || n != want {
			t.Fatalf("as of %s: %d artifacts, err %v; want %d", date, n, err, want)
		}
	}
}
