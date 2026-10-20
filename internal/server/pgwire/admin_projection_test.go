package pgwire

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// startProjectionTestServer serves a data directory with two commits and
// returns its address.
func startProjectionTestServer(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      slog.New(slog.NewTextHandler(os.Stdout, nil)),
	})
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ServeOnListener(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("serve pgwire: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("timeout waiting for pgwire server shutdown")
		}
	})

	conn, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, sql := range []string{
		"BEGIN DOMAIN shop",
		"CREATE TABLE items (id INT PRIMARY KEY, name TEXT)",
		"COMMIT",
		"BEGIN DOMAIN shop",
		"INSERT INTO items (id, name) VALUES (1, 'a')",
		"COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	return listener.Addr().String()
}

// TestAdminTablesApplyTheSelectList: asql_admin tables return the columns the
// query names, in its order and under its aliases, over both protocols.
func TestAdminTablesApplyTheSelectList(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })

		for _, tc := range []struct {
			sql     string
			columns []string
		}{
			{"SELECT * FROM asql_admin.timeline_commits", []string{"lsn", "tx_id", "timestamp", "mutation_count"}},
			{"SELECT lsn FROM asql_admin.timeline_commits", []string{"lsn"}},
			{"SELECT mutation_count, LSN FROM asql_admin.timeline_commits", []string{"mutation_count", "lsn"}},
			{`SELECT t.lsn AS commit_lsn, "tx_id" id FROM asql_admin.timeline_commits t`, []string{"commit_lsn", "id"}},
			{"SELECT tx_id, * FROM asql_admin.timeline_commits", []string{"tx_id", "lsn", "tx_id", "timestamp", "mutation_count"}},
		} {
			rows, err := conn.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("%s (%s): %v", tc.sql, mode, err)
			}
			defer rows.Close()
			var columns []string
			for _, f := range rows.FieldDescriptions() {
				columns = append(columns, f.Name)
			}
			n := 0
			for rows.Next() {
				values, err := rows.Values()
				if err != nil {
					t.Fatalf("%s (%s): %v", tc.sql, mode, err)
				}
				if len(values) != len(tc.columns) {
					t.Fatalf("%s (%s): row has %d values, want %d", tc.sql, mode, len(values), len(tc.columns))
				}
				n++
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("%s (%s): %v", tc.sql, mode, err)
			}
			if !reflect.DeepEqual(columns, tc.columns) || n != 2 {
				t.Fatalf("%s (%s): columns %v and %d rows, want %v and 2 rows", tc.sql, mode, columns, n, tc.columns)
			}
		}

		var lsn int64
		var txID string
		if err := conn.QueryRow(ctx, "SELECT tx_id, lsn FROM asql_admin.timeline_commits").Scan(&txID, &lsn); err != nil || txID == "" || lsn == 0 {
			t.Fatalf("scan projected row (%s): tx_id=%q lsn=%d err=%v", mode, txID, lsn, err)
		}

		for sql, code := range map[string]string{
			"SELECT missing FROM asql_admin.timeline_commits":  "42703",
			"SELECT count(*) FROM asql_admin.timeline_commits": "0A000",
		} {
			_, err := conn.Exec(ctx, sql)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != code {
				t.Fatalf("%s (%s): error %v, want SQLSTATE %s", sql, mode, err, code)
			}
		}
		// The connection is still usable after a rejected query.
		if _, err := conn.Exec(ctx, "SELECT * FROM asql_admin.engine_stats"); err != nil {
			t.Fatalf("query after an error (%s): %v", mode, err)
		}
	}
}

// The multi-line select lists in the getting-started guides project too.
func TestAdminProjectionAcceptsMultiLineSelectLists(t *testing.T) {
	ir := interceptResult{columns: []string{"total_commits", "total_begins", "total_cross_domain_begins"}}
	got := projectAdminColumns("SELECT\n\ttotal_begins,\n\ttotal_cross_domain_begins\nFROM asql_admin.engine_stats;", ir)
	if got.err != nil || !reflect.DeepEqual(got.columns, []string{"total_begins", "total_cross_domain_begins"}) {
		t.Fatalf("columns %v, err %v", got.columns, got.err)
	}
}
