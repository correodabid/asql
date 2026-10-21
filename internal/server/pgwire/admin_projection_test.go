package pgwire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"

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
			{"SELECT * FROM asql_admin.timeline_commits", []string{"lsn", "tx_id", "timestamp", "commit_time", "mutation_count"}},
			{"SELECT lsn FROM asql_admin.timeline_commits", []string{"lsn"}},
			{"SELECT mutation_count, LSN FROM asql_admin.timeline_commits", []string{"mutation_count", "lsn"}},
			{`SELECT t.lsn AS commit_lsn, "tx_id" id FROM asql_admin.timeline_commits t`, []string{"commit_lsn", "id"}},
			{"SELECT tx_id, * FROM asql_admin.timeline_commits", []string{"tx_id", "lsn", "tx_id", "timestamp", "commit_time", "mutation_count"}},
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

// TestAdminTablesApplyOrderByAndLimit: ORDER BY, OFFSET and LIMIT apply to
// asql_admin tables; they used to be ignored.
func TestAdminTablesApplyOrderByAndLimit(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	// Commits after the helper's first two (id 1 exists).
	for i := 2; i <= 4; i++ {
		for _, sql := range []string{"BEGIN DOMAIN shop", fmt.Sprintf("INSERT INTO items (id, name) VALUES (%d, 'x')", i), "COMMIT"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}
	lsns := func(sql string) []int64 {
		t.Helper()
		rows, err := conn.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		defer rows.Close()
		var out []int64
		for rows.Next() {
			var lsn int64
			if err := rows.Scan(&lsn); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			out = append(out, lsn)
		}
		return out
	}
	all := lsns("SELECT lsn FROM asql_admin.timeline_commits ORDER BY lsn")
	if len(all) < 3 {
		t.Fatalf("timeline has %d commits", len(all))
	}
	last := all[len(all)-1]
	for sql, want := range map[string][]int64{
		"SELECT lsn FROM asql_admin.timeline_commits ORDER BY lsn DESC LIMIT 1":             {last},
		"SELECT lsn AS l FROM asql_admin.timeline_commits ORDER BY l DESC LIMIT 2 OFFSET 1": {all[len(all)-2], all[len(all)-3]},
		"SELECT lsn FROM asql_admin.timeline_commits ORDER BY lsn LIMIT 1;":                 {all[0]},
	} {
		got := lsns(sql)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: %v, want %v", sql, got, want)
		}
	}
	for sql, code := range map[string]string{
		"SELECT lsn FROM asql_admin.timeline_commits ORDER BY nope":    "42703",
		"SELECT lsn FROM asql_admin.timeline_commits ORDER BY lsn + 1": "0A000",
		"SELECT tx_id FROM asql_admin.timeline_commits GROUP BY tx_id": "0A000",
		"SELECT lsn FROM asql_admin.timeline_commits LIMIT x":          "42601",
	} {
		_, err := conn.Exec(ctx, sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code {
			t.Fatalf("%s: %v, want %s", sql, err, code)
		}
	}
	// An ORDER BY inside the SQL a table takes as a parameter is not its.
	rows, err := conn.Query(ctx, "SELECT * FROM asql_admin.row_history WHERE sql = 'SELECT * FROM shop.items FOR HISTORY ORDER BY id DESC'")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("row_history with ORDER BY in its sql: %v", err)
	}
}

func TestCompareAdminValuesOrdersNullsAsPostgres(t *testing.T) {
	null, one := ast.Literal{Kind: ast.LiteralNull}, ast.Literal{Kind: ast.LiteralNumber, NumberValue: 1}
	if compareAdminValues(null, one, false) <= 0 || compareAdminValues(one, null, false) >= 0 {
		t.Fatal("ascending: NULL must sort last")
	}
	if compareAdminValues(null, one, true) >= 0 || compareAdminValues(one, null, true) <= 0 {
		t.Fatal("descending: NULL must sort first")
	}
	two := ast.Literal{Kind: ast.LiteralNumber, NumberValue: 2}
	if compareAdminValues(one, two, false) >= 0 || compareAdminValues(one, two, true) <= 0 {
		t.Fatal("numbers out of order")
	}
}

// TestAdminTableErrorsReachTheClient: a failing asql_admin query is an
// error, not an empty result. They used to be returned as an "ERROR: ..."
// command tag with no rows.
func TestAdminTableErrorsReachTheClient(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		for _, sql := range []string{
			"SELECT * FROM asql_admin.row_history",
			"SELECT * FROM asql_admin.row_history WHERE sql = 'SELECT * FROM shop.nowhere FOR HISTORY'",
			"SELECT * FROM asql_admin.entity_version_history WHERE domain = 'shop' AND entity = 'nope' AND root_pk = '1'",
		} {
			rows, err := conn.Query(ctx, sql)
			if err == nil {
				for rows.Next() {
				}
				rows.Close()
				err = rows.Err()
			}
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) {
				t.Fatalf("%s: %s: %v, want an error", mode, sql, err)
			}
		}
		_ = conn.Close(ctx)
	}
}
