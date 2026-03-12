package pgwire

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPGWireSimpleQueryRoundtrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: walPath,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for test: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeOnListener(ctx, listener)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("pgwire server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire server shutdown")
		}
	})

	connectionString := "postgres://asql@" + listener.Addr().String() + "/asql?sslmode=disable&default_query_exec_mode=simple_protocol"
	connection, err := pgx.Connect(ctx, connectionString)
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close(ctx) })

	if _, err := connection.Exec(ctx, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("begin domain: %v", err)
	}
	if _, err := connection.Exec(ctx, "CREATE TABLE users (id INT, email TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := connection.Exec(ctx, "INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')"); err != nil {
		t.Fatalf("insert row 1: %v", err)
	}
	if _, err := connection.Exec(ctx, "INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')"); err != nil {
		t.Fatalf("insert row 2: %v", err)
	}
	if _, err := connection.Exec(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit: %v", err)
	}

	rows, err := connection.Query(ctx, "SELECT id, email FROM accounts.users ORDER BY id ASC")
	if err != nil {
		t.Fatalf("query rows: %v", err)
	}
	defer rows.Close()

	type pair struct {
		id    int64
		email string
	}
	actual := make([]pair, 0, 2)
	for rows.Next() {
		var got pair
		if err := rows.Scan(&got.id, &got.email); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		actual = append(actual, got)
	}
	if rows.Err() != nil {
		t.Fatalf("iterate rows: %v", rows.Err())
	}

	if len(actual) != 2 {
		t.Fatalf("unexpected row count: got %d want 2", len(actual))
	}
	if actual[0].id != 1 || actual[0].email != "one@asql.dev" {
		t.Fatalf("unexpected first row: %+v", actual[0])
	}
	if actual[1].id != 2 || actual[1].email != "two@asql.dev" {
		t.Fatalf("unexpected second row: %+v", actual[1])
	}
}

func TestPGWireCompatibilityDeterministicAcrossRepeatedRuns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: walPath,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for test: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeOnListener(ctx, listener)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("pgwire server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire server shutdown")
		}
	})

	connectionString := "postgres://asql@" + listener.Addr().String() + "/asql?sslmode=disable&default_query_exec_mode=simple_protocol"
	connection, err := pgx.Connect(ctx, connectionString)
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close(ctx) })

	if _, err := connection.Exec(ctx, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("begin domain: %v", err)
	}
	if _, err := connection.Exec(ctx, "CREATE TABLE users (id INT, email TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 1; i <= 5; i++ {
		query := fmt.Sprintf("INSERT INTO users (id, email) VALUES (%d, 'user-%d@asql.dev')", i, i)
		if _, err := connection.Exec(ctx, query); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
	if _, err := connection.Exec(ctx, "COMMIT"); err != nil {
		t.Fatalf("commit: %v", err)
	}

	baseline := ""
	for i := 0; i < 16; i++ {
		rows, err := connection.Query(ctx, "SELECT id, email FROM accounts.users ORDER BY id ASC")
		if err != nil {
			t.Fatalf("query rows: %v", err)
		}

		serialized := make([]string, 0, 5)
		for rows.Next() {
			var id int64
			var email string
			if err := rows.Scan(&id, &email); err != nil {
				rows.Close()
				t.Fatalf("scan row: %v", err)
			}
			serialized = append(serialized, fmt.Sprintf("%d:%s", id, email))
		}
		if rows.Err() != nil {
			rows.Close()
			t.Fatalf("iterate rows: %v", rows.Err())
		}
		rows.Close()

		snapshot := strings.Join(serialized, "|")
		if i == 0 {
			baseline = snapshot
			continue
		}
		if snapshot != baseline {
			t.Fatalf("compatibility result drift at iteration %d\n got: %s\nwant: %s", i, snapshot, baseline)
		}
	}
}

func TestPGWirePasswordAuthenticationWithAuthToken(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: walPath,
		Logger:      logger,
		AuthToken:   "secret-token",
	})
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for test: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeOnListener(ctx, listener)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("pgwire server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire server shutdown")
		}
	})

	addr := listener.Addr().String()
	if _, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable"); err == nil {
		t.Fatal("expected connection without password to fail")
	}
	if _, err := pgx.Connect(ctx, "postgres://asql:wrong@"+addr+"/asql?sslmode=disable"); err == nil {
		t.Fatal("expected connection with wrong password to fail")
	}

	conn, err := pgx.Connect(ctx, "postgres://asql:secret-token@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect pgx with password auth: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	var version string
	if err := conn.QueryRow(ctx, "SHOW server_version").Scan(&version); err != nil {
		t.Fatalf("show server_version after auth: %v", err)
	}
	if version == "" {
		t.Fatal("expected server_version after authenticated connection")
	}
}

func TestIsWriteStatement(t *testing.T) {
	writes := []string{
		"INSERT INTO orders VALUES (1)",
		"insert into orders values (1)",
		"UPDATE orders SET status = 'done' WHERE id = 1",
		"update orders set status = 'done' where id = 1",
		"DELETE FROM orders WHERE id = 1",
		"delete from orders where id = 1",
		"CREATE TABLE foo (id INT)",
		"create table foo (id int)",
		"DROP TABLE foo",
		"drop table foo",
		"ALTER TABLE foo ADD COLUMN x TEXT",
		"alter table foo add column x text",
		"TRUNCATE TABLE orders",
		"truncate table orders",
		// leading block comment stripped
		"/* hint */ INSERT INTO things VALUES (1)",
		"/* multi\nline */ UPDATE foo SET x = 1",
		// leading whitespace
		"   INSERT INTO orders VALUES (2)",
	}
	reads := []string{
		"SELECT * FROM orders",
		"select * from orders",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SHOW server_version",
		"SELECT * FROM asql_admin.cluster_members",
		"",
		"   ",
		// Domain transactions are session-scoping, not direct mutations.
		// The actual writes (INSERT/CREATE/etc.) inside the tx trigger redirect.
		"BEGIN DOMAIN master_recipe",
		"begin domain sensor_data",
		"BEGIN CROSS DOMAIN a, b",
		"begin cross domain a, b",
	}

	for _, sql := range writes {
		if !isWriteStatement(sql) {
			t.Errorf("expected isWriteStatement(%q) = true, got false", sql)
		}
	}
	for _, sql := range reads {
		if isWriteStatement(sql) {
			t.Errorf("expected isWriteStatement(%q) = false, got true", sql)
		}
	}
}

func TestDerivePgwireFromGRPC(t *testing.T) {
	tests := []struct {
		grpc string
		want string
	}{
		{"127.0.0.1:6433", "127.0.0.1:5433"},
		{"127.0.0.1:6434", "127.0.0.1:5434"},
		{":6435", "127.0.0.1:5435"},
		{"0.0.0.0:6433", "127.0.0.1:5433"},
		{"10.0.0.1:7000", "10.0.0.1:6000"},
		// Invalid / edge cases → empty
		{"", ""},
		{"no-port", ""},
		{"127.0.0.1:500", ""}, // port ≤ 1000
	}
	for _, tt := range tests {
		got := derivePgwireFromGRPC(tt.grpc)
		if got != tt.want {
			t.Errorf("derivePgwireFromGRPC(%q) = %q, want %q", tt.grpc, got, tt.want)
		}
	}
}

// TestSSLModePreferFallback confirms that sslmode=prefer (the default for
// psql, pgx, JDBC, DBeaver, DataGrip, and pgAdmin) works correctly when ASQL
// declines TLS via SSLRequest -> N.  The client must fall back to plaintext
// and complete the handshake without error.
func TestSSLModePreferFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: walPath,
		Logger:      logger,
		AuthToken:   "test-token",
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
				t.Fatalf("pgwire server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire shutdown")
		}
	})

	addr := listener.Addr().String()

	// sslmode=prefer: sends SSLRequest, gets N, falls back to plaintext.
	t.Run("sslmode_prefer", func(t *testing.T) {
		connStr := fmt.Sprintf("postgres://asql:test-token@%s/asql?sslmode=prefer&default_query_exec_mode=simple_protocol", addr)
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("sslmode=prefer should succeed (fallback to plaintext), got: %v", err)
		}
		defer conn.Close(ctx)

		var db string
		if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&db); err != nil {
			t.Fatalf("query after sslmode=prefer connect: %v", err)
		}
		if db != "asql" {
			t.Errorf("current_database() = %q, want %q", db, "asql")
		}
	})

	// sslmode=allow: same fallback behavior as prefer.
	t.Run("sslmode_allow", func(t *testing.T) {
		connStr := fmt.Sprintf("postgres://asql:test-token@%s/asql?sslmode=allow&default_query_exec_mode=simple_protocol", addr)
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("sslmode=allow should succeed, got: %v", err)
		}
		defer conn.Close(ctx)

		var v string
		if err := conn.QueryRow(ctx, "SELECT current_setting('server_version')").Scan(&v); err != nil {
			t.Fatalf("query after sslmode=allow connect: %v", err)
		}
		if v != "16.0" {
			t.Errorf("server_version = %q, want %q", v, "16.0")
		}
	})

	// sslmode=disable: never sends SSLRequest, plain connect succeeds.
	t.Run("sslmode_disable", func(t *testing.T) {
		connStr := fmt.Sprintf("postgres://asql:test-token@%s/asql?sslmode=disable&default_query_exec_mode=simple_protocol", addr)
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("sslmode=disable should succeed, got: %v", err)
		}
		defer conn.Close(ctx)

		var v string
		if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&v); err != nil {
			t.Fatalf("query: %v", err)
		}
		if v != "asql" {
			t.Errorf("current_database() = %q, want %q", v, "asql")
		}
	})
}

// TestMainstreamToolStartupFlows simulates the sequences of introspection
// queries that common PostgreSQL clients issue during connection startup.
// These tests serve as the acceptance gate for Epic Z: 2-3 mainstream tool
// flows must work within the documented compatibility surface.
func TestMainstreamToolStartupFlows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: walPath,
		Logger:      logger,
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
				t.Fatalf("pgwire server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire shutdown")
		}
	})

	connStr := "postgres://asql@" + listener.Addr().String() + "/asql?sslmode=disable&default_query_exec_mode=simple_protocol"
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	// Helper: query a single scalar string.
	scalar := func(t *testing.T, sql string) string {
		t.Helper()
		var val string
		if err := conn.QueryRow(ctx, sql).Scan(&val); err != nil {
			t.Fatalf("scalar query %q: %v", sql, err)
		}
		return val
	}

	// Helper: exec must not error.
	exec := func(t *testing.T, sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	// Helper: query must return at least one row.
	hasRows := func(t *testing.T, sql string) int {
		t.Helper()
		rows, err := conn.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		if rows.Err() != nil {
			t.Fatalf("iterate %q: %v", sql, rows.Err())
		}
		return count
	}

	// ── Tool 1: psql startup flow ──────────────────────────────────────
	// psql issues these queries during connection setup and \d commands.
	t.Run("psql_startup", func(t *testing.T) {
		// Connection startup introspection
		v := scalar(t, "SELECT current_setting('server_version')")
		if v != "16.0" {
			t.Errorf("server_version = %q", v)
		}

		v = scalar(t, "SELECT current_setting('standard_conforming_strings')")
		if v != "on" {
			t.Errorf("standard_conforming_strings = %q", v)
		}

		v = scalar(t, "SELECT pg_encoding_to_char(6)")
		if v != "UTF8" {
			t.Errorf("pg_encoding_to_char = %q", v)
		}

		v = scalar(t, "SELECT current_database()")
		if v != "asql" {
			t.Errorf("current_database = %q", v)
		}

		v = scalar(t, "SELECT current_user")
		if v != "asql" {
			t.Errorf("current_user = %q", v)
		}

		// SHOW commands psql uses
		v = scalar(t, "SHOW search_path")
		if v == "" {
			t.Error("SHOW search_path returned empty")
		}

		v = scalar(t, "SHOW server_version")
		if !strings.Contains(v, "asql") {
			t.Errorf("SHOW server_version = %q, want contains 'asql'", v)
		}

		// \d / \dt issue obj_description and pg_namespace queries
		v = scalar(t, "SELECT obj_description(1234, 'pg_class')")
		// Should return empty string, not error
		_ = v

		n := hasRows(t, "SELECT * FROM pg_catalog.pg_namespace")
		if n == 0 {
			t.Error("pg_namespace returned 0 rows")
		}

		// \l issues pg_database query
		n = hasRows(t, "SELECT * FROM pg_database")
		if n == 0 {
			t.Error("pg_database returned 0 rows")
		}
	})

	// ── Tool 2: DBeaver / DataGrip startup flow ────────────────────────
	// DBeaver and DataGrip issue SET/current_setting/pg_is_in_recovery
	// during connection initialization.
	t.Run("dbeaver_datagrip_startup", func(t *testing.T) {
		// SET commands (no-op acceptance)
		exec(t, "SET extra_float_digits = 3")
		exec(t, "SET application_name = 'DBeaver 24.0'")

		// set_config() call
		v := scalar(t, "SELECT set_config('application_name', 'DataGrip', false)")
		if v != "DataGrip" {
			t.Errorf("set_config() = %q, want %q", v, "DataGrip")
		}

		// Connection type detection
		v = scalar(t, "SELECT pg_is_in_recovery()")
		if v != "f" {
			t.Errorf("pg_is_in_recovery() = %q, want %q", v, "f")
		}

		// Version discovery
		v = scalar(t, "SELECT current_setting('server_version_num')")
		if v != "160000" {
			t.Errorf("server_version_num = %q, want %q", v, "160000")
		}

		v = scalar(t, "SELECT version()")
		if !strings.Contains(v, "asql") {
			t.Errorf("version() = %q, want contains 'asql'", v)
		}

		// Schema introspection
		n := hasRows(t, "SELECT * FROM pg_catalog.pg_type")
		if n == 0 {
			t.Error("pg_type returned 0 rows")
		}

		n = hasRows(t, "SELECT * FROM pg_catalog.pg_settings")
		if n == 0 {
			t.Error("pg_settings returned 0 rows")
		}

		n = hasRows(t, "SELECT * FROM information_schema.schemata")
		if n == 0 {
			t.Error("information_schema.schemata returned 0 rows")
		}

		// Privilege check
		v = scalar(t, "SELECT has_database_privilege('asql', 'CONNECT')")
		if v != "t" {
			t.Errorf("has_database_privilege() = %q, want %q", v, "t")
		}
	})

	// ── Tool 3: pgx / Go SDK driver startup ────────────────────────────
	// The pgx driver issues these during connection setup.
	t.Run("pgx_go_driver_startup", func(t *testing.T) {
		// pgx startup queries
		v := scalar(t, "SELECT current_setting('server_version')")
		if v == "" {
			t.Error("server_version is empty")
		}

		v = scalar(t, "SELECT current_setting('client_encoding')")
		if v != "UTF8" {
			t.Errorf("client_encoding = %q, want %q", v, "UTF8")
		}

		v = scalar(t, "SELECT current_setting('datestyle')")
		if v != "ISO, MDY" {
			t.Errorf("datestyle = %q, want %q", v, "ISO, MDY")
		}

		v = scalar(t, "SELECT current_setting('integer_datetimes')")
		if v != "on" {
			t.Errorf("integer_datetimes = %q, want %q", v, "on")
		}

		v = scalar(t, "SELECT current_setting('intervalstyle')")
		if v != "postgres" {
			t.Errorf("intervalstyle = %q, want %q", v, "postgres")
		}

		// Backend PID
		v = scalar(t, "SELECT pg_backend_pid()")
		if v != "1" {
			t.Errorf("pg_backend_pid() = %q, want %q", v, "1")
		}

		// inet_server_addr / port
		v = scalar(t, "SELECT inet_server_addr()")
		if v != "127.0.0.1" {
			t.Errorf("inet_server_addr() = %q", v)
		}

		v = scalar(t, "SELECT inet_server_port()")
		if v == "" {
			t.Error("inet_server_port() is empty")
		}
	})

	// ── End-to-end data workflow (proves tools can do real work) ────────
	t.Run("end_to_end_data_workflow", func(t *testing.T) {
		exec(t, "BEGIN DOMAIN tooltest")
		exec(t, "CREATE TABLE records (id INT PRIMARY KEY, name TEXT, active INT)")
		exec(t, "INSERT INTO records (id, name, active) VALUES (1, 'alpha', 1)")
		exec(t, "INSERT INTO records (id, name, active) VALUES (2, 'beta', 0)")
		exec(t, "INSERT INTO records (id, name, active) VALUES (3, 'gamma', 1)")
		exec(t, "COMMIT")

		// Query data outside transaction with domain-qualified table
		n := hasRows(t, "SELECT id, name FROM tooltest.records WHERE active = 1 ORDER BY id")
		if n != 2 {
			t.Errorf("expected 2 active records, got %d", n)
		}

		// Update data
		exec(t, "BEGIN DOMAIN tooltest")
		exec(t, "UPDATE records SET name = 'ALPHA' WHERE id = 1")
		exec(t, "COMMIT")

		v := scalar(t, "SELECT name FROM tooltest.records WHERE id = 1")
		if v != "ALPHA" {
			t.Errorf("updated name = %q, want %q", v, "ALPHA")
		}

		// Delete data
		exec(t, "BEGIN DOMAIN tooltest")
		exec(t, "DELETE FROM records WHERE active = 0")
		exec(t, "COMMIT")

		n = hasRows(t, "SELECT * FROM tooltest.records")
		if n != 2 {
			t.Errorf("expected 2 records after delete, got %d", n)
		}
	})
}

// TestCatalogStartupIntrospectionQueries exercises the new catalog
// interception paths added for current_setting(), pg_is_in_recovery(),
// pg_backend_pid(), pg_settings, pg_database, set_config, and related
// startup-introspection functions that psql and GUI tools issue.
func TestCatalogStartupIntrospectionQueries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: walPath,
		Logger:      logger,
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
				t.Fatalf("pgwire server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire shutdown")
		}
	})

	connStr := "postgres://asql@" + listener.Addr().String() + "/asql?sslmode=disable&default_query_exec_mode=simple_protocol"
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	// Helper: query a single scalar string.
	scalar := func(t *testing.T, sql string) string {
		t.Helper()
		var val string
		if err := conn.QueryRow(ctx, sql).Scan(&val); err != nil {
			t.Fatalf("scalar query %q: %v", sql, err)
		}
		return val
	}

	// ── current_setting() ──────────────────────────────────────
	t.Run("current_setting_server_version", func(t *testing.T) {
		got := scalar(t, "SELECT current_setting('server_version')")
		if got != "16.0" {
			t.Errorf("current_setting('server_version') = %q, want %q", got, "16.0")
		}
	})

	t.Run("current_setting_client_encoding", func(t *testing.T) {
		got := scalar(t, "SELECT current_setting('client_encoding')")
		if got != "UTF8" {
			t.Errorf("current_setting('client_encoding') = %q, want %q", got, "UTF8")
		}
	})

	t.Run("current_setting_timezone", func(t *testing.T) {
		got := scalar(t, "SELECT current_setting('TimeZone')")
		if got != "UTC" {
			t.Errorf("current_setting('TimeZone') = %q, want %q", got, "UTC")
		}
	})

	t.Run("current_setting_unknown_returns_empty", func(t *testing.T) {
		got := scalar(t, "SELECT current_setting('nonexistent_param')")
		if got != "" {
			t.Errorf("current_setting('nonexistent_param') = %q, want empty", got)
		}
	})

	t.Run("current_setting_search_path", func(t *testing.T) {
		got := scalar(t, "SELECT current_setting('search_path')")
		if got != "\"$user\", public" {
			t.Errorf("current_setting('search_path') = %q, want %q", got, "\"$user\", public")
		}
	})

	// ── pg_is_in_recovery() ────────────────────────────────────
	t.Run("pg_is_in_recovery", func(t *testing.T) {
		got := scalar(t, "SELECT pg_is_in_recovery()")
		// Standalone server is not a follower.
		if got != "f" {
			t.Errorf("pg_is_in_recovery() = %q, want %q", got, "f")
		}
	})

	// ── pg_backend_pid() ───────────────────────────────────────
	t.Run("pg_backend_pid", func(t *testing.T) {
		got := scalar(t, "SELECT pg_backend_pid()")
		if got != "1" {
			t.Errorf("pg_backend_pid() = %q, want %q", got, "1")
		}
	})

	// ── set_config() ───────────────────────────────────────────
	t.Run("set_config_noop", func(t *testing.T) {
		got := scalar(t, "SELECT set_config('application_name', 'myapp', false)")
		if got != "myapp" {
			t.Errorf("set_config() = %q, want %q", got, "myapp")
		}
	})

	// ── SHOW search_path ───────────────────────────────────────
	t.Run("show_search_path", func(t *testing.T) {
		got := scalar(t, "SHOW search_path")
		if got != "\"$user\", public" {
			t.Errorf("SHOW search_path = %q, want %q", got, "\"$user\", public")
		}
	})

	// ── pg_encoding_to_char ────────────────────────────────────
	t.Run("pg_encoding_to_char", func(t *testing.T) {
		got := scalar(t, "SELECT pg_encoding_to_char(6)")
		if got != "UTF8" {
			t.Errorf("pg_encoding_to_char(6) = %q, want %q", got, "UTF8")
		}
	})

	// ── obj_description / col_description ──────────────────────
	t.Run("obj_description", func(t *testing.T) {
		got := scalar(t, "SELECT obj_description(1234, 'pg_class')")
		if got != "" {
			t.Errorf("obj_description() = %q, want empty", got)
		}
	})

	// ── has_schema_privilege ───────────────────────────────────
	t.Run("has_schema_privilege", func(t *testing.T) {
		got := scalar(t, "SELECT has_schema_privilege('public', 'USAGE')")
		if got != "t" {
			t.Errorf("has_schema_privilege() = %q, want %q", got, "t")
		}
	})

	// ── inet_server_addr() ─────────────────────────────────────
	t.Run("inet_server_addr", func(t *testing.T) {
		got := scalar(t, "SELECT inet_server_addr()")
		if got != "127.0.0.1" {
			t.Errorf("inet_server_addr() = %q, want %q", got, "127.0.0.1")
		}
	})

	// ── pg_database (multirow) ─────────────────────────────────
	t.Run("pg_database_datname", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT * FROM pg_database")
		if err != nil {
			t.Fatalf("pg_database query: %v", err)
		}
		defer rows.Close()
		found := false
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Fatalf("pg_database values: %v", err)
			}
			// datname is the second column (index 1).
			if len(vals) > 1 {
				if name, ok := vals[1].(string); ok && name == "asql" {
					found = true
				}
			}
		}
		if rows.Err() != nil {
			t.Fatalf("pg_database iteration: %v", rows.Err())
		}
		if !found {
			t.Error("pg_database did not contain datname='asql'")
		}
	})

	// ── pg_settings (multirow) ─────────────────────────────────
	t.Run("pg_settings_has_rows", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT * FROM pg_settings")
		if err != nil {
			t.Fatalf("pg_settings query: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
			vals, err := rows.Values()
			if err != nil {
				t.Fatalf("pg_settings values: %v", err)
			}
			_ = vals // we only care that rows iterate without error
		}
		if rows.Err() != nil {
			t.Fatalf("pg_settings iteration: %v", rows.Err())
		}
		if count == 0 {
			t.Error("pg_settings returned 0 rows, expected at least 1")
		}
	})
}
