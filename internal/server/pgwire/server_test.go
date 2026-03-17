package pgwire

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"asql/internal/engine/executor"

	"github.com/jackc/pgx/v5"
)

func TestPGWireCurrentReadAllowsAuthenticatedUsersButMutationsRequireAdmin(t *testing.T) {
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

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "secret-pass"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}
	if err := server.engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create analyst user: %v", err)
	}

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
	adminConn, err := pgx.Connect(ctx, "postgres://admin:secret-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(func() { _ = adminConn.Close(ctx) })

	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	} {
		if _, err := adminConn.Exec(ctx, sql); err != nil {
			t.Fatalf("admin setup %q: %v", sql, err)
		}
	}

	analystConn, err := pgx.Connect(ctx, "postgres://analyst:analyst-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect analyst: %v", err)
	}
	t.Cleanup(func() { _ = analystConn.Close(ctx) })

	var email string
	if err := analystConn.QueryRow(ctx, "SELECT email FROM accounts.users WHERE id = 1").Scan(&email); err != nil {
		t.Fatalf("analyst current read: %v", err)
	}
	if email != "one@asql.dev" {
		t.Fatalf("analyst read email = %q, want %q", email, "one@asql.dev")
	}

	if _, err := analystConn.Exec(ctx, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("analyst begin domain: %v", err)
	}
	_, err = analystConn.Exec(ctx, "INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')")
	pgErr := requirePGErrorCode(t, err, "42501")
	if !strings.Contains(pgErr.Message, "ADMIN privilege required") {
		t.Fatalf("unexpected analyst insert denial message: %q", pgErr.Message)
	}
	if _, err := analystConn.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("rollback after denied insert: %v", err)
	}

	if _, err := analystConn.Exec(ctx, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("analyst begin domain for schema denial: %v", err)
	}
	_, err = analystConn.Exec(ctx, "CREATE TABLE audit_log (id INT)")
	pgErr = requirePGErrorCode(t, err, "42501")
	if !strings.Contains(pgErr.Message, "ADMIN privilege required") {
		t.Fatalf("unexpected analyst create table denial message: %q", pgErr.Message)
	}
	if _, err := analystConn.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("rollback after denied create table: %v", err)
	}

	_, err = analystConn.Exec(ctx, "EXPLAIN UPDATE accounts.users SET email = 'updated@asql.dev' WHERE id = 1")
	pgErr = requirePGErrorCode(t, err, "42501")
	if !strings.Contains(pgErr.Message, "ADMIN privilege required") {
		t.Fatalf("unexpected analyst explain update denial message: %q", pgErr.Message)
	}
}

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

func TestPGWireSimpleExplainQueryRoundtrip(t *testing.T) {
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

	setup := []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	}
	for _, sql := range setup {
		if _, err := connection.Exec(ctx, sql); err != nil {
			t.Fatalf("setup %q: %v", sql, err)
		}
	}

	rows, err := connection.Query(ctx, "EXPLAIN SELECT id, email FROM accounts.users ORDER BY id ASC")
	if err != nil {
		t.Fatalf("explain query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected explain row")
	}

	var operation, domain, tableName, planShape, accessPlan string
	if err := rows.Scan(&operation, &domain, &tableName, &planShape, &accessPlan); err != nil {
		t.Fatalf("scan explain row: %v", err)
	}
	if rows.Err() != nil {
		t.Fatalf("iterate explain rows: %v", rows.Err())
	}

	if operation == "" || domain != "accounts" || tableName != "users" || planShape == "" || accessPlan == "" {
		t.Fatalf("unexpected explain row: operation=%q domain=%q table=%q plan=%q access=%q", operation, domain, tableName, planShape, accessPlan)
	}
	if rows.Next() {
		t.Fatal("expected a single explain row")
	}
}

func TestPGWireExtendedExplainQueryRoundtrip(t *testing.T) {
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

	connection, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close(ctx) })

	setup := []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	}
	for _, sql := range setup {
		if _, err := connection.Exec(ctx, sql); err != nil {
			t.Fatalf("setup %q: %v", sql, err)
		}
	}

	var operation, domain, tableName, planShape, accessPlan string
	if err := connection.QueryRow(ctx, "EXPLAIN SELECT id, email FROM accounts.users ORDER BY id ASC").Scan(&operation, &domain, &tableName, &planShape, &accessPlan); err != nil {
		t.Fatalf("extended explain query: %v", err)
	}

	if operation == "" || domain != "accounts" || tableName != "users" || planShape == "" || accessPlan == "" {
		t.Fatalf("unexpected explain row: operation=%q domain=%q table=%q plan=%q access=%q", operation, domain, tableName, planShape, accessPlan)
	}
}

func TestPGWireCompatibilitySupportedPatterns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "compatibility-data")
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

	conn, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT PRIMARY KEY, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')",
		"INSERT INTO users (id, email) VALUES (3, 'three@asql.dev')",
		"COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	rows, err := conn.Query(ctx, "SELECT id, email FROM accounts.users WHERE id >= $1 ORDER BY id ASC LIMIT 2", int64(2))
	if err != nil {
		t.Fatalf("parameterized compatibility query: %v", err)
	}
	defer rows.Close()

	var gotIDs []int64
	for rows.Next() {
		var id int64
		var email string
		if err := rows.Scan(&id, &email); err != nil {
			t.Fatalf("scan parameterized row: %v", err)
		}
		gotIDs = append(gotIDs, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate parameterized rows: %v", err)
	}
	if len(gotIDs) != 2 || gotIDs[0] != 2 || gotIDs[1] != 3 {
		t.Fatalf("unexpected parameterized query ids: %v", gotIDs)
	}

	offsetRows, err := conn.Query(ctx, "SELECT id FROM accounts.users ORDER BY id ASC LIMIT 2 OFFSET 1")
	if err != nil {
		t.Fatalf("limit/offset compatibility query: %v", err)
	}
	defer offsetRows.Close()

	gotIDs = gotIDs[:0]
	for offsetRows.Next() {
		var id int64
		if err := offsetRows.Scan(&id); err != nil {
			t.Fatalf("scan OFFSET row: %v", err)
		}
		gotIDs = append(gotIDs, id)
	}
	if err := offsetRows.Err(); err != nil {
		t.Fatalf("iterate OFFSET rows: %v", err)
	}
	if len(gotIDs) != 2 || gotIDs[0] != 2 || gotIDs[1] != 3 {
		t.Fatalf("unexpected LIMIT/OFFSET query ids: %v", gotIDs)
	}

	inRows, err := conn.Query(ctx, "SELECT id FROM accounts.users WHERE id IN (1, 3) ORDER BY id ASC LIMIT 2")
	if err != nil {
		t.Fatalf("literal IN compatibility query: %v", err)
	}
	defer inRows.Close()

	gotIDs = gotIDs[:0]
	for inRows.Next() {
		var id int64
		if err := inRows.Scan(&id); err != nil {
			t.Fatalf("scan IN row: %v", err)
		}
		gotIDs = append(gotIDs, id)
	}
	if err := inRows.Err(); err != nil {
		t.Fatalf("iterate IN rows: %v", err)
	}
	if len(gotIDs) != 2 || gotIDs[0] != 1 || gotIDs[1] != 3 {
		t.Fatalf("unexpected IN query ids: %v", gotIDs)
	}
}

func TestPGWireCompatibilityUnsupportedPatternGuidance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "compatibility-unsupported-data")
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

	conn, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	if _, err := conn.Exec(ctx, "BEGIN"); err == nil || !strings.Contains(err.Error(), "BEGIN DOMAIN") {
		t.Fatalf("expected bare BEGIN guidance error, got %v", err)
	}

	if _, err := conn.Exec(ctx, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("begin domain for ANY guidance: %v", err)
	}
	if _, err := conn.Exec(ctx, "SELECT id FROM users WHERE id = ANY(ARRAY[1,2])"); err == nil || !strings.Contains(err.Error(), "ANY(...)") {
		t.Fatalf("expected ANY guidance error, got %v", err)
	}
	if _, err := conn.Exec(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("rollback after ANY guidance: %v", err)
	}
}

func TestPGWireCreateIfNotExistsRegression(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "if-not-exists-data")
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

	connect := func(t *testing.T) *pgx.Conn {
		t.Helper()
		conn, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable")
		if err != nil {
			t.Fatalf("connect pgx: %v", err)
		}
		return conn
	}

	conn1 := connect(t)
	defer conn1.Close(ctx)
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT PRIMARY KEY, email TEXT)",
		"CREATE INDEX idx_users_email ON users (email)",
		"COMMIT",
	} {
		if _, err := conn1.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	conn2 := connect(t)
	defer conn2.Close(ctx)
	if _, err := conn2.Exec(ctx, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("begin duplicate tx: %v", err)
	}
	if _, err := conn2.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT)"); err != nil {
		t.Fatalf("queue duplicate create table: %v", err)
	}
	if _, err := conn2.Exec(ctx, "COMMIT"); err == nil {
		t.Fatal("expected duplicate CREATE TABLE to fail without IF NOT EXISTS")
	}

	conn3 := connect(t)
	defer conn3.Close(ctx)
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, email TEXT)",
		"CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)",
		"COMMIT",
	} {
		if _, err := conn3.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	rows, err := conn3.Query(ctx, "SELECT id, email FROM accounts.users ORDER BY id ASC")
	if err != nil {
		t.Fatalf("query users after IF NOT EXISTS roundtrip: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("expected table to remain empty after duplicate IF NOT EXISTS roundtrip")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate rows: %v", err)
	}
}

func TestPGWireForHistoryContract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "history-data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: dataDir,
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

	for _, sql := range []string{
		"BEGIN DOMAIN history",
		"CREATE TABLE items (id INT PRIMARY KEY, name TEXT)",
		"INSERT INTO items (id, name) VALUES (1, 'Alice')",
		"COMMIT",
		"BEGIN DOMAIN history",
		"UPDATE items SET name = 'Bob' WHERE id = 1",
		"COMMIT",
		"BEGIN DOMAIN history",
		"DELETE FROM items WHERE id = 1",
		"COMMIT",
	} {
		if _, err := connection.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	rows, err := connection.Query(ctx, "SELECT * FROM history.items FOR HISTORY WHERE id = 1")
	if err != nil {
		t.Fatalf("history query: %v", err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	gotColumns := make([]string, 0, len(fields))
	for _, field := range fields {
		gotColumns = append(gotColumns, string(field.Name))
	}
	wantColumns := []string{executor.HistoryOperationColumnName, executor.HistoryCommitLSNColumnName, "id", "name"}
	if strings.Join(gotColumns, ",") != strings.Join(wantColumns, ",") {
		t.Fatalf("unexpected FOR HISTORY columns: got %v want %v", gotColumns, wantColumns)
	}

	var operations []string
	for rows.Next() {
		var operation string
		var commitLSN int64
		var id int64
		var name string
		if err := rows.Scan(&operation, &commitLSN, &id, &name); err != nil {
			t.Fatalf("scan history row: %v", err)
		}
		if commitLSN <= 0 {
			t.Fatalf("expected positive commit lsn, got %d", commitLSN)
		}
		if id != 1 {
			t.Fatalf("expected id=1, got %d", id)
		}
		operations = append(operations, operation)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate history rows: %v", err)
	}

	if strings.Join(operations, ",") != "INSERT,UPDATE,DELETE" {
		t.Fatalf("unexpected FOR HISTORY operations: %v", operations)
	}
}

func TestPGWireCurrentLSNFunction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "current-lsn-data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: dataDir,
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

	for _, sql := range []string{
		"BEGIN DOMAIN metrics",
		"CREATE TABLE counters (id INT PRIMARY KEY, value INT)",
		"INSERT INTO counters (id, value) VALUES (1, 10)",
		"COMMIT",
	} {
		if _, err := connection.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	rows, err := connection.Query(ctx, "SELECT current_lsn()")
	if err != nil {
		t.Fatalf("query current_lsn(): %v", err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	if len(fields) != 1 || string(fields[0].Name) != "current_lsn" {
		t.Fatalf("unexpected current_lsn() columns: %+v", fields)
	}

	if !rows.Next() {
		t.Fatal("expected one current_lsn() row")
	}
	var currentLSN int64
	if err := rows.Scan(&currentLSN); err != nil {
		t.Fatalf("scan current_lsn(): %v", err)
	}
	if currentLSN <= 0 {
		t.Fatalf("expected positive current_lsn(), got %d", currentLSN)
	}
	if rows.Next() {
		t.Fatal("expected only one current_lsn() row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate current_lsn() rows: %v", err)
	}
}

func TestPGWireAdminRowHistoryUsesActiveDomainContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "admin-row-history-data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: dataDir,
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
	t.Cleanup(func() { _ = listener.Close() })

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeOnListener(ctx, listener)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("serve pgwire: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire server shutdown")
		}
	})

	connection, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close(ctx) })

	for _, sql := range []string{
		"BEGIN DOMAIN history",
		"CREATE TABLE items (id INT PRIMARY KEY, name TEXT)",
		"INSERT INTO items (id, name) VALUES (1, 'Alice')",
		"COMMIT",
		"BEGIN DOMAIN history",
		"UPDATE items SET name = 'Bob' WHERE id = 1",
		"COMMIT",
	} {
		if _, err := connection.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	if _, err := connection.Exec(ctx, "BEGIN DOMAIN history"); err != nil {
		t.Fatalf("begin domain for admin row history: %v", err)
	}
	t.Cleanup(func() {
		_, _ = connection.Exec(context.Background(), "ROLLBACK")
	})

	rows, err := connection.Query(ctx, "SELECT * FROM asql_admin.row_history WHERE sql = 'SELECT * FROM items FOR HISTORY WHERE id = 1'")
	if err != nil {
		t.Fatalf("admin row history query: %v", err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	gotColumns := make([]string, 0, len(fields))
	for _, field := range fields {
		gotColumns = append(gotColumns, string(field.Name))
	}
	wantColumns := []string{executor.HistoryOperationColumnName, executor.HistoryCommitLSNColumnName, "id", "name"}
	if strings.Join(gotColumns, ",") != strings.Join(wantColumns, ",") {
		t.Fatalf("unexpected admin row history columns: got %v want %v", gotColumns, wantColumns)
	}

	var operations []string
	for rows.Next() {
		var operation string
		var commitLSN int64
		var id int64
		var name string
		if err := rows.Scan(&operation, &commitLSN, &id, &name); err != nil {
			t.Fatalf("scan admin row history row: %v", err)
		}
		if commitLSN <= 0 {
			t.Fatalf("expected positive commit lsn, got %d", commitLSN)
		}
		if id != 1 {
			t.Fatalf("expected id=1, got %d", id)
		}
		operations = append(operations, operation)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate admin row history rows: %v", err)
	}

	if strings.Join(operations, ",") != "INSERT,UPDATE" {
		t.Fatalf("unexpected admin row history operations: %v", operations)
	}
}

func TestPGWireRowLSNAndEntityFunctions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "introspection-data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: dataDir,
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

	for _, sql := range []string{
		"BEGIN DOMAIN introspect",
		"CREATE TABLE items (id INT PRIMARY KEY, status TEXT)",
		"CREATE TABLE audit_entries (id INT PRIMARY KEY, note TEXT)",
		"CREATE ENTITY item_aggregate (ROOT items)",
		"COMMIT",
		"BEGIN DOMAIN introspect",
		"INSERT INTO items (id, status) VALUES (1, 'draft')",
		"INSERT INTO audit_entries (id, note) VALUES (7, 'created')",
		"COMMIT",
		"BEGIN DOMAIN introspect",
		"UPDATE items SET status = 'published' WHERE id = 1",
		"COMMIT",
	} {
		if _, err := connection.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	var rowLSN int64
	if err := connection.QueryRow(ctx, "SELECT row_lsn('introspect.items', '1')").Scan(&rowLSN); err != nil {
		t.Fatalf("query row_lsn(): %v", err)
	}
	if rowLSN <= 0 {
		t.Fatalf("expected positive row_lsn(), got %d", rowLSN)
	}

	var version int64
	if err := connection.QueryRow(ctx, "SELECT entity_version('introspect', 'item_aggregate', '1')").Scan(&version); err != nil {
		t.Fatalf("query entity_version(): %v", err)
	}
	if version != 2 {
		t.Fatalf("expected entity_version()=2, got %d", version)
	}

	var entityHeadLSN int64
	if err := connection.QueryRow(ctx, "SELECT entity_head_lsn('introspect', 'item_aggregate', '1')").Scan(&entityHeadLSN); err != nil {
		t.Fatalf("query entity_head_lsn(): %v", err)
	}
	if entityHeadLSN <= 0 {
		t.Fatalf("expected positive entity_head_lsn(), got %d", entityHeadLSN)
	}
	if entityHeadLSN != rowLSN {
		t.Fatalf("expected entity_head_lsn() to match row_lsn() for root-only entity, got %d vs %d", entityHeadLSN, rowLSN)
	}

	var entityVersionOneLSN int64
	if err := connection.QueryRow(ctx, "SELECT entity_version_lsn('introspect', 'item_aggregate', '1', 1)").Scan(&entityVersionOneLSN); err != nil {
		t.Fatalf("query entity_version_lsn(version 1): %v", err)
	}
	if entityVersionOneLSN <= 0 || entityVersionOneLSN >= entityHeadLSN {
		t.Fatalf("expected entity_version_lsn(version 1) to be positive and older than head, got %d vs head %d", entityVersionOneLSN, entityHeadLSN)
	}

	var entityVersionTwoLSN int64
	if err := connection.QueryRow(ctx, "SELECT entity_version_lsn('introspect', 'item_aggregate', '1', 2)").Scan(&entityVersionTwoLSN); err != nil {
		t.Fatalf("query entity_version_lsn(version 2): %v", err)
	}
	if entityVersionTwoLSN != entityHeadLSN {
		t.Fatalf("expected entity_version_lsn(version 2)=%d to match entity_head_lsn()=%d", entityVersionTwoLSN, entityHeadLSN)
	}

	var resolvedEntity int64
	if err := connection.QueryRow(ctx, "SELECT resolve_reference('introspect.items', '1')").Scan(&resolvedEntity); err != nil {
		t.Fatalf("query resolve_reference() on entity root: %v", err)
	}
	if resolvedEntity != version {
		t.Fatalf("expected resolve_reference()=%d to match entity_version()=%d", resolvedEntity, version)
	}

	var auditRowLSN int64
	if err := connection.QueryRow(ctx, "SELECT row_lsn('introspect.audit_entries', '7')").Scan(&auditRowLSN); err != nil {
		t.Fatalf("query audit row_lsn(): %v", err)
	}
	if auditRowLSN <= 0 {
		t.Fatalf("expected positive audit row_lsn(), got %d", auditRowLSN)
	}

	var resolvedAudit int64
	if err := connection.QueryRow(ctx, "SELECT resolve_reference('introspect.audit_entries', '7')").Scan(&resolvedAudit); err != nil {
		t.Fatalf("query resolve_reference() on non-entity row: %v", err)
	}
	if resolvedAudit != auditRowLSN {
		t.Fatalf("expected resolve_reference()=%d to match row_lsn()=%d for non-entity table", resolvedAudit, auditRowLSN)
	}

	var missingRowLSN *int64
	if err := connection.QueryRow(ctx, "SELECT row_lsn('introspect.items', '999')").Scan(&missingRowLSN); err != nil {
		t.Fatalf("query missing row_lsn(): %v", err)
	}
	if missingRowLSN != nil {
		t.Fatalf("expected NULL row_lsn() for missing row, got %v", *missingRowLSN)
	}

	var missingResolved *int64
	if err := connection.QueryRow(ctx, "SELECT resolve_reference('introspect.items', '999')").Scan(&missingResolved); err != nil {
		t.Fatalf("query missing resolve_reference(): %v", err)
	}
	if missingResolved != nil {
		t.Fatalf("expected NULL resolve_reference() for missing row, got %v", *missingResolved)
	}

	var missingVersionLSN *int64
	if err := connection.QueryRow(ctx, "SELECT entity_version_lsn('introspect', 'item_aggregate', '1', 99)").Scan(&missingVersionLSN); err != nil {
		t.Fatalf("query missing entity_version_lsn(): %v", err)
	}
	if missingVersionLSN != nil {
		t.Fatalf("expected NULL entity_version_lsn() for missing version, got %v", *missingVersionLSN)
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

func TestPGWirePasswordAuthenticationWithDurablePrincipal(t *testing.T) {
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

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "secret-pass"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}

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
	if _, err := pgx.Connect(ctx, "postgres://admin@"+addr+"/asql?sslmode=disable"); err == nil {
		t.Fatal("expected connection without password to fail")
	}
	if _, err := pgx.Connect(ctx, "postgres://admin:wrong@"+addr+"/asql?sslmode=disable"); err == nil {
		t.Fatal("expected connection with wrong durable principal password to fail")
	}

	conn, err := pgx.Connect(ctx, "postgres://admin:secret-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect pgx with durable principal auth: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	var version string
	if err := conn.QueryRow(ctx, "SHOW server_version").Scan(&version); err != nil {
		t.Fatalf("show server_version after durable auth: %v", err)
	}
	if version == "" {
		t.Fatal("expected server_version after authenticated durable principal connection")
	}
}

func TestPGWirePasswordAuthenticationRespectsPasswordRotation(t *testing.T) {
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

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "secret-pass"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}
	if err := server.engine.SetPrincipalPassword(ctx, "admin", "rotated-pass"); err != nil {
		t.Fatalf("set principal password: %v", err)
	}

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
	if _, err := pgx.Connect(ctx, "postgres://admin:secret-pass@"+addr+"/asql?sslmode=disable"); err == nil {
		t.Fatal("expected connection with pre-rotation password to fail")
	}

	conn, err := pgx.Connect(ctx, "postgres://admin:rotated-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect pgx with rotated durable principal password: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	var version string
	if err := conn.QueryRow(ctx, "SHOW server_version").Scan(&version); err != nil {
		t.Fatalf("show server_version after rotated auth: %v", err)
	}
	if version == "" {
		t.Fatal("expected server_version after rotated-password authentication")
	}
}

func TestPGWirePasswordAuthenticationRespectsDisableAndEnable(t *testing.T) {
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

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "secret-pass"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}

	if err := server.engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create principal: %v", err)
	}
	if err := server.engine.DisablePrincipal(ctx, "analyst"); err != nil {
		t.Fatalf("disable principal: %v", err)
	}
	if err := server.engine.EnablePrincipal(ctx, "analyst"); err != nil {
		t.Fatalf("enable principal: %v", err)
	}

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

	conn, err := pgx.Connect(ctx, "postgres://analyst:analyst-pass@"+listener.Addr().String()+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect pgx after re-enable: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
}

func TestPGWireSessionIdentityReflectsAuthenticatedPrincipal(t *testing.T) {
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

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "secret-pass"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}
	if err := server.engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create analyst principal: %v", err)
	}

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

	adminConn, err := pgx.Connect(ctx, "postgres://admin:secret-pass@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(func() { _ = adminConn.Close(ctx) })

	analystConn, err := pgx.Connect(ctx, "postgres://analyst:analyst-pass@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect analyst: %v", err)
	}
	t.Cleanup(func() { _ = analystConn.Close(ctx) })

	var currentUser string
	if err := analystConn.QueryRow(ctx, "SELECT current_user").Scan(&currentUser); err != nil {
		t.Fatalf("select current_user: %v", err)
	}
	if currentUser != "analyst" {
		t.Fatalf("current_user = %q, want %q", currentUser, "analyst")
	}

	var sessionUser string
	if err := analystConn.QueryRow(ctx, "SELECT session_user").Scan(&sessionUser); err != nil {
		t.Fatalf("select session_user: %v", err)
	}
	if sessionUser != "analyst" {
		t.Fatalf("session_user = %q, want %q", sessionUser, "analyst")
	}

	var sessionAuth string
	if err := analystConn.QueryRow(ctx, "SELECT current_setting('session_authorization')").Scan(&sessionAuth); err != nil {
		t.Fatalf("select session_authorization: %v", err)
	}
	if sessionAuth != "analyst" {
		t.Fatalf("session_authorization = %q, want %q", sessionAuth, "analyst")
	}

	var analystSuper string
	if err := analystConn.QueryRow(ctx, "SELECT current_setting('is_superuser')").Scan(&analystSuper); err != nil {
		t.Fatalf("select analyst is_superuser: %v", err)
	}
	if analystSuper != "off" {
		t.Fatalf("analyst is_superuser = %q, want %q", analystSuper, "off")
	}

	var adminSuper string
	if err := adminConn.QueryRow(ctx, "SELECT current_setting('is_superuser')").Scan(&adminSuper); err != nil {
		t.Fatalf("select admin is_superuser: %v", err)
	}
	if adminSuper != "on" {
		t.Fatalf("admin is_superuser = %q, want %q", adminSuper, "on")
	}
}

func TestPGWirePrivilegeProbesReflectDurablePrincipalGrants(t *testing.T) {
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

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "secret-pass"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}
	if err := server.engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create analyst principal: %v", err)
	}

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

	adminConn, err := pgx.Connect(ctx, "postgres://admin:secret-pass@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(func() { _ = adminConn.Close(ctx) })

	analystConn, err := pgx.Connect(ctx, "postgres://analyst:analyst-pass@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect analyst: %v", err)
	}
	t.Cleanup(func() { _ = analystConn.Close(ctx) })

	scalar := func(t *testing.T, conn *pgx.Conn, query string) string {
		t.Helper()
		var value string
		if err := conn.QueryRow(ctx, query).Scan(&value); err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		return value
	}

	if got := scalar(t, analystConn, "SELECT has_database_privilege('asql', 'CONNECT')"); got != "t" {
		t.Fatalf("analyst CONNECT privilege = %q, want %q", got, "t")
	}
	if got := scalar(t, analystConn, "SELECT has_database_privilege('asql', 'CREATE')"); got != "f" {
		t.Fatalf("analyst CREATE privilege = %q, want %q", got, "f")
	}
	if got := scalar(t, adminConn, "SELECT has_database_privilege('asql', 'CREATE')"); got != "t" {
		t.Fatalf("admin CREATE privilege = %q, want %q", got, "t")
	}
	if got := scalar(t, analystConn, "SELECT has_schema_privilege('public', 'USAGE')"); got != "t" {
		t.Fatalf("analyst schema USAGE privilege = %q, want %q", got, "t")
	}
	if got := scalar(t, analystConn, "SELECT has_schema_privilege('public', 'CREATE')"); got != "f" {
		t.Fatalf("analyst schema CREATE privilege = %q, want %q", got, "f")
	}
	if got := scalar(t, analystConn, "SELECT has_table_privilege('public.items', 'SELECT_HISTORY')"); got != "f" {
		t.Fatalf("analyst SELECT_HISTORY privilege = %q, want %q", got, "f")
	}
	if err := server.engine.GrantPrivilege(ctx, "analyst", executor.PrincipalPrivilegeSelectHistory); err != nil {
		t.Fatalf("grant analyst SELECT_HISTORY: %v", err)
	}
	if got := scalar(t, analystConn, "SELECT has_table_privilege('public.items', 'SELECT_HISTORY')"); got != "t" {
		t.Fatalf("analyst granted SELECT_HISTORY privilege = %q, want %q", got, "t")
	}
	if got := scalar(t, analystConn, "SELECT has_database_privilege('admin', 'asql', 'CREATE')"); got != "t" {
		t.Fatalf("explicit admin CREATE privilege = %q, want %q", got, "t")
	}
}

func TestPGWirePasswordAuthenticationFailsAfterPrincipalDeletion(t *testing.T) {
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

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "secret-pass"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}

	if err := server.engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create principal: %v", err)
	}
	if err := server.engine.DisablePrincipal(ctx, "analyst"); err != nil {
		t.Fatalf("disable principal: %v", err)
	}
	if err := server.engine.DeletePrincipal(ctx, "analyst"); err != nil {
		t.Fatalf("delete principal: %v", err)
	}

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

	if _, err := pgx.Connect(ctx, "postgres://analyst:analyst-pass@"+listener.Addr().String()+"/asql?sslmode=disable"); err == nil {
		t.Fatal("expected connection with deleted principal to fail")
	}
}

func TestPGWireHistoricalReadRequiresSelectHistoryPrivilege(t *testing.T) {
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

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "secret-pass"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}
	if err := server.engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create analyst user: %v", err)
	}
	if err := server.engine.CreateUser(ctx, "historian", "historian-pass"); err != nil {
		t.Fatalf("create historian user: %v", err)
	}
	if err := server.engine.GrantPrivilege(ctx, "historian", executor.PrincipalPrivilegeSelectHistory); err != nil {
		t.Fatalf("grant SELECT_HISTORY: %v", err)
	}

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
	adminConn, err := pgx.Connect(ctx, "postgres://admin:secret-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(func() { _ = adminConn.Close(ctx) })

	setup := []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	}
	for _, sql := range setup {
		if _, err := adminConn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup %q: %v", sql, err)
		}
	}

	targetLSN := server.walStore.LastLSN()
	historySQL := fmt.Sprintf("SELECT id, email FROM accounts.users /* as-of-lsn: %d */", targetLSN)

	analystConn, err := pgx.Connect(ctx, "postgres://analyst:analyst-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect analyst: %v", err)
	}
	t.Cleanup(func() { _ = analystConn.Close(ctx) })

	if _, err := analystConn.Query(ctx, historySQL); err == nil || !strings.Contains(err.Error(), "SELECT_HISTORY") {
		t.Fatalf("expected SELECT_HISTORY failure for analyst, got %v", err)
	}

	historianConn, err := pgx.Connect(ctx, "postgres://historian:historian-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect historian: %v", err)
	}
	t.Cleanup(func() { _ = historianConn.Close(ctx) })

	rows, err := historianConn.Query(ctx, historySQL)
	if err != nil {
		t.Fatalf("historian history query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected historical row")
	}
	var id int64
	var email string
	if err := rows.Scan(&id, &email); err != nil {
		t.Fatalf("scan historian row: %v", err)
	}
	if id != 1 || email != "one@asql.dev" {
		t.Fatalf("unexpected historian row: id=%d email=%q", id, email)
	}
	if rows.Err() != nil {
		t.Fatalf("iterate historian rows: %v", rows.Err())
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

	t.Run("show_unknown_param_returns_empty", func(t *testing.T) {
		got := scalar(t, "SHOW application_name")
		if got != "" {
			t.Errorf("SHOW application_name = %q, want empty", got)
		}
	})

	t.Run("reset_and_deallocate_are_noop", func(t *testing.T) {
		for _, sql := range []string{
			"SET application_name = 'myapp'",
			"RESET application_name",
			"RESET ALL",
			"DEALLOCATE temp_stmt",
			"DEALLOCATE ALL",
		} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("exec %q: %v", sql, err)
			}
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

func TestCatalogEmptyInterceptsExposeSchemaAcrossProtocols(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "empty-catalog-data")
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

	conn, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	tables := []string{"pg_index", "pg_constraint", "pg_proc", "pg_am", "pg_extension", "pg_roles", "pg_authid", "pg_user"}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			rows, err := conn.Query(ctx, "SELECT * FROM "+table)
			if err != nil {
				t.Fatalf("query %s: %v", table, err)
			}
			defer rows.Close()

			fields := rows.FieldDescriptions()
			if len(fields) == 0 {
				t.Fatalf("expected schema for %s empty intercept", table)
			}

			if rows.Next() {
				t.Fatalf("expected %s to return no rows", table)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("iterate %s: %v", table, err)
			}
		})
	}
}

func TestPGWireAuditEventsCoverLoginAndHistoricalReadAuthorization(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "data")
	handler := newPGWireAuditCaptureHandler()
	logger := slog.New(handler)

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: walPath,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "secret-pass"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}
	if err := server.engine.CreateUser(ctx, "analyst", "analyst-pass"); err != nil {
		t.Fatalf("create analyst user: %v", err)
	}
	if err := server.engine.CreateUser(ctx, "historian", "historian-pass"); err != nil {
		t.Fatalf("create historian user: %v", err)
	}
	if err := server.engine.GrantPrivilege(ctx, "historian", executor.PrincipalPrivilegeSelectHistory); err != nil {
		t.Fatalf("grant SELECT_HISTORY: %v", err)
	}

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
	adminConn, err := pgx.Connect(ctx, "postgres://admin:secret-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	t.Cleanup(func() { _ = adminConn.Close(ctx) })

	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	} {
		if _, err := adminConn.Exec(ctx, sql); err != nil {
			t.Fatalf("setup %q: %v", sql, err)
		}
	}

	if _, err := pgx.Connect(ctx, "postgres://analyst:wrong-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol"); err == nil {
		t.Fatal("expected failed login for wrong password")
	}

	analystConn, err := pgx.Connect(ctx, "postgres://analyst:analyst-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect analyst: %v", err)
	}
	t.Cleanup(func() { _ = analystConn.Close(ctx) })

	targetLSN := server.walStore.LastLSN()
	historySQL := fmt.Sprintf("SELECT id, email FROM accounts.users /* as-of-lsn: %d */", targetLSN)
	if _, err := analystConn.Query(ctx, historySQL); err == nil || !strings.Contains(err.Error(), "SELECT_HISTORY") {
		t.Fatalf("expected SELECT_HISTORY failure for analyst, got %v", err)
	}

	historianConn, err := pgx.Connect(ctx, "postgres://historian:historian-pass@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect historian: %v", err)
	}
	t.Cleanup(func() { _ = historianConn.Close(ctx) })

	rows, err := historianConn.Query(ctx, historySQL)
	if err != nil {
		t.Fatalf("historian history query: %v", err)
	}
	rows.Close()

	if !handler.hasAuditOperation("auth.login", "success") {
		t.Fatal("missing successful pgwire auth.login audit event")
	}
	if !handler.hasAuditOperation("auth.login", "failure") {
		t.Fatal("missing failed pgwire auth.login audit event")
	}
	if !handler.hasAuditOperation("authz.historical_read", "failure") {
		t.Fatal("missing failed historical-read audit event")
	}
	if !handler.hasAuditOperation("authz.historical_read", "success") {
		t.Fatal("missing successful historical-read audit event")
	}
	if !handler.hasAuditOperationReason("authz.historical_read", "failure", "privilege_denied") {
		t.Fatal("missing privilege_denied historical-read audit reason")
	}

	failedHistoryAudit, ok := handler.auditEntry("authz.historical_read", "failure", "privilege_denied")
	if !ok {
		t.Fatal("missing failed historical-read audit entry")
	}
	if got, want := failedHistoryAudit["historical_target_kind"], "lsn"; got != want {
		t.Fatalf("failed historical-read target kind = %v, want %q", got, want)
	}
	if got, want := failedHistoryAudit["historical_target_lsn"], targetLSN; got != want {
		t.Fatalf("failed historical-read target lsn = %v, want %d", got, want)
	}
	if got, want := failedHistoryAudit["grant_state_scope"], "current"; got != want {
		t.Fatalf("failed historical-read grant state scope = %v, want %q", got, want)
	}
	if got := failedHistoryAudit["principal_has_select_history"]; got != false {
		t.Fatalf("failed historical-read principal_has_select_history = %v, want false", got)
	}
	if got := stringSliceFromAny(failedHistoryAudit["principal_effective_privileges"]); len(got) != 0 {
		t.Fatalf("failed historical-read effective privileges = %v, want empty", got)
	}

	successHistoryAudit, ok := handler.auditEntry("authz.historical_read", "success", "")
	if !ok {
		t.Fatal("missing successful historical-read audit entry")
	}
	if got, want := successHistoryAudit["historical_target_kind"], "lsn"; got != want {
		t.Fatalf("successful historical-read target kind = %v, want %q", got, want)
	}
	if got, want := successHistoryAudit["historical_target_lsn"], targetLSN; got != want {
		t.Fatalf("successful historical-read target lsn = %v, want %d", got, want)
	}
	if got, want := successHistoryAudit["grant_state_scope"], "current"; got != want {
		t.Fatalf("successful historical-read grant state scope = %v, want %q", got, want)
	}
	if got := successHistoryAudit["principal_has_select_history"]; got != true {
		t.Fatalf("successful historical-read principal_has_select_history = %v, want true", got)
	}
	if got := stringSliceFromAny(successHistoryAudit["principal_effective_privileges"]); len(got) != 1 || got[0] != string(executor.PrincipalPrivilegeSelectHistory) {
		t.Fatalf("successful historical-read effective privileges = %v, want [%s]", got, executor.PrincipalPrivilegeSelectHistory)
	}
}

type pgwireAuditCaptureHandler struct {
	mu      sync.Mutex
	records []map[string]any
}

func newPGWireAuditCaptureHandler() *pgwireAuditCaptureHandler {
	return &pgwireAuditCaptureHandler{records: make([]map[string]any, 0)}
}

func (handler *pgwireAuditCaptureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (handler *pgwireAuditCaptureHandler) Handle(_ context.Context, record slog.Record) error {
	entry := map[string]any{"message": record.Message}
	record.Attrs(func(attr slog.Attr) bool {
		entry[attr.Key] = attr.Value.Any()
		return true
	})
	handler.mu.Lock()
	handler.records = append(handler.records, entry)
	handler.mu.Unlock()
	return nil
}

func (handler *pgwireAuditCaptureHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return handler
}

func (handler *pgwireAuditCaptureHandler) WithGroup(string) slog.Handler {
	return handler
}

func (handler *pgwireAuditCaptureHandler) hasAuditOperation(operation, status string) bool {
	return handler.hasAuditOperationReason(operation, status, "")
}

func (handler *pgwireAuditCaptureHandler) hasAuditOperationReason(operation, status, reason string) bool {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	for _, entry := range handler.records {
		event, _ := entry["event"].(string)
		op, _ := entry["operation"].(string)
		state, _ := entry["status"].(string)
		entryReason, _ := entry["reason"].(string)
		if event != "audit" || op != operation || state != status {
			continue
		}
		if reason == "" || entryReason == reason {
			return true
		}
	}
	return false
}

func (handler *pgwireAuditCaptureHandler) auditEntry(operation, status, reason string) (map[string]any, bool) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	for _, entry := range handler.records {
		event, _ := entry["event"].(string)
		op, _ := entry["operation"].(string)
		state, _ := entry["status"].(string)
		entryReason, _ := entry["reason"].(string)
		if event != "audit" || op != operation || state != status {
			continue
		}
		if reason != "" && entryReason != reason {
			continue
		}
		clone := make(map[string]any, len(entry))
		for key, value := range entry {
			clone[key] = value
		}
		return clone, true
	}
	return nil, false
}

func stringSliceFromAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil
			}
			result = append(result, text)
		}
		return result
	default:
		return nil
	}
}

func TestShowUnknownParamFallbackWorksOnExtendedProtocol(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "show-fallback-data")
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

	conn, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	var value string
	if err := conn.QueryRow(ctx, "SHOW application_name").Scan(&value); err != nil {
		t.Fatalf("SHOW application_name: %v", err)
	}
	if value != "" {
		t.Fatalf("SHOW application_name = %q, want empty", value)
	}

	if err := conn.QueryRow(ctx, "SHOW asql_unknown_param").Scan(&value); err == nil {
		t.Fatal("expected unknown asql_* SHOW param to remain an error")
	}
}
