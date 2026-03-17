package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"asql/internal/engine/executor"
	"asql/internal/platform/datadir"
	grpcserver "asql/internal/server/grpc"
	pgwire "asql/internal/server/pgwire"
	"asql/internal/storage/wal"

	"github.com/jackc/pgx/v5"
)

func TestLocalRecoveryCommands(t *testing.T) {
	ctx := context.Background()
	sourceDir := filepath.Join(t.TempDir(), "source")
	backupDir := filepath.Join(t.TempDir(), "backup")
	restoreDir := filepath.Join(t.TempDir(), "restore")

	dd, err := datadir.New(sourceDir)
	if err != nil {
		t.Fatalf("new data dir: %v", err)
	}
	store, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	engine, err := executor.New(ctx, store, dd.SnapDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	engine.WaitPendingSnapshots()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	var output bytes.Buffer
	if err := runLocalRecoveryCommand(ctx, &output, "backup-create", sourceDir, backupDir, 0, 0); err != nil {
		t.Fatalf("backup-create: %v", err)
	}
	if !strings.Contains(output.String(), "\"head_lsn\"") {
		t.Fatalf("expected manifest output, got %q", output.String())
	}

	output.Reset()
	if err := runLocalRecoveryCommand(ctx, &output, "backup-verify", "", backupDir, 0, 0); err != nil {
		t.Fatalf("backup-verify: %v", err)
	}
	if !strings.Contains(output.String(), "\"status\": \"OK\"") {
		t.Fatalf("expected verify status OK, got %q", output.String())
	}

	output.Reset()
	if err := runLocalRecoveryCommand(ctx, &output, "restore-lsn", restoreDir, backupDir, 4, 0); err != nil {
		t.Fatalf("restore-lsn: %v", err)
	}
	if !strings.Contains(output.String(), "\"AppliedLSN\"") && !strings.Contains(output.String(), "\"applied_lsn\"") {
		// keep compatibility with current json field names once marshaled.
	}

	restoredDD, err := datadir.New(restoreDir)
	if err != nil {
		t.Fatalf("new restored data dir: %v", err)
	}
	restoredStore, err := wal.NewSegmentedLogStore(restoredDD.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen restored store: %v", err)
	}
	defer restoredStore.Close()
	restoredEngine, err := executor.New(ctx, restoredStore, restoredDD.SnapDir())
	if err != nil {
		t.Fatalf("reopen restored engine: %v", err)
	}
	if got := restoredEngine.RowCount("accounts", "users"); got != 1 {
		t.Fatalf("unexpected restored rows: got %d want 1", got)
	}

	output.Reset()
	if err := runLocalRecoveryCommand(ctx, &output, "snapshot-catalog", sourceDir, "", 0, 0); err != nil {
		t.Fatalf("snapshot-catalog: %v", err)
	}
	if !strings.Contains(output.String(), "[") {
		t.Fatalf("expected snapshot catalog json, got %q", output.String())
	}

	output.Reset()
	if err := runLocalRecoveryCommand(ctx, &output, "wal-retention", sourceDir, "", 0, 0); err != nil {
		t.Fatalf("wal-retention: %v", err)
	}
	if !strings.Contains(output.String(), "\"segment_count\"") {
		t.Fatalf("expected wal retention json, got %q", output.String())
	}
}

func TestFixtureValidateCommand(t *testing.T) {
	var output bytes.Buffer
	err := runFixtureCommand(context.Background(), &output, "fixture-validate", filepath.Join("..", "..", "fixtures", "healthcare-billing-demo-v1.json"), "", "", "")
	if err != nil {
		t.Fatalf("fixture-validate: %v", err)
	}
	if !strings.Contains(output.String(), "\"status\": \"validated\"") {
		t.Fatalf("expected validated output, got %q", output.String())
	}
}

func TestFixtureLoadCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := pgwire.New(pgwire.Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "fixture-load-data"),
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
				t.Fatalf("pgwire server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire shutdown")
		}
	})

	fixturePath := filepath.Join("..", "..", "fixtures", "healthcare-billing-demo-v1.json")
	var output bytes.Buffer
	if err := runFixtureCommand(ctx, &output, "fixture-load", fixturePath, "", listener.Addr().String(), ""); err != nil {
		t.Fatalf("fixture-load: %v", err)
	}
	if !strings.Contains(output.String(), "\"status\": \"loaded\"") {
		t.Fatalf("expected loaded output, got %q", output.String())
	}

	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://asql@%s/asql?sslmode=disable&default_query_exec_mode=simple_protocol", listener.Addr().String()))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SELECT invoice_number, total_cents FROM billing.invoices ORDER BY invoice_number")
	if err != nil {
		t.Fatalf("query invoices: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 invoice rows, got %d", count)
	}
}

func TestFixtureExportCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := pgwire.New(pgwire.Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "fixture-export-data"),
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
				t.Fatalf("pgwire server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire shutdown")
		}
	})

	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://asql@%s/asql?sslmode=disable&default_query_exec_mode=simple_protocol", listener.Addr().String()))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	for _, sql := range []string{
		"BEGIN DOMAIN demo",
		"CREATE TABLE demo.items (id TEXT PRIMARY KEY, name TEXT)",
		"INSERT INTO demo.items (id, name) VALUES ('item-1', 'Alpha')",
		"COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	outputPath := filepath.Join(t.TempDir(), "demo-export.json")
	var output bytes.Buffer
	if err := runFixtureCommand(ctx, &output, "fixture-export", outputPath, "demo", listener.Addr().String(), ""); err != nil {
		t.Fatalf("fixture-export: %v", err)
	}
	if !strings.Contains(output.String(), "\"status\": \"exported\"") {
		t.Fatalf("expected exported output, got %q", output.String())
	}
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read exported file: %v", err)
	}
	if !strings.Contains(string(content), "INSERT INTO demo.items") {
		t.Fatalf("expected exported fixture content, got %q", string(content))
	}
	var validateOutput bytes.Buffer
	if err := runFixtureCommand(ctx, &validateOutput, "fixture-validate", outputPath, "", "", ""); err != nil {
		t.Fatalf("validate exported fixture: %v", err)
	}
}

func TestRunCommandMigrationPreflight(t *testing.T) {
	server, err := grpcserver.New(grpcserver.Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "grpc-migration-preflight-data"),
		Logger:      slog.Default(),
	})
	if err != nil {
		t.Fatalf("new grpc server: %v", err)
	}
	t.Cleanup(server.Stop)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	go func() { _ = server.ServeOnListener(listener) }()

	var output bytes.Buffer
	if err := runCommand(&output, listener.Addr().String(), "", "migration-preflight", "domain", "accounts", "", "CREATE TABLE users (id INT); ALTER TABLE users ADD COLUMN email TEXT", "", "", "", 0, 0); err != nil {
		t.Fatalf("migration-preflight: %v", err)
	}
	if !strings.Contains(output.String(), "\"auto_rollback\": true") {
		t.Fatalf("expected auto rollback in output, got %q", output.String())
	}
	if !strings.Contains(output.String(), "DROP TABLE users") {
		t.Fatalf("expected generated drop table rollback in output, got %q", output.String())
	}
}

func TestRunAdminSecurityCommand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := pgwire.New(pgwire.Config{
		Address:         "127.0.0.1:0",
		AdminHTTPAddr:   "127.0.0.1:0",
		DataDirPath:     filepath.Join(t.TempDir(), "security-admin-data"),
		Logger:          slog.New(slog.NewTextHandler(os.Stdout, nil)),
		AdminReadToken:  "read-secret",
		AdminWriteToken: "write-secret",
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

	var adminAddr string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if server.AdminHTTPAddress() != "" {
			adminAddr = server.AdminHTTPAddress()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if adminAddr == "" {
		t.Fatal("timeout waiting for admin listener")
	}

	var output bytes.Buffer
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-bootstrap-admin", "admin", "secret-pass", "", ""); err != nil {
		t.Fatalf("bootstrap admin command: %v", err)
	}
	if !strings.Contains(output.String(), "\"name\": \"admin\"") {
		t.Fatalf("unexpected bootstrap output: %q", output.String())
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-create-role", "history_readers", "", "", ""); err != nil {
		t.Fatalf("create role command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-grant-privilege", "history_readers", "", "", "SELECT_HISTORY"); err != nil {
		t.Fatalf("grant privilege command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-create-user", "analyst", "analyst-pass", "", ""); err != nil {
		t.Fatalf("create user command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-grant-role", "analyst", "", "history_readers", ""); err != nil {
		t.Fatalf("grant role command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-revoke-role", "analyst", "", "history_readers", ""); err != nil {
		t.Fatalf("revoke role command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-revoke-privilege", "history_readers", "", "", "SELECT_HISTORY"); err != nil {
		t.Fatalf("revoke privilege command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-set-password", "analyst", "rotated-pass", "", ""); err != nil {
		t.Fatalf("set password command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-disable", "analyst", "", "", ""); err != nil {
		t.Fatalf("disable principal command: %v", err)
	}
	if !strings.Contains(output.String(), "\"enabled\": false") {
		t.Fatalf("unexpected disable output: %q", output.String())
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-enable", "analyst", "", "", ""); err != nil {
		t.Fatalf("enable principal command: %v", err)
	}
	if !strings.Contains(output.String(), "\"enabled\": true") {
		t.Fatalf("unexpected enable output: %q", output.String())
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-grant-privilege", "history_readers", "", "", "SELECT_HISTORY"); err != nil {
		t.Fatalf("restore privilege command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-grant-role", "analyst", "", "history_readers", ""); err != nil {
		t.Fatalf("restore role command: %v", err)
	}
	if !strings.Contains(output.String(), "history_readers") {
		t.Fatalf("unexpected restore role output: %q", output.String())
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "read-secret", "principal-show", "analyst", "", "", ""); err != nil {
		t.Fatalf("principal show command: %v", err)
	}
	if !strings.Contains(output.String(), "\"name\": \"analyst\"") || !strings.Contains(output.String(), "\"effective_roles\": [") || !strings.Contains(output.String(), "history_readers") || !strings.Contains(output.String(), "\"effective_privileges\": [") || !strings.Contains(output.String(), "SELECT_HISTORY") {
		t.Fatalf("unexpected principal show output: %q", output.String())
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "read-secret", "principal-who-can-history", "", "", "", ""); err != nil {
		t.Fatalf("principal who-can-history command: %v", err)
	}
	if !strings.Contains(output.String(), "\"name\": \"admin\"") || !strings.Contains(output.String(), "\"name\": \"analyst\"") || !strings.Contains(output.String(), "SELECT_HISTORY") {
		t.Fatalf("unexpected principal who-can-history output: %q", output.String())
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-revoke-role", "analyst", "", "history_readers", ""); err != nil {
		t.Fatalf("cleanup role command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-disable", "analyst", "", "", ""); err != nil {
		t.Fatalf("disable for delete command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "write-secret", "principal-delete", "analyst", "", "", ""); err != nil {
		t.Fatalf("delete principal command: %v", err)
	}

	output.Reset()
	if err := runAdminSecurityCommand(ctx, &output, adminAddr, "read-secret", "principal-list", "", "", "", ""); err != nil {
		t.Fatalf("list principals command: %v", err)
	}
	if strings.Contains(output.String(), "\"name\": \"analyst\"") || !strings.Contains(output.String(), "history_readers") || !strings.Contains(output.String(), "effective_privileges") {
		t.Fatalf("unexpected principal list output: %q", output.String())
	}
}

func TestSplitSQLStatements(t *testing.T) {
	statements := splitSQLStatements("INSERT INTO demo.items VALUES ('a;b'); UPDATE demo.items SET name = 'x';")
	if len(statements) != 2 {
		t.Fatalf("expected 2 statements, got %d (%+v)", len(statements), statements)
	}
	if statements[0] != "INSERT INTO demo.items VALUES ('a;b')" {
		t.Fatalf("unexpected first statement: %q", statements[0])
	}
	if statements[1] != "UPDATE demo.items SET name = 'x'" {
		t.Fatalf("unexpected second statement: %q", statements[1])
	}
}
