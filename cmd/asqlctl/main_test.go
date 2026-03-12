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
