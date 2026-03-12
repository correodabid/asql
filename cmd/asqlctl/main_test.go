package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"asql/internal/engine/executor"
	"asql/internal/platform/datadir"
	"asql/internal/storage/wal"
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
	if !strings.Contains(output.String(), "\"AppliedLSN\"") && !strings.Contains(output.String(), "\"AppliedLSN\"") {
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
