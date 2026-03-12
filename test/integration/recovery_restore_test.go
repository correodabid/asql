package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"asql/internal/engine/executor"
	"asql/internal/platform/datadir"
	"asql/internal/storage/wal"
)

func TestBaseBackupRestoreToLSNAndTimestamp(t *testing.T) {
	ctx := context.Background()
	sourceDir := filepath.Join(t.TempDir(), "source")
	sourceDD, err := datadir.New(sourceDir)
	if err != nil {
		t.Fatalf("new source data dir: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(sourceDD.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	engine, err := executor.New(ctx, store, sourceDD.SnapDir())
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
	session = engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	engine.WaitPendingSnapshots()
	if err := store.Close(); err != nil {
		t.Fatalf("close source store: %v", err)
	}

	backupDir := filepath.Join(filepath.Dir(sourceDir), "backup")
	manifest, err := executor.CreateBaseBackup(sourceDir, backupDir)
	if err != nil {
		t.Fatalf("create base backup: %v", err)
	}
	if len(manifest.WALSegments) == 0 {
		t.Fatal("expected wal segment metadata in base backup manifest")
	}
	if manifest.HeadLSN < 7 {
		t.Fatalf("unexpected manifest head lsn: %d", manifest.HeadLSN)
	}

	restoreLSNDir := filepath.Join(filepath.Dir(sourceDir), "restore-lsn")
	result, err := executor.RestoreBaseBackupToLSN(ctx, backupDir, restoreLSNDir, 4)
	if err != nil {
		t.Fatalf("restore to lsn: %v", err)
	}
	if result.AppliedLSN != 4 {
		t.Fatalf("unexpected restore lsn: got %d want 4", result.AppliedLSN)
	}
	assertRestoredRows(t, ctx, restoreLSNDir, 1)
	assertRestoredRows(t, ctx, restoreLSNDir, 1) // survives restart

	restoreTSDir := filepath.Join(filepath.Dir(sourceDir), "restore-ts")
	result, err = executor.RestoreBaseBackupToTimestamp(ctx, backupDir, restoreTSDir, 4)
	if err != nil {
		t.Fatalf("restore to timestamp: %v", err)
	}
	if result.AppliedLSN != 4 {
		t.Fatalf("unexpected restore-to-timestamp lsn: got %d want 4", result.AppliedLSN)
	}
	assertRestoredRows(t, ctx, restoreTSDir, 1)
}

func TestBaseBackupVerificationFailsOnChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	sourceDir := filepath.Join(t.TempDir(), "source")
	backupDir := filepath.Join(t.TempDir(), "backup")
	restoreDir := filepath.Join(t.TempDir(), "restore")
	sourceDD, err := datadir.New(sourceDir)
	if err != nil {
		t.Fatalf("new source data dir: %v", err)
	}
	store, err := wal.NewSegmentedLogStore(sourceDD.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	engine, err := executor.New(ctx, store, sourceDD.SnapDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT)",
		"INSERT INTO users (id) VALUES (1)",
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
	manifest, err := executor.CreateBaseBackup(sourceDir, backupDir)
	if err != nil {
		t.Fatalf("create base backup: %v", err)
	}
	segmentPath := filepath.Join(backupDir, filepath.FromSlash(manifest.WALSegments[0].RelativePath))
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("read backup segment: %v", err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(segmentPath, data, 0o644); err != nil {
		t.Fatalf("write corrupted backup segment: %v", err)
	}
	if _, err := executor.VerifyBaseBackup(backupDir); err == nil {
		t.Fatal("expected backup verification failure after corruption")
	} else if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected checksum mismatch error, got %v", err)
	}
	if _, err := executor.RestoreBaseBackupToLSN(ctx, backupDir, restoreDir, 4); err == nil {
		t.Fatal("expected restore failure for corrupted backup")
	}
}

func assertRestoredRows(t *testing.T, ctx context.Context, dataDir string, want int) {
	t.Helper()
	dd, err := datadir.New(dataDir)
	if err != nil {
		t.Fatalf("open restored data dir: %v", err)
	}
	store, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("open restored wal store: %v", err)
	}
	defer store.Close()
	engine, err := executor.New(ctx, store, dd.SnapDir())
	if err != nil {
		t.Fatalf("open restored engine: %v", err)
	}
	if got := engine.RowCount("accounts", "users"); got != want {
		t.Fatalf("unexpected restored row count: got %d want %d", got, want)
	}
}
