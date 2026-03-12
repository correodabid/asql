package integration

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"asql/internal/engine/executor"
	"asql/internal/storage/wal"
)

func TestBackupWipeRestorePreservesQueryParity(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	walBasePath := filepath.Join(tempDir, "primary.wal")
	backupDir := filepath.Join(tempDir, "backup")

	store, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}

	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	if _, err := engine.Execute(ctx, session, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	if _, err := engine.Execute(ctx, session, "CREATE TABLE users (id INT, email TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := engine.Execute(ctx, session, "INSERT INTO users (id, email) VALUES (1, 'first@asql.dev')"); err != nil {
		t.Fatalf("insert row 1: %v", err)
	}
	if _, err := engine.Execute(ctx, session, "INSERT INTO users (id, email) VALUES (2, 'second@asql.dev')"); err != nil {
		t.Fatalf("insert row 2: %v", err)
	}
	if _, err := engine.Execute(ctx, session, "COMMIT"); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	beforeRows, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users", []string{"accounts"}, store.LastLSN())
	if err != nil {
		t.Fatalf("query parity before backup: %v", err)
	}
	if len(beforeRows.Rows) != 2 {
		t.Fatalf("unexpected rows before backup: got %d want 2", len(beforeRows.Rows))
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close wal store before backup: %v", err)
	}

	// Back up all segment files to a backup directory.
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		t.Fatalf("create backup dir: %v", err)
	}

	// Find segment files (basePath.NNNNNN pattern).
	entries, err := os.ReadDir(filepath.Dir(walBasePath))
	if err != nil {
		t.Fatalf("read wal dir: %v", err)
	}
	base := filepath.Base(walBasePath)
	var segmentFiles []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), base+".") {
			segmentFiles = append(segmentFiles, e.Name())
		}
	}
	if len(segmentFiles) == 0 {
		t.Fatal("no segment files found to backup")
	}

	// Backup each segment file individually.
	backupMeta := make(map[string]wal.BackupMetadata)
	for _, sf := range segmentFiles {
		src := filepath.Join(filepath.Dir(walBasePath), sf)
		dst := filepath.Join(backupDir, sf)
		meta, err := wal.BackupFile(src, dst)
		if err != nil {
			t.Fatalf("backup segment %s: %v", sf, err)
		}
		backupMeta[sf] = meta
	}

	// Wipe original segment files.
	for _, sf := range segmentFiles {
		if err := os.Remove(filepath.Join(filepath.Dir(walBasePath), sf)); err != nil {
			t.Fatalf("wipe segment %s: %v", sf, err)
		}
	}

	// Restore each segment file.
	for _, sf := range segmentFiles {
		src := filepath.Join(backupDir, sf)
		dst := filepath.Join(filepath.Dir(walBasePath), sf)
		if _, err := wal.RestoreFile(src, dst, backupMeta[sf].SHA256); err != nil {
			t.Fatalf("restore segment %s: %v", sf, err)
		}
	}

	restoredStore, err := wal.NewSegmentedLogStore(walBasePath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("open restored wal store: %v", err)
	}
	t.Cleanup(func() { _ = restoredStore.Close() })

	restoredEngine, err := executor.New(ctx, restoredStore, "")
	if err != nil {
		t.Fatalf("new restored engine: %v", err)
	}

	afterRows, err := restoredEngine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users", []string{"accounts"}, restoredStore.LastLSN())
	if err != nil {
		t.Fatalf("query parity after restore: %v", err)
	}
	if len(afterRows.Rows) != len(beforeRows.Rows) {
		t.Fatalf("row count parity mismatch: before=%d after=%d", len(beforeRows.Rows), len(afterRows.Rows))
	}
}
