package wal

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

func TestBackupAndRestoreFileWithIntegrityCheck(t *testing.T) {
	tempDir := t.TempDir()
	walPath := filepath.Join(tempDir, "source.wal")
	backupPath := filepath.Join(tempDir, "backup.wal")
	restorePath := filepath.Join(tempDir, "restored.wal")

	store, err := NewFileLogStore(walPath, AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}

	ctx := context.Background()
	if _, err := store.Append(ctx, ports.WALRecord{TxID: "tx1", Type: "BEGIN", Timestamp: 1, Payload: []byte("a")}); err != nil {
		t.Fatalf("append record 1: %v", err)
	}
	if _, err := store.Append(ctx, ports.WALRecord{TxID: "tx1", Type: "COMMIT", Timestamp: 2, Payload: []byte("b")}); err != nil {
		t.Fatalf("append record 2: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close source wal store: %v", err)
	}

	backupMetadata, err := BackupFile(walPath, backupPath)
	if err != nil {
		t.Fatalf("backup file: %v", err)
	}
	if backupMetadata.Bytes == 0 || backupMetadata.SHA256 == "" {
		t.Fatalf("invalid backup metadata: %+v", backupMetadata)
	}

	restoreMetadata, err := RestoreFile(backupPath, restorePath, backupMetadata.SHA256)
	if err != nil {
		t.Fatalf("restore file: %v", err)
	}
	if restoreMetadata.SHA256 != backupMetadata.SHA256 {
		t.Fatalf("checksum mismatch after restore: got=%s want=%s", restoreMetadata.SHA256, backupMetadata.SHA256)
	}

	restoredStore, err := NewFileLogStore(restorePath, AlwaysSync{})
	if err != nil {
		t.Fatalf("open restored wal store: %v", err)
	}
	t.Cleanup(func() { _ = restoredStore.Close() })

	recovered, err := restoredStore.Recover(ctx)
	if err != nil {
		t.Fatalf("recover restored wal: %v", err)
	}
	if len(recovered) != 2 {
		t.Fatalf("unexpected recovered records count: got %d want 2", len(recovered))
	}
}

func TestRestoreFileRejectsChecksumMismatch(t *testing.T) {
	tempDir := t.TempDir()
	sourcePath := filepath.Join(tempDir, "source.wal")
	backupPath := filepath.Join(tempDir, "backup.wal")
	restorePath := filepath.Join(tempDir, "restored.wal")

	store, err := NewFileLogStore(sourcePath, AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}

	ctx := context.Background()
	if _, err := store.Append(ctx, ports.WALRecord{TxID: "tx1", Type: "BEGIN", Timestamp: 1, Payload: []byte("a")}); err != nil {
		t.Fatalf("append record: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close source wal store: %v", err)
	}

	if _, err := BackupFile(sourcePath, backupPath); err != nil {
		t.Fatalf("backup file: %v", err)
	}

	if _, err := RestoreFile(backupPath, restorePath, "deadbeef"); err == nil {
		t.Fatal("expected checksum mismatch restore error")
	}
}
