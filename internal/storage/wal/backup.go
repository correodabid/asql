package wal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
)

// BackupMetadata contains backup integrity metadata.
type BackupMetadata struct {
	Bytes  int64
	SHA256 string
}

// BackupFile copies a WAL source file to destination and returns integrity metadata.
func BackupFile(sourcePath, destinationPath string) (BackupMetadata, error) {
	source, err := os.Open(sourcePath)
	if err != nil {
		return BackupMetadata{}, fmt.Errorf("open source wal: %w", err)
	}
	defer func() {
		if closeErr := source.Close(); closeErr != nil {
			slog.Debug("backup source file close error", "error", closeErr.Error())
		}
	}()

	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return BackupMetadata{}, fmt.Errorf("open destination backup: %w", err)
	}
	defer func() {
		if closeErr := destination.Close(); closeErr != nil {
			slog.Debug("backup destination file close error", "error", closeErr.Error())
		}
	}()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(destination, hasher), source)
	if err != nil {
		return BackupMetadata{}, fmt.Errorf("copy wal to backup: %w", err)
	}

	return BackupMetadata{Bytes: written, SHA256: hex.EncodeToString(hasher.Sum(nil))}, nil
}

// RestoreFile restores a WAL file from backup and validates checksum metadata.
func RestoreFile(backupPath, destinationPath, expectedSHA256 string) (BackupMetadata, error) {
	backup, err := os.Open(backupPath)
	if err != nil {
		return BackupMetadata{}, fmt.Errorf("open backup file: %w", err)
	}
	defer func() {
		if closeErr := backup.Close(); closeErr != nil {
			slog.Debug("restore backup file close error", "error", closeErr.Error())
		}
	}()

	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return BackupMetadata{}, fmt.Errorf("open restore destination: %w", err)
	}
	defer func() {
		if closeErr := destination.Close(); closeErr != nil {
			slog.Debug("restore destination file close error", "error", closeErr.Error())
		}
	}()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(destination, hasher), backup)
	if err != nil {
		return BackupMetadata{}, fmt.Errorf("restore wal from backup: %w", err)
	}

	actualSHA := hex.EncodeToString(hasher.Sum(nil))
	if expectedSHA256 != "" && actualSHA != expectedSHA256 {
		return BackupMetadata{}, fmt.Errorf("restore checksum mismatch: got=%s want=%s", actualSHA, expectedSHA256)
	}

	return BackupMetadata{Bytes: written, SHA256: actualSHA}, nil
}
