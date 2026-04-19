package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/correodabid/asql/internal/platform/datadir"
	"github.com/correodabid/asql/internal/storage/wal"
)

const (
	baseBackupManifestVersion = 1
	baseBackupManifestName    = "base-backup.json"
)

type backupFileMetadata struct {
	RelativePath string `json:"relative_path"`
	Bytes        int64  `json:"bytes"`
	SHA256       string `json:"sha256"`
}

// SnapshotBackupMetadata describes one persisted snapshot file in a base backup.
type SnapshotBackupMetadata struct {
	backupFileMetadata
	Sequence  uint64 `json:"sequence"`
	LSN       uint64 `json:"lsn"`
	LogicalTS uint64 `json:"logical_ts"`
}

// WALSegmentBackupMetadata describes one WAL segment in a base backup.
type WALSegmentBackupMetadata struct {
	backupFileMetadata
	SeqNum      uint32 `json:"seq_num"`
	FirstLSN    uint64 `json:"first_lsn"`
	LastLSN     uint64 `json:"last_lsn"`
	RecordCount uint32 `json:"record_count"`
}

// BaseBackupManifest is the on-disk metadata format for recovery backups.
type BaseBackupManifest struct {
	Version        int                        `json:"version"`
	HeadLSN        uint64                     `json:"head_lsn"`
	HeadTimestamp  uint64                     `json:"head_timestamp"`
	Snapshots      []SnapshotBackupMetadata   `json:"snapshots"`
	WALSegments    []WALSegmentBackupMetadata `json:"wal_segments"`
	TimestampIndex *backupFileMetadata        `json:"timestamp_index,omitempty"`
}

// RestoreResult reports the exact recovery boundary applied to a restored data dir.
type RestoreResult struct {
	AppliedLSN       uint64
	AppliedTimestamp uint64
}

var errBaseBackupChecksumMismatch = errors.New("base backup checksum mismatch")

// CreateBaseBackup copies the recoverable data-dir artifacts into backupDir and writes a manifest.
func CreateBaseBackup(sourceDataDir, backupDir string) (BaseBackupManifest, error) {
	source, err := datadir.New(sourceDataDir)
	if err != nil {
		return BaseBackupManifest{}, fmt.Errorf("open source data dir: %w", err)
	}
	backup, err := datadir.New(backupDir)
	if err != nil {
		return BaseBackupManifest{}, fmt.Errorf("open backup data dir: %w", err)
	}
	if err := cleanDirContents(backup.WALDir()); err != nil {
		return BaseBackupManifest{}, err
	}
	if err := cleanDirContents(backup.SnapDir()); err != nil {
		return BaseBackupManifest{}, err
	}

	walCatalog, err := wal.CatalogSegments(source.WALBasePath())
	if err != nil {
		return BaseBackupManifest{}, fmt.Errorf("catalog wal segments: %w", err)
	}
	manifest := BaseBackupManifest{Version: baseBackupManifestVersion}
	for _, segment := range walCatalog {
		src := filepath.Join(source.WALDir(), segment.FileName)
		dst := filepath.Join(backup.WALDir(), segment.FileName)
		meta, err := copyFileWithIntegrity(src, dst)
		if err != nil {
			return BaseBackupManifest{}, err
		}
		manifest.WALSegments = append(manifest.WALSegments, WALSegmentBackupMetadata{
			backupFileMetadata: meta.withRelativePath(filepath.ToSlash(filepath.Join("wal", segment.FileName))),
			SeqNum:             segment.SeqNum,
			FirstLSN:           segment.FirstLSN,
			LastLSN:            segment.LastLSN,
			RecordCount:        segment.RecordCount,
		})
		if segment.LastLSN > manifest.HeadLSN {
			manifest.HeadLSN = segment.LastLSN
		}
	}

	snapshots, err := catalogSnapshotFiles(source.SnapDir())
	if err != nil {
		return BaseBackupManifest{}, fmt.Errorf("catalog snapshot files: %w", err)
	}
	for _, snapshot := range snapshots {
		src := filepath.Join(source.SnapDir(), snapshot.FileName)
		dst := filepath.Join(backup.SnapDir(), snapshot.FileName)
		meta, err := copyFileWithIntegrity(src, dst)
		if err != nil {
			return BaseBackupManifest{}, err
		}
		manifest.Snapshots = append(manifest.Snapshots, SnapshotBackupMetadata{
			backupFileMetadata: meta.withRelativePath(filepath.ToSlash(filepath.Join("snap", snapshot.FileName))),
			Sequence:           snapshot.Sequence,
			LSN:                snapshot.LSN,
			LogicalTS:          snapshot.LogicalTS,
		})
		if snapshot.LogicalTS > manifest.HeadTimestamp {
			manifest.HeadTimestamp = snapshot.LogicalTS
		}
	}

	timestampIndexPath := filepath.Join(source.Root(), timestampIndexFileName)
	if _, err := os.Stat(timestampIndexPath); err == nil {
		meta, err := copyFileWithIntegrity(timestampIndexPath, filepath.Join(backup.Root(), timestampIndexFileName))
		if err != nil {
			return BaseBackupManifest{}, err
		}
		entry := meta.withRelativePath(timestampIndexFileName)
		manifest.TimestampIndex = &entry
	}

	if manifest.HeadTimestamp == 0 {
		sourceStore, err := wal.NewSegmentedLogStore(source.WALBasePath(), wal.AlwaysSync{})
		if err == nil {
			records, readErr := sourceStore.ReadFrom(context.Background(), 1, 0)
			if readErr == nil && len(records) > 0 {
				manifest.HeadTimestamp = records[len(records)-1].Timestamp
			}
			_ = sourceStore.Close()
		}
	}

	if err := writeBaseBackupManifest(filepath.Join(backup.MetaDir(), baseBackupManifestName), manifest); err != nil {
		return BaseBackupManifest{}, err
	}
	return manifest, nil
}

// LoadBaseBackupManifest loads the backup manifest from a backup directory.
func LoadBaseBackupManifest(backupDir string) (BaseBackupManifest, error) {
	path := filepath.Join(backupDir, "meta", baseBackupManifestName)
	data, err := os.ReadFile(path)
	if err != nil {
		return BaseBackupManifest{}, fmt.Errorf("read base backup manifest: %w", err)
	}
	var manifest BaseBackupManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return BaseBackupManifest{}, fmt.Errorf("decode base backup manifest: %w", err)
	}
	return manifest, nil
}

// VerifyBaseBackup validates the manifest and all referenced backup artifacts.
func VerifyBaseBackup(backupDir string) (BaseBackupManifest, error) {
	manifest, err := LoadBaseBackupManifest(backupDir)
	if err != nil {
		return BaseBackupManifest{}, err
	}
	for _, snapshot := range manifest.Snapshots {
		if err := verifyBackupFile(backupDir, snapshot.backupFileMetadata); err != nil {
			return BaseBackupManifest{}, err
		}
	}
	for _, segment := range manifest.WALSegments {
		if err := verifyBackupFile(backupDir, segment.backupFileMetadata); err != nil {
			return BaseBackupManifest{}, err
		}
	}
	if manifest.TimestampIndex != nil {
		if err := verifyBackupFile(backupDir, *manifest.TimestampIndex); err != nil {
			return BaseBackupManifest{}, err
		}
	}
	return manifest, nil
}

// RestoreBaseBackupToLSN restores a backup into targetDataDir trimmed to targetLSN.
func RestoreBaseBackupToLSN(ctx context.Context, backupDir, targetDataDir string, targetLSN uint64) (RestoreResult, error) {
	manifest, err := VerifyBaseBackup(backupDir)
	if err != nil {
		return RestoreResult{}, err
	}
	if targetLSN > manifest.HeadLSN {
		return RestoreResult{}, fmt.Errorf("target lsn %d exceeds backup head %d", targetLSN, manifest.HeadLSN)
	}

	backupDD, err := datadir.New(backupDir)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("open backup data dir: %w", err)
	}
	targetDD, err := datadir.New(targetDataDir)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("open target data dir: %w", err)
	}
	if err := cleanDirContents(targetDD.WALDir()); err != nil {
		return RestoreResult{}, err
	}
	if err := cleanDirContents(targetDD.SnapDir()); err != nil {
		return RestoreResult{}, err
	}
	if err := cleanDirContents(targetDD.MetaDir()); err != nil {
		return RestoreResult{}, err
	}
	_ = os.Remove(filepath.Join(targetDD.Root(), timestampIndexFileName))

	backupStore, err := wal.NewSegmentedLogStore(backupDD.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		return RestoreResult{}, fmt.Errorf("open backup wal store: %w", err)
	}
	defer backupStore.Close()

	targetStore, err := wal.NewSegmentedLogStore(targetDD.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		return RestoreResult{}, fmt.Errorf("open target wal store: %w", err)
	}

	records, err := backupStore.ReadFrom(ctx, 1, 0)
	if err != nil {
		targetStore.Close()
		return RestoreResult{}, fmt.Errorf("read backup wal: %w", err)
	}
	var appliedTimestamp uint64
	for _, record := range records {
		if record.LSN > targetLSN {
			break
		}
		if _, err := targetStore.Append(ctx, record); err != nil {
			targetStore.Close()
			return RestoreResult{}, fmt.Errorf("append restored wal record: %w", err)
		}
		appliedTimestamp = record.Timestamp
	}
	if err := targetStore.Close(); err != nil {
		return RestoreResult{}, fmt.Errorf("close target wal store: %w", err)
	}

	replayedStore, err := wal.NewSegmentedLogStore(targetDD.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		return RestoreResult{}, fmt.Errorf("reopen target wal store: %w", err)
	}
	engine, err := New(ctx, replayedStore, targetDD.SnapDir())
	if err != nil {
		replayedStore.Close()
		return RestoreResult{}, fmt.Errorf("open restored engine: %w", err)
	}
	state := engine.readState.Load()
	engine.writeMu.Lock()
	catalog := cloneCatalog(engine.catalog)
	engine.writeMu.Unlock()
	engine.snapshots.mu.Lock()
	engine.snapshots.clear()
	engine.snapshots.add(captureSnapshotWithCatalog(state, catalog))
	engine.snapshots.mu.Unlock()
	engine.persistAllSnapshots()
	engine.WaitPendingSnapshots()
	if err := replayedStore.Close(); err != nil {
		return RestoreResult{}, fmt.Errorf("close restored wal store: %w", err)
	}

	return RestoreResult{AppliedLSN: targetLSN, AppliedTimestamp: appliedTimestamp}, nil
}

// RestoreBaseBackupToTimestamp restores a backup to the latest LSN <= logicalTimestamp.
func RestoreBaseBackupToTimestamp(ctx context.Context, backupDir, targetDataDir string, logicalTimestamp uint64) (RestoreResult, error) {
	backupDD, err := datadir.New(backupDir)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("open backup data dir: %w", err)
	}
	backupStore, err := wal.NewSegmentedLogStore(backupDD.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		return RestoreResult{}, fmt.Errorf("open backup wal store: %w", err)
	}
	engine, err := New(ctx, backupStore, backupDD.SnapDir())
	if err != nil {
		backupStore.Close()
		return RestoreResult{}, fmt.Errorf("open backup engine: %w", err)
	}
	targetLSN, err := engine.LSNForTimestamp(ctx, logicalTimestamp)
	engine.WaitPendingSnapshots()
	if closeErr := backupStore.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		return RestoreResult{}, fmt.Errorf("resolve backup restore timestamp: %w", err)
	}
	return RestoreBaseBackupToLSN(ctx, backupDir, targetDataDir, targetLSN)
}

type snapshotCatalogEntry struct {
	FileName  string
	Sequence  uint64
	LSN       uint64
	LogicalTS uint64
}

func catalogSnapshotFiles(snapDir string) ([]snapshotCatalogEntry, error) {
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read snapshot dir: %w", err)
	}
	result := make([]snapshotCatalogEntry, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), snapFilePrefix) {
			continue
		}
		seqStr := strings.TrimPrefix(entry.Name(), snapFilePrefix)
		var seq uint64
		if _, err := fmt.Sscanf(seqStr, "%06d", &seq); err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(snapDir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read snapshot file %s: %w", entry.Name(), err)
		}
		if isZstd(data) {
			data, err = decompressZstd(data)
			if err != nil {
				return nil, fmt.Errorf("decompress snapshot file %s: %w", entry.Name(), err)
			}
		}
		decoded, err := decodeSnapshotFileBinaryRaw(data)
		if err != nil || len(decoded) == 0 {
			return nil, fmt.Errorf("decode snapshot file %s: %w", entry.Name(), err)
		}
		last := decoded[len(decoded)-1]
		result = append(result, snapshotCatalogEntry{FileName: entry.Name(), Sequence: seq, LSN: last.lsn, LogicalTS: last.logicalTS})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Sequence < result[j].Sequence })
	return result, nil
}

func writeBaseBackupManifest(path string, manifest BaseBackupManifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir backup manifest dir: %w", err)
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode base backup manifest: %w", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write base backup manifest: %w", err)
	}
	return nil
}

func cleanDirContents(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read directory %s: %w", dir, err)
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("clean directory %s: %w", dir, err)
		}
	}
	return nil
}

func copyFileWithIntegrity(sourcePath, destinationPath string) (backupFileMetadata, error) {
	source, err := os.Open(sourcePath)
	if err != nil {
		return backupFileMetadata{}, fmt.Errorf("open source file %s: %w", sourcePath, err)
	}
	defer source.Close()
	if err := os.MkdirAll(filepath.Dir(destinationPath), 0o755); err != nil {
		return backupFileMetadata{}, fmt.Errorf("mkdir destination dir: %w", err)
	}
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return backupFileMetadata{}, fmt.Errorf("open destination file %s: %w", destinationPath, err)
	}
	defer destination.Close()
	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(destination, hasher), source)
	if err != nil {
		return backupFileMetadata{}, fmt.Errorf("copy %s to %s: %w", sourcePath, destinationPath, err)
	}
	return backupFileMetadata{Bytes: written, SHA256: hex.EncodeToString(hasher.Sum(nil))}, nil
}

func (metadata backupFileMetadata) withRelativePath(path string) backupFileMetadata {
	metadata.RelativePath = path
	return metadata
}

func verifyBackupFile(backupDir string, metadata backupFileMetadata) error {
	path := filepath.Join(backupDir, filepath.FromSlash(metadata.RelativePath))
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open backup artifact %s: %w", metadata.RelativePath, err)
	}
	defer file.Close()
	hasher := sha256.New()
	written, err := io.Copy(hasher, file)
	if err != nil {
		return fmt.Errorf("read backup artifact %s: %w", metadata.RelativePath, err)
	}
	if metadata.Bytes > 0 && written != metadata.Bytes {
		return fmt.Errorf("backup artifact size mismatch for %s: got=%d want=%d", metadata.RelativePath, written, metadata.Bytes)
	}
	actual := hex.EncodeToString(hasher.Sum(nil))
	if metadata.SHA256 != "" && actual != metadata.SHA256 {
		return fmt.Errorf("%w for %s: got=%s want=%s", errBaseBackupChecksumMismatch, metadata.RelativePath, actual, metadata.SHA256)
	}
	return nil
}
