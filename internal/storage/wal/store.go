package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
)

// syncWithRetry attempts fsync up to maxSyncRetries times with exponential backoff.
const maxSyncRetries = 3

func syncWithRetry(file *os.File) error {
	var err error
	for attempt := range maxSyncRetries {
		if err = PlatformSync(file); err == nil {
			return nil
		}
		backoff := time.Duration(1<<attempt) * time.Millisecond
		slog.Warn("wal fsync failed, retrying", "attempt", attempt+1, "error", err.Error(), "backoff", backoff.String())
		time.Sleep(backoff)
	}
	return err
}

var (
	errInvalidVersion = errors.New("invalid wal record version")
	errChecksum       = errors.New("invalid wal record checksum")
	errOutOfOrderLSN  = errors.New("out-of-order replicated lsn")
)

// SyncStrategy controls when FileLogStore flushes WAL appends to disk.
type SyncStrategy interface {
	ShouldSync(appendCount uint64) bool
}

// AlwaysSync flushes every append to disk.
type AlwaysSync struct{}

// ShouldSync indicates if append should be synced.
func (AlwaysSync) ShouldSync(uint64) bool {
	return true
}

// EveryN flushes every N appends.
type EveryN struct {
	N uint64
}

// ShouldSync indicates if append should be synced.
func (strategy EveryN) ShouldSync(appendCount uint64) bool {
	if strategy.N == 0 {
		return true
	}

	return appendCount%strategy.N == 0
}

type diskRecord struct {
	Version   uint16
	LSN       uint64
	Term      uint64
	TxID      string
	Type      string
	Timestamp uint64
	Payload   []byte
	Checksum  uint32
}

// FileLogStore stores WAL records in a length-prefixed file format.
type FileLogStore struct {
	mu           sync.Mutex
	syncMu       sync.Mutex // serializes fsync and file-replacement ops; never held during appends
	file         *os.File
	syncStrategy SyncStrategy
	appendCount  uint64
	lastLSN      uint64
}

// NewFileLogStore initializes or opens a file-backed WAL store.
func NewFileLogStore(path string, syncStrategy SyncStrategy) (*FileLogStore, error) {
	if syncStrategy == nil {
		syncStrategy = AlwaysSync{}
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}

	store := &FileLogStore{
		file:         file,
		syncStrategy: syncStrategy,
	}

	records, err := store.readAllLocked()
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			slog.Debug("wal file close error during init failure", "error", closeErr.Error())
		}
		return nil, err
	}

	if len(records) > 0 {
		store.lastLSN = records[len(records)-1].LSN
	}

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			slog.Debug("wal file close error during init failure", "error", closeErr.Error())
		}
		return nil, fmt.Errorf("seek wal end: %w", err)
	}

	return store, nil
}

// Close closes the underlying WAL file.
func (store *FileLogStore) Close() error {
	store.syncMu.Lock()
	defer store.syncMu.Unlock()

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.file == nil {
		return nil
	}

	err := store.file.Close()
	store.file = nil
	return err
}

// Append appends a record assigning the next deterministic LSN.
func (store *FileLogStore) Append(ctx context.Context, record ports.WALRecord) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.file == nil {
		return 0, errors.New("wal store is closed")
	}

	nextLSN := store.lastLSN + 1
	encoded := encodeDiskRecord(diskRecord{
		LSN:       nextLSN,
		Term:      record.Term,
		TxID:      record.TxID,
		Type:      record.Type,
		Timestamp: record.Timestamp,
		Payload:   record.Payload,
	})

	if _, err := store.file.Write(encoded); err != nil {
		return 0, fmt.Errorf("write wal record: %w", err)
	}

	store.appendCount++
	if store.syncStrategy.ShouldSync(store.appendCount) {
		if err := syncWithRetry(store.file); err != nil {
			return 0, fmt.Errorf("sync wal file: %w", err)
		}
	}

	store.lastLSN = nextLSN
	return nextLSN, nil
}

// AppendBatch appends multiple records in a single write+sync operation.
// Each record is assigned the next deterministic LSN sequentially.
// Returns the assigned LSNs (one per record) in order.
// Only a single fsync is issued at the end of the batch, which dramatically
// reduces I/O overhead for multi-record transactions.
func (store *FileLogStore) AppendBatch(ctx context.Context, records []ports.WALRecord) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.file == nil {
		return nil, errors.New("wal store is closed")
	}

	lsns := make([]uint64, len(records))
	// Pre-encode all records and gather into a single buffer.
	var totalSize int
	encodedRecords := make([][]byte, len(records))
	for i, record := range records {
		nextLSN := store.lastLSN + uint64(i) + 1
		lsns[i] = nextLSN
		encoded := encodeDiskRecord(diskRecord{
			LSN:       nextLSN,
			Term:      record.Term,
			TxID:      record.TxID,
			Type:      record.Type,
			Timestamp: record.Timestamp,
			Payload:   record.Payload,
		})
		encodedRecords[i] = encoded
		totalSize += len(encoded)
	}

	// Coalesce into a single buffer for one write syscall.
	buf := make([]byte, 0, totalSize)
	for _, encoded := range encodedRecords {
		buf = append(buf, encoded...)
	}

	if _, err := store.file.Write(buf); err != nil {
		return nil, fmt.Errorf("write wal batch: %w", err)
	}

	store.appendCount += uint64(len(records))
	if store.syncStrategy.ShouldSync(store.appendCount) {
		if err := syncWithRetry(store.file); err != nil {
			return nil, fmt.Errorf("sync wal file: %w", err)
		}
	}

	store.lastLSN += uint64(len(records))
	return lsns, nil
}

// AppendBatchNoSync appends multiple records in a single write WITHOUT calling
// fsync. Used by the group commit path: the caller writes under the engine's
// writeMu (fast), then releases the lock and delegates fsync to the group
// syncer goroutine which batches fsyncs across concurrent commits.
func (store *FileLogStore) AppendBatchNoSync(ctx context.Context, records []ports.WALRecord) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.file == nil {
		return nil, errors.New("wal store is closed")
	}

	lsns := make([]uint64, len(records))
	var totalSize int
	encodedRecords := make([][]byte, len(records))
	for i, record := range records {
		nextLSN := store.lastLSN + uint64(i) + 1
		lsns[i] = nextLSN
		encoded := encodeDiskRecord(diskRecord{
			LSN:       nextLSN,
			Term:      record.Term,
			TxID:      record.TxID,
			Type:      record.Type,
			Timestamp: record.Timestamp,
			Payload:   record.Payload,
		})
		encodedRecords[i] = encoded
		totalSize += len(encoded)
	}

	buf := make([]byte, 0, totalSize)
	for _, encoded := range encodedRecords {
		buf = append(buf, encoded...)
	}

	if _, err := store.file.Write(buf); err != nil {
		return nil, fmt.Errorf("write wal batch: %w", err)
	}

	store.appendCount += uint64(len(records))
	store.lastLSN += uint64(len(records))
	return lsns, nil
}

// Sync forces an fsync on the WAL file. Used by the group commit syncer
// goroutine to batch multiple commits into a single fsync.
//
// syncMu is held for the duration of the fsync to prevent TruncateBefore
// from closing the file mid-sync. mu is released before the fsync so that
// concurrent AppendBatchNoSync calls are not blocked by the slow syscall.
func (store *FileLogStore) Sync() error {
	store.syncMu.Lock()
	defer store.syncMu.Unlock()

	store.mu.Lock()
	if store.file == nil {
		store.mu.Unlock()
		return errors.New("wal store is closed")
	}
	f := store.file
	store.mu.Unlock()

	return syncWithRetry(f)
}

// AppendReplicated appends a record preserving incoming LSN for follower replication.
func (store *FileLogStore) AppendReplicated(ctx context.Context, record ports.WALRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.file == nil {
		return errors.New("wal store is closed")
	}

	if record.LSN == 0 || record.LSN != store.lastLSN+1 {
		return fmt.Errorf("%w: got=%d expected=%d", errOutOfOrderLSN, record.LSN, store.lastLSN+1)
	}

	encoded := encodeDiskRecord(diskRecord{
		LSN:       record.LSN,
		Term:      record.Term,
		TxID:      record.TxID,
		Type:      record.Type,
		Timestamp: record.Timestamp,
		Payload:   record.Payload,
	})

	if _, err := store.file.Write(encoded); err != nil {
		return fmt.Errorf("write replicated wal record: %w", err)
	}

	store.appendCount++
	if store.syncStrategy.ShouldSync(store.appendCount) {
		if err := syncWithRetry(store.file); err != nil {
			return fmt.Errorf("sync replicated wal file: %w", err)
		}
	}

	store.lastLSN = record.LSN
	return nil
}

// LastLSN returns the latest appended LSN.
func (store *FileLogStore) LastLSN() uint64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.lastLSN
}

// ReadFrom reads WAL records in ascending LSN order from fromLSN.
func (store *FileLogStore) ReadFrom(ctx context.Context, fromLSN uint64, limit int) ([]ports.WALRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	records, err := store.readAllLocked()
	if err != nil {
		return nil, err
	}

	result := make([]ports.WALRecord, 0, len(records))
	for _, record := range records {
		if record.LSN < fromLSN {
			continue
		}

		result = append(result, ports.WALRecord{
			LSN:       record.LSN,
			Term:      record.Term,
			TxID:      record.TxID,
			Type:      record.Type,
			Timestamp: record.Timestamp,
			Payload:   record.Payload,
		})

		if limit > 0 && len(result) >= limit {
			break
		}
	}

	return result, nil
}

// Recover returns all valid WAL records for startup state rebuild.
func (store *FileLogStore) Recover(ctx context.Context) ([]ports.WALRecord, error) {
	return store.ReadFrom(ctx, 1, 0)
}

func (store *FileLogStore) readAllLocked() ([]diskRecord, error) {
	if _, err := store.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek wal start: %w", err)
	}

	records := make([]diskRecord, 0, 256)
	lengthBytes := make([]byte, 4) // reuse across iterations
	truncateNeeded := false
	var validEnd int64
	for {
		// Track the file position before reading each record so we know
		// where valid data ends if a partial frame is detected.
		pos, posErr := store.file.Seek(0, io.SeekCurrent)
		if posErr != nil {
			return nil, fmt.Errorf("seek wal current pos: %w", posErr)
		}

		if _, err := io.ReadFull(store.file, lengthBytes); err != nil {
			if errors.Is(err, io.EOF) {
				validEnd = pos
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// Incomplete length prefix — partial write from a crash.
				slog.Warn("wal: truncated record at end of file (incomplete length prefix)", "valid_records", len(records))
				truncateNeeded = true
				validEnd = pos
				break
			}
			return nil, fmt.Errorf("read wal length prefix: %w", err)
		}

		length := binary.BigEndian.Uint32(lengthBytes)
		payload := make([]byte, length)
		if _, err := io.ReadFull(store.file, payload); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				// Incomplete record body — partial write from a crash.
				slog.Warn("wal: truncated record at end of file (incomplete payload)", "valid_records", len(records), "expected_bytes", length)
				truncateNeeded = true
				validEnd = pos
				break
			}
			return nil, fmt.Errorf("read wal record payload: %w", err)
		}

		record, err := decodeDiskRecord(payload)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	// If a partial frame was detected, truncate the file to discard garbage
	// so subsequent appends don't write past corrupt data.
	if truncateNeeded {
		slog.Warn("wal: truncating file to discard partial trailing frame", "valid_end", validEnd)
		if err := store.file.Truncate(validEnd); err != nil {
			return nil, fmt.Errorf("truncate wal after partial frame: %w", err)
		}
	}

	if _, err := store.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek wal end: %w", err)
	}

	return records, nil
}

// encodeDiskRecord encodes a WAL record using the compact binary format.
func encodeDiskRecord(record diskRecord) []byte {
	return encodeBinaryDiskRecord(record)
}

// decodeDiskRecord decodes a binary WAL record body.
func decodeDiskRecord(body []byte) (diskRecord, error) {
	return decodeBinaryDiskRecord(body)
}

// TruncateBefore removes all WAL records with LSN < beforeLSN by rewriting
// the WAL file atomically. The caller must ensure a snapshot covering those
// LSNs has been persisted before calling this method.
//
// syncMu is only acquired for the brief atomic file swap (close+rename+reopen),
// NOT during the slow read+write+fsync phases. This prevents TruncateBefore
// from blocking the group commit Sync() path for the entire operation.
func (store *FileLogStore) TruncateBefore(ctx context.Context, beforeLSN uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if beforeLSN <= 1 {
		return nil
	}

	// Phase 1: Read current records and file path under mu only.
	store.mu.Lock()
	if store.file == nil {
		store.mu.Unlock()
		return errors.New("wal store is closed")
	}
	records, err := store.readAllLocked()
	snapshotLastLSN := store.lastLSN
	walPath := store.file.Name()
	store.mu.Unlock()

	if err != nil {
		return fmt.Errorf("read wal for truncation: %w", err)
	}

	// Filter: keep records with LSN >= beforeLSN.
	var kept []diskRecord
	for _, r := range records {
		if r.LSN >= beforeLSN {
			kept = append(kept, r)
		}
	}

	// Nothing to remove.
	if len(kept) == len(records) {
		return nil
	}

	removed := len(records) - len(kept)
	slog.Info("wal: truncating records before snapshot",
		"before_lsn", beforeLSN,
		"removed", removed,
		"kept", len(kept))

	// Phase 2: Write temp file without holding any lock.
	// Sync() and AppendBatchNoSync can proceed concurrently during this phase.
	dir := filepath.Dir(walPath)
	tmp, err := os.CreateTemp(dir, ".wal-truncate-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp wal for truncation: %w", err)
	}
	tmpPath := tmp.Name()

	for _, r := range kept {
		frame := encodeDiskRecord(r)
		if _, err := tmp.Write(frame); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("write truncated wal record: %w", err)
		}
	}

	// Phase 3: Acquire both locks for the atomic file swap.
	// Under locks: append any records written between Phase 1 and now,
	// fsync, and do the atomic close+rename+reopen.
	store.syncMu.Lock()
	defer store.syncMu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.file == nil {
		tmp.Close()
		os.Remove(tmpPath)
		return errors.New("wal store closed during truncation")
	}

	// Append records written by concurrent commits since Phase 1.
	if store.lastLSN > snapshotLastLSN {
		allNow, readErr := store.readAllLocked()
		if readErr != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("re-read wal for truncation delta: %w", readErr)
		}
		for _, r := range allNow {
			if r.LSN > snapshotLastLSN {
				frame := encodeDiskRecord(r)
				if _, err := tmp.Write(frame); err != nil {
					tmp.Close()
					os.Remove(tmpPath)
					return fmt.Errorf("write truncation delta record: %w", err)
				}
			}
		}
	}

	if err := PlatformSync(tmp); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("sync truncated wal: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close truncated wal: %w", err)
	}

	// Close current WAL and atomically replace with truncated version.
	if err := store.file.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close wal for truncation: %w", err)
	}

	if err := os.Rename(tmpPath, walPath); err != nil {
		// Recovery: reopen the original file.
		store.file, _ = os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0o644)
		if store.file != nil {
			store.file.Seek(0, io.SeekEnd)
		}
		os.Remove(tmpPath)
		return fmt.Errorf("rename truncated wal: %w", err)
	}

	// Reopen the new WAL file for future appends.
	store.file, err = os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("reopen wal after truncation: %w", err)
	}
	if _, err := store.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek wal end after truncation: %w", err)
	}

	// lastLSN stays unchanged — LSN sequence is immutable.
	return nil
}
