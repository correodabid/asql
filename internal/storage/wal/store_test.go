package wal

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"asql/internal/engine/ports"
)

func TestFileLogStoreAppendAndReadFrom(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "asql.wal")

	store, err := NewFileLogStore(path, EveryN{N: 2})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()

	lsn1, err := store.Append(ctx, ports.WALRecord{TxID: "tx1", Type: "BEGIN", Timestamp: 10, Payload: []byte("a")})
	if err != nil {
		t.Fatalf("append record 1: %v", err)
	}

	lsn2, err := store.Append(ctx, ports.WALRecord{TxID: "tx1", Type: "COMMIT", Timestamp: 11, Payload: []byte("b")})
	if err != nil {
		t.Fatalf("append record 2: %v", err)
	}

	if lsn1 != 1 || lsn2 != 2 {
		t.Fatalf("unexpected lsns: got (%d, %d), want (1, 2)", lsn1, lsn2)
	}

	records, err := store.ReadFrom(ctx, 2, 0)
	if err != nil {
		t.Fatalf("read from lsn 2: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("unexpected number of records: got %d want 1", len(records))
	}

	if records[0].LSN != 2 || records[0].Type != "COMMIT" {
		t.Fatalf("unexpected record: %+v", records[0])
	}
}

func TestFileLogStoreRecoverAfterReopen(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "asql.wal")
	ctx := context.Background()

	{
		store, err := NewFileLogStore(path, AlwaysSync{})
		if err != nil {
			t.Fatalf("new file log store: %v", err)
		}

		_, err = store.Append(ctx, ports.WALRecord{TxID: "tx2", Type: "BEGIN", Timestamp: 20, Payload: []byte("x")})
		if err != nil {
			t.Fatalf("append begin: %v", err)
		}

		_, err = store.Append(ctx, ports.WALRecord{TxID: "tx2", Type: "COMMIT", Timestamp: 21, Payload: []byte("y")})
		if err != nil {
			t.Fatalf("append commit: %v", err)
		}

		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	recovered, err := store.Recover(ctx)
	if err != nil {
		t.Fatalf("recover records: %v", err)
	}

	if len(recovered) != 2 {
		t.Fatalf("unexpected recovered size: got %d want 2", len(recovered))
	}

	if recovered[1].LSN != 2 || recovered[1].Type != "COMMIT" {
		t.Fatalf("unexpected recovered record: %+v", recovered[1])
	}
}

func TestFileLogStoreDetectsCorruption(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "asql.wal")
	ctx := context.Background()

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}

	_, err = store.Append(ctx, ports.WALRecord{TxID: "tx3", Type: "BEGIN", Timestamp: 30, Payload: []byte("p")})
	if err != nil {
		t.Fatalf("append record: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read wal file: %v", err)
	}

	if len(bytes) < 10 {
		t.Fatalf("wal file too short for corruption test")
	}

	bytes[len(bytes)-1] ^= 0xFF
	if err := os.WriteFile(path, bytes, 0o644); err != nil {
		t.Fatalf("write corrupted wal file: %v", err)
	}

	_, err = NewFileLogStore(path, AlwaysSync{})
	if err == nil {
		t.Fatal("expected corruption error when reopening WAL")
	}
}

func TestFileLogStoreRejectsUnsupportedFutureVersion(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "future-version.wal")

	// Write a binary v5 record but with a tampered version byte (6).
	// Build a valid-looking binary frame with an unsupported version.
	body := []byte{6}                        // version 6 — not supported
	body = append(body, make([]byte, 23)...) // padding to minimum size (binaryFixedOverheadV4=24, minus version byte already written)
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
	copy(frame[4:], body)
	if err := os.WriteFile(path, frame, 0o644); err != nil {
		t.Fatalf("write future version fixture: %v", err)
	}

	_, err := NewFileLogStore(path, AlwaysSync{})
	if err == nil {
		t.Fatal("expected unsupported version error when opening wal")
	}

	if !strings.Contains(err.Error(), "invalid wal record version") {
		t.Fatalf("expected invalid version error, got: %v", err)
	}
}

func TestFileLogStoreCrashRecoveryLoopWithInjectedPartialTail(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "crash-loop.wal")
	ctx := context.Background()

	expectedRecords := 0

	for round := 0; round < 5; round++ {
		store, err := NewFileLogStore(path, AlwaysSync{})
		if err != nil {
			t.Fatalf("round %d: open wal store: %v", round, err)
		}

		recovered, err := store.Recover(ctx)
		if err != nil {
			_ = store.Close()
			t.Fatalf("round %d: recover before append: %v", round, err)
		}

		if len(recovered) != expectedRecords {
			_ = store.Close()
			t.Fatalf("round %d: unexpected recovered size before append: got %d want %d", round, len(recovered), expectedRecords)
		}

		_, err = store.Append(ctx, ports.WALRecord{TxID: "loop-tx", Type: "BEGIN", Timestamp: uint64(round*2 + 1), Payload: []byte("begin")})
		if err != nil {
			_ = store.Close()
			t.Fatalf("round %d: append begin: %v", round, err)
		}

		_, err = store.Append(ctx, ports.WALRecord{TxID: "loop-tx", Type: "COMMIT", Timestamp: uint64(round*2 + 2), Payload: []byte("commit")})
		if err != nil {
			_ = store.Close()
			t.Fatalf("round %d: append commit: %v", round, err)
		}

		expectedRecords += 2

		if err := store.Close(); err != nil {
			t.Fatalf("round %d: close wal store: %v", round, err)
		}

		if err := appendInjectedPartialFrame(path, 128, []byte(`{"fault":"partial-tail"}`)); err != nil {
			t.Fatalf("round %d: inject partial frame: %v", round, err)
		}

		// The store should open successfully, auto-truncating the partial frame.
		store, err = NewFileLogStore(path, AlwaysSync{})
		if err != nil {
			t.Fatalf("round %d: expected successful open after partial frame (auto-truncate), got: %v", round, err)
		}

		recovered, err = store.Recover(ctx)
		if err != nil {
			_ = store.Close()
			t.Fatalf("round %d: recover after auto-truncate: %v", round, err)
		}

		if len(recovered) != expectedRecords {
			_ = store.Close()
			t.Fatalf("round %d: unexpected recovered size after auto-truncate: got %d want %d", round, len(recovered), expectedRecords)
		}

		if err := store.Close(); err != nil {
			t.Fatalf("round %d: close post-recover store: %v", round, err)
		}
	}
}

func TestSegmentedLogStoreRecoverAfterInjectedPartialTail(t *testing.T) {
	tempDir := t.TempDir()
	basePath := filepath.Join(tempDir, "segmented.wal")
	ctx := context.Background()

	store, err := NewSegmentedLogStore(basePath, AlwaysSync{}, WithSegmentSize(256))
	if err != nil {
		t.Fatalf("new segmented wal store: %v", err)
	}

	for i := 0; i < 20; i++ {
		if _, err := store.Append(ctx, ports.WALRecord{TxID: "seg", Type: "BEGIN", Timestamp: uint64(i + 1), Payload: []byte(strings.Repeat("x", 64))}); err != nil {
			_ = store.Close()
			t.Fatalf("append %d: %v", i, err)
		}
	}
	segmentPaths := store.SegmentPaths()
	if len(segmentPaths) == 0 {
		_ = store.Close()
		t.Fatal("expected at least one segment path")
	}
	activePath := segmentPaths[len(segmentPaths)-1]
	if err := store.Close(); err != nil {
		t.Fatalf("close segmented store: %v", err)
	}

	if err := appendInjectedPartialFrame(activePath, 128, []byte(`{"fault":"partial-tail"}`)); err != nil {
		t.Fatalf("inject partial frame: %v", err)
	}

	reopened, err := NewSegmentedLogStore(basePath, AlwaysSync{}, WithSegmentSize(256))
	if err != nil {
		t.Fatalf("reopen segmented store: %v", err)
	}
	defer reopened.Close()

	recovered, err := reopened.Recover(ctx)
	if err != nil {
		t.Fatalf("recover segmented store: %v", err)
	}
	if len(recovered) != 20 {
		t.Fatalf("unexpected recovered segmented records: got %d want 20", len(recovered))
	}
}

func appendInjectedPartialFrame(path string, declaredLength uint32, partialPayload []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	lengthPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthPrefix, declaredLength)

	if _, err := file.Write(lengthPrefix); err != nil {
		return err
	}

	if _, err := file.Write(partialPayload); err != nil {
		return err
	}

	return nil
}

func TestFileLogStoreAppendBatch(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "batch.wal")
	ctx := context.Background()

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	records := []ports.WALRecord{
		{TxID: "tx1", Type: "BEGIN", Timestamp: 1},
		{TxID: "tx1", Type: "MUTATION", Timestamp: 2, Payload: []byte(`{"sql":"INSERT"}`)},
		{TxID: "tx1", Type: "COMMIT", Timestamp: 3},
	}

	lsns, err := store.AppendBatch(ctx, records)
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}

	if len(lsns) != 3 {
		t.Fatalf("expected 3 LSNs, got %d", len(lsns))
	}
	if lsns[0] != 1 || lsns[1] != 2 || lsns[2] != 3 {
		t.Fatalf("unexpected LSNs: %v", lsns)
	}

	if store.LastLSN() != 3 {
		t.Fatalf("expected lastLSN=3, got %d", store.LastLSN())
	}

	// Verify records are readable.
	all, err := store.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatalf("read from: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 records, got %d", len(all))
	}
	if all[0].Type != "BEGIN" || all[1].Type != "MUTATION" || all[2].Type != "COMMIT" {
		t.Fatalf("unexpected record types: %s, %s, %s", all[0].Type, all[1].Type, all[2].Type)
	}

	// Verify batch survives reopen.
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	store2, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	all2, err := store2.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(all2) != 3 {
		t.Fatalf("expected 3 recovered records, got %d", len(all2))
	}
}

func TestFileLogStoreAppendBatchEmpty(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "batch-empty.wal")
	ctx := context.Background()

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	lsns, err := store.AppendBatch(ctx, nil)
	if err != nil {
		t.Fatalf("append empty batch: %v", err)
	}
	if lsns != nil {
		t.Fatalf("expected nil LSNs, got %v", lsns)
	}
}

func TestFileLogStoreAppendBatchThenIndividual(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "batch-then-single.wal")
	ctx := context.Background()

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// Batch of 2.
	lsns, err := store.AppendBatch(ctx, []ports.WALRecord{
		{TxID: "tx1", Type: "BEGIN", Timestamp: 1},
		{TxID: "tx1", Type: "COMMIT", Timestamp: 2},
	})
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if lsns[1] != 2 {
		t.Fatalf("batch last LSN: got %d want 2", lsns[1])
	}

	// Single append should continue sequence.
	lsn, err := store.Append(ctx, ports.WALRecord{TxID: "tx2", Type: "BEGIN", Timestamp: 3})
	if err != nil {
		t.Fatalf("append single: %v", err)
	}
	if lsn != 3 {
		t.Fatalf("single LSN: got %d want 3", lsn)
	}
}

func TestFileLogStoreAppendBatchNoSync(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "nosync.wal")
	ctx := context.Background()

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	records := []ports.WALRecord{
		{TxID: "tx1", Type: "BEGIN", Timestamp: 1},
		{TxID: "tx1", Type: "MUTATION", Timestamp: 2, Payload: []byte(`{"sql":"INSERT"}`)},
		{TxID: "tx1", Type: "COMMIT", Timestamp: 3},
	}

	lsns, err := store.AppendBatchNoSync(ctx, records)
	if err != nil {
		t.Fatalf("append batch no sync: %v", err)
	}

	if len(lsns) != 3 {
		t.Fatalf("expected 3 LSNs, got %d", len(lsns))
	}
	if lsns[0] != 1 || lsns[1] != 2 || lsns[2] != 3 {
		t.Fatalf("unexpected LSNs: %v", lsns)
	}

	if store.LastLSN() != 3 {
		t.Fatalf("expected lastLSN=3, got %d", store.LastLSN())
	}

	// Records are readable even before explicit sync.
	all, err := store.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatalf("read from: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 records, got %d", len(all))
	}

	// Explicit sync should succeed.
	if err := store.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Records survive reopen after sync.
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	store2, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	all2, err := store2.Recover(ctx)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(all2) != 3 {
		t.Fatalf("expected 3 recovered records, got %d", len(all2))
	}
}

func TestFileLogStoreSync(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "sync.wal")
	ctx := context.Background()

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// Sync on empty store should succeed.
	if err := store.Sync(); err != nil {
		t.Fatalf("sync empty: %v", err)
	}

	// Append without sync, then explicit sync.
	if _, err := store.AppendBatchNoSync(ctx, []ports.WALRecord{
		{TxID: "tx1", Type: "BEGIN", Timestamp: 1},
	}); err != nil {
		t.Fatalf("append: %v", err)
	}

	if err := store.Sync(); err != nil {
		t.Fatalf("sync after append: %v", err)
	}

	// Sync after close should fail.
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := store.Sync(); err == nil {
		t.Fatal("expected error syncing closed store")
	}
}

func TestFileLogStoreNoSyncBatchThenBatch(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "mixed.wal")
	ctx := context.Background()

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// NoSync batch of 2.
	lsns, err := store.AppendBatchNoSync(ctx, []ports.WALRecord{
		{TxID: "tx1", Type: "BEGIN", Timestamp: 1},
		{TxID: "tx1", Type: "COMMIT", Timestamp: 2},
	})
	if err != nil {
		t.Fatalf("nosync batch: %v", err)
	}
	if lsns[1] != 2 {
		t.Fatalf("nosync last LSN: got %d want 2", lsns[1])
	}

	// Regular batch should continue sequence.
	lsns2, err := store.AppendBatch(ctx, []ports.WALRecord{
		{TxID: "tx2", Type: "BEGIN", Timestamp: 3},
		{TxID: "tx2", Type: "COMMIT", Timestamp: 4},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if lsns2[0] != 3 || lsns2[1] != 4 {
		t.Fatalf("batch LSNs: got %v want [3,4]", lsns2)
	}
}

func TestBinaryEncodingRoundTrip(t *testing.T) {
	original := diskRecord{
		Version:   3,
		LSN:       42,
		TxID:      "tx-test-123",
		Type:      "MUTATION",
		Timestamp: 9999,
		Payload:   []byte(`{"domain":"billing","sql":"INSERT INTO invoices (id) VALUES (1)"}`),
	}

	frame := encodeBinaryDiskRecord(original)

	// Strip the 4-byte length prefix to get the body.
	bodyLen := binary.BigEndian.Uint32(frame[0:4])
	body := frame[4 : 4+bodyLen]

	decoded, err := decodeBinaryDiskRecord(body)
	if err != nil {
		t.Fatalf("decode binary record: %v", err)
	}

	if decoded.LSN != original.LSN {
		t.Fatalf("LSN mismatch: got %d want %d", decoded.LSN, original.LSN)
	}
	if decoded.TxID != original.TxID {
		t.Fatalf("TxID mismatch: got %q want %q", decoded.TxID, original.TxID)
	}
	if decoded.Type != original.Type {
		t.Fatalf("Type mismatch: got %q want %q", decoded.Type, original.Type)
	}
	if decoded.Timestamp != original.Timestamp {
		t.Fatalf("Timestamp mismatch: got %d want %d", decoded.Timestamp, original.Timestamp)
	}
	if string(decoded.Payload) != string(original.Payload) {
		t.Fatalf("Payload mismatch: got %q want %q", decoded.Payload, original.Payload)
	}
}

func appendN(t *testing.T, store *FileLogStore, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		if _, err := store.Append(ctx, ports.WALRecord{
			TxID: "tx", Type: "MUTATION", Timestamp: uint64(i), Payload: []byte("data"),
		}); err != nil {
			t.Fatalf("append record %d: %v", i, err)
		}
	}
}

func TestTruncateBefore(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "trunc.wal")

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	appendN(t, store, 10)

	// Get file size before truncation.
	infoBefore, _ := os.Stat(path)
	sizeBefore := infoBefore.Size()

	if err := store.TruncateBefore(ctx, 6); err != nil {
		t.Fatalf("truncate before 6: %v", err)
	}

	// Verify only records 6-10 remain.
	records, err := store.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatalf("read after truncation: %v", err)
	}
	if len(records) != 5 {
		t.Fatalf("expected 5 records, got %d", len(records))
	}
	if records[0].LSN != 6 {
		t.Fatalf("expected first record LSN=6, got %d", records[0].LSN)
	}
	if records[4].LSN != 10 {
		t.Fatalf("expected last record LSN=10, got %d", records[4].LSN)
	}

	// File should be smaller.
	infoAfter, _ := os.Stat(path)
	if infoAfter.Size() >= sizeBefore {
		t.Fatalf("file size did not shrink: before=%d after=%d", sizeBefore, infoAfter.Size())
	}
}

func TestTruncateBeforePreservesLSNSequence(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "trunc-lsn.wal")

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	appendN(t, store, 5)

	if err := store.TruncateBefore(ctx, 4); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// New appends should continue from LSN 6, not restart from 1.
	lsn, err := store.Append(ctx, ports.WALRecord{TxID: "tx", Type: "MUTATION", Timestamp: 100, Payload: []byte("new")})
	if err != nil {
		t.Fatalf("append after truncation: %v", err)
	}
	if lsn != 6 {
		t.Fatalf("expected LSN=6 after truncation, got %d", lsn)
	}

	// Verify full contents: records 4, 5, 6.
	records, err := store.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}
	if records[0].LSN != 4 || records[1].LSN != 5 || records[2].LSN != 6 {
		t.Fatalf("unexpected LSNs: %d, %d, %d", records[0].LSN, records[1].LSN, records[2].LSN)
	}
}

func TestTruncateBeforeSurvivesReopen(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "trunc-reopen.wal")
	ctx := context.Background()

	// Phase 1: create, append, truncate, close.
	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	appendN(t, store, 8)

	if err := store.TruncateBefore(ctx, 5); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Phase 2: reopen and verify.
	store2, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	records, err := store2.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if len(records) != 4 {
		t.Fatalf("expected 4 records (5-8), got %d", len(records))
	}
	if records[0].LSN != 5 {
		t.Fatalf("expected first LSN=5, got %d", records[0].LSN)
	}

	// LastLSN should be 8 (from the remaining records).
	if store2.LastLSN() != 8 {
		t.Fatalf("expected lastLSN=8, got %d", store2.LastLSN())
	}

	// New appends should continue from 9.
	lsn, err := store2.Append(ctx, ports.WALRecord{TxID: "tx", Type: "MUTATION", Timestamp: 99, Payload: []byte("x")})
	if err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if lsn != 9 {
		t.Fatalf("expected LSN=9, got %d", lsn)
	}
}

func TestTruncateBeforeNoOp(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "trunc-noop.wal")

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	appendN(t, store, 5)

	infoBefore, _ := os.Stat(path)

	// beforeLSN=1 should be a no-op.
	if err := store.TruncateBefore(ctx, 1); err != nil {
		t.Fatalf("truncate before 1: %v", err)
	}
	records, _ := store.ReadFrom(ctx, 1, 0)
	if len(records) != 5 {
		t.Fatalf("expected 5 records after no-op truncate, got %d", len(records))
	}

	// beforeLSN=0 should also be a no-op.
	if err := store.TruncateBefore(ctx, 0); err != nil {
		t.Fatalf("truncate before 0: %v", err)
	}

	infoAfter, _ := os.Stat(path)
	if infoBefore.Size() != infoAfter.Size() {
		t.Fatalf("file size changed on no-op truncate: %d -> %d", infoBefore.Size(), infoAfter.Size())
	}
}

func TestTruncateBeforeAllRecords(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "trunc-all.wal")

	store, err := NewFileLogStore(path, AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()

	appendN(t, store, 5)

	// Truncate ALL records (beforeLSN > lastLSN).
	if err := store.TruncateBefore(ctx, 100); err != nil {
		t.Fatalf("truncate all: %v", err)
	}

	// WAL should be empty.
	records, err := store.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatalf("read after truncate all: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}

	// File should still exist but be empty.
	info, _ := os.Stat(path)
	if info.Size() != 0 {
		t.Fatalf("expected empty file, got %d bytes", info.Size())
	}

	// lastLSN should be preserved — next append gets LSN 6.
	lsn, err := store.Append(ctx, ports.WALRecord{TxID: "tx", Type: "MUTATION", Timestamp: 1, Payload: []byte("x")})
	if err != nil {
		t.Fatalf("append after truncate all: %v", err)
	}
	if lsn != 6 {
		t.Fatalf("expected LSN=6, got %d", lsn)
	}
}
