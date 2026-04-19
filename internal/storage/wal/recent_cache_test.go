package wal

import (
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

func TestRecentReadCacheSlidingWindowPreservesTail(t *testing.T) {
	store := &SegmentedLogStore{}
	batch1 := make([]ports.WALRecord, recentReadCacheSize)
	for i := range batch1 {
		batch1[i] = ports.WALRecord{LSN: uint64(i + 1)}
	}
	store.appendRecentLocked(batch1)

	batch2 := make([]ports.WALRecord, 1024)
	for i := range batch2 {
		batch2[i] = ports.WALRecord{LSN: uint64(recentReadCacheSize + i + 1)}
	}
	store.appendRecentLocked(batch2)

	records, ok := store.readRecentLocked(uint64(len(batch2)+1), 0)
	if !ok {
		t.Fatal("expected recent cache hit")
	}
	if len(records) != recentReadCacheSize {
		t.Fatalf("unexpected recent cache size: got %d want %d", len(records), recentReadCacheSize)
	}
	if records[0].LSN != uint64(len(batch2)+1) {
		t.Fatalf("unexpected first lsn: got %d want %d", records[0].LSN, len(batch2)+1)
	}
	if records[len(records)-1].LSN != uint64(recentReadCacheSize+len(batch2)) {
		t.Fatalf("unexpected last lsn: got %d want %d", records[len(records)-1].LSN, recentReadCacheSize+len(batch2))
	}

	if len(store.recentRecords) > recentReadCacheSize*2 {
		t.Fatalf("recent cache backing store grew too large: got %d", len(store.recentRecords))
	}
}

func TestRecentReadCacheCompactionResetsTrimmedPrefix(t *testing.T) {
	store := &SegmentedLogStore{
		recentRecords: make([]ports.WALRecord, recentReadCacheSize+16),
		recentStart:   recentReadCacheSize/2 + 8,
	}
	for i := range store.recentRecords {
		store.recentRecords[i] = ports.WALRecord{LSN: uint64(i + 1)}
	}

	store.compactRecentLocked()

	if store.recentStart != 0 {
		t.Fatalf("expected recentStart reset, got %d", store.recentStart)
	}
	wantLen := recentReadCacheSize + 16 - (recentReadCacheSize/2 + 8)
	if len(store.recentRecords) != wantLen {
		t.Fatalf("unexpected compacted len: got %d want %d", len(store.recentRecords), wantLen)
	}
	if store.recentRecords[0].LSN != uint64(recentReadCacheSize/2+9) {
		t.Fatalf("unexpected first compacted lsn: got %d", store.recentRecords[0].LSN)
	}
}
