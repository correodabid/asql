package wal

import (
	"context"
	"testing"
)

// Reads that miss the in-memory tail cache must see every record of a
// segment compacted to WALZ in this process. The segment used to keep its
// header size after compaction, so readers skipped into the compressed
// payload and silently returned a fraction of the records.
func TestSegmentedReadFromAfterInProcessCompaction(t *testing.T) {
	store := newLedgerStore(t, t.TempDir(), WithSegmentSize(1024))
	defer store.Close()
	appendTxs(t, store, 0, 40, 200)
	if err := store.CompactSealedSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.recentRecords, store.recentStart = nil, 0 // force reads from segment files
	store.mu.Unlock()
	records, err := store.ReadFrom(context.Background(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(records)) != store.LastLSN() {
		t.Fatalf("ReadFrom after compaction returned %d records, want %d", len(records), store.LastLSN())
	}
	for i, r := range records {
		if r.LSN != uint64(i+1) {
			t.Fatalf("record %d has LSN %d", i, r.LSN)
		}
	}
}
