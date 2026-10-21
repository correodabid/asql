package wal

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

func TestLedgerLeavesReadSealedCompactedAndCachedRecords(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "asql.wal")
	store, err := NewSegmentedLogStore(base, AlwaysSync{}, WithSegmentSize(4<<10))
	if err != nil {
		t.Fatal(err)
	}
	var want []ChainHash
	for i := 1; i <= 400; i++ {
		record := ports.WALRecord{Term: 1, TxID: fmt.Sprintf("tx-%d", i), Type: "MUTATION", Timestamp: uint64(i), Payload: []byte(fmt.Sprintf("payload %d", i))}
		if _, err := store.Append(ctx, record); err != nil {
			t.Fatal(err)
		}
		leaf, err := LedgerLeaf(record)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, leaf)
	}
	if err := store.CompactSealedSegments(ctx); err != nil {
		t.Fatal(err)
	}
	if store.SegmentCount() < 3 {
		t.Fatalf("want several segments, got %d", store.SegmentCount())
	}

	check := func(name string, from, to uint64, wantLen int) {
		t.Helper()
		got, err := store.LedgerLeaves(ctx, from, to)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != wantLen {
			t.Fatalf("%s: %d leaves, want %d", name, len(got), wantLen)
		}
		for i, leaf := range got {
			if leaf != want[from-1+uint64(i)] {
				t.Fatalf("%s: leaf at LSN %d differs", name, from+uint64(i))
			}
		}
	}
	check("from the recent-read cache", 1, 400, 400)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopened, the cache is empty: every read goes to the segment files,
	// compacted ones included.
	store, err = NewSegmentedLogStore(base, AlwaysSync{}, WithSegmentSize(4<<10))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	check("from the segments", 1, 400, 400)
	check("across a segment boundary", 37, 251, 215)
	check("past the end", 390, 500, 11)
	check("beyond the end", 401, 410, 0)

	if err := store.TruncateBefore(ctx, 200); err != nil {
		t.Fatal(err)
	}
	if got, err := store.LedgerLeaves(ctx, 1, 10); err != nil || len(got) != 0 {
		t.Fatalf("below the first retained LSN: %d leaves, %v; want none", len(got), err)
	}
}
