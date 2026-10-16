package wal

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

// checkTruncatedStore asserts that the store holds exactly want, that the
// ledger head matches, that appends continue the chain, and that the store
// reopens and verifies cleanly.
func checkTruncatedStore(t *testing.T, dir string, store *SegmentedLogStore, want []ports.WALRecord, opts ...SegmentOption) {
	t.Helper()
	ctx := context.Background()
	if got := store.LastLSN(); got != uint64(len(want)) {
		t.Fatalf("LastLSN = %d, want %d", got, len(want))
	}
	got, err := store.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("ReadFrom returned %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].LSN != uint64(i+1) || got[i].TxID != want[i].TxID || got[i].Timestamp != want[i].Timestamp {
			t.Fatalf("record %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if h := store.LedgerHead(); !h.Valid || h.LSN != uint64(len(want)) || h.Chain != expectedChain(t, want) {
		t.Fatalf("ledger head after truncation %+v, want chain %s at LSN %d", h, expectedChain(t, want), len(want))
	}

	// A follower's next entries continue the log and the chain.
	next := ledgerTx(900, 64)
	for i := range next {
		next[i].LSN = uint64(len(want) + i + 1)
	}
	if err := store.AppendReplicatedBatch(ctx, next); err != nil {
		t.Fatalf("append after truncation: %v", err)
	}
	all := append(append([]ports.WALRecord(nil), want...), next...)
	if store.LedgerHead().Chain != expectedChain(t, all) {
		t.Fatal("chain diverged after appending past the truncation point")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewSegmentedLogStore(filepath.Join(dir, "asql.wal"), AlwaysSync{}, opts...)
	if err != nil {
		t.Fatalf("reopen after truncation: %v", err)
	}
	defer reopened.Close()
	if h := reopened.LedgerHead(); h.LSN != uint64(len(all)) || h.Chain != expectedChain(t, all) {
		t.Fatalf("ledger head after reopen %+v", h)
	}
	report, err := VerifyLedger(filepath.Join(dir, "asql.wal"), VerifyOptions{})
	if err != nil || !report.OK || report.Records != uint64(len(all)) {
		t.Fatalf("verify after truncation: err=%v failure=%v records=%d", err, report.Failure, report.Records)
	}
}

func TestSegmentedTruncateAfterInsideActiveSegment(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir)
	all := appendTxs(t, store, 0, 10, 40)
	if err := store.TruncateAfter(context.Background(), 20); err != nil {
		t.Fatal(err)
	}
	checkTruncatedStore(t, dir, store, all[:20])
}

// Raft can roll a follower back more than once in a process; each rewrite
// of the same segment must leave it under its own name.
func TestSegmentedTruncateAfterTwiceInSameSegment(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir)
	all := appendTxs(t, store, 0, 10, 40)
	for _, after := range []uint64{30, 20} {
		if err := store.TruncateAfter(context.Background(), after); err != nil {
			t.Fatal(err)
		}
	}
	checkTruncatedStore(t, dir, store, all[:20])
}

// Truncating everything after a rewrite removes the rewritten segment. Its
// file handle still carried the rewrite's temporary name, which was gone.
func TestSegmentedTruncateAfterRewriteThenEverything(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir)
	appendTxs(t, store, 0, 10, 40)
	for _, after := range []uint64{20, 0} {
		if err := store.TruncateAfter(context.Background(), after); err != nil {
			t.Fatal(err)
		}
	}
	checkTruncatedStore(t, dir, store, nil)
}

func TestSegmentedTruncateAfterAcrossSealedSegments(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	all := appendTxs(t, store, 0, 40, 200)
	before := store.SegmentCount()
	if before < 4 {
		t.Fatalf("expected several segments, got %d", before)
	}
	// Cut in the middle of the second segment: later segments are deleted.
	cut := uint64(len(all) / 4)
	if err := store.TruncateAfter(context.Background(), cut); err != nil {
		t.Fatal(err)
	}
	if store.SegmentCount() >= before {
		t.Fatalf("segments above the cut were not removed: %d -> %d", before, store.SegmentCount())
	}
	checkTruncatedStore(t, dir, store, all[:cut], WithSegmentSize(1024))
}

func TestSegmentedTruncateAfterInsideCompactedSegment(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	all := appendTxs(t, store, 0, 40, 200)
	if err := store.CompactSealedSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !store.segments[1].compressed {
		t.Fatal("expected the second segment to be compacted")
	}
	cut := store.segments[1].firstLSN + 2
	if err := store.TruncateAfter(context.Background(), cut); err != nil {
		t.Fatal(err)
	}
	if store.activeSegment().compressed {
		t.Fatal("the active segment after truncation must be raw")
	}
	checkTruncatedStore(t, dir, store, all[:cut], WithSegmentSize(1024))
}

func TestSegmentedTruncateAfterAtCompactedSegmentBoundary(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	all := appendTxs(t, store, 0, 40, 200)
	if err := store.CompactSealedSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	cut := store.segments[1].lastLSN
	if err := store.TruncateAfter(context.Background(), cut); err != nil {
		t.Fatal(err)
	}
	if store.SegmentCount() != 3 || store.activeSegment().compressed {
		t.Fatalf("expected two compacted segments plus a new raw active one, got %d", store.SegmentCount())
	}
	checkTruncatedStore(t, dir, store, all[:cut], WithSegmentSize(1024))
}

func TestSegmentedTruncateAfterLedgerOff(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithLedgerMode(LedgerOff), WithSegmentSize(1024))
	all := appendTxs(t, store, 0, 20, 100)
	if err := store.TruncateAfter(context.Background(), 31); err != nil {
		t.Fatal(err)
	}
	if store.LastLSN() != 31 || store.LedgerHead().Valid {
		t.Fatalf("LastLSN=%d head=%+v", store.LastLSN(), store.LedgerHead())
	}
	got, _ := store.ReadFrom(context.Background(), 1, 0)
	if len(got) != 31 || got[30].TxID != all[30].TxID {
		t.Fatalf("records after truncation: %d", len(got))
	}
	_ = store.Close()
	store = newLedgerStore(t, dir, WithLedgerMode(LedgerOff), WithSegmentSize(1024))
	defer store.Close()
	if store.LastLSN() != 31 {
		t.Fatalf("LastLSN after reopen = %d", store.LastLSN())
	}
}

func TestSegmentedTruncateAfterNoOpAndEmpty(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	appendTxs(t, store, 0, 10, 100)
	head := store.LedgerHead()
	if err := store.TruncateAfter(ctx, store.LastLSN()); err != nil {
		t.Fatal(err)
	}
	if store.LedgerHead() != head {
		t.Fatal("truncating at the last LSN must be a no-op")
	}
	if err := store.TruncateAfter(ctx, 0); err != nil {
		t.Fatal(err)
	}
	checkTruncatedStore(t, dir, store, nil, WithSegmentSize(1024))
}
