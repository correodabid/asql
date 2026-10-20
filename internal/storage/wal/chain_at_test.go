package wal

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

// prefixChains returns chain_n for every n over records starting at LSN 1.
func prefixChains(t *testing.T, records []ports.WALRecord) []ChainHash {
	t.Helper()
	chains := []ChainHash{GenesisChain}
	for i, r := range records {
		leaf, err := LedgerLeaf(r)
		if err != nil {
			t.Fatal(err)
		}
		chains = append(chains, ChainStep(chains[i], uint64(i+1), leaf))
	}
	return chains
}

func requireChainsAt(t *testing.T, label string, store *SegmentedLogStore, want []ChainHash, from uint64) {
	t.Helper()
	for lsn := from; lsn < uint64(len(want)); lsn++ {
		got, err := store.ChainAt(lsn)
		if err != nil {
			t.Fatalf("%s: ChainAt(%d): %v", label, lsn, err)
		}
		if got != want[lsn] {
			t.Fatalf("%s: ChainAt(%d) = %s, want %s", label, lsn, got, want[lsn])
		}
	}
}

// TestChainAtMatchesTheRecomputedChain: ChainAt gives the chain value at
// every LSN, from memory or by rehashing a segment from its anchor, before
// and after a restart, which leaves only the active segment in memory, and
// after compaction.
func TestChainAtMatchesTheRecomputedChain(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	all := appendTxs(t, store, 0, 40, 200)
	if store.SegmentCount() < 3 {
		t.Fatalf("expected rotation, got %d segments", store.SegmentCount())
	}
	want := prefixChains(t, all)
	requireChainsAt(t, "in memory", store, want, 0)
	_ = store.Close()

	store = newLedgerStore(t, dir, WithSegmentSize(1024))
	requireChainsAt(t, "after restart", store, want, 0)
	if _, ok := store.LedgerChainAt(1); ok {
		t.Fatal("LSN 1 still in memory after restart: the test does not exercise rehashing")
	}
	if err := store.CompactSealedSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireChainsAt(t, "after compaction", store, want, 0)
	_ = store.Close()

	report, err := VerifyLedger(filepath.Join(dir, "asql.wal"), VerifyOptions{})
	if err != nil || !report.OK || report.Head != want[len(want)-1] {
		t.Fatalf("verify: err=%v report=%+v", err, report)
	}
}

func TestChainAtRefusesWhatItCannotAnchor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	all := appendTxs(t, store, 0, 30, 200)
	want := prefixChains(t, all)
	head := store.LastLSN()
	if _, err := store.ChainAt(head + 1); !errors.Is(err, ErrChainUnavailable) {
		t.Fatalf("beyond the head: %v", err)
	}

	cut := head - 10
	if err := store.TruncateBefore(ctx, cut); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	store = newLedgerStore(t, dir, WithSegmentSize(1024))
	defer store.Close()
	first := store.segments[0].firstLSN
	if first <= 1 {
		t.Fatalf("GC kept the first segment (first LSN %d)", first)
	}
	if _, err := store.ChainAt(first - 2); !errors.Is(err, ErrChainUnavailable) {
		t.Fatalf("before the retained WAL: %v", err)
	}
	requireChainsAt(t, "after GC", store, want, first-1)

	off := newLedgerStore(t, t.TempDir(), WithLedgerMode(LedgerOff))
	defer off.Close()
	appendTxs(t, off, 0, 2, 10)
	if _, err := off.ChainAt(1); !errors.Is(err, ErrChainUnavailable) {
		t.Fatalf("ledger off: %v", err)
	}
}

// TestChainAtWhileAppending: rehashing an old segment does not hold the
// store lock, and appends running meanwhile do not change its answer.
func TestChainAtWhileAppending(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	all := appendTxs(t, store, 0, 30, 200)
	_ = store.Close()
	store = newLedgerStore(t, dir, WithSegmentSize(1024))
	defer store.Close()
	want := prefixChains(t, all)

	done := make(chan error, 1)
	go func() {
		for i := 30; i < 60; i++ {
			if _, err := store.AppendBatch(context.Background(), ledgerTx(i, 200)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for round := 0; round < 5; round++ {
		requireChainsAt(t, "while appending", store, want, 0)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
