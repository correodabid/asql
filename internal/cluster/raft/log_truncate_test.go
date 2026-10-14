package raft

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/platform/clock"
	"github.com/correodabid/asql/internal/storage/wal"
)

// A follower that holds uncommitted entries from an old term must drop that
// conflicting suffix and accept the new leader's entries, keeping its
// committed prefix intact.
func TestWALLogTruncateAfterDropsConflictingSuffix(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "raft.wal"), wal.AlwaysSync{}, wal.WithSegmentSize(512))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	log, err := NewWALLog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	// Committed history in term 1, then uncommitted entries from term 2.
	for i := 0; i < 30; i++ {
		if _, err := log.AppendLeader(ctx, 1, "MUTATION", "tx-old", []byte("committed-payload")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		if _, err := log.AppendLeader(ctx, 2, "MUTATION", "tx-stale", []byte("uncommitted")); err != nil {
			t.Fatal(err)
		}
	}
	if store.SegmentCount() < 3 {
		t.Fatalf("expected several segments, got %d", store.SegmentCount())
	}

	// New leader (term 3) disagrees from index 31 on.
	if err := log.TruncateAfter(ctx, 30); err != nil {
		t.Fatalf("TruncateAfter: %v", err)
	}
	if got := log.LastIndex(); got != 30 {
		t.Fatalf("LastIndex after truncate = %d, want 30", got)
	}
	if got, _ := log.Term(30); got != 1 {
		t.Fatalf("Term(30) = %d, want 1", got)
	}
	newEntries := []Entry{
		{Index: 31, Term: 3, Type: "MUTATION", TxID: "tx-new", Payload: []byte("a")},
		{Index: 32, Term: 3, Type: "MUTATION", TxID: "tx-new", Payload: []byte("b")},
	}
	if err := log.AppendFollower(ctx, newEntries); err != nil {
		t.Fatalf("AppendFollower after truncate: %v", err)
	}

	all, err := log.Entries(ctx, 1, 33)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 32 {
		t.Fatalf("log holds %d entries, want 32 (committed prefix must survive)", len(all))
	}
	for i, e := range all {
		if e.Index != uint64(i+1) {
			t.Fatalf("entry %d has index %d", i, e.Index)
		}
		wantTerm := uint64(1)
		if e.Index > 30 {
			wantTerm = 3
		}
		if e.Term != wantTerm || (e.Index > 30 && e.TxID != "tx-new") {
			t.Fatalf("entry %d: term=%d txid=%q", e.Index, e.Term, e.TxID)
		}
	}
}

// End to end on a follower node: uncommitted entries from an old term
// conflict with the new leader's, and AppendEntries must replace them.
func TestFollowerReplacesConflictingSuffixOnAppendEntries(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "raft.wal"), wal.AlwaysSync{}, wal.WithSegmentSize(512))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log, err := NewWALLog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := log.AppendLeader(ctx, 1, "MUTATION", "tx-committed", []byte("committed-payload")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ { // written by a leader of term 2 that lost leadership before replicating
		if _, err := log.AppendLeader(ctx, 2, "MUTATION", "tx-stale", nil); err != nil {
			t.Fatal(err)
		}
	}

	n, err := NewRaftNode(ctx, Config{
		NodeID:             "n1",
		Peers:              []Peer{{NodeID: "n2", RaftAddr: "n2"}},
		Storage:            NewMemStorage(),
		Log:                log,
		Transport:          &captureTransport{appendCh: make(chan AppendEntriesRequest, 8)},
		Clock:              clock.Realtime{},
		ElectionMinTimeout: 10 * time.Second,
		ElectionMaxTimeout: 20 * time.Second,
		HeartbeatInterval:  10 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(testWriter{t}, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	req := AppendEntriesRequest{
		Term:         3,
		LeaderID:     "n2",
		PrevLogIndex: 20,
		PrevLogTerm:  1,
		Entries: []RaftEntry{
			{Index: 21, Term: 3, Type: "MUTATION", TxID: "tx-new"},
			{Index: 22, Term: 3, Type: "MUTATION", TxID: "tx-new"},
		},
		LeaderCommit: 22,
	}
	resp, err := n.HandleAppendEntries(ctx, req)
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !resp.Success {
		t.Fatalf("AppendEntries rejected: %+v", resp)
	}
	entries, err := log.Entries(ctx, 1, 23)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 22 {
		t.Fatalf("follower log has %d entries, want 22", len(entries))
	}
	for _, e := range entries[20:] {
		if e.Term != 3 || e.TxID != "tx-new" {
			t.Fatalf("entry %d still from the stale suffix: term=%d txid=%q", e.Index, e.Term, e.TxID)
		}
	}
	if h := store.LedgerHead(); !h.Valid || h.LSN != 22 {
		t.Fatalf("ledger head after rollback: %+v", h)
	}
}
