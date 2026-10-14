package raft

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/platform/clock"
	"github.com/correodabid/asql/internal/storage/wal"
)

func newWatermarkLog(t *testing.T, committed, uncommitted int) (*wal.SegmentedLogStore, Log) {
	t.Helper()
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "raft.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	log, err := NewWALLog(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < committed; i++ {
		if _, err := log.AppendLeader(ctx, 1, "MUTATION", "tx", nil); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < uncommitted; i++ {
		if _, err := log.AppendLeader(ctx, 2, "MUTATION", "tx-tail", nil); err != nil {
			t.Fatal(err)
		}
	}
	return store, log
}

func TestNewRaftNodeStartsFromPersistedCommitIndex(t *testing.T) {
	ctx := context.Background()
	_, log := newWatermarkLog(t, 10, 3)
	for _, tc := range []struct {
		name  string
		saved *uint64
		want  uint64
	}{
		{name: "persisted", saved: ptr(uint64(10)), want: 10},
		{name: "persisted beyond log", saved: ptr(uint64(99)), want: 13},
		{name: "never persisted", saved: nil, want: 13},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := NewMemStorage()
			if tc.saved != nil {
				_ = storage.(CommitIndexStore).SaveCommitIndex(ctx, *tc.saved)
			}
			n, err := NewRaftNode(ctx, Config{NodeID: "n1", Storage: storage, Log: log, Transport: &captureTransport{}, Clock: clock.Realtime{},
				Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err != nil {
				t.Fatal(err)
			}
			if n.commitIndex != tc.want || n.lastApplied != tc.want {
				t.Fatalf("commitIndex=%d lastApplied=%d, want %d", n.commitIndex, n.lastApplied, tc.want)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

// A new leader commits a no-op in its term; that commits the uncommitted
// tail it inherited, and only then does the engine catch up.
func TestLeaderCommitsNoopBeforeCatchUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, log := newWatermarkLog(t, 10, 3)
	storage := NewMemStorage()
	_ = storage.(CommitIndexStore).SaveCommitIndex(ctx, 10)

	var caughtUpTo atomic.Uint64
	n, err := NewRaftNode(ctx, Config{
		NodeID:             "n1",
		Storage:            storage,
		Log:                log,
		Transport:          &captureTransport{},
		Clock:              clock.Realtime{},
		ElectionMinTimeout: 20 * time.Millisecond,
		ElectionMaxTimeout: 40 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		OnEntriesCommitted: func(_ context.Context, commitIndex uint64, _ []ports.WALRecord) {
			caughtUpTo.Store(commitIndex)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if n.commitIndex != 10 {
		t.Fatalf("start commitIndex=%d, want 10", n.commitIndex)
	}
	go func() { _ = n.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for caughtUpTo.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := caughtUpTo.Load(); got != 15 {
		t.Fatalf("engine caught up to %d, want 15 (13 entries + 2-record no-op)", got)
	}
	n.mu.Lock()
	ci, ready := n.commitIndex, n.leaderReadyCh
	n.mu.Unlock()
	if ci != 15 {
		t.Fatalf("commitIndex=%d, want 15", ci)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("leader never became ready for writes")
	}
	records, _ := store.ReadFrom(ctx, 14, 0)
	if len(records) != 2 || records[0].Type != "BEGIN" || records[1].Type != "COMMIT" || records[0].TxID != "raft-noop-"+itoa(records[0].Term) {
		t.Fatalf("expected a BEGIN/COMMIT no-op at 14-15, got %+v", records)
	}

	cancel() // Run flushes the commit index on shutdown
	time.Sleep(50 * time.Millisecond)
	if saved, ok, _ := storage.(CommitIndexStore).LoadCommitIndex(context.Background()); !ok || saved != 15 {
		t.Fatalf("persisted commit index=%d ok=%v, want 15", saved, ok)
	}
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }

func TestFileStorageCommitIndexRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewFileStorage(filepath.Join(t.TempDir(), "raft-state.json"))
	cs := s.(CommitIndexStore)
	if _, ok, err := cs.LoadCommitIndex(ctx); ok || err != nil {
		t.Fatalf("fresh storage: ok=%v err=%v", ok, err)
	}
	if err := cs.SaveCommitIndex(ctx, 42); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveState(ctx, 3, "n2"); err != nil {
		t.Fatal(err)
	}
	if idx, ok, err := cs.LoadCommitIndex(ctx); !ok || err != nil || idx != 42 {
		t.Fatalf("commit index=%d ok=%v err=%v", idx, ok, err)
	}
	if term, voted, err := s.LoadState(ctx); err != nil || term != 3 || voted != "n2" {
		t.Fatalf("state term=%d voted=%q err=%v", term, voted, err)
	}
}
