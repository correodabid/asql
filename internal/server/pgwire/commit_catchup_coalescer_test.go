package pgwire

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
)

func TestCommitCatchUpCoalescerCoalescesBackgroundRequests(t *testing.T) {
	var mu sync.Mutex
	calls := make([]uint64, 0)
	started := make(chan struct{}, 1)
	unblock := make(chan struct{})

	c := newCommitCatchUpCoalescer(slog.Default(), func(_ context.Context, lsn uint64, _ []ports.WALRecord) error {
		mu.Lock()
		calls = append(calls, lsn)
		mu.Unlock()
		started <- struct{}{}
		<-unblock
		return nil
	})

	c.handle(context.Background(), 10, nil)
	<-started
	c.handle(context.Background(), 12, nil)
	c.handle(context.Background(), 15, nil)
	close(unblock)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		running := c.running
		c.mu.Unlock()
		if !running {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("calls=%d want 2 (%v)", len(calls), calls)
	}
	if calls[0] != 10 {
		t.Fatalf("first call=%d want 10", calls[0])
	}
	if calls[1] != 15 {
		t.Fatalf("second call=%d want 15", calls[1])
	}
}

func TestCommitCatchUpCoalescerRunsSynchronousContextInline(t *testing.T) {
	var calls int
	c := newCommitCatchUpCoalescer(slog.Default(), func(ctx context.Context, lsn uint64, _ []ports.WALRecord) error {
		calls++
		if ctx.Done() == nil {
			t.Fatal("expected synchronous context with non-nil Done channel")
		}
		if lsn != 7 {
			t.Fatalf("lsn=%d want 7", lsn)
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.handle(ctx, 7, nil)
	if calls != 1 {
		t.Fatalf("calls=%d want 1", calls)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		t.Fatal("coalescer should not leave background worker running for synchronous path")
	}
}

func TestCommitCatchUpCoalescerSplitsLargeCommittedBatch(t *testing.T) {
	var mu sync.Mutex
	var calls []struct {
		lsn   uint64
		count int
	}

	coalescer := newCommitCatchUpCoalescer(slog.Default(), func(_ context.Context, lsn uint64, records []ports.WALRecord) error {
		mu.Lock()
		calls = append(calls, struct {
			lsn   uint64
			count int
		}{lsn: lsn, count: len(records)})
		mu.Unlock()
		return nil
	})

	records := make([]ports.WALRecord, commitCatchUpMaxRecordsPerApply+5)
	for i := range records {
		records[i] = ports.WALRecord{LSN: uint64(i + 1)}
	}
	coalescer.handle(context.Background(), records[len(records)-1].LSN, records)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		coalescer.mu.Lock()
		running := coalescer.running
		pending := len(coalescer.pending)
		coalescer.mu.Unlock()
		if !running && pending == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("calls=%d want 2", len(calls))
	}
	if calls[0].count != commitCatchUpMaxRecordsPerApply {
		t.Fatalf("first call count=%d want %d", calls[0].count, commitCatchUpMaxRecordsPerApply)
	}
	if calls[0].lsn != records[commitCatchUpMaxRecordsPerApply-1].LSN {
		t.Fatalf("first call lsn=%d want %d", calls[0].lsn, records[commitCatchUpMaxRecordsPerApply-1].LSN)
	}
	if calls[1].count != 5 {
		t.Fatalf("second call count=%d want 5", calls[1].count)
	}
	if calls[1].lsn != records[len(records)-1].LSN {
		t.Fatalf("second call lsn=%d want %d", calls[1].lsn, records[len(records)-1].LSN)
	}
}
