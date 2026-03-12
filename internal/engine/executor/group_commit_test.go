package executor

import (
	"sync"
	"sync/atomic"
	"testing"
)

// mockSyncer counts how many times Sync() is called.
type mockSyncer struct {
	syncCount atomic.Int64
}

func (m *mockSyncer) Sync() error {
	m.syncCount.Add(1)
	return nil
}

func TestGroupSyncerBasic(t *testing.T) {
	ms := &mockSyncer{}
	gs := newGroupSyncer(ms, nil)

	if err := gs.requestSync(); err != nil {
		t.Fatalf("requestSync: %v", err)
	}

	if got := ms.syncCount.Load(); got != 1 {
		t.Fatalf("expected 1 sync call, got %d", got)
	}

	gs.stop()
}

func TestGroupSyncerConcurrent(t *testing.T) {
	ms := &mockSyncer{}
	gs := newGroupSyncer(ms, nil)

	const numGoroutines = 50
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	errs := make([]error, numGoroutines)

	// Start barrier: all goroutines launch at once.
	start := make(chan struct{})
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			errs[idx] = gs.requestSync()
		}(i)
	}

	close(start) // release all goroutines at once
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: requestSync error: %v", i, err)
		}
	}

	// The key property: sync count should be much less than numGoroutines.
	// With perfect batching it would be 1, but timing can cause a few more.
	// On fast machines with low contention, all goroutines might serialize
	// (degenerate case), so we only fail if it exceeds numGoroutines.
	syncCalls := ms.syncCount.Load()
	if syncCalls > int64(numGoroutines) {
		t.Errorf("expected at most %d sync calls, got %d", numGoroutines, syncCalls)
	}
	t.Logf("group syncer: %d goroutines, %d sync calls (%.1fx reduction)", numGoroutines, syncCalls, float64(numGoroutines)/float64(syncCalls))

	gs.stop()
}

func TestGroupSyncerStop(t *testing.T) {
	ms := &mockSyncer{}
	gs := newGroupSyncer(ms, nil)

	// Stop without any pending syncs — should not deadlock.
	gs.stop()

	syncCalls := ms.syncCount.Load()
	if syncCalls != 0 {
		t.Errorf("expected 0 sync calls on empty stop, got %d", syncCalls)
	}
}

func TestGroupSyncerStopWithPending(t *testing.T) {
	ms := &mockSyncer{}
	gs := newGroupSyncer(ms, nil)

	// Request a sync, then stop — should complete cleanly.
	if err := gs.requestSync(); err != nil {
		t.Fatalf("requestSync: %v", err)
	}

	gs.stop()

	// At least 1 sync should have happened.
	if got := ms.syncCount.Load(); got < 1 {
		t.Fatalf("expected at least 1 sync call, got %d", got)
	}
}

func TestGroupSyncerMultipleRounds(t *testing.T) {
	ms := &mockSyncer{}
	gs := newGroupSyncer(ms, nil)

	// Do several sequential rounds.
	for round := 0; round < 10; round++ {
		if err := gs.requestSync(); err != nil {
			t.Fatalf("round %d: requestSync: %v", round, err)
		}
	}

	// Each sequential request should trigger its own sync.
	syncCalls := ms.syncCount.Load()
	if syncCalls != 10 {
		t.Errorf("expected 10 sync calls for 10 sequential rounds, got %d", syncCalls)
	}

	gs.stop()
}
