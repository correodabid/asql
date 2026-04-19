package wal

import (
	"context"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
)

// TestSubscribeNotifiesOnAppend verifies that the notification channel returned
// by Subscribe() is closed (and therefore readable) within a very short window
// after an Append(), and that a fresh channel is issued for the next waiter.
func TestSubscribeNotifiesOnAppend(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewSegmentedLogStore(dir+"/wal", AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	// Subscribe BEFORE the append so we don't miss the notification.
	ch := store.Subscribe()

	// Append in a goroutine after a short delay so we're already waiting.
	go func() {
		time.Sleep(5 * time.Millisecond)
		if _, err := store.Append(ctx, ports.WALRecord{Type: "COMMIT", Payload: []byte(`{}`)}); err != nil {
			t.Errorf("Append: %v", err)
		}
	}()

	const maxWait = 500 * time.Millisecond
	select {
	case <-ch:
		// Good – notification arrived.
	case <-time.After(maxWait):
		t.Fatalf("Subscribe() channel was not closed within %s after Append()", maxWait)
	}

	// A new channel must be issued for subsequent waiters.
	ch2 := store.Subscribe()
	if ch2 == ch {
		t.Fatal("Subscribe() returned the same channel after a write; expected a fresh one")
	}
	select {
	case <-ch2:
		t.Fatal("fresh Subscribe() channel should not be closed yet")
	default:
		// Correct – no new write has happened.
	}
}

// TestSubscribeLatency measures the median wake-up latency for the notification
// path.  It is not a hard pass/fail benchmark – it just prints timing to give
// visibility into replication propagation delay.
func TestSubscribeLatency(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := NewSegmentedLogStore(dir+"/wal", AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	const rounds = 20
	latencies := make([]time.Duration, rounds)

	for i := range rounds {
		ch := store.Subscribe()
		start := time.Now()
		if _, err := store.Append(ctx, ports.WALRecord{Type: "COMMIT", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
		// The channel should already be closed because notifyWriters() runs
		// synchronously inside Append() before returning.
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatal("notification not received within 1s")
		}
		latencies[i] = time.Since(start)
	}

	var total time.Duration
	for _, l := range latencies {
		total += l
	}
	avg := total / rounds
	t.Logf("Subscribe() avg wake-up latency over %d rounds: %v", rounds, avg)
	if avg > 5*time.Millisecond {
		t.Errorf("avg notification latency %v exceeds 5ms threshold", avg)
	}
}
