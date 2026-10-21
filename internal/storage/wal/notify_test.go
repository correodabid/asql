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

// TestSubscribeLatency: Append wakes subscribers before it returns, so a
// replication follower waiting on Subscribe never waits on its own for a
// record that is already written. It logs how long Append and the wake-up
// took, which on CI is dominated by fsync and is not asserted.
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
	var total time.Duration
	for range rounds {
		ch := store.Subscribe()
		start := time.Now()
		if _, err := store.Append(ctx, ports.WALRecord{Type: "COMMIT", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
		// notifyWriters() runs synchronously inside Append(), so the channel
		// is closed by the time it returns.
		select {
		case <-ch:
		default:
			t.Fatal("Append returned before waking subscribers")
		}
		total += time.Since(start)
	}
	t.Logf("Append and wake-up: %v on average over %d rounds", total/rounds, rounds)
}
