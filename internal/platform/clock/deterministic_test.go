package clock

import (
	"testing"
	"time"
)

func TestDeterministicNowSequence(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	step := 5 * time.Second

	clock, err := NewDeterministic(start, step)
	if err != nil {
		t.Fatalf("unexpected error creating deterministic clock: %v", err)
	}

	first := clock.Now()
	second := clock.Now()
	third := clock.Now()

	if !first.Equal(start) {
		t.Fatalf("first tick mismatch: got %v want %v", first, start)
	}

	if !second.Equal(start.Add(step)) {
		t.Fatalf("second tick mismatch: got %v want %v", second, start.Add(step))
	}

	if !third.Equal(start.Add(step * 2)) {
		t.Fatalf("third tick mismatch: got %v want %v", third, start.Add(step*2))
	}
}

func TestNewDeterministicRejectsNonPositiveStep(t *testing.T) {
	_, err := NewDeterministic(time.Now().UTC(), 0)
	if err == nil {
		t.Fatal("expected error when step is zero")
	}
}
