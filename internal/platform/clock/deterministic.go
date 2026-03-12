package clock

import (
	"errors"
	"sync"
	"time"
)

var errStepMustBePositive = errors.New("step must be positive")

// Deterministic provides a monotonic deterministic clock for tests/replay.
type Deterministic struct {
	mu      sync.Mutex
	current time.Time
	step    time.Duration
}

// NewDeterministic builds a deterministic clock starting at start with fixed step.
func NewDeterministic(start time.Time, step time.Duration) (*Deterministic, error) {
	if step <= 0 {
		return nil, errStepMustBePositive
	}

	return &Deterministic{
		current: start,
		step:    step,
	}, nil
}

// Now returns current time and advances the clock by one fixed step.
func (clock *Deterministic) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()

	now := clock.current
	clock.current = clock.current.Add(clock.step)
	return now
}
