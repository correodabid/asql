package pgwire

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"asql/internal/engine/ports"
)

const commitCatchUpDebounce = 2 * time.Millisecond
const commitCatchUpMaxRecordsPerApply = 1024

// commitCatchUpCoalescer coalesces follower commit notifications so only one
// catch-up worker replays WAL at a time while still converging to the latest
// committed LSN. Contexts with a non-nil Done channel are treated as
// synchronous callers and execute inline; this preserves the leader ready-path
// semantics during election.
type commitCatchUpCoalescer struct {
	mu      sync.Mutex
	running bool
	target  uint64
	pending []ports.WALRecord
	apply   func(context.Context, uint64, []ports.WALRecord) error
	logger  *slog.Logger
}

func newCommitCatchUpCoalescer(logger *slog.Logger, apply func(context.Context, uint64, []ports.WALRecord) error) *commitCatchUpCoalescer {
	return &commitCatchUpCoalescer{logger: logger, apply: apply}
}

func (c *commitCatchUpCoalescer) handle(ctx context.Context, commitIndex uint64, records []ports.WALRecord) {
	if c == nil || c.apply == nil {
		return
	}
	if ctx.Done() != nil {
		if err := c.apply(ctx, commitIndex, records); err != nil && c.logger != nil {
			c.logger.Warn("engine catchup after raft commit",
				"commit_index", commitIndex,
				"error", err)
		}
		return
	}

	c.mu.Lock()
	if commitIndex > c.target {
		c.target = commitIndex
	}
	if len(records) > 0 {
		c.pending = appendPendingCommittedRecords(c.pending, records)
	}
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.mu.Unlock()

	go c.run()
}

func (c *commitCatchUpCoalescer) run() {
	for {
		c.mu.Lock()
		target := c.target
		records := c.pending
		c.pending = nil
		c.mu.Unlock()

		if commitCatchUpDebounce > 0 {
			time.Sleep(commitCatchUpDebounce)
			c.mu.Lock()
			target = c.target
			if len(c.pending) > 0 {
				records = appendPendingCommittedRecords(records, c.pending)
				c.pending = nil
			}
			c.mu.Unlock()
		}

		applyTarget := target
		applyRecords := records
		if len(records) > commitCatchUpMaxRecordsPerApply {
			applyRecords = append([]ports.WALRecord(nil), records[:commitCatchUpMaxRecordsPerApply]...)
			applyTarget = applyRecords[len(applyRecords)-1].LSN
			remaining := append([]ports.WALRecord(nil), records[commitCatchUpMaxRecordsPerApply:]...)
			c.mu.Lock()
			c.pending = appendPendingCommittedRecords(remaining, c.pending)
			c.mu.Unlock()
		}

		err := c.apply(context.Background(), applyTarget, applyRecords)
		if err != nil && c.logger != nil {
			c.logger.Warn("engine catchup after raft commit",
				"commit_index", applyTarget,
				"error", err)
		}

		c.mu.Lock()
		if err != nil || (c.target == applyTarget && len(c.pending) == 0) {
			c.running = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}

func appendPendingCommittedRecords(base []ports.WALRecord, extra []ports.WALRecord) []ports.WALRecord {
	if len(extra) == 0 {
		return base
	}
	if len(base) == 0 {
		out := make([]ports.WALRecord, len(extra))
		copy(out, extra)
		return out
	}
	lastLSN := base[len(base)-1].LSN
	start := 0
	for start < len(extra) && extra[start].LSN <= lastLSN {
		start++
	}
	if start >= len(extra) {
		return base
	}
	return append(base, extra[start:]...)
}
