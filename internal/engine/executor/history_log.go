package executor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
)

// Row history (FOR HISTORY) is an index of the WAL kept in the audit store:
// every row change of every committed transaction, in commit order and,
// within a commit, in statement and row order. It is derived the same way on
// every path that applies committed records (the commit queue, Raft
// followers, startup replay), so it is the same on every node and after any
// restart.
//
// The store records how far it covers the WAL. Writes run off the commit
// path, so a crash can leave the store behind the WAL; startup replay then
// regenerates the missing commits. That only works while the WAL still
// holds them after the snapshot replay starts from, so a disk checkpoint is
// written only once the history covers it, and replay ignores snapshots
// beyond the store's coverage.

// historyCollector receives the row changes that mutations apply while it is
// installed as engine.history. Callers hold writeMu.
type historyCollector struct {
	entries       []ports.AuditEntry
	domain, table string
}

// target names the table the next mutation changes.
func (h *historyCollector) target(domain, table string) {
	if h != nil {
		h.domain, h.table = domain, table
	}
}

// logChange records a row change in the table's in-memory change log and,
// while a collector is installed, in the row history.
func (engine *Engine) logChange(table *tableState, entry changeLogEntry) {
	table.changeLog = append(table.changeLog, entry)
	if h := engine.history; h != nil {
		h.entries = append(h.entries, ports.AuditEntry{
			CommitLSN: entry.commitLSN,
			Domain:    h.domain,
			Table:     h.table,
			Operation: entry.operation,
			OldRow:    entry.oldRow,
			NewRow:    entry.newRow,
		})
	}
}

// historyWriter appends row history to the audit store in commit order, on
// one goroutine at a time, off the commit path.
type historyWriter struct {
	store ports.AuditStore

	mu      sync.Mutex
	pending []ports.AuditEntry
	through uint64 // highest LSN queued
	durable uint64 // highest LSN the store covers
	running bool
	changed chan struct{} // closed when durable advances or a write fails
	// failures counts consecutive failed writes; after historyMaxFailures
	// the writer stops for good (stopped), leaving the store's coverage
	// where it was so that the next start regenerates the rest.
	failures int
	stopped  error
	// wake interrupts the idle wait before a deferred sync; unsynced is set
	// while written history awaits a sync.
	wake     chan struct{}
	lastSync time.Time
	unsynced bool
}

func newHistoryWriter(store ports.AuditStore) *historyWriter {
	covered, _ := store.Covered()
	return &historyWriter{store: store, through: covered, durable: covered, changed: make(chan struct{}), wake: make(chan struct{}, 1)}
}

// enqueue queues the row changes of the commits up to through.
func (w *historyWriter) enqueue(entries []ports.AuditEntry, through uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped != nil || (through <= w.through && len(entries) == 0) {
		return
	}
	w.pending = append(w.pending, entries...)
	w.through = max(w.through, through)
	if !w.running {
		w.running = true
		go w.run()
		return
	}
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *historyWriter) run() {
	for {
		w.mu.Lock()
		if w.durable >= w.through && len(w.pending) == 0 {
			if w.unsynced {
				// Idle with unsynced writes: sync once more work stops
				// arriving for a moment, or the sync interval is up.
				w.mu.Unlock()
				w.idleSync()
				continue
			}
			w.running = false
			w.mu.Unlock()
			return
		}
		entries, through := w.pending, w.through
		w.pending = nil
		w.mu.Unlock()

		err := w.store.AppendCommitted(context.Background(), entries, through)

		w.mu.Lock()
		if err != nil {
			w.failures++
			if w.failures >= historyMaxFailures {
				w.stopped = fmt.Errorf("row history writer stopped after %d failed writes (last: %w); restart to regenerate it from the WAL", w.failures, err)
				w.pending, w.running = nil, false
				close(w.changed)
				w.changed = make(chan struct{})
				w.mu.Unlock()
				slog.Error("history: audit append keeps failing; row history stops until restart", "error", err.Error(), "covered_lsn", w.durable)
				return
			}
			// Keep the entries queued, ahead of any newer ones, and retry.
			w.pending = append(entries, w.pending...)
		} else {
			w.failures = 0
			w.durable = through
			w.unsynced = true
		}
		close(w.changed)
		w.changed = make(chan struct{})
		syncDue := err == nil && time.Since(w.lastSync) >= historySyncInterval
		w.mu.Unlock()
		if syncDue {
			w.sync()
		}
		if err != nil {
			slog.Warn("history: audit append failed; retrying", "error", err.Error(), "entries", len(entries), "through_lsn", through)
			time.Sleep(historyRetryDelay)
		}
	}
}

const (
	historyRetryDelay  = 100 * time.Millisecond
	historyMaxFailures = 20
	// historySyncInterval bounds how often written history is synced, and
	// so how late readers in other processes (asqlctl audit-*) see it.
	historySyncInterval = 50 * time.Millisecond
)

// idleSync waits until no write arrives for the rest of the sync interval,
// then syncs.
func (w *historyWriter) idleSync() {
	w.mu.Lock()
	wait := max(0, historySyncInterval-time.Since(w.lastSync))
	w.mu.Unlock()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-w.wake:
		return // more work: the next write syncs when the interval is up
	case <-timer.C:
	}
	w.sync()
}

// flush syncs written history now. Shutdown calls it before the audit store
// is closed, which leaves an idle writer nothing to sync.
func (w *historyWriter) flush() { w.sync() }

// sync syncs written history, if any is unsynced, without holding mu: the
// commit path enqueues under it. A failed sync is only logged: the data is
// still written, and checkpoints sync again before relying on it.
func (w *historyWriter) sync() {
	w.mu.Lock()
	if !w.unsynced {
		w.mu.Unlock()
		return
	}
	w.lastSync, w.unsynced = time.Now(), false
	w.mu.Unlock()
	if err := w.store.Sync(); err != nil {
		slog.Warn("history: audit sync failed", "error", err.Error())
	}
}

// wait blocks until the store covers lsn, or ctx is done.
func (w *historyWriter) wait(ctx context.Context, lsn uint64) error {
	for {
		w.mu.Lock()
		durable, changed, stopped := w.durable, w.changed, w.stopped
		w.mu.Unlock()
		if durable >= lsn {
			return nil
		}
		if stopped != nil {
			return stopped
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("row history covers LSN %d, waiting for %d: %w", durable, lsn, ctx.Err())
		}
	}
}

// queued returns the highest LSN queued so far.
func (w *historyWriter) queued() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.through
}

// reset restarts the writer at the store's coverage once nothing is queued.
func (w *historyWriter) reset() {
	covered, _ := w.store.Covered()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending, w.through, w.durable = nil, covered, covered
	w.failures, w.stopped = 0, nil
}

// enqueueHistory queues the row changes of the commits up to through.
func (engine *Engine) enqueueHistory(entries []ports.AuditEntry, through uint64) {
	if engine.historyWriter != nil {
		engine.historyWriter.enqueue(entries, through)
	}
}

// historyCheckpointWait bounds how long a disk checkpoint waits for row
// history to cover it before it is skipped (and retried later).
const historyCheckpointWait = 10 * time.Second

// historyCovers reports whether row history durably covers lsn, waiting
// for queued writes and syncing them. A disk checkpoint must not get ahead of it: replay regenerates
// missing history only from the WAL after the checkpoint.
func (engine *Engine) historyCovers(lsn uint64) bool {
	if engine.historyWriter == nil {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), historyCheckpointWait)
	defer cancel()
	if err := engine.historyWriter.wait(ctx, lsn); err != nil {
		slog.Warn("snapshot: disk checkpoint deferred until row history catches up", "error", err.Error())
		return false
	}
	if err := engine.auditStore.Sync(); err != nil {
		slog.Warn("snapshot: disk checkpoint deferred; row history sync failed", "error", err.Error())
		return false
	}
	return true
}

// drainHistory waits until everything queued is written, or ctx is done.
func (engine *Engine) drainHistory(ctx context.Context) error {
	if engine.historyWriter == nil {
		return nil
	}
	return engine.historyWriter.wait(ctx, engine.historyWriter.queued())
}

// replayHistory returns the collector that a replay over the committed WAL
// installs before mutations whose commit the audit store does not cover yet.
// Bounded replays (time travel, ReplayToLSN) never record history.
func (engine *Engine) replayHistory(bounded bool) *historyCollector {
	if engine.historyWriter == nil || bounded {
		return nil
	}
	return &historyCollector{}
}

// historyFor installs collector for a mutation that commits at commitLSN when
// the audit store does not hold that commit yet.
func (engine *Engine) historyFor(collector *historyCollector, commitLSN uint64) {
	if collector != nil && commitLSN > engine.historyFrom {
		engine.history = collector
		return
	}
	engine.history = nil
}

// finishReplayHistory queues what a replay collected, up to its head.
func (engine *Engine) finishReplayHistory(collector *historyCollector) {
	engine.history = nil
	if collector != nil {
		engine.enqueueHistory(collector.entries, engine.headLSN)
	}
}

// prepareHistory aligns the audit store with the WAL before replay and
// returns the LSN through which it covers it. It drops history of commits
// the WAL no longer has (the WAL is synced after the audit store is
// written), and replaces history written before coverage was tracked.
func (engine *Engine) prepareHistory(ctx context.Context) (uint64, error) {
	store := engine.auditStore
	covered, tracked := store.Covered()
	walLast, walFirst, err := engine.walBounds(ctx)
	if err != nil {
		return 0, err
	}
	if !tracked {
		if walFirst <= 1 {
			// The WAL holds every commit: rebuild the history from it.
			slog.Info("history: rebuilding row history from the WAL (audit store predates coverage tracking)")
			if err := store.TruncateAfter(ctx, 0); err != nil {
				return 0, fmt.Errorf("history: reset legacy audit store: %w", err)
			}
		} else {
			// Earlier commits are gone from the WAL: keep what the store
			// has and continue from its newest commit.
			entries, err := store.ReadAll(ctx)
			if err != nil {
				return 0, fmt.Errorf("history: read legacy audit store: %w", err)
			}
			var newest uint64
			for _, e := range entries {
				newest = max(newest, e.CommitLSN)
			}
			slog.Warn("history: audit store predates coverage tracking and the WAL starts after LSN 1; row history before the WAL may be incomplete",
				"wal_first_lsn", walFirst, "kept_through_lsn", newest)
			if err := store.AppendCommitted(ctx, nil, newest); err != nil {
				return 0, fmt.Errorf("history: adopt legacy audit store: %w", err)
			}
		}
		covered, _ = store.Covered()
	}
	if covered > walLast {
		if err := store.TruncateAfter(ctx, walLast); err != nil {
			return 0, fmt.Errorf("history: drop history beyond the WAL: %w", err)
		}
		covered, _ = store.Covered()
	}
	engine.historyWriter.reset()
	return covered, nil
}

// walBounds returns the WAL's last LSN and its first retained LSN (0 when
// the WAL is empty).
func (engine *Engine) walBounds(ctx context.Context) (last, first uint64, err error) {
	first = 1
	head, err := engine.logStore.ReadFrom(ctx, 1, 1)
	if err != nil {
		return 0, 0, fmt.Errorf("history: read wal head: %w", err)
	}
	if len(head) > 0 {
		first = head[0].LSN
	}
	if lastLSNer, ok := engine.logStore.(interface{ LastLSN() uint64 }); ok {
		return lastLSNer.LastLSN(), first, nil
	}
	records, err := engine.logStore.ReadFrom(ctx, first, 0)
	if err != nil {
		return 0, 0, fmt.Errorf("history: read wal: %w", err)
	}
	if len(records) > 0 {
		last = records[len(records)-1].LSN
	}
	return last, first, nil
}
