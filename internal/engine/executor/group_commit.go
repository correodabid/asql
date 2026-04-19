package executor

import (
	"sync"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
)

// groupSyncer batches WAL fsync calls across concurrent commits.
//
// Instead of each commit calling fsync individually while holding writeMu,
// commits write to the WAL without fsync (fast, ~5us) and then call
// requestSync(). A dedicated background goroutine performs a single fsync
// that covers all pending commits, then wakes them all up.
//
// This is the same "group commit" pattern used by PostgreSQL and other
// databases: N concurrent commits → 1 fsync instead of N fsyncs.
type groupSyncer struct {
	mu         sync.Mutex
	cond       *sync.Cond
	pending    int    // number of commits waiting for fsync
	generation uint64 // increments after each fsync
	syncer     ports.Syncer
	perf       *perfStats
	syncErr    error // last sync error, delivered to all waiters
	stopped    bool
	wg         sync.WaitGroup
}

// newGroupSyncer starts the background sync loop.
func newGroupSyncer(syncer ports.Syncer, perf *perfStats) *groupSyncer {
	gs := &groupSyncer{
		syncer: syncer,
		perf:   perf,
	}
	gs.cond = sync.NewCond(&gs.mu)
	gs.wg.Add(1)
	go gs.loop()
	return gs
}

// requestSync registers that a commit needs fsync and waits for the next
// sync cycle to complete. Returns the error from fsync (if any).
func (gs *groupSyncer) requestSync() error {
	gs.mu.Lock()
	myGen := gs.generation
	gs.pending++
	gs.cond.Signal() // wake the sync loop
	for gs.generation == myGen && !gs.stopped {
		gs.cond.Wait()
	}
	err := gs.syncErr
	gs.mu.Unlock()
	return err
}

// loop is the background goroutine that batches fsyncs.
func (gs *groupSyncer) loop() {
	defer gs.wg.Done()
	gs.mu.Lock()
	defer gs.mu.Unlock()

	for {
		// Wait until there are pending commits or we're stopped.
		for gs.pending == 0 && !gs.stopped {
			gs.cond.Wait()
		}
		if gs.stopped {
			// Final sync for any remaining pending commits.
			if gs.pending > 0 {
				gs.mu.Unlock()
				started := time.Now()
				err := gs.syncer.Sync()
				gs.mu.Lock()
				if gs.perf != nil {
					gs.perf.recordFsync(time.Since(started), err)
				}
				gs.syncErr = err
				gs.pending = 0
				gs.generation++
				gs.cond.Broadcast()
			}
			return
		}

		// Reset pending count — new arrivals during our sync will
		// increment pending again and be covered by the next cycle.
		gs.pending = 0

		// Release lock during fsync (the slow part).
		gs.mu.Unlock()
		started := time.Now()
		err := gs.syncer.Sync()
		gs.mu.Lock()
		if gs.perf != nil {
			gs.perf.recordFsync(time.Since(started), err)
		}

		gs.syncErr = err
		gs.generation++
		gs.cond.Broadcast() // wake all waiters whose generation is done
	}
}

// stop shuts down the sync loop, performing a final sync for any pending
// commits. Blocks until the background goroutine exits.
func (gs *groupSyncer) stop() {
	gs.mu.Lock()
	gs.stopped = true
	gs.cond.Signal() // wake the loop so it can exit
	gs.mu.Unlock()
	gs.wg.Wait()
}
