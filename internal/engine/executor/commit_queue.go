package executor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"asql/internal/engine/parser/ast"
	"asql/internal/engine/planner"
	"asql/internal/engine/ports"
)

// commitJob represents a single commit request submitted to the commit queue.
// The caller goroutine populates these fields before submission and waits on resultCh.
type commitJob struct {
	ctx         context.Context
	session     *Session
	tx          *transaction
	ordered     []preparedMutation
	preEncoded  [][]byte
	affected    map[string]struct{}
	allDML      bool
	commitStart time.Time
	queuedAt    time.Time
	resultCh    chan commitJobResult
}

// commitJobResult is sent back to the caller goroutine via resultCh.
type commitJobResult struct {
	commitLSN uint64
	err       error
}

// commitQueue batches concurrent commits into fewer writeMu critical sections.
// Instead of each goroutine competing for writeMu, commits are submitted to
// a queue and processed by a single writer goroutine. Under high concurrency,
// multiple commits are coalesced into a single lock acquisition, WAL write,
// state swap, and fsync — reducing per-commit overhead from O(N) locks+writes
// to O(1).
//
// Under low concurrency, the queue drains a single job and processes it
// with the same overhead as the original direct-locking path.
type commitQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	jobs    []*commitJob
	stopped bool
	wg      sync.WaitGroup
	engine  *Engine
}

// newCommitQueue creates and starts the commit queue writer loop.
func newCommitQueue(engine *Engine) *commitQueue {
	cq := &commitQueue{
		engine: engine,
	}
	cq.cond = sync.NewCond(&cq.mu)
	cq.wg.Add(1)
	go cq.writerLoop()
	return cq
}

// submit adds a commit job to the queue and returns when the job completes.
func (cq *commitQueue) submit(job *commitJob) commitJobResult {
	cq.mu.Lock()
	cq.jobs = append(cq.jobs, job)
	cq.cond.Signal()
	cq.mu.Unlock()
	return <-job.resultCh
}

// drain waits for pending jobs, then returns all of them as a batch.
// Returns nil when stopped.
func (cq *commitQueue) drain() []*commitJob {
	cq.mu.Lock()
	defer cq.mu.Unlock()

	for len(cq.jobs) == 0 && !cq.stopped {
		cq.cond.Wait()
	}
	if cq.stopped && len(cq.jobs) == 0 {
		return nil
	}

	jobs := cq.jobs
	cq.jobs = nil
	return jobs
}

// writerLoop is the background goroutine that processes commit batches.
func (cq *commitQueue) writerLoop() {
	defer cq.wg.Done()

	for {
		jobs := cq.drain()
		if jobs == nil {
			return
		}

		cq.engine.processCommitBatch(jobs)
	}
}

// stop shuts down the commit queue, processing any remaining pending jobs.
func (cq *commitQueue) stop() {
	cq.mu.Lock()
	cq.stopped = true
	cq.cond.Signal()
	cq.mu.Unlock()
	cq.wg.Wait()
}

// saveTableRefs captures current table pointers for affected domains.
// Used to restore state if a job fails mid-apply.
func saveTableRefs(state *readableState, affected map[string]struct{}) map[string]map[string]*tableState {
	refs := make(map[string]map[string]*tableState, len(affected))
	for domainName := range affected {
		ds, ok := state.domains[domainName]
		if !ok {
			continue
		}
		tables := make(map[string]*tableState, len(ds.tables))
		for k, v := range ds.tables {
			tables[k] = v
		}
		refs[domainName] = tables
	}
	return refs
}

// restoreTableRefs restores table pointers from a snapshot.
func restoreTableRefs(state *readableState, refs map[string]map[string]*tableState) {
	for domainName, tables := range refs {
		ds, ok := state.domains[domainName]
		if !ok {
			continue
		}
		ds.tables = tables
	}
}

// processCommitBatch processes multiple commits under a single writeMu
// acquisition, WAL write, state swap, and fsync.
func (engine *Engine) processCommitBatch(jobs []*commitJob) {
	batchStartedAt := time.Now()
	engine.writeMu.Lock()
	writeLockedAt := time.Now()

	// Merge affected domains across all jobs for a single COW clone.
	allAffected := make(map[string]struct{})
	for _, job := range jobs {
		for k := range job.affected {
			allAffected[k] = struct{}{}
		}
	}
	// Include subscriber domains that will receive VFK projection fan-outs.
	engine.addProjectionDomainsToAffected(allAffected, jobs)

	current := engine.readState.Load()
	newState := current.cloneForMutation(allAffected)

	savedLogicalTS := engine.logicalTS

	// Process each job, collecting WAL records for successful ones.
	type jobOutcome struct {
		commitLSN      uint64
		walRecordStart int
		walRecordCount int
		totalMutations int
		err            error
	}
	outcomes := make([]jobOutcome, len(jobs))
	var allWALRecords []ports.WALRecord
	var allAuditEntries []ports.AuditEntry
	runningLSN := engine.headLSN
	mutationCountBefore := engine.mutationCount

	// Batch-level caches: allocated once, cleared between jobs to avoid
	// per-job map allocation that creates short-lived GC garbage.
	// batchFKEntityCache: resolved entity root PKs for FK chain walks.
	batchFKEntityCache := make(map[entityFKCacheKey]string)
	// batchFKValCache: positive FK-existence lookups (hasBucket results).
	batchFKValCache := make(map[string]struct{})

	for i, job := range jobs {
		// Clear batch-level caches between jobs (different tx state).
		clear(batchFKEntityCache)
		clear(batchFKValCache)

		savedTSBefore := engine.logicalTS

		// Advance logicalTS for this job.
		engine.logicalTS += uint64(len(job.ordered)) + 2 // BEGIN + mutations + COMMIT

		predictedCommitLSN := runningLSN + uint64(len(job.ordered)) + 2

		// Validate write conflicts against accumulated state.
		if err := engine.validateWriteConflicts(newState, job.tx, job.ordered); err != nil {
			engine.logicalTS = savedTSBefore
			outcomes[i] = jobOutcome{err: err}
			continue
		}

		// Save table references for rollback on failure.
		tableRefs := saveTableRefs(newState, job.affected)

		// Apply mutations (DML-first path for all jobs).
		clonedTables := make(map[string]struct{}, len(job.ordered))
		entityCollector := make(map[string]map[string][]string)

		applyFailed := false
		for _, mutation := range job.ordered {
			// Invalidate FK validation cache on non-INSERT mutations
			// (DELETEs/UPDATEs may remove parent rows).
			if mutation.plan.Operation != planner.OperationInsert && len(batchFKValCache) > 0 {
				clear(batchFKValCache)
			}

			// Entity tracking for DELETE: collect before rows are removed.
			if mutation.plan.Operation == planner.OperationDelete {
				if ds := newState.domains[mutation.plan.DomainName]; ds != nil {
					if tbl := ds.tables[mutation.plan.TableName]; tbl != nil {
						var deletedRows []map[string]ast.Literal
						for _, rowSlice := range tbl.rows {
							row := rowToMap(tbl, rowSlice)
							if matchPredicate(row, mutation.plan.Filter, newState, engine) {
								deletedRows = append(deletedRows, row)
							}
						}
						collectEntityMutations(ds, mutation.plan, deletedRows, entityCollector)
					}
				}
			}

			if err := engine.applyPlanToStateTracked(newState, mutation.plan, predictedCommitLSN, clonedTables, batchFKValCache, entityCollector); err != nil {
				engine.logicalTS = savedTSBefore
				restoreTableRefs(newState, tableRefs)
				outcomes[i] = jobOutcome{err: err}
				applyFailed = true
				break
			}

			// Fan out DML changes to VFK projection shadow tables in subscriber domains.
			engine.fanoutProjections(newState, mutation.plan)

			// Entity tracking for INSERT/UPDATE: collect after mutation.
			if mutation.plan.Operation == planner.OperationInsert || mutation.plan.Operation == planner.OperationUpdate {
				if ds := newState.domains[mutation.plan.DomainName]; ds != nil {
					if tbl := ds.tables[mutation.plan.TableName]; tbl != nil {
						if mutation.plan.Operation == planner.OperationInsert {
							// Single-row fast path: avoid slice allocation.
							collectEntityMutationsSingle(ds, mutation.plan, rowToMap(tbl, tbl.rows[len(tbl.rows)-1]), entityCollector, batchFKEntityCache)
						} else {
							var affectedRows []map[string]ast.Literal
							for _, rowSlice := range tbl.rows {
								row := rowToMap(tbl, rowSlice)
								if matchPredicate(row, mutation.plan.Filter, newState, engine) {
									affectedRows = append(affectedRows, row)
								}
							}
							collectEntityMutations(ds, mutation.plan, affectedRows, entityCollector)
						}
					}
				}
			}
		}

		if applyFailed {
			continue
		}

		// Record entity versions after all mutations for this job.
		if len(entityCollector) > 0 {
			recordEntityVersions(newState, entityCollector, predictedCommitLSN)
		}

		// Collect audit entries from changeLog if an audit store is configured.
		//
		// Strategy — "lazy INSERT anchoring":
		//   • INSERT commits are NOT written to audit at INSERT time.
		//   • When the first UPDATE or DELETE for a row arrives, the original
		//     INSERT is found in the changeLog by PK and added to the audit
		//     batch together with the mutating entry.
		//   • Subsequent UPDATEs/DELETEs only write their own entry (INSERT
		//     already in audit).
		//   • FOR HISTORY queries: if rows are in audit → return from audit;
		//     if not (insert-only rows) → fall back to changeLog.
		if engine.auditStore != nil {
			// insertAuditKeys prevents adding the same INSERT commitLSN twice
			// within a single batch (e.g. two UPDATEs to the same row).
			type insertAuditKey struct {
				domain, table string
				lsn           uint64
			}
			insertAuditKeys := make(map[insertAuditKey]struct{})

			for domainName := range job.affected {
				ds := newState.domains[domainName]
				if ds == nil {
					continue
				}
				for tableName, tbl := range ds.tables {
					pkCol := tbl.primaryKey

					// Phase 1: collect UPDATE/DELETE entries for this commit.
					var tableEntries []ports.AuditEntry
					var mutatedPKVals []ast.Literal
					for i := len(tbl.changeLog) - 1; i >= 0; i-- {
						e := tbl.changeLog[i]
						if e.commitLSN < predictedCommitLSN {
							break
						}
						if e.commitLSN != predictedCommitLSN || e.operation == "INSERT" {
							continue
						}
						tableEntries = append(tableEntries, ports.AuditEntry{
							CommitLSN: e.commitLSN,
							Domain:    domainName,
							Table:     tableName,
							Operation: e.operation,
							OldRow:    e.oldRow,
							NewRow:    e.newRow,
						})
						if pkCol != "" && e.oldRow != nil {
							if pkV, ok := e.oldRow[pkCol]; ok {
								mutatedPKVals = append(mutatedPKVals, pkV)
							}
						}
					}
					if len(tableEntries) == 0 {
						continue
					}
					allAuditEntries = append(allAuditEntries, tableEntries...)

					// Phase 2: lazy INSERT anchoring.
					// For each mutated PK, find the original INSERT in the changeLog
					// and add it to the batch so FOR HISTORY returns complete history.
					if pkCol == "" || len(mutatedPKVals) == 0 {
						continue
					}
					needed := make(map[string]struct{}, len(mutatedPKVals))
					for _, v := range mutatedPKVals {
						needed[literalKey(v)] = struct{}{}
					}
					for _, entry := range tbl.changeLog {
						if len(needed) == 0 {
							break
						}
						if entry.operation != "INSERT" || entry.commitLSN >= predictedCommitLSN || entry.newRow == nil {
							continue
						}
						pkV, ok := entry.newRow[pkCol]
						if !ok {
							continue
						}
						k := literalKey(pkV)
						if _, want := needed[k]; !want {
							continue
						}
						// Guard against duplicates within this batch.
						iKey := insertAuditKey{domainName, tableName, entry.commitLSN}
						if _, dup := insertAuditKeys[iKey]; !dup {
							insertAuditKeys[iKey] = struct{}{}
							allAuditEntries = append(allAuditEntries, ports.AuditEntry{
								CommitLSN: entry.commitLSN,
								Domain:    domainName,
								Table:     tableName,
								Operation: "INSERT",
								OldRow:    nil,
								NewRow:    entry.newRow,
							})
						}
						delete(needed, k)
					}
				}
			}
		}

		// Build WAL records for this job.
		walTS := savedTSBefore
		walRecordStart := len(allWALRecords)

		walTS++
		allWALRecords = append(allWALRecords, ports.WALRecord{TxID: job.tx.id, Type: walTypeBegin, Timestamp: walTS})

		for idx := range job.ordered {
			walTS++
			allWALRecords = append(allWALRecords, ports.WALRecord{TxID: job.tx.id, Type: walTypeMutation, Timestamp: walTS, Payload: job.preEncoded[idx]})
		}

		walTS++
		allWALRecords = append(allWALRecords, ports.WALRecord{TxID: job.tx.id, Type: walTypeCommit, Timestamp: walTS})

		outcomes[i] = jobOutcome{
			commitLSN:      predictedCommitLSN,
			walRecordStart: walRecordStart,
			walRecordCount: len(allWALRecords) - walRecordStart,
			totalMutations: len(job.ordered),
		}
		runningLSN = predictedCommitLSN

		// Count mutations for snapshot scheduling.
		engine.mutationCount += uint64(len(job.ordered))
	}

	// Write all WAL records — either through Raft quorum or directly to the WAL.
	//
	// Raft path (cluster mode):
	//   All records are proposed via RaftCommitter.ApplyBatch(), which appends
	//   every record locally and issues a single broadcastAndCommit for the
	//   highest index.  This collapses N sequential round-trips into 1,
	//   restoring throughput parity with the standalone path.
	//
	// Direct path (standalone mode):
	//   Records are written without fsync (AppendBatchNoSync) and then fsynced
	//   once per batch by the group commit syncer outside the lock.
	var raftPath bool
	var applyDur time.Duration
	if len(allWALRecords) > 0 {
		if engine.raftCommitter != nil {
			raftPath = true
			var lastLSN uint64
			raftRecords := make([]ports.RaftRecord, len(allWALRecords))
			for i, rec := range allWALRecords {
				raftRecords[i] = ports.RaftRecord{Type: rec.Type, TxID: rec.TxID, Payload: rec.Payload}
			}
			applyStartedAt := time.Now()
			lsns, walErr := engine.raftCommitter.ApplyBatch(context.Background(), raftRecords)
			applyDur = time.Since(applyStartedAt)
			if walErr == nil {
				for i, lsn := range lsns {
					allWALRecords[i].LSN = lsn
					if lsn > lastLSN {
						lastLSN = lsn
					}
				}
			}
			if walErr != nil {
				engine.logicalTS = savedLogicalTS
				engine.writeMu.Unlock()
				for i, job := range jobs {
					if outcomes[i].err == nil {
						job.resultCh <- commitJobResult{err: walErr}
					} else {
						job.resultCh <- commitJobResult{err: outcomes[i].err}
					}
				}
				return
			}
			if lastLSN > engine.headLSN {
				engine.headLSN = lastLSN
			}
		} else {
			applyStartedAt := time.Now()
			lsns, err := engine.logStore.(ports.NoSyncBatchAppender).AppendBatchNoSync(
				context.Background(), allWALRecords)
			applyDur = time.Since(applyStartedAt)
			if err != nil {
				// WAL write failed — fail all pending (non-error) jobs.
				engine.logicalTS = savedLogicalTS
				engine.writeMu.Unlock()
				for i, job := range jobs {
					if outcomes[i].err == nil {
						job.resultCh <- commitJobResult{err: fmt.Errorf("append wal batch: %w", err)}
					} else {
						job.resultCh <- commitJobResult{err: outcomes[i].err}
					}
				}
				return
			}
			for i, lsn := range lsns {
				allWALRecords[i].LSN = lsn
			}
			// Update headLSN to the last LSN from the batch.
			lastLSN := lsns[len(lsns)-1]
			if lastLSN > engine.headLSN {
				engine.headLSN = lastLSN
			}
		}

		// Flush audit entries asynchronously after successful WAL write.
		if engine.auditStore != nil && len(allAuditEntries) > 0 {
			entriesToFlush := allAuditEntries
			go func() {
				if err := engine.auditStore.AppendBatch(context.Background(), entriesToFlush); err != nil {
					if engine.perf != nil {
						engine.perf.recordAuditError()
					}
					slog.Warn("audit: append batch failed", "err", err, "count", len(entriesToFlush))
				}
			}()
		}
		if engine.timestampIndex != nil {
			if err := engine.timestampIndex.append(allWALRecords); err != nil {
				slog.Warn("timestamp index append failed", "error", err.Error(), "records", len(allWALRecords))
			}
		}
	}

	// Update state metadata and atomic swap.
	newState.headLSN = engine.headLSN
	newState.logicalTS = engine.logicalTS
	engine.readState.Store(newState)
	engine.lastWriteUnixNano.Store(time.Now().UnixNano())

	// Snapshot check: one per batch based on total mutations.
	// In-memory snapshots are kept for fast time-travel during this session.
	// Disk checkpoint is written periodically for crash recovery.
	var needSnapshot bool
	var needDiskCheckpoint bool
	totalBatchMutations := 0
	for _, o := range outcomes {
		if o.err == nil {
			totalBatchMutations += o.totalMutations
		}
	}
	if engine.snapshots != nil && totalBatchMutations > 0 {
		interval := uint64(adaptiveSnapshotInterval(engine.mutationCount))
		if interval > 0 && mutationCountBefore/interval != engine.mutationCount/interval {
			needSnapshot = true
		}
	}
	// Periodic disk checkpoint: trigger when the WAL has grown by at least
	// diskCheckpointWALBytes since the last checkpoint. Using WAL size rather
	// than mutation count makes the trigger independent of row size and workload.
	if needSnapshot && engine.snapDir != "" && !engine.snapWriteInFlight.Load() {
		var walSize uint64
		if sizer, ok := engine.logStore.(ports.Sizer); ok {
			if sz, err := sizer.TotalSize(); err == nil && sz > 0 {
				walSize = uint64(sz)
			}
		}
		checkpointBytes := adaptiveDiskCheckpointWALBytes(engine.mutationCount)
		if walSize-engine.lastCheckpointWALSize >= checkpointBytes {
			needDiskCheckpoint = true
			engine.lastCheckpointWALSize = walSize
		}
	}

	engine.writeMu.Unlock()
	writeHoldDur := time.Since(writeLockedAt)

	// Capture in-memory snapshot for time-travel asynchronously so foreground
	// commits don't pay the O(database size) deep-clone cost.
	if needSnapshot && engine.snapCaptureInFlight.CompareAndSwap(false, true) {
		engine.snapshotWg.Add(1)
		go func(mutationCount uint64, needDiskCheckpoint bool, dir string) {
			defer engine.snapshotWg.Done()
			defer engine.snapCaptureInFlight.Store(false)

			for {
				lastWrite := engine.lastWriteUnixNano.Load()
				if lastWrite == 0 {
					break
				}
				idleFor := time.Since(time.Unix(0, lastWrite))
				if idleFor >= snapshotIdleDelay {
					break
				}
				time.Sleep(snapshotIdleDelay - idleFor)
			}

			state := engine.readState.Load()
			engine.writeMu.Lock()
			catalog := cloneCatalog(engine.catalog)
			engine.writeMu.Unlock()

			snap := captureSnapshotWithCatalog(state, catalog)
			snapStore := engine.snapshots
			snapStore.mu.Lock()
			snapStore.add(snap)
			snapStore.evictOldest(adaptiveMaxMemorySnapshots(mutationCount))
			snapStore.mu.Unlock()

			if needDiskCheckpoint {
				engine.snapWriteInFlight.Store(true)
				defer engine.snapWriteInFlight.Store(false)
				// Allocate next sequence number and capture delta base under writeMu.
				engine.writeMu.Lock()
				engine.snapSeq++
				seq := engine.snapSeq
				isFull := (seq-1)%fullSnapshotFrequency == 0
				prevLogicalTS := engine.lastDiskSnapshotLogicalTS
				engine.writeMu.Unlock()
				if err := writeSnapshotToDir(dir, seq, snap, isFull, prevLogicalTS); err != nil {
					slog.Warn("periodic disk checkpoint failed", "error", err.Error(), "seq", seq, "full", isFull)
				} else {
					slog.Info("periodic disk checkpoint written", "lsn", snap.lsn, "seq", seq, "full", isFull)
					engine.writeMu.Lock()
					engine.lastDiskSnapshotLogicalTS = snap.logicalTS
					engine.writeMu.Unlock()
					_ = cleanupOldSnapshotFiles(dir, maxDiskSnapshots)
					engine.maybeGCWAL(snap.lsn)
				}
			}
		}(engine.mutationCount, needDiskCheckpoint, engine.snapDir)
	}

	// Group commit fsync: only needed when NOT using the Raft path.
	// In Raft mode each entry is already durable on a quorum of nodes;
	// the leader's local fsync was performed by AppendLeader → store.Append.
	var syncErr error
	if len(allWALRecords) > 0 && !raftPath {
		syncErr = engine.groupSync.requestSync()
	}

	// Notify all jobs with results.
	for i, job := range jobs {
		if outcomes[i].err != nil {
			job.resultCh <- commitJobResult{err: outcomes[i].err}
		} else {
			err := syncErr
			if err != nil {
				err = fmt.Errorf("group commit sync: %w", err)
			}
			job.resultCh <- commitJobResult{commitLSN: outcomes[i].commitLSN, err: err}
		}
	}

	maxQueueWait := time.Duration(0)
	for _, job := range jobs {
		if !job.queuedAt.IsZero() {
			if queueWait := batchStartedAt.Sub(job.queuedAt); queueWait > maxQueueWait {
				maxQueueWait = queueWait
			}
		}
	}
	engine.perf.recordCommitBatch(len(jobs), len(allWALRecords), maxQueueWait, writeHoldDur, applyDur)

	// Record perf stats for all jobs in this batch.
	for _, job := range jobs {
		engine.perf.recordCommit(time.Since(job.commitStart))
		engine.perf.recordEndTx()
	}
}
