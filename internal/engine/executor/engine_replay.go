package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"asql/internal/engine/domains"
	"asql/internal/engine/planner"
	"asql/internal/engine/ports"
)

var errSnapshotWALMismatch = errors.New("snapshot and wal are inconsistent")

// Replay replays WAL mutations to rebuild state.
// If persisted snapshots exist on disk, it restores ALL intermediate
// snapshots for time-travel and uses the latest for engine state,
// then only replays WAL records after that LSN.
func (engine *Engine) Replay(ctx context.Context) error {
	started := time.Now()
	defer func() {
		if engine != nil && engine.perf != nil {
			engine.perf.recordReplay(time.Since(started))
		}
	}()

	loadedTimestampIndex := false
	if engine.timestampIndex != nil {
		loaded, err := engine.timestampIndex.load()
		if err != nil {
			slog.Warn("failed to load timestamp index; rebuilding from wal", "error", err.Error())
		} else {
			loadedTimestampIndex = loaded
		}
	}

	// When a snapshot exists, read only the WAL delta after the snapshot LSN —
	// O(delta) instead of O(total WAL), making cold start proportional to
	// unsnapshotted writes rather than total history.
	if engine.snapDir != "" {
		snapshots, maxSeq, err := readAllSnapshotsFromDir(engine.snapDir)
		if err != nil {
			return fmt.Errorf("load snapshots from dir: %w", err)
		}

		// If no numbered files found, try migrating a legacy state.snap.
		if len(snapshots) == 0 {
			snapshots, maxSeq, err = migrateOldSnapshot(engine.snapDir)
			if err != nil {
				slog.Warn("legacy snapshot migration failed, falling back to full replay", "error", err.Error())
			}
		}

		if maxSeq > engine.snapSeq {
			engine.snapSeq = maxSeq
		}

		if len(snapshots) > 0 {
			latest := &snapshots[len(snapshots)-1]
			// Read only WAL records after the snapshot LSN. If the snapshot
			// already covers the full WAL (e.g. post-GC), delta is empty and
			// the snapshot state is used as-is.
			deltaRecords, err := engine.logStore.ReadFrom(ctx, latest.lsn+1, 0)
			if err != nil {
				return fmt.Errorf("replay read wal delta: %w", err)
			}
			if len(deltaRecords) > 0 && deltaRecords[0].LSN != latest.lsn+1 {
				return fmt.Errorf("%w: snapshot lsn=%d next wal lsn=%d", errSnapshotWALMismatch, latest.lsn, deltaRecords[0].LSN)
			}
			if err := engine.replayFromSnapshots(snapshots, deltaRecords); err != nil {
				return err
			}
			return engine.ensureTimestampIndex(ctx, loadedTimestampIndex, deltaRecords, false)
		}
	}

	// No snapshot available: full WAL rebuild from the beginning.
	records, err := engine.readAllRecords(ctx)
	if err != nil {
		return fmt.Errorf("replay read wal: %w", err)
	}
	if err := engine.rebuildFromRecords(records, 0, false); err != nil {
		return err
	}
	return engine.ensureTimestampIndex(ctx, loadedTimestampIndex, records, true)
}

// replayFromSnapshots restores all intermediate snapshots into the in-memory
// store (for fast time-travel) and sets engine state from the latest one.
// Only WAL records after the latest snapshot's LSN are replayed.
func (engine *Engine) replayFromSnapshots(snapshots []engineSnapshot, records []ports.WALRecord) error {
	engine.writeMu.Lock()

	latest := &snapshots[len(snapshots)-1]

	// Restore base state from the latest snapshot.
	engine.catalog = cloneCatalog(latest.catalog)
	newState := readableStateFromSnapshotShared(latest)
	engine.readState.Store(newState)
	engine.logicalTS = latest.logicalTS
	engine.headLSN = latest.lsn
	engine.mutationCount = 0
	// Track the logical timestamp of the latest disk snapshot so that the first
	// post-restart delta checkpoint uses the correct base for buildChangedDomains.
	engine.lastDiskSnapshotLogicalTS = latest.logicalTS

	// Load ALL intermediate snapshots into the in-memory store.
	if engine.snapshots != nil {
		engine.snapshots.clear()
		for i := range snapshots {
			engine.snapshots.add(snapshots[i])
		}
	}

	engine.writeMu.Unlock()

	// Replay the WAL delta (records after the snapshot LSN).
	return engine.rebuildFromRecordsAfterSnapshot(records, latest.lsn)
}

// ReplayToLSN replays WAL mutations up to targetLSN and resets in-memory state.
func (engine *Engine) ReplayToLSN(ctx context.Context, targetLSN uint64) error {
	if currentState := engine.readState.Load(); currentState != nil && currentState.headLSN == targetLSN {
		return nil
	}

	records, replayPlans, err := engine.readAllReplayRecords(ctx)
	if err != nil {
		return fmt.Errorf("replay to lsn read wal: %w", err)
	}

	return engine.rebuildFromRecordsWithReplayPlans(records, replayPlans, targetLSN, true)
}

func (engine *Engine) readAllReplayRecords(ctx context.Context) ([]ports.WALRecord, []replayPlanCacheEntry, error) {
	records, err := engine.readAllRecords(ctx)
	if err != nil {
		return nil, nil, err
	}
	headLSN := uint64(0)
	if len(records) > 0 {
		headLSN = records[len(records)-1].LSN
	}

	if cached := engine.walReplayPlansCache.Load(); cached != nil && cached.headLSN == headLSN && len(cached.entries) == len(records) {
		return records, cached.entries, nil
	}

	engine.walCacheMu.Lock()
	defer engine.walCacheMu.Unlock()

	if cached := engine.walReplayPlansCache.Load(); cached != nil && cached.headLSN == headLSN && len(cached.entries) == len(records) {
		return records, cached.entries, nil
	}

	entries := make([]replayPlanCacheEntry, len(records))
	for i, record := range records {
		if record.Type != walTypeMutation {
			continue
		}
		_, plan, err := decodeMutationPayloadV2(record.Payload)
		if err != nil {
			return nil, nil, fmt.Errorf("decode mutation payload lsn=%d: %w", record.LSN, err)
		}
		entries[i] = replayPlanCacheEntry{plan: plan, ok: true}
	}
	engine.walReplayPlansCache.Store(&walReplayPlanCache{headLSN: headLSN, entries: entries})
	return records, entries, nil
}

// LSNForTimestamp resolves the latest LSN whose timestamp is <= logicalTimestamp.
func (engine *Engine) LSNForTimestamp(ctx context.Context, logicalTimestamp uint64) (uint64, error) {
	if engine.timestampIndex != nil {
		return engine.timestampIndex.Resolve(logicalTimestamp), nil
	}

	records, err := engine.readAllRecords(ctx)
	if err != nil {
		return 0, fmt.Errorf("resolve lsn for timestamp read wal: %w", err)
	}

	var resolved uint64
	for _, record := range records {
		if record.Timestamp <= logicalTimestamp && record.LSN > resolved {
			resolved = record.LSN
		}
	}

	return resolved, nil
}

func (engine *Engine) ensureTimestampIndex(ctx context.Context, loaded bool, replayRecords []ports.WALRecord, recordsCoverFullHistory bool) error {
	if engine.timestampIndex == nil {
		return nil
	}
	if recordsCoverFullHistory {
		if err := engine.timestampIndex.rebuild(replayRecords); err != nil {
			slog.Warn("failed to persist rebuilt timestamp index", "error", err.Error())
		}
		return nil
	}
	if loaded {
		lastIndexedLSN := engine.timestampIndex.LastLSN()
		if lastIndexedLSN >= engine.headLSN {
			return nil
		}
		missingRecords, err := engine.logStore.ReadFrom(ctx, lastIndexedLSN+1, 0)
		if err != nil {
			return fmt.Errorf("read wal for timestamp index catch-up: %w", err)
		}
		if err := engine.timestampIndex.append(missingRecords); err != nil {
			slog.Warn("failed to persist timestamp index catch-up", "error", err.Error())
		}
		return nil
	}

	allRecords, err := engine.readAllRecords(ctx)
	if err != nil {
		return fmt.Errorf("read wal for timestamp index rebuild: %w", err)
	}
	if err := engine.timestampIndex.rebuild(allRecords); err != nil {
		slog.Warn("failed to persist rebuilt timestamp index", "error", err.Error())
	}
	return nil
}

// buildStateFromRecords reconstructs a readableState at the given target LSN
// using pre-loaded WAL records. If targetLSN >= current head, returns the
// current materialized state directly. Otherwise it creates a temporary engine,
// restores from the closest snapshot, and replays WAL up to targetLSN.
func (engine *Engine) buildStateFromRecords(records []ports.WALRecord, targetLSN uint64) (*readableState, error) {
	currentState := engine.readState.Load()
	if targetLSN >= currentState.headLSN && currentState.headLSN > 0 {
		return currentState, nil
	}
	if cachedState, ok := engine.cachedHistoricalState(targetLSN); ok {
		return cachedState, nil
	}

	temp := &Engine{
		catalog:          domains.NewCatalog(),
		scanStats:        make(map[scanStrategy]uint64),
		logStore:         engine.logStore,
		snapshots:        engine.snapshots,
		vfkSubscriptions: make(map[string][]projectionSubscription),
	}
	tempInitial := &readableState{domains: make(map[string]*domainState)}
	temp.readState.Store(tempInitial)

	engine.snapshots.mu.Lock()
	snap := engine.snapshots.closest(targetLSN)
	engine.snapshots.mu.Unlock()

	startFromLSN := uint64(0)
	if snap != nil {
		temp.restoreSnapshot(snap)
		startFromLSN = snap.lsn
	}

	if err := temp.rebuildFromRecordsPartial(records, startFromLSN, targetLSN); err != nil {
		return nil, err
	}

	tempState := temp.readState.Load()
	engine.storeHistoricalState(targetLSN, tempState)
	return tempState, nil
}

func (engine *Engine) rebuildFromRecords(records []ports.WALRecord, targetLSN uint64, bounded bool) error {
	return engine.rebuildFromRecordsWithReplayPlans(records, nil, targetLSN, bounded)
}

func (engine *Engine) rebuildFromRecordsWithReplayPlans(records []ports.WALRecord, replayPlans []replayPlanCacheEntry, targetLSN uint64, bounded bool) error {

	engine.writeMu.Lock()
	defer engine.writeMu.Unlock()

	engine.replayMode = true
	defer func() { engine.replayMode = false }()

	engine.catalog = domains.NewCatalog()
	newState := &readableState{domains: make(map[string]*domainState)}
	engine.logicalTS = 0
	engine.headLSN = 0
	engine.mutationCount = 0
	if engine.snapshots != nil {
		engine.snapshots.clear()
	}
	captureReplaySnapshots := !bounded

	txCommitLSN := buildTxCommitLSNIndex(records)
	txEntityCollectors := make(map[string]map[string]map[string][]string)
	txClonedTables := make(map[string]map[string]struct{})

	for i, record := range records {
		if bounded && record.LSN > targetLSN {
			break
		}

		if record.LSN > engine.headLSN {
			engine.headLSN = record.LSN
		}

		if record.Timestamp > engine.logicalTS {
			engine.logicalTS = record.Timestamp
		}

		// Record entity versions eagerly on COMMIT so that subsequent
		// transactions can resolve versioned FK references during replay.
		// WAL records are in LSN order, so this preserves deterministic
		// version numbering without explicit sorting.
		if record.Type == walTypeCommit {
			if collector := txEntityCollectors[record.TxID]; len(collector) > 0 {
				recordEntityVersions(newState, collector, record.LSN)
				delete(txEntityCollectors, record.TxID)
			}
			delete(txClonedTables, record.TxID)
			continue
		}

		if record.Type != walTypeMutation {
			continue
		}

		plan, ok, err := replayPlanForRecord(record, replayPlans, i)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		commitLSN := txCommitLSN[record.TxID]
		if txEntityCollectors[record.TxID] == nil {
			txEntityCollectors[record.TxID] = make(map[string]map[string][]string)
		}
		if txClonedTables[record.TxID] == nil {
			txClonedTables[record.TxID] = make(map[string]struct{})
		}
		if err := engine.applyMutationPlanWithEntityTrackingTracked(newState, plan, commitLSN, txEntityCollectors[record.TxID], txClonedTables[record.TxID], nil); err != nil {
			return fmt.Errorf("apply replay mutation lsn=%d: %w", record.LSN, err)
		}

		engine.mutationCount++

		// Capture periodic snapshots for fast time-travel queries.
		interval := uint64(adaptiveSnapshotInterval(engine.mutationCount))
		if captureReplaySnapshots && engine.snapshots != nil && interval > 0 && engine.mutationCount%interval == 0 {
			newState.headLSN = engine.headLSN
			newState.logicalTS = engine.logicalTS
			engine.snapshots.add(captureSnapshot(newState, engine.catalog))
		}
	}

	// Record entity versions for any remaining transactions whose COMMIT
	// records were not within the replayed range (bounded replay).
	replayEntityVersionsInOrder(newState, txEntityCollectors, txCommitLSN)

	newState.headLSN = engine.headLSN
	newState.logicalTS = engine.logicalTS

	// Flatten all index overlay chains produced during replay so that
	// subsequent reads get O(1) hash lookups instead of walking deep chains.
	flattenStateIndexes(newState)

	engine.readState.Store(newState)

	// Add a final snapshot at head for time-travel coverage.
	if captureReplaySnapshots && engine.snapshots != nil && engine.headLSN > 0 {
		snap := captureSnapshot(newState, engine.catalog)
		engine.snapshots.add(snap)
	}

	// Write a single checkpoint to disk for fast restart recovery.
	if captureReplaySnapshots && engine.snapDir != "" && engine.headLSN > 0 {
		engine.persistAllSnapshots()
	}

	// Evict old in-memory snapshots to bound memory usage.
	if captureReplaySnapshots && engine.snapshots != nil {
		engine.snapshots.evictOldest(adaptiveMaxMemorySnapshots(engine.mutationCount))
	}

	return nil
}

func replayPlanForRecord(record ports.WALRecord, replayPlans []replayPlanCacheEntry, idx int) (planner.Plan, bool, error) {
	if record.Type != walTypeMutation {
		return planner.Plan{}, false, nil
	}
	if replayPlans != nil && idx >= 0 && idx < len(replayPlans) && replayPlans[idx].ok {
		return replayPlans[idx].plan, true, nil
	}
	_, plan, err := decodeMutationPayloadV2(record.Payload)
	if err != nil {
		return planner.Plan{}, false, fmt.Errorf("decode mutation payload lsn=%d: %w", record.LSN, err)
	}
	return plan, true, nil
}

// rebuildFromRecordsAfterSnapshot replays only WAL mutations with LSN > afterLSN.
// Used after loading a persisted snapshot.
func (engine *Engine) rebuildFromRecordsAfterSnapshot(records []ports.WALRecord, afterLSN uint64) error {
	return engine.rebuildFromRecordsAfterSnapshotWithOptions(records, afterLSN, true)
}

func (engine *Engine) rebuildFromRecordsAfterSnapshotWithOptions(records []ports.WALRecord, afterLSN uint64, captureSnapshots bool) error {
	engine.writeMu.Lock()
	defer engine.writeMu.Unlock()

	engine.replayMode = true
	defer func() { engine.replayMode = false }()

	current := engine.readState.Load()
	if current != nil && current.headLSN > afterLSN {
		afterLSN = current.headLSN
	}
	if len(records) == 0 || records[len(records)-1].LSN <= afterLSN {
		return nil
	}
	newState := current.cloneForMutation(allDomainKeys(current.domains))

	txCommitLSN := buildTxCommitLSNIndex(records)
	txEntityCollectors := make(map[string]map[string]map[string][]string)
	txClonedTables := make(map[string]map[string]struct{})
	touchedTables := make(map[string]map[string]struct{})

	for _, record := range records {
		if record.LSN <= afterLSN {
			continue
		}

		if record.LSN > engine.headLSN {
			engine.headLSN = record.LSN
		}
		if record.Timestamp > engine.logicalTS {
			engine.logicalTS = record.Timestamp
		}

		// Record entity versions eagerly on COMMIT (see rebuildFromRecords).
		if record.Type == walTypeCommit {
			if collector := txEntityCollectors[record.TxID]; len(collector) > 0 {
				recordEntityVersions(newState, collector, record.LSN)
				delete(txEntityCollectors, record.TxID)
			}
			delete(txClonedTables, record.TxID)
			continue
		}

		if record.Type != walTypeMutation {
			continue
		}

		_, plan, err := decodeMutationPayloadV2(record.Payload)
		if err != nil {
			return fmt.Errorf("decode mutation payload lsn=%d: %w", record.LSN, err)
		}
		if touchedTables[plan.DomainName] == nil {
			touchedTables[plan.DomainName] = make(map[string]struct{})
		}
		touchedTables[plan.DomainName][plan.TableName] = struct{}{}

		commitLSN := txCommitLSN[record.TxID]
		if txEntityCollectors[record.TxID] == nil {
			txEntityCollectors[record.TxID] = make(map[string]map[string][]string)
		}
		if txClonedTables[record.TxID] == nil {
			txClonedTables[record.TxID] = make(map[string]struct{})
		}
		if err := engine.applyMutationPlanWithEntityTrackingTracked(newState, plan, commitLSN, txEntityCollectors[record.TxID], txClonedTables[record.TxID], nil); err != nil {
			return fmt.Errorf("apply replay mutation lsn=%d: %w", record.LSN, err)
		}

		engine.mutationCount++

		interval := uint64(adaptiveSnapshotInterval(engine.mutationCount))
		if captureSnapshots && engine.snapshots != nil && interval > 0 && engine.mutationCount%interval == 0 {
			newState.headLSN = engine.headLSN
			newState.logicalTS = engine.logicalTS
			engine.snapshots.add(captureSnapshot(newState, engine.catalog))
		}
	}

	// Record entity versions for any remaining transactions.
	replayEntityVersionsInOrder(newState, txEntityCollectors, txCommitLSN)

	newState.headLSN = engine.headLSN
	newState.logicalTS = engine.logicalTS

	// Incremental follower direct-apply runs on every committed delta, so a
	// full flatten of touched indexes makes replay cost grow with table size.
	// Keep full flattening for snapshot-producing catch-up, but use a cheaper
	// tiered compaction pass for direct apply so cost stays closer to O(delta).
	if captureSnapshots {
		flattenStateIndexesForTables(newState, touchedTables)
	} else {
		compactStateIndexesForTables(newState, touchedTables)
	}

	engine.readState.Store(newState)

	// Add a final snapshot at head for time-travel coverage.
	if captureSnapshots && engine.snapshots != nil && engine.headLSN > afterLSN {
		snap := captureSnapshot(newState, engine.catalog)
		engine.snapshots.add(snap)
	}

	// Do NOT persist a disk checkpoint here. This path runs during follower
	// catch-up and post-snapshot delta replay, where writing a new checkpoint on
	// every incremental WAL application causes pathological background IO under
	// sustained ingest. Runtime disk persistence is handled by the periodic
	// snapshot scheduler; startup full replay already persists a single checkpoint
	// through rebuildFromRecords().

	// Evict old in-memory snapshots to bound memory usage.
	if captureSnapshots && engine.snapshots != nil {
		engine.snapshots.evictOldest(adaptiveMaxMemorySnapshots(engine.mutationCount))
	}

	return nil
}

// CatchUp replays WAL records that have been appended externally (e.g. by
// Raft follower replication) but not yet applied to the in-memory readState.
// upToLSN bounds the replay to committed entries only — records with LSN >
// upToLSN are skipped.  Pass 0 to replay everything in the WAL.
func (engine *Engine) CatchUp(ctx context.Context, upToLSN uint64) error {
	current := engine.readState.Load()
	if upToLSN > 0 && upToLSN <= current.headLSN {
		return nil // already up to date
	}
	records, err := engine.logStore.ReadFrom(ctx, current.headLSN+1, 0)
	if err != nil {
		return fmt.Errorf("catchup read wal: %w", err)
	}
	if len(records) == 0 {
		return nil
	}
	// Bound to committed entries only.
	if upToLSN > 0 {
		cut := 0
		for i, r := range records {
			if r.LSN > upToLSN {
				break
			}
			cut = i + 1
		}
		records = records[:cut]
		if len(records) == 0 {
			return nil
		}
	}
	latest := engine.readState.Load()
	if latest != nil && len(records) > 0 && records[len(records)-1].LSN <= latest.headLSN {
		return nil
	}
	return engine.rebuildFromRecordsAfterSnapshot(records, current.headLSN)
}

// ApplyCommittedRecords applies a directly supplied committed WAL delta when it
// is contiguous with the current in-memory head. This avoids rereading the WAL
// on followers after every AppendEntries commit notification. When the delta is
// missing records, out of order, or unavailable, it safely falls back to the
// standard catch-up path.
func (engine *Engine) ApplyCommittedRecords(ctx context.Context, upToLSN uint64, records []ports.WALRecord) error {
	current := engine.readState.Load()
	if current == nil {
		logDirectApplyFallback("nil-state", 0, upToLSN, records)
		return engine.CatchUp(ctx, upToLSN)
	}
	if upToLSN > 0 && upToLSN <= current.headLSN {
		return nil
	}
	if len(records) == 0 {
		logDirectApplyFallback("empty-records", current.headLSN, upToLSN, records)
		return engine.CatchUp(ctx, upToLSN)
	}

	expected := current.headLSN + 1
	filtered := make([]ports.WALRecord, 0, len(records))
	for _, record := range records {
		if record.LSN <= current.headLSN {
			continue
		}
		if upToLSN > 0 && record.LSN > upToLSN {
			break
		}
		filtered = append(filtered, record)
	}
	if len(filtered) == 0 {
		return nil
	}
	if filtered[0].LSN != expected {
		logDirectApplyFallback("unexpected-first-lsn", current.headLSN, upToLSN, filtered)
		return engine.CatchUp(ctx, upToLSN)
	}
	for i := 1; i < len(filtered); i++ {
		if filtered[i].LSN != filtered[i-1].LSN+1 {
			logDirectApplyFallback("non-contiguous-records", current.headLSN, upToLSN, filtered)
			return engine.CatchUp(ctx, upToLSN)
		}
	}
	if upToLSN > 0 && filtered[len(filtered)-1].LSN < upToLSN {
		logDirectApplyFallback("missing-commit-tail", current.headLSN, upToLSN, filtered)
		return engine.CatchUp(ctx, upToLSN)
	}

	return engine.rebuildFromRecordsAfterSnapshotWithOptions(filtered, current.headLSN, false)
}

func logDirectApplyFallback(reason string, headLSN, upToLSN uint64, records []ports.WALRecord) {
	firstLSN := uint64(0)
	lastLSN := uint64(0)
	if len(records) > 0 {
		firstLSN = records[0].LSN
		lastLSN = records[len(records)-1].LSN
	}
	slog.Info("engine direct-apply fallback",
		"reason", reason,
		"head_lsn", headLSN,
		"target_lsn", upToLSN,
		"records", len(records),
		"first_lsn", firstLSN,
		"last_lsn", lastLSN)
}

// rebuildFromRecordsPartial replays only WAL mutations with LSN in (afterLSN, targetLSN].
// This is used by the snapshot-accelerated time-travel path.
func (engine *Engine) rebuildFromRecordsPartial(records []ports.WALRecord, afterLSN, targetLSN uint64) error {
	engine.writeMu.Lock()
	defer engine.writeMu.Unlock()

	engine.replayMode = true
	defer func() { engine.replayMode = false }()

	current := engine.readState.Load()
	newState := current.cloneForMutation(allDomainKeys(current.domains))

	txCommitLSN := buildTxCommitLSNIndex(records)
	txEntityCollectors := make(map[string]map[string]map[string][]string)
	txClonedTables := make(map[string]map[string]struct{})

	for _, record := range records {
		if record.LSN <= afterLSN {
			continue
		}
		if record.LSN > targetLSN {
			break
		}

		if record.LSN > engine.headLSN {
			engine.headLSN = record.LSN
		}
		if record.Timestamp > engine.logicalTS {
			engine.logicalTS = record.Timestamp
		}

		// Record entity versions eagerly on COMMIT (see rebuildFromRecords).
		if record.Type == walTypeCommit {
			if collector := txEntityCollectors[record.TxID]; len(collector) > 0 {
				recordEntityVersions(newState, collector, record.LSN)
				delete(txEntityCollectors, record.TxID)
			}
			delete(txClonedTables, record.TxID)
			continue
		}

		if record.Type != walTypeMutation {
			continue
		}

		_, plan, err := decodeMutationPayloadV2(record.Payload)
		if err != nil {
			return fmt.Errorf("decode mutation payload lsn=%d: %w", record.LSN, err)
		}

		commitLSN := txCommitLSN[record.TxID]
		if txEntityCollectors[record.TxID] == nil {
			txEntityCollectors[record.TxID] = make(map[string]map[string][]string)
		}
		if txClonedTables[record.TxID] == nil {
			txClonedTables[record.TxID] = make(map[string]struct{})
		}
		if err := engine.applyMutationPlanWithEntityTrackingTracked(newState, plan, commitLSN, txEntityCollectors[record.TxID], txClonedTables[record.TxID], nil); err != nil {
			return fmt.Errorf("apply replay mutation lsn=%d: %w", record.LSN, err)
		}
	}

	// Record entity versions for any remaining transactions whose COMMIT
	// was outside the (afterLSN, targetLSN] window.
	replayEntityVersionsInOrder(newState, txEntityCollectors, txCommitLSN)

	newState.headLSN = engine.headLSN
	newState.logicalTS = engine.logicalTS
	engine.readState.Store(newState)

	return nil
}

// replayEntityVersionsInOrder records entity versions for replayed transactions
// in commitLSN order. This ensures deterministic version numbering regardless
// of Go map iteration order.
func replayEntityVersionsInOrder(state *readableState, collectors map[string]map[string]map[string][]string, commitLSNs map[string]uint64) {
	if len(collectors) == 0 {
		return
	}

	type txEntry struct {
		txID      string
		commitLSN uint64
		collector map[string]map[string][]string
	}
	ordered := make([]txEntry, 0, len(collectors))
	for txID, collector := range collectors {
		if len(collector) > 0 {
			ordered = append(ordered, txEntry{txID: txID, commitLSN: commitLSNs[txID], collector: collector})
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].commitLSN < ordered[j].commitLSN
	})

	for _, entry := range ordered {
		recordEntityVersions(state, entry.collector, entry.commitLSN)
	}
}

// buildTxCommitLSNIndex pre-scans WAL records to map each transaction ID to its commit LSN.
func buildTxCommitLSNIndex(records []ports.WALRecord) map[string]uint64 {
	index := make(map[string]uint64)
	for _, r := range records {
		if r.Type == walTypeCommit {
			index[r.TxID] = r.LSN
		}
	}
	return index
}
