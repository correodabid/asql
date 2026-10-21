package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/correodabid/asql/internal/engine/domains"
	"github.com/correodabid/asql/internal/engine/planner"
	"github.com/correodabid/asql/internal/engine/ports"
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

	loadedCommitTimes, err := engine.commitTimes.load()
	if err != nil {
		slog.Warn("failed to load commit time index; rebuilding from wal", "error", err.Error())
		loadedCommitTimes = false
	}

	loadedTimestampIndex := false
	if engine.timestampIndex != nil {
		loaded, err := engine.timestampIndex.load()
		if err != nil {
			slog.Warn("failed to load timestamp index; rebuilding from wal", "error", err.Error())
		} else {
			loadedTimestampIndex = loaded
		}
	}

	if engine.historyWriter != nil {
		covered, err := engine.prepareHistory(ctx)
		if err != nil {
			return err
		}
		engine.historyFrom = covered
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

		dropLegacyProjectionTables(snapshots)
		if maxSeq > engine.snapSeq {
			engine.snapSeq = maxSeq
		}
		if engine.historyWriter != nil {
			// Replay regenerates missing row history only after the snapshot
			// it starts from.
			for len(snapshots) > 0 && snapshots[len(snapshots)-1].lsn > engine.historyFrom {
				slog.Info("replay: ignoring snapshot beyond row history coverage",
					"snapshot_lsn", snapshots[len(snapshots)-1].lsn, "history_lsn", engine.historyFrom)
				snapshots = snapshots[:len(snapshots)-1]
			}
		}
		if engine.replayLimited {
			for len(snapshots) > 0 && snapshots[len(snapshots)-1].lsn > engine.replayLimit {
				slog.Warn("replay: ignoring snapshot beyond replay limit",
					"snapshot_lsn", snapshots[len(snapshots)-1].lsn, "replay_limit", engine.replayLimit)
				snapshots = snapshots[:len(snapshots)-1]
			}
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
			engine.reserveWALTxIDs(deltaRecords)
			deltaRecords = engine.recordsWithinReplayLimit(deltaRecords)
			if len(deltaRecords) > 0 && deltaRecords[0].LSN != latest.lsn+1 {
				return fmt.Errorf("%w: snapshot lsn=%d next wal lsn=%d", errSnapshotWALMismatch, latest.lsn, deltaRecords[0].LSN)
			}
			if err := engine.replayFromSnapshots(snapshots, deltaRecords); err != nil {
				return err
			}
			if err := engine.ensureCommitTimeIndex(ctx, loadedCommitTimes, deltaRecords, false); err != nil {
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
	engine.reserveWALTxIDs(records)
	records = engine.recordsWithinReplayLimit(records)
	if err := engine.rebuildFromRecords(records, 0, false); err != nil {
		return err
	}
	if err := engine.ensureCommitTimeIndex(ctx, loadedCommitTimes, records, true); err != nil {
		return err
	}
	return engine.ensureTimestampIndex(ctx, loadedTimestampIndex, records, true)
}

// reserveWALTxIDs keeps newTxID from reusing the TxID of any transaction in
// records, including ones not applied yet: Raft may still commit them.
func (engine *Engine) reserveWALTxIDs(records []ports.WALRecord) {
	for _, record := range records {
		if record.Type == walTypeBegin {
			engine.reserveTxID(record.TxID)
		}
	}
}

// recordsWithinReplayLimit returns the prefix of LSN-ordered records allowed
// by the replay limit. A transaction the limit cuts through is deferred
// whole: Raft may still commit the rest of it.
//
// Without a limit the WAL is all there is, and a transaction left without
// its COMMIT never commits. Replay skips its mutations but keeps the head at
// the WAL's last LSN, which the commit path counts on to assign LSNs.
func (engine *Engine) recordsWithinReplayLimit(records []ports.WALRecord) []ports.WALRecord {
	if !engine.replayLimited {
		return records
	}
	limit := engine.replayLimit
	n := sort.Search(len(records), func(i int) bool { return records[i].LSN > limit })
	within := withoutUnfinishedTx(records[:n])
	if deferred := len(records) - len(within); deferred > 0 {
		slog.Info("replay: deferring WAL records beyond replay limit to raft",
			"replay_limit", limit, "deferred", deferred)
	}
	return within
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
	engine.setLastTxNum(latest.lastTxNum)
	engine.headLSN = latest.lsn
	engine.mutationCount = 0
	// Track the logical timestamp of the latest disk snapshot so that the first
	// post-restart delta checkpoint uses the correct base for buildChangedDomains.
	engine.lastDiskSnapshotLSN = latest.lsn
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

// ReplayToLSNAsPrincipal replays WAL mutations only when the authenticated
// durable principal holds the explicit admin privilege for replay-sensitive
// operations.
func (engine *Engine) ReplayToLSNAsPrincipal(ctx context.Context, targetLSN uint64, principal string) error {
	if err := engine.AuthorizePrincipalPrivilege(principal, PrincipalPrivilegeAdmin, "replay_to_lsn"); err != nil {
		return err
	}
	return engine.ReplayToLSN(ctx, targetLSN)
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

// LSNForCommitTime resolves a wall-clock time, in Unix µs, to the LSN of the
// last commit at or before it (ADR 0007): the state of the database at that
// time. It returns 0 when no commit is that old, and ErrCommitTimeUnknown
// when the commits before the time recorded none.
func (engine *Engine) LSNForCommitTime(micros int64) (uint64, error) {
	return engine.commitTimes.resolve(micros)
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
		catalog:   domains.NewCatalog(),
		scanStats: make(map[scanStrategy]uint64),
		logStore:  engine.logStore,
		snapshots: engine.snapshots,
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
	engine.lastTxNum = 0
	engine.headLSN = 0
	engine.mutationCount = 0
	if engine.snapshots != nil {
		engine.snapshots.clear()
	}
	captureReplaySnapshots := !bounded

	commitLSNs := txCommitLSNs(records)
	txEntityCollectors := make(map[string]map[string]map[string][]string)
	txClonedTables := make(map[string]map[string]struct{})
	snapshotDue := false
	history := engine.replayHistory(bounded)
	defer func() { engine.history = nil }()

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
		if record.Type == walTypeBegin {
			engine.noteTxID(record.TxID)
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
			if snapshotDue {
				newState.headLSN = engine.headLSN
				newState.logicalTS = engine.logicalTS
				newState.lastTxNum = engine.lastTxNum
				engine.snapshots.add(captureSnapshot(newState, engine.catalog))
				snapshotDue = false
			}
			continue
		}

		if record.Type == walTypeSecurity {
			payload, err := decodeSecurityMutationPayload(record.Payload)
			if err != nil {
				return fmt.Errorf("decode security payload lsn=%d: %w", record.LSN, err)
			}
			if err := applySecurityMutation(newState, payload); err != nil {
				return fmt.Errorf("apply security mutation lsn=%d: %w", record.LSN, err)
			}
			continue
		}

		if record.Type != walTypeMutation {
			continue
		}

		commitLSN := commitLSNs[i]
		if !committedWithin(commitLSN, targetLSN, bounded) {
			continue
		}

		plan, ok, err := replayPlanForRecord(record, replayPlans, i)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}

		if txEntityCollectors[record.TxID] == nil {
			txEntityCollectors[record.TxID] = make(map[string]map[string][]string)
		}
		if txClonedTables[record.TxID] == nil {
			txClonedTables[record.TxID] = make(map[string]struct{})
		}
		engine.historyFor(history, commitLSN)
		if err := engine.applyMutationPlanWithEntityTrackingTracked(newState, plan, commitLSN, txEntityCollectors[record.TxID], txClonedTables[record.TxID], nil); err != nil {
			return fmt.Errorf("apply replay mutation lsn=%d: %w", record.LSN, err)
		}

		engine.mutationCount++

		// Capture periodic snapshots for fast time-travel queries, at the
		// next COMMIT so that no snapshot holds a partial transaction.
		interval := uint64(adaptiveSnapshotInterval(engine.mutationCount))
		if captureReplaySnapshots && engine.snapshots != nil && interval > 0 && engine.mutationCount%interval == 0 {
			snapshotDue = true
		}
	}

	newState.headLSN = engine.headLSN
	newState.logicalTS = engine.logicalTS
	newState.lastTxNum = engine.lastTxNum
	engine.finishReplayHistory(history)

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

	commitLSNs := txCommitLSNs(records)
	txEntityCollectors := make(map[string]map[string]map[string][]string)
	txClonedTables := make(map[string]map[string]struct{})
	touchedTables := make(map[string]map[string]struct{})
	snapshotDue := false
	history := engine.replayHistory(false)
	defer func() { engine.history = nil }()

	for i, record := range records {
		if record.LSN <= afterLSN {
			continue
		}

		if record.LSN > engine.headLSN {
			engine.headLSN = record.LSN
		}
		if record.Timestamp > engine.logicalTS {
			engine.logicalTS = record.Timestamp
		}
		if record.Type == walTypeBegin {
			engine.noteTxID(record.TxID)
		}

		// Record entity versions eagerly on COMMIT (see rebuildFromRecords).
		if record.Type == walTypeCommit {
			if collector := txEntityCollectors[record.TxID]; len(collector) > 0 {
				recordEntityVersions(newState, collector, record.LSN)
				delete(txEntityCollectors, record.TxID)
			}
			delete(txClonedTables, record.TxID)
			if snapshotDue {
				newState.headLSN = engine.headLSN
				newState.logicalTS = engine.logicalTS
				newState.lastTxNum = engine.lastTxNum
				engine.snapshots.add(captureSnapshot(newState, engine.catalog))
				snapshotDue = false
			}
			continue
		}

		if record.Type == walTypeSecurity {
			payload, err := decodeSecurityMutationPayload(record.Payload)
			if err != nil {
				return fmt.Errorf("decode security payload lsn=%d: %w", record.LSN, err)
			}
			if err := applySecurityMutation(newState, payload); err != nil {
				return fmt.Errorf("apply security mutation lsn=%d: %w", record.LSN, err)
			}
			continue
		}

		if record.Type != walTypeMutation {
			continue
		}

		commitLSN := commitLSNs[i]
		if !committedWithin(commitLSN, 0, false) {
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

		if txEntityCollectors[record.TxID] == nil {
			txEntityCollectors[record.TxID] = make(map[string]map[string][]string)
		}
		if txClonedTables[record.TxID] == nil {
			txClonedTables[record.TxID] = make(map[string]struct{})
		}
		engine.historyFor(history, commitLSN)
		if err := engine.applyMutationPlanWithEntityTrackingTracked(newState, plan, commitLSN, txEntityCollectors[record.TxID], txClonedTables[record.TxID], nil); err != nil {
			return fmt.Errorf("apply replay mutation lsn=%d: %w", record.LSN, err)
		}

		engine.mutationCount++

		interval := uint64(adaptiveSnapshotInterval(engine.mutationCount))
		if captureSnapshots && engine.snapshots != nil && interval > 0 && engine.mutationCount%interval == 0 {
			snapshotDue = true
		}
	}

	newState.headLSN = engine.headLSN
	newState.logicalTS = engine.logicalTS
	newState.lastTxNum = engine.lastTxNum
	engine.finishReplayHistory(history)

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
	engine.reserveWALTxIDs(records)
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
	if records = withoutUnfinishedTx(records); len(records) == 0 {
		return nil
	}
	latest := engine.readState.Load()
	if latest != nil && len(records) > 0 && records[len(records)-1].LSN <= latest.headLSN {
		return nil
	}
	if err := engine.rebuildFromRecordsAfterSnapshot(records, current.headLSN); err != nil {
		return err
	}
	engine.indexAppliedRecords(records)
	return nil
}

// withoutUnfinishedTx drops a transaction that the records end inside of:
// its BEGIN is present but its COMMIT is not yet. Raft can commit an index
// in the middle of a transaction's records, and a crash can cut a batch
// short; applying them would expose a partial transaction. The head stays
// before the transaction, so the next catch-up rereads it from the WAL once
// its COMMIT arrives. A transaction's records are contiguous (one batch per
// commit), so only a tail that holds nothing but that transaction's records
// can still commit; anything appended after it means it never will.
func withoutUnfinishedTx(records []ports.WALRecord) []ports.WALRecord {
	if len(records) == 0 {
		return records
	}
	txID := records[len(records)-1].TxID
	for i := len(records) - 1; i >= 0; i-- {
		switch {
		case records[i].TxID != txID || records[i].Type == walTypeCommit || records[i].Type == walTypeSecurity:
			return records
		case records[i].Type == walTypeBegin:
			return records[:i]
		}
	}
	// The records start inside the transaction (the head is past its BEGIN)
	// and do not reach its COMMIT.
	return records[:0]
}

// indexAppliedRecords adds records applied outside the commit queue (Raft
// followers) to the timestamp index, as the commit queue does for local
// commits.
func (engine *Engine) indexAppliedRecords(records []ports.WALRecord) {
	if err := engine.timestampIndex.append(records); err != nil {
		slog.Warn("timestamp index append failed", "error", err.Error(), "records", len(records))
	}
	engine.indexCommitTimes(records)
}

// indexCommitTimes adds the commit times of records to the index and keeps
// the newest one, which the next local commit never goes below.
func (engine *Engine) indexCommitTimes(records []ports.WALRecord) {
	if err := engine.commitTimes.append(records); err != nil {
		slog.Warn("commit time index append failed", "error", err.Error(), "records", len(records))
	}
	engine.noteCommitTime(engine.commitTimes.last())
}

func (engine *Engine) noteCommitTime(micros int64) {
	for {
		current := engine.lastCommitMicros.Load()
		if micros <= current || engine.lastCommitMicros.CompareAndSwap(current, micros) {
			return
		}
	}
}

// ensureCommitTimeIndex brings the commit time index up to the head at
// startup, like ensureTimestampIndex: records are the replayed WAL, the
// whole retained WAL when full.
func (engine *Engine) ensureCommitTimeIndex(ctx context.Context, loaded bool, records []ports.WALRecord, full bool) error {
	defer func() { engine.noteCommitTime(engine.commitTimes.last()) }()
	switch {
	case full:
		if err := engine.commitTimes.rebuild(records); err != nil {
			slog.Warn("failed to persist rebuilt commit time index", "error", err.Error())
		}
		return nil
	case loaded:
		scanned := engine.commitTimes.scannedLSN()
		if scanned >= engine.headLSN {
			return nil
		}
		missing, err := engine.logStore.ReadFrom(ctx, scanned+1, 0)
		if err != nil {
			return fmt.Errorf("read wal for commit time index catch-up: %w", err)
		}
		if err := engine.commitTimes.append(missing); err != nil {
			slog.Warn("failed to persist commit time index catch-up", "error", err.Error())
		}
		return nil
	}
	all, err := engine.readAllRecords(ctx)
	if err != nil {
		return fmt.Errorf("read wal for commit time index rebuild: %w", err)
	}
	if err := engine.commitTimes.rebuild(all); err != nil {
		slog.Warn("failed to persist rebuilt commit time index", "error", err.Error())
	}
	return nil
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
	engine.reserveWALTxIDs(records)

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
	if filtered = withoutUnfinishedTx(filtered); len(filtered) == 0 {
		return nil
	}

	if err := engine.rebuildFromRecordsAfterSnapshotWithOptions(filtered, current.headLSN, false); err != nil {
		return err
	}
	engine.indexAppliedRecords(filtered)
	return nil
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

	commitLSNs := txCommitLSNs(records)
	txEntityCollectors := make(map[string]map[string]map[string][]string)
	txClonedTables := make(map[string]map[string]struct{})

	for i, record := range records {
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
		if record.Type == walTypeBegin {
			engine.noteTxID(record.TxID)
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

		if record.Type == walTypeSecurity {
			payload, err := decodeSecurityMutationPayload(record.Payload)
			if err != nil {
				return fmt.Errorf("decode security payload lsn=%d: %w", record.LSN, err)
			}
			if err := applySecurityMutation(newState, payload); err != nil {
				return fmt.Errorf("apply security mutation lsn=%d: %w", record.LSN, err)
			}
			continue
		}

		if record.Type != walTypeMutation {
			continue
		}

		commitLSN := commitLSNs[i]
		if !committedWithin(commitLSN, targetLSN, true) {
			continue
		}

		_, plan, err := decodeMutationPayloadV2(record.Payload)
		if err != nil {
			return fmt.Errorf("decode mutation payload lsn=%d: %w", record.LSN, err)
		}

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

	newState.headLSN = engine.headLSN
	newState.logicalTS = engine.logicalTS
	newState.lastTxNum = engine.lastTxNum
	engine.readState.Store(newState)

	return nil
}

// committedWithin reports whether a replayed mutation's transaction commits
// at or before targetLSN (any LSN when not bounded). commitLSN is 0 when the
// COMMIT is not in the WAL: the transaction never committed (its batch was
// cut short by a crash, or a new Raft leader replaced the rest) and none of
// its mutations may be applied.
func committedWithin(commitLSN, targetLSN uint64, bounded bool) bool {
	return commitLSN != 0 && (!bounded || commitLSN <= targetLSN)
}

// txCommitLSNs maps each record to the LSN of the COMMIT that closes its
// transaction, or 0 when that COMMIT is not among records.
//
// TxIDs are unique in WALs written since newTxID resumes after the WAL's
// highest one, but older WALs repeat them: the counter restarted with every
// process, and each Raft leader had its own. A transaction's records
// follow its BEGIN and precede its COMMIT, so a record belongs to the next
// COMMIT with its TxID. A BEGIN starts a new transaction: records left open
// under the same TxID never committed (a crashed process) and resolve to 0.
func txCommitLSNs(records []ports.WALRecord) []uint64 {
	commits := make([]uint64, len(records))
	open := make(map[string][]int)
	for i, r := range records {
		switch r.Type {
		case walTypeBegin:
			open[r.TxID] = append(open[r.TxID][:0], i)
		case walTypeCommit:
			for _, j := range open[r.TxID] {
				commits[j] = r.LSN
			}
			commits[i] = r.LSN
			delete(open, r.TxID)
		default:
			open[r.TxID] = append(open[r.TxID], i)
		}
	}
	return commits
}

// RollbackTo discards engine state beyond lsn and rebuilds it from the
// persisted snapshots and WAL records up to lsn. Raft calls it on a follower
// whose engine applied entries that a new leader then replaced: the WAL has
// already been truncated after lsn. It must not run concurrently with
// commits, which on a follower go through the leader.
func (engine *Engine) RollbackTo(ctx context.Context, lsn uint64) error {
	if engine.HeadLSN() <= lsn {
		return nil
	}
	slog.Warn("engine: rolling back state", "from_lsn", engine.HeadLSN(), "to_lsn", lsn)
	engine.clearWALRecordCache()
	engine.clearHistoricalStateCache()
	if engine.snapshots != nil {
		engine.snapshots.mu.Lock()
		engine.snapshots.clear()
		engine.snapshots.mu.Unlock()
	}
	// Derived state persisted beyond lsn describes entries that no longer
	// exist; a later restart would otherwise load it.
	if engine.snapDir != "" {
		if err := removeSnapshotFilesAfter(engine.snapDir, lsn); err != nil {
			return fmt.Errorf("rollback snapshots: %w", err)
		}
	}
	if err := engine.timestampIndex.truncateAfter(lsn); err != nil {
		return fmt.Errorf("rollback timestamp index: %w", err)
	}
	if err := engine.commitTimes.truncateAfter(lsn); err != nil {
		return fmt.Errorf("rollback commit time index: %w", err)
	}
	if engine.historyWriter != nil {
		if err := engine.drainHistory(ctx); err != nil {
			return fmt.Errorf("rollback row history: %w", err)
		}
		if err := engine.auditStore.TruncateAfter(ctx, lsn); err != nil {
			return fmt.Errorf("rollback row history: %w", err)
		}
	}

	prevLimit, prevLimited := engine.replayLimit, engine.replayLimited
	engine.replayLimit, engine.replayLimited = lsn, true
	defer func() { engine.replayLimit, engine.replayLimited = prevLimit, prevLimited }()

	engine.writeMu.Lock()
	engine.catalog = domains.NewCatalog()
	engine.headLSN = 0
	engine.logicalTS = 0
	engine.lastTxNum = 0
	// Replay re-derives these from the snapshots that survived; with none,
	// the next checkpoint must not be a delta against a removed one.
	engine.lastDiskSnapshotLSN = 0
	engine.lastDiskSnapshotLogicalTS = 0
	engine.readState.Store(&readableState{domains: make(map[string]*domainState)})
	engine.writeMu.Unlock()
	return engine.Replay(ctx)
}
