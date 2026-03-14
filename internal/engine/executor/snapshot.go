package executor

import (
	"sort"
	"sync"
	"time"

	"asql/internal/engine/domains"
)

// snapshotInterval controls how many WAL records are processed between
// automatic snapshots during startup replay. Smaller values trade more
// memory for faster time-travel queries.
const defaultSnapshotInterval = 500

const snapshotIdleDelay = 2 * time.Second

const (
	recentMutationWindowSize               = 256
	recentMutationMinSampleSize            = 64
	persistedCheckpointPressureThreshold   = 250 // average weighted pressure >= 2.5x insert baseline shrinks interval
	minPersistedCheckpointMutationInterval = 250
)

type mutationPressureKind uint8

const (
	mutationPressureInsert mutationPressureKind = 1
	mutationPressureUpdate mutationPressureKind = 4
	mutationPressureDelete mutationPressureKind = 3
)

const (
	snapshotIntervalMediumThreshold = 5_000
	snapshotIntervalHighThreshold   = 20_000
	snapshotIntervalXLThreshold     = 100_000

	snapshotIntervalMedium = 10_000
	snapshotIntervalHigh   = 50_000
	snapshotIntervalXL     = 200_000
)

const (
	diskCheckpointWALBytesMediumThreshold = 5_000
	diskCheckpointWALBytesHighThreshold   = 20_000
	diskCheckpointWALBytesXLThreshold     = 100_000

	diskCheckpointWALBytesMedium = 256 * 1024 * 1024
	diskCheckpointWALBytesHigh   = 512 * 1024 * 1024
	diskCheckpointWALBytesXL     = 1024 * 1024 * 1024
)

// fullSnapshotFrequency controls how often a full (non-delta) snapshot is
// written within the persisted snapshot file. Every Nth snapshot is a full
// baseline; the rest are deltas containing only changed tables.
const fullSnapshotFrequency = 10

// maxDiskSnapshots is the maximum number of checkpoint files kept in the snap
// directory. Multiple checkpoints serve as WAL anchors for time-travel queries:
// when replaying to a past LSN the engine loads the closest snapshot before
// that LSN and only replays the delta, keeping O(delta) replay cost even for
// large WAL volumes. The WAL is never truncated (retainWAL=true by default).
const maxDiskSnapshots = 20

// diskCheckpointWALBytes is the WAL growth (in bytes) between async disk
// checkpoint writes during runtime. Using WAL size rather than mutation count
// makes the trigger independent of row size and workload mix.
// 64 MiB balances checkpoint frequency against serialization I/O overhead:
// at typical row sizes this corresponds to tens of thousands of mutations,
// and each checkpoint enables the engine to skip that WAL segment on restart.
const diskCheckpointWALBytes = 64 * 1024 * 1024

// Mutation-count thresholds for adaptive in-memory snapshot retention.
const (
	memorySnapshotMediumThreshold = 100_000
	memorySnapshotHighThreshold   = 300_000
)

// maxMemorySnapshotsBase caps how many snapshots are kept in-memory under
// normal write volume.
const maxMemorySnapshotsBase = 20

// maxMemorySnapshotsMediumVolume is used after medium mutation volume.
const maxMemorySnapshotsMediumVolume = 8

// maxMemorySnapshotsHighVolume is used after high mutation volume.
// Kept low (2) to reduce GC pressure — disk checkpoint enables time-travel
// fallback for older LSNs.
const maxMemorySnapshotsHighVolume = 2

// adaptiveMaxMemorySnapshots chooses in-memory snapshot retention based on
// cumulative mutation volume to keep GC pressure bounded under sustained ingest.
func adaptiveMaxMemorySnapshots(mutationCount uint64) int {
	if mutationCount >= memorySnapshotHighThreshold {
		return maxMemorySnapshotsHighVolume
	}
	if mutationCount >= memorySnapshotMediumThreshold {
		return maxMemorySnapshotsMediumVolume
	}
	return maxMemorySnapshotsBase
}

// adaptiveSnapshotInterval increases runtime snapshot spacing as cumulative
// mutation volume grows. Full-state snapshot capture is O(database size), so
// taking them every fixed 500 mutations makes sustained ingest slow down as the
// dataset grows. Wider spacing preserves time-travel anchors while keeping the
// write path closer to O(new data) instead of O(total data).
func adaptiveSnapshotInterval(mutationCount uint64) int {
	if mutationCount >= snapshotIntervalXLThreshold {
		return snapshotIntervalXL
	}
	if mutationCount >= snapshotIntervalHighThreshold {
		return snapshotIntervalHigh
	}
	if mutationCount >= snapshotIntervalMediumThreshold {
		return snapshotIntervalMedium
	}
	return defaultSnapshotInterval
}

// adaptivePersistedCheckpointMutationInterval chooses how many mutations to
// allow between persisted checkpoints. It keeps the same volume-based baseline
// as adaptiveSnapshotInterval, but shrinks the interval when the recent
// mutation mix is dominated by UPDATE/DELETE pressure.
func adaptivePersistedCheckpointMutationInterval(mutationCount uint64, recentPressure int, recentSamples int) int {
	base := adaptiveSnapshotInterval(mutationCount)
	if recentSamples < recentMutationMinSampleSize {
		return base
	}
	avgPressureTimes100 := (recentPressure * 100) / recentSamples
	if avgPressureTimes100 < persistedCheckpointPressureThreshold {
		return base
	}
	adjusted := base / 2
	if adjusted < minPersistedCheckpointMutationInterval {
		return minPersistedCheckpointMutationInterval
	}
	return adjusted
}

// adaptiveDiskCheckpointWALBytes increases disk-checkpoint spacing as total
// mutation volume grows. Persisted checkpoints deep-copy and serialize large
// states, so keeping the trigger fixed at 64 MiB makes sustained ingest spend
// increasing background IO/CPU on checkpoints as the database gets larger.
func adaptiveDiskCheckpointWALBytes(mutationCount uint64) uint64 {
	if mutationCount >= diskCheckpointWALBytesXLThreshold {
		return diskCheckpointWALBytesXL
	}
	if mutationCount >= diskCheckpointWALBytesHighThreshold {
		return diskCheckpointWALBytesHigh
	}
	if mutationCount >= diskCheckpointWALBytesMediumThreshold {
		return diskCheckpointWALBytesMedium
	}
	return diskCheckpointWALBytes
}

func (engine *Engine) recordMutationPressure(kind mutationPressureKind) {
	weight := uint8(kind)
	if weight == 0 {
		weight = uint8(mutationPressureInsert)
	}

	if engine.recentMutationCount < recentMutationWindowSize {
		engine.recentMutationWeights[engine.recentMutationIndex] = weight
		engine.recentMutationPressure += int(weight)
		engine.recentMutationCount++
		engine.recentMutationIndex = (engine.recentMutationIndex + 1) % recentMutationWindowSize
		return
	}

	old := engine.recentMutationWeights[engine.recentMutationIndex]
	engine.recentMutationPressure -= int(old)
	engine.recentMutationWeights[engine.recentMutationIndex] = weight
	engine.recentMutationPressure += int(weight)
	engine.recentMutationIndex = (engine.recentMutationIndex + 1) % recentMutationWindowSize
}

// engineSnapshot captures a deep copy of the engine's in-memory state at a
// specific LSN. Snapshots are created during WAL replay so that time-travel
// queries can restore from the nearest snapshot instead of replaying the
// entire WAL from scratch.
type engineSnapshot struct {
	lsn       uint64
	logicalTS uint64
	catalog   *domains.Catalog
	state     engineState
}

// snapshotStore keeps an ordered list of snapshots indexed by LSN.
// It has its own mutex to allow async snapshot persistence.
type snapshotStore struct {
	mu        sync.Mutex
	snapshots []engineSnapshot
}

// newSnapshotStore creates an empty snapshot store.
func newSnapshotStore() *snapshotStore {
	return &snapshotStore{
		snapshots: make([]engineSnapshot, 0),
	}
}

// add appends a snapshot. Snapshots must be added in LSN order.
func (store *snapshotStore) add(snap engineSnapshot) {
	store.snapshots = append(store.snapshots, snap)
}

// evictOldest trims the snapshot store to keep at most maxKeep entries,
// removing the oldest (lowest-LSN) snapshots first.
func (store *snapshotStore) evictOldest(maxKeep int) {
	if len(store.snapshots) <= maxKeep {
		return
	}
	drop := len(store.snapshots) - maxKeep
	// Zero out evicted slots to let GC reclaim their state.
	for i := 0; i < drop; i++ {
		store.snapshots[i] = engineSnapshot{}
	}
	store.snapshots = store.snapshots[drop:]
}

// closest returns the snapshot with the highest LSN that is <= targetLSN.
// Returns nil if no suitable snapshot exists.
// Caller must hold store.mu if concurrent access is possible.
func (store *snapshotStore) closest(targetLSN uint64) *engineSnapshot {
	if len(store.snapshots) == 0 {
		return nil
	}

	// Binary search for the rightmost snapshot with LSN <= targetLSN.
	index := sort.Search(len(store.snapshots), func(i int) bool {
		return store.snapshots[i].lsn > targetLSN
	})

	if index == 0 {
		return nil
	}

	snap := store.snapshots[index-1]
	return &snap
}

// count returns the number of stored snapshots.
func (store *snapshotStore) count() int {
	return len(store.snapshots)
}

// clear removes all snapshots to free memory.
func (store *snapshotStore) clear() {
	store.snapshots = nil
}

// oldestAvailableSnapshot returns the snapshot with the lowest LSN currently
// held in memory or on disk. Used as a fallback when closestSnapshot returns nil
// (i.e., WAL has been compacted and no snapshot ≤ targetLSN exists). Providing
// *any* schema base prevents spurious "table not found" errors for time-travel
// queries to LSNs earlier than the oldest checkpoint; the result is approximate
// (state at snapshot LSN) rather than the exact state at targetLSN.
func (engine *Engine) oldestAvailableSnapshot() *engineSnapshot {
	// Check in-memory store first.
	if engine.snapshots != nil {
		engine.snapshots.mu.Lock()
		snaps := engine.snapshots.snapshots
		engine.snapshots.mu.Unlock()
		if len(snaps) > 0 {
			s := snaps[0]
			return &s
		}
	}

	// Fall back to disk.
	if engine.snapDir == "" {
		return nil
	}
	diskSnaps, _, err := readAllSnapshotsFromDir(engine.snapDir)
	if err != nil || len(diskSnaps) == 0 {
		return nil
	}
	s := diskSnaps[0] // sorted by LSN ascending
	return &s
}

// closestSnapshot finds the closest snapshot with LSN <= targetLSN.
// It checks the in-memory store first. If no match is found, it falls back
// to loading the closest snapshot from disk.
func (engine *Engine) closestSnapshot(targetLSN uint64) *engineSnapshot {
	if engine.snapshots == nil {
		return nil
	}

	// Fast path: check in-memory store.
	engine.snapshots.mu.Lock()
	snap := engine.snapshots.closest(targetLSN)
	engine.snapshots.mu.Unlock()
	if snap != nil {
		return snap
	}

	// Slow path: load from disk.
	if engine.snapDir == "" {
		return nil
	}

	diskSnaps, _, err := readAllSnapshotsFromDir(engine.snapDir)
	if err != nil || len(diskSnaps) == 0 {
		return nil
	}

	// Find the closest disk snapshot with LSN <= targetLSN.
	for i := len(diskSnaps) - 1; i >= 0; i-- {
		if diskSnaps[i].lsn <= targetLSN {
			s := diskSnaps[i]
			return &s
		}
	}

	return nil
}

// captureSnapshotWithCatalog creates a deep copy of the given readableState for
// snapshotting using a caller-provided catalog clone.
// changeLog is stripped from cloned tables — it is not needed for time-travel
// reads (which use rows) and dropping it avoids pinning the live changeLog
// backing array in long-lived snapshot references.
func captureSnapshotWithCatalog(state *readableState, catalog *domains.Catalog) engineSnapshot {
	domains := cloneDomains(state.domains)
	// Strip changeLog from snapshot tables to avoid memory retention.
	for _, ds := range domains {
		for _, tbl := range ds.tables {
			tbl.changeLog = nil
		}
	}
	return engineSnapshot{
		lsn:       state.headLSN,
		logicalTS: state.logicalTS,
		catalog:   cloneCatalog(catalog),
		state:     engineState{domains: domains},
	}
}

// captureSnapshot clones the catalog and then captures a deep-copy snapshot.
func captureSnapshot(state *readableState, catalog *domains.Catalog) engineSnapshot {
	return captureSnapshotWithCatalog(state, cloneCatalog(catalog))
}

func readableStateFromSnapshotShared(snap *engineSnapshot) *readableState {
	if snap == nil {
		return &readableState{domains: make(map[string]*domainState)}
	}
	return &readableState{
		domains:   snap.state.domains,
		logicalTS: snap.logicalTS,
		headLSN:   snap.lsn,
	}
}

// restoreSnapshot replaces the engine's in-memory state with the snapshot's
// deep-copied data.
func (engine *Engine) restoreSnapshot(snap *engineSnapshot) {
	engine.catalog = cloneCatalog(snap.catalog)
	newState := readableStateFromSnapshotShared(snap)
	engine.readState.Store(newState)
	engine.logicalTS = snap.logicalTS
	engine.headLSN = snap.lsn
}

// ---------- deep-copy helpers ----------

func cloneCatalog(src *domains.Catalog) *domains.Catalog {
	clone := domains.NewCatalog()
	for domain, tables := range src.Domains() {
		clone.EnsureDomain(domain)
		for table := range tables {
			clone.RegisterTable(domain, table)
		}
	}
	return clone
}
