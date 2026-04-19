package executor

import (
	"sort"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
)

type Result struct {
	Status    string
	TxID      string
	Rows      []map[string]ast.Literal
	CommitLSN uint64
}

type Session struct {
	activeTx  *transaction
	principal string
}

// SetPrincipal binds the authenticated database principal to the session.
func (session *Session) SetPrincipal(name string) {
	if session == nil {
		return
	}
	session.principal = normalizePrincipalName(name)
}

// Principal returns the authenticated database principal for the session.
func (session *Session) Principal() string {
	if session == nil {
		return ""
	}
	return session.principal
}

// InTransaction reports whether the session currently has an active transaction.
func (session *Session) InTransaction() bool {
	return session != nil && session.activeTx != nil
}

// ActiveDomains returns a copy of active transaction domains in canonical order.
func (session *Session) ActiveDomains() []string {
	if session == nil || session.activeTx == nil {
		return nil
	}

	domains := make([]string, len(session.activeTx.domains))
	copy(domains, session.activeTx.domains)
	return domains
}

type transaction struct {
	id             string
	domains        []string
	startLogicalTS uint64
	statements     []string
	plans          []planner.Plan // cached plans from Execute-time validation
	savepoints     []savepointMarker
}

type savepointMarker struct {
	name           string
	statementCount int
}

type engineState struct {
	domains    map[string]*domainState
	principals map[string]*principalState
}

// readableState is the immutable state snapshot visible to lock-free readers.
// Writers create a COW clone, apply mutations, then atomically swap the pointer.
type readableState struct {
	domains    map[string]*domainState
	principals map[string]*principalState
	headLSN    uint64
	logicalTS  uint64
}

// cloneForMutation creates a shallow copy suitable for mutation.
// Only the domains listed in affectedDomains are shallow-cloned;
// all other domain pointers are shared with the original.
func (s *readableState) cloneForMutation(affectedDomains map[string]struct{}) *readableState {
	newDomains := make(map[string]*domainState, len(s.domains))
	for k, v := range s.domains {
		if _, affected := affectedDomains[k]; affected {
			newDomains[k] = v.shallowClone()
		} else {
			newDomains[k] = v
		}
	}
	return &readableState{
		domains:    newDomains,
		principals: clonePrincipals(s.principals),
		headLSN:    s.headLSN,
		logicalTS:  s.logicalTS,
	}
}

// shallowClone creates a copy of domainState with a new tables map
// that shares all tableState pointers with the original.
func (d *domainState) shallowClone() *domainState {
	newTables := make(map[string]*tableState, len(d.tables))
	for k, v := range d.tables {
		newTables[k] = v
	}
	newEntities := d.entities
	newEntityVersions := d.entityVersions
	return &domainState{
		tables:         newTables,
		entities:       newEntities,
		entityVersions: newEntityVersions,
		entityTables:   d.entityTables, // share read-only set
	}
}

// toEngineState converts readableState to the legacy engineState type
// used by snapshots (deep copy of domains).
func (s *readableState) toEngineState() engineState {
	return engineState{domains: cloneDomains(s.domains), principals: clonePrincipals(s.principals)}
}

func clonePrincipals(src map[string]*principalState) map[string]*principalState {
	if src == nil {
		return nil
	}
	cloned := make(map[string]*principalState, len(src))
	for name, principal := range src {
		if principal == nil {
			continue
		}
		roles := make(map[string]struct{}, len(principal.roles))
		for role := range principal.roles {
			roles[role] = struct{}{}
		}
		privileges := make(map[PrincipalPrivilege]struct{}, len(principal.privileges))
		for privilege := range principal.privileges {
			privileges[privilege] = struct{}{}
		}
		cloned[name] = &principalState{
			name:         principal.name,
			kind:         principal.kind,
			passwordHash: principal.passwordHash,
			enabled:      principal.enabled,
			roles:        roles,
			privileges:   privileges,
		}
	}
	return cloned
}

func cloneDomains(src map[string]*domainState) map[string]*domainState {
	cloned := make(map[string]*domainState, len(src))
	for domainName, domain := range src {
		copiedDomain := &domainState{
			tables:         make(map[string]*tableState, len(domain.tables)),
			entities:       cloneEntities(domain.entities),
			entityVersions: cloneEntityVersions(domain.entityVersions),
		}
		for tableName, table := range domain.tables {
			copiedDomain.tables[tableName] = cloneTableState(table)
		}
		cloned[domainName] = copiedDomain
	}
	return cloned
}

func cloneEntities(src map[string]*entityDefinition) map[string]*entityDefinition {
	if src == nil {
		return nil
	}
	cloned := make(map[string]*entityDefinition, len(src))
	for k, v := range src {
		tables := make([]string, len(v.tables))
		copy(tables, v.tables)
		fkPaths := make(map[string][]fkHop, len(v.fkPaths))
		for t, hops := range v.fkPaths {
			copied := make([]fkHop, len(hops))
			copy(copied, hops)
			fkPaths[t] = copied
		}
		tableSet := make(map[string]struct{}, len(tables))
		for _, t := range tables {
			tableSet[t] = struct{}{}
		}
		cloned[k] = &entityDefinition{
			name:      v.name,
			rootTable: v.rootTable,
			tables:    tables,
			tableSet:  tableSet,
			fkPaths:   fkPaths,
		}
	}
	return cloned
}

func cloneEntityVersions(src map[string]*entityVersionIndex) map[string]*entityVersionIndex {
	if src == nil {
		return nil
	}
	cloned := make(map[string]*entityVersionIndex, len(src))
	for k, v := range src {
		// Flatten overlay chain before deep clone.
		flat := flattenEntityVersionIndex(v)
		versions := make(map[string][]entityVersion, len(flat.versions))
		for pk, vers := range flat.versions {
			copied := make([]entityVersion, len(vers))
			for i, ev := range vers {
				tables := make([]string, len(ev.tables))
				copy(tables, ev.tables)
				copied[i] = entityVersion{version: ev.version, commitLSN: ev.commitLSN, tables: tables}
			}
			versions[pk] = copied
		}
		cloned[k] = &entityVersionIndex{versions: versions}
	}
	return cloned
}

type domainState struct {
	tables         map[string]*tableState
	entities       map[string]*entityDefinition
	entityVersions map[string]*entityVersionIndex
	entityTables   map[string]struct{} // all table names that participate in any entity (fast skip)
}

type tableState struct {
	columns           []string
	columnDefinitions map[string]ast.ColumnDefinition
	// columnIndex maps each column name to its ordinal position in the columns
	// slice. Provides O(1) column-name→position lookup and is the backing
	// schema for the [][]ast.Literal row storage format.
	// Always kept in sync with columns via rebuildColumnIndex.
	columnIndex map[string]int
	// rows stores each row as a positional value slice aligned to columns.
	// Index i in a row corresponds to columns[i]. This eliminates the
	// per-row map overhead (~240 B/row) and reduces stored-data memory by
	// ~40-50% for typical 10-column tables compared to map[string]ast.Literal.
	rows                 [][]ast.Literal
	indexes              map[string]*indexState
	indexedColumns       map[string]string
	indexedColumnSets    map[string]string
	primaryKey           string
	uniqueColumns        map[string]struct{}
	uniqueColumnList     []string // pre-computed: unique columns excluding PK (avoids map iteration)
	notNullColumns       []string // pre-computed: NOT NULL columns excluding PK (avoids map iteration)
	pkAutoUUID           bool     // true when PK column default is UUIDv7 — skip uniqueness check for auto-gen
	foreignKeys          []foreignKeyConstraint
	checkConstraints     []checkConstraint
	versionedForeignKeys []versionedForeignKeyConstraint
	isProjection         bool // true for VFK projection shadow tables (hidden from schema introspection)
	lastMutationTS       uint64
	changeLog            []changeLogEntry
	// indexesLoaded is true when all index bucket/entry data was restored from
	// a v11+ snapshot, meaning rebuildTableIndexes can be skipped on startup.
	indexesLoaded bool
}

// rebuildColumnIndex rebuilds the columnIndex map from the columns slice.
// Must be called after any change to ts.columns (ADD COLUMN, DROP COLUMN, CREATE TABLE).
func rebuildColumnIndex(ts *tableState) {
	idx := make(map[string]int, len(ts.columns))
	for i, col := range ts.columns {
		idx[col] = i
	}
	ts.columnIndex = idx
}

// rebuildNotNullColumns rebuilds the notNullColumns slice from columnDefinitions.
// Must be called after any change to columnDefinitions or primaryKey.
func rebuildNotNullColumns(ts *tableState) {
	var cols []string
	for colName, colDef := range ts.columnDefinitions {
		if colDef.NotNull && colName != ts.primaryKey {
			cols = append(cols, colName)
		}
	}
	ts.notNullColumns = cols
}

// rebuildUniqueColumnList rebuilds the uniqueColumnList slice from uniqueColumns.
// Must be called after any change to uniqueColumns or primaryKey.
func rebuildUniqueColumnList(ts *tableState) {
	var cols []string
	for col := range ts.uniqueColumns {
		if col != ts.primaryKey {
			cols = append(cols, col)
		}
	}
	ts.uniqueColumnList = cols
}

// rebuildPKAutoUUID sets pkAutoUUID based on PK column default.
func rebuildPKAutoUUID(ts *tableState) {
	ts.pkAutoUUID = false
	if ts.primaryKey == "" {
		return
	}
	if def, ok := ts.columnDefinitions[ts.primaryKey]; ok && def.DefaultValue != nil {
		ts.pkAutoUUID = def.DefaultValue.Kind == ast.DefaultUUIDv7
	}
}

type changeLogEntry struct {
	commitLSN uint64
	operation string                 // "INSERT", "UPDATE", "DELETE"
	oldRow    map[string]ast.Literal // nil for INSERT
	newRow    map[string]ast.Literal // nil for DELETE
}

// maxChangeLogPerTable caps the in-memory changeLog per table.
// When exceeded, the oldest entries are trimmed. RowHistory for
// entries older than the cap requires WAL-based replay.
// Keeping this bounded prevents unbounded memory growth across
// long-running sessions with heavy ingest workloads.
const maxChangeLogPerTable = 5_000

// changeLogTrimBatch amortizes trimming work by dropping more than one entry
// once the changeLog crosses the cap. Without a low-water mark, sustained
// INSERT workloads that sit just above the cap end up paying O(cap) slice
// churn on nearly every append.
const changeLogTrimBatch = 512

// trimChangeLog removes old entries when changeLog exceeds the cap.
// Only called on mutable (COW-cloned) tableState, so it is safe.
func trimChangeLog(table *tableState) {
	if len(table.changeLog) <= maxChangeLogPerTable {
		return
	}
	targetLen := maxChangeLogPerTable
	if maxChangeLogPerTable > changeLogTrimBatch {
		targetLen = maxChangeLogPerTable - changeLogTrimBatch
	}
	drop := len(table.changeLog) - targetLen
	// Zero evicted slots so GC can reclaim old/new row maps.
	for i := 0; i < drop; i++ {
		table.changeLog[i] = changeLogEntry{}
	}
	table.changeLog = table.changeLog[drop:]

	// Compact the backing array so long-running ingest workloads do not keep
	// large pointer-heavy changeLog arrays alive indefinitely. Recent history
	// beyond the in-memory cap already falls back to WAL replay, so preserving
	// extra capacity only increases GC scan work as the database grows.
	if cap(table.changeLog) > maxChangeLogPerTable+changeLogTrimBatch {
		compacted := make([]changeLogEntry, len(table.changeLog), maxChangeLogPerTable)
		copy(compacted, table.changeLog)
		table.changeLog = compacted
	}
}

type foreignKeyConstraint struct {
	column           string
	referencesTable  string
	referencesColumn string
}

// projectionSubscription records a subscriber domain table that mirrors a
// source domain's table via a Versioned Foreign Key projection.
type projectionSubscription struct {
	subscriberDomain string // domain that holds the projected table
	projTableName    string // name of the projected (read-only mirror) table
}

type versionedForeignKeyConstraint struct {
	column           string
	lsnColumn        string
	referencesDomain string
	referencesTable  string
	referencesColumn string
}

type checkConstraint struct {
	column    string
	predicate *ast.Predicate
}

type entityDefinition struct {
	name      string
	rootTable string
	tables    []string
	tableSet  map[string]struct{} // O(1) membership check
	fkPaths   map[string][]fkHop  // child table -> FK chain back to root
}

type fkHop struct {
	fromTable  string
	fromColumn string
	toTable    string
	toColumn   string
}

type entityVersionIndex struct {
	versions map[string][]entityVersion // rootPKValue (string key) -> ordered versions
	parent   *entityVersionIndex        // overlay parent for O(affected) COW instead of O(all)
	depth    int                        // cached overlay depth
}

// maxEntityVersionOverlayDepth bounds the entity version overlay chain.
// When exceeded, the chain is flattened into a single map.
const maxEntityVersionOverlayDepth = 64

// getVersions returns the version history for a root PK, walking the
// overlay chain. Returns nil if not found.
func (idx *entityVersionIndex) getVersions(rootPK string) []entityVersion {
	for p := idx; p != nil; p = p.parent {
		if vers, ok := p.versions[rootPK]; ok {
			return vers
		}
	}
	return nil
}

// flattenEntityVersionIndex merges all overlay levels into a single flat
// entityVersionIndex. The newest (top-most) entry for each rootPK wins.
func flattenEntityVersionIndex(idx *entityVersionIndex) *entityVersionIndex {
	if idx == nil || idx.parent == nil {
		return idx
	}
	// Collect levels, then iterate bottom-up so that newer levels overwrite older.
	levels := make([]*entityVersionIndex, 0, idx.depth+1)
	for p := idx; p != nil; p = p.parent {
		levels = append(levels, p)
	}
	// Estimate capacity from deepest (oldest, typically largest) level.
	base := levels[len(levels)-1]
	merged := make(map[string][]entityVersion, len(base.versions))
	// Bottom-up: newer levels overwrite older entries for the same PK.
	for i := len(levels) - 1; i >= 0; i-- {
		for pk, vers := range levels[i].versions {
			merged[pk] = vers
		}
	}
	return &entityVersionIndex{versions: merged}
}

type entityVersion struct {
	version   uint64
	commitLSN uint64
	tables    []string // which tables were mutated
}

type indexState struct {
	name    string
	column  string
	columns []string
	kind    string
	buckets map[string][]int
	entries []indexEntry

	// unsortedEntries is true when entries were concatenated without
	// sorting during compactOverlayAboveBase. allEntries() and
	// flattenIndex sort them lazily on demand. This avoids O(N log K)
	// comparison work during write-heavy compaction — the sort only
	// happens when btree entries are actually read (SELECT/ORDER BY).
	unsortedEntries bool

	// parent enables O(1) index COW for INSERT. Instead of copying the
	// entire buckets map (O(N) for unique columns), we create an overlay
	// with an empty map and a pointer to the previous version. Lookups
	// walk the chain; writes go to the current level only.
	// Depth is bounded by adaptiveOverlayMaxDepth; when exceeded, all
	// levels are flattened into a single map.
	parent *indexState

	// cachedDepth stores the number of parent links so that
	// overlayDepth() is O(1) instead of O(depth).
	cachedDepth int

	// baseSize tracks how many unique keys exist in the bottom-most
	// flat level of the chain.  Used by adaptiveOverlayMaxDepth to
	// choose between shallow (fast lookup) and deep (infrequent
	// flatten) overlay chains.
	baseSize int

	// isCompacted marks a level produced by compactOverlayAboveBase.
	// Tiered compaction uses this to avoid re-hashing previously
	// compacted keys: only new overlay levels above the nearest
	// compacted tier are merged, giving O(delta) cost per cycle
	// instead of O(accumulated).
	isCompacted bool
}

// addToBucket appends a rowID to the given key's bucket, lazily
// allocating the buckets map if it is nil (overlay levels start
// with nil buckets to avoid make(map) allocation on creation).
func (idx *indexState) addToBucket(key string, rowID int) {
	if idx.buckets == nil {
		idx.buckets = make(map[string][]int, 4)
	}
	idx.buckets[key] = append(idx.buckets[key], rowID)
}

// lookupBucket returns all rowIDs stored under key across this index
// and its parent chain. Callers in the read/query path should use this
// instead of accessing buckets directly.
// Iterative implementation to avoid O(depth) stack frames.
func (idx *indexState) lookupBucket(key string) []int {
	if idx == nil {
		return nil
	}
	// Fast path: no overlay chain.
	if idx.parent == nil {
		return idx.buckets[key]
	}
	// Single-pass with lazy segment collection.
	// Avoids slice allocation when key exists in only one level (common case).
	var first []int
	var segments [][]int
	for p := idx; p != nil; p = p.parent {
		if len(p.buckets) > 0 {
			if ids := p.buckets[key]; len(ids) > 0 {
				if first == nil {
					first = ids
				} else {
					if segments == nil {
						segments = append(segments, first)
					}
					segments = append(segments, ids)
				}
			}
		}
	}
	if first == nil {
		return nil
	}
	if segments == nil {
		return first
	}
	// Merge in parent-first order (segments were collected top-down).
	total := 0
	for _, s := range segments {
		total += len(s)
	}
	merged := make([]int, 0, total)
	for i := len(segments) - 1; i >= 0; i-- {
		merged = append(merged, segments[i]...)
	}
	return merged
}

// hasBucket returns true if key has any rowIDs in this index or its
// parent chain. Allocation-free; used for constraint validation.
// Iterative to avoid O(depth) stack frames.
func (idx *indexState) hasBucket(key string) bool {
	// Fast path: no overlay chain (post-compaction or fresh index).
	if idx.parent == nil {
		return len(idx.buckets[key]) > 0
	}
	for p := idx; p != nil; p = p.parent {
		if len(p.buckets) > 0 {
			if len(p.buckets[key]) > 0 {
				return true
			}
		}
	}
	return false
}

// foreachBucket calls fn for every (key, rowIDs) pair across this
// index and its parent chain. A key may appear in multiple levels;
// the caller collects all rowIDs.
// Iterative to avoid deep recursion on long overlay chains.
func (idx *indexState) foreachBucket(fn func(key string, rowIDs []int)) {
	if idx == nil {
		return
	}
	// Fast path: no overlay chain.
	if idx.parent == nil {
		for k, v := range idx.buckets {
			fn(k, v)
		}
		return
	}
	// Collect levels, then iterate bottom-up (oldest first).
	levels := make([]*indexState, 0, idx.cachedDepth+1)
	for p := idx; p != nil; p = p.parent {
		levels = append(levels, p)
	}
	for i := len(levels) - 1; i >= 0; i-- {
		for k, v := range levels[i].buckets {
			fn(k, v)
		}
	}
}

// overlayDepth returns the number of parent links.
// Uses cachedDepth for O(1) when available.
func (idx *indexState) overlayDepth() int {
	return idx.cachedDepth
}

// compactOverlayAboveBase uses tiered compaction to merge overlay levels
// efficiently. Instead of merging ALL overlays above the base every time
// (which re-hashes previously compacted keys at O(accumulated) cost), it
// merges only the NEW overlays above the nearest compacted tier, giving
// O(delta) cost per compaction cycle.
//
// Chain structure after multiple cycles:
//
//	[compacted_K] → [compacted_K-1] → … → [compacted_1] → [base]
//
// When too many tiers accumulate (maxCompactedTiers), a full flatten
// merges everything back into a single level above base.
//
// Empirically, this reduces total hash operations by ~2.25× over 8
// compaction cycles compared to always merging to base.
const maxCompactedTiers = 16

func compactOverlayAboveBase(idx *indexState) *indexState {
	if idx == nil || idx.parent == nil {
		return idx
	}

	// Collect all levels top-down.
	levels := make([]*indexState, 0, idx.cachedDepth+1)
	for p := idx; p != nil; p = p.parent {
		levels = append(levels, p)
	}
	base := levels[len(levels)-1] // deepest = base (no parent)

	if len(levels) == 2 {
		return idx // already [overlay] → [base], compact not needed
	}

	// Find the nearest compacted tier above base, and count total tiers.
	mergeTarget := base
	mergeTargetIdx := len(levels) - 1
	compactedTiers := 0
	for i := len(levels) - 2; i >= 1; i-- {
		if levels[i].isCompacted {
			compactedTiers++
		}
	}

	// If we have room for another tier, merge only above the nearest
	// compacted ancestor. Otherwise do a full flatten to reset tiers.
	if compactedTiers >= maxCompactedTiers {
		return flattenIndex(idx)
	}

	// Find nearest compacted ancestor to use as merge target.
	for i := len(levels) - 2; i >= 1; i-- {
		if levels[i].isCompacted {
			mergeTarget = levels[i]
			mergeTargetIdx = i
			break
		}
	}

	// Number of overlay levels to merge (above merge target).
	overlayCount := mergeTargetIdx
	if overlayCount <= 1 {
		return idx // single overlay above target, nothing to merge
	}

	// Count overlay entries to size the compacted map.
	var totalBucketKeys int
	for i := 0; i < overlayCount; i++ {
		totalBucketKeys += len(levels[i].buckets)
	}

	// Flatten threshold: if accumulated delta exceeds baseSize/4,
	// do a full flatten to prevent unbounded growth.
	if totalBucketKeys > base.baseSize/4 && base.baseSize > 0 {
		return flattenIndex(idx)
	}

	// Merge overlay levels (bottom-up, excluding merge target) into one map.
	// Reuse existing rowID slices when a key appears in only one level
	// (the common case for INSERT-only workloads) to avoid per-key
	// slice allocation.
	merged := make(map[string][]int, totalBucketKeys)
	for i := overlayCount - 1; i >= 0; i-- {
		for k, v := range levels[i].buckets {
			if existing := merged[k]; existing != nil {
				merged[k] = append(existing, v...)
			} else {
				merged[k] = v // reuse slice — no allocation
			}
		}
	}

	// For btree entries: concatenate without sorting. The sort is
	// deferred to allEntries() which only runs during SELECT/ORDER BY.
	var mergedEntries []indexEntry
	unsorted := false
	if idx.kind == "btree" {
		var total int
		for i := 0; i < overlayCount; i++ {
			total += len(levels[i].entries)
		}
		if total > 0 {
			mergedEntries = make([]indexEntry, 0, total)
			for i := overlayCount - 1; i >= 0; i-- {
				mergedEntries = append(mergedEntries, levels[i].entries...)
			}
			unsorted = true
		}
	}

	return &indexState{
		name:            idx.name,
		column:          idx.column,
		columns:         idx.columns,
		kind:            idx.kind,
		buckets:         merged,
		entries:         mergedEntries,
		unsortedEntries: unsorted,
		parent:          mergeTarget,
		cachedDepth:     mergeTarget.cachedDepth + 1,
		baseSize:        base.baseSize,
		isCompacted:     true,
	}
}

// flattenIndex merges all overlay levels into a single flat indexState
// with no parent. Used for full deep clone (snapshots) and as fallback
// when compactOverlayAboveBase detects excessive delta accumulation.
func flattenIndex(idx *indexState) *indexState {
	if idx == nil || idx.parent == nil {
		return idx
	}

	// Collect all levels bottom-up for correct merge order.
	levels := make([]*indexState, 0, idx.cachedDepth+1)
	for p := idx; p != nil; p = p.parent {
		levels = append(levels, p)
	}

	// Estimate capacity from bottom level (largest).
	base := levels[len(levels)-1]
	merged := make(map[string][]int, len(base.buckets))

	// Merge bottom-up (oldest first) so append order is correct.
	for i := len(levels) - 1; i >= 0; i-- {
		for k, v := range levels[i].buckets {
			merged[k] = append(merged[k], v...)
		}
	}

	// Merge btree entries from all levels using pairwise sorted merge.
	// Levels with unsortedEntries (from compactOverlayAboveBase) are
	// sorted before the merge.
	var mergedEntries []indexEntry
	if idx.kind == "btree" {
		var sortedRuns [][]indexEntry
		for i := len(levels) - 1; i >= 0; i-- {
			if len(levels[i].entries) > 0 {
				entries := levels[i].entries
				if levels[i].unsortedEntries {
					entries = sortEntriesCopy(entries)
				}
				sortedRuns = append(sortedRuns, entries)
			}
		}
		mergedEntries = pairwiseMergeSorted(sortedRuns)
	}

	return &indexState{
		name:    idx.name,
		column:  idx.column,
		columns: idx.columns,
		kind:    idx.kind,
		buckets: merged,
		entries: mergedEntries,
		baseSize: func() int {
			if idx.kind == "hash" {
				return len(merged)
			}
			return len(mergedEntries)
		}(),
	}
}

// flattenStateIndexes walks every table in every domain and flattens
// any index that has an overlay chain. Called after WAL replay so that
// subsequent reads get O(1) hash lookups and consistent baseSize values.
func flattenStateIndexes(state *readableState) {
	for _, dom := range state.domains {
		for _, tbl := range dom.tables {
			for name, idx := range tbl.indexes {
				if idx != nil && idx.parent != nil {
					tbl.indexes[name] = flattenIndex(idx)
				}
			}
		}
	}
}

// flattenStateIndexesForTables flattens overlay indexes only for the touched
// tables listed in the map. Used by incremental replay/catch-up so follower
// apply cost scales with the replay delta instead of total database size.
func flattenStateIndexesForTables(state *readableState, tables map[string]map[string]struct{}) {
	if len(tables) == 0 {
		return
	}
	for domainName, tableNames := range tables {
		dom := state.domains[domainName]
		if dom == nil {
			continue
		}
		for tableName := range tableNames {
			tbl := dom.tables[tableName]
			if tbl == nil {
				continue
			}
			for name, idx := range tbl.indexes {
				if idx != nil && idx.parent != nil {
					tbl.indexes[name] = flattenIndex(idx)
				}
			}
		}
	}
}

// compactStateIndexesForTables normalizes overlay indexes for touched tables
// without forcing a full flatten of the current table contents. This is used
// by follower direct-apply replay, which runs on every committed delta and
// must keep maintenance cost proportional to the replay batch.
func compactStateIndexesForTables(state *readableState, tables map[string]map[string]struct{}) {
	if len(tables) == 0 {
		return
	}
	for domainName, tableNames := range tables {
		dom := state.domains[domainName]
		if dom == nil {
			continue
		}
		for tableName := range tableNames {
			tbl := dom.tables[tableName]
			if tbl == nil {
				continue
			}
			for name, idx := range tbl.indexes {
				if idx == nil || idx.parent == nil {
					continue
				}
				if idx.kind == "hash" {
					// Hash overlays are already flattened aggressively on the write
					// path when depth exceeds the adaptive threshold, so leave them
					// as-is here to avoid rebuilding the full key map on every delta.
					continue
				}
				tbl.indexes[name] = compactOverlayAboveBase(idx)
			}
		}
	}
}

// allEntries returns a merged, sorted view of all btree entries across
// the overlay chain. When there is no parent, it returns entries directly
// (zero-allocation fast path). Levels with unsorted entries (from
// compactOverlayAboveBase) are sorted on demand.
func (idx *indexState) allEntries() []indexEntry {
	if idx == nil {
		return nil
	}
	if idx.parent == nil {
		if idx.unsortedEntries {
			return sortEntriesCopy(idx.entries)
		}
		return idx.entries
	}

	// Collect entry slices from each level; sort unsorted levels lazily.
	var levels [][]indexEntry
	for p := idx; p != nil; p = p.parent {
		if len(p.entries) > 0 {
			entries := p.entries
			if p.unsortedEntries {
				entries = sortEntriesCopy(entries)
			}
			levels = append(levels, entries)
		}
	}
	if len(levels) == 0 {
		return nil
	}
	if len(levels) == 1 {
		return levels[0]
	}

	// Merge from deepest ancestor forward. levels[last] is the root.
	merged := levels[len(levels)-1]
	for i := len(levels) - 2; i >= 0; i-- {
		merged = mergeSortedEntries(merged, levels[i])
	}
	return merged
}

// sortEntriesCopy returns a sorted copy of entries without mutating the
// original slice. Used to defer the sort cost from compaction (write path)
// to query time (read path). Thread-safe — each call creates an
// independent copy.
func sortEntriesCopy(entries []indexEntry) []indexEntry {
	sorted := make([]indexEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		c := compareIndexEntries(sorted[i], sorted[j])
		if c != 0 {
			return c < 0
		}
		return sorted[i].rowID < sorted[j].rowID
	})
	return sorted
}

// mergeSortedEntries merges two individually-sorted indexEntry slices
// into a single sorted slice.
func mergeSortedEntries(a, b []indexEntry) []indexEntry {
	result := make([]indexEntry, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		cmp := compareIndexEntries(a[i], b[j])
		if cmp < 0 || (cmp == 0 && a[i].rowID < b[j].rowID) {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// pairwiseMergeSorted merges K pre-sorted entry slices into one sorted
// slice using pairwise bottom-up merges. Total cost: O(N log K) where
// N is total entries and K is number of runs — significantly cheaper
// than re-sorting the concatenated result in O(N log N).
func pairwiseMergeSorted(runs [][]indexEntry) []indexEntry {
	if len(runs) == 0 {
		return nil
	}
	if len(runs) == 1 {
		return runs[0]
	}
	for len(runs) > 1 {
		next := make([][]indexEntry, 0, (len(runs)+1)/2)
		for i := 0; i < len(runs); i += 2 {
			if i+1 < len(runs) {
				next = append(next, mergeSortedEntries(runs[i], runs[i+1]))
			} else {
				next = append(next, runs[i])
			}
		}
		runs = next
	}
	return runs[0]
}

type indexEntry struct {
	value  ast.Literal
	values []ast.Literal
	rowID  int
}

type scanStrategy string

type mutationPayload struct {
	Domain string
	SQL    string
}

type preparedMutation struct {
	domain string
	sql    string
	plan   planner.Plan
}

type MigrationValidationReport struct {
	Domain          string
	ForwardCount    int
	RollbackCount   int
	RollbackSafe    bool
	ForwardAccepted bool
	RollbackChecked bool
	AutoRollback    bool
	RollbackSQL     []string
	Issues          []string
}

type SchemaSnapshot struct {
	Domains []SchemaDomain
}

type SchemaDomain struct {
	Name     string
	Tables   []SchemaTable
	Entities []SchemaEntity
}

type SchemaTable struct {
	Name                 string
	Columns              []SchemaColumn
	Indexes              []SchemaIndex
	VersionedForeignKeys []SchemaVersionedFK
}

type SchemaIndex struct {
	Name    string
	Columns []string
	Method  string // "hash" or "btree"
}

type SchemaVersionedFK struct {
	Column           string
	LSNColumn        string
	ReferencesDomain string
	ReferencesTable  string
	ReferencesColumn string
}

type SchemaEntity struct {
	Name      string
	RootTable string
	Tables    []string
}

type SchemaColumn struct {
	Name             string
	Type             string
	PrimaryKey       bool
	Unique           bool
	ReferencesTable  string
	ReferencesColumn string
	DefaultValue     *ast.DefaultExpr
}

// EntityVersionHistoryEntry represents one version of an entity aggregate instance.
type EntityVersionHistoryEntry struct {
	Version   uint64
	CommitLSN uint64
	Tables    []string
}

type aggregateSelectSpec struct {
	OutputColumn string
	Function     string
	Argument     string
	CountAll     bool
	Distinct     bool
}

type scanCostEstimate struct {
	strategy scanStrategy
	cost     int
	priority int
	detail   string
}

type ternaryResult int

type windowPartition struct {
	indices []int
}

// cloneTableStateProjection creates a schema-only clone of src for use as a
// VFK projection table. Rows, changeLog, and index data are empty; schema
// (columns, definitions, indexes structure, constraints) is copied.
func cloneTableStateProjection(src *tableState) *tableState {
	columns := make([]string, len(src.columns))
	copy(columns, src.columns)

	colDefs := make(map[string]ast.ColumnDefinition, len(src.columnDefinitions))
	for k, v := range src.columnDefinitions {
		colDefs[k] = v
	}

	uniqueColumns := make(map[string]struct{}, len(src.uniqueColumns))
	for k := range src.uniqueColumns {
		uniqueColumns[k] = struct{}{}
	}
	uniqueColumnList := make([]string, len(src.uniqueColumnList))
	copy(uniqueColumnList, src.uniqueColumnList)
	notNullColumns := make([]string, len(src.notNullColumns))
	copy(notNullColumns, src.notNullColumns)
	foreIgnKeys := make([]foreignKeyConstraint, len(src.foreignKeys))
	copy(foreIgnKeys, src.foreignKeys)
	checkConstrs := make([]checkConstraint, len(src.checkConstraints))
	copy(checkConstrs, src.checkConstraints)
	vfks := make([]versionedForeignKeyConstraint, len(src.versionedForeignKeys))
	copy(vfks, src.versionedForeignKeys)

	// Clone index structure with empty data.
	idxs := make(map[string]*indexState, len(src.indexes))
	for name, srcIdx := range src.indexes {
		dst := &indexState{
			name:    srcIdx.name,
			kind:    srcIdx.kind,
			columns: make([]string, len(srcIdx.columns)),
		}
		copy(dst.columns, srcIdx.columns)
		if srcIdx.kind == "hash" {
			dst.buckets = make(map[string][]int)
		}
		idxs[name] = dst
	}
	idxedCols := make(map[string]string, len(src.indexedColumns))
	for k, v := range src.indexedColumns {
		idxedCols[k] = v
	}
	idxedColSets := make(map[string]string, len(src.indexedColumnSets))
	for k, v := range src.indexedColumnSets {
		idxedColSets[k] = v
	}

	ts := &tableState{
		columns:              columns,
		columnDefinitions:    colDefs,
		rows:                 make([][]ast.Literal, 0),
		indexes:              idxs,
		indexedColumns:       idxedCols,
		indexedColumnSets:    idxedColSets,
		primaryKey:           src.primaryKey,
		uniqueColumns:        uniqueColumns,
		uniqueColumnList:     uniqueColumnList,
		notNullColumns:       notNullColumns,
		pkAutoUUID:           src.pkAutoUUID,
		foreignKeys:          foreIgnKeys,
		checkConstraints:     checkConstrs,
		versionedForeignKeys: vfks,
		isProjection:         true,
	}
	rebuildColumnIndex(ts)
	return ts
}

// rowToMap converts a stored []ast.Literal row to a map[string]ast.Literal
// for use in the query pipeline (predicates, projections, aggregations).
// Each call allocates a fresh map, so callers own the result and mutations
// to the returned map do not affect the stored row.
func rowToMap(ts *tableState, row []ast.Literal) map[string]ast.Literal {
	m := make(map[string]ast.Literal, len(ts.columns))
	for i, col := range ts.columns {
		if i < len(row) {
			m[col] = row[i]
		}
	}
	return m
}

// rowFromMap converts a map[string]ast.Literal to a positional []ast.Literal
// aligned to the given column order. Missing columns get an explicit NULL
// literal (Kind: LiteralNull). Used at INSERT time and for CTE virtual-table
// injection.
func rowFromMap(columns []string, m map[string]ast.Literal) []ast.Literal {
	row := make([]ast.Literal, len(columns))
	for i, col := range columns {
		if v, ok := m[col]; ok {
			row[i] = v
		} else {
			row[i] = ast.Literal{Kind: ast.LiteralNull}
		}
	}
	return row
}

// tableRowsToMaps converts all stored rows to maps for full-scan use.
// Returns nil when the table has no rows.
func tableRowsToMaps(ts *tableState) []map[string]ast.Literal {
	if len(ts.rows) == 0 {
		return nil
	}
	result := make([]map[string]ast.Literal, len(ts.rows))
	for i, row := range ts.rows {
		result[i] = rowToMap(ts, row)
	}
	return result
}

// projectionTableName returns the internal name used for VFK projection shadow
// tables inside subscriber domains. The "__proj__" prefix ensures these tables
// are hidden from normal schema introspection.
func projectionTableName(srcDomain, srcTable string) string {
	return "__proj__" + srcDomain + "__" + srcTable
}
