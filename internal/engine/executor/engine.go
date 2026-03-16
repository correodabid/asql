package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"asql/internal/engine/domains"
	"asql/internal/engine/parser"
	"asql/internal/engine/parser/ast"
	"asql/internal/engine/planner"
	"asql/internal/engine/ports"
	"asql/internal/storage/wal"
)

const (
	walTypeBegin    = "BEGIN"
	walTypeMutation = "MUTATION"
	walTypeCommit   = "COMMIT"
)

var (
	errSessionRequired    = errors.New("session is required")
	errTxRequired         = errors.New("active transaction required")
	errTxActive           = errors.New("transaction already active")
	errTxDomainMissing    = errors.New("domain is required")
	errExplainSQLRequired = errors.New("explain sql is required")
	errSavepointName      = errors.New("savepoint name is required")
	errSavepointMissing   = errors.New("savepoint not found")
	errTableExists        = errors.New("table already exists")
	errTableNotFound      = errors.New("table not found")
	errColumnExists       = errors.New("column already exists")
	errIndexExists        = errors.New("index already exists")
	errConstraint         = errors.New("constraint violation")
	errWriteConflict      = errors.New("write conflict detected")
	errEntityExists       = errors.New("entity already exists")
	errEntityTableMissing = errors.New("entity references table that does not exist")
	errEntityFKPath       = errors.New("entity table is not FK-connected to root")
)

// maxIndexOverlayDepth bounds the overlay chain length. When depth
// reaches this threshold, overlayIndexForInsert flattens the chain
// into a single map. Amortized commit cost: O(N / maxDepth).
//
// The actual depth threshold adapts based on index size via
// adaptiveOverlayMaxDepth: small indexes use indexOverlayMinDepth for
// fast lookups, large indexes use indexOverlayMaxDepth to amortise
// the O(N) flatten cost.
const (
	indexOverlayMinDepth        = 512   // depth for indexes < scaleThreshold entries
	indexOverlayMaxDepth        = 2048  // depth for medium indexes
	indexOverlayLargeDepth      = 2048  // depth for large indexes
	indexOverlayXLargeDepth     = 4096  // depth for very large indexes
	indexOverlayScaleThreshold  = 10000 // entry count boundary
	indexOverlayLargeThreshold  = 100000
	indexOverlayXLargeThreshold = 500000
)

// adaptiveOverlayMaxDepth picks an overlay chain depth threshold based
// on the number of entries in the base (flat) level of the index.
// Small indexes keep short chains for fast unique-constraint lookups.
// Large indexes accept longer chains to reduce amortised flatten cost.
func adaptiveOverlayMaxDepth(baseSize int) int {
	if baseSize < indexOverlayScaleThreshold {
		return indexOverlayMinDepth
	}
	if baseSize < indexOverlayLargeThreshold {
		return indexOverlayMaxDepth
	}
	if baseSize < indexOverlayXLargeThreshold {
		return indexOverlayLargeDepth
	}
	return indexOverlayXLargeDepth
}

// adaptiveHashOverlayMaxDepth keeps hash-index overlay chains short and,
// critically, stable as the table grows. PK/UNIQUE/FK validation hits
// `hasBucket()` on almost every foreground insert; letting the permitted
// chain depth scale up with base index size turns “same shape, bigger table”
// workloads into progressively more map lookups per insert.
//
// Tiered compaction already merges only the recent delta above the nearest
// compacted tier, so keeping the live-write depth fixed preserves near-O(1)
// validation while avoiding O(table) full flatten work.
func adaptiveHashOverlayMaxDepth(baseSize int) int {
	_ = baseSize
	return 32
}

// adaptiveReplayHashOverlayMaxDepth relaxes hash overlay flattening during
// WAL replay/direct follower apply. Followers do not sit on the hot PK/UNIQUE
// validation path, so allowing deeper chains here avoids repeated O(table)
// flatten work while a leader is streaming committed deltas.
func adaptiveReplayHashOverlayMaxDepth(baseSize int) int {
	if baseSize < indexOverlayScaleThreshold {
		return 8_192
	}
	if baseSize < indexOverlayLargeThreshold {
		return 32_768
	}
	if baseSize < indexOverlayXLargeThreshold {
		return 65_536
	}
	return 131_072
}

const (
	scanStrategyFullScan    scanStrategy = "full-scan"
	scanStrategyHashLookup  scanStrategy = "hash"
	scanStrategyIndexUnion  scanStrategy = "index-union"
	scanStrategyIndexUnionP scanStrategy = "index-union-partial"
	scanStrategyIndexNot    scanStrategy = "index-not"
	scanStrategyBTreeLookup scanStrategy = "btree-lookup"
	scanStrategyBTreeOrder  scanStrategy = "btree-order"
	scanStrategyBTreePrefix scanStrategy = "btree-prefix"
	scanStrategyBTreeIOScan scanStrategy = "btree-index-only" // index-only scan: no row access
	scanStrategyJoinRightIx scanStrategy = "join-right-index"
	scanStrategyJoinLeftIx  scanStrategy = "join-left-index"
	scanStrategyJoinNested  scanStrategy = "join-nested-loop"
)

// Engine executes SQL statements with deterministic WAL-backed state transitions.
// Reads are lock-free via an atomic pointer to an immutable readableState.
// Writes are serialized by writeMu and use COW (copy-on-write) semantics.
type Engine struct {
	writeMu                       sync.Mutex                    // serializes write path (commit, beginDomain, replay)
	readState                     atomic.Pointer[readableState] // lock-free read path
	walCacheMu                    sync.Mutex
	walRecordsCache               atomic.Pointer[walRecordCache]
	walReplayPlansCache           atomic.Pointer[walReplayPlanCache]
	historicalStateCache          *historicalStateCache
	logStore                      ports.LogStore
	snapDir                       string      // directory for numbered snapshot files ("" = disabled)
	snapSeq                       uint64      // monotonic sequence number for the next snapshot file
	snapCaptureInFlight           atomic.Bool // true while an in-memory snapshot capture goroutine is running
	snapWriteInFlight             atomic.Bool // true while a disk snapshot goroutine is running
	lastWriteUnixNano             atomic.Int64
	catalog                       *domains.Catalog
	txCount                       uint64
	logicalTS                     uint64
	headLSN                       uint64
	statsMu                       sync.Mutex
	scanStats                     map[scanStrategy]uint64
	snapshots                     *snapshotStore
	snapshotWg                    sync.WaitGroup // tracks in-flight async snapshot goroutines
	groupSync                     *groupSyncer   // non-nil when group commit is enabled
	commitQ                       *commitQueue   // batches concurrent commits under single writeMu
	mutationCount                 uint64         // total mutations applied, for snapshot scheduling
	recentMutationPressure        int            // rolling weighted mutation pressure for persisted checkpoint cadence
	recentMutationCount           int            // number of mutations currently represented in the rolling window
	recentMutationIndex           int            // next slot to overwrite in recentMutationWeights
	lastDiskSnapshotMutationCount uint64         // mutationCount at the last successful disk checkpoint
	recentMutationWeights         [recentMutationWindowSize]uint8
	lastCheckpointWALSize         uint64 // WAL TotalSize at last disk checkpoint, for size-based trigger
	lastDiskSnapshotLSN           uint64 // headLSN of the last successfully written disk snapshot
	lastDiskSnapshotLogicalTS     uint64 // logicalTS of the last successfully written disk snapshot
	perf                          *perfStats
	retainWAL                     bool                // when true, WAL is never truncated after snapshot persistence (audit/compliance)
	auditStore                    ports.AuditStore    // non-nil when a persistent audit log is wired in
	raftCommitter                 ports.RaftCommitter // non-nil in cluster mode; routes every WAL write through Raft quorum
	timestampIndex                *timestampLSNIndex
	// vfkSubscriptions maps "sourceDomain.sourceTable" to the list of subscriber
	// domains that maintain a read-only projection of that table. This index is
	// populated when a CREATE TABLE with VFK constraints is processed and is
	// used by fanoutProjections to materialise rows into subscriber domains.
	vfkSubscriptions map[string][]projectionSubscription
	replayMode       bool // true during WAL replay; skips VFK validation
}

type walRecordCache struct {
	headLSN uint64
	records []ports.WALRecord
}

type walReplayPlanCache struct {
	headLSN uint64
	entries []replayPlanCacheEntry
}

type replayPlanCacheEntry struct {
	plan planner.Plan
	ok   bool
}

type historicalStateCache struct {
	mu      sync.Mutex
	entries map[uint64]*readableState
	order   []uint64
}

const historicalStateCacheMaxEntries = 8

// walRecordCacheMaxIncrementalRecords bounds how many WAL records the engine
// will maintain via incremental slice appends after a full-WAL cache has been
// populated. Small cached histories still benefit from cheap append reuse for
// repeated time-travel/history queries, but once the cache grows beyond this
// window it is cheaper and safer for the write path to invalidate it than to
// copy an O(total WAL size) slice on every commit.
const walRecordCacheMaxIncrementalRecords = 4096

func newHistoricalStateCache() *historicalStateCache {
	return &historicalStateCache{entries: make(map[uint64]*readableState)}
}

func (cache *historicalStateCache) get(lsn uint64) (*readableState, bool) {
	if cache == nil {
		return nil, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	state, ok := cache.entries[lsn]
	if !ok {
		return nil, false
	}
	cache.touch(lsn)
	return state, true
}

func (cache *historicalStateCache) put(lsn uint64, state *readableState) {
	if cache == nil || state == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.entries[lsn]; exists {
		cache.entries[lsn] = state
		cache.touch(lsn)
		return
	}
	cache.entries[lsn] = state
	cache.order = append(cache.order, lsn)
	if len(cache.order) <= historicalStateCacheMaxEntries {
		return
	}
	evict := cache.order[0]
	cache.order = cache.order[1:]
	delete(cache.entries, evict)
}

func (cache *historicalStateCache) clear() {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.entries = make(map[uint64]*readableState)
	cache.order = cache.order[:0]
}

func (cache *historicalStateCache) touch(lsn uint64) {
	for i, candidate := range cache.order {
		if candidate != lsn {
			continue
		}
		copy(cache.order[i:], cache.order[i+1:])
		cache.order[len(cache.order)-1] = lsn
		return
	}
	cache.order = append(cache.order, lsn)
}

// EngineOption configures optional behaviours of the Engine.
type EngineOption func(*Engine)

// WithRetainWAL disables WAL truncation after snapshot persistence.
// Use in regulated/audit environments where full WAL history must be preserved.
func WithRetainWAL(retain bool) EngineOption {
	return func(e *Engine) { e.retainWAL = retain }
}

// WithAuditStore wires a persistent audit log into the engine.
// When set, every committed DML mutation is also appended to the audit store,
// preserving row-level history for FOR HISTORY queries independent of the WAL.
//
// The audit store does NOT replace the WAL for time-travel (AS OF LSN/TS)
// because the WAL also contains DDL; without it the schema cannot be
// reconstructed at an arbitrary historical LSN. WAL retention therefore
// remains enabled by default (retainWAL=true) and must be disabled explicitly
// via WithWALGC if WAL truncation is acceptable.
func WithAuditStore(s ports.AuditStore) EngineOption {
	return func(e *Engine) {
		e.auditStore = s
		// retainWAL is intentionally NOT set to false here.
		// Use WithWALGC() explicitly if you want WAL truncation after snapshots.
	}
}

// WithWALGC enables WAL truncation after each disk checkpoint.
// Segments whose LSN range is fully covered by the latest snapshot are removed,
// keeping the WAL footprint bounded.
//
// WARNING: enabling WAL GC makes time-travel queries (AS OF LSN/TS) to LSNs
// earlier than the oldest retained snapshot return approximate results (the
// closest available snapshot state) instead of exact historical state. The
// audit store (if wired) still provides correct FOR HISTORY results.
// Only enable this if you do not require exact time-travel before the
// checkpoint LSN.
func WithWALGC() EngineOption {
	return func(e *Engine) { e.retainWAL = false }
}

// SetRaftCommitter wires a Raft committer into the engine after construction.
// Once set, every WAL write in the commit path goes through Raft quorum before
// the commit is acknowledged to the client.  Safe to call exactly once, before
// the first commit is submitted (i.e. during server startup).
func (engine *Engine) SetRaftCommitter(c ports.RaftCommitter) {
	engine.raftCommitter = c
}

// maybeGCWAL truncates WAL records before snapLSN when retainWAL is false.
// Safe to call after any successful snapshot persist; the audit store (if wired)
// independently preserves full row-level history.
func (engine *Engine) maybeGCWAL(snapLSN uint64) {
	if engine.retainWAL || engine.logStore == nil {
		return
	}
	tr, ok := engine.logStore.(ports.Truncator)
	if !ok {
		return
	}
	if err := tr.TruncateBefore(context.Background(), snapLSN); err != nil {
		slog.Warn("wal: gc after snapshot failed", "lsn", snapLSN, "err", err)
	} else {
		engine.clearWALRecordCache()
		engine.clearHistoricalStateCache()
		slog.Info("wal: gc complete", "before_lsn", snapLSN)
	}
}

// New creates a new engine and replays existing WAL records into state.
// snapDir is the directory where numbered snapshot files are stored.
// Pass "" to disable snapshot persistence (tests).
func New(ctx context.Context, logStore ports.LogStore, snapDir string, opts ...EngineOption) (*Engine, error) {
	engine := &Engine{
		logStore:             logStore,
		snapDir:              snapDir,
		catalog:              domains.NewCatalog(),
		scanStats:            make(map[scanStrategy]uint64),
		snapshots:            newSnapshotStore(),
		historicalStateCache: newHistoricalStateCache(),
		perf:                 newPerfStats(),
		retainWAL:            true, // default: preserve full WAL for compliance
		timestampIndex:       newTimestampLSNIndex(snapDir),
		vfkSubscriptions:     make(map[string][]projectionSubscription),
	}

	for _, opt := range opts {
		opt(engine)
	}

	initial := &readableState{
		domains: make(map[string]*domainState),
	}
	engine.readState.Store(initial)

	if err := engine.Replay(ctx); err != nil {
		return nil, err
	}

	// Group commit: WAL writes happen without fsync under writeMu (fast),
	// then fsync is batched across concurrent commits outside the lock.
	engine.groupSync = newGroupSyncer(logStore.(ports.Syncer), engine.perf)

	// Commit queue: batches concurrent commits under a single writeMu acquisition.
	engine.commitQ = newCommitQueue(engine)

	return engine, nil
}

// NewSession creates a new execution session.
func (engine *Engine) NewSession() *Session {
	return &Session{}
}

// WaitPendingSnapshots blocks until all in-flight async snapshot goroutines
// have completed. Also shuts down the group commit syncer and flushes any
// in-memory snapshots not yet persisted to disk.
// Call before closing the engine or WAL store.
func (engine *Engine) WaitPendingSnapshots() {
	if engine.commitQ != nil {
		engine.commitQ.stop()
		engine.commitQ = nil
	}
	if engine.groupSync != nil {
		engine.groupSync.stop()
		engine.groupSync = nil
	}
	engine.snapshotWg.Wait()

	// Flush remaining in-memory snapshots to disk on shutdown.
	engine.persistAllSnapshots()
}

// Execute executes SQL in the context of a session.
func (engine *Engine) Execute(ctx context.Context, session *Session, sql string) (Result, error) {
	if session == nil {
		return Result{}, errSessionRequired
	}

	trimmed := strings.TrimSpace(sql)
	// Use case-insensitive prefix checks instead of uppercasing the full SQL.
	// INSERT statements can be very long, so avoiding the allocation matters.
	hasPrefixCI := func(s, prefix string) bool {
		if len(s) < len(prefix) {
			return false
		}
		return strings.EqualFold(s[:len(prefix)], prefix)
	}
	equalsCI := func(s, target string) bool {
		return strings.EqualFold(s, target)
	}

	switch {
	case hasPrefixCI(trimmed, "BEGIN DOMAIN "):
		return engine.beginDomain(session, trimmed)
	case hasPrefixCI(trimmed, "BEGIN CROSS DOMAIN "):
		return engine.beginCrossDomain(session, trimmed)
	case equalsCI(trimmed, "BEGIN") || equalsCI(trimmed, "BEGIN;"):
		return Result{}, fmt.Errorf("unsupported sql statement: use BEGIN DOMAIN <name> or BEGIN CROSS DOMAIN <a>, <b>")
	case hasPrefixCI(trimmed, "START TRANSACTION"):
		return Result{}, fmt.Errorf("unsupported sql statement: use BEGIN DOMAIN <name> or BEGIN CROSS DOMAIN <a>, <b> instead of START TRANSACTION")
	case hasPrefixCI(trimmed, "SAVEPOINT "):
		if session.activeTx == nil {
			return Result{}, errTxRequired
		}
		name, err := parseSavepointName(trimmed)
		if err != nil {
			return Result{}, err
		}
		return engine.savepoint(session.activeTx, name)
	case hasPrefixCI(trimmed, "ROLLBACK TO SAVEPOINT ") || hasPrefixCI(trimmed, "ROLLBACK TO "):
		if session.activeTx == nil {
			return Result{}, errTxRequired
		}
		name, err := parseRollbackToSavepointName(trimmed)
		if err != nil {
			return Result{}, err
		}
		return engine.rollbackToSavepoint(session.activeTx, name)
	case equalsCI(trimmed, "COMMIT") || equalsCI(trimmed, "COMMIT;"):
		return engine.commit(ctx, session)
	case equalsCI(trimmed, "ROLLBACK") || equalsCI(trimmed, "ROLLBACK;"):
		return engine.rollback(session), nil
	default:
		if session.activeTx == nil {
			return Result{}, errTxRequired
		}

		statement, err := parser.Parse(trimmed)
		if err != nil {
			return Result{}, fmt.Errorf("parse sql %q: %w", trimmed, err)
		}

		plan, err := planner.BuildForDomains(statement, session.activeTx.domains)
		if err != nil {
			return Result{}, fmt.Errorf("plan sql %q: %w", trimmed, err)
		}

		// Eager default resolution for INSERT ... RETURNING.
		// Resolve defaults (UUID_V7, autoincrement) and VFK versions now so we
		// can return them to the client immediately. The resolved values are
		// baked into the plan so that resolveDefaults() and
		// validateVersionedForeignKeys() at commit time see them and skip.
		var returningRows []map[string]ast.Literal
		if plan.Operation == planner.OperationInsert && len(plan.ReturningColumns) > 0 {
			// Lazy catch-up: after a Raft follower→leader transition the
			// in-memory readState may lag behind the WAL (which already
			// contains records replicated from the old leader). Replay them
			// now so the table schema is available for default resolution.
			if err := engine.CatchUp(context.Background(), 0); err != nil {
				return Result{}, fmt.Errorf("catchup before returning: %w", err)
			}
			snap := engine.readState.Load()
			domainFound := false
			tableFound := false
			if domain, ok := snap.domains[plan.DomainName]; ok {
				domainFound = true
				if table, ok := domain.tables[plan.TableName]; ok {
					tableFound = true
					// Handle first row.
					row := make(map[string]ast.Literal, len(plan.Columns))
					for i, col := range plan.Columns {
						row[col] = plan.Values[i]
					}
					resolveDefaults(table, row)
					resolveVFKVersions(snap, table, row)
					plan.Columns, plan.Values = flattenRow(row, table.columns)
					returningRows = append(returningRows,
						buildReturningRow(row, plan.ReturningColumns, table.columns),
					)

					// Handle additional rows (multi-row INSERT).
					for i, vals := range plan.MultiValues {
						mrow := make(map[string]ast.Literal, len(plan.Columns))
						for j, col := range plan.Columns {
							mrow[col] = vals[j]
						}
						resolveDefaults(table, mrow)
						resolveVFKVersions(snap, table, mrow)
						_, resolvedVals := flattenRow(mrow, table.columns)
						plan.MultiValues[i] = resolvedVals
						returningRows = append(returningRows,
							buildReturningRow(mrow, plan.ReturningColumns, table.columns),
						)
					}

					// Rebuild SQL with resolved defaults so the WAL stores the
					// materialized INSERT. Without this, replay generates new
					// UUIDs that don't match child FK references.
					if len(plan.MultiValues) > 0 {
						trimmed = rebuildMultiInsertSQL(plan.TableName, plan.Columns, plan.Values, plan.MultiValues)
					} else {
						trimmed = rebuildInsertSQL(plan.TableName, plan.Columns, plan.Values)
					}
				} else {
					slog.Warn("RETURNING: table not found in readState",
						slog.String("domain", plan.DomainName),
						slog.String("table", plan.TableName),
						slog.Int("domain_tables", len(domain.tables)),
					)
				}
			} else {
				var knownDomains []string
				for d := range snap.domains {
					knownDomains = append(knownDomains, d)
				}
				slog.Warn("RETURNING: domain not found in readState",
					slog.String("domain", plan.DomainName),
					slog.Any("known_domains", knownDomains),
				)
			}
			if !domainFound || !tableFound {
				slog.Warn("RETURNING: returning nil rows",
					slog.String("domain", plan.DomainName),
					slog.String("table", plan.TableName),
					slog.Bool("domain_found", domainFound),
					slog.Bool("table_found", tableFound),
					slog.Int("returning_cols", len(plan.ReturningColumns)),
				)
			}
		}

		session.activeTx.statements = append(session.activeTx.statements, trimmed)
		session.activeTx.plans = append(session.activeTx.plans, plan)
		return Result{Status: "QUEUED", Rows: returningRows}, nil
	}
}

// PerfStats returns a snapshot of engine performance metrics.
func (engine *Engine) PerfStats() PerfStatsSnapshot {
	if engine == nil || engine.perf == nil {
		return PerfStatsSnapshot{}
	}
	snap := engine.perf.snapshot()
	// File sizes: WAL (all segments) and snapshot (all files in dir).
	if sizer, ok := engine.logStore.(ports.Sizer); ok {
		if size, err := sizer.TotalSize(); err == nil {
			snap.WALFileSize = size
		}
	}
	if engine.snapDir != "" {
		snap.SnapshotFileSize = totalSnapshotDirSize(engine.snapDir)
	}
	if engine.auditStore != nil {
		if size, err := engine.auditStore.TotalSize(); err == nil {
			snap.AuditFileSize = size
		}
	}
	return snap
}

// Query executes a read-only SELECT against current head state with IMPORT support.
func (engine *Engine) Query(ctx context.Context, sql string, domains []string) (Result, error) {
	return engine.TimeTravelQueryAsOfLSN(ctx, sql, domains, math.MaxUint64)
}

// Explain returns deterministic plan diagnostics for a supported SQL statement.
func (engine *Engine) Explain(sql string, txDomains []string) (Result, error) {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return Result{}, errExplainSQLRequired
	}

	// Strip repeated EXPLAIN prefixes so the parser receives a valid statement.
	for len(trimmed) >= len("EXPLAIN") && strings.EqualFold(trimmed[:len("EXPLAIN")], "EXPLAIN") {
		if len(trimmed) > len("EXPLAIN") {
			next := trimmed[len("EXPLAIN")]
			if next != ' ' && next != '\t' && next != '\n' && next != '\r' {
				break
			}
		}
		trimmed = strings.TrimSpace(trimmed[len("EXPLAIN"):])
		if trimmed == "" {
			return Result{}, errExplainSQLRequired
		}
	}

	_, trimmed, err := parser.ExtractImports(trimmed)
	if err != nil {
		return Result{}, fmt.Errorf("extract imports: %w", err)
	}

	statement, err := parser.Parse(trimmed)
	if err != nil {
		return Result{}, fmt.Errorf("parse explain sql %q: %w", trimmed, err)
	}

	txDomains = engine.expandDomainsForVFKJoins(statement, txDomains)

	plan, err := planner.BuildForDomains(statement, txDomains)
	if err != nil {
		return Result{}, fmt.Errorf("plan explain sql: %w", err)
	}

	shapeBytes, err := wal.CanonicalJSON(plan)
	if err != nil {
		return Result{}, fmt.Errorf("encode explain plan shape: %w", err)
	}

	accessPlan := engine.buildAccessPlan(plan)
	accessBytes, err := wal.CanonicalJSON(accessPlan)
	if err != nil {
		return Result{}, fmt.Errorf("encode explain access plan: %w", err)
	}

	row := map[string]ast.Literal{
		"operation":   {Kind: ast.LiteralString, StringValue: string(plan.Operation)},
		"domain":      {Kind: ast.LiteralString, StringValue: plan.DomainName},
		"table":       {Kind: ast.LiteralString, StringValue: plan.TableName},
		"plan_shape":  {Kind: ast.LiteralString, StringValue: string(shapeBytes)},
		"access_plan": {Kind: ast.LiteralString, StringValue: string(accessBytes)},
	}

	return Result{Status: "EXPLAIN", Rows: []map[string]ast.Literal{row}}, nil
}

func (engine *Engine) readAllRecords(ctx context.Context) ([]ports.WALRecord, error) {
	currentHead := uint64(0)
	if state := engine.readState.Load(); state != nil {
		currentHead = state.headLSN
	}

	if cached := engine.walRecordsCache.Load(); cached != nil && cached.headLSN == currentHead {
		return cached.records, nil
	}

	engine.walCacheMu.Lock()
	defer engine.walCacheMu.Unlock()

	if cached := engine.walRecordsCache.Load(); cached != nil && cached.headLSN == currentHead {
		return cached.records, nil
	}

	recoverable, ok := engine.logStore.(interface {
		Recover(ctx context.Context) ([]ports.WALRecord, error)
	})

	if ok {
		records, err := recoverable.Recover(ctx)
		if err != nil {
			return nil, err
		}
		engine.storeWALRecordCache(currentHead, records)
		return records, nil
	}

	records, err := engine.logStore.ReadFrom(ctx, 1, 0)
	if err != nil {
		return nil, err
	}
	engine.storeWALRecordCache(currentHead, records)
	return records, nil
}

func (engine *Engine) storeWALRecordCache(headLSN uint64, records []ports.WALRecord) {
	engine.walRecordsCache.Store(&walRecordCache{headLSN: headLSN, records: records})
	engine.walReplayPlansCache.Store(nil)
}

func (engine *Engine) clearWALRecordCache() {
	engine.walRecordsCache.Store(nil)
	engine.walReplayPlansCache.Store(nil)
}

func (engine *Engine) cachedHistoricalState(targetLSN uint64) (*readableState, bool) {
	if engine == nil || engine.historicalStateCache == nil {
		return nil, false
	}
	return engine.historicalStateCache.get(targetLSN)
}

func (engine *Engine) storeHistoricalState(targetLSN uint64, state *readableState) {
	if engine == nil || engine.historicalStateCache == nil || state == nil {
		return
	}
	engine.historicalStateCache.put(targetLSN, state)
}

func (engine *Engine) clearHistoricalStateCache() {
	if engine == nil || engine.historicalStateCache == nil {
		return
	}
	engine.historicalStateCache.clear()
}

func (engine *Engine) appendWALRecordCache(records []ports.WALRecord) {
	if len(records) == 0 {
		return
	}

	engine.walCacheMu.Lock()
	defer engine.walCacheMu.Unlock()

	cached := engine.walRecordsCache.Load()
	if cached == nil {
		return
	}
	firstLSN := records[0].LSN
	if cached.headLSN+1 != firstLSN {
		engine.walRecordsCache.Store(nil)
		engine.walReplayPlansCache.Store(nil)
		return
	}
	if len(cached.records)+len(records) > walRecordCacheMaxIncrementalRecords {
		engine.walRecordsCache.Store(nil)
		engine.walReplayPlansCache.Store(nil)
		return
	}

	combined := make([]ports.WALRecord, len(cached.records)+len(records))
	copy(combined, cached.records)
	copy(combined[len(cached.records):], records)
	engine.walRecordsCache.Store(&walRecordCache{headLSN: records[len(records)-1].LSN, records: combined})
	engine.walReplayPlansCache.Store(nil)
}

// TimelineCommit describes a single committed transaction for the timeline UI.
type TimelineCommit struct {
	LSN       uint64                   `json:"lsn"`
	TxID      string                   `json:"tx_id"`
	Timestamp uint64                   `json:"timestamp"`
	Tables    []TimelineCommitMutation `json:"tables"`
}

// TimelineCommitMutation describes one mutation within a commit.
type TimelineCommitMutation struct {
	Domain    string `json:"domain"`
	Table     string `json:"table"`
	Operation string `json:"operation"`
}

// TimelineCommits scans WAL records between fromLSN and toLSN and returns
// aggregated commit summaries for the timeline UI. When domain is non-empty,
// only commits that touch the specified domain are returned. limit caps the
// number of commits returned (0 = unlimited).
func (engine *Engine) TimelineCommits(ctx context.Context, fromLSN, toLSN uint64, domain string, limit int) ([]TimelineCommit, error) {
	domainFilter := strings.ToLower(strings.TrimSpace(domain))
	records, err := engine.logStore.ReadFrom(ctx, fromLSN, 0)
	if err != nil {
		return nil, fmt.Errorf("read wal for timeline: %w", err)
	}

	// Index: txID -> commit LSN & timestamp
	type commitInfo struct {
		lsn       uint64
		timestamp uint64
	}
	txCommit := make(map[string]commitInfo)
	for _, r := range records {
		if toLSN > 0 && r.LSN > toLSN {
			break
		}
		if r.Type == walTypeCommit {
			txCommit[r.TxID] = commitInfo{lsn: r.LSN, timestamp: r.Timestamp}
		}
	}

	// Collect mutations grouped by txID
	type mutation struct {
		domain    string
		table     string
		operation string
	}
	txMutations := make(map[string][]mutation)
	for _, r := range records {
		if toLSN > 0 && r.LSN > toLSN {
			break
		}
		if r.Type != walTypeMutation {
			continue
		}
		if _, ok := txCommit[r.TxID]; !ok {
			continue // uncommitted
		}
		recDomain, plan, err := decodeMutationPayloadV2(r.Payload)
		if err != nil {
			continue
		}
		// Skip mutations outside the requested domain
		if domainFilter != "" && strings.ToLower(recDomain) != domainFilter {
			continue
		}
		txMutations[r.TxID] = append(txMutations[r.TxID], mutation{
			domain:    plan.DomainName,
			table:     plan.TableName,
			operation: string(plan.Operation),
		})
	}

	// Build sorted commit list — only include commits that have mutations
	// surviving the domain filter.
	commits := make([]TimelineCommit, 0, len(txCommit))
	for txID, info := range txCommit {
		muts := txMutations[txID]
		if domainFilter != "" && len(muts) == 0 {
			continue // commit doesn't touch this domain
		}
		tables := make([]TimelineCommitMutation, len(muts))
		for i, m := range muts {
			tables[i] = TimelineCommitMutation{Domain: m.domain, Table: m.table, Operation: m.operation}
		}
		commits = append(commits, TimelineCommit{
			LSN:       info.lsn,
			TxID:      txID,
			Timestamp: info.timestamp,
			Tables:    tables,
		})
	}
	sort.Slice(commits, func(i, j int) bool { return commits[i].LSN < commits[j].LSN })

	if limit > 0 && len(commits) > limit {
		commits = commits[:limit]
	}

	return commits, nil
}

// ListSnapshotPoints returns the LSNs of all available in-memory and on-disk
// checkpoint snapshots. Results are sorted ascending by LSN.
func (engine *Engine) ListSnapshotPoints() (memory []uint64, disk []uint64) {
	// In-memory snapshots held by the snapshot store.
	if engine.snapshots != nil {
		engine.snapshots.mu.Lock()
		for _, s := range engine.snapshots.snapshots {
			memory = append(memory, s.lsn)
		}
		engine.snapshots.mu.Unlock()
	}

	// Disk checkpoints (may contain entries no longer in memory).
	if engine.snapDir != "" {
		if diskSnaps, _, err := readAllSnapshotsFromDir(engine.snapDir); err == nil {
			for _, s := range diskSnaps {
				disk = append(disk, s.lsn)
			}
		}
	}
	return
}

// RowCount returns current row count for a table in a domain.
func (engine *Engine) RowCount(domain, table string) int {
	state := engine.readState.Load()

	domainState, exists := state.domains[strings.ToLower(domain)]
	if !exists {
		return 0
	}

	tableState, exists := domainState.tables[strings.ToLower(table)]
	if !exists {
		return 0
	}

	return len(tableState.rows)
}

// nextLogicalTimestamp increments and returns the next logical timestamp.
// Caller must hold writeMu.
func (engine *Engine) nextLogicalTimestamp() uint64 {
	engine.logicalTS++
	return engine.logicalTS
}

const (
	ternaryFalse ternaryResult = iota
	ternaryUnknown
	ternaryTrue
)

// Window function evaluation
