package executor

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"asql/internal/engine/domains"
	"asql/internal/engine/parser"
	"asql/internal/engine/parser/ast"
	"asql/internal/engine/planner"
)

var insertMutationPerf struct {
	mu            sync.Mutex
	samples       int64
	totalRows     int64
	totalIndexes  int64
	totalBuild    time.Duration
	totalValidate time.Duration
	totalApply    time.Duration
	totalIndex    time.Duration
	totalTotal    time.Duration
	maxTotal      time.Duration
	maxIndex      time.Duration
}

func recordInsertMutationPerf(_ planner.Plan, rowCount int, indexCount int, buildDur, validateDur, applyDur, indexDur, totalDur time.Duration) {
	insertMutationPerf.mu.Lock()
	defer insertMutationPerf.mu.Unlock()

	p := &insertMutationPerf
	p.samples++
	p.totalRows += int64(rowCount)
	p.totalIndexes += int64(indexCount)
	p.totalBuild += buildDur
	p.totalValidate += validateDur
	p.totalApply += applyDur
	p.totalIndex += indexDur
	p.totalTotal += totalDur
	if totalDur > p.maxTotal {
		p.maxTotal = totalDur
	}
	if indexDur > p.maxIndex {
		p.maxIndex = indexDur
	}
}

// engineStatesEqual compares two engine states for structural equality.
// Replaces reflect.DeepEqual for significantly better performance on large states.
func engineStatesEqual(a, b engineState) bool {
	if len(a.domains) != len(b.domains) {
		return false
	}
	for name, domA := range a.domains {
		domB, exists := b.domains[name]
		if !exists {
			return false
		}
		if !domainStatesEqual(domA, domB) {
			return false
		}
	}
	return true
}

func domainStatesEqual(a, b *domainState) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a.tables) != len(b.tables) {
		return false
	}
	for name, tblA := range a.tables {
		tblB, exists := b.tables[name]
		if !exists {
			return false
		}
		if !tableStatesEqual(tblA, tblB) {
			return false
		}
	}
	return true
}

func tableStatesEqual(a, b *tableState) bool {
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a.columns) != len(b.columns) || a.primaryKey != b.primaryKey {
		return false
	}
	for i, col := range a.columns {
		if col != b.columns[i] {
			return false
		}
	}
	if len(a.rows) != len(b.rows) {
		return false
	}
	for i, rowA := range a.rows {
		rowB := b.rows[i]
		if len(rowA) != len(rowB) {
			return false
		}
		for ci, valA := range rowA {
			if !literalEqual(valA, rowB[ci]) {
				return false
			}
		}
	}
	return true
}

func (engine *Engine) validateMutationsCanApply(state *readableState, mutations []preparedMutation) error {
	if len(mutations) == 0 {
		return nil
	}

	// Only deep-clone the specific domains touched by mutations (not all domains).
	affectedDomains := make(map[string]struct{}, len(mutations))
	for _, m := range mutations {
		affectedDomains[m.plan.DomainName] = struct{}{}
	}

	shadowDomains := make(map[string]*domainState, len(state.domains))
	for name, domain := range state.domains {
		if _, affected := affectedDomains[name]; affected {
			// Deep clone only affected domains.
			cloned := &domainState{tables: make(map[string]*tableState, len(domain.tables))}
			for tableName, table := range domain.tables {
				cloned.tables[tableName] = cloneTableState(table)
			}
			shadowDomains[name] = cloned
		} else {
			shadowDomains[name] = domain
		}
	}

	shadowState := &readableState{
		domains:   shadowDomains,
		headLSN:   state.headLSN,
		logicalTS: state.logicalTS,
	}
	shadow := buildShadowEngine(shadowState, engine.catalog, engine.logicalTS)

	for _, mutation := range mutations {
		if err := shadow.applyPlanToState(shadowState, mutation.plan, 0); err != nil {
			return err
		}
	}

	return nil
}

// ValidateMigrationPlan runs deterministic preflight and rollback-safety checks for migration statements.
func (engine *Engine) ValidateMigrationPlan(domain string, forwardSQL []string, rollbackSQL []string) (MigrationValidationReport, error) {
	report := MigrationValidationReport{
		Domain:        strings.TrimSpace(strings.ToLower(domain)),
		ForwardCount:  len(forwardSQL),
		RollbackCount: len(rollbackSQL),
		Issues:        make([]string, 0),
	}

	if report.Domain == "" {
		report.Issues = append(report.Issues, "domain is required")
		return report, nil
	}
	if len(forwardSQL) == 0 {
		report.Issues = append(report.Issues, "at least one forward migration statement is required")
		return report, nil
	}

	forwardPlans := make([]planner.Plan, 0, len(forwardSQL))
	for index, sql := range forwardSQL {
		statement, err := parser.Parse(sql)
		if err != nil {
			report.Issues = append(report.Issues, fmt.Sprintf("forward[%d] parse failed: %v", index, err))
			continue
		}

		plan, err := planner.BuildForDomains(statement, []string{report.Domain})
		if err != nil {
			report.Issues = append(report.Issues, fmt.Sprintf("forward[%d] plan failed: %v", index, err))
			continue
		}

		if !isMigrationMutationOperation(plan.Operation) {
			report.Issues = append(report.Issues, fmt.Sprintf("forward[%d] unsupported migration operation: %s", index, plan.Operation))
			continue
		}

		forwardPlans = append(forwardPlans, plan)
	}

	rollbackPlans := make([]planner.Plan, 0, len(rollbackSQL))
	for index, sql := range rollbackSQL {
		statement, err := parser.Parse(sql)
		if err != nil {
			report.Issues = append(report.Issues, fmt.Sprintf("rollback[%d] parse failed: %v", index, err))
			continue
		}

		plan, err := planner.BuildForDomains(statement, []string{report.Domain})
		if err != nil {
			report.Issues = append(report.Issues, fmt.Sprintf("rollback[%d] plan failed: %v", index, err))
			continue
		}

		if !isMigrationMutationOperation(plan.Operation) {
			report.Issues = append(report.Issues, fmt.Sprintf("rollback[%d] unsupported migration operation: %s", index, plan.Operation))
			continue
		}

		rollbackPlans = append(rollbackPlans, plan)
	}

	if len(report.Issues) > 0 {
		return report, nil
	}

	report.ForwardAccepted = true
	report.RollbackChecked = len(rollbackPlans) > 0
	if len(rollbackPlans) == 0 {
		report.Issues = append(report.Issues, "rollback plan is required for rollback safety validation")
		return report, nil
	}

	engine.writeMu.Lock()
	state := engine.readState.Load()
	baseline := cloneEngineState(engineState{domains: state.domains})
	logicalTS := engine.logicalTS
	engine.writeMu.Unlock()

	shadowState := &readableState{
		domains:   cloneDomains(state.domains),
		headLSN:   state.headLSN,
		logicalTS: state.logicalTS,
	}
	shadow := buildShadowEngine(shadowState, cloneCatalog(engine.catalog), logicalTS)
	for index, plan := range forwardPlans {
		if err := shadow.applyPlanToState(shadowState, plan, 0); err != nil {
			report.Issues = append(report.Issues, fmt.Sprintf("forward[%d] apply failed: %v", index, err))
			return report, nil
		}
	}

	for index, plan := range rollbackPlans {
		if err := shadow.applyPlanToState(shadowState, plan, 0); err != nil {
			report.Issues = append(report.Issues, fmt.Sprintf("rollback[%d] apply failed: %v", index, err))
			return report, nil
		}
	}

	afterRollback := engineState{domains: shadowState.domains}
	report.RollbackSafe = engineStatesEqual(baseline, afterRollback)
	if !report.RollbackSafe {
		report.Issues = append(report.Issues, "rollback plan does not restore baseline state")
	}

	return report, nil
}

func buildShadowEngine(state *readableState, catalog *domains.Catalog, logicalTS uint64) *Engine {
	shadow := &Engine{
		catalog:   catalog,
		logicalTS: logicalTS,
		scanStats: make(map[scanStrategy]uint64),
	}
	shadow.readState.Store(state)

	for domainName, ds := range state.domains {
		shadow.catalog.EnsureDomain(domainName)
		for tableName := range ds.tables {
			shadow.catalog.RegisterTable(domainName, tableName)
		}
	}

	return shadow
}

func isMigrationMutationOperation(operation planner.Operation) bool {
	switch operation {
	case planner.OperationCreateTable,
		planner.OperationAlterTableAddColumn,
		planner.OperationAlterTableDropColumn,
		planner.OperationAlterTableRenameColumn,
		planner.OperationCreateIndex,
		planner.OperationCreateEntity,
		planner.OperationInsert,
		planner.OperationUpdate,
		planner.OperationDelete,
		planner.OperationDropTable,
		planner.OperationDropIndex,
		planner.OperationTruncateTable:
		return true
	default:
		return false
	}
}

func cloneEngineState(source engineState) engineState {
	cloned := engineState{domains: make(map[string]*domainState, len(source.domains))}
	for domainName, domain := range source.domains {
		copiedDomain := &domainState{tables: make(map[string]*tableState, len(domain.tables))}
		for tableName, table := range domain.tables {
			copiedDomain.tables[tableName] = cloneTableState(table)
		}
		cloned.domains[domainName] = copiedDomain
	}

	return cloned
}

// shareTableForInsert creates a new tableState that shares row data with the
// source via Go's slice semantics. Because INSERT only appends new rows, old
// snapshots (with a shorter slice len) never see appended elements — no data
// copy is needed. This reduces INSERT COW cost from O(N) to amortized O(1)
// for the row slice.
//
// Both hash and btree indexes use the overlay/parent chain pattern for O(1)
// COW. INSERT adds entries only to the overlay level; reads merge the chain.
func shareTableForInsert(source *tableState, replayMode bool) *tableState {
	if source == nil {
		return nil
	}

	// Share row slice directly — append() will either use existing capacity
	// (writing beyond old snapshot's len, invisible to it) or reallocate.
	// Either way, old snapshots are safe.
	indexes := make(map[string]*indexState, len(source.indexes))
	for name, idx := range source.indexes {
		indexes[name] = overlayIndexForInsert(idx, replayMode)
	}

	return &tableState{
		columns:              source.columns,
		columnDefinitions:    source.columnDefinitions,
		columnIndex:          source.columnIndex,
		rows:                 source.rows, // shared — append is safe per slice semantics
		indexes:              indexes,
		indexedColumns:       source.indexedColumns,
		indexedColumnSets:    source.indexedColumnSets,
		primaryKey:           source.primaryKey,
		uniqueColumns:        source.uniqueColumns,
		uniqueColumnList:     source.uniqueColumnList,
		notNullColumns:       source.notNullColumns,
		pkAutoUUID:           source.pkAutoUUID,
		foreignKeys:          source.foreignKeys,
		checkConstraints:     source.checkConstraints,
		versionedForeignKeys: source.versionedForeignKeys,
		lastMutationTS:       source.lastMutationTS,
		changeLog:            source.changeLog, // shared — append is safe per slice semantics
		indexesLoaded:        source.indexesLoaded,
	}
}

// overlayIndexForInsert creates a new indexState with an empty buckets map
// and a parent pointer to the source. INSERT operations add entries to the
// overlay; lookups walk the chain. This makes INSERT COW O(1) instead of
// O(N) for the buckets map copy.
// Both hash and btree indexes use the overlay pattern. Btree entries are
// appended to a local (small) slice per overlay level; reads merge the
// chain via allEntries().
func overlayIndexForInsert(source *indexState, replayMode bool) *indexState {
	if source == nil {
		return nil
	}

	// If the overlay chain is too deep, compact before adding a new level.
	// compactOverlayAboveBase merges only the overlay deltas (O(delta))
	// instead of recreating the entire base map (O(base_size)).
	src := source
	maxDepth := adaptiveOverlayMaxDepth(source.baseSize)
	if source.kind == "hash" {
		if replayMode {
			maxDepth = adaptiveReplayHashOverlayMaxDepth(source.baseSize)
		} else {
			maxDepth = adaptiveHashOverlayMaxDepth(source.baseSize)
		}
	}
	if source.cachedDepth >= maxDepth {
		// Hash indexes sit directly on the PK/UNIQUE/FK validation path for
		// foreground writes, so leaders flatten them to restore O(1) lookup
		// depth. Replay applies already-validated WAL, so followers can keep
		// the cheaper tiered compaction here and avoid O(table) rebuilds.
		// Btree indexes always keep the cheaper tiered compaction because their
		// write path is append-heavy and their read path already merges lazily.
		if source.kind == "hash" {
			if replayMode {
				src = compactOverlayAboveBase(source)
			} else {
				src = flattenIndex(source)
			}
		} else {
			src = compactOverlayAboveBase(source)
		}
	}

	return &indexState{
		name:        source.name,
		column:      source.column,
		columns:     source.columns,
		kind:        source.kind,
		buckets:     nil, // lazily allocated on first addToBucket call
		parent:      src,
		cachedDepth: src.cachedDepth + 1,
		baseSize:    src.baseSize,
	}
}

func cloneTableState(source *tableState) *tableState {
	if source == nil {
		return nil
	}

	rows := make([][]ast.Literal, len(source.rows))
	for i, row := range source.rows {
		newRow := make([]ast.Literal, len(row))
		copy(newRow, row)
		rows[i] = newRow
	}

	indexes := make(map[string]*indexState, len(source.indexes))
	for name, index := range source.indexes {
		indexes[name] = cloneIndexState(index)
	}

	indexedColumns := make(map[string]string, len(source.indexedColumns))
	for column, name := range source.indexedColumns {
		indexedColumns[column] = name
	}

	indexedColumnSets := make(map[string]string, len(source.indexedColumnSets))
	for columns, name := range source.indexedColumnSets {
		indexedColumnSets[columns] = name
	}

	uniqueColumns := make(map[string]struct{}, len(source.uniqueColumns))
	for column := range source.uniqueColumns {
		uniqueColumns[column] = struct{}{}
	}

	foreignKeys := make([]foreignKeyConstraint, len(source.foreignKeys))
	copy(foreignKeys, source.foreignKeys)

	checkConstraints := make([]checkConstraint, len(source.checkConstraints))
	for index, check := range source.checkConstraints {
		checkConstraints[index] = checkConstraint{column: check.column, predicate: clonePredicate(check.predicate)}
	}

	columns := make([]string, len(source.columns))
	copy(columns, source.columns)

	columnDefinitions := make(map[string]ast.ColumnDefinition, len(source.columnDefinitions))
	for name, definition := range source.columnDefinitions {
		columnDefinitions[name] = definition
	}

	columnIndex := make(map[string]int, len(source.columnIndex))
	for k, v := range source.columnIndex {
		columnIndex[k] = v
	}

	versionedFKs := make([]versionedForeignKeyConstraint, len(source.versionedForeignKeys))
	copy(versionedFKs, source.versionedForeignKeys)

	return &tableState{
		columns:              columns,
		columnDefinitions:    columnDefinitions,
		columnIndex:          columnIndex,
		rows:                 rows,
		indexes:              indexes,
		indexedColumns:       indexedColumns,
		indexedColumnSets:    indexedColumnSets,
		primaryKey:           source.primaryKey,
		uniqueColumns:        uniqueColumns,
		foreignKeys:          foreignKeys,
		checkConstraints:     checkConstraints,
		versionedForeignKeys: versionedFKs,
		lastMutationTS:       source.lastMutationTS,
		changeLog:            source.changeLog, // shared — append is COW-safe per slice semantics
		indexesLoaded:        source.indexesLoaded,
	}
}

func cloneIndexState(source *indexState) *indexState {
	if source == nil {
		return nil
	}

	// If the index has an overlay chain, flatten first to collect all data.
	flat := source
	if source.parent != nil {
		flat = flattenIndex(source)
	}

	buckets := make(map[string][]int, len(flat.buckets))
	for key, rowIDs := range flat.buckets {
		copied := make([]int, len(rowIDs))
		copy(copied, rowIDs)
		buckets[key] = copied
	}

	entries := make([]indexEntry, len(flat.entries))
	for i, entry := range flat.entries {
		copiedValues := make([]ast.Literal, len(entry.values))
		copy(copiedValues, entry.values)
		entries[i] = indexEntry{value: entry.value, values: copiedValues, rowID: entry.rowID}
	}

	columns := make([]string, len(source.columns))
	copy(columns, source.columns)

	// Compute baseSize from the flattened data so that
	// adaptiveOverlayMaxDepth triggers compaction correctly.
	bs := flat.baseSize
	if bs == 0 {
		if source.kind == "hash" {
			bs = len(buckets)
		} else {
			bs = len(entries)
		}
	}

	return &indexState{
		name:     source.name,
		column:   source.column,
		columns:  columns,
		kind:     source.kind,
		buckets:  buckets,
		entries:  entries,
		baseSize: bs,
	}
}

func clonePredicate(source *ast.Predicate) *ast.Predicate {
	if source == nil {
		return nil
	}

	copy := *source
	return &copy
}

func (engine *Engine) applyMutationToState(state *readableState, domain, sql string, mutationLSN uint64) error {
	statement, err := parser.Parse(sql)
	if err != nil {
		return fmt.Errorf("parse sql %q: %w", sql, err)
	}

	plan, err := planner.BuildForDomains(statement, []string{domain})
	if err != nil {
		return fmt.Errorf("build plan: %w", err)
	}

	return engine.applyPlanToState(state, plan, mutationLSN)
}

// applyMutationToStateWithEntityTracking applies a mutation and also collects
// entity version tracking information. Used during WAL replay so that entity
// versions are correctly recorded for post-snapshot commits.
func (engine *Engine) applyMutationToStateWithEntityTracking(state *readableState, domain, sql string, mutationLSN uint64, collector map[string]map[string][]string) error {
	statement, err := parser.Parse(sql)
	if err != nil {
		return fmt.Errorf("parse sql %q: %w", sql, err)
	}

	plan, err := planner.BuildForDomains(statement, []string{domain})
	if err != nil {
		return fmt.Errorf("build plan: %w", err)
	}

	domainState := state.domains[plan.DomainName]

	// For DELETE: collect affected entity root PKs BEFORE rows are removed.
	if plan.Operation == planner.OperationDelete && domainState != nil {
		if tbl := domainState.tables[plan.TableName]; tbl != nil {
			var deletedRows []map[string]ast.Literal
			for _, rowSlice := range tbl.rows {
				rowMap := rowToMap(tbl, rowSlice)
				if matchPredicate(rowMap, plan.Filter, state, engine) {
					deletedRows = append(deletedRows, rowMap)
				}
			}
			collectEntityMutations(domainState, plan, deletedRows, collector)
		}
	}

	if err := engine.applyPlanToState(state, plan, mutationLSN); err != nil {
		return err
	}

	// For INSERT/UPDATE: collect affected entity root PKs AFTER mutation is applied.
	domainState = state.domains[plan.DomainName]
	if (plan.Operation == planner.OperationInsert || plan.Operation == planner.OperationUpdate) && domainState != nil {
		if tbl := domainState.tables[plan.TableName]; tbl != nil {
			var affectedRows []map[string]ast.Literal
			if plan.Operation == planner.OperationInsert {
				// Multi-row INSERT: collect all newly inserted rows.
				insertedCount := 1 + len(plan.MultiValues)
				startIdx := len(tbl.rows) - insertedCount
				if startIdx < 0 {
					startIdx = 0
				}
				for _, rowSlice := range tbl.rows[startIdx:] {
					affectedRows = append(affectedRows, rowToMap(tbl, rowSlice))
				}
			} else {
				for _, rowSlice := range tbl.rows {
					rowMap := rowToMap(tbl, rowSlice)
					if matchPredicate(rowMap, plan.Filter, state, engine) {
						affectedRows = append(affectedRows, rowMap)
					}
				}
			}
			collectEntityMutations(domainState, plan, affectedRows, collector)
		}
	}

	return nil
}

// applyMutationPlanWithEntityTracking applies a pre-decoded plan and collects
// entity version tracking information. Used during WAL replay with V2 payloads
// to avoid re-parsing SQL.
func (engine *Engine) applyMutationPlanWithEntityTracking(state *readableState, plan planner.Plan, mutationLSN uint64, collector map[string]map[string][]string) error {
	return engine.applyMutationPlanWithEntityTrackingTracked(state, plan, mutationLSN, collector, nil, nil)
}

func (engine *Engine) applyMutationPlanWithEntityTrackingTracked(state *readableState, plan planner.Plan, mutationLSN uint64, collector map[string]map[string][]string, clonedTables map[string]struct{}, fkValCache map[string]struct{}) error {
	domainState := state.domains[plan.DomainName]

	// For DELETE: collect affected entity root PKs BEFORE rows are removed.
	if plan.Operation == planner.OperationDelete && domainState != nil {
		if tbl := domainState.tables[plan.TableName]; tbl != nil {
			var deletedRows []map[string]ast.Literal
			for _, rowSlice := range tbl.rows {
				rowMap := rowToMap(tbl, rowSlice)
				if matchPredicate(rowMap, plan.Filter, state, engine) {
					deletedRows = append(deletedRows, rowMap)
				}
			}
			collectEntityMutations(domainState, plan, deletedRows, collector)
		}
	}

	if err := engine.applyPlanToStateTracked(state, plan, mutationLSN, clonedTables, fkValCache, collector); err != nil {
		return err
	}

	// For INSERT/UPDATE: collect affected entity root PKs AFTER mutation is applied.
	domainState = state.domains[plan.DomainName]
	if (plan.Operation == planner.OperationInsert || plan.Operation == planner.OperationUpdate) && domainState != nil {
		if tbl := domainState.tables[plan.TableName]; tbl != nil {
			var affectedRows []map[string]ast.Literal
			if plan.Operation == planner.OperationInsert {
				insertedCount := 1 + len(plan.MultiValues)
				startIdx := len(tbl.rows) - insertedCount
				if startIdx < 0 {
					startIdx = 0
				}
				for _, rowSlice := range tbl.rows[startIdx:] {
					affectedRows = append(affectedRows, rowToMap(tbl, rowSlice))
				}
			} else {
				for _, rowSlice := range tbl.rows {
					rowMap := rowToMap(tbl, rowSlice)
					if matchPredicate(rowMap, plan.Filter, state, engine) {
						affectedRows = append(affectedRows, rowMap)
					}
				}
			}
			collectEntityMutations(domainState, plan, affectedRows, collector)
		}
	}

	return nil
}

// applyPlanToState applies a mutation plan to the given state with COW cloning.
// For callers that apply multiple mutations to the same table (e.g., commit),
// use applyPlanToStateTracked to avoid redundant clones.
func (engine *Engine) applyPlanToState(state *readableState, plan planner.Plan, mutationLSN uint64) error {
	return engine.applyPlanToStateTracked(state, plan, mutationLSN, nil, nil, nil)
}

// applyPlanToStateTracked applies a mutation with optional clone tracking.
// When clonedTables is non-nil, tables already present in the map are not
// re-cloned, saving O(N) allocations for subsequent mutations to the same table
// within a single transaction.
// fkValCache, when non-nil, caches positive FK-existence lookups to avoid
// redundant hasBucket walks when multiple child rows reference the same parent.
func (engine *Engine) applyPlanToStateTracked(state *readableState, plan planner.Plan, mutationLSN uint64, clonedTables map[string]struct{}, fkValCache map[string]struct{}, pendingEntityVersions map[string]map[string][]string) error {

	domainState := getOrCreateDomain(state, plan.DomainName)

	switch plan.Operation {
	case planner.OperationCreateTable:
		if _, exists := domainState.tables[plan.TableName]; exists {
			if plan.IfNotExists {
				return nil
			}
			return errTableExists
		}

		engine.catalog.RegisterTable(plan.DomainName, plan.TableName)

		columns := make([]string, 0, len(plan.Schema))
		columnDefinitions := make(map[string]ast.ColumnDefinition, len(plan.Schema))
		primaryKey := ""
		uniqueColumns := make(map[string]struct{})
		foreignKeys := make([]foreignKeyConstraint, 0)
		checkConstraints := make([]checkConstraint, 0)
		for _, column := range plan.Schema {
			columns = append(columns, column.Name)
			columnDefinitions[column.Name] = column
			if column.PrimaryKey {
				if primaryKey != "" {
					return fmt.Errorf("%w: multiple PRIMARY KEY columns are not supported", errConstraint)
				}
				primaryKey = column.Name
				uniqueColumns[column.Name] = struct{}{}
			}
			if column.Unique {
				uniqueColumns[column.Name] = struct{}{}
			}

			if column.ReferencesTable != "" || column.ReferencesColumn != "" {
				if column.ReferencesTable == "" || column.ReferencesColumn == "" {
					return fmt.Errorf("%w: invalid foreign key definition on column %s", errConstraint, column.Name)
				}
				foreignKeys = append(foreignKeys, foreignKeyConstraint{
					column:           column.Name,
					referencesTable:  column.ReferencesTable,
					referencesColumn: column.ReferencesColumn,
				})
			}

			if column.Check != nil {
				checkConstraints = append(checkConstraints, checkConstraint{column: column.Name, predicate: column.Check})
			}
		}

		if err := validateForeignKeyDefinitions(domainState, plan.TableName, uniqueColumns, foreignKeys); err != nil {
			return err
		}

		// _lsn is a built-in meta-column appended to every table's column list.
		// It records the WAL commit LSN of the last write to each row.
		// Iteration code that processes only user columns explicitly skips _lsn
		// (resolveDefaults, flattenRow, buildReturningRow).
		columns = append(columns, "_lsn")

		// Build versioned foreign key constraints.
		versionedFKs := make([]versionedForeignKeyConstraint, 0, len(plan.VersionedForeignKeys))
		for _, vfk := range plan.VersionedForeignKeys {
			// Validate FK column exists.
			if _, exists := columnDefinitions[vfk.Column]; !exists {
				return fmt.Errorf("%w: versioned foreign key column %q not found in table definition", errConstraint, vfk.Column)
			}
			// Validate LSN column exists and is INT.
			lsnDef, lsnExists := columnDefinitions[vfk.LSNColumn]
			if !lsnExists {
				return fmt.Errorf("%w: versioned foreign key LSN column %q not found in table definition", errConstraint, vfk.LSNColumn)
			}
			if lsnDef.Type != ast.DataTypeInt {
				return fmt.Errorf("%w: versioned foreign key LSN column %q must be INT", errConstraint, vfk.LSNColumn)
			}
			// Resolve reference domain: if empty, use the table's own domain.
			refDomain := vfk.ReferencesDomain
			if refDomain == "" {
				refDomain = plan.DomainName
			}
			// Validate referenced domain/table exist.
			refDomainState, refDomainExists := state.domains[refDomain]
			if !refDomainExists {
				return fmt.Errorf("%w: versioned foreign key references domain %q which does not exist", errConstraint, refDomain)
			}
			refTable, refTableExists := refDomainState.tables[vfk.ReferencesTable]
			if !refTableExists {
				return fmt.Errorf("%w: versioned foreign key references table %s.%s which does not exist", errConstraint, refDomain, vfk.ReferencesTable)
			}
			// Validate referenced column is PK or UNIQUE.
			if refTable.primaryKey != vfk.ReferencesColumn {
				if _, isUnique := refTable.uniqueColumns[vfk.ReferencesColumn]; !isUnique {
					return fmt.Errorf("%w: versioned foreign key referenced column %s.%s(%s) must be PRIMARY KEY or UNIQUE", errConstraint, refDomain, vfk.ReferencesTable, vfk.ReferencesColumn)
				}
			}
			versionedFKs = append(versionedFKs, versionedForeignKeyConstraint{
				column:           vfk.Column,
				lsnColumn:        vfk.LSNColumn,
				referencesDomain: refDomain,
				referencesTable:  vfk.ReferencesTable,
				referencesColumn: vfk.ReferencesColumn,
			})
		}

		indexes := make(map[string]*indexState)
		indexedColumns := make(map[string]string)

		// Create implicit hash indexes for PK and UNIQUE columns so that
		// INSERT validation is O(1) instead of O(n).
		for col := range uniqueColumns {
			idxName := fmt.Sprintf("__auto_%s_%s", plan.TableName, col)
			indexes[idxName] = &indexState{
				name:    idxName,
				column:  col,
				columns: []string{col},
				kind:    "hash",
				buckets: make(map[string][]int),
			}
			indexedColumns[col] = idxName
		}

		ts := &tableState{
			columns:              columns,
			columnDefinitions:    columnDefinitions,
			rows:                 make([][]ast.Literal, 0),
			indexes:              indexes,
			indexedColumns:       indexedColumns,
			indexedColumnSets:    make(map[string]string),
			primaryKey:           primaryKey,
			uniqueColumns:        uniqueColumns,
			foreignKeys:          foreignKeys,
			checkConstraints:     checkConstraints,
			versionedForeignKeys: versionedFKs,
			lastMutationTS:       engine.logicalTS,
		}
		rebuildColumnIndex(ts)
		rebuildNotNullColumns(ts)
		rebuildUniqueColumnList(ts)
		rebuildPKAutoUUID(ts)
		domainState.tables[plan.TableName] = ts

		// Register VFK subscriptions and seed initial projection tables for
		// any cross-domain VERSIONED FOREIGN KEY constraints.  These shadow
		// tables live inside the subscriber domain (plan.DomainName) and are
		// rebuilt on every mutation to the source table via fanoutProjections.
		for _, vfk := range versionedFKs {
			if vfk.referencesDomain == "" || vfk.referencesDomain == plan.DomainName {
				continue // same-domain VFK — no projection needed
			}
			subKey := vfk.referencesDomain + "." + vfk.referencesTable
			projName := projectionTableName(vfk.referencesDomain, vfk.referencesTable)
			// Avoid duplicate subscription entries.
			alreadyRegistered := false
			for _, existing := range engine.vfkSubscriptions[subKey] {
				if existing.subscriberDomain == plan.DomainName && existing.projTableName == projName {
					alreadyRegistered = true
					break
				}
			}
			if !alreadyRegistered {
				engine.vfkSubscriptions[subKey] = append(engine.vfkSubscriptions[subKey], projectionSubscription{
					subscriberDomain: plan.DomainName,
					projTableName:    projName,
				})
			}
			// Seed the projection table from current source rows if present.
			if srcDS, ok := state.domains[vfk.referencesDomain]; ok {
				if srcTable, ok := srcDS.tables[vfk.referencesTable]; ok {
					domainState.tables[projName] = rebuildProjectionFromSource(srcTable)
				}
			}
		}
		return nil
	case planner.OperationAlterTableAddColumn:
		if !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
			return errTableNotFound
		}

		table, exists := domainState.tables[plan.TableName]
		if !exists {
			return errTableNotFound
		}

		if plan.AlterColumn == nil {
			return fmt.Errorf("alter table add column requires column definition")
		}

		column := strings.TrimSpace(strings.ToLower(plan.AlterColumn.Name))
		if column == "" {
			return fmt.Errorf("alter table add column requires column name")
		}

		for _, existing := range table.columns {
			if existing == column {
				return errColumnExists
			}
		}

		// COW: clone only if not already cloned in this transaction.
		tableKey := plan.DomainName + "." + plan.TableName
		if clonedTables != nil {
			if _, already := clonedTables[tableKey]; !already {
				table = cloneTableState(table)
				domainState.tables[plan.TableName] = table
				clonedTables[tableKey] = struct{}{}
			}
		} else {
			table = cloneTableState(table)
			domainState.tables[plan.TableName] = table
		}

		table.columns = append(table.columns, column)
		if table.columnDefinitions == nil {
			table.columnDefinitions = make(map[string]ast.ColumnDefinition)
		}
		table.columnDefinitions[column] = *plan.AlterColumn
		rebuildColumnIndex(table)
		rebuildNotNullColumns(table)
		rebuildUniqueColumnList(table)
		rebuildPKAutoUUID(table)
		// Extend every existing row with a null value for the new column.
		// Since the column was appended to table.columns, its position is len-1.
		for rowID := range table.rows {
			table.rows[rowID] = append(table.rows[rowID], ast.Literal{Kind: ast.LiteralNull})
		}
		table.lastMutationTS = engine.logicalTS
		return nil

	case planner.OperationAlterTableDropColumn:
		if !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
			return errTableNotFound
		}
		table, exists := domainState.tables[plan.TableName]
		if !exists {
			return errTableNotFound
		}

		column := strings.TrimSpace(strings.ToLower(plan.DropColumnName))
		found := false
		for _, c := range table.columns {
			if c == column {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column %q does not exist in table %s", column, plan.TableName)
		}

		// Prevent dropping primary key columns.
		if table.primaryKey == column {
			return fmt.Errorf("cannot drop primary key column %q", column)
		}

		// COW
		tableKey := plan.DomainName + "." + plan.TableName
		if clonedTables != nil {
			if _, already := clonedTables[tableKey]; !already {
				table = cloneTableState(table)
				domainState.tables[plan.TableName] = table
				clonedTables[tableKey] = struct{}{}
			}
		} else {
			table = cloneTableState(table)
			domainState.tables[plan.TableName] = table
		}

		// Remove column data from all rows using positional index.
		// Get old position BEFORE rebuilding columnIndex.
		oldPos := table.columnIndex[column]
		// Remove from columns list.
		newCols := make([]string, 0, len(table.columns)-1)
		for _, c := range table.columns {
			if c != column {
				newCols = append(newCols, c)
			}
		}
		table.columns = newCols

		// Remove from column definitions.
		delete(table.columnDefinitions, column)
		rebuildColumnIndex(table)
		rebuildNotNullColumns(table)
		rebuildUniqueColumnList(table)
		rebuildPKAutoUUID(table)

		for rowID, row := range table.rows {
			if oldPos < len(row) {
				newRow := make([]ast.Literal, 0, len(row)-1)
				newRow = append(newRow, row[:oldPos]...)
				newRow = append(newRow, row[oldPos+1:]...)
				table.rows[rowID] = newRow
			}
		}

		// Remove hash index for this column if it exists.
		if idxName, ok := table.indexedColumns[column]; ok {
			delete(table.indexes, idxName)
			delete(table.indexedColumns, column)
		}

		table.lastMutationTS = engine.logicalTS
		return nil

	case planner.OperationAlterTableRenameColumn:
		if !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
			return errTableNotFound
		}
		table, exists := domainState.tables[plan.TableName]
		if !exists {
			return errTableNotFound
		}

		oldCol := strings.TrimSpace(strings.ToLower(plan.RenameOldColumn))
		newCol := strings.TrimSpace(strings.ToLower(plan.RenameNewColumn))

		found := false
		for _, c := range table.columns {
			if c == oldCol {
				found = true
			}
			if c == newCol {
				return fmt.Errorf("column %q already exists in table %s", newCol, plan.TableName)
			}
		}
		if !found {
			return fmt.Errorf("column %q does not exist in table %s", oldCol, plan.TableName)
		}

		// COW
		tableKey := plan.DomainName + "." + plan.TableName
		if clonedTables != nil {
			if _, already := clonedTables[tableKey]; !already {
				table = cloneTableState(table)
				domainState.tables[plan.TableName] = table
				clonedTables[tableKey] = struct{}{}
			}
		} else {
			table = cloneTableState(table)
			domainState.tables[plan.TableName] = table
		}

		// Rename in columns list.
		for i, c := range table.columns {
			if c == oldCol {
				table.columns[i] = newCol
				break
			}
		}

		// Update primary key if renamed.
		if table.primaryKey == oldCol {
			table.primaryKey = newCol
		}

		// Move column definition.
		if def, ok := table.columnDefinitions[oldCol]; ok {
			def.Name = newCol
			table.columnDefinitions[newCol] = def
			delete(table.columnDefinitions, oldCol)
		}
		rebuildColumnIndex(table)
		rebuildNotNullColumns(table)
		rebuildUniqueColumnList(table)
		rebuildPKAutoUUID(table)

		// RENAME COLUMN does NOT change row data: rows are positional slices
		// aligned to table.columns. Renaming only updates the schema (columns
		// slice, columnDefinitions, columnIndex, primaryKey). No row mutation needed.

		// Rename index entry if it exists.
		if idxName, ok := table.indexedColumns[oldCol]; ok {
			table.indexedColumns[newCol] = idxName
			delete(table.indexedColumns, oldCol)
		}

		table.lastMutationTS = engine.logicalTS
		return nil

	case planner.OperationCreateIndex:
		if !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
			return errTableNotFound
		}

		table, exists := domainState.tables[plan.TableName]
		if !exists {
			return errTableNotFound
		}

		if _, exists := table.indexes[plan.IndexName]; exists {
			if plan.IfNotExists {
				return nil
			}
			return errIndexExists
		}

		indexColumns := make([]string, 0, len(plan.IndexColumns))
		if len(plan.IndexColumns) > 0 {
			indexColumns = append(indexColumns, plan.IndexColumns...)
		} else if plan.IndexColumn != "" {
			indexColumns = append(indexColumns, plan.IndexColumn)
		}
		if len(indexColumns) == 0 {
			return fmt.Errorf("index requires at least one column")
		}
		for i := range indexColumns {
			indexColumns[i] = strings.TrimSpace(strings.ToLower(indexColumns[i]))
			if indexColumns[i] == "" {
				return fmt.Errorf("index requires valid columns")
			}
		}

		indexKind := strings.TrimSpace(strings.ToLower(plan.IndexMethod))
		if indexKind == "" {
			indexKind = "hash"
		}
		if indexKind != "hash" && indexKind != "btree" {
			return fmt.Errorf("unsupported index method %s", plan.IndexMethod)
		}
		if indexKind == "hash" && len(indexColumns) > 1 {
			return fmt.Errorf("hash indexes do not support multiple columns")
		}

		if len(indexColumns) == 1 {
			if _, exists := table.indexedColumns[indexColumns[0]]; exists {
				return errIndexExists
			}
		}

		if table.indexedColumnSets == nil {
			table.indexedColumnSets = make(map[string]string)
		}
		columnsKey := indexColumnSetKey(indexColumns)
		if _, exists := table.indexedColumnSets[columnsKey]; exists {
			return errIndexExists
		}

		index := &indexState{name: plan.IndexName, column: indexColumns[0], columns: indexColumns, kind: indexKind, buckets: make(map[string][]int), entries: make([]indexEntry, 0)}

		// COW: clone only if not already cloned in this transaction.
		tableKey := plan.DomainName + "." + plan.TableName
		if clonedTables != nil {
			if _, already := clonedTables[tableKey]; !already {
				table = cloneTableState(table)
				domainState.tables[plan.TableName] = table
				clonedTables[tableKey] = struct{}{}
			}
		} else {
			table = cloneTableState(table)
			domainState.tables[plan.TableName] = table
		}

		for rowID, row := range table.rows {
			entry, exists := buildIndexEntryForRow(index, row, table.columnIndex, rowID)
			if !exists {
				continue
			}
			if index.kind == "hash" {
				addIndexEntry(index, entry)
				continue
			}
			index.entries = append(index.entries, entry)
		}
		if index.kind == "btree" && len(index.entries) > 1 {
			sort.Slice(index.entries, func(i, j int) bool {
				cmp := compareIndexEntries(index.entries[i], index.entries[j])
				if cmp != 0 {
					return cmp < 0
				}
				return index.entries[i].rowID < index.entries[j].rowID
			})
		}

		// Set baseSize after populating the newly created flat index.
		if index.kind == "hash" {
			index.baseSize = len(index.buckets)
		} else {
			index.baseSize = len(index.entries)
		}

		table.indexes[plan.IndexName] = index
		table.indexedColumnSets[columnsKey] = plan.IndexName
		if len(indexColumns) == 1 {
			table.indexedColumns[indexColumns[0]] = plan.IndexName
		}
		table.lastMutationTS = engine.logicalTS
		return nil
	case planner.OperationInsert:
		insertStarted := time.Now()
		if !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
			return errTableNotFound
		}

		table, exists := domainState.tables[plan.TableName]
		if !exists {
			return errTableNotFound
		}

		// Collect all value rows (first row + optional multi-row).
		allValueRows := make([][]ast.Literal, 0, 1+len(plan.MultiValues))
		allValueRows = append(allValueRows, plan.Values)
		allValueRows = append(allValueRows, plan.MultiValues...)

		// Build rows from all value groups.
		buildStarted := time.Now()
		rows := make([]map[string]ast.Literal, 0, len(allValueRows))
		for _, valueRow := range allValueRows {
			// Pre-size map for ALL columns (user + defaults + _lsn) to avoid
			// intermediate bucket resizes when resolveDefaults and _lsn add keys.
			row := make(map[string]ast.Literal, len(table.columns)+1)
			for index, column := range plan.Columns {
				row[column] = valueRow[index]
			}

			// Resolve default values for columns not present in the INSERT.
			resolveDefaults(table, row)

			// Coerce string values to JSON for JSON-typed columns.
			if err := coerceJSONValues(table, row); err != nil {
				return err
			}

			if mutationLSN > 0 {
				row["_lsn"] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(mutationLSN)}
			}

			// Validate versioned foreign keys and resolve auto-captured reference
			// tokens against the current transaction-visible state. Replay uses
			// the same path so auto-captured values are reconstructed
			// deterministically from WAL order.
			if err := validateVersionedForeignKeys(state, engine, table, row, pendingEntityVersions); err != nil {
				return err
			}

			rows = append(rows, row)
		}
		buildDur := time.Since(buildStarted)

		// COW: clone table only if not already cloned in this transaction.
		tableKey := plan.DomainName + "." + plan.TableName
		if clonedTables != nil {
			if _, already := clonedTables[tableKey]; !already {
				table = shareTableForInsert(table, engine.replayMode)
				domainState.tables[plan.TableName] = table
				clonedTables[tableKey] = struct{}{}
			}
		} else {
			table = shareTableForInsert(table, engine.replayMode)
			domainState.tables[plan.TableName] = table
		}

		// Insert rows one at a time so each subsequent row's unique validation
		// sees the previous rows (indexes are updated incrementally).
		// Skip PK uniqueness check when PK was auto-generated via UUIDv7.
		skipPKUnique := false
		if table.pkAutoUUID {
			skipPKUnique = true
			for _, c := range plan.Columns {
				if c == table.primaryKey {
					skipPKUnique = false
					break
				}
			}
		}
		validateDur := time.Duration(0)
		applyDur := time.Duration(0)
		indexDur := time.Duration(0)
		indexCount := len(table.indexes)
		for _, row := range rows {
			validateStarted := time.Now()
			// Handle ON CONFLICT: pre-check for matching rows on conflict columns.
			if plan.OnConflict != nil {
				conflictRowIdx := findConflictingRow(table, row, plan.OnConflict.ConflictColumns)
				if conflictRowIdx >= 0 {
					if plan.OnConflict.Action == ast.OnConflictDoNothing {
						validateDur += time.Since(validateStarted)
						continue // silently skip this row
					}
					if plan.OnConflict.Action == ast.OnConflictDoUpdate {
						if err := applyOnConflictUpdate(table, row, plan.OnConflict, conflictRowIdx, mutationLSN); err != nil {
							return err
						}
						validateDur += time.Since(validateStarted)
						continue
					}
				}
			}

			if !engine.replayMode {
				if err := validateInsertRow(domainState, table, row, skipPKUnique, fkValCache); err != nil { // FK cache threaded
					return err
				}
			}
			validateDur += time.Since(validateStarted)

			// Intern string values to deduplicate repeated categorical data
			// (status, country, etc.) across rows and reduce GC pressure.
			applyStarted := time.Now()
			internRowStrings(row)

			// Convert map to positional slice for compact storage.
			rowSlice := rowFromMap(table.columns, row)
			table.rows = append(table.rows, rowSlice)
			rowID := len(table.rows) - 1
			indexStarted := time.Now()
			for _, index := range table.indexes {
				entry, exists := buildIndexEntryForRow(index, rowSlice, table.columnIndex, rowID)
				if !exists {
					continue
				}
				addIndexEntry(index, entry)
			}
			indexDur += time.Since(indexStarted)
			// Change log keeps maps for history/audit readability.
			table.changeLog = append(table.changeLog, changeLogEntry{
				commitLSN: mutationLSN,
				operation: "INSERT",
				newRow:    row,
			})
			applyDur += time.Since(applyStarted)
		}
		recordInsertMutationPerf(plan, len(rows), indexCount, buildDur, validateDur, applyDur, indexDur, time.Since(insertStarted))
		table.lastMutationTS = engine.logicalTS
		trimChangeLog(table)
		return nil
	case planner.OperationUpdate:
		if !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
			return errTableNotFound
		}

		table, exists := domainState.tables[plan.TableName]
		if !exists {
			return errTableNotFound
		}

		if len(plan.Columns) != len(plan.Values) {
			return fmt.Errorf("update plan mismatch: columns=%d values=%d", len(plan.Columns), len(plan.Values))
		}

		updatedRows := 0
		type rowUpdate struct {
			oldRow map[string]ast.Literal
			newRow map[string]ast.Literal
		}
		updatedByRowID := make(map[int]rowUpdate)
		for rowID := range table.rows {
			rowSlice := table.rows[rowID]
			row := rowToMap(table, rowSlice) // convert stored slice to map for predicate + update
			if !matchPredicate(row, plan.Filter, state, engine) {
				continue
			}

			updated := cloneRow(row) // cloneRow creates a fresh map from the map
			for index, column := range plan.Columns {
				if len(plan.UpdateExprs) > 0 && plan.UpdateExprs[index].Kind == ast.UpdateExprArithmetic {
					expr := plan.UpdateExprs[index]
					resolved, err := evaluateArithmeticExpr(updated, expr)
					if err != nil {
						return fmt.Errorf("column %q: %w", column, err)
					}
					updated[column] = resolved
				} else {
					updated[column] = plan.Values[index]
				}
			}
			if mutationLSN > 0 {
				updated["_lsn"] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(mutationLSN)}
			}
			updatedByRowID[rowID] = rowUpdate{oldRow: row, newRow: updated}
			updatedRows++
		}

		if updatedRows > 0 {
			candidateRows := make([]map[string]ast.Literal, len(table.rows))
			for rowID, rowSlice := range table.rows {
				if u, ok := updatedByRowID[rowID]; ok {
					candidateRows[rowID] = u.newRow
					continue
				}
				candidateRows[rowID] = rowToMap(table, rowSlice)
			}

			if err := validateConstraints(domainState, table, candidateRows); err != nil {
				return err
			}

			// Validate versioned FKs for updated rows that touch FK or LSN columns.
			if len(table.versionedForeignKeys) > 0 {
				for _, u := range updatedByRowID {
					if err := validateVersionedForeignKeys(state, engine, table, u.newRow, pendingEntityVersions); err != nil {
						return err
					}
				}
			}

			// COW: clone only if not already cloned in this transaction.
			tableKey := plan.DomainName + "." + plan.TableName
			if clonedTables != nil {
				if _, already := clonedTables[tableKey]; !already {
					table = cloneTableState(table)
					domainState.tables[plan.TableName] = table
					clonedTables[tableKey] = struct{}{}
				}
			} else {
				table = cloneTableState(table)
				domainState.tables[plan.TableName] = table
			}

			for rowID, u := range updatedByRowID {
				// Convert updated map back to positional slice for storage.
				table.rows[rowID] = rowFromMap(table.columns, u.newRow)
				// Change log keeps maps for history/audit readability.
				table.changeLog = append(table.changeLog, changeLogEntry{
					commitLSN: mutationLSN,
					operation: "UPDATE",
					oldRow:    u.oldRow,
					newRow:    u.newRow,
				})
			}
			rebuildTableIndexes(table)
			table.lastMutationTS = engine.logicalTS
			trimChangeLog(table)
		}

		return nil
	case planner.OperationDelete:
		if !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
			return errTableNotFound
		}

		table, exists := domainState.tables[plan.TableName]
		if !exists {
			return errTableNotFound
		}

		remaining := make([][]ast.Literal, 0, len(table.rows))
		var deletedRowData []map[string]ast.Literal
		deletedRows := 0
		for _, rowSlice := range table.rows {
			rowMap := rowToMap(table, rowSlice)
			if matchPredicate(rowMap, plan.Filter, state, engine) {
				deletedRowData = append(deletedRowData, rowMap)
				deletedRows++
				continue
			}
			remaining = append(remaining, rowSlice)
		}

		if deletedRows > 0 {
			// COW: clone only if not already cloned in this transaction.
			tableKey := plan.DomainName + "." + plan.TableName
			if clonedTables != nil {
				if _, already := clonedTables[tableKey]; !already {
					table = cloneTableState(table)
					domainState.tables[plan.TableName] = table
					clonedTables[tableKey] = struct{}{}
				}
			} else {
				table = cloneTableState(table)
				domainState.tables[plan.TableName] = table
			}

			for _, row := range deletedRowData {
				table.changeLog = append(table.changeLog, changeLogEntry{
					commitLSN: mutationLSN,
					operation: "DELETE",
					oldRow:    row,
				})
			}
			table.rows = remaining
			rebuildTableIndexes(table)
			table.lastMutationTS = engine.logicalTS
			trimChangeLog(table)
		}

		return nil
	case planner.OperationDropTable:
		table, exists := domainState.tables[plan.TableName]
		if !exists {
			if plan.IfExists {
				return nil
			}
			return errTableNotFound
		}

		// Check if any other table in this domain references this table via FK.
		if !plan.Cascade {
			for otherName, otherTable := range domainState.tables {
				if otherName == plan.TableName {
					continue
				}
				for _, fk := range otherTable.foreignKeys {
					if fk.referencesTable == plan.TableName {
						return fmt.Errorf("%w: table %s is referenced by foreign key on %s(%s)",
							errConstraint, plan.TableName, otherName, fk.column)
					}
				}
			}
		}

		// Check if any entity references this table.
		for entityName, entity := range domainState.entities {
			for _, entityTable := range entity.tables {
				if entityTable == plan.TableName {
					if !plan.Cascade {
						return fmt.Errorf("%w: table %s is part of entity %s",
							errConstraint, plan.TableName, entityName)
					}
					// CASCADE: remove entity that references this table.
					delete(domainState.entities, entityName)
					delete(domainState.entityVersions, entityName)
					break
				}
			}
		}

		// CASCADE: drop FK references from other tables pointing to this table.
		if plan.Cascade {
			for otherName, otherTable := range domainState.tables {
				if otherName == plan.TableName {
					continue
				}
				newFKs := make([]foreignKeyConstraint, 0, len(otherTable.foreignKeys))
				for _, fk := range otherTable.foreignKeys {
					if fk.referencesTable != plan.TableName {
						newFKs = append(newFKs, fk)
					}
				}
				if len(newFKs) != len(otherTable.foreignKeys) {
					// COW: clone the referencing table to update its FK list.
					tableKey := plan.DomainName + "." + otherName
					if clonedTables != nil {
						if _, already := clonedTables[tableKey]; !already {
							otherTable = cloneTableState(otherTable)
							domainState.tables[otherName] = otherTable
							clonedTables[tableKey] = struct{}{}
						}
					} else {
						otherTable = cloneTableState(otherTable)
						domainState.tables[otherName] = otherTable
					}
					otherTable.foreignKeys = newFKs
				}
			}
		}

		// Record change log entries for all deleted rows.
		_ = table // suppress unused warning — change log is on the dropped table itself, no need to preserve

		engine.catalog.UnregisterTable(plan.DomainName, plan.TableName)
		delete(domainState.tables, plan.TableName)
		return nil
	case planner.OperationDropIndex:
		// Find the index across all tables in the domain.
		found := false
		for tableName, table := range domainState.tables {
			if _, exists := table.indexes[plan.IndexName]; !exists {
				continue
			}
			// COW: clone table.
			tableKey := plan.DomainName + "." + tableName
			if clonedTables != nil {
				if _, already := clonedTables[tableKey]; !already {
					table = cloneTableState(table)
					domainState.tables[tableName] = table
					clonedTables[tableKey] = struct{}{}
				}
			} else {
				table = cloneTableState(table)
				domainState.tables[tableName] = table
			}

			idx := table.indexes[plan.IndexName]
			// Remove from indexedColumns if single-column.
			if len(idx.columns) == 1 {
				// Only remove if this index is the one mapped.
				if mapped, ok := table.indexedColumns[idx.columns[0]]; ok && mapped == plan.IndexName {
					delete(table.indexedColumns, idx.columns[0])
				}
			}
			// Remove from indexedColumnSets.
			columnsKey := indexColumnSetKey(idx.columns)
			delete(table.indexedColumnSets, columnsKey)
			delete(table.indexes, plan.IndexName)
			table.lastMutationTS = engine.logicalTS
			found = true
			break
		}
		if !found {
			if plan.IfExists {
				return nil
			}
			return fmt.Errorf("index %s not found", plan.IndexName)
		}
		return nil
	case planner.OperationTruncateTable:
		if !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
			return errTableNotFound
		}

		table, exists := domainState.tables[plan.TableName]
		if !exists {
			return errTableNotFound
		}

		// COW: clone table.
		tableKey := plan.DomainName + "." + plan.TableName
		if clonedTables != nil {
			if _, already := clonedTables[tableKey]; !already {
				table = cloneTableState(table)
				domainState.tables[plan.TableName] = table
				clonedTables[tableKey] = struct{}{}
			}
		} else {
			table = cloneTableState(table)
			domainState.tables[plan.TableName] = table
		}

		// Record change log entries for all truncated rows.
		for _, rowSlice := range table.rows {
			table.changeLog = append(table.changeLog, changeLogEntry{
				commitLSN: mutationLSN,
				operation: "DELETE",
				oldRow:    rowToMap(table, rowSlice),
			})
		}

		table.rows = make([][]ast.Literal, 0)
		rebuildTableIndexes(table)
		table.lastMutationTS = engine.logicalTS
		trimChangeLog(table)
		return nil
	case planner.OperationCreateEntity:
		if domainState.entities == nil {
			domainState.entities = make(map[string]*entityDefinition)
		}
		if domainState.entityVersions == nil {
			domainState.entityVersions = make(map[string]*entityVersionIndex)
		}

		entityName := strings.ToLower(strings.TrimSpace(plan.EntityName))
		if _, exists := domainState.entities[entityName]; exists {
			if plan.IfNotExists {
				return nil
			}
			return errEntityExists
		}

		rootTable := strings.ToLower(strings.TrimSpace(plan.EntityRootTable))
		if _, exists := domainState.tables[rootTable]; !exists {
			return fmt.Errorf("%w: root table %s", errEntityTableMissing, rootTable)
		}

		allTables := make([]string, 0, len(plan.EntityTables))
		for _, t := range plan.EntityTables {
			normalized := strings.ToLower(strings.TrimSpace(t))
			if _, exists := domainState.tables[normalized]; !exists {
				return fmt.Errorf("%w: %s", errEntityTableMissing, normalized)
			}
			allTables = append(allTables, normalized)
		}

		fkPaths := make(map[string][]fkHop)
		for _, tableName := range allTables {
			if tableName == rootTable {
				continue
			}
			path, err := resolveEntityFKPath(domainState, tableName, rootTable)
			if err != nil {
				return err
			}
			fkPaths[tableName] = path
		}

		tableSet := make(map[string]struct{}, len(allTables))
		for _, t := range allTables {
			tableSet[t] = struct{}{}
		}
		domainState.entities[entityName] = &entityDefinition{
			name:      entityName,
			rootTable: rootTable,
			tables:    allTables,
			tableSet:  tableSet,
			fkPaths:   fkPaths,
		}
		domainState.entityVersions[entityName] = &entityVersionIndex{
			versions: make(map[string][]entityVersion),
		}
		// Rebuild domain-level entityTables set.
		rebuildEntityTablesSet(domainState)
		return nil
	case planner.OperationSelect:
		return nil
	default:
		return fmt.Errorf("unsupported operation %s", plan.Operation)
	}
}

// evaluateArithmeticExpr evaluates an arithmetic SET expression (e.g. col + 1)
// against the current row values and returns the resulting literal.
func evaluateArithmeticExpr(row map[string]ast.Literal, expr ast.UpdateExpr) (ast.Literal, error) {
	current, exists := row[expr.Column]
	if !exists {
		return ast.Literal{}, fmt.Errorf("referenced column %q not found in row", expr.Column)
	}

	// Resolve both sides to float64 for uniform arithmetic.
	left, leftIsFloat, err := literalToFloat64(current)
	if err != nil {
		return ast.Literal{}, fmt.Errorf("cannot apply arithmetic to column %q value: %w", expr.Column, err)
	}
	right, rightIsFloat, err := literalToFloat64(expr.Operand)
	if err != nil {
		return ast.Literal{}, fmt.Errorf("cannot apply arithmetic with operand: %w", err)
	}

	var result float64
	switch expr.Operator {
	case "+":
		result = left + right
	case "-":
		result = left - right
	case "*":
		result = left * right
	case "/":
		if right == 0 {
			return ast.Literal{}, fmt.Errorf("division by zero")
		}
		result = left / right
	default:
		return ast.Literal{}, fmt.Errorf("unsupported arithmetic operator %q", expr.Operator)
	}

	// Preserve integer kind when both operands are integers and result is exact.
	if !leftIsFloat && !rightIsFloat && result == float64(int64(result)) {
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(result)}, nil
	}
	return ast.Literal{Kind: ast.LiteralFloat, FloatValue: result}, nil
}

// literalToFloat64 converts a numeric literal to float64 for arithmetic.
// Returns the value, whether it was originally a float, and any error.
func literalToFloat64(l ast.Literal) (float64, bool, error) {
	switch l.Kind {
	case ast.LiteralNumber:
		return float64(l.NumberValue), false, nil
	case ast.LiteralFloat:
		return l.FloatValue, true, nil
	default:
		return 0, false, fmt.Errorf("non-numeric literal kind %q", l.Kind)
	}
}

// findConflictingRow scans the table for an existing row that matches the
// given row on all conflict columns. Returns the row index (>= 0) if found,
// or -1 if no conflict exists. Uses hash indexes when available for O(1) lookup.
func findConflictingRow(table *tableState, row map[string]ast.Literal, conflictColumns []string) int {
	if len(conflictColumns) == 0 || table == nil {
		return -1
	}

	// Optimisation: for a single conflict column, try hash index lookup.
	if len(conflictColumns) == 1 {
		col := conflictColumns[0]
		val, exists := row[col]
		if !exists || val.Kind == ast.LiteralNull {
			return -1
		}
		if idxName, ok := table.indexedColumns[col]; ok {
			if idx, has := table.indexes[idxName]; has && idx.kind == "hash" {
				key := literalKey(val)
				rowIDs := idx.lookupBucket(key)
				if len(rowIDs) > 0 {
					return rowIDs[0]
				}
				return -1
			}
		}
	}

	// Fallback: linear scan (supports multi-column conflict targets).
	for i, existingSlice := range table.rows {
		if existingSlice == nil {
			continue
		}
		existing := rowToMap(table, existingSlice)
		match := true
		for _, col := range conflictColumns {
			rowVal, rowExists := row[col]
			existVal, existExists := existing[col]
			if !rowExists || !existExists {
				match = false
				break
			}
			if literalKey(rowVal) != literalKey(existVal) {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

// applyOnConflictUpdate modifies the existing conflicting row in-place
// using the DO UPDATE SET assignments. Handles EXCLUDED.col references
// by substituting values from the would-be-inserted row.
func applyOnConflictUpdate(table *tableState, newRow map[string]ast.Literal, oc *ast.OnConflictClause, conflictRowIdx int, mutationLSN uint64) error {
	existingSlice := table.rows[conflictRowIdx]
	if existingSlice == nil {
		return fmt.Errorf("conflict row at index %d is nil", conflictRowIdx)
	}

	// Convert to map for modification. Save old state for change log + index removal.
	existing := rowToMap(table, existingSlice)
	oldRow := cloneRow(existing)

	// Apply each SET assignment.
	for i, col := range oc.UpdateColumns {
		var value ast.Literal
		if i < len(oc.UpdateExcluded) && oc.UpdateExcluded[i] != "" {
			// EXCLUDED.col reference: use the value from the new (conflicting) row.
			excCol := oc.UpdateExcluded[i]
			val, exists := newRow[excCol]
			if !exists {
				return fmt.Errorf("EXCLUDED column %q not found in inserted row", excCol)
			}
			value = val
		} else if i < len(oc.UpdateValues) {
			value = oc.UpdateValues[i]
		} else {
			return fmt.Errorf("no value or EXCLUDED reference for update column %q", col)
		}
		existing[col] = value
	}

	// Convert updated map back to positional slice and store.
	newSlice := rowFromMap(table.columns, existing)
	table.rows[conflictRowIdx] = newSlice

	// Rebuild hash indexes for columns that changed.
	for _, index := range table.indexes {
		if index.kind != "hash" {
			continue
		}
		// Check if any indexed column was updated.
		needsRebuild := false
		for _, col := range index.columns {
			if _, ok := findInSlice(oc.UpdateColumns, col); ok {
				needsRebuild = true
				break
			}
		}
		if !needsRebuild {
			continue
		}
		// Remove old entry using old slice.
		oldEntry, oldExists := buildIndexEntryForRow(index, existingSlice, table.columnIndex, conflictRowIdx)
		if oldExists {
			oldKey := literalKey(oldEntry.value)
			if index.buckets != nil {
				bucket := index.buckets[oldKey]
				for j, rid := range bucket {
					if rid == conflictRowIdx {
						index.buckets[oldKey] = append(bucket[:j], bucket[j+1:]...)
						break
					}
				}
			}
		}
		// Add new entry using new slice.
		newEntry, newExists := buildIndexEntryForRow(index, newSlice, table.columnIndex, conflictRowIdx)
		if newExists {
			newKey := literalKey(newEntry.value)
			index.addToBucket(newKey, conflictRowIdx)
		}
	}

	// Record the update in changeLog.
	table.changeLog = append(table.changeLog, changeLogEntry{
		commitLSN: mutationLSN,
		operation: "UPDATE",
		oldRow:    oldRow,
		newRow:    existing,
	})

	return nil
}

// findInSlice returns the index and true if needle is found in the slice.
func findInSlice(slice []string, needle string) (int, bool) {
	for i, s := range slice {
		if s == needle {
			return i, true
		}
	}
	return -1, false
}
