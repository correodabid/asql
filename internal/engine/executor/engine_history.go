package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/correodabid/asql/internal/engine/domains"
	"github.com/correodabid/asql/internal/engine/parser"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
)

// TimeTravelQueryAsOfLSN evaluates a SELECT query against state at targetLSN.
// When targetLSN matches the engine's current head LSN, the query is served
// directly from the in-memory materialized state — skipping WAL replay.
func (engine *Engine) TimeTravelQueryAsOfLSN(ctx context.Context, sql string, txDomains []string, targetLSN uint64) (Result, error) {
	readStart := time.Now()

	imports, selectSQL, err := parser.ExtractImports(sql)
	if err != nil {
		return Result{}, fmt.Errorf("extract imports: %w", err)
	}

	statement, err := parser.Parse(selectSQL)
	if err != nil {
		return Result{}, fmt.Errorf("parse time travel sql %q: %w", selectSQL, err)
	}

	txDomains = engine.expandDomainsForVFKJoins(statement, txDomains)

	plan, err := planner.BuildForDomains(statement, txDomains)
	if err != nil {
		return Result{}, fmt.Errorf("plan time travel sql: %w", err)
	}

	if plan.Operation != planner.OperationSelect && plan.Operation != planner.OperationSetOp {
		return Result{}, errors.New("time travel query only supports SELECT")
	}

	// Fast path: query at current head LSN — use main engine's materialized state.
	state := engine.readState.Load()
	currentHead := state.headLSN

	if targetLSN >= currentHead && currentHead > 0 {
		if len(imports) > 0 {
			state, err = applyImports(state, imports)
			if err != nil {
				return Result{}, err
			}
		}
		rows, err := engine.executeQueryPlan(ctx, state, plan)
		if err != nil {
			return Result{}, err
		}
		engine.perf.recordRead(time.Since(readStart))
		return Result{Status: "OK", Rows: rows}, nil
	}

	tempState, temp, err := engine.historicalStateAt(ctx, targetLSN)
	if err != nil {
		return Result{}, err
	}
	if len(imports) > 0 {
		tempState, err = applyImports(tempState, imports)
		if err != nil {
			return Result{}, err
		}
	}
	if temp == nil {
		// A cached state runs on the engine itself, as before.
		temp = engine
	}
	rows, err := temp.executeQueryPlan(ctx, tempState, plan)
	if err != nil {
		return Result{}, err
	}
	if temp != engine {
		engine.mergeScanStrategyCounts(temp.scanStrategySnapshot())
	}

	engine.perf.recordRead(time.Since(readStart))
	return Result{Status: "OK", Rows: rows}, nil
}

// historicalStateAt returns the engine state at targetLSN, from the cache
// of historical states or rebuilt from the closest snapshot and the WAL.
// temp is the scratch engine that rebuilt it, nil for a cached state.
func (engine *Engine) historicalStateAt(ctx context.Context, targetLSN uint64) (state *readableState, temp *Engine, err error) {
	if cachedState, ok := engine.cachedHistoricalState(targetLSN); ok {
		return cachedState, nil, nil
	}

	// Slow path: historical query — restore from nearest snapshot + partial replay.
	records, err := engine.readAllRecords(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("time travel read wal: %w", err)
	}

	temp = &Engine{
		catalog:   domains.NewCatalog(),
		scanStats: make(map[scanStrategy]uint64),
		logStore:  engine.logStore,
		snapshots: engine.snapshots,
	}
	tempInitial := &readableState{domains: make(map[string]*domainState)}
	temp.readState.Store(tempInitial)

	// Try to restore from the closest snapshot to avoid full replay.
	snap := engine.closestSnapshot(targetLSN)

	if snap == nil {
		// No snapshot at or before targetLSN. Check if the WAL actually has
		// records that cover this range before deciding on a fallback.
		//
		// When the WAL is intact (retainWAL=true), records from LSN 1 are
		// present and a full replay from LSN 0 is correct — no fallback needed.
		//
		// When the WAL has been truncated (retainWAL=false + WAL GC), all
		// remaining records have LSN > targetLSN. In that case the exact
		// state at targetLSN can no longer be reconstructed; fall back to
		// the oldest available snapshot (schema + rows at checkpoint LSN)
		// so that at minimum "table not found" errors are avoided and the
		// caller gets the closest approximation.
		walCoversTarget := false
		for _, r := range records {
			if r.LSN <= targetLSN {
				walCoversTarget = true
				break
			}
		}
		if !walCoversTarget {
			snap = engine.oldestAvailableSnapshot()
			if snap != nil {
				slog.Warn("time travel: WAL truncated before targetLSN; returning oldest available snapshot state",
					"target_lsn", targetLSN, "fallback_lsn", snap.lsn)
			}
		}
	}

	startFromLSN := uint64(0)
	if snap != nil {
		temp.restoreSnapshot(snap)
		startFromLSN = snap.lsn
	}

	// Replay WAL records in (startFromLSN, targetLSN].
	// When startFromLSN > targetLSN (fallback snapshot is newer than targetLSN
	// because WAL was truncated), rebuildFromRecordsPartial applies no records
	// and the caller receives the fallback snapshot state.
	if err := temp.rebuildFromRecordsPartial(records, startFromLSN, targetLSN); err != nil {
		return nil, nil, err
	}

	tempState := temp.readState.Load()
	engine.storeHistoricalState(targetLSN, tempState)
	return tempState, temp, nil
}

// TimeTravelQueryAsOfLSNAsPrincipal evaluates a SELECT query at targetLSN only
// when the authenticated durable principal is allowed current read access and
// explicit temporal-read access.
func (engine *Engine) TimeTravelQueryAsOfLSNAsPrincipal(ctx context.Context, sql string, txDomains []string, targetLSN uint64, principal string) (Result, error) {
	if _, err := engine.AuthorizeSQL(principal, sql, txDomains); err != nil {
		return Result{}, err
	}
	if err := engine.AuthorizeHistoricalRead(principal); err != nil {
		return Result{}, err
	}
	return engine.TimeTravelQueryAsOfLSN(ctx, sql, txDomains, targetLSN)
}

// RowHistory returns the full change history of rows matching a FOR HISTORY query.
// It reads from the in-memory changeLog on the table — O(changeLog) linear scan.
func (engine *Engine) RowHistory(ctx context.Context, sql string, txDomains []string) (Result, error) {
	historyStart := time.Now()

	imports, selectSQL, err := parser.ExtractImports(sql)
	if err != nil {
		return Result{}, fmt.Errorf("extract imports: %w", err)
	}

	statement, err := parser.Parse(selectSQL)
	if err != nil {
		return Result{}, fmt.Errorf("parse history sql %q: %w", selectSQL, err)
	}

	txDomains = engine.expandDomainsForVFKJoins(statement, txDomains)

	plan, err := planner.BuildForDomains(statement, txDomains)
	if err != nil {
		return Result{}, fmt.Errorf("plan history sql: %w", err)
	}

	if plan.Operation != planner.OperationSelect || !plan.ForHistory {
		return Result{}, errors.New("RowHistory requires a SELECT ... FOR HISTORY query")
	}

	// Fast path: audit store is wired — read directly without WAL replay.
	if engine.auditStore != nil {
		return engine.rowHistoryFromAuditStore(ctx, &plan, imports)
	}

	state := engine.readState.Load()
	if len(imports) > 0 {
		state, err = applyImports(state, imports)
		if err != nil {
			return Result{}, err
		}
	}

	ds, ok := state.domains[plan.DomainName]
	if !ok {
		return Result{}, errTableNotFound
	}
	table, ok := ds.tables[plan.TableName]
	if !ok {
		return Result{}, errTableNotFound
	}

	if plan.Filter, err = coercePredicate(plan.Filter, selectColumnType(state, plan)); err != nil {
		return Result{}, err
	}
	prefilter := historyPrefilter(&plan)
	var historyRows []map[string]ast.Literal
	for _, entry := range table.changeLog {
		// The row after the change, or before it for a DELETE.
		row := entry.newRow
		if entry.operation == "DELETE" {
			row = entry.oldRow
		}
		if row == nil || (prefilter != nil && !matchPredicate(row, prefilter, state, engine)) {
			continue
		}
		historyRows = append(historyRows, buildHistoryResultRow(row, entry.commitLSN, entry.operation))
	}

	rows, err := engine.selectHistory(ctx, state, plan, historyRows)
	if err != nil {
		return Result{}, err
	}
	engine.perf.recordTimeTravel(time.Since(historyStart))
	return Result{Status: "OK", Rows: rows}, nil
}

// RowHistoryAsPrincipal returns row history only when the authenticated durable
// principal holds the explicit temporal-read privilege.
func (engine *Engine) RowHistoryAsPrincipal(ctx context.Context, sql string, txDomains []string, principal string) (Result, error) {
	if _, err := engine.AuthorizeSQL(principal, sql, txDomains); err != nil {
		return Result{}, err
	}
	if err := engine.AuthorizeHistoricalRead(principal); err != nil {
		return Result{}, err
	}
	return engine.RowHistory(ctx, sql, txDomains)
}

// EntityVersionHistory returns the version history for an entity aggregate.
// If rootPK is non-empty, returns versions for that specific root PK.
// If rootPK is empty, returns all versions across all root PKs sorted by commitLSN.
func (engine *Engine) EntityVersionHistory(ctx context.Context, domain string, entityName string, rootPK string) ([]EntityVersionHistoryEntry, error) {
	state := engine.readState.Load()
	domainState, ok := state.domains[strings.ToLower(strings.TrimSpace(domain))]
	if !ok {
		return nil, fmt.Errorf("domain %q not found", domain)
	}

	entityName = strings.ToLower(strings.TrimSpace(entityName))
	if domainState.entityVersions == nil {
		return nil, fmt.Errorf("entity %q not found in domain %q", entityName, domain)
	}
	idx, ok := domainState.entityVersions[entityName]
	if !ok {
		return nil, fmt.Errorf("entity %q not found in domain %q", entityName, domain)
	}

	var result []EntityVersionHistoryEntry
	if rootPK != "" {
		versions := idx.getVersions(rootPK)
		if versions == nil {
			// Try with literalKey prefixes — the internal index uses
			// literalKey() format (e.g. "n:1" for numbers, "s:foo" for strings).
			for _, prefix := range []string{"n:", "s:", "f:", "b:", "t:"} {
				if v := idx.getVersions(prefix + rootPK); v != nil {
					versions = v
					break
				}
			}
		}
		for _, v := range versions {
			tables := make([]string, len(v.tables))
			copy(tables, v.tables)
			result = append(result, EntityVersionHistoryEntry{
				Version: v.version, CommitLSN: v.commitLSN, Tables: tables,
			})
		}
	} else {
		// Browse all entity versions: flatten overlay chain first for complete view.
		flat := flattenEntityVersionIndex(idx)
		for _, versions := range flat.versions {
			for _, v := range versions {
				tables := make([]string, len(v.tables))
				copy(tables, v.tables)
				result = append(result, EntityVersionHistoryEntry{
					Version: v.version, CommitLSN: v.commitLSN, Tables: tables,
				})
			}
		}
		sort.Slice(result, func(i, j int) bool { return result[i].CommitLSN < result[j].CommitLSN })
	}
	return result, nil
}

// EntityVersionHistoryAsPrincipal returns entity-version history only when the
// authenticated durable principal holds the explicit temporal-read privilege.
func (engine *Engine) EntityVersionHistoryAsPrincipal(ctx context.Context, domain string, entityName string, rootPK string, principal string) ([]EntityVersionHistoryEntry, error) {
	if err := engine.AuthorizeHistoricalRead(principal); err != nil {
		return nil, err
	}
	return engine.EntityVersionHistory(ctx, domain, entityName, rootPK)
}

func (engine *Engine) recordScanStrategy(strategy scanStrategy) {
	if engine == nil {
		return
	}

	engine.statsMu.Lock()
	defer engine.statsMu.Unlock()
	if engine.scanStats == nil {
		engine.scanStats = make(map[scanStrategy]uint64)
	}
	engine.scanStats[strategy]++
}

func (engine *Engine) scanStrategySnapshot() map[scanStrategy]uint64 {
	if engine == nil {
		return nil
	}

	engine.statsMu.Lock()
	defer engine.statsMu.Unlock()
	snapshot := make(map[scanStrategy]uint64, len(engine.scanStats))
	for key, value := range engine.scanStats {
		snapshot[key] = value
	}
	return snapshot
}

func (engine *Engine) mergeScanStrategyCounts(source map[scanStrategy]uint64) {
	if engine == nil || len(source) == 0 {
		return
	}

	engine.statsMu.Lock()
	defer engine.statsMu.Unlock()
	if engine.scanStats == nil {
		engine.scanStats = make(map[scanStrategy]uint64)
	}
	for key, value := range source {
		engine.scanStats[key] += value
	}
}

// ScanStrategyCounts returns a snapshot of scan strategy counters.
func (engine *Engine) ScanStrategyCounts() map[string]uint64 {
	if engine == nil {
		return nil
	}

	engine.statsMu.Lock()
	defer engine.statsMu.Unlock()
	result := make(map[string]uint64, len(engine.scanStats))
	for key, value := range engine.scanStats {
		result[string(key)] = value
	}
	return result
}

// TimeTravelQueryAsOfTimestamp evaluates a SELECT query at timestamp boundary.
func (engine *Engine) TimeTravelQueryAsOfTimestamp(ctx context.Context, sql string, txDomains []string, logicalTimestamp uint64) (Result, error) {
	resolvedLSN, err := engine.LSNForTimestamp(ctx, logicalTimestamp)
	if err != nil {
		return Result{}, err
	}

	return engine.TimeTravelQueryAsOfLSN(ctx, sql, txDomains, resolvedLSN)
}

// TimeTravelQueryAsOfTimestampAsPrincipal evaluates a timestamp-boundary query
// only when the authenticated durable principal is allowed current read access
// and explicit temporal-read access.
func (engine *Engine) TimeTravelQueryAsOfTimestampAsPrincipal(ctx context.Context, sql string, txDomains []string, logicalTimestamp uint64, principal string) (Result, error) {
	if _, err := engine.AuthorizeSQL(principal, sql, txDomains); err != nil {
		return Result{}, err
	}
	if err := engine.AuthorizeHistoricalRead(principal); err != nil {
		return Result{}, err
	}
	return engine.TimeTravelQueryAsOfTimestamp(ctx, sql, txDomains, logicalTimestamp)
}

// rowHistoryFromAuditStore serves a FOR HISTORY query from the audit store,
// which holds every committed row change in commit order (see
// history_log.go). It first waits for the history of everything committed
// so far, so a query sees its own writes.
func (engine *Engine) rowHistoryFromAuditStore(ctx context.Context, plan *planner.Plan, imports []ast.ImportDirective) (Result, error) {
	if err := engine.drainHistory(ctx); err != nil {
		return Result{}, err
	}
	entries, err := engine.auditStore.ReadTable(ctx, plan.DomainName, plan.TableName)
	if err != nil {
		return Result{}, fmt.Errorf("audit store read: %w", err)
	}

	state := engine.readState.Load()
	if len(imports) > 0 {
		state, err = applyImports(state, imports)
		if err != nil {
			return Result{}, err
		}
	}
	// A dropped table keeps its history; one that never existed has none.
	if len(entries) == 0 {
		if ds, ok := state.domains[plan.DomainName]; !ok || ds.tables[plan.TableName] == nil {
			return Result{}, errTableNotFound
		}
	}

	selectPlan := *plan
	if selectPlan.Filter, err = coercePredicate(selectPlan.Filter, selectColumnType(state, selectPlan)); err != nil {
		return Result{}, err
	}
	prefilter := historyPrefilter(&selectPlan)
	var historyRows []map[string]ast.Literal
	for _, e := range entries {
		// The row after the change, or before it for a DELETE.
		row := e.NewRow
		if e.Operation == "DELETE" {
			row = e.OldRow
		}
		if row == nil || (prefilter != nil && !matchPredicate(row, prefilter, state, engine)) {
			continue
		}
		historyRows = append(historyRows, buildHistoryResultRow(row, e.CommitLSN, e.Operation))
	}

	rows, err := engine.selectHistory(ctx, state, selectPlan, historyRows)
	if err != nil {
		return Result{}, err
	}
	return Result{Status: "OK", Rows: rows}, nil
}

// historyPrefilter moves a WHERE that does not test the history columns out
// of plan, so it can run on each version's row before that row is copied.
// It returns nil when the WHERE stays in plan.
func historyPrefilter(plan *planner.Plan) *ast.Predicate {
	filter := plan.Filter
	if filter == nil || predicateMayReadHistoryColumns(filter) {
		return nil
	}
	plan.Filter = nil
	return filter
}

// predicateMayReadHistoryColumns reports whether p refers to __operation or
// __commit_lsn, or might (a subquery).
func predicateMayReadHistoryColumns(p *ast.Predicate) bool {
	if p == nil {
		return false
	}
	if p.Subquery != nil {
		return true
	}
	for _, column := range []string{p.Column, p.RightColumn} {
		if strings.Contains(column, HistoryOperationColumnName) || strings.Contains(column, HistoryCommitLSNColumnName) {
			return true
		}
	}
	return predicateMayReadHistoryColumns(p.Left) || predicateMayReadHistoryColumns(p.Right)
}

// selectHistory runs a FOR HISTORY query's SELECT over its history rows, in
// commit order, as over a table: WHERE (which can test __operation and
// __commit_lsn), the select list, aggregates, GROUP BY, ORDER BY, OFFSET and
// LIMIT all apply.
func (engine *Engine) selectHistory(ctx context.Context, state *readableState, plan planner.Plan, historyRows []map[string]ast.Literal) ([]map[string]ast.Literal, error) {
	columnSet := make(map[string]struct{})
	for _, row := range historyRows {
		for column := range row {
			columnSet[column] = struct{}{}
		}
	}
	columnSet[HistoryOperationColumnName] = struct{}{}
	columnSet[HistoryCommitLSNColumnName] = struct{}{}
	columns := make([]string, 0, len(columnSet))
	for column := range columnSet {
		columns = append(columns, column)
	}
	sort.Strings(columns)

	history := &tableState{
		columns:           columns,
		columnDefinitions: make(map[string]ast.ColumnDefinition),
		rows:              make([][]ast.Literal, len(historyRows)),
		columnIndex:       make(map[string]int, len(columns)),
		indexes:           make(map[string]*indexState),
		indexedColumns:    make(map[string]string),
		indexedColumnSets: make(map[string]string),
		uniqueColumns:     make(map[string]struct{}),
	}
	for i, column := range columns {
		history.columnIndex[column] = i
	}
	// Typed like the table, so the WHERE's quoted literals are coerced.
	if original, ok := state.domains[plan.DomainName]; ok {
		if table, ok := original.tables[plan.TableName]; ok {
			for name, def := range table.columnDefinitions {
				history.columnDefinitions[name] = def
			}
		}
	}
	history.columnDefinitions[HistoryCommitLSNColumnName] = ast.ColumnDefinition{Name: HistoryCommitLSNColumnName, Type: ast.DataTypeInt}
	history.columnDefinitions[HistoryOperationColumnName] = ast.ColumnDefinition{Name: HistoryOperationColumnName, Type: ast.DataTypeText}
	for i, row := range historyRows {
		history.rows[i] = rowFromMap(columns, row)
	}

	// A copy of the state where the table is its history.
	historyState := &readableState{domains: make(map[string]*domainState, len(state.domains)), principals: state.principals, headLSN: state.headLSN, logicalTS: state.logicalTS}
	for name, domain := range state.domains {
		historyState.domains[name] = domain
	}
	domain := &domainState{tables: make(map[string]*tableState)}
	if original, ok := state.domains[plan.DomainName]; ok {
		for name, table := range original.tables {
			domain.tables[name] = table
		}
	}
	domain.tables[plan.TableName] = history
	historyState.domains[plan.DomainName] = domain

	plan.ForHistory = false
	rows, err := engine.selectRows(ctx, historyState, plan)
	if err != nil {
		return nil, fmt.Errorf("history query: %w", err)
	}
	return rows, nil
}
