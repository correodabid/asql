package executor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"asql/internal/engine/domains"
	"asql/internal/engine/parser"
	"asql/internal/engine/parser/ast"
	"asql/internal/engine/planner"
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

	// Slow path: historical query — restore from nearest snapshot + partial replay.
	records, err := engine.readAllRecords(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("time travel read wal: %w", err)
	}

	temp := &Engine{
		catalog:          domains.NewCatalog(),
		scanStats:        make(map[scanStrategy]uint64),
		vfkSubscriptions: make(map[string][]projectionSubscription),
		logStore:         engine.logStore,
		snapshots:        engine.snapshots,
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
		return Result{}, err
	}

	tempState := temp.readState.Load()
	if len(imports) > 0 {
		tempState, err = applyImports(tempState, imports)
		if err != nil {
			return Result{}, err
		}
	}
	rows, err := temp.executeQueryPlan(ctx, tempState, plan)
	tempStats := temp.scanStrategySnapshot()
	if err != nil {
		return Result{}, err
	}

	engine.mergeScanStrategyCounts(tempStats)

	engine.perf.recordRead(time.Since(readStart))
	return Result{Status: "OK", Rows: rows}, nil
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
		return Result{Status: "OK"}, nil
	}
	table, ok := ds.tables[plan.TableName]
	if !ok {
		return Result{Status: "OK"}, nil
	}

	var historyRows []map[string]ast.Literal
	for _, entry := range table.changeLog {
		// Use newRow for INSERT/UPDATE, oldRow for DELETE when matching WHERE.
		matchRow := entry.newRow
		if entry.operation == "DELETE" {
			matchRow = entry.oldRow
		}
		if matchRow == nil {
			continue
		}

		if plan.Filter != nil && !matchPredicate(matchRow, plan.Filter, state, engine) {
			continue
		}

		resultRow := cloneRow(matchRow)
		resultRow["_lsn"] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(entry.commitLSN)}
		resultRow["_operation"] = ast.Literal{Kind: ast.LiteralString, StringValue: entry.operation}
		historyRows = append(historyRows, resultRow)
	}

	engine.perf.recordTimeTravel(time.Since(historyStart))
	return Result{Status: "OK", Rows: historyRows}, nil
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

// rowHistoryFromAuditStore serves a FOR HISTORY query from the persistent audit
// log rather than the WAL-based changeLog. This path is active whenever an
// AuditStore is wired into the engine, and it remains correct even after WAL GC.
//
// Behaviour:
//   - Reads all audit entries and filters by domain/table/predicate.
//   - INSERT entries are deduplicated by CommitLSN: the lazy-anchoring write
//     path may re-add the INSERT after an engine restart, so duplicates are
//     possible in the persistent store. Deduplication is O(n) via a seen-LSN map.
//   - Results are sorted chronologically by CommitLSN.
//   - If no audit entries match (row is insert-only, never mutated), falls back
//     to the in-memory changeLog so the INSERT is still visible to the caller.
func (engine *Engine) rowHistoryFromAuditStore(ctx context.Context, plan *planner.Plan, imports []ast.ImportDirective) (Result, error) {
	entries, err := engine.auditStore.ReadAll(ctx)
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

	targetDomain := plan.DomainName
	targetTable := plan.TableName

	// seenInsertLSN deduplicates INSERT entries: the lazy-anchor write path
	// may write the same INSERT multiple times across engine restarts.
	seenInsertLSN := make(map[uint64]struct{})
	var historyRows []map[string]ast.Literal
	for _, e := range entries {
		if e.Table != targetTable {
			continue
		}
		if targetDomain != "" && e.Domain != targetDomain {
			continue
		}

		// Deduplicate INSERT entries written by the lazy-anchoring path.
		if e.Operation == "INSERT" {
			if _, dup := seenInsertLSN[e.CommitLSN]; dup {
				continue
			}
			seenInsertLSN[e.CommitLSN] = struct{}{}
		}

		// Use newRow for INSERT/UPDATE, oldRow for DELETE when matching WHERE.
		matchRow := e.NewRow
		if e.Operation == "DELETE" {
			matchRow = e.OldRow
		}
		if matchRow == nil {
			continue
		}

		if plan.Filter != nil && !matchPredicate(matchRow, plan.Filter, state, engine) {
			continue
		}

		resultRow := cloneRow(matchRow)
		resultRow["_lsn"] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(e.CommitLSN)}
		resultRow["_operation"] = ast.Literal{Kind: ast.LiteralString, StringValue: e.Operation}
		historyRows = append(historyRows, resultRow)
	}

	// Fallback for insert-only rows: not yet in audit (INSERT anchoring happens
	// only on first UPDATE/DELETE). Serve directly from the in-memory changeLog.
	if len(historyRows) == 0 {
		ds, ok := state.domains[targetDomain]
		if ok {
			tbl, ok := ds.tables[targetTable]
			if ok {
				for _, entry := range tbl.changeLog {
					matchRow := entry.newRow
					if entry.operation == "DELETE" {
						matchRow = entry.oldRow
					}
					if matchRow == nil {
						continue
					}
					if plan.Filter != nil && !matchPredicate(matchRow, plan.Filter, state, engine) {
						continue
					}
					resultRow := cloneRow(matchRow)
					resultRow["_lsn"] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(entry.commitLSN)}
					resultRow["_operation"] = ast.Literal{Kind: ast.LiteralString, StringValue: entry.operation}
					historyRows = append(historyRows, resultRow)
				}
			}
		}
	}

	// Sort chronologically — lazy-anchored INSERT entries land after the
	// UPDATE/DELETE that triggered them in the slice, reorder here.
	sort.Slice(historyRows, func(i, j int) bool {
		return historyRows[i]["_lsn"].NumberValue < historyRows[j]["_lsn"].NumberValue
	})

	if plan.Limit != nil && len(historyRows) > *plan.Limit {
		historyRows = historyRows[:*plan.Limit]
	}

	return Result{Status: "OK", Rows: historyRows}, nil
}
