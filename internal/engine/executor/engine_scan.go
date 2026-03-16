package executor

import (
	"sort"
	"strings"

	"asql/internal/engine/parser/ast"
	"asql/internal/engine/planner"
)

// Scan cost estimation constants. These factors tune the cost model
// that chooses between full-scan, hash-lookup, and btree strategies.
const (
	// sortCostFactor is the additional cost ratio for sorting results (N/2).
	sortCostFactor = 2

	// hashHighSelectivityThreshold is the percentage (0–100) at which a hash
	// lookup is considered too broad; an overhead penalty is added.
	hashHighSelectivityThreshold = 90

	// hashOverheadDivisor scales the penalty when hash selectivity is too broad.
	hashOverheadDivisor = 4

	// btreeHighSelectivityThreshold is the percentage (0–100) at which a btree
	// lookup is considered too broad; an overhead penalty is added.
	btreeHighSelectivityThreshold = 95

	// btreeOverheadDivisor scales the penalty when btree selectivity is too broad.
	btreeOverheadDivisor = 5

	// btreePrefixMergeOverhead is the divisor for extra merge cost in prefix scans.
	btreePrefixMergeOverhead = 3

	// scanPriorityFullScan is the tiebreaker priority for full table scans.
	scanPriorityFullScan = 40

	// scanPriorityHashLookup is the tiebreaker priority for hash index lookups.
	scanPriorityHashLookup = 10

	// scanPriorityBTreeLookup is the tiebreaker priority for btree equality lookups.
	scanPriorityBTreeLookup = 15

	// scanPriorityBTreeOrder is the tiebreaker priority for btree-ordered scans.
	scanPriorityBTreeOrder = 20

	// scanPriorityBTreePrefix is the tiebreaker priority for btree prefix scans.
	scanPriorityBTreePrefix = 30
)

func chooseSingleTableScanStrategy(table *tableState, predicate *ast.Predicate, orderBy []ast.OrderByClause) scanStrategy {
	if table == nil {
		return scanStrategyFullScan
	}

	if !supportsIndexSelection(predicate) {
		return scanStrategyFullScan
	}

	totalRows := len(table.rows)
	candidates := make([]scanCostEstimate, 0, 4)
	candidates = append(candidates, estimateFullScanCost(totalRows, orderBy))
	predicateCandidates := collectIndexablePredicates(predicate)

	if len(orderBy) == 1 {
		if index, ok := indexForColumn(table, orderBy[0].Column); ok && index.kind == "btree" {
			candidates = append(candidates, estimateBTreeOrderCost(index, bestPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
	}

	if len(orderBy) > 1 {
		if _, _, ok := compositeBTreeIndexForOrder(table, orderBy); ok {
			candidates = append(candidates, estimateCompositeBTreeOrderCost(table, bestPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
		if index, ok := indexForColumn(table, orderBy[0].Column); ok && index.kind == "btree" {
			candidates = append(candidates, estimateBTreePrefixCost(index, bestPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
	}

	for _, candidate := range predicateCandidates {
		if index, ok := indexForColumn(table, candidate.Column); ok && index.kind == "hash" {
			if estimate, ok := estimateHashLookupCost(index, candidate, totalRows, len(orderBy) > 0); ok {
				candidates = append(candidates, estimate)
			}
		}

		if index, ok := indexForColumn(table, candidate.Column); ok && index.kind == "btree" {
			if estimate, ok := estimateBTreeLookupCost(index, candidate, totalRows, len(orderBy) > 0); ok {
				candidates = append(candidates, estimate)
			}
		}
	}

	return pickBestScanStrategy(candidates)
}

func supportsIndexSelection(predicate *ast.Predicate) bool {
	if predicate == nil {
		return true
	}

	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "AND":
		return supportsIndexSelection(predicate.Left) && supportsIndexSelection(predicate.Right)
	case "OR", "NOT":
		return false
	default:
		return isSimplePredicate(predicate)
	}
}

func collectIndexablePredicates(predicate *ast.Predicate) []*ast.Predicate {
	if predicate == nil {
		return nil
	}

	if strings.EqualFold(strings.TrimSpace(predicate.Operator), "AND") {
		left := collectIndexablePredicates(predicate.Left)
		right := collectIndexablePredicates(predicate.Right)
		return append(left, right...)
	}

	if isSimplePredicate(predicate) {
		return []*ast.Predicate{predicate}
	}

	return nil
}

func bestPredicateForColumn(table *tableState, predicates []*ast.Predicate, column string) *ast.Predicate {
	if table == nil || len(predicates) == 0 {
		return nil
	}

	canonical := strings.ToLower(strings.TrimSpace(column))
	var best *ast.Predicate
	bestRows := len(table.rows)
	for _, predicate := range predicates {
		if predicate == nil || strings.ToLower(strings.TrimSpace(predicate.Column)) != canonical {
			continue
		}
		index, ok := indexForColumn(table, predicate.Column)
		if !ok {
			continue
		}
		rows, ok := estimateRowsByIndexPredicate(index, predicate, len(table.rows))
		if !ok {
			continue
		}
		if best == nil || rows < bestRows {
			best = predicate
			bestRows = rows
		}
	}
	return best
}

func bestLookupPredicate(table *tableState, predicate *ast.Predicate, hasOrderBy bool) (*ast.Predicate, scanStrategy, bool) {
	if table == nil || !supportsIndexSelection(predicate) {
		return nil, scanStrategyFullScan, false
	}

	totalRows := len(table.rows)
	var bestPredicate *ast.Predicate
	var bestEstimate scanCostEstimate
	haveBest := false
	for _, candidate := range collectIndexablePredicates(predicate) {
		if index, ok := indexForColumn(table, candidate.Column); ok && index.kind == "hash" {
			if estimate, ok := estimateHashLookupCost(index, candidate, totalRows, hasOrderBy); ok {
				if !haveBest || estimate.cost < bestEstimate.cost || (estimate.cost == bestEstimate.cost && estimate.priority < bestEstimate.priority) {
					bestPredicate = candidate
					bestEstimate = estimate
					haveBest = true
				}
			}
		}
		if index, ok := indexForColumn(table, candidate.Column); ok && index.kind == "btree" {
			if estimate, ok := estimateBTreeLookupCost(index, candidate, totalRows, hasOrderBy); ok {
				if !haveBest || estimate.cost < bestEstimate.cost || (estimate.cost == bestEstimate.cost && estimate.priority < bestEstimate.priority) {
					bestPredicate = candidate
					bestEstimate = estimate
					haveBest = true
				}
			}
		}
	}

	if !haveBest {
		return nil, scanStrategyFullScan, false
	}

	return bestPredicate, bestEstimate.strategy, true
}

func candidateRowIDsForPredicate(table *tableState, predicate *ast.Predicate, hasOrderBy bool) ([]int, *ast.Predicate, scanStrategy, bool) {
	lookupPredicate, strategy, ok := bestLookupPredicate(table, predicate, hasOrderBy)
	if !ok {
		return nil, nil, scanStrategyFullScan, false
	}

	index, ok := indexForColumn(table, lookupPredicate.Column)
	if !ok {
		return nil, nil, scanStrategyFullScan, false
	}

	rowIDs := rowIDsForPredicate(index, lookupPredicate)
	if len(rowIDs) == 0 {
		return nil, lookupPredicate, strategy, true
	}

	return rowIDs, lookupPredicate, strategy, true
}

func estimateFullScanCost(totalRows int, orderBy []ast.OrderByClause) scanCostEstimate {
	if totalRows < 0 {
		totalRows = 0
	}

	cost := totalRows
	if len(orderBy) > 0 {
		cost += totalRows / sortCostFactor
	}

	return scanCostEstimate{strategy: scanStrategyFullScan, cost: cost, priority: scanPriorityFullScan}
}

func estimateHashLookupCost(index *indexState, predicate *ast.Predicate, totalRows int, hasOrderBy bool) (scanCostEstimate, bool) {
	if index == nil || predicate == nil {
		return scanCostEstimate{}, false
	}

	estimatedRows, ok := estimateRowsByIndexPredicate(index, predicate, totalRows)
	if !ok {
		return scanCostEstimate{}, false
	}

	cost := estimatedRows
	if hasOrderBy {
		cost += estimatedRows / sortCostFactor
	}

	if totalRows > 0 && estimatedRows*100 >= totalRows*hashHighSelectivityThreshold {
		cost += totalRows / hashOverheadDivisor
	}

	return scanCostEstimate{strategy: scanStrategyHashLookup, cost: cost, priority: scanPriorityHashLookup}, true
}

func estimateBTreeOrderCost(index *indexState, predicate *ast.Predicate, totalRows int) scanCostEstimate {
	estimatedRows := totalRows
	if predicate != nil {
		if rows, ok := estimateRowsByIndexPredicate(index, predicate, totalRows); ok {
			estimatedRows = rows
		}
	}

	return scanCostEstimate{strategy: scanStrategyBTreeOrder, cost: estimatedRows, priority: scanPriorityBTreeOrder}
}

func estimateCompositeBTreeOrderCost(table *tableState, predicate *ast.Predicate, totalRows int) scanCostEstimate {
	estimatedRows := totalRows
	if predicate != nil {
		if index, ok := indexForColumn(table, predicate.Column); ok {
			if rows, ok := estimateRowsByIndexPredicate(index, predicate, totalRows); ok {
				estimatedRows = rows
			}
		}
	}

	return scanCostEstimate{strategy: scanStrategyBTreeOrder, cost: estimatedRows, priority: scanPriorityBTreeOrder}
}

func estimateBTreePrefixCost(index *indexState, predicate *ast.Predicate, totalRows int) scanCostEstimate {
	estimatedRows := totalRows
	if predicate != nil {
		if rows, ok := estimateRowsByIndexPredicate(index, predicate, totalRows); ok {
			estimatedRows = rows
		}
	}

	cost := estimatedRows + estimatedRows/btreePrefixMergeOverhead
	return scanCostEstimate{strategy: scanStrategyBTreePrefix, cost: cost, priority: scanPriorityBTreePrefix}
}

func estimateBTreeLookupCost(index *indexState, predicate *ast.Predicate, totalRows int, hasOrderBy bool) (scanCostEstimate, bool) {
	if index == nil || index.kind != "btree" || predicate == nil {
		return scanCostEstimate{}, false
	}

	estimatedRows, ok := estimateRowsByIndexPredicate(index, predicate, totalRows)
	if !ok {
		return scanCostEstimate{}, false
	}

	cost := estimatedRows
	if hasOrderBy {
		cost += estimatedRows / sortCostFactor
	}

	if totalRows > 0 && estimatedRows*100 >= totalRows*btreeHighSelectivityThreshold {
		cost += totalRows / btreeOverheadDivisor
	}

	return scanCostEstimate{strategy: scanStrategyBTreeLookup, cost: cost, priority: scanPriorityBTreeLookup}, true
}

func estimateRowsByIndexPredicate(index *indexState, predicate *ast.Predicate, totalRows int) (int, bool) {
	if index == nil || predicate == nil {
		return 0, false
	}

	operator := strings.ToUpper(strings.TrimSpace(predicate.Operator))
	switch index.kind {
	case "hash":
		if operator != "=" && operator != "IS NULL" && operator != "IS NOT NULL" {
			return 0, false
		}
	case "btree":
		if operator != "=" && operator != ">" && operator != ">=" && operator != "<" && operator != "<=" && operator != "IS NULL" && operator != "IS NOT NULL" {
			return 0, false
		}
	default:
		return 0, false
	}

	if operator == "=" && predicate.Value.Kind == ast.LiteralNull {
		return 0, true
	}

	rowIDs := rowIDsForPredicate(index, predicate)
	if rowIDs == nil {
		return 0, true
	}

	estimated := len(rowIDs)
	if estimated < 0 {
		estimated = 0
	}
	if totalRows >= 0 && estimated > totalRows {
		estimated = totalRows
	}

	return estimated, true
}

func pickBestScanStrategy(candidates []scanCostEstimate) scanStrategy {
	if len(candidates) == 0 {
		return scanStrategyFullScan
	}

	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.cost < best.cost {
			best = candidate
			continue
		}
		if candidate.cost == best.cost && candidate.priority < best.priority {
			best = candidate
		}
	}

	return best.strategy
}

func rowsForPredicate(table *tableState, predicate *ast.Predicate, state *readableState, engine *Engine) []map[string]ast.Literal {
	if predicate == nil {
		return tableRowsToMaps(table)
	}

	rowIDs, _, _, ok := candidateRowIDsForPredicate(table, predicate, false)
	if !ok {
		return tableRowsToMaps(table)
	}
	if len(rowIDs) == 0 {
		return nil
	}

	rows := make([]map[string]ast.Literal, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		if rowID < 0 || rowID >= len(table.rows) {
			continue
		}
		rows = append(rows, rowToMap(table, table.rows[rowID]))
	}

	return rows
}

func orderedRowsFromBTreeIndex(table *tableState, predicate *ast.Predicate, orderBy []ast.OrderByClause, limit *int, state *readableState, engine *Engine) ([]map[string]ast.Literal, bool) {
	if table == nil || len(orderBy) == 0 {
		return nil, false
	}

	if len(orderBy) > 1 {
		return orderedRowsFromCompositeBTreeIndex(table, predicate, orderBy, limit, state, engine)
	}

	index, ok := indexForColumn(table, orderBy[0].Column)
	if !ok || index.kind != "btree" {
		return nil, false
	}

	boundedPredicate := bestPredicateForColumn(table, collectIndexablePredicates(predicate), orderBy[0].Column)
	useBoundedScan := false
	if boundedPredicate != nil {
		switch boundedPredicate.Operator {
		case "=", ">", ">=", "<", "<=":
			useBoundedScan = true
		}
	}
	postFilter := predicate != nil && (!useBoundedScan || boundedPredicate != predicate)

	entries := index.allEntries()
	rowsCap := len(entries)
	if limit != nil && *limit >= 0 && *limit < rowsCap {
		rowsCap = *limit
	}
	scanStart, scanEnd := 0, len(entries)
	if useBoundedScan {
		scanStart, scanEnd = btreeBoundedScanRange(entries, func(entry indexEntry) ast.Literal {
			return entry.value
		}, boundedPredicate)
	}
	rows := make([]map[string]ast.Literal, 0, rowsCap)
	if orderBy[0].Direction == ast.SortDesc {
		for i := scanEnd - 1; i >= scanStart; i-- {
			if limit != nil && *limit >= 0 && len(rows) >= *limit {
				break
			}

			rowID := entries[i].rowID
			if rowID < 0 || rowID >= len(table.rows) {
				continue
			}
			row := rowToMap(table, table.rows[rowID])
			if postFilter && !matchPredicate(row, predicate, state, engine) {
				continue
			}
			rows = append(rows, row)
		}
		return rows, true
	}

	for i := scanStart; i < scanEnd; i++ {
		if limit != nil && *limit >= 0 && len(rows) >= *limit {
			break
		}

		entry := entries[i]

		if entry.rowID < 0 || entry.rowID >= len(table.rows) {
			continue
		}
		row := rowToMap(table, table.rows[entry.rowID])
		if postFilter && !matchPredicate(row, predicate, state, engine) {
			continue
		}
		rows = append(rows, row)
	}

	return rows, true
}

func orderedRowsFromCompositeBTreeIndex(table *tableState, predicate *ast.Predicate, orderBy []ast.OrderByClause, limit *int, state *readableState, engine *Engine) ([]map[string]ast.Literal, bool) {
	index, descending, ok := compositeBTreeIndexForOrder(table, orderBy)
	if !ok {
		return nil, false
	}

	entries := index.allEntries()
	rows := make([]map[string]ast.Literal, 0, len(entries))
	if descending {
		for i := len(entries) - 1; i >= 0; i-- {
			if limit != nil && *limit >= 0 && len(rows) >= *limit {
				break
			}
			rowID := entries[i].rowID
			if rowID < 0 || rowID >= len(table.rows) {
				continue
			}
			row := rowToMap(table, table.rows[rowID])
			if !matchPredicate(row, predicate, state, engine) {
				continue
			}
			rows = append(rows, row)
		}
		return rows, true
	}

	for _, entry := range entries {
		if limit != nil && *limit >= 0 && len(rows) >= *limit {
			break
		}
		if entry.rowID < 0 || entry.rowID >= len(table.rows) {
			continue
		}
		row := rowToMap(table, table.rows[entry.rowID])
		if !matchPredicate(row, predicate, state, engine) {
			continue
		}
		rows = append(rows, row)
	}

	return rows, true
}

func compositeBTreeIndexForOrder(table *tableState, orderBy []ast.OrderByClause) (*indexState, bool, bool) {
	if table == nil || len(orderBy) < 2 {
		return nil, false, false
	}

	firstDirection := orderBy[0].Direction
	if firstDirection != ast.SortAsc && firstDirection != ast.SortDesc {
		return nil, false, false
	}

	columns := make([]string, 0, len(orderBy))
	for _, clause := range orderBy {
		if clause.Direction != firstDirection {
			return nil, false, false
		}
		canonical := strings.TrimSpace(strings.ToLower(clause.Column))
		if !isSimpleColumnReference(canonical) {
			return nil, false, false
		}
		columns = append(columns, canonical)
	}

	if len(table.indexedColumnSets) == 0 {
		return nil, false, false
	}

	indexName, ok := table.indexedColumnSets[indexColumnSetKey(columns)]
	if !ok {
		return nil, false, false
	}

	index, ok := table.indexes[indexName]
	if !ok || index.kind != "btree" {
		return nil, false, false
	}

	if len(index.columns) != len(columns) {
		return nil, false, false
	}
	for i := range columns {
		if index.columns[i] != columns[i] {
			return nil, false, false
		}
	}

	return index, firstDirection == ast.SortDesc, true
}

func orderedRowsFromBTreePrefix(table *tableState, predicate *ast.Predicate, orderBy []ast.OrderByClause, state *readableState, engine *Engine) ([]map[string]ast.Literal, bool) {
	if len(orderBy) < 2 {
		return nil, false
	}

	first := []ast.OrderByClause{orderBy[0]}
	rows, ok := orderedRowsFromBTreeIndex(table, predicate, first, nil, state, engine)
	if !ok {
		return nil, false
	}

	sortRowsWithinFirstOrderGroups(rows, orderBy[0], orderBy[1:])
	return rows, true
}

func sortRowsWithinFirstOrderGroups(rows []map[string]ast.Literal, firstClause ast.OrderByClause, remaining []ast.OrderByClause) {
	if len(rows) <= 1 || len(remaining) == 0 {
		return
	}

	groupStart := 0
	for groupStart < len(rows) {
		groupEnd := groupStart + 1
		groupValue := valueOrNull(rows[groupStart], firstClause.Column)

		for groupEnd < len(rows) {
			if compareLiterals(valueOrNull(rows[groupEnd], firstClause.Column), groupValue) != 0 {
				break
			}
			groupEnd++
		}

		sort.SliceStable(rows[groupStart:groupEnd], func(i, j int) bool {
			leftRow := rows[groupStart+i]
			rightRow := rows[groupStart+j]

			for _, clause := range remaining {
				left := valueOrNull(leftRow, clause.Column)
				right := valueOrNull(rightRow, clause.Column)

				comparison := compareLiterals(left, right)
				if comparison == 0 {
					continue
				}

				if clause.Direction == ast.SortDesc {
					return comparison > 0
				}

				return comparison < 0
			}

			return false
		})

		groupStart = groupEnd
	}
}

func btreeBoundDecision(cmp int, operator string, descending bool) (accept bool, stop bool) {
	if !descending {
		switch operator {
		case "=":
			if cmp < 0 {
				return false, false
			}
			if cmp > 0 {
				return false, true
			}
			return true, false
		case ">":
			return cmp > 0, false
		case ">=":
			return cmp >= 0, false
		case "<":
			if cmp < 0 {
				return true, false
			}
			return false, true
		case "<=":
			if cmp <= 0 {
				return true, false
			}
			return false, true
		}
		return false, false
	}

	switch operator {
	case "=":
		if cmp > 0 {
			return false, false
		}
		if cmp < 0 {
			return false, true
		}
		return true, false
	case ">":
		if cmp > 0 {
			return true, false
		}
		return false, true
	case ">=":
		if cmp >= 0 {
			return true, false
		}
		return false, true
	case "<":
		return cmp < 0, false
	case "<=":
		return cmp <= 0, false
	}

	return false, false
}

func btreeBoundedScanRange(entries []indexEntry, valueAt func(indexEntry) ast.Literal, predicate *ast.Predicate) (start int, end int) {
	if predicate == nil || len(entries) == 0 {
		return 0, len(entries)
	}

	firstGTE := sort.Search(len(entries), func(i int) bool {
		return compareLiterals(valueAt(entries[i]), predicate.Value) >= 0
	})
	firstGT := sort.Search(len(entries), func(i int) bool {
		return compareLiterals(valueAt(entries[i]), predicate.Value) > 0
	})

	switch predicate.Operator {
	case "=":
		return firstGTE, firstGT
	case ">":
		return firstGT, len(entries)
	case ">=":
		return firstGTE, len(entries)
	case "<":
		return 0, firstGTE
	case "<=":
		return 0, firstGT
	default:
		return 0, len(entries)
	}
}

func joinCandidateRows(table *tableState, tableName string, columnRef string, value ast.Literal) []map[string]ast.Literal {
	column, ok := joinColumnForTable(tableName, columnRef)
	if !ok {
		return tableRowsToMaps(table)
	}
	index, ok := indexForColumn(table, column)
	if !ok {
		return tableRowsToMaps(table)
	}

	rowIDs := rowIDsForEquality(index, value)
	if len(rowIDs) == 0 {
		return nil
	}

	rows := make([]map[string]ast.Literal, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		if rowID < 0 || rowID >= len(table.rows) {
			continue
		}
		rows = append(rows, rowToMap(table, table.rows[rowID]))
	}

	return rows
}

func chooseJoinScanStrategy(leftTable *tableState, leftTableName string, leftColumnRef string, rightTable *tableState, rightTableName string, rightColumnRef string) scanStrategy {
	hasLeftIndex := hasJoinIndex(leftTable, leftTableName, leftColumnRef)
	hasRightIndex := hasJoinIndex(rightTable, rightTableName, rightColumnRef)

	if hasLeftIndex && hasRightIndex {
		leftRows := len(leftTable.rows)
		rightRows := len(rightTable.rows)
		if leftRows == rightRows {
			if strings.ToLower(leftTableName) <= strings.ToLower(rightTableName) {
				return scanStrategyJoinRightIx
			}
			return scanStrategyJoinLeftIx
		}
		if leftRows <= rightRows {
			return scanStrategyJoinRightIx
		}
		return scanStrategyJoinLeftIx
	}

	if hasRightIndex {
		return scanStrategyJoinRightIx
	}

	if hasLeftIndex {
		return scanStrategyJoinLeftIx
	}

	return scanStrategyJoinNested
}

func hasJoinIndex(table *tableState, tableName string, columnRef string) bool {
	column, ok := joinColumnForTable(tableName, columnRef)
	if !ok {
		return false
	}

	_, ok = indexForColumn(table, column)
	return ok
}

func indexForColumn(table *tableState, column string) (*indexState, bool) {
	if table == nil {
		return nil, false
	}

	canonical := strings.TrimSpace(strings.ToLower(column))
	indexName, ok := table.indexedColumns[canonical]
	if !ok {
		return nil, false
	}

	index, ok := table.indexes[indexName]
	if !ok {
		return nil, false
	}

	return index, true
}

func joinColumnForTable(tableName string, columnRef string) (string, bool) {
	reference := strings.TrimSpace(strings.ToLower(columnRef))
	parts := strings.Split(reference, ".")
	if len(parts) != 2 {
		return "", false
	}
	if parts[0] != strings.ToLower(tableName) {
		return "", false
	}
	if parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

type accessPlanInfo struct {
	Strategy      string          `json:"strategy"`
	TableRows     int             `json:"table_rows,omitempty"`
	EstimatedRows int             `json:"estimated_rows,omitempty"`
	IndexUsed     string          `json:"index_used,omitempty"`
	IndexType     string          `json:"index_type,omitempty"`
	IndexColumn   string          `json:"index_column,omitempty"`
	Candidates    []candidateInfo `json:"candidates,omitempty"`
	Joins         []joinPlanInfo  `json:"joins,omitempty"`
}

type candidateInfo struct {
	Strategy string `json:"strategy"`
	Cost     int    `json:"cost"`
	Chosen   bool   `json:"chosen,omitempty"`
}

type joinPlanInfo struct {
	Table     string `json:"table"`
	JoinType  string `json:"join_type"`
	Strategy  string `json:"strategy"`
	TableRows int    `json:"table_rows"`
	IndexUsed string `json:"index_used,omitempty"`
}

// buildAccessPlan computes the runtime access plan for an EXPLAIN query.
// It loads the current readable state (lock-free) and runs the scan
// strategy selection logic without executing the query.
func (engine *Engine) buildAccessPlan(plan planner.Plan) accessPlanInfo {
	state := engine.readState.Load()
	if state == nil {
		return accessPlanInfo{Strategy: string(scanStrategyFullScan)}
	}

	ds, ok := state.domains[plan.DomainName]
	if !ok {
		return accessPlanInfo{Strategy: string(scanStrategyFullScan)}
	}

	table := ds.tables[plan.TableName]

	info := accessPlanInfo{}

	if table != nil {
		info.TableRows = len(table.rows)
	}

	switch plan.Operation {
	case planner.OperationSelect:
		if len(plan.Joins) == 0 {
			info.Strategy, info.Candidates = collectScanCandidates(table, plan.Filter, plan.OrderBy)
		} else {
			info.Strategy = string(scanStrategyJoinNested)
			info.Joins = collectJoinPlans(state, ds, plan)
		}

		// Report index and row estimate for filter column.
		if table != nil {
			if lookupPredicate, _, ok := bestLookupPredicate(table, plan.Filter, len(plan.OrderBy) > 0); ok {
				if idx, ok := indexForColumn(table, lookupPredicate.Column); ok {
					info.IndexUsed = indexNameForColumn(table, lookupPredicate.Column)
					info.IndexType = idx.kind
					info.IndexColumn = lookupPredicate.Column
					if est, ok := estimateRowsByIndexPredicate(idx, lookupPredicate, len(table.rows)); ok {
						info.EstimatedRows = est
					}
				}
			}
		}

	case planner.OperationInsert, planner.OperationUpdate, planner.OperationDelete:
		info.Strategy = string(scanStrategyFullScan)
		if table != nil {
			if lookupPredicate, strat, ok := bestLookupPredicate(table, plan.Filter, false); ok {
				if idx, ok := indexForColumn(table, lookupPredicate.Column); ok {
					info.Strategy = string(strat)
					info.IndexUsed = indexNameForColumn(table, lookupPredicate.Column)
					info.IndexType = idx.kind
					info.IndexColumn = lookupPredicate.Column
					if est, ok := estimateRowsByIndexPredicate(idx, lookupPredicate, len(table.rows)); ok {
						info.EstimatedRows = est
					}
				}
			}
		}

	default:
		info.Strategy = "n/a"
	}

	return info
}

// collectScanCandidates runs the cost model and returns the chosen strategy
// plus all evaluated candidates with their costs.
func collectScanCandidates(table *tableState, predicate *ast.Predicate, orderBy []ast.OrderByClause) (string, []candidateInfo) {
	if table == nil {
		return string(scanStrategyFullScan), nil
	}

	if !supportsIndexSelection(predicate) {
		return string(scanStrategyFullScan), []candidateInfo{{Strategy: string(scanStrategyFullScan), Cost: len(table.rows), Chosen: true}}
	}

	totalRows := len(table.rows)
	candidates := make([]scanCostEstimate, 0, 4)
	candidates = append(candidates, estimateFullScanCost(totalRows, orderBy))
	predicateCandidates := collectIndexablePredicates(predicate)

	if len(orderBy) == 1 {
		if index, ok := indexForColumn(table, orderBy[0].Column); ok && index.kind == "btree" {
			candidates = append(candidates, estimateBTreeOrderCost(index, bestPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
	}

	if len(orderBy) > 1 {
		if _, _, ok := compositeBTreeIndexForOrder(table, orderBy); ok {
			candidates = append(candidates, estimateCompositeBTreeOrderCost(table, bestPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
		if index, ok := indexForColumn(table, orderBy[0].Column); ok && index.kind == "btree" {
			candidates = append(candidates, estimateBTreePrefixCost(index, bestPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
	}

	for _, candidate := range predicateCandidates {
		if index, ok := indexForColumn(table, candidate.Column); ok && index.kind == "hash" {
			if estimate, ok := estimateHashLookupCost(index, candidate, totalRows, len(orderBy) > 0); ok {
				candidates = append(candidates, estimate)
			}
		}
		if index, ok := indexForColumn(table, candidate.Column); ok && index.kind == "btree" {
			if estimate, ok := estimateBTreeLookupCost(index, candidate, totalRows, len(orderBy) > 0); ok {
				candidates = append(candidates, estimate)
			}
		}
	}

	best := pickBestScanStrategy(candidates)

	result := make([]candidateInfo, len(candidates))
	for i, c := range candidates {
		result[i] = candidateInfo{
			Strategy: string(c.strategy),
			Cost:     c.cost,
			Chosen:   c.strategy == best,
		}
	}

	return string(best), result
}

// collectJoinPlans builds access info for each join in the plan.
func collectJoinPlans(state *readableState, baseDomain *domainState, plan planner.Plan) []joinPlanInfo {
	plans := make([]joinPlanInfo, 0, len(plan.Joins))
	for _, j := range plan.Joins {
		joinDS := baseDomain
		if j.DomainName != "" && j.DomainName != plan.DomainName {
			if ds, ok := state.domains[j.DomainName]; ok {
				joinDS = ds
			}
		}

		jp := joinPlanInfo{
			Table:    j.TableName,
			JoinType: string(j.JoinType),
			Strategy: string(scanStrategyJoinNested),
		}

		rightTable := joinDS.tables[j.TableName]
		if rightTable != nil {
			jp.TableRows = len(rightTable.rows)

			if j.RightColumn != "" {
				if col, ok := joinColumnForTable(j.TableName, j.RightColumn); ok {
					if _, hasIdx := indexForColumn(rightTable, col); hasIdx {
						jp.Strategy = string(scanStrategyJoinRightIx)
						jp.IndexUsed = indexNameForColumn(rightTable, col)
					}
				}
			}
		}

		// Check left-side index if right index not found.
		if jp.Strategy == string(scanStrategyJoinNested) && j.LeftColumn != "" {
			baseTable := baseDomain.tables[plan.TableName]
			if baseTable != nil {
				if col, ok := joinColumnForTable(plan.TableName, j.LeftColumn); ok {
					if _, hasIdx := indexForColumn(baseTable, col); hasIdx {
						jp.Strategy = string(scanStrategyJoinLeftIx)
						jp.IndexUsed = indexNameForColumn(baseTable, col)
					}
				}
			}
		}

		plans = append(plans, jp)
	}
	return plans
}

func indexNameForColumn(table *tableState, column string) string {
	if table == nil {
		return ""
	}
	canonical := strings.TrimSpace(strings.ToLower(column))
	return table.indexedColumns[canonical]
}

// ---------- index-only scan ----------

// tryIndexOnlyScan checks whether the query can be served entirely from a btree
// index without accessing table.rows.  Returns the result rows and true when
// all of the following hold:
//   - single table (no joins), no aggregation, no window functions, no DISTINCT
//   - projected columns are an explicit non-wildcard set, without JSON / CASE-WHEN
//   - ORDER BY is present and its leading column is covered by a btree index
//   - every projected column is covered by that btree index
//   - the predicate (if any) only references indexed column(s)
func tryIndexOnlyScan(
	table *tableState,
	plan planner.Plan,
	state *readableState,
	engine *Engine,
) ([]map[string]ast.Literal, bool) {
	if table == nil {
		return nil, false
	}
	// No joins, no aggregation, no window functions.
	if len(plan.Joins) > 0 || requiresAggregation(plan) || len(plan.WindowFunctions) > 0 {
		return nil, false
	}
	// Explicit non-wildcard column list.
	if len(plan.Columns) == 0 || (len(plan.Columns) == 1 && plan.Columns[0] == "*") {
		return nil, false
	}
	// No computed columns — we can only serve raw stored values.
	if len(plan.JsonAccessColumns) > 0 || len(plan.CaseWhenColumns) > 0 {
		return nil, false
	}
	// DISTINCT requires comparing full rows; skip for safety.
	if plan.Distinct {
		return nil, false
	}
	// Need ORDER BY to choose the index.
	if len(plan.OrderBy) == 0 {
		return nil, false
	}

	// Find a btree index that covers the ORDER BY shape.
	var (
		index *indexState
		ok    bool
	)
	if len(plan.OrderBy) > 1 {
		index, _, ok = compositeBTreeIndexForOrder(table, plan.OrderBy)
	} else {
		orderCol := strings.TrimSpace(strings.ToLower(plan.OrderBy[0].Column))
		index, ok = indexForColumn(table, orderCol)
		if ok && index.kind != "btree" {
			ok = false
		}
	}
	if !ok || index == nil || index.kind != "btree" {
		return nil, false
	}

	// All projected columns must be in this index.
	if !indexCoversColumns(index, plan.Columns) {
		return nil, false
	}

	// Predicate (if any) must reference only indexed columns.
	if plan.Filter != nil && !indexCoversPredicate(index, plan.Filter) {
		return nil, false
	}

	rows := orderedRowsFromBTreeIndexOnly(table, index, plan, state, engine)
	return rows, true
}

// indexCoversColumns returns true when every projected column is stored in the
// btree index (either index.column for single-column or index.columns for
// composite).  Column expressions are compared after stripping AS aliases and
// normalising to lower-case.
func indexCoversColumns(index *indexState, cols []string) bool {
	if index.kind != "btree" {
		return false
	}
	// Build lookup set over the indexed column(s).
	indexColSet := make(map[string]struct{}, len(index.columns)+1)
	if len(index.columns) == 0 {
		indexColSet[strings.TrimSpace(strings.ToLower(index.column))] = struct{}{}
	} else {
		for _, c := range index.columns {
			indexColSet[strings.TrimSpace(strings.ToLower(c))] = struct{}{}
		}
	}
	// Every projected column (after stripping alias / row-function wrappers)
	// must appear in the index.
	for _, raw := range cols {
		expr, _ := stripColumnAlias(raw)
		col := strings.TrimSpace(strings.ToLower(expr))
		if _, ok := indexColSet[col]; !ok {
			return false
		}
	}
	return true
}

// indexCoversPredicate returns true when the predicate's column is covered by
// the index so that filtering can be done on entry.value without row access.
func indexCoversPredicate(index *indexState, pred *ast.Predicate) bool {
	if pred == nil {
		return true
	}
	predCol := strings.TrimSpace(strings.ToLower(pred.Column))
	if len(index.columns) == 0 {
		return predCol == strings.TrimSpace(strings.ToLower(index.column))
	}
	for _, c := range index.columns {
		if predCol == strings.TrimSpace(strings.ToLower(c)) {
			return true
		}
	}
	return false
}

// orderedRowsFromBTreeIndexOnly reads the btree entries in order and builds
// result rows from index entry values only — table.rows is never accessed.
func orderedRowsFromBTreeIndexOnly(
	table *tableState,
	index *indexState,
	plan planner.Plan,
	state *readableState,
	engine *Engine,
) []map[string]ast.Literal {
	entries := index.allEntries()

	// Precompute column→entry-values-index mapping.
	// Single-column index: index.column → entry.value (values slice is nil).
	// Composite index (len(index.columns) > 1): index.columns[i] → entry.values[i].
	type colMapping struct {
		pos      int
		isSingle bool
	}
	colMappings := make(map[string]colMapping, len(index.columns)+1)
	isSingleCol := len(index.columns) <= 1
	if isSingleCol {
		// Single-column btree: entry.value holds the key, entry.values is nil.
		colMappings[strings.TrimSpace(strings.ToLower(index.column))] = colMapping{isSingle: true}
	} else {
		// Composite btree: entry.values[i] holds the i-th key column.
		for i, c := range index.columns {
			colMappings[strings.TrimSpace(strings.ToLower(c))] = colMapping{pos: i}
		}
	}

	entryVal := func(e indexEntry, col string) ast.Literal {
		cm, ok := colMappings[strings.TrimSpace(strings.ToLower(col))]
		if !ok {
			return ast.Literal{Kind: ast.LiteralNull}
		}
		if cm.isSingle {
			return e.value
		}
		if cm.pos < len(e.values) {
			return e.values[cm.pos]
		}
		return ast.Literal{Kind: ast.LiteralNull}
	}

	matchEntry := func(e indexEntry) bool {
		if plan.Filter == nil {
			return true
		}
		// Build a minimal row from index values for predicate evaluation.
		minRow := map[string]ast.Literal{
			plan.Filter.Column: entryVal(e, plan.Filter.Column),
		}
		return matchPredicate(minRow, plan.Filter, state, engine)
	}

	buildRow := func(e indexEntry) map[string]ast.Literal {
		row := make(map[string]ast.Literal, len(plan.Columns))
		for _, raw := range plan.Columns {
			expr, alias := stripColumnAlias(raw)
			if alias == "" {
				alias = strings.TrimSpace(expr)
			}
			row[alias] = entryVal(e, expr)
		}
		return row
	}

	limit := plan.Limit
	offset := 0
	if plan.Offset != nil && *plan.Offset > 0 {
		offset = *plan.Offset
	}
	descending := plan.OrderBy[0].Direction == ast.SortDesc
	useBoundedScan := false
	if plan.Filter != nil && isSimplePredicate(plan.Filter) && strings.EqualFold(strings.TrimSpace(plan.Filter.Column), strings.TrimSpace(plan.OrderBy[0].Column)) {
		switch plan.Filter.Operator {
		case "=", ">", ">=", "<", "<=":
			useBoundedScan = true
		}
	}
	scanStart, scanEnd := 0, len(entries)
	if useBoundedScan {
		scanStart, scanEnd = btreeBoundedScanRange(entries, func(entry indexEntry) ast.Literal {
			return entryVal(entry, plan.Filter.Column)
		}, plan.Filter)
	}
	rowsCap := len(entries)
	if limit != nil && *limit >= 0 {
		rowsCap = *limit + offset
		if rowsCap > len(entries) {
			rowsCap = len(entries)
		}
	}
	rows := make([]map[string]ast.Literal, 0, rowsCap)

	if descending {
		for i := scanEnd - 1; i >= scanStart; i-- {
			if limit != nil && *limit >= 0 && len(rows) >= *limit {
				break
			}
			if !useBoundedScan && !matchEntry(entries[i]) {
				continue
			}
			if offset > 0 {
				offset--
				continue
			}
			rows = append(rows, buildRow(entries[i]))
		}
	} else {
		for i := scanStart; i < scanEnd; i++ {
			if limit != nil && *limit >= 0 && len(rows) >= *limit {
				break
			}
			e := entries[i]
			if !useBoundedScan && !matchEntry(e) {
				continue
			}
			if offset > 0 {
				offset--
				continue
			}
			rows = append(rows, buildRow(e))
		}
	}

	return rows
}
