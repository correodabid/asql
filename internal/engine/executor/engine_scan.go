package executor

import (
	"sort"
	"strconv"
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

	// scanPriorityIndexIntersection is the tiebreaker priority for intersected indexed conjuncts.
	scanPriorityIndexIntersection = 9

	// scanPriorityIndexUnion is the tiebreaker priority for index-union scans.
	scanPriorityIndexUnion = 12

	// scanPriorityIndexUnionPartial is the tiebreaker priority for hybrid OR scans.
	scanPriorityIndexUnionPartial = 13

	// scanPriorityBTreeLookup is the tiebreaker priority for btree equality lookups.
	scanPriorityBTreeLookup = 15

	// scanPriorityBTreeOrder is the tiebreaker priority for btree-ordered scans.
	scanPriorityBTreeOrder = 20

	// scanPriorityBTreePrefix is the tiebreaker priority for btree prefix scans.
	scanPriorityBTreePrefix = 30

	// scanPriorityIndexNot is the tiebreaker priority for index-backed NOT scans.
	scanPriorityIndexNot = 35
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
	if estimate, ok := estimateCompoundIndexLookupCost(table, predicate, totalRows, len(orderBy) > 0); ok {
		candidates = append(candidates, estimate)
	}
	if estimate, ok := estimateHybridORLookupCost(table, predicate, totalRows, len(orderBy) > 0); ok {
		candidates = append(candidates, estimate)
	}

	if len(orderBy) == 1 {
		if index, ok := indexForColumn(table, orderBy[0].Column); ok && index.kind == "btree" {
			candidates = append(candidates, estimateBTreeOrderCost(index, bestOrderBoundPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
	}

	if len(orderBy) > 1 {
		if _, _, ok := compositeBTreeIndexForOrder(table, orderBy); ok {
			candidates = append(candidates, estimateCompositeBTreeOrderCost(table, bestOrderBoundPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
		if index, ok := indexForColumn(table, orderBy[0].Column); ok && index.kind == "btree" {
			candidates = append(candidates, estimateBTreePrefixCost(index, bestOrderBoundPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
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
		return supportsIndexSelection(predicate.Left) || supportsIndexSelection(predicate.Right)
	case "OR":
		return supportsIndexSelection(predicate.Left) || supportsIndexSelection(predicate.Right)
	case "NOT":
		return supportsIndexSelection(predicate.Left)
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

func bestOrderBoundPredicateForColumn(table *tableState, predicates []*ast.Predicate, column string) *ast.Predicate {
	filtered := make([]*ast.Predicate, 0, len(predicates))
	for _, predicate := range predicates {
		if !supportsBTreeOrderBoundPredicate(predicate) {
			continue
		}
		filtered = append(filtered, predicate)
	}
	return bestPredicateForColumn(table, filtered, column)
}

func supportsBTreeOrderBoundPredicate(predicate *ast.Predicate) bool {
	if predicate == nil {
		return false
	}

	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "=", ">", ">=", "<", "<=":
		return predicate.Subquery == nil
	default:
		return false
	}
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
	if table == nil || predicate == nil {
		return nil, nil, scanStrategyFullScan, false
	}

	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "AND":
		leftIDs, leftPredicate, leftStrategy, leftOK := candidateRowIDsForPredicate(table, predicate.Left, hasOrderBy)
		rightIDs, rightPredicate, rightStrategy, rightOK := candidateRowIDsForPredicate(table, predicate.Right, hasOrderBy)
		switch {
		case leftOK && rightOK:
			intersected := intersectRowIDs(leftIDs, rightIDs)
			_ = leftPredicate
			_ = leftStrategy
			_ = rightPredicate
			_ = rightStrategy
			return intersected, nil, scanStrategyIndexInter, true
		case leftOK:
			return leftIDs, leftPredicate, leftStrategy, true
		case rightOK:
			return rightIDs, rightPredicate, rightStrategy, true
		default:
			return nil, nil, scanStrategyFullScan, false
		}
	case "OR":
		leftIDs, _, _, leftOK := candidateRowIDsForPredicate(table, predicate.Left, hasOrderBy)
		rightIDs, _, _, rightOK := candidateRowIDsForPredicate(table, predicate.Right, hasOrderBy)
		if !leftOK || !rightOK {
			return nil, nil, scanStrategyFullScan, false
		}
		return unionRowIDs(leftIDs, rightIDs), nil, scanStrategyIndexUnion, true
	case "NOT":
		childIDs, _, _, childOK := candidateRowIDsForPredicate(table, predicate.Left, hasOrderBy)
		if !childOK {
			return nil, nil, scanStrategyFullScan, false
		}
		return complementRowIDs(len(table.rows), childIDs), nil, scanStrategyIndexNot, true
	case "NOT IN":
		if predicate.Subquery != nil || len(predicate.InValues) == 0 {
			return nil, nil, scanStrategyFullScan, false
		}
		index, ok := indexForColumn(table, predicate.Column)
		if !ok {
			return nil, nil, scanStrategyFullScan, false
		}
		excludedRowIDs := rowIDsForPredicate(index, &ast.Predicate{
			Column:   predicate.Column,
			Operator: "IN",
			InValues: predicate.InValues,
		})
		return complementRowIDs(len(table.rows), excludedRowIDs), nil, scanStrategyIndexNot, true
	default:
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
}

func estimateCompoundIndexLookupCost(table *tableState, predicate *ast.Predicate, totalRows int, hasOrderBy bool) (scanCostEstimate, bool) {
	rowIDs, _, strategy, ok := candidateRowIDsForPredicate(table, predicate, hasOrderBy)
	if !ok || (strategy != scanStrategyIndexUnion && strategy != scanStrategyIndexNot && strategy != scanStrategyIndexInter) {
		return scanCostEstimate{}, false
	}
	if strategy == scanStrategyIndexNot {
		if _, ok := prunedIndexNotCandidate(table, predicate, totalRows, hasOrderBy); ok {
			return scanCostEstimate{}, false
		}
		return estimateIndexNotCost(table, predicate, totalRows, len(rowIDs), hasOrderBy)
	}

	estimatedRows := len(rowIDs)
	cost := estimatedRows
	if hasOrderBy {
		cost += estimatedRows / sortCostFactor
	}
	if totalRows > 0 && estimatedRows*100 >= totalRows*btreeHighSelectivityThreshold {
		cost += totalRows / btreeOverheadDivisor
	}
	priority := scanPriorityIndexUnion
	if strategy == scanStrategyIndexInter {
		priority = scanPriorityIndexIntersection
	}
	return scanCostEstimate{strategy: strategy, cost: cost, priority: priority, detail: formatPredicateForExplain(predicate)}, true
}

func estimateIndexNotCost(table *tableState, predicate *ast.Predicate, totalRows int, complementRows int, hasOrderBy bool) (scanCostEstimate, bool) {
	if table == nil || predicate == nil {
		return scanCostEstimate{}, false
	}

	excludedRows, ok := estimateExcludedRowsForNotPredicate(table, predicate)
	if !ok {
		return scanCostEstimate{}, false
	}

	cost := complementRows
	if hasOrderBy {
		cost += complementRows / sortCostFactor
	}

	if totalRows > 0 {
		broadnessPct := complementRows * 100 / totalRows
		switch {
		case broadnessPct >= 85:
			cost += totalRows / sortCostFactor
		case broadnessPct >= 70:
			cost += totalRows / hashOverheadDivisor
		}

		if excludedRows*5 <= totalRows {
			cost += totalRows / btreeOverheadDivisor
		}
	}

	return scanCostEstimate{strategy: scanStrategyIndexNot, cost: cost, priority: scanPriorityIndexNot, detail: formatPredicateForExplain(predicate)}, true
}

func estimateExcludedRowsForNotPredicate(table *tableState, predicate *ast.Predicate) (int, bool) {
	if table == nil || predicate == nil {
		return 0, false
	}

	operator := strings.ToUpper(strings.TrimSpace(predicate.Operator))
	switch operator {
	case "NOT":
		rowIDs, ok := exactCandidateRowIDsForPredicate(table, predicate.Left, false)
		if !ok {
			return 0, false
		}
		return len(rowIDs), true
	case "NOT IN":
		if predicate.Subquery != nil || len(predicate.InValues) == 0 {
			return 0, false
		}
		index, ok := indexForColumn(table, predicate.Column)
		if !ok {
			return 0, false
		}
		return len(rowIDsForPredicate(index, &ast.Predicate{Column: predicate.Column, Operator: "IN", InValues: predicate.InValues})), true
	default:
		return 0, false
	}
}

func exactCandidateRowIDsForPredicate(table *tableState, predicate *ast.Predicate, hasOrderBy bool) ([]int, bool) {
	if table == nil || predicate == nil {
		return nil, false
	}

	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "AND":
		leftIDs, leftOK := exactCandidateRowIDsForPredicate(table, predicate.Left, hasOrderBy)
		rightIDs, rightOK := exactCandidateRowIDsForPredicate(table, predicate.Right, hasOrderBy)
		if !leftOK || !rightOK {
			return nil, false
		}
		return intersectRowIDs(leftIDs, rightIDs), true
	case "OR":
		leftIDs, leftOK := exactCandidateRowIDsForPredicate(table, predicate.Left, hasOrderBy)
		rightIDs, rightOK := exactCandidateRowIDsForPredicate(table, predicate.Right, hasOrderBy)
		if !leftOK || !rightOK {
			return nil, false
		}
		return unionRowIDs(leftIDs, rightIDs), true
	case "NOT":
		childIDs, childOK := exactCandidateRowIDsForPredicate(table, predicate.Left, hasOrderBy)
		if !childOK {
			return nil, false
		}
		return complementRowIDs(len(table.rows), childIDs), true
	case "NOT IN":
		if predicate.Subquery != nil || len(predicate.InValues) == 0 {
			return nil, false
		}
		index, ok := indexForColumn(table, predicate.Column)
		if !ok {
			return nil, false
		}
		return complementRowIDs(len(table.rows), rowIDsForPredicate(index, &ast.Predicate{Column: predicate.Column, Operator: "IN", InValues: predicate.InValues})), true
	default:
		if !isSimplePredicate(predicate) {
			return nil, false
		}
		lookupPredicate, _, ok := bestLookupPredicate(table, predicate, hasOrderBy)
		if !ok || lookupPredicate == nil {
			return nil, false
		}
		index, ok := indexForColumn(table, lookupPredicate.Column)
		if !ok {
			return nil, false
		}
		return rowIDsForPredicate(index, lookupPredicate), true
	}
}

func decomposeHybridORPredicate(table *tableState, predicate *ast.Predicate, hasOrderBy bool) ([]int, *ast.Predicate, bool) {
	if table == nil || predicate == nil || !strings.EqualFold(strings.TrimSpace(predicate.Operator), "OR") {
		return nil, nil, false
	}

	candidateRowIDs, residual, found := decomposeHybridORNode(table, predicate, hasOrderBy)
	if !found || residual == nil || len(candidateRowIDs) == 0 {
		return nil, nil, false
	}
	return candidateRowIDs, residual, true
}

func decomposeHybridORNode(table *tableState, predicate *ast.Predicate, hasOrderBy bool) ([]int, *ast.Predicate, bool) {
	if predicate == nil {
		return nil, nil, false
	}

	if strings.EqualFold(strings.TrimSpace(predicate.Operator), "OR") {
		leftIDs, leftResidual, leftFound := decomposeHybridORNode(table, predicate.Left, hasOrderBy)
		rightIDs, rightResidual, rightFound := decomposeHybridORNode(table, predicate.Right, hasOrderBy)
		return unionRowIDs(leftIDs, rightIDs), mergeORPredicates(leftResidual, rightResidual), leftFound || rightFound
	}

	if rowIDs, ok := exactCandidateRowIDsForPredicate(table, predicate, hasOrderBy); ok {
		return rowIDs, nil, true
	}

	return nil, predicate, false
}

func mergeORPredicates(left *ast.Predicate, right *ast.Predicate) *ast.Predicate {
	switch {
	case left == nil:
		return right
	case right == nil:
		return left
	default:
		return &ast.Predicate{Operator: "OR", Left: left, Right: right}
	}
}

func estimateHybridORLookupCost(table *tableState, predicate *ast.Predicate, totalRows int, hasOrderBy bool) (scanCostEstimate, bool) {
	candidateRowIDs, residual, ok := decomposeHybridORPredicate(table, predicate, hasOrderBy)
	if !ok {
		return scanCostEstimate{}, false
	}
	if totalRows > 0 && len(candidateRowIDs)*100 >= totalRows*70 {
		return scanCostEstimate{}, false
	}

	residualRows := totalRows - len(candidateRowIDs)
	if residualRows < 0 {
		residualRows = 0
	}
	cost := totalRows - len(candidateRowIDs)/sortCostFactor
	if cost < 0 {
		cost = 0
	}
	if hasOrderBy {
		cost += totalRows / sortCostFactor
	}
	detail := formatHybridORCandidateDetail(table, predicate, residual)
	return scanCostEstimate{strategy: scanStrategyIndexUnionP, cost: cost, priority: scanPriorityIndexUnionPartial, detail: detail}, true
}

func formatHybridORCandidateDetail(table *tableState, predicate *ast.Predicate, residual *ast.Predicate) string {
	detail := strings.Join(collectExplainIndexedPredicates(table, predicate), " | ")
	residualText := ""
	if residual != nil {
		residualText = formatPredicateForExplain(residual)
	} else {
		residualText = explainResidualPredicate(table, predicate)
	}
	if residualText != "" {
		if detail != "" {
			detail += " ; residual: " + residualText
		} else {
			detail = "residual: " + residualText
		}
	}
	return detail
}

func unionRowIDs(left []int, right []int) []int {
	if len(left) == 0 {
		return append([]int(nil), right...)
	}
	if len(right) == 0 {
		return append([]int(nil), left...)
	}
	seen := make(map[int]struct{}, len(left)+len(right))
	merged := make([]int, 0, len(left)+len(right))
	for _, rowID := range left {
		if _, exists := seen[rowID]; exists {
			continue
		}
		seen[rowID] = struct{}{}
		merged = append(merged, rowID)
	}
	for _, rowID := range right {
		if _, exists := seen[rowID]; exists {
			continue
		}
		seen[rowID] = struct{}{}
		merged = append(merged, rowID)
	}
	sort.Ints(merged)
	return merged
}

func intersectRowIDs(left []int, right []int) []int {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(left))
	for _, rowID := range left {
		seen[rowID] = struct{}{}
	}
	intersected := make([]int, 0, min(len(left), len(right)))
	for _, rowID := range right {
		if _, exists := seen[rowID]; !exists {
			continue
		}
		intersected = append(intersected, rowID)
	}
	sort.Ints(intersected)
	return dedupeSortedRowIDs(intersected)
}

func complementRowIDs(totalRows int, rowIDs []int) []int {
	if totalRows <= 0 {
		return nil
	}
	excluded := make(map[int]struct{}, len(rowIDs))
	for _, rowID := range rowIDs {
		excluded[rowID] = struct{}{}
	}
	complement := make([]int, 0, totalRows-len(excluded))
	for rowID := 0; rowID < totalRows; rowID++ {
		if _, exists := excluded[rowID]; exists {
			continue
		}
		complement = append(complement, rowID)
	}
	return complement
}

func dedupeSortedRowIDs(rowIDs []int) []int {
	if len(rowIDs) <= 1 {
		return rowIDs
	}
	write := 1
	for read := 1; read < len(rowIDs); read++ {
		if rowIDs[read] == rowIDs[write-1] {
			continue
		}
		rowIDs[write] = rowIDs[read]
		write++
	}
	return rowIDs[:write]
}

func estimateFullScanCost(totalRows int, orderBy []ast.OrderByClause) scanCostEstimate {
	if totalRows < 0 {
		totalRows = 0
	}

	cost := totalRows
	if len(orderBy) > 0 {
		cost += totalRows / sortCostFactor
	}

	detail := "scan all rows"
	if len(orderBy) > 0 {
		detail = "scan all rows + sort"
	}
	return scanCostEstimate{strategy: scanStrategyFullScan, cost: cost, priority: scanPriorityFullScan, detail: detail}
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

	return scanCostEstimate{strategy: scanStrategyHashLookup, cost: cost, priority: scanPriorityHashLookup, detail: formatPredicateForExplain(predicate)}, true
}

func estimateBTreeOrderCost(index *indexState, predicate *ast.Predicate, totalRows int) scanCostEstimate {
	estimatedRows := totalRows
	if predicate != nil {
		if rows, ok := estimateRowsByIndexPredicate(index, predicate, totalRows); ok {
			estimatedRows = rows
		}
	}

	detail := "ordered index scan"
	if predicate != nil {
		detail = formatPredicateForExplain(predicate)
	}
	return scanCostEstimate{strategy: scanStrategyBTreeOrder, cost: estimatedRows, priority: scanPriorityBTreeOrder, detail: detail}
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

	detail := "composite ordered index scan"
	if predicate != nil {
		detail = formatPredicateForExplain(predicate)
	}
	return scanCostEstimate{strategy: scanStrategyBTreeOrder, cost: estimatedRows, priority: scanPriorityBTreeOrder, detail: detail}
}

func estimateBTreePrefixCost(index *indexState, predicate *ast.Predicate, totalRows int) scanCostEstimate {
	estimatedRows := totalRows
	if predicate != nil {
		if rows, ok := estimateRowsByIndexPredicate(index, predicate, totalRows); ok {
			estimatedRows = rows
		}
	}

	cost := estimatedRows + estimatedRows/btreePrefixMergeOverhead
	detail := "prefix ordered index scan"
	if predicate != nil {
		detail = formatPredicateForExplain(predicate)
	}
	return scanCostEstimate{strategy: scanStrategyBTreePrefix, cost: cost, priority: scanPriorityBTreePrefix, detail: detail}
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

	return scanCostEstimate{strategy: scanStrategyBTreeLookup, cost: cost, priority: scanPriorityBTreeLookup, detail: formatPredicateForExplain(predicate)}, true
}

func estimateRowsByIndexPredicate(index *indexState, predicate *ast.Predicate, totalRows int) (int, bool) {
	if index == nil || predicate == nil {
		return 0, false
	}

	operator := strings.ToUpper(strings.TrimSpace(predicate.Operator))
	switch index.kind {
	case "hash":
		if operator != "=" && operator != "IN" && operator != "IS NULL" && operator != "IS NOT NULL" {
			return 0, false
		}
	case "btree":
		if operator != "=" && operator != "IN" && operator != ">" && operator != ">=" && operator != "<" && operator != "<=" && operator != "IS NULL" && operator != "IS NOT NULL" {
			return 0, false
		}
	default:
		return 0, false
	}

	if operator == "IN" && (predicate.Subquery != nil || len(predicate.InValues) == 0) {
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
		if hybridRowIDs, ok := matchedRowIDsForHybridORPredicate(table, predicate, state, engine); ok {
			return rowsFromRowIDs(table, hybridRowIDs)
		}
		return tableRowsToMaps(table)
	}
	if len(rowIDs) == 0 {
		return nil
	}

	return rowsFromRowIDs(table, rowIDs)
}

func rowsFromRowIDs(table *tableState, rowIDs []int) []map[string]ast.Literal {
	rows := make([]map[string]ast.Literal, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		if rowID < 0 || rowID >= len(table.rows) {
			continue
		}
		rows = append(rows, rowToMap(table, table.rows[rowID]))
	}
	return rows
}

func matchedRowIDsForHybridORPredicate(table *tableState, predicate *ast.Predicate, state *readableState, engine *Engine) ([]int, bool) {
	candidateRowIDs, residual, ok := decomposeHybridORPredicate(table, predicate, false)
	if !ok {
		return nil, false
	}

	candidateSet := make(map[int]struct{}, len(candidateRowIDs))
	for _, rowID := range candidateRowIDs {
		candidateSet[rowID] = struct{}{}
	}

	matched := make([]int, 0, len(candidateRowIDs))
	for rowID, rowSlice := range table.rows {
		if _, exact := candidateSet[rowID]; exact {
			matched = append(matched, rowID)
			continue
		}
		row := rowToMap(table, rowSlice)
		if matchPredicate(row, residual, state, engine) {
			matched = append(matched, rowID)
		}
	}
	return matched, true
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
	Strategy      string                `json:"strategy"`
	TableRows     int                   `json:"table_rows,omitempty"`
	EstimatedRows int                   `json:"estimated_rows,omitempty"`
	IndexUsed     string                `json:"index_used,omitempty"`
	IndexType     string                `json:"index_type,omitempty"`
	IndexColumn   string                `json:"index_column,omitempty"`
	IndexedPreds  []string              `json:"indexed_predicates,omitempty"`
	ResidualPred  string                `json:"residual_predicate,omitempty"`
	Candidates    []candidateInfo       `json:"candidates,omitempty"`
	Pruned        []prunedCandidateInfo `json:"pruned_candidates,omitempty"`
	Joins         []joinPlanInfo        `json:"joins,omitempty"`
}

type candidateInfo struct {
	Strategy       string `json:"strategy"`
	Cost           int    `json:"cost"`
	Detail         string `json:"detail,omitempty"`
	Chosen         bool   `json:"chosen,omitempty"`
	RejectedReason string `json:"rejected_reason,omitempty"`
}

type prunedCandidateInfo struct {
	Strategy string `json:"strategy"`
	Detail   string `json:"detail,omitempty"`
	Reason   string `json:"reason"`
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
		info.IndexedPreds, info.ResidualPred = explainPredicateContributions(table, plan.Filter)
	}

	switch plan.Operation {
	case planner.OperationSelect:
		if len(plan.Joins) == 0 {
			info.Strategy, info.Candidates, info.Pruned = collectScanCandidates(table, plan.Filter, plan.OrderBy)
		} else {
			info.Strategy = string(scanStrategyJoinNested)
			info.Joins = collectJoinPlans(state, ds, plan)
		}

		// Report index and row estimate for filter column.
		if table != nil {
			if rowIDs, lookupPredicate, strategy, ok := candidateRowIDsForPredicate(table, plan.Filter, len(plan.OrderBy) > 0); ok {
				if strategy == scanStrategyIndexUnion || strategy == scanStrategyIndexNot || strategy == scanStrategyIndexInter {
					info.EstimatedRows = len(rowIDs)
				} else if lookupPredicate != nil {
					if idx, ok := indexForColumn(table, lookupPredicate.Column); ok {
						info.IndexUsed = indexNameForColumn(table, lookupPredicate.Column)
						info.IndexType = idx.kind
						info.IndexColumn = lookupPredicate.Column
						if est, ok := estimateRowsByIndexPredicate(idx, lookupPredicate, len(table.rows)); ok {
							info.EstimatedRows = est
						}
					}
				}
			} else if hybridRowIDs, ok := matchedRowIDsForHybridORPredicate(table, plan.Filter, state, engine); ok {
				info.EstimatedRows = len(hybridRowIDs)
				info.Strategy = string(scanStrategyIndexUnionP)
			}
		}

	case planner.OperationInsert, planner.OperationUpdate, planner.OperationDelete:
		info.Strategy = string(scanStrategyFullScan)
		if table != nil {
			if rowIDs, lookupPredicate, strat, ok := candidateRowIDsForPredicate(table, plan.Filter, false); ok {
				info.Strategy = string(strat)
				if strat == scanStrategyIndexUnion || strat == scanStrategyIndexNot || strat == scanStrategyIndexInter {
					info.EstimatedRows = len(rowIDs)
				} else if lookupPredicate != nil {
					if idx, ok := indexForColumn(table, lookupPredicate.Column); ok {
						info.IndexUsed = indexNameForColumn(table, lookupPredicate.Column)
						info.IndexType = idx.kind
						info.IndexColumn = lookupPredicate.Column
						if est, ok := estimateRowsByIndexPredicate(idx, lookupPredicate, len(table.rows)); ok {
							info.EstimatedRows = est
						}
					}
				}
			} else if hybridRowIDs, ok := matchedRowIDsForHybridORPredicate(table, plan.Filter, state, engine); ok {
				info.Strategy = string(scanStrategyIndexUnionP)
				info.EstimatedRows = len(hybridRowIDs)
			}
		}

	default:
		info.Strategy = "n/a"
	}

	return info
}

func explainPredicateContributions(table *tableState, predicate *ast.Predicate) ([]string, string) {
	if table == nil || predicate == nil {
		return nil, ""
	}

	indexed := collectExplainIndexedPredicates(table, predicate)
	indexed = dedupeStrings(indexed)
	residual := explainResidualPredicate(table, predicate)
	return indexed, residual
}

func collectExplainIndexedPredicates(table *tableState, predicate *ast.Predicate) []string {
	if table == nil || predicate == nil {
		return nil
	}

	if rowIDs, _, strategy, ok := candidateRowIDsForPredicate(table, predicate, false); ok {
		_ = rowIDs
		switch strategy {
		case scanStrategyHashLookup, scanStrategyBTreeLookup, scanStrategyIndexNot:
			return []string{formatPredicateForExplain(predicate)}
		case scanStrategyIndexUnion, scanStrategyIndexInter:
			return flattenExplainIndexedLeaves(predicate)
		}
	}

	if _, residual, ok := decomposeHybridORPredicate(table, predicate, false); ok {
		allLeaves := flattenExplainIndexedLeaves(predicate)
		residualLeaves := flattenExplainResidualLeaves(residual)
		return subtractStrings(allLeaves, residualLeaves)
	}

	return nil
}

func explainResidualPredicate(table *tableState, predicate *ast.Predicate) string {
	if table == nil || predicate == nil {
		return ""
	}

	if _, residual, ok := decomposeHybridORPredicate(table, predicate, false); ok && residual != nil {
		return formatPredicateForExplain(residual)
	}

	if rowIDs, lookupPredicate, strategy, ok := candidateRowIDsForPredicate(table, predicate, false); ok {
		_ = rowIDs
		switch strategy {
		case scanStrategyHashLookup, scanStrategyBTreeLookup:
			if strings.EqualFold(formatPredicateForExplain(predicate), formatPredicateForExplain(lookupPredicate)) {
				return ""
			}
			return formatPredicateForExplain(predicate)
		case scanStrategyIndexUnion, scanStrategyIndexInter, scanStrategyIndexNot:
			return ""
		}
	}

	return formatPredicateForExplain(predicate)
}

func flattenExplainIndexedLeaves(predicate *ast.Predicate) []string {
	if predicate == nil {
		return nil
	}

	operator := strings.ToUpper(strings.TrimSpace(predicate.Operator))
	switch operator {
	case "AND", "OR":
		left := flattenExplainIndexedLeaves(predicate.Left)
		right := flattenExplainIndexedLeaves(predicate.Right)
		return append(left, right...)
	default:
		return []string{formatPredicateForExplain(predicate)}
	}
}

func flattenExplainResidualLeaves(predicate *ast.Predicate) []string {
	if predicate == nil {
		return nil
	}
	operator := strings.ToUpper(strings.TrimSpace(predicate.Operator))
	switch operator {
	case "AND", "OR":
		left := flattenExplainResidualLeaves(predicate.Left)
		right := flattenExplainResidualLeaves(predicate.Right)
		return append(left, right...)
	default:
		return []string{formatPredicateForExplain(predicate)}
	}
}

func subtractStrings(values []string, excluded []string) []string {
	if len(values) == 0 {
		return nil
	}
	excludedSet := make(map[string]int, len(excluded))
	for _, value := range excluded {
		excludedSet[value]++
	}
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		if excludedSet[value] > 0 {
			excludedSet[value]--
			continue
		}
		filtered = append(filtered, value)
	}
	return filtered
}

func dedupeStrings(values []string) []string {
	if len(values) <= 1 {
		return values
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func formatPredicateForExplain(predicate *ast.Predicate) string {
	if predicate == nil {
		return ""
	}

	operator := strings.ToUpper(strings.TrimSpace(predicate.Operator))
	switch operator {
	case "AND", "OR":
		return "(" + formatPredicateForExplain(predicate.Left) + " " + operator + " " + formatPredicateForExplain(predicate.Right) + ")"
	case "NOT":
		return "(NOT " + formatPredicateForExplain(predicate.Left) + ")"
	case "IN", "NOT IN":
		parts := make([]string, 0, len(predicate.InValues))
		for _, value := range predicate.InValues {
			parts = append(parts, formatLiteralForExplain(value))
		}
		return predicate.Column + " " + operator + " (" + strings.Join(parts, ", ") + ")"
	case "IS NULL", "IS NOT NULL":
		return predicate.Column + " " + operator
	default:
		return predicate.Column + " " + operator + " " + formatLiteralForExplain(predicate.Value)
	}
}

func formatLiteralForExplain(value ast.Literal) string {
	switch value.Kind {
	case ast.LiteralString:
		return "'" + value.StringValue + "'"
	case ast.LiteralNumber:
		return strconv.FormatInt(value.NumberValue, 10)
	case ast.LiteralBoolean:
		if value.BoolValue {
			return "TRUE"
		}
		return "FALSE"
	case ast.LiteralNull:
		return "NULL"
	default:
		if value.StringValue != "" {
			return "'" + value.StringValue + "'"
		}
		return ""
	}
}

// collectScanCandidates runs the cost model and returns the chosen strategy,
// all evaluated candidates with their costs, and any heuristic-pruned plans.
func collectScanCandidates(table *tableState, predicate *ast.Predicate, orderBy []ast.OrderByClause) (string, []candidateInfo, []prunedCandidateInfo) {
	if table == nil {
		return string(scanStrategyFullScan), nil, nil
	}

	if !supportsIndexSelection(predicate) {
		return string(scanStrategyFullScan), []candidateInfo{{Strategy: string(scanStrategyFullScan), Cost: len(table.rows), Chosen: true}}, nil
	}

	totalRows := len(table.rows)
	pruned := collectPrunedScanCandidates(table, predicate, orderBy)
	candidates := make([]scanCostEstimate, 0, 4)
	candidates = append(candidates, estimateFullScanCost(totalRows, orderBy))
	predicateCandidates := collectIndexablePredicates(predicate)
	if estimate, ok := estimateCompoundIndexLookupCost(table, predicate, totalRows, len(orderBy) > 0); ok {
		candidates = append(candidates, estimate)
	}
	if estimate, ok := estimateHybridORLookupCost(table, predicate, totalRows, len(orderBy) > 0); ok {
		candidates = append(candidates, estimate)
	}

	if len(orderBy) == 1 {
		if index, ok := indexForColumn(table, orderBy[0].Column); ok && index.kind == "btree" {
			candidates = append(candidates, estimateBTreeOrderCost(index, bestOrderBoundPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
	}

	if len(orderBy) > 1 {
		if _, _, ok := compositeBTreeIndexForOrder(table, orderBy); ok {
			candidates = append(candidates, estimateCompositeBTreeOrderCost(table, bestOrderBoundPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
		}
		if index, ok := indexForColumn(table, orderBy[0].Column); ok && index.kind == "btree" {
			candidates = append(candidates, estimateBTreePrefixCost(index, bestOrderBoundPredicateForColumn(table, predicateCandidates, orderBy[0].Column), totalRows))
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
	bestEstimate := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.cost < bestEstimate.cost {
			bestEstimate = candidate
			continue
		}
		if candidate.cost == bestEstimate.cost && candidate.priority < bestEstimate.priority {
			bestEstimate = candidate
		}
	}

	result := make([]candidateInfo, len(candidates))
	for i, c := range candidates {
		chosen := c.strategy == best && c.cost == bestEstimate.cost && c.priority == bestEstimate.priority
		rejectedReason := ""
		if !chosen {
			switch {
			case c.cost > bestEstimate.cost:
				if c.priority < bestEstimate.priority {
					rejectedReason = "higher cost than chosen candidate"
				} else {
					rejectedReason = "higher cost than chosen candidate"
				}
			case c.cost == bestEstimate.cost && c.priority > bestEstimate.priority:
				rejectedReason = "lost tie-break on priority"
			case c.strategy != bestEstimate.strategy:
				rejectedReason = "another candidate ranked ahead"
			}
		}
		result[i] = candidateInfo{
			Strategy:       string(c.strategy),
			Cost:           c.cost,
			Detail:         c.detail,
			Chosen:         chosen,
			RejectedReason: rejectedReason,
		}
	}

	return string(best), result, pruned
}

func collectPrunedScanCandidates(table *tableState, predicate *ast.Predicate, orderBy []ast.OrderByClause) []prunedCandidateInfo {
	if table == nil || predicate == nil {
		return nil
	}

	totalRows := len(table.rows)
	if totalRows == 0 {
		return nil
	}
	hasOrderBy := len(orderBy) > 0
	pruned := make([]prunedCandidateInfo, 0, 2)

	if candidate, ok := prunedIndexNotCandidate(table, predicate, totalRows, hasOrderBy); ok {
		pruned = append(pruned, candidate)
	}

	if candidateRowIDs, residual, ok := decomposeHybridORPredicate(table, predicate, hasOrderBy); ok {
		if len(candidateRowIDs)*100 >= totalRows*70 {
			detail := formatHybridORCandidateDetail(table, predicate, residual)
			reason := "indexed OR branch covers " + strconv.Itoa(len(candidateRowIDs)) + "/" + strconv.Itoa(totalRows) + " rows; crossover prefers full-scan"
			pruned = append(pruned, prunedCandidateInfo{
				Strategy: string(scanStrategyIndexUnionP),
				Detail:   detail,
				Reason:   reason,
			})
		}
	}

	if len(pruned) == 0 {
		return nil
	}

	return pruned
}

func prunedIndexNotCandidate(table *tableState, predicate *ast.Predicate, totalRows int, hasOrderBy bool) (prunedCandidateInfo, bool) {
	if table == nil || predicate == nil || totalRows <= 0 {
		return prunedCandidateInfo{}, false
	}

	rowIDs, _, strategy, ok := candidateRowIDsForPredicate(table, predicate, hasOrderBy)
	if !ok || strategy != scanStrategyIndexNot {
		return prunedCandidateInfo{}, false
	}

	estimate, ok := estimateIndexNotCost(table, predicate, totalRows, len(rowIDs), hasOrderBy)
	if !ok {
		return prunedCandidateInfo{}, false
	}

	fullScanCost := totalRows
	if hasOrderBy {
		fullScanCost += totalRows / sortCostFactor
	}
	if estimate.cost <= fullScanCost {
		return prunedCandidateInfo{}, false
	}

	label := "NOT"
	if strings.EqualFold(strings.TrimSpace(predicate.Operator), "NOT IN") {
		label = "NOT IN"
	}

	return prunedCandidateInfo{
		Strategy: string(scanStrategyIndexNot),
		Detail:   estimate.detail,
		Reason:   "indexed " + label + " complement keeps " + strconv.Itoa(len(rowIDs)) + "/" + strconv.Itoa(totalRows) + " rows; crossover prefers full-scan",
	}, true
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
