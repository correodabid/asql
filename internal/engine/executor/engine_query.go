package executor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"asql/internal/engine/parser/ast"
	"asql/internal/engine/planner"
)

func (engine *Engine) selectRows(ctx context.Context, state *readableState, plan planner.Plan) ([]map[string]ast.Literal, error) {
	// Materialize CTEs and inject as virtual tables
	if len(plan.CTEs) > 0 {
		state = materializeCTEs(state, plan.CTEs, engine)
	}

	domainState, exists := state.domains[plan.DomainName]
	if !exists {
		return nil, errTableNotFound
	}

	table, exists := domainState.tables[plan.TableName]
	if !exists {
		return nil, errTableNotFound
	}

	matched := make([]map[string]ast.Literal, 0, len(table.rows))
	orderedByIndex := false
	selectedStrategy := scanStrategyFullScan

	if len(plan.Joins) == 0 {
		// Index-only scan: serve from btree entries without touching table.rows.
		// Checked first because it is strictly cheaper when applicable.
		if rows, ok := tryIndexOnlyScan(table, plan, state, engine); ok {
			engine.recordScanStrategy(scanStrategyBTreeIOScan)
			return rows, nil
		}

		strategy := chooseSingleTableScanStrategy(table, plan.Filter, plan.OrderBy)
		switch strategy {
		case scanStrategyBTreeOrder:
			if ordered, ok := orderedRowsFromBTreeIndex(table, plan.Filter, plan.OrderBy, plan.Limit, state, engine); ok {
				matched = ordered
				orderedByIndex = true
				selectedStrategy = scanStrategyBTreeOrder
			} else {
				strategy = scanStrategyFullScan
			}
		case scanStrategyBTreePrefix:
			if ordered, ok := orderedRowsFromBTreePrefix(table, plan.Filter, plan.OrderBy, state, engine); ok {
				matched = ordered
				orderedByIndex = true
				selectedStrategy = scanStrategyBTreePrefix
			} else {
				strategy = scanStrategyFullScan
			}
		case scanStrategyHashLookup:
			candidateRows := rowsForPredicate(table, plan.Filter, state, engine)
			for _, row := range candidateRows {
				row = qualifySingleTableRow(table, plan, row)
				if !matchPredicate(row, plan.Filter, state, engine) {
					continue
				}
				matched = append(matched, row)
			}
			selectedStrategy = scanStrategyHashLookup
		case scanStrategyIndexInter:
			candidateRows := rowsForPredicate(table, plan.Filter, state, engine)
			for _, row := range candidateRows {
				row = qualifySingleTableRow(table, plan, row)
				if !matchPredicate(row, plan.Filter, state, engine) {
					continue
				}
				matched = append(matched, row)
			}
			selectedStrategy = scanStrategyIndexInter
		case scanStrategyIndexUnion:
			candidateRows := rowsForPredicate(table, plan.Filter, state, engine)
			for _, row := range candidateRows {
				row = qualifySingleTableRow(table, plan, row)
				if !matchPredicate(row, plan.Filter, state, engine) {
					continue
				}
				matched = append(matched, row)
			}
			selectedStrategy = scanStrategyIndexUnion
		case scanStrategyIndexUnionP:
			candidateRows := rowsForPredicate(table, plan.Filter, state, engine)
			for _, row := range candidateRows {
				row = qualifySingleTableRow(table, plan, row)
				if !matchPredicate(row, plan.Filter, state, engine) {
					continue
				}
				matched = append(matched, row)
			}
			selectedStrategy = scanStrategyIndexUnionP
		case scanStrategyIndexNot:
			candidateRows := rowsForPredicate(table, plan.Filter, state, engine)
			for _, row := range candidateRows {
				row = qualifySingleTableRow(table, plan, row)
				if !matchPredicate(row, plan.Filter, state, engine) {
					continue
				}
				matched = append(matched, row)
			}
			selectedStrategy = scanStrategyIndexNot
		case scanStrategyBTreeLookup:
			candidateRows := rowsForPredicate(table, plan.Filter, state, engine)
			for _, row := range candidateRows {
				row = qualifySingleTableRow(table, plan, row)
				if !matchPredicate(row, plan.Filter, state, engine) {
					continue
				}
				matched = append(matched, row)
			}
			selectedStrategy = scanStrategyBTreeLookup
		}

		if strategy == scanStrategyFullScan {
			for _, rowSlice := range table.rows {
				row := rowToMap(table, rowSlice)
				row = qualifySingleTableRow(table, plan, row)
				if !matchPredicate(row, plan.Filter, state, engine) {
					continue
				}
				matched = append(matched, row)
			}
			selectedStrategy = scanStrategyFullScan
		}
	} else {
		var err error
		matched, selectedStrategy, err = engine.executeJoinPipeline(ctx, state, domainState, plan)
		if err != nil {
			return nil, err
		}
	}

	engine.recordScanStrategy(selectedStrategy)

	if requiresAggregation(plan) {
		aggregated, err := aggregateRows(matched, plan, state, engine)
		if err != nil {
			return nil, err
		}

		if len(plan.WindowFunctions) > 0 {
			aggregated = applyWindowFunctions(aggregated, plan.WindowFunctions)
		}

		if len(plan.OrderBy) > 0 {
			sort.SliceStable(aggregated, func(i, j int) bool {
				for _, clause := range plan.OrderBy {
					left := valueOrNull(aggregated[i], clause.Column)
					right := valueOrNull(aggregated[j], clause.Column)

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
		}

		if plan.Offset != nil && *plan.Offset > 0 {
			if *plan.Offset >= len(aggregated) {
				aggregated = aggregated[:0]
			} else {
				aggregated = aggregated[*plan.Offset:]
			}
		}

		if plan.Limit != nil {
			if *plan.Limit == 0 {
				aggregated = aggregated[:0]
			} else if *plan.Limit < len(aggregated) {
				aggregated = aggregated[:*plan.Limit]
			}
		}

		if plan.Distinct {
			aggregated = deduplicateRows(aggregated)
		}

		return aggregated, nil
	}

	if len(plan.WindowFunctions) > 0 {
		matched = applyWindowFunctions(matched, plan.WindowFunctions)
	}

	if len(plan.OrderBy) > 0 && !orderedByIndex {
		sort.SliceStable(matched, func(i, j int) bool {
			for _, clause := range plan.OrderBy {
				left := valueOrNull(matched[i], clause.Column)
				right := valueOrNull(matched[j], clause.Column)

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
	}

	if plan.Offset != nil && *plan.Offset > 0 {
		if *plan.Offset >= len(matched) {
			matched = matched[:0]
		} else {
			matched = matched[*plan.Offset:]
		}
	}

	if plan.Limit != nil {
		if *plan.Limit == 0 {
			matched = matched[:0]
		} else if *plan.Limit < len(matched) {
			matched = matched[:*plan.Limit]
		}
	}

	// Build a lookup map for JSON access columns by alias.
	jsonAccessMap := make(map[string]*ast.JsonAccess, len(plan.JsonAccessColumns))
	for i := range plan.JsonAccessColumns {
		jsonAccessMap[plan.JsonAccessColumns[i].Alias] = &plan.JsonAccessColumns[i]
	}

	// Build a lookup map for CASE WHEN columns by alias.
	caseWhenMap := make(map[string]*ast.CaseWhenExpression, len(plan.CaseWhenColumns))
	for i := range plan.CaseWhenColumns {
		caseWhenMap[plan.CaseWhenColumns[i].Alias] = &plan.CaseWhenColumns[i]
	}

	result := make([]map[string]ast.Literal, 0, len(matched))
	for _, row := range matched {

		if len(plan.Columns) == 1 && plan.Columns[0] == "*" {
			if len(plan.Joins) == 0 {
				result = append(result, stripQualifiedColumns(row))
				continue
			}
			result = append(result, row)
			continue
		}

		projected := make(map[string]ast.Literal, len(plan.Columns))
		for _, column := range plan.Columns {
			if prefix, ok := parseQualifiedStarColumn(column); ok {
				expandQualifiedStar(projected, row, table, plan, prefix)
				continue
			}
			expr, alias := stripColumnAlias(column)
			if ja, ok := jsonAccessMap[column]; ok {
				if val, resolved := resolveJsonAccess(row, ja); resolved {
					projected[column] = val
				} else {
					projected[column] = ast.Literal{Kind: ast.LiteralNull}
				}
				continue
			}
			// CASE WHEN expression.
			if cw, ok := caseWhenMap[column]; ok {
				projected[column] = evaluateCaseWhenExpression(row, *cw, state, engine)
				continue
			}
			// Row-level functions: COALESCE, NULLIF.
			// NOW() — returns deterministic logical timestamp.
			if strings.ToLower(strings.TrimSpace(expr)) == "now()" {
				projected[alias] = ast.Literal{Kind: ast.LiteralString, StringValue: formatLogicalTimestamp(state.logicalTS)}
				continue
			}
			if val, ok := evaluateRowFunction(expr, row); ok {
				projected[alias] = val
				continue
			}
			// CAST(expr AS type)
			if val, ok := evaluateCast(expr, row); ok {
				projected[alias] = val
				continue
			}
			if value, ok := row[expr]; ok {
				projected[alias] = value
			}
		}
		result = append(result, projected)
	}

	if plan.Distinct {
		result = deduplicateRows(result)
	}

	return result, nil
}

func qualifySingleTableRow(table *tableState, plan planner.Plan, row map[string]ast.Literal) map[string]ast.Literal {
	if len(row) == 0 {
		return row
	}
	prefix := displayPrefix(plan.TableName, plan.TableAlias)
	if prefix == "" {
		return row
	}
	return prefixRowWithNames(qualifiedColumnNames(prefix, table.columns), prefix, row)
}

func stripQualifiedColumns(row map[string]ast.Literal) map[string]ast.Literal {
	result := make(map[string]ast.Literal, len(row))
	for key, value := range row {
		if strings.Contains(key, ".") {
			continue
		}
		result[key] = value
	}
	return result
}

func parseQualifiedStarColumn(column string) (string, bool) {
	trimmed := strings.TrimSpace(strings.ToLower(column))
	if len(trimmed) <= 2 || !strings.HasSuffix(trimmed, ".*") {
		return "", false
	}
	prefix := strings.TrimSpace(strings.TrimSuffix(trimmed, ".*"))
	if prefix == "" {
		return "", false
	}
	return prefix, true
}

func expandQualifiedStar(projected map[string]ast.Literal, row map[string]ast.Literal, table *tableState, plan planner.Plan, prefix string) {
	if len(plan.Joins) == 0 {
		basePrefix := strings.ToLower(displayPrefix(plan.TableName, plan.TableAlias))
		baseTableName := strings.ToLower(plan.TableName)
		if prefix == basePrefix || prefix == baseTableName {
			for _, col := range table.columns {
				if val, ok := row[col]; ok {
					projected[col] = val
				}
			}
			return
		}
	}

	qualifiedPrefix := prefix + "."
	matchedColumns := make([]string, 0)
	for key := range row {
		if !strings.HasPrefix(strings.ToLower(key), qualifiedPrefix) {
			continue
		}
		columnName := key[len(qualifiedPrefix):]
		if columnName == "" || strings.Contains(columnName, ".") {
			continue
		}
		matchedColumns = append(matchedColumns, columnName)
	}
	sort.Strings(matchedColumns)
	for _, columnName := range matchedColumns {
		qualifiedName := qualifiedPrefix + columnName
		if val, ok := row[qualifiedName]; ok {
			projected[columnName] = val
		}
	}
}

func deduplicateRows(rows []map[string]ast.Literal) []map[string]ast.Literal {
	if len(rows) <= 1 {
		return rows
	}
	seen := make(map[string]struct{}, len(rows))
	deduped := make([]map[string]ast.Literal, 0, len(rows))
	for _, row := range rows {
		key := rowDeduplicationKey(row)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		deduped = append(deduped, row)
	}
	return deduped
}

func rowDeduplicationKey(row map[string]ast.Literal) string {
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+literalKey(row[k]))
	}
	return strings.Join(parts, "|")
}

func valueOrNull(row map[string]ast.Literal, column string) ast.Literal {
	value, exists := row[column]
	if !exists {
		return ast.Literal{Kind: ast.LiteralNull}
	}

	return value
}

func requiresAggregation(plan planner.Plan) bool {
	if len(plan.GroupBy) > 0 || plan.Having != nil {
		return true
	}

	for _, column := range plan.Columns {
		if _, ok := parseAggregateSelectColumn(column); ok {
			return true
		}
	}

	return false
}

func aggregateRows(rows []map[string]ast.Literal, plan planner.Plan, state *readableState, engine *Engine) ([]map[string]ast.Literal, error) {
	aggregateSpecs := make(map[string]aggregateSelectSpec)
	hasAggregate := false
	for _, column := range plan.Columns {
		if spec, ok := parseAggregateSelectColumn(column); ok {
			hasAggregate = true
			aggregateSpecs[column] = spec
		}
	}

	groupBySet := make(map[string]struct{}, len(plan.GroupBy))
	for _, column := range plan.GroupBy {
		groupBySet[column] = struct{}{}
	}

	if len(plan.GroupBy) == 0 && !hasAggregate {
		return nil, errors.New("HAVING/GROUP BY requires aggregate projection")
	}

	if hasAggregate {
		for _, column := range plan.Columns {
			if _, isAggregate := aggregateSpecs[column]; isAggregate {
				continue
			}
			expr, _ := stripColumnAlias(column)
			if len(plan.GroupBy) == 0 {
				return nil, fmt.Errorf("non-aggregated column %s requires GROUP BY", expr)
			}
			if _, ok := groupBySet[expr]; !ok {
				return nil, fmt.Errorf("non-aggregated column %s must appear in GROUP BY", expr)
			}
		}
	}

	type groupState struct {
		rows       []map[string]ast.Literal
		groupValue map[string]ast.Literal
	}

	groups := make(map[string]*groupState)
	if len(plan.GroupBy) == 0 {
		groups["__all__"] = &groupState{rows: rows, groupValue: make(map[string]ast.Literal)}
	} else {
		for _, row := range rows {
			key := buildGroupKey(row, plan.GroupBy)
			group := groups[key]
			if group == nil {
				values := make(map[string]ast.Literal, len(plan.GroupBy))
				for _, column := range plan.GroupBy {
					values[column] = valueOrNull(row, column)
				}
				group = &groupState{rows: make([]map[string]ast.Literal, 0), groupValue: values}
				groups[key] = group
			}
			group.rows = append(group.rows, row)
		}
	}

	groupKeys := make([]string, 0, len(groups))
	for key := range groups {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)

	result := make([]map[string]ast.Literal, 0, len(groupKeys))
	for _, key := range groupKeys {
		group := groups[key]
		projected := make(map[string]ast.Literal, len(plan.Columns))

		for _, column := range plan.Columns {
			expr, alias := stripColumnAlias(column)
			if spec, ok := aggregateSpecs[column]; ok {
				value, err := computeAggregate(spec, group.rows)
				if err != nil {
					return nil, err
				}
				projected[spec.OutputColumn] = value
				continue
			}

			if len(plan.GroupBy) > 0 {
				projected[alias] = group.groupValue[expr]
				continue
			}

			if len(group.rows) == 0 {
				projected[alias] = ast.Literal{Kind: ast.LiteralNull}
				continue
			}
			projected[alias] = valueOrNull(group.rows[0], expr)
		}

		if plan.Having != nil && !matchPredicate(projected, plan.Having, state, engine) {
			continue
		}

		result = append(result, projected)
	}

	return result, nil
}

func buildGroupKey(row map[string]ast.Literal, groupBy []string) string {
	if len(groupBy) == 0 {
		return "__all__"
	}

	parts := make([]string, 0, len(groupBy))
	for _, column := range groupBy {
		parts = append(parts, column+"="+literalKey(valueOrNull(row, column)))
	}

	return strings.Join(parts, "|")
}

func parseAggregateSelectColumn(column string) (aggregateSelectSpec, bool) {
	expr, alias := stripColumnAlias(column)
	canonical := strings.TrimSpace(strings.ToLower(expr))
	open := strings.Index(canonical, "(")
	close := strings.LastIndex(canonical, ")")
	if open <= 0 || close <= open || close != len(canonical)-1 {
		return aggregateSelectSpec{}, false
	}

	function := strings.TrimSpace(canonical[:open])
	argument := strings.TrimSpace(canonical[open+1 : close])
	spec := aggregateSelectSpec{OutputColumn: alias, Function: function, Argument: argument}

	switch function {
	case "count":
		if argument == "*" {
			spec.CountAll = true
			return spec, true
		}
		if argument == "" {
			return aggregateSelectSpec{}, false
		}
		return spec, true
	case "sum", "avg", "min", "max":
		if argument == "" || argument == "*" {
			return aggregateSelectSpec{}, false
		}
		return spec, true
	default:
		return aggregateSelectSpec{}, false
	}
}

func computeAggregate(spec aggregateSelectSpec, rows []map[string]ast.Literal) (ast.Literal, error) {
	switch spec.Function {
	case "count":
		if spec.CountAll {
			return ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(len(rows))}, nil
		}

		count := int64(0)
		for _, row := range rows {
			value := valueOrNull(row, spec.Argument)
			if value.Kind == ast.LiteralNull {
				continue
			}
			count++
		}
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: count}, nil
	case "sum":
		hasValue := false
		hasFloat := false
		totalInt := int64(0)
		totalFloat := float64(0)
		for _, row := range rows {
			value := valueOrNull(row, spec.Argument)
			if value.Kind == ast.LiteralNull {
				continue
			}
			if value.Kind == ast.LiteralFloat {
				hasValue = true
				hasFloat = true
				totalFloat += value.FloatValue
			} else if value.Kind == ast.LiteralNumber {
				hasValue = true
				totalInt += value.NumberValue
			} else {
				return ast.Literal{}, fmt.Errorf("SUM requires numeric column %s", spec.Argument)
			}
		}
		if !hasValue {
			return ast.Literal{Kind: ast.LiteralNull}, nil
		}
		if hasFloat {
			return ast.Literal{Kind: ast.LiteralFloat, FloatValue: totalFloat + float64(totalInt)}, nil
		}
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: totalInt}, nil
	case "avg":
		count := int64(0)
		hasFloat := false
		totalInt := int64(0)
		totalFloat := float64(0)
		for _, row := range rows {
			value := valueOrNull(row, spec.Argument)
			if value.Kind == ast.LiteralNull {
				continue
			}
			if value.Kind == ast.LiteralFloat {
				hasFloat = true
				totalFloat += value.FloatValue
				count++
			} else if value.Kind == ast.LiteralNumber {
				totalInt += value.NumberValue
				count++
			} else {
				return ast.Literal{}, fmt.Errorf("AVG requires numeric column %s", spec.Argument)
			}
		}
		if count == 0 {
			return ast.Literal{Kind: ast.LiteralNull}, nil
		}
		if hasFloat {
			return ast.Literal{Kind: ast.LiteralFloat, FloatValue: (totalFloat + float64(totalInt)) / float64(count)}, nil
		}
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: totalInt / count}, nil
	case "min":
		var best ast.Literal
		hasBest := false
		for _, row := range rows {
			value := valueOrNull(row, spec.Argument)
			if value.Kind == ast.LiteralNull {
				continue
			}
			if !hasBest || compareLiterals(value, best) < 0 {
				best = value
				hasBest = true
			}
		}
		if !hasBest {
			return ast.Literal{Kind: ast.LiteralNull}, nil
		}
		return best, nil
	case "max":
		var best ast.Literal
		hasBest := false
		for _, row := range rows {
			value := valueOrNull(row, spec.Argument)
			if value.Kind == ast.LiteralNull {
				continue
			}
			if !hasBest || compareLiterals(value, best) > 0 {
				best = value
				hasBest = true
			}
		}
		if !hasBest {
			return ast.Literal{Kind: ast.LiteralNull}, nil
		}
		return best, nil
	default:
		return ast.Literal{}, fmt.Errorf("unsupported aggregate function %s", spec.Function)
	}
}

// executeSubquery runs a subquery SELECT statement against the current state.
func executeSubquery(sub *ast.Subquery, state *readableState, engine *Engine) ([]map[string]ast.Literal, error) {
	if state == nil || engine == nil {
		return nil, fmt.Errorf("subquery requires engine context")
	}

	// Determine available domains from state.
	txDomains := make([]string, 0, len(state.domains))
	for d := range state.domains {
		txDomains = append(txDomains, d)
	}

	plan, err := planner.BuildForDomains(sub.Statement, txDomains)
	if err != nil {
		return nil, fmt.Errorf("subquery plan: %w", err)
	}

	rows, err := engine.selectRows(context.Background(), state, plan)
	if err != nil {
		return nil, fmt.Errorf("subquery exec: %w", err)
	}

	return rows, nil
}

// evaluateExistsSubquery evaluates EXISTS (SELECT ...).
func evaluateExistsSubquery(sub *ast.Subquery, state *readableState, engine *Engine) ternaryResult {
	rows, err := executeSubquery(sub, state, engine)
	if err != nil {
		return ternaryFalse
	}
	if len(rows) > 0 {
		return ternaryTrue
	}
	return ternaryFalse
}

// evaluateInSubquery evaluates col IN (SELECT ...) or col NOT IN (SELECT ...).
// Implements proper SQL NULL semantics for IN/NOT IN.
func evaluateInSubquery(row map[string]ast.Literal, predicate *ast.Predicate, state *readableState, engine *Engine, negate bool) ternaryResult {
	value, exists := resolvePredicateOperand(row, predicate.Column)
	if !exists {
		return ternaryFalse
	}

	// NULL IN (...) → UNKNOWN
	if value.Kind == ast.LiteralNull {
		return ternaryUnknown
	}

	subRows, err := executeSubquery(predicate.Subquery, state, engine)
	if err != nil {
		return ternaryFalse
	}

	// col IN (empty) → FALSE, col NOT IN (empty) → TRUE
	if len(subRows) == 0 {
		if negate {
			return ternaryTrue
		}
		return ternaryFalse
	}

	// Get the first column name from subquery results.
	var subCol string
	for k := range subRows[0] {
		subCol = k
		break
	}

	found := false
	hasNull := false
	for _, sr := range subRows {
		sv := sr[subCol]
		if sv.Kind == ast.LiteralNull {
			hasNull = true
			continue
		}
		if compareLiteralByOperator(value, "=", sv) {
			found = true
			break
		}
	}

	if negate {
		// NOT IN
		if found {
			return ternaryFalse
		}
		if hasNull {
			return ternaryUnknown
		}
		return ternaryTrue
	}

	// IN
	if found {
		return ternaryTrue
	}
	if hasNull {
		return ternaryUnknown
	}
	return ternaryFalse
}

// evaluateScalarSubquery evaluates col op (SELECT ... LIMIT 1).
func evaluateScalarSubquery(value ast.Literal, operator string, sub *ast.Subquery, state *readableState, engine *Engine) ternaryResult {
	subRows, err := executeSubquery(sub, state, engine)
	if err != nil {
		return ternaryFalse
	}

	if len(subRows) == 0 {
		return ternaryUnknown
	}
	if len(subRows) > 1 {
		return ternaryFalse
	}

	// Get scalar value from single row.
	var subVal ast.Literal
	for _, v := range subRows[0] {
		subVal = v
		break
	}

	if subVal.Kind == ast.LiteralNull {
		return ternaryUnknown
	}

	if compareLiteralByOperator(value, operator, subVal) {
		return ternaryTrue
	}
	return ternaryFalse
}

func cloneRow(input map[string]ast.Literal) map[string]ast.Literal {
	clone := make(map[string]ast.Literal, len(input))
	for key, value := range input {
		clone[key] = value
	}
	return clone
}

func applyWindowFunctions(rows []map[string]ast.Literal, windowFunctions []ast.WindowFunction) []map[string]ast.Literal {
	if len(windowFunctions) == 0 || len(rows) == 0 {
		return rows
	}

	for _, wf := range windowFunctions {
		partitions := windowPartitionRows(rows, wf.Partition)

		for pi := range partitions {
			windowSortPartition(rows, partitions[pi].indices, wf.OrderBy)
		}

		funcName := strings.ToUpper(wf.Function)
		alias := strings.ToLower(wf.Alias)

		switch funcName {
		case "ROW_NUMBER":
			for _, p := range partitions {
				for i, idx := range p.indices {
					rows[idx][alias] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(i + 1)}
				}
			}
		case "RANK":
			for _, p := range partitions {
				for i, idx := range p.indices {
					rank := int64(1)
					if i > 0 {
						prevIdx := p.indices[i-1]
						if windowOrderByEqual(rows[prevIdx], rows[idx], wf.OrderBy) {
							rank = rows[prevIdx][alias].NumberValue
						} else {
							rank = int64(i + 1)
						}
					}
					rows[idx][alias] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: rank}
				}
			}
		case "LAG":
			argCol := ""
			if len(wf.Args) > 0 {
				argCol = strings.ToLower(wf.Args[0])
			}
			for _, p := range partitions {
				for i, idx := range p.indices {
					if i == 0 {
						rows[idx][alias] = ast.Literal{Kind: ast.LiteralNull}
					} else {
						prevIdx := p.indices[i-1]
						rows[idx][alias] = valueOrNull(rows[prevIdx], argCol)
					}
				}
			}
		case "LEAD":
			argCol := ""
			if len(wf.Args) > 0 {
				argCol = strings.ToLower(wf.Args[0])
			}
			for _, p := range partitions {
				for i, idx := range p.indices {
					if i == len(p.indices)-1 {
						rows[idx][alias] = ast.Literal{Kind: ast.LiteralNull}
					} else {
						nextIdx := p.indices[i+1]
						rows[idx][alias] = valueOrNull(rows[nextIdx], argCol)
					}
				}
			}
		}
	}

	return rows
}

func windowPartitionRows(rows []map[string]ast.Literal, partitionBy []string) []windowPartition {
	if len(partitionBy) == 0 {
		indices := make([]int, len(rows))
		for i := range rows {
			indices[i] = i
		}
		return []windowPartition{{indices: indices}}
	}

	groups := make(map[string][]int)
	groupOrder := make([]string, 0, 8)
	for i, row := range rows {
		key := buildGroupKey(row, partitionBy)
		if _, exists := groups[key]; !exists {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], i)
	}

	partitions := make([]windowPartition, 0, len(groupOrder))
	for _, key := range groupOrder {
		partitions = append(partitions, windowPartition{indices: groups[key]})
	}
	return partitions
}

func windowSortPartition(rows []map[string]ast.Literal, indices []int, orderBy []ast.OrderByClause) {
	if len(orderBy) == 0 || len(indices) <= 1 {
		return
	}

	sort.SliceStable(indices, func(i, j int) bool {
		left := rows[indices[i]]
		right := rows[indices[j]]
		for _, clause := range orderBy {
			lv := valueOrNull(left, clause.Column)
			rv := valueOrNull(right, clause.Column)
			cmp := compareLiterals(lv, rv)
			if cmp == 0 {
				continue
			}
			if clause.Direction == ast.SortDesc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
}

func windowOrderByEqual(a, b map[string]ast.Literal, orderBy []ast.OrderByClause) bool {
	for _, clause := range orderBy {
		av := valueOrNull(a, clause.Column)
		bv := valueOrNull(b, clause.Column)
		if compareLiterals(av, bv) != 0 {
			return false
		}
	}
	return true
}

// executeQueryPlan dispatches to selectRows for normal SELECTs or
// executeSetOp for UNION / INTERSECT / EXCEPT plans.
func (engine *Engine) executeQueryPlan(ctx context.Context, state *readableState, plan planner.Plan) ([]map[string]ast.Literal, error) {
	if plan.Operation == planner.OperationSetOp {
		return engine.executeSetOp(ctx, state, plan)
	}
	return engine.selectRows(ctx, state, plan)
}

// executeSetOp executes both sides of a set operation and combines them.
func (engine *Engine) executeSetOp(ctx context.Context, state *readableState, plan planner.Plan) ([]map[string]ast.Literal, error) {
	if plan.LeftPlan == nil || plan.RightPlan == nil {
		return nil, errors.New("set operation requires both left and right plans")
	}

	leftRows, err := engine.executeQueryPlan(ctx, state, *plan.LeftPlan)
	if err != nil {
		return nil, fmt.Errorf("execute left side of %s: %w", plan.SetOp, err)
	}

	rightRows, err := engine.executeQueryPlan(ctx, state, *plan.RightPlan)
	if err != nil {
		return nil, fmt.Errorf("execute right side of %s: %w", plan.SetOp, err)
	}

	switch plan.SetOp {
	case ast.SetOpUnion:
		return setUnion(leftRows, rightRows, true), nil
	case ast.SetOpUnionAll:
		return setUnion(leftRows, rightRows, false), nil
	case ast.SetOpIntersect:
		return setIntersect(leftRows, rightRows), nil
	case ast.SetOpExcept:
		return setExcept(leftRows, rightRows), nil
	default:
		return nil, fmt.Errorf("unsupported set operation: %s", plan.SetOp)
	}
}

// rowKey builds a deterministic string key for a row by sorting column names
// and concatenating their literal keys. Used for set operation deduplication.
func rowKey(row map[string]ast.Literal) string {
	cols := make([]string, 0, len(row))
	for col := range row {
		cols = append(cols, col)
	}
	sort.Strings(cols)
	var b strings.Builder
	for i, col := range cols {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(col)
		b.WriteByte('=')
		b.WriteString(literalKey(row[col]))
	}
	return b.String()
}

// setUnion returns all rows from both sets. If deduplicate is true,
// duplicate rows are removed (UNION); otherwise kept (UNION ALL).
func setUnion(left, right []map[string]ast.Literal, deduplicate bool) []map[string]ast.Literal {
	result := make([]map[string]ast.Literal, 0, len(left)+len(right))
	if !deduplicate {
		result = append(result, left...)
		result = append(result, right...)
		return result
	}
	seen := make(map[string]struct{}, len(left)+len(right))
	for _, row := range left {
		key := rowKey(row)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	for _, row := range right {
		key := rowKey(row)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}

// setIntersect returns rows that appear in both sets.
func setIntersect(left, right []map[string]ast.Literal) []map[string]ast.Literal {
	rightSet := make(map[string]struct{}, len(right))
	for _, row := range right {
		rightSet[rowKey(row)] = struct{}{}
	}
	seen := make(map[string]struct{})
	result := make([]map[string]ast.Literal, 0)
	for _, row := range left {
		key := rowKey(row)
		if _, exists := rightSet[key]; exists {
			if _, already := seen[key]; !already {
				seen[key] = struct{}{}
				result = append(result, row)
			}
		}
	}
	return result
}

// setExcept returns rows from left that do not appear in right.
func setExcept(left, right []map[string]ast.Literal) []map[string]ast.Literal {
	rightSet := make(map[string]struct{}, len(right))
	for _, row := range right {
		rightSet[rowKey(row)] = struct{}{}
	}
	seen := make(map[string]struct{})
	result := make([]map[string]ast.Literal, 0)
	for _, row := range left {
		key := rowKey(row)
		if _, exists := rightSet[key]; !exists {
			if _, already := seen[key]; !already {
				seen[key] = struct{}{}
				result = append(result, row)
			}
		}
	}
	return result
}

// formatLogicalTimestamp converts a deterministic logical timestamp (microseconds
// since epoch) to an ISO 8601 string suitable for query results.
func formatLogicalTimestamp(ts uint64) string {
	t := time.UnixMicro(int64(ts))
	return t.UTC().Format(time.RFC3339Nano)
}
