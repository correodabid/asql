package executor

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
)

// applyImports clones the readable state and injects imported tables from
// other domains. Each imported table becomes accessible by its bare name
// (or alias) in every domain, similar to CTE injection.
func applyImports(state *readableState, imports []ast.ImportDirective) (*readableState, error) {
	if len(imports) == 0 {
		return state, nil
	}

	newDomains := make(map[string]*domainState, len(state.domains))
	for k, v := range state.domains {
		newTables := make(map[string]*tableState, len(v.tables))
		for tk, tv := range v.tables {
			newTables[tk] = tv
		}
		newDomains[k] = &domainState{tables: newTables}
	}
	newState := &readableState{domains: newDomains, headLSN: state.headLSN}

	for _, imp := range imports {
		srcDS, ok := newState.domains[imp.SourceDomain]
		if !ok {
			return nil, fmt.Errorf("import: domain %q not found", imp.SourceDomain)
		}
		srcTable, ok := srcDS.tables[imp.SourceTable]
		if !ok {
			return nil, fmt.Errorf("import: table %s.%s not found", imp.SourceDomain, imp.SourceTable)
		}

		name := imp.SourceTable
		if imp.Alias != "" {
			name = imp.Alias
		}
		for _, ds := range newState.domains {
			ds.tables[name] = srcTable
		}
	}

	return newState, nil
}

// materializeCTEs executes each CTE SELECT and injects the results as virtual tables
// in the domain state, so the main query can reference them by name.
func materializeCTEs(state *readableState, ctes []ast.CTE, engine *Engine) *readableState {
	// Create a deep-enough copy of state so we don't mutate the original
	newDomains := make(map[string]*domainState, len(state.domains))
	for k, v := range state.domains {
		// Copy each domainState with a new tables map
		newTables := make(map[string]*tableState, len(v.tables))
		for tk, tv := range v.tables {
			newTables[tk] = tv
		}
		newDomains[k] = &domainState{tables: newTables}
	}
	newState := &readableState{domains: newDomains}

	for _, cte := range ctes {
		// Build a plan for the CTE's SELECT
		ctePlan, err := planner.BuildForDomains(cte.Statement, domainsFromState(newState))
		if err != nil {
			continue
		}
		cteRows, err := engine.selectRows(context.Background(), newState, ctePlan)
		if err != nil || len(cteRows) == 0 {
			// Create empty virtual table
			cols := cte.Statement.Columns
			vt := &tableState{
				columns:           cols,
				columnDefinitions: make(map[string]ast.ColumnDefinition),
				rows:              make([][]ast.Literal, 0),
				columnIndex:       make(map[string]int),
				indexes:           make(map[string]*indexState),
				indexedColumns:    make(map[string]string),
				indexedColumnSets: make(map[string]string),
				uniqueColumns:     make(map[string]struct{}),
			}
			injectVirtualTable(newState, cte.Name, vt)
			continue
		}

		// Derive column names from first row
		columns := make([]string, 0, len(cteRows[0]))
		for col := range cteRows[0] {
			columns = append(columns, col)
		}
		sort.Strings(columns)

		// Build columnIndex and convert cteRows (maps) to positional slices.
		colIdx := make(map[string]int, len(columns))
		for i, col := range columns {
			colIdx[col] = i
		}
		vtRows := make([][]ast.Literal, len(cteRows))
		for i, m := range cteRows {
			vtRows[i] = rowFromMap(columns, m)
		}

		vt := &tableState{
			columns:           columns,
			columnDefinitions: make(map[string]ast.ColumnDefinition),
			rows:              vtRows,
			columnIndex:       colIdx,
			indexes:           make(map[string]*indexState),
			indexedColumns:    make(map[string]string),
			indexedColumnSets: make(map[string]string),
			uniqueColumns:     make(map[string]struct{}),
		}
		injectVirtualTable(newState, cte.Name, vt)
	}

	return newState
}

func domainsFromState(state *readableState) []string {
	domains := make([]string, 0, len(state.domains))
	for d := range state.domains {
		domains = append(domains, d)
	}
	return domains
}

func injectVirtualTable(state *readableState, tableName string, vt *tableState) {
	// Inject into every domain so the CTE can be referenced from any domain context
	for _, ds := range state.domains {
		ds.tables[tableName] = vt
	}
}

func nullFilledRow(columns []string) map[string]ast.Literal {
	row := make(map[string]ast.Literal, len(columns))
	for _, col := range columns {
		row[col] = ast.Literal{Kind: ast.LiteralNull}
	}
	return row
}

// buildAliasMap creates a mapping from alias (or table name) to the display prefix
// used in qualified row keys. When aliases are present, the alias is used as the
// prefix; otherwise the table name itself is used.
func buildAliasMap(domainName, tableName, tableAlias string, joins []ast.JoinClause) map[string]string {
	m := make(map[string]string, 2+len(joins)*2)
	registerAliasMapping(m, domainName, tableName, tableAlias)
	for _, j := range joins {
		registerAliasMapping(m, j.DomainName, j.TableName, j.Alias)
	}
	return m
}

func registerAliasMapping(target map[string]string, domainName, tableName, alias string) {
	if alias != "" {
		target[alias] = alias
		target[tableName] = alias
	} else {
		target[tableName] = tableName
	}
	domainName = strings.TrimSpace(strings.ToLower(domainName))
	tableName = strings.TrimSpace(strings.ToLower(tableName))
	alias = strings.TrimSpace(strings.ToLower(alias))
	if domainName != "" && tableName != "" {
		qualified := domainName + "." + tableName
		if alias != "" {
			target[qualified] = alias
		} else {
			target[qualified] = tableName
		}
	}
}

// displayPrefix returns the prefix to use for qualified row keys.
// If an alias is set it returns the alias; otherwise the table name.
func displayPrefix(tableName, alias string) string {
	if alias != "" {
		return alias
	}
	return tableName
}

// qualifiedColumnNames pre-computes "prefix.col" strings for a table's columns.
// Avoids repeated string concatenation in the join hot path.
func qualifiedColumnNames(prefix string, columns []string) map[string]string {
	qualified := make(map[string]string, len(columns))
	for _, col := range columns {
		qualified[col] = prefix + "." + col
	}
	return qualified
}

// prefixRow creates a new row with keys prefixed by the given prefix ("prefix.col").
// It also keeps unqualified keys for columns that don't conflict.
func prefixRow(prefix string, row map[string]ast.Literal) map[string]ast.Literal {
	out := make(map[string]ast.Literal, len(row)*2)
	for col, val := range row {
		out[prefix+"."+col] = val
		if _, exists := out[col]; !exists {
			out[col] = val
		}
	}
	return out
}

// prefixRowWithNames creates a new row using pre-computed qualified column names.
// Faster than prefixRow when processing many rows with the same columns.
func prefixRowWithNames(qualifiedNames map[string]string, prefix string, row map[string]ast.Literal) map[string]ast.Literal {
	out := make(map[string]ast.Literal, len(row)*2)
	for col, val := range row {
		if qName, ok := qualifiedNames[col]; ok {
			out[qName] = val
		} else {
			out[prefix+"."+col] = val
		}
		if _, exists := out[col]; !exists {
			out[col] = val
		}
	}
	return out
}

func baseJoinPredicate(plan planner.Plan, aliasMap map[string]string, baseTable *tableState) *ast.Predicate {
	basePrefix := displayPrefix(plan.TableName, plan.TableAlias)
	return bestBaseJoinPredicate(plan.Filter, aliasMap, plan.TableName, basePrefix, baseTable)
}

func bestBaseJoinPredicate(predicate *ast.Predicate, aliasMap map[string]string, baseTableName string, basePrefix string, baseTable *tableState) *ast.Predicate {
	if predicate == nil {
		return nil
	}

	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "AND":
		left := bestBaseJoinPredicate(predicate.Left, aliasMap, baseTableName, basePrefix, baseTable)
		right := bestBaseJoinPredicate(predicate.Right, aliasMap, baseTableName, basePrefix, baseTable)
		switch {
		case left == nil:
			return right
		case right == nil:
			return left
		default:
			candidate := &ast.Predicate{Operator: "AND", Left: left, Right: right}
			if chooseSingleTableScanStrategy(baseTable, candidate, nil) == scanStrategyFullScan {
				return preferredBaseJoinPredicate(baseTable, left, right)
			}
			return candidate
		}
	case "OR":
		left := bestBaseJoinPredicate(predicate.Left, aliasMap, baseTableName, basePrefix, baseTable)
		right := bestBaseJoinPredicate(predicate.Right, aliasMap, baseTableName, basePrefix, baseTable)
		if left == nil {
			left = localBaseJoinPredicate(predicate.Left, aliasMap, baseTableName, basePrefix, baseTable)
		}
		if right == nil {
			right = localBaseJoinPredicate(predicate.Right, aliasMap, baseTableName, basePrefix, baseTable)
		}
		if left == nil || right == nil {
			return nil
		}
		candidate := &ast.Predicate{Operator: "OR", Left: left, Right: right}
		if chooseSingleTableScanStrategy(baseTable, candidate, nil) == scanStrategyFullScan {
			return nil
		}
		return candidate
	case "NOT":
		child := bestBaseJoinPredicate(predicate.Left, aliasMap, baseTableName, basePrefix, baseTable)
		if child == nil {
			return nil
		}
		candidate := &ast.Predicate{Operator: "NOT", Left: child}
		if chooseSingleTableScanStrategy(baseTable, candidate, nil) == scanStrategyFullScan {
			return nil
		}
		return candidate
	}

	if !isSimplePredicate(predicate) {
		return nil
	}

	column, ok := normalizeBasePredicateColumn(predicate.Column, aliasMap, baseTableName, basePrefix, baseTable)
	if !ok {
		return nil
	}

	candidate := &ast.Predicate{
		Column:   column,
		Operator: predicate.Operator,
		Value:    predicate.Value,
		Value2:   predicate.Value2,
		InValues: predicate.InValues,
		Subquery: predicate.Subquery,
	}

	if chooseSingleTableScanStrategy(baseTable, candidate, nil) == scanStrategyFullScan {
		return nil
	}

	return candidate
}

func localBaseJoinPredicate(predicate *ast.Predicate, aliasMap map[string]string, baseTableName string, basePrefix string, baseTable *tableState) *ast.Predicate {
	if predicate == nil {
		return nil
	}

	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "AND", "OR":
		left := localBaseJoinPredicate(predicate.Left, aliasMap, baseTableName, basePrefix, baseTable)
		right := localBaseJoinPredicate(predicate.Right, aliasMap, baseTableName, basePrefix, baseTable)
		if left == nil || right == nil {
			return nil
		}
		return &ast.Predicate{Operator: strings.ToUpper(strings.TrimSpace(predicate.Operator)), Left: left, Right: right}
	case "NOT":
		child := localBaseJoinPredicate(predicate.Left, aliasMap, baseTableName, basePrefix, baseTable)
		if child == nil {
			return nil
		}
		return &ast.Predicate{Operator: "NOT", Left: child}
	}

	if !isSimplePredicate(predicate) {
		return nil
	}

	column, ok := normalizeBasePredicateColumn(predicate.Column, aliasMap, baseTableName, basePrefix, baseTable)
	if !ok {
		return nil
	}

	return &ast.Predicate{
		Column:   column,
		Operator: predicate.Operator,
		Value:    predicate.Value,
		Value2:   predicate.Value2,
		InValues: predicate.InValues,
		Subquery: predicate.Subquery,
	}
}

func preferredBaseJoinPredicate(baseTable *tableState, left *ast.Predicate, right *ast.Predicate) *ast.Predicate {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}

	leftRank := rankBaseJoinPredicate(baseTable, left)
	if rightRank := rankBaseJoinPredicate(baseTable, right); rightRank > leftRank {
		return right
	}
	return left
}

func rankBaseJoinPredicate(baseTable *tableState, predicate *ast.Predicate) int {
	switch chooseSingleTableScanStrategy(baseTable, predicate, nil) {
	case scanStrategyHashLookup:
		return 3
	case scanStrategyBTreeLookup:
		return 2
	default:
		return 1
	}
}

func rootJoinPredicate(plan planner.Plan, aliasMap map[string]string, baseTable *tableState) *ast.Predicate {
	basePrefix := displayPrefix(plan.TableName, plan.TableAlias)
	return conjunctiveRootJoinPredicate(plan.Filter, aliasMap, plan.TableName, basePrefix, baseTable)
}

func conjunctiveRootJoinPredicate(predicate *ast.Predicate, aliasMap map[string]string, baseTableName string, basePrefix string, baseTable *tableState) *ast.Predicate {
	if predicate == nil {
		return nil
	}

	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "AND":
		left := conjunctiveRootJoinPredicate(predicate.Left, aliasMap, baseTableName, basePrefix, baseTable)
		right := conjunctiveRootJoinPredicate(predicate.Right, aliasMap, baseTableName, basePrefix, baseTable)
		switch {
		case left == nil:
			return right
		case right == nil:
			return left
		default:
			return &ast.Predicate{Operator: "AND", Left: left, Right: right}
		}
	case "OR":
		left := conjunctiveRootJoinPredicate(predicate.Left, aliasMap, baseTableName, basePrefix, baseTable)
		right := conjunctiveRootJoinPredicate(predicate.Right, aliasMap, baseTableName, basePrefix, baseTable)
		if left == nil || right == nil {
			return nil
		}
		return &ast.Predicate{Operator: "OR", Left: left, Right: right}
	case "NOT":
		child := conjunctiveRootJoinPredicate(predicate.Left, aliasMap, baseTableName, basePrefix, baseTable)
		if child == nil {
			return nil
		}
		return &ast.Predicate{Operator: "NOT", Left: child}
	}

	if !isSimplePredicate(predicate) {
		return nil
	}

	column, ok := normalizeBasePredicateColumn(predicate.Column, aliasMap, baseTableName, basePrefix, baseTable)
	if !ok {
		return nil
	}

	return &ast.Predicate{
		Column:   column,
		Operator: predicate.Operator,
		Value:    predicate.Value,
		Value2:   predicate.Value2,
		InValues: predicate.InValues,
		Subquery: predicate.Subquery,
	}
}

func predicateRejectsNull(predicate *ast.Predicate) bool {
	if predicate == nil {
		return false
	}

	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "AND":
		return predicateRejectsNull(predicate.Left) && predicateRejectsNull(predicate.Right)
	case "IS NULL", "OR", "NOT":
		return false
	default:
		return true
	}
}

func matchTablePredicateOnRow(table *tableState, row []ast.Literal, predicate *ast.Predicate) bool {
	if predicate == nil {
		return true
	}

	switch strings.ToUpper(strings.TrimSpace(predicate.Operator)) {
	case "AND":
		return matchTablePredicateOnRow(table, row, predicate.Left) && matchTablePredicateOnRow(table, row, predicate.Right)
	case "OR":
		return matchTablePredicateOnRow(table, row, predicate.Left) || matchTablePredicateOnRow(table, row, predicate.Right)
	case "NOT":
		return !matchTablePredicateOnRow(table, row, predicate.Left)
	}

	colPos, ok := table.columnIndex[predicate.Column]
	if !ok || colPos < 0 || colPos >= len(row) {
		return false
	}
	value := row[colPos]

	operator := strings.ToUpper(strings.TrimSpace(predicate.Operator))
	switch operator {
	case "IS NULL", "IS NOT NULL", "=", ">", "<", ">=", "<=":
		return compareLiteralByOperator(value, operator, predicate.Value)
	case "IN":
		return matchLiteralInList(value, predicate.InValues, false)
	case "NOT IN":
		return matchLiteralInList(value, predicate.InValues, true)
	default:
		return false
	}
}

func matchLiteralInList(value ast.Literal, inValues []ast.Literal, negate bool) bool {
	if value.Kind == ast.LiteralNull {
		return false
	}

	found := false
	hasNull := false
	for _, candidate := range inValues {
		if candidate.Kind == ast.LiteralNull {
			hasNull = true
			continue
		}
		if compareLiteralByOperator(value, "=", candidate) {
			found = true
			break
		}
	}

	if negate {
		return !found && !hasNull
	}
	return found
}

func normalizeBasePredicateColumn(column string, aliasMap map[string]string, baseTableName string, basePrefix string, baseTable *tableState) (string, bool) {
	trimmed := strings.TrimSpace(strings.ToLower(column))
	if trimmed == "" {
		return "", false
	}

	prefix, bareColumn, ok := splitQualifiedColumnRef(trimmed)
	if ok {
		resolvedPrefix := prefix
		if aliasPrefix, exists := aliasMap[prefix]; exists {
			resolvedPrefix = aliasPrefix
		}
		if resolvedPrefix != strings.ToLower(basePrefix) {
			return "", false
		}
		if !tableHasColumn(baseTable, bareColumn) {
			return "", false
		}
		return bareColumn, true
	}

	if !tableHasColumn(baseTable, trimmed) {
		return "", false
	}

	for tableName, prefix := range aliasMap {
		if tableName == baseTableName || prefix != tableName {
			continue
		}
		if prefix == basePrefix {
			continue
		}
		return "", false
	}

	return trimmed, true
}

// mergePipelineRows merges a left pipeline row (already prefixed) with a raw right
// table row, prefixing the right columns with rightPrefix.
func mergePipelineRows(left map[string]ast.Literal, rightPrefix string, right map[string]ast.Literal) map[string]ast.Literal {
	merged := make(map[string]ast.Literal, len(left)+len(right)*2)
	for k, v := range left {
		merged[k] = v
	}
	for col, val := range right {
		qualified := rightPrefix + "." + col
		merged[qualified] = val
		if _, exists := merged[col]; !exists {
			merged[col] = val
		}
	}
	return merged
}

// resolveJoinColumnRef resolves a column reference like "o.id" against the alias
// map to find which prefix it maps to in the pipeline rows. It returns the key
// to use when looking up the value in pipeline rows.
func resolveJoinColumnRef(colRef string, aliasMap map[string]string) string {
	colRef = strings.TrimSpace(strings.ToLower(colRef))
	prefixRef, bareColumn, ok := splitQualifiedColumnRef(colRef)
	if !ok {
		return colRef
	}
	prefix, ok := aliasMap[prefixRef]
	if !ok {
		return colRef
	}
	return prefix + "." + bareColumn
}

func splitQualifiedColumnRef(colRef string) (prefix string, column string, ok bool) {
	dot := strings.LastIndex(colRef, ".")
	if dot <= 0 || dot+1 >= len(colRef) {
		return "", "", false
	}
	return colRef[:dot], colRef[dot+1:], true
}

// extractPipelineJoinValue extracts a join column value from a pipeline row.
// The colRef is resolved through the alias map to find the correct key.
func extractPipelineJoinValue(row map[string]ast.Literal, colRef string, aliasMap map[string]string) (ast.Literal, bool) {
	key := resolveJoinColumnRef(colRef, aliasMap)
	val, ok := row[key]
	return val, ok
}

// findVFKForJoin checks if a cross-domain join is backed by a versioned
// foreign key on the base table. Returns the matching VFK constraint or nil.
func findVFKForJoin(baseTable *tableState, j ast.JoinClause, baseDomain string) *versionedForeignKeyConstraint {
	if j.DomainName == "" || j.DomainName == baseDomain {
		return nil
	}
	for i, vfk := range baseTable.versionedForeignKeys {
		if vfk.referencesDomain == j.DomainName && vfk.referencesTable == j.TableName {
			return &baseTable.versionedForeignKeys[i]
		}
	}
	return nil
}

// executeJoinPipeline performs a left-to-right pipeline of JOINs.
// Starting from the base table, each JOIN step produces intermediate rows
// that feed into the next JOIN. WHERE is applied after all JOINs complete.
func (engine *Engine) executeJoinPipeline(ctx context.Context, state *readableState, baseDomainState *domainState, plan planner.Plan) ([]map[string]ast.Literal, scanStrategy, error) {
	aliasMap := buildAliasMap(plan.DomainName, plan.TableName, plan.TableAlias, plan.Joins)

	baseTable, exists := baseDomainState.tables[plan.TableName]
	if !exists {
		return nil, scanStrategyFullScan, errTableNotFound
	}

	// Materialise the base table with prefixed keys.
	basePrefix := displayPrefix(plan.TableName, plan.TableAlias)
	baseQualifiedNames := qualifiedColumnNames(basePrefix, baseTable.columns)
	baseRows := rowsForPredicate(baseTable, baseJoinPredicate(plan, aliasMap, baseTable), state, engine)
	rootPredicate := rootJoinPredicate(plan, aliasMap, baseTable)
	currentRows := make([]map[string]ast.Literal, 0, len(baseRows))
	for _, row := range baseRows {
		if rootPredicate != nil && !matchPredicate(row, rootPredicate, state, engine) {
			continue
		}
		currentRows = append(currentRows, prefixRowWithNames(baseQualifiedNames, basePrefix, row))
	}

	lastStrategy := scanStrategyJoinNested

	for _, j := range plan.Joins {
		// Resolve join table from the correct domain state.
		joinDS := baseDomainState
		if j.DomainName != "" && j.DomainName != plan.DomainName {
			if ds, ok := state.domains[j.DomainName]; ok {
				joinDS = ds
			}
		}
		rightTable, exists := joinDS.tables[j.TableName]
		if !exists {
			return nil, scanStrategyFullScan, errTableNotFound
		}

		joinType := j.JoinType
		if joinType == "" {
			joinType = ast.JoinInner
		}

		rightPrefix := displayPrefix(j.TableName, j.Alias)

		if joinType == ast.JoinCross {
			var nextRows []map[string]ast.Literal
			for _, leftRow := range currentRows {
				for _, rightRowSlice := range rightTable.rows {
					nextRows = append(nextRows, mergePipelineRows(leftRow, rightPrefix, rowToMap(rightTable, rightRowSlice)))
				}
			}
			currentRows = nextRows
			lastStrategy = scanStrategyJoinNested
			continue
		}

		// Resolve ON column references through alias map.
		leftColKey := resolveJoinColumnRef(j.LeftColumn, aliasMap)
		rightColKey := resolveJoinColumnRef(j.RightColumn, aliasMap)

		// Determine which ON column references the right table being joined.
		// The other references the left (pipeline) side.
		rightColName := "" // bare column name in the right table
		var pipelineColKey string
		rightPrefixRef, rightBareCol, rightOK := splitQualifiedColumnRef(rightColKey)
		leftPrefixRef, leftBareCol, leftOK := splitQualifiedColumnRef(leftColKey)

		if rightOK && rightPrefixRef == rightPrefix {
			rightColName = rightBareCol
			pipelineColKey = leftColKey
		} else if leftOK && leftPrefixRef == rightPrefix {
			rightColName = leftBareCol
			pipelineColKey = rightColKey
		} else {
			// Fallback: try both; the one matching the right table is pipelineColKey's complement.
			rightColName = rightBareCol
			pipelineColKey = leftColKey
		}

		// For RIGHT JOIN, swap: iterate over right rows, probe pipeline rows.
		if joinType == ast.JoinRight {
			var nextRows []map[string]ast.Literal
			leftMatched := make(map[int]bool)

			for _, rightRowSlice := range rightTable.rows {
				rightRow := rowToMap(rightTable, rightRowSlice)
				rightVal, rightOK := rightRow[rightColName]
				if !rightOK {
					continue
				}
				matched := false
				for i, leftRow := range currentRows {
					leftVal, leftOK := leftRow[pipelineColKey]
					if !leftOK {
						continue
					}
					if !literalEqual(leftVal, rightVal) {
						continue
					}
					nextRows = append(nextRows, mergePipelineRows(leftRow, rightPrefix, rightRow))
					leftMatched[i] = true
					matched = true
				}
				if !matched {
					// Emit right row with NULLs for all left columns.
					nullLeft := make(map[string]ast.Literal, len(currentRows[0]))
					if len(currentRows) > 0 {
						for k := range currentRows[0] {
							nullLeft[k] = ast.Literal{Kind: ast.LiteralNull}
						}
					}
					nextRows = append(nextRows, mergePipelineRows(nullLeft, rightPrefix, rightRow))
				}
			}
			currentRows = nextRows
			lastStrategy = scanStrategyJoinNested
			continue
		}

		// Entity version cascade: if a previous VFK JOIN resolved via entity version,
		// subsequent JOINs to tables in the same entity also resolve at the same commitLSN.
		if j.DomainName != "" && j.DomainName != plan.DomainName && len(currentRows) > 0 {
			if cascadeDS, cascadeOK := state.domains[j.DomainName]; cascadeOK {
				cascadeEntityName, _ := findEntityForTableInDomain(cascadeDS, j.TableName)
				if cascadeEntityName != "" {
					cascadeKey := "__evs__" + cascadeEntityName
					if _, hasCascade := currentRows[0][cascadeKey]; hasCascade {
						// Collect distinct commitLSNs from pipeline rows.
						cascadeLSNSet := make(map[uint64]struct{})
						for _, row := range currentRows {
							if v, ok := row[cascadeKey]; ok && v.Kind == ast.LiteralNumber {
								cascadeLSNSet[uint64(v.NumberValue)] = struct{}{}
							}
						}
						if len(cascadeLSNSet) > 0 {
							records, err := engine.readAllRecords(ctx)
							if err != nil {
								return nil, scanStrategyFullScan, fmt.Errorf("entity cascade read wal: %w", err)
							}
							cascadeStateCache := make(map[uint64]*readableState, len(cascadeLSNSet))
							for lsn := range cascadeLSNSet {
								s, err := engine.buildStateFromRecords(records, lsn)
								if err != nil {
									return nil, scanStrategyFullScan, fmt.Errorf("entity cascade build state at lsn %d: %w", lsn, err)
								}
								cascadeStateCache[lsn] = s
							}

							var cascadeNextRows []map[string]ast.Literal
							for _, leftRow := range currentRows {
								leftVal, leftOK := leftRow[pipelineColKey]
								if !leftOK {
									if joinType == ast.JoinLeft {
										nullRight := nullFilledRow(tableColumnNames(rightTable))
										cascadeNextRows = append(cascadeNextRows, mergePipelineRows(leftRow, rightPrefix, nullRight))
									}
									continue
								}

								versionedRightTable := rightTable
								if cv, ok := leftRow[cascadeKey]; ok && cv.Kind == ast.LiteralNumber {
									targetLSN := uint64(cv.NumberValue)
									if vs, cached := cascadeStateCache[targetLSN]; cached {
										if vDS, dOk := vs.domains[j.DomainName]; dOk {
											if vT, tOk := vDS.tables[j.TableName]; tOk {
												versionedRightTable = vT
											}
										}
									}
								}

								matched := false
								for _, rRowSlice := range versionedRightTable.rows {
									rRow := rowToMap(versionedRightTable, rRowSlice)
									rVal, rOK := rRow[rightColName]
									if !rOK {
										continue
									}
									if !literalEqual(leftVal, rVal) {
										continue
									}
									cascadeNextRows = append(cascadeNextRows, mergePipelineRows(leftRow, rightPrefix, rRow))
									matched = true
								}

								if !matched && joinType == ast.JoinLeft {
									nullRight := nullFilledRow(tableColumnNames(rightTable))
									cascadeNextRows = append(cascadeNextRows, mergePipelineRows(leftRow, rightPrefix, nullRight))
								}
							}

							currentRows = cascadeNextRows
							lastStrategy = scanStrategyJoinNested
							continue
						}
					}
				}
			}
		}

		// VFK time-travel JOIN: resolve right table per-row at the captured version/LSN.
		vfk := findVFKForJoin(baseTable, j, plan.DomainName)
		if vfk != nil {
			lsnColKey := basePrefix + "." + vfk.lsnColumn
			fkColKey := basePrefix + "." + vfk.column

			// Check if this VFK references an entity table.
			var vfkEntityName string
			if refDS, refOK := state.domains[vfk.referencesDomain]; refOK {
				vfkEntityName, _ = findEntityForTableInDomain(refDS, vfk.referencesTable)
			}

			if vfkEntityName != "" {
				// Entity-aware VFK JOIN: lsn_column stores entity VERSION NUMBER.
				// Resolve per (rootPK, version) → commitLSN.
				type versionKey struct {
					rootPK  string
					version uint64
				}
				refDS := state.domains[vfk.referencesDomain]
				versionToLSN := make(map[versionKey]uint64)
				lsnSet := make(map[uint64]struct{})

				for _, row := range currentRows {
					v, ok := row[lsnColKey]
					if !ok || v.Kind != ast.LiteralNumber {
						continue
					}
					version := uint64(v.NumberValue)
					fkVal, fkOK := row[fkColKey]
					if !fkOK {
						continue
					}
					rootPK := literalKey(fkVal)
					key := versionKey{rootPK, version}
					if _, seen := versionToLSN[key]; seen {
						continue
					}
					commitLSN, ok := resolveEntityVersionCommitLSN(refDS, vfkEntityName, rootPK, version)
					if ok {
						versionToLSN[key] = commitLSN
						lsnSet[commitLSN] = struct{}{}
					}
				}

				if len(lsnSet) > 0 {
					records, err := engine.readAllRecords(ctx)
					if err != nil {
						return nil, scanStrategyFullScan, fmt.Errorf("vfk entity join read wal: %w", err)
					}
					stateCache := make(map[uint64]*readableState, len(lsnSet))
					for lsn := range lsnSet {
						s, err := engine.buildStateFromRecords(records, lsn)
						if err != nil {
							return nil, scanStrategyFullScan, fmt.Errorf("vfk entity join build state at lsn %d: %w", lsn, err)
						}
						stateCache[lsn] = s
					}

					cascadeKey := "__evs__" + vfkEntityName
					var vfkNextRows []map[string]ast.Literal
					for _, leftRow := range currentRows {
						leftVal, leftOK := leftRow[pipelineColKey]
						if !leftOK {
							if joinType == ast.JoinLeft {
								nullRight := nullFilledRow(tableColumnNames(rightTable))
								vfkNextRows = append(vfkNextRows, mergePipelineRows(leftRow, rightPrefix, nullRight))
							}
							continue
						}

						// Resolve commitLSN for this row's (rootPK, version) pair.
						versionedRightTable := rightTable
						var resolvedCommitLSN uint64
						if lsnVal, ok := leftRow[lsnColKey]; ok && lsnVal.Kind == ast.LiteralNumber {
							version := uint64(lsnVal.NumberValue)
							if fkVal, fkOK := leftRow[fkColKey]; fkOK {
								rootPK := literalKey(fkVal)
								key := versionKey{rootPK, version}
								if commitLSN, found := versionToLSN[key]; found {
									resolvedCommitLSN = commitLSN
									if vs, cached := stateCache[commitLSN]; cached {
										if vDS, dOk := vs.domains[j.DomainName]; dOk {
											if vT, tOk := vDS.tables[j.TableName]; tOk {
												versionedRightTable = vT
											}
										}
									}
								}
							}
						}

						// Inject cascade metadata for subsequent JOINs to entity tables.
						leftRow[cascadeKey] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(resolvedCommitLSN)}

						matched := false
						for _, rRowSlice := range versionedRightTable.rows {
							rRow := rowToMap(versionedRightTable, rRowSlice)
							rVal, rOK := rRow[rightColName]
							if !rOK {
								continue
							}
							if !literalEqual(leftVal, rVal) {
								continue
							}
							vfkNextRows = append(vfkNextRows, mergePipelineRows(leftRow, rightPrefix, rRow))
							matched = true
						}

						if !matched && joinType == ast.JoinLeft {
							nullRight := nullFilledRow(tableColumnNames(rightTable))
							vfkNextRows = append(vfkNextRows, mergePipelineRows(leftRow, rightPrefix, nullRight))
						}
					}

					currentRows = vfkNextRows
					lastStrategy = scanStrategyJoinNested
					continue
				}
			} else {
				// No entity — raw LSN mode (current behavior).
				lsnSet := make(map[uint64]struct{})
				for _, row := range currentRows {
					if v, ok := row[lsnColKey]; ok && v.Kind == ast.LiteralNumber {
						lsnSet[uint64(v.NumberValue)] = struct{}{}
					}
				}

				if len(lsnSet) > 0 {
					records, err := engine.readAllRecords(ctx)
					if err != nil {
						return nil, scanStrategyFullScan, fmt.Errorf("vfk join read wal: %w", err)
					}
					stateCache := make(map[uint64]*readableState, len(lsnSet))
					for lsn := range lsnSet {
						s, err := engine.buildStateFromRecords(records, lsn)
						if err != nil {
							return nil, scanStrategyFullScan, fmt.Errorf("vfk join build state at lsn %d: %w", lsn, err)
						}
						stateCache[lsn] = s
					}

					var vfkNextRows []map[string]ast.Literal
					for _, leftRow := range currentRows {
						leftVal, leftOK := leftRow[pipelineColKey]
						if !leftOK {
							if joinType == ast.JoinLeft {
								nullRight := nullFilledRow(tableColumnNames(rightTable))
								vfkNextRows = append(vfkNextRows, mergePipelineRows(leftRow, rightPrefix, nullRight))
							}
							continue
						}

						// Determine versioned right table for this row.
						versionedRightTable := rightTable
						if lsnVal, ok := leftRow[lsnColKey]; ok && lsnVal.Kind == ast.LiteralNumber {
							targetLSN := uint64(lsnVal.NumberValue)
							if vs, cached := stateCache[targetLSN]; cached {
								if vDS, dOk := vs.domains[j.DomainName]; dOk {
									if vT, tOk := vDS.tables[j.TableName]; tOk {
										versionedRightTable = vT
									}
								}
							}
						}

						matched := false
						for _, rRowSlice := range versionedRightTable.rows {
							rRow := rowToMap(versionedRightTable, rRowSlice)
							rVal, rOK := rRow[rightColName]
							if !rOK {
								continue
							}
							if !literalEqual(leftVal, rVal) {
								continue
							}
							vfkNextRows = append(vfkNextRows, mergePipelineRows(leftRow, rightPrefix, rRow))
							matched = true
						}

						if !matched && joinType == ast.JoinLeft {
							nullRight := nullFilledRow(tableColumnNames(rightTable))
							vfkNextRows = append(vfkNextRows, mergePipelineRows(leftRow, rightPrefix, nullRight))
						}
					}

					currentRows = vfkNextRows
					lastStrategy = scanStrategyJoinNested
					continue
				}
			}
		}

		// INNER or LEFT JOIN: iterate over pipeline (left) rows, probe right table.
		var nextRows []map[string]ast.Literal
		leftMatched := make(map[int]bool)

		// Try index-accelerated lookup on the right table.
		hasRightIndex := hasJoinIndex(rightTable, j.TableName, j.TableName+"."+rightColName)

		// Also check for left-table index. The pipelineColKey is like "users.id" —
		// extract the table-name part and look it up in domainState.
		hasLeftIndex := false
		var leftIndexTable *tableState
		var leftIndexColRef string
		if !hasRightIndex {
			pipelinePrefix, pipelineBareCol, pipelineOK := splitQualifiedColumnRef(pipelineColKey)
			if pipelineOK {
				// Resolve alias to real table name.
				// Skip domain-qualified keys (e.g. "accounts.users") — they
				// are reverse-lookup entries, not alias→table mappings.
				leftRealTable := pipelinePrefix
				for tbl, prefix := range aliasMap {
					if prefix == pipelinePrefix && tbl != prefix && !strings.Contains(tbl, ".") {
						leftRealTable = tbl
						break
					}
				}
				// Resolve left table from correct domain state.
				leftDS := baseDomainState
				for _, ds := range state.domains {
					if _, found := ds.tables[leftRealTable]; found {
						leftDS = ds
						break
					}
				}
				if lt, ok := leftDS.tables[leftRealTable]; ok {
					leftIndexColRef = leftRealTable + "." + pipelineBareCol
					if hasJoinIndex(lt, leftRealTable, leftIndexColRef) {
						hasLeftIndex = true
						leftIndexTable = lt
					}
				}
			}
		}

		rightPredicate := conjunctiveRootJoinPredicate(plan.Filter, aliasMap, j.TableName, rightPrefix, rightTable)
		bestRightPredicate := bestBaseJoinPredicate(plan.Filter, aliasMap, j.TableName, rightPrefix, rightTable)
		pushRightPredicate := rightPredicate != nil && (joinType == ast.JoinInner || (joinType == ast.JoinLeft && predicateRejectsNull(rightPredicate)))

		var reusableRightRows []map[string]ast.Literal
		if !hasRightIndex && !(hasLeftIndex && leftIndexTable != nil && bestRightPredicate == nil) {
			reusableRightRows = rowsForPredicate(rightTable, bestRightPredicate, state, engine)
			if pushRightPredicate {
				filteredRightRows := reusableRightRows[:0]
				for _, rightRow := range reusableRightRows {
					if !matchPredicate(rightRow, rightPredicate, state, engine) {
						continue
					}
					filteredRightRows = append(filteredRightRows, rightRow)
				}
				reusableRightRows = filteredRightRows
			}
		}

		if hasLeftIndex && leftIndexTable != nil {
			// Left-index strategy: iterate right rows, probe left table index.
			pipelineByVal := make(map[string][]int) // serialized value → pipeline row indices
			for i, leftRow := range currentRows {
				val, ok := leftRow[pipelineColKey]
				if !ok {
					continue
				}
				key := literalKey(val)
				pipelineByVal[key] = append(pipelineByVal[key], i)
			}

			if reusableRightRows != nil {
				for _, rightRow := range reusableRightRows {
					rightVal, rightOK := rightRow[rightColName]
					if !rightOK {
						continue
					}
					key := literalKey(rightVal)
					for _, idx := range pipelineByVal[key] {
						nextRows = append(nextRows, mergePipelineRows(currentRows[idx], rightPrefix, rightRow))
						leftMatched[idx] = true
					}
				}
			} else {
				rightColPos, ok := rightTable.columnIndex[rightColName]
				if !ok {
					return nil, scanStrategyFullScan, errTableNotFound
				}
				for _, rightRowSlice := range rightTable.rows {
					if rightColPos < 0 || rightColPos >= len(rightRowSlice) {
						continue
					}
					rightVal := rightRowSlice[rightColPos]
					key := literalKey(rightVal)
					if len(pipelineByVal[key]) == 0 {
						continue
					}
					if pushRightPredicate && !matchTablePredicateOnRow(rightTable, rightRowSlice, rightPredicate) {
						continue
					}
					rightRow := rowToMap(rightTable, rightRowSlice)
					for _, idx := range pipelineByVal[key] {
						nextRows = append(nextRows, mergePipelineRows(currentRows[idx], rightPrefix, rightRow))
						leftMatched[idx] = true
					}
				}
			}
		} else {
			if !hasRightIndex && reusableRightRows == nil {
				reusableRightRows = tableRowsToMaps(rightTable)
			}
			for i, leftRow := range currentRows {
				leftVal, leftOK := leftRow[pipelineColKey]
				if !leftOK {
					continue
				}

				var candidates []map[string]ast.Literal
				if hasRightIndex {
					candidates = joinCandidateRows(rightTable, j.TableName, j.TableName+"."+rightColName, leftVal)
					if pushRightPredicate {
						filteredCandidates := candidates[:0]
						for _, rightRow := range candidates {
							if !matchPredicate(rightRow, rightPredicate, state, engine) {
								continue
							}
							filteredCandidates = append(filteredCandidates, rightRow)
						}
						candidates = filteredCandidates
					}
				} else {
					candidates = reusableRightRows
				}

				for _, rightRow := range candidates {
					rightVal, rightOK := rightRow[rightColName]
					if !rightOK {
						continue
					}
					if !literalEqual(leftVal, rightVal) {
						continue
					}
					nextRows = append(nextRows, mergePipelineRows(leftRow, rightPrefix, rightRow))
					leftMatched[i] = true
				}
			}
		}

		// LEFT JOIN: emit unmatched left rows with NULLs for right columns.
		if joinType == ast.JoinLeft {
			rightCols := tableColumnNames(rightTable)
			for i, leftRow := range currentRows {
				if leftMatched[i] {
					continue
				}
				nullRight := nullFilledRow(rightCols)
				nextRows = append(nextRows, mergePipelineRows(leftRow, rightPrefix, nullRight))
			}
		}

		currentRows = nextRows
		if hasRightIndex {
			lastStrategy = scanStrategyJoinRightIx
		} else if hasLeftIndex {
			lastStrategy = scanStrategyJoinLeftIx
		} else {
			lastStrategy = scanStrategyJoinNested
		}
	}

	// Apply WHERE filter after all JOINs.
	result := make([]map[string]ast.Literal, 0, len(currentRows))
	for _, row := range currentRows {
		if !matchPredicate(row, plan.Filter, state, engine) {
			continue
		}
		result = append(result, row)
	}

	// Strip internal entity version cascade metadata from result rows.
	for _, row := range result {
		for k := range row {
			if strings.HasPrefix(k, "__evs__") {
				delete(row, k)
			}
		}
	}

	return result, lastStrategy, nil
}
