package executor

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
)

// A recursive CTE that has not finished after this many rounds, or holds
// more rows than this, fails instead of running on: UNION ALL over cyclic
// data never ends.
const (
	maxRecursiveCTEIterations = 10_000
	maxRecursiveCTERows       = 1_000_000
)

// materializeCTEs runs each CTE and adds its rows to a copy of state as a
// virtual table, so the query, and the CTEs after it, read it by name.
// Their unqualified tables resolve to domain, the query's.
func (engine *Engine) materializeCTEs(ctx context.Context, state *readableState, ctes []ast.CTE, domain string) (*readableState, error) {
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
		if err := engine.materializeCTE(ctx, newState, cte, domain); err != nil {
			return nil, fmt.Errorf("CTE %s: %w", cte.Name, err)
		}
	}
	return newState, nil
}

func (engine *Engine) materializeCTE(ctx context.Context, state *readableState, cte ast.CTE, domain string) error {
	anchor, err := engine.runCTESelect(ctx, state, cte.Statement, domain)
	if err != nil {
		return err
	}
	if cte.Recursive == nil && len(cte.Columns) == 0 {
		columns, rows := cteRowsByKey(cte.Statement, anchor)
		injectVirtualTable(state, cte.Name, newVirtualTable(columns, rows))
		return nil
	}

	columns, rows, err := cteRowsBySelectList(cte.Statement, anchor, cte.Columns)
	if err != nil {
		return err
	}
	if cte.Recursive == nil {
		injectVirtualTable(state, cte.Name, newVirtualTable(columns, rows))
		return nil
	}

	// Each round runs the recursive term over the rows the previous round
	// added, until a round adds none. Without ALL, rows already produced
	// are not added again.
	var seen map[string]struct{}
	if !cte.Recursive.All {
		seen = make(map[string]struct{}, len(rows))
		rows = unseenRows(rows, seen)
	}
	term := cteDrivenTerm(cte.Recursive.Statement, cte.Name)
	result := rows
	working := rows
	for round := 0; len(working) > 0; round++ {
		if round == maxRecursiveCTEIterations {
			return fmt.Errorf("recursive CTE did not finish after %d iterations", maxRecursiveCTEIterations)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		injectVirtualTable(state, cte.Name, newVirtualTable(columns, working))
		produced, err := engine.runCTESelect(ctx, state, term, domain)
		if err != nil {
			return err
		}
		termColumns, next, err := cteRowsBySelectList(term, produced, nil)
		if err != nil {
			return err
		}
		if len(termColumns) != len(columns) {
			return fmt.Errorf("recursive term returns %d columns, the CTE has %d", len(termColumns), len(columns))
		}
		if seen != nil {
			next = unseenRows(next, seen)
		}
		result = append(result, next...)
		if len(result) > maxRecursiveCTERows {
			return fmt.Errorf("recursive CTE produced more than %d rows", maxRecursiveCTERows)
		}
		working = next
	}
	injectVirtualTable(state, cte.Name, newVirtualTable(columns, result))
	return nil
}

// cteDrivenTerm returns the recursive term "FROM t JOIN cte ON ..." as
// "FROM cte JOIN t ON ...". An inner join of two tables has the same rows
// either way, but a join runs from its first table: from the few rows of
// the last round it uses the index of t, instead of reading all of t every
// round. The ON columns must be qualified, so they still name their table.
func cteDrivenTerm(term ast.SelectStatement, cteName string) ast.SelectStatement {
	if len(term.Joins) != 1 || strings.EqualFold(term.TableName, cteName) {
		return term
	}
	join := term.Joins[0]
	if join.JoinType != ast.JoinInner || !strings.EqualFold(join.TableName, cteName) || join.DomainName != "" ||
		!strings.Contains(join.LeftColumn, ".") || !strings.Contains(join.RightColumn, ".") {
		return term
	}
	swapped := term
	swapped.TableName, swapped.TableAlias = join.TableName, join.Alias
	join.TableName, join.Alias = term.TableName, term.TableAlias
	swapped.Joins = []ast.JoinClause{join}
	return swapped
}

func (engine *Engine) runCTESelect(ctx context.Context, state *readableState, sel ast.SelectStatement, domain string) ([]map[string]ast.Literal, error) {
	plan, err := planner.BuildForDomains(sel, domainsFromState(state, domain))
	if err != nil {
		return nil, err
	}
	return engine.selectRows(ctx, state, plan)
}

// cteRowsByKey names a CTE's columns after the keys of its rows, sorted,
// without the table qualifier a joined or aliased column has.
func cteRowsByKey(sel ast.SelectStatement, rows []map[string]ast.Literal) ([]string, [][]ast.Literal) {
	var keys []string
	if len(rows) > 0 {
		for key := range rows[0] {
			keys = append(keys, key)
		}
	} else {
		keys = append(keys, sel.Columns...)
	}
	sort.Strings(keys)
	columns := make([]string, len(keys))
	for i, key := range keys {
		columns[i] = key
		if bare := unqualifiedColumn(key); bare != key && countUnqualified(keys, bare) == 1 {
			columns[i] = bare
		}
	}
	out := make([][]ast.Literal, len(rows))
	for i, row := range rows {
		out[i] = rowFromMap(keys, row)
	}
	return columns, out
}

func countUnqualified(keys []string, bare string) int {
	n := 0
	for _, key := range keys {
		if unqualifiedColumn(key) == bare {
			n++
		}
	}
	return n
}

// cteRowsBySelectList takes a CTE's columns from its select list, in order,
// as PostgreSQL does: named by the column list when given, else by each
// item's alias or column name.
func cteRowsBySelectList(sel ast.SelectStatement, rows []map[string]ast.Literal, names []string) ([]string, [][]ast.Literal, error) {
	if len(sel.Columns) == 1 && strings.TrimSpace(sel.Columns[0]) == "*" {
		columns, out := cteRowsByKey(sel, rows)
		if len(names) > 0 {
			if len(names) != len(columns) {
				return nil, nil, fmt.Errorf("column list names %d columns, the query returns %d", len(names), len(columns))
			}
			columns = names
		}
		return columns, out, nil
	}
	keys := make([]string, len(sel.Columns))
	columns := make([]string, len(sel.Columns))
	for i, item := range sel.Columns {
		_, alias := stripColumnAlias(strings.TrimSpace(item))
		keys[i] = strings.TrimSpace(alias)
		columns[i] = unqualifiedColumn(keys[i])
	}
	if len(names) > 0 {
		if len(names) != len(columns) {
			return nil, nil, fmt.Errorf("column list names %d columns, the query returns %d", len(names), len(columns))
		}
		columns = names
	}
	out := make([][]ast.Literal, len(rows))
	for i, row := range rows {
		values := make([]ast.Literal, len(keys))
		for j, key := range keys {
			value, ok := row[key]
			if !ok {
				value, ok = row[unqualifiedColumn(key)]
			}
			if !ok {
				value = ast.Literal{Kind: ast.LiteralNull}
			}
			values[j] = value
		}
		out[i] = values
	}
	return columns, out, nil
}

func unqualifiedColumn(column string) string {
	if dot := strings.LastIndexByte(column, '.'); dot >= 0 {
		return column[dot+1:]
	}
	return column
}

// unseenRows returns the rows not in seen, once each, and adds them to it.
func unseenRows(rows [][]ast.Literal, seen map[string]struct{}) [][]ast.Literal {
	out := rows[:0:0]
	var key strings.Builder
	for _, row := range rows {
		key.Reset()
		for _, value := range row {
			key.WriteString(literalKey(value))
			key.WriteByte(0)
		}
		if _, ok := seen[key.String()]; ok {
			continue
		}
		seen[key.String()] = struct{}{}
		out = append(out, row)
	}
	return out
}

func newVirtualTable(columns []string, rows [][]ast.Literal) *tableState {
	columnIndex := make(map[string]int, len(columns))
	for i, column := range columns {
		columnIndex[column] = i
	}
	return &tableState{
		columns:           columns,
		columnDefinitions: make(map[string]ast.ColumnDefinition),
		rows:              rows,
		columnIndex:       columnIndex,
		indexes:           make(map[string]*indexState),
		indexedColumns:    make(map[string]string),
		indexedColumnSets: make(map[string]string),
		uniqueColumns:     make(map[string]struct{}),
	}
}
