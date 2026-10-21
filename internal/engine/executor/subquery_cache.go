package executor

import (
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// Subqueries in a WHERE are evaluated for every row of the outer query. One
// that does not refer to the outer row gives the same rows each time, so a
// query runs it once: x IN (SELECT ...) over a table of 100,000 rows used to
// run the subquery 100,000 times.

// subqueryCache holds the uncorrelated subqueries a query has run, and the
// first error a subquery returned, which the predicate evaluator, returning
// a ternary value, cannot.
type subqueryCache struct {
	results map[*ast.Subquery]*subqueryResult
	err     error
}

type subqueryResult struct {
	rows []map[string]ast.Literal
	err  error
	// For IN: the keys of the non-NULL values of the first column.
	set     map[string]struct{}
	hasNull bool
}

// withSubqueryCache returns state with a cache for one query, or state
// itself when it already has one (a subquery of that query).
func withSubqueryCache(state *readableState) *readableState {
	if state == nil || state.subqueries != nil {
		return state
	}
	scoped := *state
	scoped.subqueries = &subqueryCache{results: make(map[*ast.Subquery]*subqueryResult)}
	return &scoped
}

// noteSubqueryError keeps the first subquery error for the query to return.
func noteSubqueryError(state *readableState, err error) {
	if err != nil && state != nil && state.subqueries != nil && state.subqueries.err == nil {
		state.subqueries.err = err
	}
}

// subqueryRows runs sub for outerRow, or returns the rows of an earlier run
// when sub does not refer to the outer row.
func subqueryRows(sub *ast.Subquery, state *readableState, engine *Engine, outerRow map[string]ast.Literal) (*subqueryResult, error) {
	cache := (*subqueryCache)(nil)
	if state != nil {
		cache = state.subqueries
	}
	if cache != nil && !subqueryReadsOuterRow(sub.Statement.Where, outerRow) {
		if cached, ok := cache.results[sub]; ok {
			return cached, cached.err
		}
		rows, err := executeSubquery(sub, state, engine, outerRow)
		result := &subqueryResult{rows: rows, err: err}
		cache.results[sub] = result
		noteSubqueryError(state, err)
		return result, err
	}
	rows, err := executeSubquery(sub, state, engine, outerRow)
	noteSubqueryError(state, err)
	return &subqueryResult{rows: rows, err: err}, err
}

// subqueryReadsOuterRow reports whether a subquery's WHERE, or one of its
// nested subqueries, compares with a column of the outer row: those are the
// references decorrelateFilter replaces.
func subqueryReadsOuterRow(pred *ast.Predicate, outerRow map[string]ast.Literal) bool {
	if pred == nil || len(outerRow) == 0 {
		return false
	}
	if pred.RightColumn != "" {
		if _, ok := outerRow[strings.ToLower(pred.RightColumn)]; ok {
			return true
		}
	}
	if pred.Subquery != nil && subqueryReadsOuterRow(pred.Subquery.Statement.Where, outerRow) {
		return true
	}
	return subqueryReadsOuterRow(pred.Left, outerRow) || subqueryReadsOuterRow(pred.Right, outerRow)
}

// subqueryValue returns the value of a subquery row: its first column, named
// as the select list names it.
func subqueryValue(sub *ast.Subquery, row map[string]ast.Literal) (ast.Literal, error) {
	columns := sub.Statement.Columns
	if len(columns) > 1 || (len(columns) == 1 && strings.TrimSpace(columns[0]) == "*" && len(row) > 1) {
		return ast.Literal{}, sqlerr.New("42601", "subquery has too many columns")
	}
	if len(columns) == 1 && strings.TrimSpace(columns[0]) != "*" {
		_, alias := stripColumnAlias(strings.TrimSpace(sub.Statement.Columns[0]))
		key := strings.ToLower(strings.TrimSpace(alias))
		if v, ok := row[key]; ok {
			return v, nil
		}
		if dot := strings.LastIndexByte(key, '.'); dot >= 0 {
			if v, ok := row[key[dot+1:]]; ok {
				return v, nil
			}
		}
	}
	if len(row) == 1 {
		for _, v := range row {
			return v, nil
		}
	}
	return ast.Literal{}, sqlerr.New("42601", "subquery must return only one column")
}

// inSet builds, once, the set of values IN tests against.
func (r *subqueryResult) inSet(sub *ast.Subquery) (map[string]struct{}, bool, error) {
	if r.set != nil {
		return r.set, r.hasNull, nil
	}
	set := make(map[string]struct{}, len(r.rows))
	hasNull := false
	for _, row := range r.rows {
		v, err := subqueryValue(sub, row)
		if err != nil {
			return nil, false, err
		}
		if v.Kind == ast.LiteralNull {
			hasNull = true
			continue
		}
		set[inKey(v)] = struct{}{}
	}
	r.set, r.hasNull = set, hasNull
	return set, hasNull, nil
}

// inKey is the key of a value in an IN set: literalKey, with -0 as 0, which
// compareLiterals finds equal.
func inKey(v ast.Literal) string {
	if v.Kind == ast.LiteralFloat && v.FloatValue == 0 {
		v.FloatValue = 0
	}
	return literalKey(v)
}

// inlineInSubqueries replaces each "x IN (SELECT ...)" of pred whose
// subquery cannot refer to an outer row with "x IN (values)", run once, so
// the scan can use an index on x instead of reading every row. A result
// with NULL is left to per-row evaluation, which gives NULL its meaning. It
// copies what it changes.
func (engine *Engine) inlineInSubqueries(state *readableState, pred *ast.Predicate) (*ast.Predicate, error) {
	if pred == nil {
		return nil, nil
	}
	left, err := engine.inlineInSubqueries(state, pred.Left)
	if err != nil {
		return nil, err
	}
	right, err := engine.inlineInSubqueries(state, pred.Right)
	if err != nil {
		return nil, err
	}
	out := pred
	if left != pred.Left || right != pred.Right {
		copied := *pred
		copied.Left, copied.Right = left, right
		out = &copied
	}
	if pred.Subquery == nil || pred.Column == "" || pred.JsonAccess != nil || subqueryComparesColumns(pred.Subquery.Statement.Where) {
		return out, nil
	}
	switch strings.ToUpper(strings.TrimSpace(pred.Operator)) {
	case "IN":
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		return engine.inlineScalarSubquery(state, out)
	default:
		return out, nil
	}
	result, err := subqueryRows(pred.Subquery, state, engine, nil)
	if err != nil {
		return nil, err
	}
	if len(result.rows) == 0 {
		return out, nil
	}
	seen := make(map[string]struct{}, len(result.rows))
	values := make([]ast.Literal, 0, len(result.rows))
	for _, row := range result.rows {
		v, err := subqueryValue(pred.Subquery, row)
		if err != nil {
			return nil, err
		}
		if v.Kind == ast.LiteralNull {
			return out, nil
		}
		key := inKey(v)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, v)
	}
	inlined := *out
	inlined.Subquery, inlined.InValues = nil, values
	return &inlined, nil
}

// inlineScalarSubquery replaces "x op (SELECT ...)", whose subquery cannot
// refer to an outer row, with "x op value", run once, so the comparison can
// use an index on x. With no row or a NULL the comparison is unknown for
// every row, which per-row evaluation already answers; more than one row is
// an error.
func (engine *Engine) inlineScalarSubquery(state *readableState, pred *ast.Predicate) (*ast.Predicate, error) {
	result, err := subqueryRows(pred.Subquery, state, engine, nil)
	if err != nil {
		return nil, err
	}
	switch {
	case len(result.rows) == 0:
		return pred, nil
	case len(result.rows) > 1:
		return nil, sqlerr.New("21000", "more than one row returned by a subquery used as an expression")
	}
	v, err := subqueryValue(pred.Subquery, result.rows[0])
	if err != nil {
		return nil, err
	}
	if v.Kind == ast.LiteralNull {
		return pred, nil
	}
	inlined := *pred
	inlined.Subquery, inlined.Value = nil, v
	return &inlined, nil
}

// subqueryComparesColumns reports whether a subquery's WHERE, or a nested
// one, compares two columns: such a subquery may be correlated.
func subqueryComparesColumns(pred *ast.Predicate) bool {
	if pred == nil {
		return false
	}
	if pred.RightColumn != "" {
		return true
	}
	if pred.Subquery != nil && subqueryComparesColumns(pred.Subquery.Statement.Where) {
		return true
	}
	return subqueryComparesColumns(pred.Left) || subqueryComparesColumns(pred.Right)
}
