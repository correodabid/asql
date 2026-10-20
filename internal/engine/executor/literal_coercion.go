package executor

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// A quoted literal has no type of its own in PostgreSQL: it takes the type of
// the column it is stored in or compared with. Clients rely on it. pgx, over
// the simple protocol, sends every argument as a quoted string, so without
// this '2' would be stored in an INT column as text and WHERE id = '2' would
// miss the row whose id is 2.
//
// Writes are coerced when the statement is queued, so the WAL records typed
// values and replay never coerces them again. Reads are coerced when they
// run, against the schema they read.

// columnTypeFunc returns the type of a column as a predicate or a SET names
// it, if it is known.
type columnTypeFunc func(column string) (ast.DataType, bool)

// coerceLiteral converts a string literal to the column type. Other literals,
// and strings for TEXT, JSON or unknown columns, are returned as they are.
func coerceLiteral(lit ast.Literal, dataType ast.DataType) (ast.Literal, error) {
	if lit.Kind != ast.LiteralString {
		return lit, nil
	}
	text := strings.TrimSpace(lit.StringValue)
	switch dataType {
	case ast.DataTypeInt:
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return lit, invalidInput("integer", lit.StringValue)
		}
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: n}, nil
	case ast.DataTypeFloat:
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return lit, invalidInput("double precision", lit.StringValue)
		}
		return ast.Literal{Kind: ast.LiteralFloat, FloatValue: f}, nil
	case ast.DataTypeBool:
		switch strings.ToLower(text) {
		case "t", "true", "y", "yes", "on", "1":
			return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: true}, nil
		case "f", "false", "n", "no", "off", "0":
			return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: false}, nil
		}
		return lit, invalidInput("boolean", lit.StringValue)
	case ast.DataTypeTimestamp:
		ts, ok := parseTimestampText(text)
		if !ok {
			return lit, sqlerr.New("22007", fmt.Sprintf("invalid input syntax for type timestamp: %q", lit.StringValue))
		}
		return ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: ts.UnixMicro()}, nil
	}
	return lit, nil
}

func invalidInput(typeName, text string) error {
	return sqlerr.New("22P02", fmt.Sprintf("invalid input syntax for type %s: %q", typeName, text))
}

// timestampLayouts are the forms PostgreSQL clients send: ISO 8601 with a
// T or a space, with or without fractional seconds and a zone, which pgx
// writes as +hh:mm:ss. A value without a zone is UTC.
var timestampLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07:00:00",
	"2006-01-02T15:04:05.999999999Z07:00:00",
	"2006-01-02 15:04:05.999999999Z07",
	"2006-01-02T15:04:05.999999999Z07",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02",
}

func parseTimestampText(text string) (time.Time, bool) {
	for _, layout := range timestampLayouts {
		if ts, err := time.Parse(layout, text); err == nil {
			return ts.UTC(), true
		}
	}
	return time.Time{}, false
}

// coercePredicate returns the predicate with the literals compared with a
// column of known type coerced to it. It copies what it changes and leaves
// the input alone: plans are shared.
func coercePredicate(pred *ast.Predicate, columnType columnTypeFunc) (*ast.Predicate, error) {
	if pred == nil {
		return nil, nil
	}
	left, err := coercePredicate(pred.Left, columnType)
	if err != nil {
		return nil, err
	}
	right, err := coercePredicate(pred.Right, columnType)
	if err != nil {
		return nil, err
	}
	out := pred
	clone := func() {
		if out == pred {
			copied := *pred
			out = &copied
		}
	}
	if left != pred.Left || right != pred.Right {
		clone()
		out.Left, out.Right = left, right
	}
	if pred.Column == "" || pred.JsonAccess != nil || pred.RightColumn != "" || pred.Subquery != nil || !hasStringLiteral(pred) {
		return out, nil
	}
	dataType, ok := columnType(pred.Column)
	if !ok {
		return out, nil
	}
	value, err := coerceLiteral(pred.Value, dataType)
	if err != nil {
		return nil, err
	}
	if value != pred.Value {
		clone()
		out.Value = value
	}
	if pred.Value2 != nil {
		value2, err := coerceLiteral(*pred.Value2, dataType)
		if err != nil {
			return nil, err
		}
		if value2 != *pred.Value2 {
			clone()
			out.Value2 = &value2
		}
	}
	var inValues []ast.Literal
	for i, in := range pred.InValues {
		coerced, err := coerceLiteral(in, dataType)
		if err != nil {
			return nil, err
		}
		if coerced == in {
			continue
		}
		if inValues == nil {
			inValues = append([]ast.Literal(nil), pred.InValues...)
		}
		inValues[i] = coerced
	}
	if inValues != nil {
		clone()
		out.InValues = inValues
	}
	return out, nil
}

func hasStringLiteral(pred *ast.Predicate) bool {
	if pred.Value.Kind == ast.LiteralString || (pred.Value2 != nil && pred.Value2.Kind == ast.LiteralString) {
		return true
	}
	for _, in := range pred.InValues {
		if in.Kind == ast.LiteralString {
			return true
		}
	}
	return false
}

// coerceWritePlan coerces the values an INSERT or UPDATE stores, and the
// literals of its WHERE, to the types of the target table. It replaces the
// slices it changes, never their elements, so it can run on a plan whose
// slices are shared.
func coerceWritePlan(plan *planner.Plan, columnType columnTypeFunc) error {
	coerceRow := func(columns []string, values []ast.Literal) ([]ast.Literal, error) {
		var out []ast.Literal
		for i, value := range values {
			if i >= len(columns) {
				break
			}
			if value.Kind != ast.LiteralString {
				continue
			}
			dataType, ok := columnType(columns[i])
			if !ok {
				continue
			}
			coerced, err := coerceLiteral(value, dataType)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", columns[i], err)
			}
			if coerced == value {
				continue
			}
			if out == nil {
				out = append([]ast.Literal(nil), values...)
			}
			out[i] = coerced
		}
		if out == nil {
			return values, nil
		}
		return out, nil
	}

	switch plan.Operation {
	case planner.OperationInsert:
		values, err := coerceRow(plan.Columns, plan.Values)
		if err != nil {
			return err
		}
		plan.Values = values
		if len(plan.MultiValues) > 0 {
			rows := make([][]ast.Literal, len(plan.MultiValues))
			for i, row := range plan.MultiValues {
				if rows[i], err = coerceRow(plan.Columns, row); err != nil {
					return err
				}
			}
			plan.MultiValues = rows
		}
		if oc := plan.OnConflict; oc != nil && len(oc.UpdateValues) > 0 {
			updates, err := coerceRow(oc.UpdateColumns, oc.UpdateValues)
			if err != nil {
				return err
			}
			if &updates[0] != &oc.UpdateValues[0] {
				copied := *oc
				copied.UpdateValues = updates
				plan.OnConflict = &copied
			}
		}
	case planner.OperationUpdate:
		values, err := coerceRow(plan.Columns, plan.Values)
		if err != nil {
			return err
		}
		plan.Values = values
		var exprs []ast.UpdateExpr
		for i, expr := range plan.UpdateExprs {
			if i >= len(plan.Columns) {
				break
			}
			dataType, ok := columnType(plan.Columns[i])
			if !ok {
				continue
			}
			coerced := expr
			if coerced.Value, err = coerceLiteral(expr.Value, dataType); err != nil {
				return fmt.Errorf("column %s: %w", plan.Columns[i], err)
			}
			if expr.Kind == ast.UpdateExprArithmetic {
				if coerced.Operand, err = coerceLiteral(expr.Operand, dataType); err != nil {
					return fmt.Errorf("column %s: %w", plan.Columns[i], err)
				}
			}
			if coerced == expr {
				continue
			}
			if exprs == nil {
				exprs = append([]ast.UpdateExpr(nil), plan.UpdateExprs...)
			}
			exprs[i] = coerced
		}
		if exprs != nil {
			plan.UpdateExprs = exprs
		}
	case planner.OperationDelete:
	default:
		return nil
	}
	filter, err := coercePredicate(plan.Filter, columnType)
	if err != nil {
		return err
	}
	plan.Filter = filter
	return nil
}

// tableColumnType resolves the columns of one table, named bare or qualified
// by the table or its alias.
func tableColumnType(table *tableState, names ...string) columnTypeFunc {
	return func(column string) (ast.DataType, bool) {
		if table == nil {
			return "", false
		}
		if dot := strings.LastIndexByte(column, '.'); dot >= 0 {
			qualifier := column[:dot]
			matches := false
			for _, name := range names {
				if name != "" && strings.EqualFold(qualifier, name) {
					matches = true
					break
				}
			}
			if !matches {
				return "", false
			}
			column = column[dot+1:]
		}
		def, ok := table.columnDefinitions[column]
		if !ok {
			return "", false
		}
		return def.Type, true
	}
}

// selectColumnType resolves the columns of a SELECT: those of its table and
// of the tables it joins. A bare name that more than one of them has is left
// alone.
func selectColumnType(state *readableState, plan planner.Plan) columnTypeFunc {
	type source struct {
		table *tableState
		names []string
	}
	lookup := func(domain, table string) *tableState {
		if domain == "" {
			domain = plan.DomainName
		}
		if ds, ok := state.domains[domain]; ok {
			return ds.tables[table]
		}
		return nil
	}
	var sources []source
	return func(column string) (ast.DataType, bool) {
		if sources == nil {
			sources = append(sources, source{lookup(plan.DomainName, plan.TableName), []string{plan.TableName, plan.TableAlias}})
			for _, join := range plan.Joins {
				sources = append(sources, source{lookup(join.DomainName, join.TableName), []string{join.TableName, join.Alias}})
			}
		}
		var found ast.DataType
		matches := 0
		for _, src := range sources {
			if dataType, ok := tableColumnType(src.table, src.names...)(column); ok {
				found = dataType
				matches++
			}
		}
		return found, matches == 1
	}
}

// pendingColumnType resolves the columns of the table a statement writes,
// as the transaction will see them when it commits: the committed schema
// with the schema changes queued before it applied.
func pendingColumnType(committed *tableState, queued []planner.Plan, domain, table string) columnTypeFunc {
	var types map[string]ast.DataType
	build := func() {
		types = make(map[string]ast.DataType)
		if committed != nil {
			for name, def := range committed.columnDefinitions {
				types[name] = def.Type
			}
		}
		for _, p := range queued {
			if p.DomainName != domain || p.TableName != table {
				continue
			}
			switch p.Operation {
			case planner.OperationCreateTable:
				if committed != nil && p.IfNotExists {
					continue
				}
				types = make(map[string]ast.DataType, len(p.Schema))
				for _, def := range p.Schema {
					types[def.Name] = def.Type
				}
			case planner.OperationDropTable:
				types = map[string]ast.DataType{}
			case planner.OperationAlterTableAddColumn:
				if p.AlterColumn != nil {
					types[p.AlterColumn.Name] = p.AlterColumn.Type
				}
			case planner.OperationAlterTableDropColumn:
				delete(types, p.DropColumnName)
			case planner.OperationAlterTableRenameColumn:
				if dataType, ok := types[p.RenameOldColumn]; ok {
					delete(types, p.RenameOldColumn)
					types[p.RenameNewColumn] = dataType
				}
			}
		}
	}
	return func(column string) (ast.DataType, bool) {
		if dot := strings.LastIndexByte(column, '.'); dot >= 0 {
			if !strings.EqualFold(column[:dot], table) {
				return "", false
			}
			column = column[dot+1:]
		}
		if types == nil {
			build()
		}
		dataType, ok := types[column]
		return dataType, ok
	}
}

// coerceDecodedPlan coerces a plan replay decoded from SQL text (see
// planner.Plan.CoerceLiterals) against the table as state has it, once.
func coerceDecodedPlan(state *readableState, plan *planner.Plan) error {
	if !plan.CoerceLiterals {
		return nil
	}
	var table *tableState
	if ds, ok := state.domains[plan.DomainName]; ok {
		table = ds.tables[plan.TableName]
	}
	if err := coerceWritePlan(plan, tableColumnType(table, plan.TableName)); err != nil {
		return err
	}
	plan.CoerceLiterals = false
	return nil
}
