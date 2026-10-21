package executor

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

// The ledger's state tree (ADR 0008) is a function of the engine's state at
// a checkpoint: the live rows of its VERIFIABLE tables. A VerifiableView is
// that state at one LSN, for the tree builder and for row proofs. It only
// reads states the engine already holds; nothing it does reaches the WAL.

// VerifiableTable is a verifiable table at a view's LSN.
type VerifiableTable struct {
	Domain, Name string
	// PrimaryKey is the primary key columns, in key order.
	PrimaryKey []string
	// Columns is the table's columns. A change in them or in PrimaryKey
	// means rows changed without row changes in the audit store (DROP,
	// RENAME or ADD COLUMN), so the builder rebuilds the table.
	Columns []string
}

// VerifiableRow is a live row of a verifiable table.
type VerifiableRow struct {
	// PrimaryKey is the row's primary key values, in key order.
	PrimaryKey []ast.Literal
	// Row is every column of the row, without the engine's _lsn.
	Row map[string]ast.Literal
	// LSN is the LSN of the WAL record that wrote this version of the row.
	LSN uint64
}

// VerifiableView is the engine state at one LSN, seen through its
// verifiable tables.
type VerifiableView struct {
	LSN   uint64
	state *readableState
}

// VerifiableView returns the view of the committed head.
func (engine *Engine) VerifiableView() VerifiableView {
	state := engine.readState.Load()
	if state == nil {
		return VerifiableView{}
	}
	return VerifiableView{LSN: state.headLSN, state: state}
}

// VerifiableViewAt returns the view at lsn, rebuilt from the closest
// snapshot and the WAL when it is not the head.
func (engine *Engine) VerifiableViewAt(ctx context.Context, lsn uint64) (VerifiableView, error) {
	if head := engine.VerifiableView(); head.LSN == lsn {
		return head, nil
	}
	if lsn > engine.HeadLSN() {
		return VerifiableView{}, fmt.Errorf("LSN %d is not committed", lsn)
	}
	state, _, err := engine.historicalStateAt(ctx, lsn)
	if err != nil {
		return VerifiableView{}, err
	}
	return VerifiableView{LSN: lsn, state: state}, nil
}

// Tables returns the verifiable tables, sorted by domain and name.
func (v VerifiableView) Tables() []VerifiableTable {
	if v.state == nil {
		return nil
	}
	var tables []VerifiableTable
	for domainName, domain := range v.state.domains {
		for tableName, table := range domain.tables {
			if table == nil || !table.verifiable {
				continue
			}
			tables = append(tables, VerifiableTable{
				Domain:     domainName,
				Name:       tableName,
				PrimaryKey: slices.Clone(primaryKeyColumnsOf(table)),
				Columns:    slices.Clone(table.columns),
			})
		}
	}
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Domain != tables[j].Domain {
			return tables[i].Domain < tables[j].Domain
		}
		return tables[i].Name < tables[j].Name
	})
	return tables
}

func (v VerifiableView) table(domain, name string) (*tableState, error) {
	if v.state != nil {
		if d, ok := v.state.domains[domain]; ok {
			if t, ok := d.tables[name]; ok && t != nil && t.verifiable {
				return t, nil
			}
		}
	}
	return nil, fmt.Errorf("%s.%s is not a VERIFIABLE table at LSN %d", domain, name, v.LSN)
}

// Rows calls fn for every live row of a verifiable table.
func (v VerifiableView) Rows(domain, name string, fn func(VerifiableRow) error) error {
	table, err := v.table(domain, name)
	if err != nil {
		return err
	}
	columns := primaryKeyColumnsOf(table)
	for _, values := range table.rows {
		if values == nil {
			continue
		}
		if err := fn(verifiableRow(table, columns, values)); err != nil {
			return err
		}
	}
	return nil
}

// Row returns the row of a verifiable table with the given primary key
// values, in key order. Values are matched by their text, so a client may
// give an INT key as "42".
func (v VerifiableView) Row(domain, name string, primaryKey []ast.Literal) (VerifiableRow, bool, error) {
	table, err := v.table(domain, name)
	if err != nil {
		return VerifiableRow{}, false, err
	}
	columns := primaryKeyColumnsOf(table)
	if len(primaryKey) != len(columns) {
		return VerifiableRow{}, false, fmt.Errorf("%s.%s has a %d-column primary key (%s), %d values given", domain, name, len(columns), strings.Join(columns, ", "), len(primaryKey))
	}
	want := make([]string, len(primaryKey))
	for i, value := range primaryKey {
		want[i] = keyText(value)
	}
	matches := func(values []ast.Literal) bool {
		for i, column := range columns {
			pos, ok := table.columnIndex[column]
			if !ok || pos >= len(values) || keyText(values[pos]) != want[i] {
				return false
			}
		}
		return true
	}
	// Through the first key column's hash index when there is one.
	candidates := func(yield func([]ast.Literal) bool) {
		if idxName, ok := table.indexedColumns[columns[0]]; ok {
			if idx, ok := table.indexes[idxName]; ok && idx.kind == "hash" {
				for _, rowID := range idx.lookupBucket(literalKey(coerceKeyValue(table, columns[0], primaryKey[0]))) {
					if rowID >= 0 && rowID < len(table.rows) && table.rows[rowID] != nil && !yield(table.rows[rowID]) {
						return
					}
				}
				return
			}
		}
		for _, values := range table.rows {
			if values != nil && !yield(values) {
				return
			}
		}
	}
	for values := range candidates {
		if matches(values) {
			return verifiableRow(table, columns, values), true, nil
		}
	}
	return VerifiableRow{}, false, nil
}

func verifiableRow(table *tableState, keyColumns []string, values []ast.Literal) VerifiableRow {
	row := rowToMap(table, values)
	var lsn uint64
	if l, ok := row["_lsn"]; ok && l.Kind == ast.LiteralNumber && l.NumberValue > 0 {
		lsn = uint64(l.NumberValue)
	}
	delete(row, "_lsn")
	key := make([]ast.Literal, len(keyColumns))
	for i, column := range keyColumns {
		key[i] = row[column]
	}
	return VerifiableRow{PrimaryKey: key, Row: row, LSN: lsn}
}

// keyText compares key values by their text, whatever kind the client gave.
func keyText(value ast.Literal) string {
	switch value.Kind {
	case ast.LiteralNumber, ast.LiteralTimestamp:
		return strconv.FormatInt(value.NumberValue, 10)
	case ast.LiteralFloat:
		return strconv.FormatFloat(value.FloatValue, 'g', -1, 64)
	case ast.LiteralBoolean:
		return strconv.FormatBool(value.BoolValue)
	default:
		return value.StringValue
	}
}

// coerceKeyValue turns a client's key value into the column's kind, for an
// index lookup.
func coerceKeyValue(table *tableState, column string, value ast.Literal) ast.Literal {
	def, ok := table.columnDefinitions[column]
	if !ok || value.Kind != ast.LiteralString {
		return value
	}
	switch def.Type {
	case ast.DataTypeInt:
		if n, err := strconv.ParseInt(value.StringValue, 10, 64); err == nil {
			return ast.Literal{Kind: ast.LiteralNumber, NumberValue: n}
		}
	}
	return value
}
