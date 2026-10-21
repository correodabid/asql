package executor

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
)

// Composite primary keys: PRIMARY KEY (a, b, ...) on two or more columns.
//
// A one-column key keeps its historical form, tableState.primaryKey, which
// foreign keys, entities and row introspection build on. A composite key is
// tableState.primaryKeyColumns, in key order, with primaryKey empty: those
// single-column features do not apply to it. Its columns are NOT NULL, and
// the tuple is unique, enforced through an implicit hash index named
// compositePKIndexPrefix+table whose buckets are keyed by the first key
// column, as every hash index is; a bucket's rows are compared on the whole
// tuple.

const compositePKIndexPrefix = "__pk_"

// primaryKeyColumnsOf returns the table's primary key columns in key order,
// one for a single-column key, or none.
func primaryKeyColumnsOf(table *tableState) []string {
	if table == nil {
		return nil
	}
	if table.primaryKey != "" {
		return []string{table.primaryKey}
	}
	return table.primaryKeyColumns
}

// compositeKey encodes a row's composite primary key, or reports false when
// a key column is missing or NULL.
func compositeKey(columns []string, row map[string]ast.Literal) (string, bool) {
	var b strings.Builder
	for _, column := range columns {
		value, ok := row[column]
		if !ok || value.Kind == ast.LiteralNull {
			return "", false
		}
		key := literalKey(value)
		b.WriteString(strconv.Itoa(len(key)))
		b.WriteByte(':')
		b.WriteString(key)
	}
	return b.String(), true
}

// describeCompositeKey renders a key tuple for error messages.
func describeCompositeKey(columns []string, row map[string]ast.Literal) string {
	values := make([]string, len(columns))
	for i, column := range columns {
		values[i] = literalKey(row[column])
	}
	return "(" + strings.Join(values, ", ") + ")"
}

// compositePKIndex returns the implicit index of a composite primary key.
func compositePKIndex(table *tableState) *indexState {
	if table == nil || len(table.primaryKeyColumns) == 0 {
		return nil
	}
	for name, idx := range table.indexes {
		if strings.HasPrefix(name, compositePKIndexPrefix) && idx.kind == "hash" && slices.Equal(idx.columns, table.primaryKeyColumns) {
			return idx
		}
	}
	return nil
}

// newCompositePKIndex returns the empty implicit index of a composite key.
func newCompositePKIndex(tableName string, columns []string) *indexState {
	name := compositePKIndexPrefix + tableName
	return &indexState{name: name, column: columns[0], columns: append([]string(nil), columns...), kind: "hash", buckets: make(map[string][]int)}
}

// findCompositeKeyRow returns the row whose composite key is key, or -1.
func findCompositeKeyRow(table *tableState, row map[string]ast.Literal, key string) int {
	idx := compositePKIndex(table)
	if idx == nil {
		return -1
	}
	for _, rowID := range idx.lookupBucket(literalKey(row[table.primaryKeyColumns[0]])) {
		if rowID < 0 || rowID >= len(table.rows) || table.rows[rowID] == nil {
			continue
		}
		if existing, ok := compositeKey(table.primaryKeyColumns, rowToMap(table, table.rows[rowID])); ok && existing == key {
			return rowID
		}
	}
	return -1
}

// validateCompositeKeyInsert checks a new row against a composite key.
func validateCompositeKeyInsert(table *tableState, row map[string]ast.Literal) error {
	if len(table.primaryKeyColumns) == 0 {
		return nil
	}
	key, ok := compositeKey(table.primaryKeyColumns, row)
	if !ok {
		return fmt.Errorf("%w: primary key (%s) cannot be null", errConstraintPKNull, strings.Join(table.primaryKeyColumns, ", "))
	}
	if findCompositeKeyRow(table, row, key) >= 0 {
		return fmt.Errorf("%w: primary key (%s) duplicate value %s", errConstraintPKDup, strings.Join(table.primaryKeyColumns, ", "), describeCompositeKey(table.primaryKeyColumns, row))
	}
	return nil
}

// validateCompositeKeyRows checks a table's rows against a composite key.
func validateCompositeKeyRows(columns []string, rows []map[string]ast.Literal) error {
	if len(columns) == 0 {
		return nil
	}
	seen := make(map[string]int, len(rows))
	for rowID, row := range rows {
		key, ok := compositeKey(columns, row)
		if !ok {
			return fmt.Errorf("%w: primary key (%s) cannot be null at row %d", errConstraintPKNull, strings.Join(columns, ", "), rowID)
		}
		if previous, dup := seen[key]; dup {
			return fmt.Errorf("%w: primary key (%s) duplicate value %s between rows %d and %d", errConstraintPKDup, strings.Join(columns, ", "), describeCompositeKey(columns, row), previous, rowID)
		}
		seen[key] = rowID
	}
	return nil
}

// checkPrimaryKeyColumns validates a primary key's columns against a table's
// column definitions.
func checkPrimaryKeyColumns(columns []string, definitions map[string]ast.ColumnDefinition) error {
	if len(columns) == 0 {
		return fmt.Errorf("%w: a primary key needs at least one column", errConstraintGeneric)
	}
	seen := make(map[string]bool, len(columns))
	for _, column := range columns {
		if _, ok := definitions[column]; !ok {
			return fmt.Errorf("%w: primary key column %q does not exist", errConstraintGeneric, column)
		}
		if seen[column] {
			return fmt.Errorf("%w: primary key column %q repeated", errConstraintGeneric, column)
		}
		seen[column] = true
	}
	return nil
}

// applyAddPrimaryKey runs ALTER TABLE ... ADD PRIMARY KEY on a COW-cloned
// table: the existing rows must already satisfy the key, and nothing changes
// when they do not.
func (engine *Engine) applyAddPrimaryKey(domainState *domainState, plan planner.Plan, clonedTables map[string]struct{}) error {
	table, exists := domainState.tables[plan.TableName]
	if !exists || !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
		return errTableNotFound
	}
	if len(primaryKeyColumnsOf(table)) > 0 {
		return fmt.Errorf("%w: table %s already has a primary key (%s)", errConstraintGeneric, plan.TableName, strings.Join(primaryKeyColumnsOf(table), ", "))
	}
	columns := plan.PrimaryKeyColumns
	if err := checkPrimaryKeyColumns(columns, table.columnDefinitions); err != nil {
		return err
	}
	if len(columns) == 1 {
		if idxName, ok := table.indexedColumns[columns[0]]; ok {
			if idx := table.indexes[idxName]; idx == nil || idx.kind != "hash" {
				return fmt.Errorf("%w: column %s has the non-hash index %s; drop it before ADD PRIMARY KEY", errConstraintGeneric, columns[0], idxName)
			}
		}
	}

	rows := make([]map[string]ast.Literal, len(table.rows))
	for rowID, rowSlice := range table.rows {
		rows[rowID] = rowToMap(table, rowSlice)
	}
	if err := validateCompositeKeyRows(columns, rows); err != nil {
		return err
	}

	tableKey := plan.DomainName + "." + plan.TableName
	if clonedTables == nil {
		table = cloneTableState(table)
		domainState.tables[plan.TableName] = table
	} else if _, already := clonedTables[tableKey]; !already {
		table = cloneTableState(table)
		domainState.tables[plan.TableName] = table
		clonedTables[tableKey] = struct{}{}
	}

	for _, column := range columns {
		def := table.columnDefinitions[column]
		def.NotNull = true
		if len(columns) == 1 {
			def.PrimaryKey, def.Unique = true, true
		}
		table.columnDefinitions[column] = def
	}

	var index *indexState
	if len(columns) == 1 {
		table.primaryKey = columns[0]
		table.uniqueColumns[columns[0]] = struct{}{}
		if _, ok := table.indexedColumns[columns[0]]; !ok {
			name := fmt.Sprintf("__auto_%s_%s", plan.TableName, columns[0])
			index = &indexState{name: name, column: columns[0], columns: []string{columns[0]}, kind: "hash", buckets: make(map[string][]int)}
			table.indexedColumns[columns[0]] = name
		}
	} else {
		table.primaryKeyColumns = append([]string(nil), columns...)
		index = newCompositePKIndex(plan.TableName, columns)
	}
	if index != nil {
		for rowID, row := range table.rows {
			if entry, ok := buildIndexEntryForRow(index, row, table.columnIndex, rowID); ok {
				addIndexEntry(index, entry)
			}
		}
		index.baseSize = len(index.buckets)
		table.indexes[index.name] = index
	}
	rebuildNotNullColumns(table)
	rebuildUniqueColumnList(table)
	rebuildPKAutoUUID(table)
	table.lastMutationTS = engine.logicalTS
	return nil
}

// renameInList returns a copy of list with oldName renamed to newName.
func renameInList(list []string, oldName, newName string) []string {
	out := append([]string(nil), list...)
	for i, v := range out {
		if v == oldName {
			out[i] = newName
		}
	}
	return out
}

// compositeKeyRef names a table's composite primary key, for snapshots.
type compositeKeyRef struct {
	domain, table string
	columns       []string
}

// compositeKeyRefs lists the composite primary keys, sorted by table.
func compositeKeyRefs(domains map[string]*domainState) []compositeKeyRef {
	tables := tableRefs(domains, func(t *tableState) bool { return len(t.primaryKeyColumns) > 0 })
	refs := make([]compositeKeyRef, len(tables))
	for i, ref := range tables {
		refs[i] = compositeKeyRef{domain: ref[0], table: ref[1], columns: domains[ref[0]].tables[ref[1]].primaryKeyColumns}
	}
	return refs
}

// markCompositeKeys restores the composite primary keys a snapshot recorded.
func markCompositeKeys(domains map[string]*domainState, refs []compositeKeyRef) {
	for _, ref := range refs {
		if domain, ok := domains[ref.domain]; ok {
			if table, ok := domain.tables[ref.table]; ok {
				table.primaryKeyColumns = append([]string(nil), ref.columns...)
			}
		}
	}
}
