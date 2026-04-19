package executor

import (
	"sort"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

func rowIDsForPredicate(index *indexState, predicate *ast.Predicate) []int {
	if index == nil || predicate == nil {
		return nil
	}

	if !isSimplePredicate(predicate) {
		return nil
	}

	if predicate.Operator == "IS NULL" {
		rowIDs := make([]int, 0)
		if index.kind == "hash" {
			rowIDs = append(rowIDs, index.lookupBucket(literalKey(ast.Literal{Kind: ast.LiteralNull}))...)
			sort.Ints(rowIDs)
			return rowIDs
		}

		for _, entry := range index.allEntries() {
			if entry.value.Kind != ast.LiteralNull {
				continue
			}
			rowIDs = append(rowIDs, entry.rowID)
		}
		return rowIDs
	}

	if predicate.Operator == "IS NOT NULL" {
		rowIDs := make([]int, 0)
		if index.kind == "hash" {
			nullKey := literalKey(ast.Literal{Kind: ast.LiteralNull})
			index.foreachBucket(func(key string, bucket []int) {
				if key == nullKey {
					return
				}
				rowIDs = append(rowIDs, bucket...)
			})
			sort.Ints(rowIDs)
			return rowIDs
		}

		for _, entry := range index.allEntries() {
			if entry.value.Kind == ast.LiteralNull {
				continue
			}
			rowIDs = append(rowIDs, entry.rowID)
		}
		return rowIDs
	}

	if predicate.Operator == "=" {
		return rowIDsForEquality(index, predicate.Value)
	}

	if predicate.Operator == "IN" {
		return rowIDsForInList(index, predicate.InValues)
	}

	if index.kind != "btree" {
		return nil
	}

	rowIDs := make([]int, 0)
	for _, entry := range index.allEntries() {
		if compareLiteralByOperator(entry.value, predicate.Operator, predicate.Value) {
			rowIDs = append(rowIDs, entry.rowID)
		}
	}

	return rowIDs
}

func rowIDsForInList(index *indexState, values []ast.Literal) []int {
	if index == nil || len(values) == 0 {
		return nil
	}

	rowIDs := make([]int, 0, len(values))
	for _, value := range values {
		if value.Kind == ast.LiteralNull {
			continue
		}
		rowIDs = append(rowIDs, rowIDsForEquality(index, value)...)
	}
	if len(rowIDs) == 0 {
		return nil
	}
	sort.Ints(rowIDs)
	return dedupeSortedRowIDs(rowIDs)
}

func rowIDsForEquality(index *indexState, value ast.Literal) []int {
	if index == nil {
		return nil
	}

	if value.Kind == ast.LiteralNull {
		return nil
	}

	if index.kind == "hash" {
		return index.lookupBucket(literalKey(value))
	}

	rowIDs := make([]int, 0)
	for _, entry := range index.allEntries() {
		if compareLiterals(entry.value, value) == 0 {
			rowIDs = append(rowIDs, entry.rowID)
		}
	}

	return rowIDs
}

func addIndexEntry(idx *indexState, entry indexEntry) {
	if idx == nil {
		return
	}
	if idx.kind == "hash" {
		idx.addToBucket(literalKey(entry.value), entry.rowID)
		return
	}

	// INSERT uses overlay indexes created by `shareTableForInsert()`. On those
	// short-lived overlay levels, maintaining global sort order per append turns
	// each row insert into O(N) slice shifting. Appending unsorted and sorting
	// lazily on the read path keeps write-heavy workloads close to O(1).
	if idx.parent != nil {
		idx.entries = append(idx.entries, entry)
		idx.unsortedEntries = true
		return
	}

	idx.entries = insertIndexEntry(idx.entries, entry)
}

func insertIndexEntry(entries []indexEntry, target indexEntry) []indexEntry {
	insertAt := sort.Search(len(entries), func(i int) bool {
		cmp := compareIndexEntries(entries[i], target)
		if cmp != 0 {
			return cmp > 0
		}
		return entries[i].rowID >= target.rowID
	})

	entries = append(entries, indexEntry{})
	copy(entries[insertAt+1:], entries[insertAt:])
	entries[insertAt] = target
	return entries
}

func compareIndexEntries(left, right indexEntry) int {
	leftValues := left.values
	rightValues := right.values

	if len(leftValues) == 0 {
		leftValues = []ast.Literal{left.value}
	}
	if len(rightValues) == 0 {
		rightValues = []ast.Literal{right.value}
	}

	maxLen := len(leftValues)
	if len(rightValues) < maxLen {
		maxLen = len(rightValues)
	}

	for i := 0; i < maxLen; i++ {
		cmp := compareLiterals(leftValues[i], rightValues[i])
		if cmp != 0 {
			return cmp
		}
	}

	if len(leftValues) < len(rightValues) {
		return -1
	}
	if len(leftValues) > len(rightValues) {
		return 1
	}

	return 0
}

func buildIndexEntryForRow(index *indexState, row []ast.Literal, colIdx map[string]int, rowID int) (indexEntry, bool) {
	if index == nil || row == nil {
		return indexEntry{}, false
	}

	columns := index.columns
	if len(columns) == 0 && index.column != "" {
		columns = []string{index.column}
	}
	if len(columns) == 0 {
		return indexEntry{}, false
	}

	values := make([]ast.Literal, 0, len(columns))
	for _, column := range columns {
		pos, ok := colIdx[column]
		if !ok || pos >= len(row) {
			return indexEntry{}, false
		}
		values = append(values, row[pos])
	}

	entry := indexEntry{value: values[0], rowID: rowID}
	if len(values) > 1 {
		entry.values = values
	}

	return entry, true
}

func indexColumnSetKey(columns []string) string {
	if len(columns) == 0 {
		return ""
	}
	return strings.Join(columns, ",")
}
