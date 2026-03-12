package executor

import (
	"strings"
	"sync"

	"asql/internal/engine/parser/ast"
)

// stringInterner deduplicates backing strings for LiteralString row values.
// Repeated column values like "active", "pending", "US" are stored once in the
// intern table and all rows with the same value share that pointer.
// This reduces heap allocations and GC pressure under high-cardinality inserts
// where many rows share categorical string values.
//
// Only strings at or below stringInternMaxLen are interned to avoid bloating the
// intern table with long unique values (UUIDs, free-form text, etc.).
const stringInternMaxLen = 128

// stringInternMaxEntries bounds the global intern table so sustained ingest of
// mostly-unique strings cannot grow it without limit and turn every INSERT into
// additional lock/GC work.
const stringInternMaxEntries = 32_768

var globalStringIntern = struct {
	mu    sync.RWMutex
	table map[string]string
}{table: make(map[string]string, 1024)}

// shouldInternString filters out values that are unlikely to repeat enough to
// justify global interning. This keeps the interner focused on low-cardinality
// categorical values (statuses, enums, domains) and avoids retaining large
// volumes of unique identifiers such as UUIDs.
func shouldInternString(s string) bool {
	if len(s) == 0 || len(s) > stringInternMaxLen {
		return false
	}
	if looksLikeUUIDString(s) {
		return false
	}

	digits := 0
	separators := 0
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			digits++
		case r == '-' || r == '_' || r == ':':
			separators++
		}
	}

	// High-cardinality identifiers tend to mix many digits with separators.
	if digits >= 6 {
		return false
	}
	if digits >= 3 && separators >= 2 {
		return false
	}

	// Long multi-token labels are usually unique job names rather than enums.
	if len(s) > 48 && strings.IndexByte(s, ' ') >= 0 {
		return false
	}

	return true
}

func looksLikeUUIDString(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, r := range s {
		switch i {
		case 8, 13, 18, 23:
			if r != '-' {
				return false
			}
		default:
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
				return false
			}
		}
	}
	return true
}

// internString returns a deduplicated version of s.  If s is already in the
// table the existing string is returned; otherwise s is added and returned.
// Long strings (> stringInternMaxLen bytes) are returned as-is.
func internString(s string) string {
	if !shouldInternString(s) {
		return s
	}
	globalStringIntern.mu.RLock()
	if c, ok := globalStringIntern.table[s]; ok {
		globalStringIntern.mu.RUnlock()
		return c
	}
	globalStringIntern.mu.RUnlock()

	globalStringIntern.mu.Lock()
	if len(globalStringIntern.table) >= stringInternMaxEntries {
		globalStringIntern.mu.Unlock()
		return s
	}
	// Double-check after acquiring write lock.
	if c, ok := globalStringIntern.table[s]; ok {
		globalStringIntern.mu.Unlock()
		return c
	}
	globalStringIntern.table[s] = s
	globalStringIntern.mu.Unlock()
	return s
}

// internRowStrings applies string interning to all LiteralString values in a
// row map.  Called at INSERT time so every inserted row benefits immediately.
func internRowStrings(row map[string]ast.Literal) {
	for col, lit := range row {
		if lit.Kind == ast.LiteralString {
			interned := internString(lit.StringValue)
			if interned != lit.StringValue {
				lit.StringValue = interned
				row[col] = lit
			}
		}
	}
}

// internTableRowStrings interns all existing rows in a table.  Called once
// after snapshot load so historical data benefits from deduplication too.
func internTableRowStrings(table *tableState) {
	for _, row := range table.rows {
		for i, lit := range row {
			if lit.Kind == ast.LiteralString {
				interned := internString(lit.StringValue)
				if interned != lit.StringValue {
					row[i].StringValue = interned
				}
			}
		}
	}
}
