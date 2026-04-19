package pgwire

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"
)

var reAsOfComment = regexp.MustCompile(`(?i)/\*\s*as-of-(lsn|ts):\s*\d+\s*\*/`)
var reAsOfExtract = regexp.MustCompile(`(?i)/\*\s*as-of-(lsn|ts):\s*(\d+)\s*\*/`)

// stripAsOfComment removes /* as-of-lsn: N */ and /* as-of-ts: N */ tokens
// from SQL before parsing so the parser doesn't choke on them.
func stripAsOfComment(sql string) string {
	return strings.TrimSpace(reAsOfComment.ReplaceAllString(sql, ""))
}

// extractAsOf parses an optional /* as-of-lsn: N */ or /* as-of-ts: N */
// token appended to a SQL statement. Returns:
//
//	kind    – "lsn", "ts", or "" (no comment present)
//	value   – the numeric value (0 when kind=="")
//	stripped – SQL with the comment removed AND trailing semicolons/whitespace trimmed
//
// This is used by executeSQL so we can route to TimeTravelQueryAsOfLSN /
// TimeTravelQueryAsOfTimestamp with the exact LSN/TS the caller requested,
// while handing the parser a clean statement.
func extractAsOf(sql string) (kind string, value uint64, stripped string) {
	if m := reAsOfExtract.FindStringSubmatch(sql); m != nil {
		kind = strings.ToLower(m[1])
		value, _ = strconv.ParseUint(m[2], 10, 64)
	}
	// Remove the comment token and strip trailing semicolons that may have
	// preceded it (e.g. "SELECT ... LIMIT 100; /* as-of-lsn: N */").
	stripped = strings.TrimRight(strings.TrimSpace(reAsOfComment.ReplaceAllString(sql, "")), "; \t\n\r")
	return
}

// literalToText converts an ast.Literal to its PostgreSQL text-wire representation.
func literalToText(lit ast.Literal) []byte {
	switch lit.Kind {
	case ast.LiteralNumber:
		return []byte(strconv.FormatInt(lit.NumberValue, 10))
	case ast.LiteralString:
		return []byte(lit.StringValue)
	case ast.LiteralBoolean:
		if lit.BoolValue {
			return []byte("t")
		}
		return []byte("f")
	case ast.LiteralFloat:
		return []byte(strconv.FormatFloat(lit.FloatValue, 'g', -1, 64))
	case ast.LiteralTimestamp:
		t := time.UnixMicro(lit.NumberValue).UTC()
		return []byte(t.Format("2006-01-02 15:04:05"))
	case ast.LiteralJSON:
		return []byte(lit.StringValue)
	default:
		return nil
	}
}

// allColumnsFromRows collects every column key that appears in any row and
// returns them in canonical column order (see sortColumns).
// Used where a deterministic column list is required regardless of Go map
// iteration order.
func allColumnsFromRows(rows []map[string]ast.Literal) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for k := range row {
			seen[k] = struct{}{}
		}
	}
	cols := make([]string, 0, len(seen))
	for k := range seen {
		cols = append(cols, k)
	}
	sortColumns(cols)
	return cols
}

// sortColumns sorts a column slice in the canonical display order:
//  1. FOR HISTORY contract metadata columns in explicit order.
//  2. Other system columns (names that start with "_").
//  3. "id" (exact match, case-insensitive).
//  4. All remaining columns, sorted alphabetically.
//
// This produces a consistent, predictable column order across SELECT *,
// schema snapshots, and fallback row-derived column lists.
func sortColumns(cols []string) {
	sort.SliceStable(cols, func(i, j int) bool {
		a, b := cols[i], cols[j]
		aHistoryRank, aIsHistory := historyColumnRank(a)
		bHistoryRank, bIsHistory := historyColumnRank(b)
		aIsSys := strings.HasPrefix(a, "_")
		bIsSys := strings.HasPrefix(b, "_")
		aIsID := strings.EqualFold(a, "id")
		bIsID := strings.EqualFold(b, "id")

		switch {
		case aIsHistory && bIsHistory:
			return aHistoryRank < bHistoryRank
		case aIsHistory:
			return true
		case bIsHistory:
			return false
		case aIsSys && bIsSys:
			return a < b // both system: alphabetical
		case aIsSys:
			return true // system before everything
		case bIsSys:
			return false
		case aIsID && bIsID:
			return false // identical
		case aIsID:
			return true // id before non-system non-id
		case bIsID:
			return false
		default:
			return a < b // both regular: alphabetical
		}
	})
}

func historyColumnRank(col string) (int, bool) {
	switch col {
	case executor.HistoryOperationColumnName:
		return 0, true
	case executor.HistoryCommitLSNColumnName:
		return 1, true
	default:
		return 0, false
	}
}
