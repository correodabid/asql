package asqldb

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// ── Pagination helpers ──────────────────────────────────────────────────────

// paginationClause returns " LIMIT <size> OFFSET <offset>" for a ListFilter.
func paginationClause(f model.ListFilter) string {
	page := f.Page
	if page < 1 {
		page = 1
	}
	size := f.PageSize
	if size < 1 {
		size = 20
	}
	offset := (page - 1) * size
	return fmt.Sprintf(" LIMIT %d OFFSET %d", size, offset)
}

// orderClause returns " ORDER BY <col> <dir>" or empty string.
func orderClause(f model.ListFilter, defaultCol string) string {
	col := f.SortBy
	if col == "" {
		col = defaultCol
	}
	dir := f.SortDir
	if dir == "" {
		dir = "ASC"
	}
	return fmt.Sprintf(" ORDER BY %s %s", col, dir)
}

// buildListResult creates a ListResult from items and a total count.
func buildListResult[T any](items []T, total int, f model.ListFilter) *model.ListResult[T] {
	page := f.Page
	if page < 1 {
		page = 1
	}
	size := f.PageSize
	if size < 1 {
		size = 20
	}
	totalPages := total / size
	if total%size > 0 {
		totalPages++
	}
	return &model.ListResult[T]{
		Items:      items,
		Total:      total,
		Page:       page,
		PageSize:   size,
		TotalPages: totalPages,
	}
}

// ── Type conversion helpers ─────────────────────────────────────────────────

// parseUUID safely parses a string UUID, returning uuid.Nil on empty/invalid.
func parseUUID(s string) uuid.UUID {
	if s == "" {
		return uuid.Nil
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil
	}
	return id
}

// ts converts a time.Time to a format ASQL can store.
func ts(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// tsPtr formats a *time.Time as timestamp string.
func tsPtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return ts(*t)
}

// parseTS parses a timestamp string from ASQL.
// When pgx auto-decodes timestamps (OID 1114), it returns time.Time directly;
// fmt.Sprintf("%v", t) produces "2006-01-02 15:04:05 +0000 UTC".
func parseTS(s string) time.Time {
	if s == "" || s == "<nil>" {
		return time.Time{}
	}
	// Try PostgreSQL format first (what ASQL now sends).
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
		return t.UTC()
	}
	// pgx may return time.Time formatted via fmt.Sprintf("%v",...).
	if t, err := time.Parse("2006-01-02 15:04:05 +0000 UTC", s); err == nil {
		return t.UTC()
	}
	if t, err := time.Parse("2006-01-02 15:04:05 -0700 MST", s); err == nil {
		return t.UTC()
	}
	// Fallback RFC3339.
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC()
	}
	return time.Time{}
}

// nullableTS returns NULL or a quoted timestamp for SQL interpolation.
func nullableTS(t *time.Time) string {
	if t == nil {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", ts(*t))
}

// sqlStr escapes single quotes for safe SQL interpolation.
// ASQL's pgwire uses text-mode parameter substitution.
func sqlStr(s string) string {
	return "'" + escapeSQL(s) + "'"
}

// escapeSQL doubles single quotes.
func escapeSQL(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			result = append(result, '\'', '\'')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}

// boolToSQL converts a bool to ASQL boolean literal.
func boolToSQL(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// timeNow returns UTC now. Extracted for determinism.
func timeNow() time.Time { return time.Now().UTC() }
