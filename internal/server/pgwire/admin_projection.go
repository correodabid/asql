package pgwire

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// reAdminSelectList captures the select list of a query on an asql_admin
// virtual table.
var reAdminSelectList = regexp.MustCompile(`(?is)^\s*select\s+(.*?)\s+from\s+asql_admin\.`)

// reAdminSelectItem matches one select-list item: a column or a star,
// optionally qualified, with an optional alias.
var reAdminSelectItem = regexp.MustCompile(`(?is)^(?:(?:"[^"]+"|[a-z_][a-z0-9_$]*)\.)*("[^"]+"|[a-z_][a-z0-9_$]*|\*)(?:\s+(?:as\s+)?("[^"]+"|[a-z_][a-z0-9_$]*))?$`)

// projectAdminColumns applies the select list, ORDER BY, OFFSET and LIMIT of
// sql to ir, which holds all of an asql_admin table's columns. It supports
// columns, stars and aliases in the select list and ORDER BY; anything else
// is rejected rather than answered with the wrong shape or order.
func projectAdminColumns(sql string, ir interceptResult) interceptResult {
	if ir.err != nil {
		return ir
	}
	m := reAdminSelectList.FindStringSubmatch(sql)
	if m == nil {
		return ir
	}
	tail, err := parseAdminTail(sql)
	if err != nil {
		return interceptResult{err: err}
	}
	list := strings.TrimSpace(m[1])

	type output struct{ name, source string }
	var outputs []output
	items := strings.Split(list, ",")
	if list == "*" {
		items = nil
		for _, column := range ir.columns {
			outputs = append(outputs, output{column, column})
		}
	}
	for _, item := range items {
		item = strings.TrimSpace(item)
		parts := reAdminSelectItem.FindStringSubmatch(item)
		if parts == nil {
			return interceptResult{err: sqlerr.New("0A000",
				fmt.Sprintf("asql_admin tables support only column names, * and aliases in the select list, not %q", item))}
		}
		if parts[1] == "*" {
			if parts[2] != "" {
				return interceptResult{err: sqlerr.New("42601", fmt.Sprintf("syntax error at or near %q", parts[2]))}
			}
			for _, column := range ir.columns {
				outputs = append(outputs, output{column, column})
			}
			continue
		}
		source := identifierName(parts[1])
		// Tables whose columns come from their rows have none when empty.
		if len(ir.columns) > 0 && !slices.Contains(ir.columns, source) {
			return interceptResult{err: sqlerr.New("42703", fmt.Sprintf("column %q does not exist", source))}
		}
		name := source
		if parts[2] != "" {
			name = identifierName(parts[2])
		}
		outputs = append(outputs, output{name, source})
	}

	// ORDER BY names an output column or a column of the table.
	sources := make(map[string]string, len(outputs))
	for _, o := range outputs {
		sources[o.name] = o.source
	}
	for i, key := range tail.orderBy {
		if source, ok := sources[key.column]; ok {
			tail.orderBy[i].column = source
		} else if len(ir.columns) > 0 && !slices.Contains(ir.columns, key.column) {
			return interceptResult{err: sqlerr.New("42703", fmt.Sprintf("column %q does not exist", key.column))}
		}
	}
	all := ir.result.Rows
	if len(tail.orderBy) > 0 {
		all = slices.Clone(all)
		slices.SortStableFunc(all, func(a, b map[string]ast.Literal) int {
			for _, key := range tail.orderBy {
				if c := compareAdminValues(a[key.column], b[key.column], key.desc); c != 0 {
					return c
				}
			}
			return 0
		})
	}
	if tail.offset > 0 {
		all = all[min(tail.offset, len(all)):]
	}
	if tail.limit >= 0 && tail.limit < len(all) {
		all = all[:tail.limit]
	}
	if list == "*" {
		ir.result.Rows = all
		return ir
	}

	columns := make([]string, len(outputs))
	for i, o := range outputs {
		columns[i] = o.name
	}
	rows := make([]map[string]ast.Literal, len(all))
	for i, row := range all {
		projected := make(map[string]ast.Literal, len(outputs))
		for _, o := range outputs {
			if value, ok := row[o.source]; ok {
				projected[o.name] = value
			}
		}
		rows[i] = projected
	}
	ir.columns = columns
	ir.result.Rows = rows
	return ir
}

// identifierName folds an unquoted identifier to lower case and strips the
// quotes from a quoted one, as PostgreSQL does.
func identifierName(identifier string) string {
	if len(identifier) >= 2 && identifier[0] == '"' {
		return identifier[1 : len(identifier)-1]
	}
	return strings.ToLower(identifier)
}

type adminOrderKey struct {
	column string
	desc   bool
}

type adminTail struct {
	orderBy []adminOrderKey
	// limit is -1 when there is none.
	limit, offset int
}

var (
	reAdminOrderKey = regexp.MustCompile(`(?is)^("[^"]+"|[a-z_][a-z0-9_$]*)(?:\s+(asc|desc))?$`)
	reAdminLimit    = regexp.MustCompile(`(?is)\blimit\s+(\d+)`)
	reAdminOffset   = regexp.MustCompile(`(?is)\boffset\s+(\d+)`)
	reAdminClause   = regexp.MustCompile(`(?is)\b(order\s+by|group\s+by|having|limit|offset)\b`)
)

// parseAdminTail reads ORDER BY, LIMIT and OFFSET outside quoted text, so
// the SQL a table takes as a parameter (row_history's sql = '...') does not
// count. GROUP BY and HAVING are rejected.
func parseAdminTail(sql string) (adminTail, error) {
	tail := adminTail{limit: -1}
	// Blank out quoted text, keeping positions.
	blank := []byte(sql)
	for i := 0; i < len(blank); i++ {
		if q := blank[i]; q == '\'' || q == '"' {
			for i++; i < len(blank) && blank[i] != q; i++ {
				blank[i] = ' '
			}
		}
	}
	text := strings.TrimRight(string(blank), "; \t\r\n")
	clauses := reAdminClause.FindAllStringSubmatchIndex(text, -1)
	for n, c := range clauses {
		keyword := strings.ToLower(strings.Join(strings.Fields(text[c[2]:c[3]]), " "))
		end := len(text)
		if n+1 < len(clauses) {
			end = clauses[n+1][0]
		}
		body := strings.TrimSpace(sql[c[1]:end])
		switch keyword {
		case "group by", "having":
			return adminTail{}, sqlerr.New("0A000", fmt.Sprintf("asql_admin tables do not support %s", strings.ToUpper(keyword)))
		case "order by":
			for _, item := range strings.Split(body, ",") {
				parts := reAdminOrderKey.FindStringSubmatch(strings.TrimSpace(item))
				if parts == nil {
					return adminTail{}, sqlerr.New("0A000", fmt.Sprintf("asql_admin tables support only column names in ORDER BY, not %q", strings.TrimSpace(item)))
				}
				tail.orderBy = append(tail.orderBy, adminOrderKey{column: identifierName(parts[1]), desc: strings.EqualFold(parts[2], "desc")})
			}
		case "limit", "offset":
			re := reAdminLimit
			if keyword == "offset" {
				re = reAdminOffset
			}
			m := re.FindStringSubmatch(text[c[0]:end])
			if m == nil || strings.TrimSpace(text[c[0]+len(m[0]):end]) != "" {
				return adminTail{}, sqlerr.New("42601", fmt.Sprintf("syntax error in %s", strings.ToUpper(keyword)))
			}
			value, err := strconv.Atoi(m[1])
			if err != nil {
				return adminTail{}, sqlerr.New("22003", fmt.Sprintf("%s out of range", strings.ToUpper(keyword)))
			}
			if keyword == "limit" {
				tail.limit = value
			} else {
				tail.offset = value
			}
		}
	}
	return tail, nil
}

// compareAdminValues orders two values as PostgreSQL does: NULL last when
// ascending and first when descending.
func compareAdminValues(a, b ast.Literal, desc bool) int {
	aNull, bNull := a.Kind == ast.LiteralNull || a.Kind == "", b.Kind == ast.LiteralNull || b.Kind == ""
	switch {
	case aNull && bNull:
		return 0
	case aNull != desc:
		// a is NULL ascending, or b is NULL descending: a goes after.
		if aNull || bNull {
			return 1
		}
	case aNull || bNull:
		return -1
	}
	c := 0
	switch {
	case a.Kind == ast.LiteralFloat || b.Kind == ast.LiteralFloat:
		af, bf := adminFloat(a), adminFloat(b)
		c = cmpOrdered(af, bf)
	case a.Kind == ast.LiteralString || a.Kind == ast.LiteralJSON:
		c = strings.Compare(a.StringValue, b.StringValue)
	case a.Kind == ast.LiteralBoolean:
		c = cmpOrdered(boolRank(a.BoolValue), boolRank(b.BoolValue))
	default:
		c = cmpOrdered(a.NumberValue, b.NumberValue)
	}
	if desc {
		return -c
	}
	return c
}

func adminFloat(v ast.Literal) float64 {
	if v.Kind == ast.LiteralFloat {
		return v.FloatValue
	}
	return float64(v.NumberValue)
}

func boolRank(b bool) int {
	if b {
		return 1
	}
	return 0
}

func cmpOrdered[T int | int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
