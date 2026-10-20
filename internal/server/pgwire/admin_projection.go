package pgwire

import (
	"fmt"
	"regexp"
	"slices"
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

// projectAdminColumns applies the select list of sql to ir, which holds all
// of an asql_admin table's columns. It supports columns, stars and aliases;
// anything else is rejected rather than answered with the wrong shape.
func projectAdminColumns(sql string, ir interceptResult) interceptResult {
	if ir.err != nil {
		return ir
	}
	m := reAdminSelectList.FindStringSubmatch(sql)
	if m == nil {
		return ir
	}
	list := strings.TrimSpace(m[1])
	if list == "*" {
		return ir
	}

	type output struct{ name, source string }
	var outputs []output
	for _, item := range strings.Split(list, ",") {
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

	columns := make([]string, len(outputs))
	for i, o := range outputs {
		columns[i] = o.name
	}
	rows := make([]map[string]ast.Literal, len(ir.result.Rows))
	for i, row := range ir.result.Rows {
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
