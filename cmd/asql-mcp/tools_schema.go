package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ─── list_domains ────────────────────────────────────────────────────

func registerListDomains(s *mcp.Server, c *conn) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "list_domains",
		Description: `List every domain present in the ASQL catalog along with the tables
each contains. A domain is ASQL's top-level isolation boundary —
every transaction scopes to one or more domains explicitly.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		// ASQL intercepts information_schema.tables and returns a fixed
		// tuple shape (table_schema, table_name, table_type). Aggregation
		// is not supported on that intercept, so we aggregate client-side.
		rows, err := c.pool.Query(ctx, "SELECT * FROM information_schema.tables")
		if err != nil {
			return toolError("list_domains failed: %v", err), nil, nil
		}
		defer rows.Close()

		byDomain := map[string][]string{}
		var order []string
		for rows.Next() {
			values, verr := rows.Values()
			if verr != nil {
				return toolError("list_domains scan: %v", verr), nil, nil
			}
			if len(values) < 2 {
				continue
			}
			schema := fmt.Sprint(values[0])
			table := fmt.Sprint(values[1])
			if schema == "information_schema" || schema == "pg_catalog" {
				continue
			}
			if _, ok := byDomain[schema]; !ok {
				order = append(order, schema)
			}
			byDomain[schema] = append(byDomain[schema], table)
		}
		if err := rows.Err(); err != nil {
			return toolError("list_domains: %v", err), nil, nil
		}
		if len(order) == 0 {
			return toolText("(no user domains yet — create one with `BEGIN DOMAIN <name>`)"), nil, nil
		}

		var b strings.Builder
		b.WriteString("| domain | tables | table names |\n|---|---|---|\n")
		for _, d := range order {
			tables := byDomain[d]
			fmt.Fprintf(&b, "| %s | %d | %s |\n", d, len(tables), strings.Join(tables, ", "))
		}
		fmt.Fprintf(&b, "\n%d domain(s)", len(order))
		return toolText(b.String()), nil, nil
	})
}

// ─── describe_schema ─────────────────────────────────────────────────

type describeSchemaArgs struct {
	Domain string `json:"domain" jsonschema:"domain (schema) to inspect; use list_domains to discover names"`
}

func registerDescribeSchema(s *mcp.Server, c *conn) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "describe_schema",
		Description: `List every table in a domain along with its column count. Use describe_table to drill into a specific table's columns, keys, and foreign-key edges.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args describeSchemaArgs) (*mcp.CallToolResult, any, error) {
		domain := normalizeDomain(args.Domain)
		if domain == "" {
			return toolError("domain is required"), nil, nil
		}
		sql := fmt.Sprintf(`
			SELECT t.table_name, COUNT(c.column_name) AS columns
			FROM information_schema.tables t
			LEFT JOIN information_schema.columns c
			  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
			WHERE t.table_schema = '%s'
			GROUP BY t.table_name
			ORDER BY t.table_name
		`, escapeLiteral(domain))
		out, err := runQuery(ctx, c, "", sql, 500)
		if err != nil {
			return toolError("describe_schema %s: %v", domain, err), nil, nil
		}
		return toolText(out), nil, nil
	})
}

// ─── describe_table ──────────────────────────────────────────────────

type describeTableArgs struct {
	Domain string `json:"domain" jsonschema:"domain (schema) name"`
	Table  string `json:"table" jsonschema:"table name"`
}

func registerDescribeTable(s *mcp.Server, c *conn) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "describe_table",
		Description: `Return the full column layout of a table: name, data type, nullability,
default expression, plus a secondary section listing primary and foreign
keys. This is the canonical exploration tool before writing any query.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args describeTableArgs) (*mcp.CallToolResult, any, error) {
		domain := normalizeDomain(args.Domain)
		table := strings.ToLower(strings.TrimSpace(args.Table))
		if domain == "" || table == "" {
			return toolError("both domain and table are required"), nil, nil
		}

		// ASQL returns a wide JDBC-metadata shape when information_schema.
		// columns is filtered by both schema and table at once. To get the
		// friendly shape, query by schema only and filter client-side.
		rows, err := c.pool.Query(ctx, fmt.Sprintf(
			"SELECT * FROM information_schema.columns WHERE table_schema = '%s'",
			escapeLiteral(domain),
		))
		if err != nil {
			return toolError("describe_table columns: %v", err), nil, nil
		}
		defer rows.Close()

		var b strings.Builder
		fmt.Fprintf(&b, "## %s.%s\n\n### Columns\n\n", domain, table)

		descs := rows.FieldDescriptions()
		headerCols := make([]string, len(descs))
		for i, d := range descs {
			headerCols[i] = string(d.Name)
		}
		// Find the table_name column index so we can filter.
		tableIdx := -1
		for i, h := range headerCols {
			if strings.EqualFold(h, "table_name") {
				tableIdx = i
				break
			}
		}
		if tableIdx < 0 {
			return toolError("describe_table: information_schema.columns did not expose table_name; got %s",
				strings.Join(headerCols, ", ")), nil, nil
		}

		// Header (drop the table_schema/table_name columns since they're
		// the same for every row we emit).
		out := make([]string, 0, len(headerCols))
		keepIdx := make([]int, 0, len(headerCols))
		for i, h := range headerCols {
			lower := strings.ToLower(h)
			if lower == "table_schema" || lower == "table_name" {
				continue
			}
			out = append(out, h)
			keepIdx = append(keepIdx, i)
		}
		b.WriteString("| " + strings.Join(out, " | ") + " |\n|")
		for range out {
			b.WriteString("---|")
		}
		b.WriteString("\n")

		matched := 0
		for rows.Next() {
			values, verr := rows.Values()
			if verr != nil {
				return toolError("describe_table scan: %v", verr), nil, nil
			}
			if fmt.Sprint(values[tableIdx]) != table {
				continue
			}
			parts := make([]string, 0, len(keepIdx))
			for _, ki := range keepIdx {
				parts = append(parts, formatValue(values[ki]))
			}
			b.WriteString("| " + strings.Join(parts, " | ") + " |\n")
			matched++
		}
		if err := rows.Err(); err != nil {
			return toolError("describe_table: %v", err), nil, nil
		}
		if matched == 0 {
			return toolText(fmt.Sprintf("No table named %s.%s — use list_domains / describe_schema to discover names.", domain, table)), nil, nil
		}
		fmt.Fprintf(&b, "\n%d column(s)", matched)
		return toolText(b.String()), nil, nil
	})
}

// escapeLiteral escapes a string for inclusion in a single-quoted SQL
// literal.  Only enough for identifier-like inputs where we don't
// expect quotes; caller is responsible for lowercasing / trimming
// first.
func escapeLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
