package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ─── query_sql ───────────────────────────────────────────────────────

type querySQLArgs struct {
	SQL    string `json:"sql" jsonschema:"the SELECT / WITH / TABLE / EXPLAIN statement to execute; must be a single read-only statement"`
	Domain string `json:"domain,omitempty" jsonschema:"optional domain to open with BEGIN DOMAIN before the query runs; use when the query references tables without a domain prefix"`
}

func registerQuerySQL(s *mcp.Server, c *conn, cfg *config) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "query_sql",
		Description: `Execute a read-only SQL statement against ASQL and return the result rows as a markdown table.

Accepts SELECT, WITH (CTE), TABLE, EXPLAIN, and SHOW. Does not accept INSERT, UPDATE, DELETE, or any DDL — use other tools for structured introspection and the allow-writes mode for mutations.

Useful for ad-hoc queries when you already know the table layout. For exploration, prefer list_domains + describe_table.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args querySQLArgs) (*mcp.CallToolResult, any, error) {
		sql := strings.TrimSpace(args.SQL)
		if sql == "" {
			return toolError("sql is required"), nil, nil
		}
		if !isReadOnlySQL(sql) {
			return toolError("query_sql only accepts read-only statements (SELECT, WITH, TABLE, EXPLAIN, SHOW, TAIL); use execute_sql when -allow-writes is enabled"), nil, nil
		}
		out, err := runQuery(ctx, c, args.Domain, sql, cfg.MaxRows)
		if err != nil {
			return toolError("query failed: %v", err), nil, nil
		}
		return toolText(out), nil, nil
	})
}

// ─── execute_sql (gated by -allow-writes) ─────────────────────────────

type executeSQLArgs struct {
	Domain string   `json:"domain" jsonschema:"domain to open with BEGIN DOMAIN (required)"`
	SQL    []string `json:"sql" jsonschema:"one or more SQL statements to run inside a single domain transaction; DDL and DML both accepted"`
}

func registerExecuteSQL(s *mcp.Server, c *conn, _ *config) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "execute_sql",
		Description: `Execute one or more SQL statements inside a single BEGIN DOMAIN transaction. Only available when asql-mcp was launched with -allow-writes.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args executeSQLArgs) (*mcp.CallToolResult, any, error) {
		domain := normalizeDomain(args.Domain)
		if domain == "" {
			return toolError("domain is required"), nil, nil
		}
		if len(args.SQL) == 0 {
			return toolError("sql is required (provide at least one statement)"), nil, nil
		}
		// Run inside a single connection so the BEGIN DOMAIN / statements /
		// COMMIT are all handled by the same session.
		pgc, err := c.pool.Acquire(ctx)
		if err != nil {
			return toolError("acquire connection: %v", err), nil, nil
		}
		defer pgc.Release()

		tag := func(s string) error {
			_, err := pgc.Exec(ctx, s)
			return err
		}
		if err := tag("BEGIN DOMAIN " + domain); err != nil {
			return toolError("BEGIN DOMAIN %s: %v", domain, err), nil, nil
		}
		committed := false
		defer func() {
			if !committed {
				_, _ = pgc.Exec(ctx, "ROLLBACK")
			}
		}()
		for i, stmt := range args.SQL {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if err := tag(stmt); err != nil {
				return toolError("statement %d (%q) failed: %v", i+1, truncate(stmt, 80), err), nil, nil
			}
		}
		if err := tag("COMMIT"); err != nil {
			return toolError("COMMIT: %v", err), nil, nil
		}
		committed = true
		return toolText(fmt.Sprintf("OK — %d statement(s) committed in domain %s.", len(args.SQL), domain)), nil, nil
	})
}

// ─── explain_query ───────────────────────────────────────────────────

type explainArgs struct {
	SQL string `json:"sql" jsonschema:"SELECT statement to explain"`
}

func registerExplain(s *mcp.Server, c *conn) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "explain_query",
		Description: `Return the query plan for a SELECT statement. Wraps the query in EXPLAIN if needed.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args explainArgs) (*mcp.CallToolResult, any, error) {
		sql := strings.TrimSpace(args.SQL)
		if sql == "" {
			return toolError("sql is required"), nil, nil
		}
		if !strings.HasPrefix(strings.ToUpper(sql), "EXPLAIN") {
			sql = "EXPLAIN " + sql
		}
		out, err := runQuery(ctx, c, "", sql, 256)
		if err != nil {
			return toolError("explain failed: %v", err), nil, nil
		}
		return toolText(out), nil, nil
	})
}

// ─── current_lsn ─────────────────────────────────────────────────────

func registerCurrentLSN(s *mcp.Server, c *conn) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "current_lsn",
		Description: `Return the current log sequence number (LSN) of the ASQL engine. Useful before taking an AS OF LSN snapshot of live data.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		var lsn int64
		if err := c.pool.QueryRow(ctx, "SELECT current_lsn()").Scan(&lsn); err != nil {
			return toolError("current_lsn: %v", err), nil, nil
		}
		return toolText(fmt.Sprintf("Current LSN: %d", lsn)), nil, nil
	})
}

// ─── helpers ─────────────────────────────────────────────────────────

// runQuery runs a SELECT/WITH/EXPLAIN and formats the result as a
// markdown table capped at maxRows.  If domain is non-empty, the query
// runs inside a BEGIN DOMAIN transaction so IMPORTs and domain-qualified
// references resolve correctly.
func runQuery(ctx context.Context, c *conn, domain, sql string, maxRows int) (string, error) {
	if maxRows <= 0 {
		maxRows = 1000
	}

	var rows pgx.Rows
	var err error
	if domain = normalizeDomain(domain); domain != "" {
		pgc, aerr := c.pool.Acquire(ctx)
		if aerr != nil {
			return "", aerr
		}
		defer pgc.Release()
		if _, err := pgc.Exec(ctx, "BEGIN DOMAIN "+domain); err != nil {
			return "", fmt.Errorf("BEGIN DOMAIN %s: %w", domain, err)
		}
		defer pgc.Exec(ctx, "COMMIT")
		rows, err = pgc.Query(ctx, sql)
	} else {
		rows, err = c.pool.Query(ctx, sql)
	}
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var b strings.Builder

	// Collect field names once.
	descs := rows.FieldDescriptions()
	cols := make([]string, len(descs))
	for i, d := range descs {
		cols[i] = string(d.Name)
	}
	if len(cols) == 0 {
		// No columns (e.g. for INSERT ... RETURNING that got past the filter
		// and produced nothing, or an EXPLAIN with no output).
		return "(no rows)", nil
	}

	// Header row
	b.WriteString("| ")
	b.WriteString(strings.Join(cols, " | "))
	b.WriteString(" |\n|")
	for range cols {
		b.WriteString("---|")
	}
	b.WriteString("\n")

	count := 0
	truncated := false
	for rows.Next() {
		if count >= maxRows {
			truncated = true
			break
		}
		values, verr := rows.Values()
		if verr != nil {
			return "", verr
		}
		b.WriteString("| ")
		parts := make([]string, len(values))
		for i, v := range values {
			parts[i] = formatValue(v)
		}
		b.WriteString(strings.Join(parts, " | "))
		b.WriteString(" |\n")
		count++
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	fmt.Fprintf(&b, "\n%d row(s)", count)
	if truncated {
		fmt.Fprintf(&b, " — result capped at %d (increase -max-rows to see more)", maxRows)
	}
	return b.String(), nil
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch t := v.(type) {
	case []byte:
		return string(t)
	case string:
		// Markdown cell escaping: pipes and newlines.
		s := strings.ReplaceAll(t, "|", "\\|")
		s = strings.ReplaceAll(s, "\n", " ")
		return s
	default:
		return fmt.Sprintf("%v", t)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
