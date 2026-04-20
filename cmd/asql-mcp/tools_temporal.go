package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ─── read_as_of_lsn ──────────────────────────────────────────────────

type readAsOfArgs struct {
	SQL    string `json:"sql" jsonschema:"the SELECT/WITH statement to run; do NOT include AS OF LSN — the tool appends it for you"`
	LSN    int64  `json:"lsn" jsonschema:"commit LSN to read as-of (from current_lsn or row_history)"`
	Domain string `json:"domain,omitempty" jsonschema:"optional domain to open before the query"`
}

func registerReadAsOfLSN(s *mcp.Server, c *conn, cfg *config) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "read_as_of_lsn",
		Description: `Run a SELECT against the state that existed at commit position LSN.
This is ASQL's time-travel primitive: instead of reconstructing history
from a CDC pipeline or audit table, you query any past commit directly.

Example flow:
  1. current_lsn() returns 124
  2. row_history(users) shows that user 7 was UPDATEd at LSN 100
  3. read_as_of_lsn("SELECT * FROM app.users WHERE id = 7", 99) returns
     the row as it existed just before that update.

No other SQL database offers this natively.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args readAsOfArgs) (*mcp.CallToolResult, any, error) {
		sql := strings.TrimSpace(args.SQL)
		if sql == "" || args.LSN <= 0 {
			return toolError("both sql (non-empty) and lsn (>0) are required"), nil, nil
		}
		if !isReadOnlySQL(sql) {
			return toolError("read_as_of_lsn only accepts read-only statements"), nil, nil
		}
		// If the SQL already contains "AS OF LSN", reject so we don't
		// produce a confusingly double-qualified query.
		upper := strings.ToUpper(sql)
		if strings.Contains(upper, "AS OF LSN") {
			return toolError("SQL already contains AS OF LSN; remove it and pass lsn instead"), nil, nil
		}
		// ASQL's AS OF LSN clause attaches to a table reference in the
		// FROM list, not to an outer query. Append it verbatim — the
		// caller is responsible for passing a SELECT that references
		// exactly one table (or uses IMPORT).
		sql = strings.TrimRight(sql, "; \n\t")
		withClause := sql + fmt.Sprintf(" AS OF LSN %d", args.LSN)
		out, err := runQuery(ctx, c, args.Domain, withClause, cfg.MaxRows)
		if err != nil {
			return toolError("read_as_of_lsn failed: %v", err), nil, nil
		}
		return toolText(fmt.Sprintf("Result at LSN %d:\n\n%s", args.LSN, out)), nil, nil
	})
}

// ─── row_history ─────────────────────────────────────────────────────

type rowHistoryArgs struct {
	Domain string `json:"domain" jsonschema:"domain (schema) containing the table"`
	Table  string `json:"table" jsonschema:"table to inspect"`
	Limit  int    `json:"limit,omitempty" jsonschema:"max history rows to return (default 100)"`
	Where  string `json:"where,omitempty" jsonschema:"optional SQL WHERE clause (without the WHERE keyword) to filter by primary key, e.g. id = 42"`
}

func registerRowHistory(s *mcp.Server, c *conn, cfg *config) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "row_history",
		Description: `Return the row-level change history for a table using ASQL's FOR HISTORY
clause. Each row in the output carries:
  __operation   INSERT | UPDATE | DELETE
  __commit_lsn  the LSN at which this mutation committed
  <columns>     the row image at that commit (post-state for INSERT/UPDATE,
                pre-state for DELETE)

This is a first-class part of ASQL — no CDC pipeline, no triggers, no
auxiliary audit table. Pair with read_as_of_lsn to inspect full database
state around any interesting commit.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args rowHistoryArgs) (*mcp.CallToolResult, any, error) {
		domain := normalizeDomain(args.Domain)
		table := strings.ToLower(strings.TrimSpace(args.Table))
		if domain == "" || table == "" {
			return toolError("both domain and table are required"), nil, nil
		}
		limit := args.Limit
		if limit <= 0 || limit > cfg.MaxRows {
			limit = 100
		}
		where := strings.TrimSpace(args.Where)
		if where != "" {
			where = " WHERE " + where
		}
		sql := fmt.Sprintf("SELECT * FROM %s.%s FOR HISTORY%s ORDER BY __commit_lsn DESC LIMIT %d",
			domain, table, where, limit)
		out, err := runQuery(ctx, c, "", sql, limit)
		if err != nil {
			return toolError("row_history failed: %v", err), nil, nil
		}
		return toolText(out), nil, nil
	})
}
