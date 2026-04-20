package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ─── entity_version_history ──────────────────────────────────────────

type entityVersionArgs struct {
	Domain string `json:"domain" jsonschema:"domain containing the entity root table"`
	Entity string `json:"entity" jsonschema:"entity name (the aggregate name declared in CREATE ENTITY)"`
	RootPK string `json:"root_pk" jsonschema:"primary key value of the entity root row"`
	Limit  int    `json:"limit,omitempty" jsonschema:"max versions to return (default 50)"`
}

func registerEntityVersionHistory(s *mcp.Server, c *conn, cfg *config) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "entity_version_history",
		Description: `List every version of an entity aggregate, latest first. An ASQL entity
groups a root table plus related tables; its version increments whenever
any table in the group is modified. Each version row returns its LSN,
timestamp, and the tables whose changes contributed.

Useful for answering "when did this order last change?" without
reconstructing a JOIN history manually. Pair with read_as_of_lsn to see
the aggregate state at any version.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args entityVersionArgs) (*mcp.CallToolResult, any, error) {
		domain := normalizeDomain(args.Domain)
		entity := strings.ToLower(strings.TrimSpace(args.Entity))
		root := strings.TrimSpace(args.RootPK)
		if domain == "" || entity == "" || root == "" {
			return toolError("domain, entity, and root_pk are all required"), nil, nil
		}
		limit := args.Limit
		if limit <= 0 || limit > cfg.MaxRows {
			limit = 50
		}
		sql := fmt.Sprintf(`
			TAIL ENTITY CHANGES %s.%s FOR '%s' LIMIT %d
		`, domain, entity, escapeLiteral(root), limit)
		out, err := runQuery(ctx, c, "", sql, limit)
		if err != nil {
			return toolError("entity_version_history failed: %v", err), nil, nil
		}
		return toolText(out), nil, nil
	})
}

// ─── resolve_reference ───────────────────────────────────────────────

type resolveReferenceArgs struct {
	QualifiedTable string `json:"qualified_table" jsonschema:"table reference in domain.table form (e.g. billing.invoices)"`
	Key            string `json:"key" jsonschema:"primary key value to resolve"`
}

func registerResolveReference(s *mcp.Server, c *conn) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "resolve_reference",
		Description: `Return the exact token a versioned foreign key would capture right now
for a given (domain.table, pk) pair. For entity-root tables, this is the
entity version; for ordinary tables, it is the row-head LSN.

Use this when reasoning about what a VERSIONED FOREIGN KEY ... AS OF
column is about to store, or when debugging why a JOIN through a
versioned reference returned a specific historical row.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args resolveReferenceArgs) (*mcp.CallToolResult, any, error) {
		qt := strings.ToLower(strings.TrimSpace(args.QualifiedTable))
		key := strings.TrimSpace(args.Key)
		if qt == "" || !strings.Contains(qt, ".") || key == "" {
			return toolError("qualified_table (in domain.table form) and key are required"), nil, nil
		}
		sql := fmt.Sprintf("SELECT resolve_reference('%s', '%s')",
			escapeLiteral(qt), escapeLiteral(key))
		out, err := runQuery(ctx, c, "", sql, 1)
		if err != nil {
			return toolError("resolve_reference failed: %v", err), nil, nil
		}
		return toolText(out), nil, nil
	})
}
