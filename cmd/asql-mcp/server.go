package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// buildServer creates the MCP server and registers every tool.
func buildServer(c *conn, cfg *config) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "asql-mcp",
		Title:   "ASQL Database",
		Version: version,
	}, &mcp.ServerOptions{
		Instructions: `ASQL is a deterministic SQL engine that speaks pgwire.
Unique capabilities beyond standard PostgreSQL: time-travel reads with
AS OF LSN, row history via FOR HISTORY, explicit domain boundaries,
and entity versioning.

Use list_domains first to discover tenants, then describe_table to
explore a schema. Use read_as_of_lsn / row_history to audit historical
state — a capability no other SQL database offers natively.

All tools are read-only by default. The server refuses DDL and
(unless -allow-writes was passed) all DML.`,
	})

	registerQuerySQL(server, c, cfg)
	registerExplain(server, c)
	registerCurrentLSN(server, c)
	registerListDomains(server, c)
	registerDescribeSchema(server, c)
	registerDescribeTable(server, c)
	registerReadAsOfLSN(server, c, cfg)
	registerRowHistory(server, c, cfg)
	registerEntityVersionHistory(server, c, cfg)
	registerResolveReference(server, c)

	if cfg.AllowWrites {
		registerExecuteSQL(server, c, cfg)
	}

	return server
}

// toolError returns a Content slice that signals a tool-level error.
// The MCP spec uses IsError=true on the result, not a transport error.
func toolError(msg string, args ...any) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf(msg, args...)},
		},
	}
}

// toolText returns a plain-text success result.
func toolText(s string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: s}},
	}
}

// normalizeDomain trims and lowercases a domain name, since ASQL
// domain names are case-folded internally.
func normalizeDomain(d string) string { return strings.ToLower(strings.TrimSpace(d)) }

// Used by tools_*.go files.
var _ = context.Background
