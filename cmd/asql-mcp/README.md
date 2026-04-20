# asql-mcp — Model Context Protocol server for ASQL

`asql-mcp` lets any [Model Context Protocol](https://modelcontextprotocol.io)
capable AI agent talk to a running ASQL instance. The agent gets
typed tools for SELECT queries, schema introspection, **time-travel
reads** (`AS OF LSN`), **row history** (`FOR HISTORY`), and **entity
version** inspection — the primitives that make ASQL worth reaching
for over a plain Postgres.

No other SQL database ships its own MCP server. ASQL does.

## Why this matters

Most databases expose themselves to an agent through a generic
`postgres-mcp` or `sql-query-mcp` wrapper. Those work for SELECT but
lose the things that make ASQL different. `asql-mcp` is native — it
understands domains, LSNs, entity versions, and ASQL's historical
read surface, and it exposes them as first-class tools the agent
can reason about.

Example prompt: *"What was the invoice status for customer 42 just
before the March 14 pricing update?"*

A generic SQL wrapper can't answer this. `asql-mcp` can:

1. `current_lsn()` → 124
2. `row_history(domain="billing", table="invoices", where="customer_id = 42")`
   → commit at LSN 100 changed status paid → refunded.
3. `read_as_of_lsn(sql="SELECT * FROM billing.invoices WHERE customer_id = 42", lsn=99)`
   → the pre-refund state.

## Installation

```bash
go install github.com/correodabid/asql/cmd/asql-mcp@latest
```

The binary lands in `$GOBIN` (or `$GOPATH/bin` / `~/go/bin`). From
the repo you can also just run it directly:

```bash
go run ./cmd/asql-mcp
```

## Quick check

Start `asqld` in one terminal:

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
```

Then launch the MCP server with stdio transport (what most agents
use):

```bash
asql-mcp -pgwire 127.0.0.1:5433
```

It logs `connected to asqld at 127.0.0.1:5433 (read-only=true)` on
stderr and then waits for MCP messages on stdin. That's it — it's
designed to be wrapped by an agent, not run by hand.

## Tool catalog

All tools are **read-only** by default. Use `-allow-writes` to
enable `execute_sql`.

| Tool | What it does |
|---|---|
| `query_sql` | Run one read-only SQL statement (SELECT / WITH / TABLE / EXPLAIN / SHOW / TAIL). Rejects DDL and DML. |
| `explain_query` | Return the query plan for a SELECT. |
| `current_lsn` | Return the current commit LSN of the engine. |
| `list_domains` | Enumerate every domain with its table count. |
| `describe_schema` | List tables inside one domain. |
| `describe_table` | Full column layout of a table plus its constraints (PKs, FKs). |
| `read_as_of_lsn` | **Time-travel**: run a SELECT against the state as it was at the given LSN. |
| `row_history` | **Row-level audit**: return the mutation history of a table via `FOR HISTORY`. |
| `entity_version_history` | **Entity**: list every version of an aggregate, via `TAIL ENTITY CHANGES`. |
| `resolve_reference` | Return what a versioned foreign key would capture right now for a given row. |
| `execute_sql` | *(writes mode only)* Run a batch of statements inside one `BEGIN DOMAIN` transaction. |

## Configuring MCP clients

### Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS) or the equivalent on your OS:

```json
{
  "mcpServers": {
    "asql": {
      "command": "asql-mcp",
      "args": ["-pgwire", "127.0.0.1:5433"]
    }
  }
}
```

Restart Claude Desktop. You'll see an `asql` server in the tools
popup.

### Claude Code (CLI)

Add an entry to your `~/.claude/mcp.json`:

```json
{
  "mcpServers": {
    "asql": {
      "command": "asql-mcp",
      "args": ["-pgwire", "127.0.0.1:5433"]
    }
  }
}
```

Or per-repo in `.mcp.json` at the project root.

### Cursor

Cursor → Settings → MCP → "+ Add new MCP server":

```json
{
  "mcpServers": {
    "asql": {
      "command": "asql-mcp",
      "args": ["-pgwire", "127.0.0.1:5433"]
    }
  }
}
```

### Zed

In `~/.config/zed/settings.json`:

```json
{
  "context_servers": {
    "asql": {
      "command": {
        "path": "asql-mcp",
        "args": ["-pgwire", "127.0.0.1:5433"]
      }
    }
  }
}
```

## Flags

```
-pgwire         asqld pgwire endpoint (default 127.0.0.1:5433)
-user           pgwire user (default asql)
-password       pgwire password (default empty)
-database       pgwire database (default asql)
-transport      stdio (default) or http
-http-addr      http transport bind address (default 127.0.0.1:6799)
-allow-writes   enable execute_sql tool (default: read-only)
-max-rows       row cap per query (default 1000)
-version        print version and exit
```

Every flag has a corresponding environment variable
(`ASQL_PGWIRE`, `ASQL_USER`, `ASQL_PASSWORD`, `ASQL_DATABASE`,
`ASQL_MCP_TRANSPORT`, `ASQL_MCP_HTTP_ADDR`) for agent config that
prefers `env` over `args`.

## HTTP (remote agents)

For remote or team-shared setups use streamable HTTP:

```bash
asql-mcp -transport http -http-addr :6799 -pgwire 10.0.0.5:5433
```

Then point any HTTP-speaking MCP client at `http://host:6799/`.

## Safety posture

- **Read-only by default.** All read tools refuse `INSERT`, `UPDATE`,
  `DELETE`, and any DDL through a conservative lexical check.
- **Single statement per call.** `query_sql` and `read_as_of_lsn`
  reject queries with interior semicolons to prevent chained
  statements.
- **Transaction boundaries are explicit.** `execute_sql` always
  wraps its statements in exactly one `BEGIN DOMAIN … COMMIT` block,
  and rolls back on any failure before `COMMIT`.
- **Row cap.** `max-rows` (default 1000) bounds the result set the
  agent sees, preventing runaway context usage.

## Why this is a differentiator for ASQL

MCP is becoming the default integration surface between AI agents
and developer tools (editors, shells, CI, docs). Every database
that expects to live inside an agent workflow in 2026 needs an MCP
story. `postgres-mcp` and friends exist — but they treat the DB as
generic SQL.

`asql-mcp` is the only MCP server that can answer **"what did this
table look like before commit N"** natively, without joining CDC
logs or reconstructing audit trails. For workflows where the model
is doing debugging, forensics, reconciliation, or compliance work,
that is the whole point.
