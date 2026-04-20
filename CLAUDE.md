# CLAUDE.md — extensions for Claude Code

**Read `AGENTS.md` first.** It holds the durable contract: determinism
rules, repo layout, pgwire protocol gotchas, test tiers, commit
conventions, and what-not-to-do.

This file only adds the Claude-Code-specific bits on top.

## Skills available in this repo

Skills live under `.claude/skills/`. Invoke with `/<name>` in a Claude
Code session:

| Skill | Use when |
|---|---|
| `/run-tests` | Running tests efficiently, isolating failures, diagnosing hangs |
| `/pgwire-feature` | Adding or modifying pgwire-visible behavior (the pgx v5.9 trap checklist lives here) |
| `/wal-determinism` | Anything in engine, executor, or WAL — the 6 hard rules in practice |
| `/commit-and-pr` | Format, trailers, safety rules, pre-commit checks |
| `/govulncheck` | Dep bumps and CVE resolution |

Each skill is a self-contained markdown file — no external state, no
side effects.

## Working patterns for this codebase

**Before editing `internal/server/pgwire/`**: run the pgwire tests to
confirm they're green (`go test ./internal/server/pgwire/... -timeout
60s`). Many bugs there surface only under specific pgx client
behavior, and you want a known-good baseline before your change.

**Before editing `internal/engine/executor/` or `internal/storage/wal/`**:
run the determinism suite (`go test ./test/determinism/... -timeout
300s`). A pass there is your replay-safety guarantee.

**Tail logs during a local run**: `go run ./cmd/asqld` logs structured
JSON to stdout. Pipe through `| jq` if readable logs matter. `.asql/`
is the default data dir and is gitignored.

**Reading the WAL for debugging**: there is no "WAL dump" command yet.
Use the internal `executor.EntityChanges` or the `asqlctl recovery`
subcommands for structured views.

**Protocol deadlocks in pgwire**: if a test hangs in `pgproto3.
(*chunkReader).Next` on both client and server goroutines, you have a
protocol deadlock. Look at `/pgwire-feature` for the common culprits,
especially the pgx v5.9 `Describe Portal` skip.

## MCP integration

ASQL ships an MCP server (`cmd/asql-mcp`) that exposes ASQL's unique
primitives to any MCP-capable agent — including Claude Code itself.
See `cmd/asql-mcp/README.md` for configuring Claude Desktop / Cursor /
Claude Code to use it locally.

## Anything else

Everything structural — rules, layout, commands, conventions — lives in
`AGENTS.md`. When in doubt about a decision, that's the canonical
source.
