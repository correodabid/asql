# AGENTS.md — ASQL contributor guide for AI agents

This is the canonical guide for any AI agent (Claude Code, Cursor, Windsurf,
Aider, Copilot, Codex, or anything that reads `AGENTS.md`) working on this
repository. Read it before you touch code.

Agent-specific extensions live next to this file:

- `CLAUDE.md` — additions for Claude Code (skills, slash commands)
- `.github/copilot-instructions.md` — deeper product context for Copilot
- `.cursor/rules/asql.mdc` — Cursor-specific rule notes

All of them defer to this file for the durable contract.

## What ASQL is (one paragraph)

ASQL is a deterministic SQL engine written in Go. The WAL is the source of
truth; materialized state is derived. Every committed transaction gets a
monotonic LSN, history is replay-safe, and the primary application-facing
surface is pgwire (PostgreSQL wire protocol). The engine exposes domain
isolation, cross-domain transactions, entity versioning with versioned
foreign keys, and time-travel reads (`AS OF LSN`, `FOR HISTORY`). It is
**not** a drop-in PostgreSQL replacement — it is a deterministic engine
that happens to speak a pragmatic PostgreSQL-compatible subset.

## Module path

`github.com/correodabid/asql` — use this in all imports.

## Repo layout

```
cmd/
  asqld/              Server binary
  asqlctl/            CLI (shell, fixtures, recovery ops)
  asql-mcp/           MCP server binary (exposes ASQL to AI agents)

internal/
  engine/
    executor/         Query execution, transactions, snapshots
    parser/           SQL parser + AST
    domains/          Domain catalog
    ports/            Interface definitions (hexagonal)
  storage/
    wal/              Write-ahead log (segmented, append-only)
  cluster/
    coordinator/      Leadership & coordination
    replication/      WAL streaming / catch-up
    heartbeat/        Node health monitoring
  server/
    grpc/             Admin / gRPC transport
    pgwire/           PostgreSQL wire protocol (primary surface)
  platform/
    clock/            Deterministic time abstraction

pkg/
  adminapi/           Public admin API types
  servertest/         Test helpers

sdk/                  Typed Go client over the gRPC admin surface
api/proto/            Protobuf definitions
site/                 Static docs site

docs/
  adr/                Architecture Decision Records
  architecture/       Architecture one-pager
  getting-started/    User-facing onboarding path
  maintenance/        Internal maintainer checklists
  migration/          Migration guides
  operations/         Runbooks, SLOs, incident & security policies
  reference/          SQL/pgwire/fixture reference material

test/
  integration/        Cross-package end-to-end tests
  determinism/        Replay/determinism tests
```

## Non-negotiable rules — the determinism contract

These are load-bearing for time-travel, replay, failover, and recovery.
Breaking them is a production-severity bug, not a stylistic choice.

1. **The WAL is truth.** Anything that must survive replay goes through the
   engine, not around it. Never bypass WAL append.
2. **No wall-clock, no randomness, no process-derived values** in anything
   the engine observes. Timestamps must come through the
   `platform/clock` abstraction; IDs must be explicit or WAL-derived
   (`UUID_V7` is deterministic via the injected clock).
3. **Parser and executor are pure functions of their input.** Two replays
   of the same WAL must produce byte-identical state and query-visible
   results.
4. **No map iteration order dependencies.** Sort keys or use ordered
   containers when iteration order is observable.
5. **Fixture files are strict JSON scenarios** — no `NOW()`,
   `CURRENT_TIMESTAMP`, `RANDOM()`, runtime-derived IDs, or transaction
   control inside steps. `asqlctl fixture-validate` enforces this.
6. **Snapshots must be reproducible from WAL.** Treat the snapshot as a
   cache — an empty `snap/` dir + the full WAL must yield identical
   serving state.

When unsure: assume the change must replay byte-identically across
restarts, failovers, and fresh-checkout rebuilds.

## pgwire — essential knowledge for any protocol edit

- **Extended query protocol**: Parse → Bind → Describe → Execute → Sync.
  pgx pipelines these.
- **pgx v5.9+ skips `Describe Portal`** for cached statements and uses the
  statement description's `Fields` + `ResultFormatCodes` from `Bind`.
  If a streaming `Execute` path blocks before sending anything, send
  `RowDescription` eagerly — otherwise `readUntilRowDescription` on the
  client deadlocks. See `streamExtendedTailEntityChanges` for the
  reference fix pattern.
- **`ResultFormatCodes` matters.** When the client requests binary (`1`)
  for a column, serialize via `literalToBinary(lit, oid)`, not text.
- **Common OIDs**: int8=20, int4=23, int2=21, bool=16, text=25, json=114,
  timestamp=1114, timestamptz=1184.
- **Timestamp binary format** is PG-epoch microseconds (since 2000-01-01
  UTC), not Unix epoch. Offset: `946_684_800_000_000`.
- **Cancel flow**: pgx opens a separate TCP connection to send
  `CancelRequest`. The server looks up the session by
  `(processID, secretKey)` and calls `currentCancel()` on its
  `beginQuery` context. `SecretKey` in pgx v5.9+ is `[]byte`, not
  `uint32` — use `uint32ToSecretKey` / `secretKeyToUint32`.

## Test cost tiers — know which one your change needs

| Package | Cost | Run when |
|---|---|---|
| `./internal/...` | fast (~10–30 s) | any edit |
| `./internal/server/pgwire/...` | medium (~30–60 s) | pgwire / engine protocol edits |
| `./...` | 1–3 min | before committing |
| `./test/integration/...` | minutes | cluster / replication / recovery edits |
| `./test/determinism/...` | minutes | engine / WAL / executor changes |

**Default loop**: `go test ./internal/...`. Full suite:
`go test -timeout 300s ./...`. Isolate a hanging test with `-run Name
-timeout 30s -v`.

When a test hangs, Go's test framework dumps all goroutines on timeout —
that is usually enough to diagnose the issue. Common pattern: protocol
deadlock where both client and server are in `(*chunkReader).Next` IO
wait. See pgwire notes above.

## Common commands

```bash
# Run server locally
go run ./cmd/asqld -addr :5433 -data-dir .asql

# Interactive shell
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433

# Full build
go build ./...

# Lint (CI enforces this)
golangci-lint run

# Format (CI enforces this)
gofmt -w .

# Vulnerability scan — run after any dep bump
govulncheck ./...

# MCP server (stdio transport, for AI agent clients)
go run ./cmd/asql-mcp -pgwire 127.0.0.1:5433
```

## Commit and PR conventions

- **Subject**: `<type>(<scope>): <imperative summary>`
  - types: `feat`, `fix`, `chore`, `docs`, `refactor`, `style`, `test`
  - scopes from recent history: `pgwire`, `deps`, `ci`, `engine`, `wal`,
    `storage`
- First line under 72 chars; body explains the **why**.
- Trailer when an agent contributed:
  `Co-Authored-By: <Agent name> <noreply@anthropic.com>` (or appropriate
  email).
- **Do not** `--amend` once pushed. Create a new commit.
- **Do not** `--force-push` unless the human explicitly asked.
- **Do not** skip hooks (`--no-verify`) unless the human explicitly asked.
- Prefer a new commit over amending.

## What not to do

- Don't add wall-clock time or randomness to the engine, executor, or
  WAL layer. Use `platform/clock`.
- Don't invent a side channel around the WAL for "performance" reasons.
- Don't propagate pgx-specific types (`pgconn`, `pgproto3`, `pgtype`)
  into the engine. Keep them at the transport boundary.
- Don't add emoji to source files or commit messages unless the human
  explicitly asked.
- Don't create markdown documentation files proactively — only when
  explicitly requested or genuinely needed to unblock a change.
- Don't commit `.asql/`, `.asql-*/`, `.tmp/`, built binaries, or
  anything the `.gitignore` already protects.
- Don't assume pgx sent `Describe Portal`. It usually did not (pgx v5.9+).

## Deeper references

- `docs/adr/` — durable rationale for engine-surface decisions.
- `docs/reference/postgres-compatibility-surface-v1.md` — what works
  over pgwire and what does not.
- `docs/architecture/architecture-one-pager-v1.md` — system layout.
- `docs/operations/runbook.md` — running the server and the cluster.

Agent-specific files:

- `CLAUDE.md` — Claude Code extensions (skills, slash commands).
- `.github/copilot-instructions.md` — extended product context.
- `.cursor/rules/asql.mdc` — Cursor editor rules.
