# CLAUDE.md — ASQL contributor guide for Claude Code

This file is the durable context for any Claude Code session working on this
repository. Read it before you touch code.

## What ASQL is (in one paragraph)

ASQL is a deterministic SQL engine written in Go. The WAL is the source of
truth; materialized state is derived. Every committed transaction gets a
monotonic LSN, history is replay-safe, and the primary application-facing
surface is pgwire (PostgreSQL wire protocol). The engine exposes domain
isolation, cross-domain transactions, entity versioning with versioned
foreign keys, and time-travel reads (`AS OF LSN`, `FOR HISTORY`). It is
**not** a drop-in PostgreSQL replacement.

## Repo layout (only what matters for contributors)

```
cmd/
  asqld/                 Server binary
  asqlctl/               CLI (shell, fixtures, recovery ops)

internal/
  engine/
    executor/            Query execution, transactions, snapshots
    parser/              SQL parser + AST
    domains/             Domain catalog
    ports/               Interface definitions (hexagonal)
  storage/
    wal/                 Write-ahead log (segmented, append-only)
  cluster/
    coordinator/         Leadership & coordination
    replication/         WAL streaming / catch-up
    heartbeat/           Node health monitoring
  server/
    grpc/                Admin/gRPC transport
    pgwire/              PostgreSQL wire protocol (main application surface)
  platform/
    clock/               Deterministic time abstraction

pkg/
  adminapi/              Public admin API types
  servertest/            Test helpers

sdk/                     Typed Go client over the gRPC admin surface
api/proto/               Protobuf definitions
site/                    Static docs site

docs/
  adr/                   Architecture Decision Records (durable rationale)
  architecture/          Architecture one-pager
  getting-started/       Primary user-facing onboarding path
  maintenance/           Internal maintainer checklists
  migration/             Migration guides (SQLite/Postgres-lite → ASQL)
  operations/            Runbooks, SLOs, incident/security policies
  reference/             SQL/pgwire/fixture reference material

test/
  integration/           Cross-package end-to-end tests (failover, replay, ...)
  determinism/           Replay/determinism tests
```

## Hard rules — the determinism contract

These are non-negotiable. Break them and CI fails (or worse, a silent
divergence ships).

1. **The WAL is truth.** Anything that must survive replay goes through the
   engine, not around it. Never bypass WAL append for "just a test".
2. **No wall-clock, no randomness, no process-derived values** in anything
   the engine processes. Timestamps must come through the
   `platform/clock` abstraction; IDs must be explicit or WAL-derived
   (`UUID_V7` is deterministic via the clock).
3. **Parser/executor are pure functions of their input.** Two replays of
   the same WAL must produce identical query-visible state.
4. **Fixture files must be strict JSON scenarios** — no `NOW()`,
   `CURRENT_TIMESTAMP`, `RANDOM()`, or runtime-derived values. `asqlctl
   fixture-validate` enforces this.

When you're unsure whether something is safe: assume it must replay byte-
identically across restarts.

## pgwire — what to know before touching `internal/server/pgwire`

- **Extended query protocol**: Parse → Bind → Describe → Execute → Sync.
  pgx (the reference client) pipelines these.
- **pgx v5.9+ skips `Describe Portal`** for cached statements and uses the
  statement description's `Fields` + `ResultFormatCodes` from `Bind`.
  If you add a streaming query that blocks before sending anything, send
  `RowDescription` eagerly — otherwise `readUntilRowDescription` on the
  client deadlocks. (This is the exact bug that bit
  `streamExtendedTailEntityChanges`.)
- **`ResultFormatCodes` matters.** When the client requests binary (`1`)
  for a column, serialize via `literalToBinary(lit, oid)` — don't ship
  text.
- **OID contract for common types**: int8=20, int4=23, int2=21, bool=16,
  text=25, json=114, timestamp=1114, timestamptz=1184.
- **Timestamp binary is PG-epoch microseconds** (2000-01-01 UTC), not
  Unix epoch. Offset constant: `946_684_800_000_000`.
- **Cancel flow**: pgx opens a *separate* TCP connection to send
  `CancelRequest`. The server must look up the session by `(processID,
  secretKey)` and call `currentCancel()` on its `beginQuery` context.
  `SecretKey` in pgx v5.9+ is `[]byte`, not `uint32`. See
  `uint32ToSecretKey` / `secretKeyToUint32` helpers in `server.go`.

## Tests — cost tiers

| Package | Cost | When |
|---|---|---|
| `internal/...` (unit) | fast (~10s total) | on every edit |
| `internal/server/pgwire/...` | medium (~30s) | on any pgwire/engine edit |
| `test/integration/...` | heavy (minutes) | before opening a PR that touches cluster/replication/recovery |
| `test/determinism/...` | heavy (minutes) | before any engine/WAL/executor change |

Default command during work: `go test ./internal/...`. Full suite:
`go test -timeout 300s ./...`. Use `-run TestName` to isolate a hanging
test; the test harness dumps all goroutines on timeout, which is usually
enough to diagnose protocol deadlocks.

## Common commands

```bash
# Run server locally
go run ./cmd/asqld -addr :5433 -data-dir .asql

# Interactive shell against the server
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433

# Full build (all packages)
go build ./...

# Lint
golangci-lint run

# Vulnerability scan (run after any dep bump)
govulncheck ./...

# Format everything (CI enforces this)
gofmt -w .
```

## Commit & PR conventions

- **Subject**: `<type>(<scope>): <imperative summary>`
  - types: `feat`, `fix`, `chore`, `docs`, `refactor`, `style`, `test`
  - scopes seen in the log: `pgwire`, `deps`, `ci`, `engine`, `wal`, etc.
- Keep the first line under 72 chars. Use the body for the "why".
- Co-author trailer when Claude contributed:
  `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`
- **Don't skip hooks**. `--no-verify` is never appropriate here.
- Prefer a new commit over amending once it's pushed.

## Module path

`github.com/correodabid/asql` — remember this when writing imports.

## Where to go deeper

- Protocol/pgwire work → `.claude/skills/pgwire-feature.md`
- Running tests efficiently → `.claude/skills/run-tests.md`
- Determinism rules in practice → `.claude/skills/wal-determinism.md`
- Getting a change shipped → `.claude/skills/commit-and-pr.md`
- Dep bumps & CVE hygiene → `.claude/skills/govulncheck.md`

ADRs under `docs/adr/` hold the durable rationale for engine-surface
decisions. When a design question comes up, look there before inventing
a new direction.
