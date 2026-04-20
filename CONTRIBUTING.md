# Contributing to ASQL

Thanks for considering a contribution. ASQL is open-source under
[Apache 2.0](LICENSE) and welcomes bug reports, bug fixes, documentation
improvements, and well-scoped feature proposals.

## TL;DR — before your first PR

1. Read **[AGENTS.md](AGENTS.md)** at the repo root. It holds the
   durable contract: repo layout, determinism rules, pgwire protocol
   invariants, test tiers, and commit conventions.
2. Sign the [Contributor License Agreement](CLA.md). The CLA bot posts
   a comment on your first PR with a one-click link, or you can simply
   comment `I have read the CLA Document and I hereby sign the CLA`.
3. Follow the commit format described below.
4. Run the appropriate test tier before opening a PR.

## Ways to contribute

### Report a bug

Open an [issue](https://github.com/correodabid/asql/issues) with:

- What you expected to happen.
- What actually happened (include error messages and, if possible, the
  goroutine dump on hangs — Go's `go test -timeout` already prints one).
- A minimal reproduction: schema, seed data, the exact query, the
  commit LSNs involved if temporal state matters.
- Your environment: Go version (`go version`), OS, single-node vs.
  cluster, any non-default flags on `asqld`.

For **security issues**, do not open a public issue — see
[SECURITY.md](SECURITY.md).

### Propose a feature

Open an issue labelled `proposal` describing:

- The use case. *Who* hits this? *When?* What are they doing that makes
  the current surface inadequate?
- The smallest change that would unblock the use case.
- Whether it belongs in the engine, in pgwire, in a tool, or in an
  application layer on top of ASQL. See the "scope guidance" section
  of `.github/copilot-instructions.md` for where the boundary lives.

ASQL intentionally keeps a small surface. Proposals that would add
vertical-specific semantics (compliance objects, workflow engines,
approval flows, case management) belong in the application layer, not
the engine.

### Fix a bug or ship a small improvement

Open a PR directly. For anything larger than ~300 lines of
non-mechanical code, open an issue first to align on the shape.

## Development setup

```bash
# Prerequisites
go version  # 1.25.9 or newer

# Clone and build
git clone https://github.com/correodabid/asql
cd asql
go build ./...

# Run tests (start here)
go test ./internal/...

# Run the server locally
go run ./cmd/asqld -addr :5433 -data-dir .asql

# Open an interactive shell
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

## Test tiers

Run the cheapest suite that covers your change, escalate only when it
matters:

| Tier | Command | Typical time | Run when |
|---|---|---|---|
| Unit (default) | `go test ./internal/...` | ~10–30 s | any edit |
| pgwire | `go test ./internal/server/pgwire/...` | ~30–60 s | pgwire / engine protocol edits |
| Full | `go test -timeout 300s ./...` | 1–3 min | before committing |
| Integration | `go test ./test/integration/...` | minutes | cluster / replication / recovery edits |
| Determinism | `go test ./test/determinism/...` | minutes | engine / WAL / executor changes |

## Pre-commit checks (CI also runs these)

```bash
gofmt -w .
golangci-lint run
go test -timeout 300s ./...
# Only after a dep bump:
govulncheck ./...
```

## Commit conventions

```
<type>(<scope>): <imperative summary>

<why, not what — the diff shows what>

<optional Co-Authored-By trailer>
```

- **types**: `feat`, `fix`, `chore`, `docs`, `refactor`, `style`, `test`
- **scopes seen in history**: `pgwire`, `deps`, `ci`, `engine`, `wal`,
  `storage`, `mcp`
- Subject line under 72 chars, imperative ("add" not "added").
- Body explains *why* the change is correct and what invariants it
  preserves. The diff already shows what.

Examples from this repo:

```
fix(pgwire): guard adminListener/adminServer with a mutex
chore(deps): bump pgx to v5.9.0 and adapt pgwire to new protocol semantics
fix(parser,pgwire): recognise inline AS OF LSN / TIMESTAMP as SELECT clause
feat(mcp): add asql-mcp server — native Model Context Protocol surface for ASQL
```

## What NOT to do

- Don't introduce wall-clock time, randomness, or map-iteration-order
  dependencies in the engine, executor, or WAL layers. See AGENTS.md
  for the determinism contract.
- Don't `--amend` a pushed commit or `--force-push` without an explicit
  request from a maintainer.
- Don't skip pre-commit hooks (`--no-verify`).
- Don't commit `.asql/`, `.asql-*/`, `.tmp/`, or built binaries — they
  are gitignored but `git add .` can still catch them.
- Don't propagate pgx-specific types (`pgconn`, `pgproto3`, `pgtype`)
  into the engine. Keep them at the transport boundary.
- Don't add emoji to source files, commit messages, or documentation
  unless a maintainer asks for them.

## CLA — Contributor License Agreement

ASQL requires a one-time Contributor License Agreement before merging
contributions that are not trivial (typo fixes and test data are
exempt). The full text is in [CLA.md](CLA.md) — it follows the Apache
Software Foundation individual CLA model closely.

The automated CLA assistant workflow will post a comment on your first
PR with a one-click signing link. You only need to sign once; future
PRs from the same GitHub account require no action.

## Code of Conduct

Participation in the ASQL project is governed by the
[Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By
contributing you agree to abide by its terms.

## Licensing of your contribution

Contributions are accepted under the Apache License, Version 2.0 (the
Project's current license). The CLA additionally authorises the Project
to distribute your contribution under future license combinations
consistent with keeping a version of the Project openly available. See
CLA.md Section 5 for details.

## Release cadence

ASQL does not yet have a formal release schedule. Follow the repository
and watch the `CHANGELOG` (when it lands) for notable changes.

## Getting help

- Questions, discussion, and casual design feedback: open a
  [Discussion](https://github.com/correodabid/asql/discussions) (if
  enabled) or an issue labelled `question`.
- For AI-agent workflows inside this repo, see
  `.claude/skills/` and `AGENTS.md`.
- For adoption questions (Postgres compatibility, workload fit,
  integration patterns), see `docs/getting-started/` and
  `docs/reference/`.
