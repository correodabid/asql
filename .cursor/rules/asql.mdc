---
description: ASQL repo — deterministic SQL engine in Go. Read AGENTS.md for the full contract.
alwaysApply: true
---

# ASQL — Cursor rules

**Read `AGENTS.md` at the repo root first.** It holds the durable
contract: layout, determinism rules, pgwire protocol gotchas, test
tiers, commit conventions, and what-not-to-do.

This file is a short Cursor-facing summary that points at the canonical
source.

## Three things that must stay true

1. **WAL is the source of truth.** Replay the same WAL, get byte-identical
   state. No wall-clock time, no `math/rand`, no map iteration order
   dependencies in the engine, executor, or storage layers. Use
   `internal/platform/clock`.

2. **pgwire is the primary surface.** The extended query protocol must
   stay compatible with pgx v5.9+. If you add a streaming Execute path,
   send `RowDescription` eagerly — the client's `readUntilRowDescription`
   will otherwise deadlock. See `AGENTS.md` for the full OID/binary
   cheat sheet.

3. **Module path is `github.com/correodabid/asql`.** Not `asql`, not
   anything else.

## Default test loop

```
go test ./internal/...
```

Full suite before committing:

```
go test -timeout 300s ./...
```

## Commit format

```
<type>(<scope>): <imperative summary>

<why, not what>

Co-Authored-By: <Agent> <noreply@anthropic.com>
```

Types: `feat`, `fix`, `chore`, `docs`, `refactor`, `style`, `test`.
Recent scopes: `pgwire`, `deps`, `ci`, `engine`, `wal`, `storage`.

## Don't

- Don't emit emoji into source files, commit messages, or documentation.
- Don't amend pushed commits or force-push without an explicit request.
- Don't skip pre-commit hooks.
- Don't add markdown files that duplicate `AGENTS.md`.
- Don't propagate `pgx` / `pgproto3` types into the engine. Keep them at
  the transport boundary.

Full detail lives in `AGENTS.md` and the `.claude/skills/` directory.
