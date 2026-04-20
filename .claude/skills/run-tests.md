---
name: run-tests
description: Run ASQL tests efficiently — knowing which tiers are cheap vs expensive, how to isolate a single test, and how to diagnose hangs.
---

# run-tests

Use this when you need to verify a change. The goal is to run the cheapest
suite that covers your edit, escalate only when it matters.

## Cost tiers

| Tier | Command | Typical time | Run when |
|---|---|---|---|
| Unit (default) | `go test ./internal/...` | ~10–30 s | any edit |
| pgwire | `go test ./internal/server/pgwire/...` | ~30–60 s | pgwire / engine protocol edits |
| Full | `go test -timeout 300s ./...` | 1–3 min | before committing |
| Integration | `go test ./test/integration/...` | minutes | cluster / replication / recovery edits |
| Determinism | `go test ./test/determinism/...` | minutes | engine / WAL / executor edits |

**Default loop while iterating: `go test ./internal/...`** is almost always
the right tradeoff. Save the full suite for the last verification before
a commit.

## Isolate a single test

```bash
go test -v -run TestSpecificName -timeout 30s ./path/to/package/...
```

- `-v`: shows test names as they run (useful when a test hangs — the last
  printed name is the one that stuck).
- `-timeout 30s`: keeps you from waiting 10 minutes when something hangs.
  Raise to `300s` only when you expect genuine long work.
- `-run` takes a regex. To run multiple: `-run 'TestA|TestB'`.

## When a test hangs

Go's test framework dumps **all goroutines** on timeout. That dump is almost
always enough to diagnose the problem.

What to look for:

1. **Both client and server in `pgproto3.(*chunkReader).Next` (IO wait)** →
   protocol deadlock. One side isn't sending what the other expects.
   Common culprit on this codebase: pgx v5.9+ `ExecStatement` skips
   `Describe Portal` and `readUntilRowDescription` blocks until the
   server sends `RowDescription`/`DataRow`/`CommandComplete`. Fix is to
   send `RowDescription` eagerly before any long wait.

2. **Server goroutine in `waitForTailEntityChangesWake` / `sync.Cond.Wait`**
   → the server is waiting for a signal that must come from the test
   goroutine. If the test goroutine is also blocked in `Query()`, that's a
   deadlock. Check the producer goroutine is actually running.

3. **Test goroutine in `pgx.(*Conn).Query`** → `Query()` hasn't returned
   yet. In pgx v5.9+, `Query()` only returns after the first server
   response. Don't design flows that require work *after* `Query()`
   returns but *before* the server sends anything.

## Coverage

```bash
go test -cover ./internal/...
# Per-file detail:
go test -coverprofile=/tmp/cover.out ./internal/server/pgwire/...
go tool cover -html=/tmp/cover.out
```

## Flakiness

Determinism tests should **never** be flaky. If one is:

- First check whether it imports real time or `math/rand` directly.
- Then check whether it bypasses the engine's WAL path.
- Finally check for accidental map iteration order dependencies.

A flake here is a bug, not a retry candidate.

## Verification before commit

```bash
# Format
gofmt -w .

# Lint (CI will run this — don't let it find things you could have)
golangci-lint run

# Full test suite
go test -timeout 300s ./...
```

If any of these fail locally, they will fail in CI.
