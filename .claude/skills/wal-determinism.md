---
name: wal-determinism
description: The determinism contract — rules every engine, executor, WAL, and fixture change must follow so replay stays byte-identical.
---

# wal-determinism

ASQL is a deterministic engine. Replaying the same WAL must produce the
same query-visible state, byte for byte. That guarantee is load-bearing
for time-travel reads, failover, recovery, tests, and incident
reproduction. Breaking it is a production-severity bug.

## The three things replay must not see differ

1. **Timestamps** — all observable timestamps must be the WAL commit
   timestamp or explicit SQL literals. Never `time.Now()`.
2. **Identifiers** — IDs must be explicit (provided by the caller) or
   WAL-derived. Volatile defaults (`UUID_V7`, `TX_TIMESTAMP`) are
   generated once at commit and materialized into the WAL payload, so
   replay reads them back. Never generate one during replay.
3. **Ordering** — anything observable must have a stable source of
   order. No map iteration order. No goroutine-race-dependent
   interleaving. No wall-clock-sorted results.

## Concrete rules

### R1 — clock always comes from `platform/clock`

Never call `time.Now()` in the engine, executor, parser, or WAL layer.
Use the injected clock. The test doubles (`clock.Fixed`, etc.) are the
only way tests can pin time.

Checking: `grep -rn 'time.Now()' internal/engine internal/storage/wal
internal/server/pgwire` should not return matches inside production code
— only inside test helpers that stub the clock.

### R2 — no randomness in the hot path

`math/rand` and `crypto/rand` may appear in networking code (jitter,
retry backoff) but must never influence **SQL-visible output** or WAL
contents.

### R3 — map iteration is unordered

If you iterate a map and emit results in that order, the results are
nondeterministic. Sort keys first, or use a slice.

```go
// WRONG
for key, val := range m {
    rows = append(rows, build(key, val))
}

// RIGHT
keys := make([]string, 0, len(m))
for k := range m {
    keys = append(keys, k)
}
sort.Strings(keys)
for _, k := range keys {
    rows = append(rows, build(k, m[k]))
}
```

### R4 — fixtures reject nondeterminism at validate time

Strict JSON scenarios. No `NOW()`, `CURRENT_TIMESTAMP`, `RANDOM()`,
runtime IDs, or transaction control inside a step. `asqlctl
fixture-validate` enforces this; keep it enforcing it.

### R5 — WAL is append-only

Never rewrite WAL bytes to "fix" something. If the state is wrong, the
replay-side code or the snapshot is what needs to adapt. Rewriting WAL
breaks recovery for every existing deployment.

### R6 — snapshots must be reproducible from WAL

If a snapshot captures something you can't regenerate deterministically
from the WAL, that's a leak. Treat the snapshot as a cache: an empty
`snap/` dir + the full WAL must yield the same serving state.

## How to verify determinism

Three layers of defense:

1. **Unit tests** use `clock.Fixed` + explicit IDs. If your code needs
   real time to pass a test, that's a smell.
2. **`test/determinism/`** runs a workload (multi- and cross-domain
   transactions, entities, VFKs, volatile defaults, schema changes,
   savepoints, rollbacks) and compares a fingerprint of everything
   query-visible across every way of rebuilding state: restart with and
   without snapshots, restarts and crashes mid-workload, repeated
   replays, follower apply in arbitrary batches, prefix replay vs time
   travel, WAL compaction, ledger on/off, and a transaction whose COMMIT
   never reached the WAL (it must never apply). Run it after any engine or
   WAL-layer change. The known gap it still excludes (row history) is
   listed in `harness_test.go`.
3. **`test/integration/`** exercises failover, restart, and snapshot
   replay end to end.

A change that passes unit tests but breaks determinism tests is common —
that's why the determinism layer exists.

## Common ways people accidentally break determinism

- Using `time.Now()` in a new helper because "it's just for logging" —
  fine for logs, **not fine** if the value flows into query results or
  WAL.
- Generating an ID with `uuid.New()` during replay instead of reading
  the value materialized in the WAL.
- Iterating `map[...]struct{...}` when building a result set.
- Writing a feature test that starts two goroutines and asserts on which
  committed first.
- Serializing JSON with `encoding/json` and assuming field order is
  stable. It is — but only for struct fields, not for `map[string]any`.
  Use `map` only when you don't need determinism on the wire.

## When you must introduce nondeterminism

Some things genuinely need real time or random: TCP retry backoff,
cluster jitter, cache eviction. **These must not be visible through
SQL, the WAL, or any replayable surface.** Isolate them behind the
transport or operational layer and add a comment explaining why.
