# ASQL Performance Benchmark Plan (v1)

Date: 2026-03-10
Audience: engine, storage, cluster, and product engineering.

## Goal

Create a benchmark ladder that separates:

1. core engine cost,
2. SQL path cost,
3. transport cost,
4. cluster/replication cost,
5. real scenario load cost.

This avoids comparing a cluster seed script directly against embedded or single-node database marketing numbers.

## Benchmark layers

### L0 — Core microbenchmarks

Purpose: detect regressions in engine and WAL internals without transport noise.

Commands:

- `make bench-core`
- `make bench-write`
- `make bench-seed-single BENCH_WORKERS=8 BENCH_STEPS=10 BENCH_MATERIALS=5`
- `make bench-matrix-single`
- `make bench-matrix-cluster`

Coverage:

- `BenchmarkEngineWriteCommit`
- `BenchmarkEngineWriteCommitPreSeeded`
- `BenchmarkEngineWriteCommitPreSeededBTree`
- `BenchmarkEngineWriteCommitBulk10`
- `BenchmarkEngineWriteCommitReturningUUID`
- `BenchmarkEngineWriteCommitBulk10ReturningUUID`
- `BenchmarkEngineWriteCommitConcurrent`
- WAL append/read/recover benchmarks

Primary metrics:

- `ns/op`
- `B/op`
- `allocs/op`
- write scaling versus seeded table size

Optimization hypotheses this layer can validate:

- PK/unique/index maintenance cost
- `INSERT ... RETURNING` overhead
- UUID/default resolution overhead
- commit queue/group commit effectiveness
- WAL sync policy impact

### L1 — SQL scenario benchmarks

Purpose: measure realistic table shapes without cluster replication.

Recommended scenarios:

- single table insert-only
- parent/child inserts with `RETURNING id`
- pre-indexed insert workload
- bulk 10 / bulk 100 per commit
- schema creation separated from load phase

Recommended outputs:

- rows/sec
- commits/sec
- p50/p95/p99 per transaction
- retries/conflicts

### L2 — pgwire transport benchmarks

Purpose: isolate client/server round-trip overhead.

Command:

- `make bench-pgwire`

Recommended scenarios:

- single-node pgwire insert+commit
- pgwire `INSERT ... RETURNING`
- single transaction with N statements
- single transaction with N inserts and one final select

Recommended measurement method:

- use a dedicated Go benchmark/runner against `cmd/asqld`
- compare engine-direct versus pgwire numbers on the same hardware

Primary optimization targets:

- statement round-trips
- `RETURNING` path overhead
- connection lifecycle cost
- server parsing/planning overhead under repeated statements

### L3 — Cluster / replication benchmarks

Purpose: measure the cost of quorum, catch-up, and failover safety.

Recommended scenarios:

- 3-node cluster, leader-only writes
- 3-node cluster with follower catch-up active
- repeated writes under induced leader change
- write workload with periodic node restarts

Primary metrics:

- rows/sec
- commits/sec
- retry count
- catch-up lag
- leader commit latency
- follower apply latency

Primary optimization targets:

- Raft commit path
- follower catch-up cost
- replay amplification
- commit batching across concurrent clients

Latest validated reference (2026-03-10):

- sustained `wide-tx` cluster run (`1000 tx`, `4 workers`): `587 tx/sec`, `18,201 rows/sec`, `P50 6ms`, `P95 10ms`, `P99 15ms`
- short cluster matrix (`300 tx`):
	- `baseline`: `722 tx/sec`
	- `wide-tx`: `392 tx/sec`
	- `no-indexes`: `941 tx/sec`
	- `high-workers`: `917 tx/sec`

Recent validated wins behind this step-change:

- real protobuf for internal Raft RPCs;
- suppression of heartbeat/commit replication overlap;
- commit replication batch cap;
- preserving late follower success updates;
- compact-range overlap matching on followers;
- coalesced follower `CatchUp()` replay so only one replay worker runs per node at a time.

### L4 — End-to-end scenario benchmarks

Purpose: measure product workflows that are meaningful for demos and evaluations.

Commands:

- `make bench-seed-single`
- `make bench-seed-cluster`
- `make bench-matrix-single`
- `make bench-matrix-cluster`

Implementation note:

- These targets should use a dedicated benchmark workload script, not `seed_domains`, so benchmark evolution does not distort product/demo seeding behavior.

Interpretation rule:

These are scenario benchmarks, not core engine benchmarks.

The current `seed_domains` workload includes:

- schema creation,
- index creation,
- many `INSERT ... RETURNING` operations,
- client-side orchestration,
- cluster redirects/retries when running against a cluster.

So it should be used for regression tracking and workflow optimization, not headline database comparison.

Parameterization note:

- `BENCH_TX` controls transaction count.
- `BENCH_WORKERS` controls client concurrency.
- `BENCH_STEPS` and `BENCH_MATERIALS` control rows per transaction.
- `BENCH_INDEXES=true|false` toggles secondary indexes during the load phase.
- The matrix targets reuse the prepared environment and run a baseline, a wider transaction shape, a no-index variant, and a higher-concurrency variant.

## Benchmark matrix

| Layer | Path | Top metric | Purpose |
|---|---|---:|---|
| L0 | engine direct | ns/op | regressions in core write/read/replay |
| L1 | SQL scenario | rows/sec | realistic single-node data path |
| L2 | pgwire | tx/sec, p95 | transport and statement overhead |
| L3 | cluster | tx/sec, retries, lag | quorum and replay overhead |
| L4 | seed workflow | rows/sec | end-to-end operator workflow |

## Immediate optimization priorities

### P1 — Reduce statement round-trips in seed and bulk write workloads

Observed issue:

- `execSQLBatch()` still executes one statement at a time in a loop.

Relevant code:

- [scripts/seed_domains/main.go](scripts/seed_domains/main.go)

Likely wins:

- multi-row `INSERT`
- fewer `RETURNING` calls
- hierarchical insert primitives where possible

### P2 — Benchmark and reduce `INSERT ... RETURNING` overhead

Observed issue:

- seed workload depends heavily on server-generated IDs.

Relevant paths:

- eager default resolution before `RETURNING`
- UUID v7 generation
- replay/catch-up interaction already fixed

Likely wins:

- batch parent creation when IDs can be client-generated deterministically
- reduce per-row planning/returning overhead

### P3 — Separate schema/index build from load benchmark

Observed issue:

- current seed throughput includes schema and index creation.

Likely wins:

- cleaner comparisons
- better regression attribution

### P4 — Keep cluster tax explicitly measured

Observed issue:

- cluster mode adds retry, quorum, and catch-up overhead.

Updated note:

- the largest recent cluster gain came from follower catch-up coalescing, not
	transport serialization or leader fanout changes. Keep follower replay cost
	visible in future regressions.

Likely wins:

- direct visibility into single-node vs cluster delta
- better prioritization between engine and replication work

### P5 — Add latency histograms to benchmark runners

Primary outputs to collect for scenario runners:

- p50/p95/p99 transaction latency
- rows/sec
- retries/conflicts
- leader/follower lag

## Success criteria

The benchmark program is useful when it can answer these questions quickly:

1. Is the slowdown in engine, pgwire, or cluster?
2. How much does `RETURNING` cost?
3. How much does each extra index cost on writes?
4. What is the single-node baseline versus 3-node baseline?
5. Did a code change improve throughput without violating determinism?

## Recommended execution order

1. Stabilize L0 and L1 measurements in CI/manual repeat runs.
2. Add L2 pgwire benchmark runner.
3. Add L3 cluster benchmark runner.
4. Keep L4 scenario benchmark for workflow realism.
