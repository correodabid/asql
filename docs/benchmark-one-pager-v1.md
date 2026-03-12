# ASQL Benchmark One-Pager (v1)

Date: 2026-03-01
Audience: engineering, architecture, and procurement evaluation.

## 1) What this benchmark shows

This benchmark provides a reproducible baseline for ASQL core paths:
- write + commit lifecycle,
- historical reads (`AS OF LSN`),
- replay execution,
- WAL append/read/recover performance.

It is intended for regression tracking and comparative technical due diligence, not as a universal SLA.

## 2) Test environment

- OS: macOS (darwin/arm64)
- CPU: Apple M1
- Runtime: Go toolchain from project `go.mod`

Command:

```bash
make bench
```

Source of record:
- `docs/ai/09-benchmark-baseline.md`

## 3) Baseline results (2026-02-28)

### Engine path (`internal/engine/executor`)

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkEngineWriteCommit-8` | 69,430 | 4,248 | 62 |
| `BenchmarkEngineReadAsOfLSN-8` | 5,038,622 | 3,178,457 | 39,126 |
| `BenchmarkEngineReplayToLSN-8` | 4,794,214 | 2,631,836 | 37,097 |

### WAL path (`internal/storage/wal`)

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkFileLogStoreAppend-8` | 24,561 | 573 | 6 |
| `BenchmarkFileLogStoreReadFrom-8` | 15,634,191 | 5,593,017 | 60,029 |
| `BenchmarkFileLogStoreRecover-8` | 13,377,896 | 5,592,932 | 60,028 |

## 4) Cluster scenario reference (2026-03-10)

Purpose: capture the current 3-node quorum write baseline after the Raft transport,
late-ack follower progress, overlap-scan, and follower catch-up coalescing fixes.

Commands:

```bash
make bench-seed-cluster BENCH_SCENARIO=wide-tx BENCH_TX=1000 BENCH_WORKERS=4 BENCH_STEPS=20 BENCH_MATERIALS=10 BENCH_INDEXES=true
make bench-seed-cluster BENCH_SCENARIO=baseline BENCH_TX=300 BENCH_WORKERS=4 BENCH_STEPS=5 BENCH_MATERIALS=3 BENCH_INDEXES=true
make bench-seed-cluster BENCH_SCENARIO=wide-tx BENCH_TX=300 BENCH_WORKERS=4 BENCH_STEPS=20 BENCH_MATERIALS=10 BENCH_INDEXES=true
make bench-seed-cluster BENCH_SCENARIO=no-indexes BENCH_TX=300 BENCH_WORKERS=4 BENCH_STEPS=5 BENCH_MATERIALS=3 BENCH_INDEXES=false
make bench-seed-cluster BENCH_SCENARIO=high-workers BENCH_TX=300 BENCH_WORKERS=8 BENCH_STEPS=5 BENCH_MATERIALS=3 BENCH_INDEXES=true
```

### Sustained cluster write workload

| Scenario | tx/sec | rows/sec | P50 | P95 | P99 |
|---|---:|---:|---:|---:|---:|
| `wide-tx` (`1000 tx`, `4 workers`) | 587 | 18,201 | 6ms | 10ms | 15ms |

### Short cluster matrix

| Scenario | tx/sec | rows/sec | P50 | P95 | P99 |
|---|---:|---:|---:|---:|---:|
| `baseline` | 722 | 6,496 | 5ms | 10ms | 13ms |
| `wide-tx` | 392 | 12,151 | 9ms | 19ms | 24ms |
| `no-indexes` | 941 | 8,469 | 4ms | 8ms | 10ms |
| `high-workers` | 917 | 8,252 | 8ms | 16ms | 22ms |

Key interpretation:

- follower catch-up coalescing removed a large amount of redundant replay work,
	which was previously dominating quorum write latency;
- the current cluster write path is now limited more by real append/apply work
	than by duplicate follower catch-up or repeated overlap scans.

## 5) Determinism and interpretation guidance

- Workload shape and command path are deterministic.
- Absolute timings are hardware/OS dependent.
- Use this sheet for trend and release-over-release comparison.
- For vendor comparison, run same workload profile on same hardware and publish scripts/configs.

## 6) Recommended evaluation rubric

When validating ASQL in a pilot, track:
- p95/p99 latency for domain writes and historical reads,
- replay throughput and replay equivalence pass rate,
- crash-recovery success rate,
- replication catch-up time under controlled lag.

## 7) Commercial claim boundaries

ASQL performance claims should:
- cite benchmark method and environment,
- distinguish baseline from customer production SLOs,
- avoid extrapolation across different hardware profiles without new evidence.

## 8) Seed degradation monitor (repeat-run guardrail)

To monitor long-run degradation specifically on repeated `seed_domains` runs,
use the dedicated benchmark runner:

```bash
make bench-seed-degradation
```

Environment-assisted variants:

```bash
make bench-seed-degradation-single   # levanta entorno single-node y ejecuta benchmark
make bench-seed-degradation-cluster  # levanta entorno 3 nodos y ejecuta benchmark
```

Configurable knobs:

```bash
make bench-seed-degradation \
	BENCH_DEG_ROUNDS=10 \
	BENCH_DEG_WARMUP=1 \
	BENCH_DEG_FAIL_RATIO=2.00 \
	BENCH_DEG_CMD="go run ./scripts/seed_domains -workers 4"
```

What it reports:
- best/worst/avg round duration,
- P50/P95 round duration,
- worst-to-best ratio (primary degradation signal),
- per-round raw logs in `.bench/seed-degradation/`.

Current `seed_domains` default behavior resets the target domains before each
load. This keeps the seeded dataset size stable across repeated runs, which is
the right default for onboarding/demo seeding and for regression monitoring.

If you explicitly want to benchmark cumulative growth instead, disable it:

```bash
go run ./scripts/seed_domains -reset-domains=false
```

CI behavior:
- exits non-zero when `worst/best > BENCH_DEG_FAIL_RATIO`,
- making it suitable as a regression gate for write-path fixes.

Latest validated cluster reference after follower direct-delta apply and idle WAL compaction scheduling:
- best considered round: `1.524s`
- worst considered round: `3.419s`
- worst/best: `x2.24`

Latest validated cluster reference after extending the WAL compaction idle window:
- best considered round: `1.481s`
- worst considered round: `2.902s`
- worst/best: `x1.96`

Latest validated cluster reference after disabling repeated secondary-index rebuilds in `seed_domains` by default:
- benchmark: `10 rounds`, `warmup=1`
- command shape: repeated `go run ./scripts/seed_domains` against the same 3-node cluster
- best considered round: `1.038s`
- worst considered round: `1.477s`
- worst/best: `x1.42`

Latest validated cluster reference for repeat-run stability with `reset-domains=true`:
- benchmark: `10 rounds`, `warmup=1`
- best considered round: `1.002s`
- worst considered round: `1.634s`
- worst/best: `x1.63`
- last/best: `x1.12`

Root cause of the previous degradation:
- `seed_domains` was dropping and recreating secondary indexes on every repeat run;
- as the seeded domains accumulated more rows, each round paid a larger full index rebuild cost;
- with the current engine write path, keeping secondary indexes online is cheaper than rebuilding them each run for this workload.

Additional note:
- when `reset-domains=false`, repeated `seed_domains` runs are intentionally a growth workload, not a stable repeat-run workload;
- some slowdown is therefore expected because each round inserts into larger indexed tables.

Recommended default going forward: `BENCH_DEG_FAIL_RATIO=2.00` for cluster repeat-seed monitoring.

## 9) Append-growth monitor (pure ingest guardrail)

To measure pure append scalability on an already existing table, use the
focused append-growth benchmark. Unlike repeated `seed_domains`, this benchmark
does not recreate schemas every round, so it isolates the question “does
ingest get slower just because the table is larger?”.

Commands:

```bash
make bench-append-growth-single
make bench-append-growth-cluster
```

Configurable knobs:

```bash
make bench-append-growth \
	BENCH_APPEND_ADDR=127.0.0.1:5433 \
	BENCH_APPEND_ROUNDS=10 \
	BENCH_APPEND_WARMUP=1 \
	BENCH_APPEND_ROWS=20000 \
	BENCH_APPEND_BATCH=250 \
	BENCH_APPEND_PAYLOAD=96 \
	BENCH_APPEND_SECONDARY_INDEX=true \
	BENCH_APPEND_FAIL_RATIO=1.50
```

What it reports:
- round-by-round append duration,
- throughput in rows/sec,
- best/worst/avg duration,
- worst-to-best ratio as the primary growth signal.

Latest cluster investigation result:
- the former `~80k rows` cliff was not caused by the table or secondary index itself;
- profiling showed the leader spending most of the slow round inside the Raft replication path `sendHeartbeats -> walLog.Entries() -> SegmentedLogStore.ReadFrom()`;
- once follower lag pushed `nextIndex` behind the WAL hot-tail cache, the leader started rescanning segment files from disk on heartbeats/broadcasts;
- increasing the recent WAL read cache in [internal/storage/wal/segmented.go](../internal/storage/wal/segmented.go) removed the cliff in both the original `4 x 20k` case and the split `8 x 10k` case.

Current single-node reference with a secondary index on the target table:
- 10 rounds
- 20k rows per round
- best: `629ms`
- worst: `940ms`
- worst/best: `x1.49`

Current cluster reference after enlarging the WAL hot-tail cache:
- `8 x 10k` rounds with secondary index:
	- best: `349ms`
	- worst: `390ms`
	- worst/best: `x1.12`
- original `4 x 20k` reproduction:
	- best: `708ms`
	- worst: `740ms`
	- worst/best: `x1.05`

Recommended cluster guardrail command:

```bash
make bench-append-growth-cluster-guardrail
```

This target uses the validated `8 x 10k` workload and exits non-zero when
`worst/best > 1.25`.

Recommended default: `BENCH_APPEND_FAIL_RATIO=1.50`.
