# ASQL Benchmark Baseline (Phase K)

Date: 2026-02-28

Status note (2026-03-12): this is an internal benchmark snapshot.
Use [docs/product/benchmark-one-pager-v1.md](../product/benchmark-one-pager-v1.md) for the current benchmark summary and [docs/product/performance-benchmark-plan-v1.md](../product/performance-benchmark-plan-v1.md) for the active layered benchmark plan.

Environment:
- OS: macOS (darwin/arm64)
- CPU: Apple M1
- Go: from `go.mod` toolchain

## Command

```bash
make bench
```

## Results

### Engine (`internal/engine/executor`)

- `BenchmarkEngineWriteCommit-8`: `69,430 ns/op`, `4,248 B/op`, `62 allocs/op`
- `BenchmarkEngineReadAsOfLSN-8`: `5,038,622 ns/op`, `3,178,457 B/op`, `39,126 allocs/op`
- `BenchmarkEngineReplayToLSN-8`: `4,794,214 ns/op`, `2,631,836 B/op`, `37,097 allocs/op`

### WAL (`internal/storage/wal`)

- `BenchmarkFileLogStoreAppend-8`: `24,561 ns/op`, `573 B/op`, `6 allocs/op`
- `BenchmarkFileLogStoreReadFrom-8`: `15,634,191 ns/op`, `5,593,017 B/op`, `60,029 allocs/op`
- `BenchmarkFileLogStoreRecover-8`: `13,377,896 ns/op`, `5,592,932 B/op`, `60,028 allocs/op`

### Restart-path validation (`internal/engine/executor`)

Initial dry-run on 2026-03-14 using `go test ./internal/engine/executor -run '^$' -bench 'BenchmarkEngineRestart(ReplayOnly|FromPersistedSnapshot)$' -benchtime=1x -count=1`:

- `BenchmarkEngineRestartReplayOnly-8`: `10,152,376 ns/op`
- `BenchmarkEngineRestartFromPersistedSnapshot-8`: `5,134,292 ns/op`

### Indexed-read validation (`internal/engine/executor`)

Repeated sample on 2026-03-14 using `go test ./internal/engine/executor -run '^$' -bench ... -benchmem -benchtime=200ms -count=3`:

- `BenchmarkEngineReadIndexedRangeBTree-8`: ~`353,070–360,654 ns/op`, `309,272–309,273 B/op`, `656 allocs/op` (`btree-order`)
- `BenchmarkEngineReadIndexOnlyOrderBTree-8`: ~`34,027–34,369 ns/op`, `233,320–233,321 B/op`, `220 allocs/op` (`btree-index-only`)

Selective covered-vs-non-covered repeated sample on 2026-03-14:

- `BenchmarkEngineReadIndexOnlySelectiveCoveredBTree-8`: ~`722,951–734,480 ns/op`, `234,072–234,073 B/op`, `235 allocs/op` (`btree-index-only`)
- `BenchmarkEngineReadSelectiveNonCoveredBTree-8`: ~`406,344–407,120 ns/op`, `309,561 B/op`, `656 allocs/op` (`btree-order`)

### Failover / recovery validation (`test/integration`)

Initial dry-run on 2026-03-14 using `go test ./test/integration -run '^$' -bench 'BenchmarkFailover(CoordinatorPromotion|RecoveryReplay)$' -benchtime=1x -count=1`:

- `BenchmarkFailoverCoordinatorPromotion-8`: `125,667 ns/op`
- `BenchmarkFailoverRecoveryReplay-8`: `227,584 ns/op`

## Notes

- This baseline is deterministic in workload shape and command path.
- Values are hardware/OS dependent and serve as a regression reference, not cross-machine SLA numbers.
- The restart-path numbers above are a single-iteration validation sample, useful for directional comparison only; they are not yet closure-grade AB evidence.
- Current read-path interpretation:
	- on the simple covered ordered-read shape, `btree-index-only` is about $10\times$ faster than `btree-order` and materially reduces allocations;
	- on the selective covered shape, `btree-index-only` is slower than `btree-order`, which points to path-shape limitations rather than a general verdict against index-only reads.