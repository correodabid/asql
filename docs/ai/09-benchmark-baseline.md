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
- `BenchmarkEngineReadIndexOnlyOrderOffsetBTree-8`: ~`58,433–58,963 ns/op`, `233,392 B/op`, `222 allocs/op` (`btree-index-only`)

Selective covered-vs-non-covered repeated sample on 2026-03-14:

- `BenchmarkEngineReadIndexOnlySelectiveCoveredBTree-8`: ~`273,630–275,369 ns/op`, `234,072 B/op`, `235 allocs/op` (`btree-index-only`)
- `BenchmarkEngineReadSelectiveNonCoveredBTree-8`: ~`406,344–407,120 ns/op`, `309,561 B/op`, `656 allocs/op` (`btree-order`)

Composite-order repeated sample on 2026-03-14:

- `BenchmarkEngineReadCompositeCoveredFallbackBTree-8`: ~`62,906–72,783 ns/op`, `304,936–304,940 B/op`, `628 allocs/op` (`btree-order`)
- `BenchmarkEngineReadCompositeNonCoveredBTree-8`: ~`75,423–75,543 ns/op`, `305,864 B/op`, `732 allocs/op` (`btree-order`)

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
	- adding `OFFSET` to the covered ordered-read shape still keeps `btree-index-only` comfortably fast (about $1.7\times$ slower than the zero-offset variant, but still far below row-fetch ordered reads);
	- on the selective covered shape, adding bounded early-stop to the index-only path moved it ahead of `btree-order` as well (~`274 µs/op` vs ~`406 µs/op`), while keeping allocations materially lower.
	- on the current composite-order shape, even the covered projection still falls back to `btree-order`, so composite index-only coverage is not yet justified by implementation evidence.