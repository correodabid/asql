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

Initial dry-run on 2026-03-14 using `go test ./internal/engine/executor -run '^$' -bench 'BenchmarkEngineRead(IndexedRangeBTree|IndexOnlyOrderBTree)$' -benchtime=1x -count=1`:

- `BenchmarkEngineReadIndexedRangeBTree-8`: `2,000,362,042 ns/op`
- `BenchmarkEngineReadIndexOnlyOrderBTree-8`: `2,002,305,583 ns/op`

### Failover / recovery validation (`test/integration`)

Initial dry-run on 2026-03-14 using `go test ./test/integration -run '^$' -bench 'BenchmarkFailover(CoordinatorPromotion|RecoveryReplay)$' -benchtime=1x -count=1`:

- `BenchmarkFailoverCoordinatorPromotion-8`: `125,667 ns/op`
- `BenchmarkFailoverRecoveryReplay-8`: `227,584 ns/op`

## Notes

- This baseline is deterministic in workload shape and command path.
- Values are hardware/OS dependent and serve as a regression reference, not cross-machine SLA numbers.
- The restart-path numbers above are a single-iteration validation sample, useful for directional comparison only; they are not yet closure-grade AB evidence.