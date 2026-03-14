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

Harness note on 2026-03-14: the restart benchmark now clones the seeded WAL/snapshot fixture per iteration before timing starts, so persisted-snapshot runs no longer accumulate extra checkpoint files between iterations.

Repeated sample on 2026-03-14 using isolated fixtures and `go test ./internal/engine/executor -run '^$' -bench '^BenchmarkEngineRestart...$' -benchmem -benchtime=200ms -count=2`:

- `BenchmarkEngineRestartReplayOnly-8`: `3,330,478–3,413,274 ns/op`, `2,303,626–2,303,673 B/op`, `15,573 allocs/op`
- `BenchmarkEngineRestartFromPersistedSnapshot-8`: `3,910,771–3,918,000 ns/op`, `1,122,907–1,123,694 B/op`, `6,309 allocs/op`

Focused persisted-snapshot load split on 2026-03-14 using `go test ./internal/engine/executor -run '^$' -bench 'BenchmarkEngine(ReadPersistedSnapshotsFromDir|ReplayFromPersistedSnapshots)$' -benchmem -benchtime=100ms -count=1`:

- `BenchmarkEngineReadPersistedSnapshotsFromDir-8`: `302,163–377,369 ns/op`, `353,257–633,829 B/op`, `1,724–2,277 allocs/op`
- `BenchmarkEngineReplayFromPersistedSnapshots-8`: `59,530 ns/op`, `167,720 B/op`, `1,132 allocs/op`

Finer-grained persisted-snapshot microbenchmarks on 2026-03-14:

- `BenchmarkEngineReadPersistedSnapshotFilesOnly-8`: `87,171 ns/op`, `8,880 B/op`, `15 allocs/op`
- `BenchmarkEngineDecompressPersistedSnapshotFiles-8`: `74,280 ns/op`, `32,784 B/op`, `1 allocs/op`
- `BenchmarkEngineDecodePersistedSnapshotFiles-8`: `79,390–166,199 ns/op`, `185,552–458,408 B/op`, `1,128–1,678 allocs/op`
- `BenchmarkEngineDecodePersistedSnapshotFilesIndexed-8`: `166,620 ns/op`, `337,897 B/op`, `1,689 allocs/op`
- `BenchmarkEngineDecodeFullSnapshotDirect-8`: `126,367 ns/op`, `298,559 B/op`, `1,700 allocs/op`
- `BenchmarkEngineDecodeFullSnapshotDirectIndexed-8`: `254,789 ns/op`, `527,075 B/op`, `2,266 allocs/op`
- `BenchmarkEngineMergePersistedSnapshotDeltas-8`: `3.271 ns/op`, `0 B/op`, `0 allocs/op`
- `BenchmarkEngineMaterializePersistedSnapshots-8`: `49,473–57,281 ns/op`, `113,050–113,054 B/op`, `573 allocs/op`
- `BenchmarkEngineReadPersistedSnapshotsFromDirIndexed-8`: `492,740 ns/op`, `655,010 B/op`, `2,293 allocs/op`

Replay-throughput repeated sample on 2026-03-14:

- `BenchmarkEngineReplayToLSN-8`: ~`1,998,773,750–2,000,865,708 ns/op`, `2,693,952–2,704,560 B/op`, `16,664–16,676 allocs/op`

### Indexed-read validation (`internal/engine/executor`)

Repeated sample on 2026-03-14 using `go test ./internal/engine/executor -run '^$' -bench ... -benchmem -benchtime=200ms -count=3`:

- `BenchmarkEngineReadIndexedRangeBTree-8`: ~`353,070–360,654 ns/op`, `309,272–309,273 B/op`, `656 allocs/op` (`btree-order`)
- `BenchmarkEngineReadIndexOnlyOrderBTree-8`: ~`34,027–34,369 ns/op`, `233,320–233,321 B/op`, `220 allocs/op` (`btree-index-only`)
- `BenchmarkEngineReadIndexOnlyOrderOffsetBTree-8`: ~`58,433–58,963 ns/op`, `233,392 B/op`, `222 allocs/op` (`btree-index-only`)

Selective covered-vs-non-covered repeated sample on 2026-03-14:

- `BenchmarkEngineReadIndexOnlySelectiveCoveredBTree-8`: ~`273,630–275,369 ns/op`, `234,072 B/op`, `235 allocs/op` (`btree-index-only`)
- `BenchmarkEngineReadSelectiveNonCoveredBTree-8`: ~`406,344–407,120 ns/op`, `309,561 B/op`, `656 allocs/op` (`btree-order`)

Composite-order repeated sample on 2026-03-14:

- `BenchmarkEngineReadCompositeCoveredIndexOnlyBTree-8`: ~`36,164–36,575 ns/op`, `233,632–233,633 B/op`, `226 allocs/op` (`btree-index-only`)
- `BenchmarkEngineReadCompositeNonCoveredBTree-8`: ~`76,383–77,565 ns/op`, `305,872 B/op`, `733 allocs/op` (`btree-order`)

### Failover / recovery validation (`test/integration`)

Initial dry-run on 2026-03-14 using `go test ./test/integration -run '^$' -bench 'BenchmarkFailover(CoordinatorPromotion|RecoveryReplay)$' -benchtime=1x -count=1`:

- `BenchmarkFailoverCoordinatorPromotion-8`: `125,667 ns/op`
- `BenchmarkFailoverRecoveryReplay-8`: `227,584 ns/op`

## Notes

- This baseline is deterministic in workload shape and command path.
- Values are hardware/OS dependent and serve as a regression reference, not cross-machine SLA numbers.
- The restart/replay numbers above are useful internal evidence but are not closure-grade AB evidence yet.
- Current restart-path interpretation:
	- replay-to-LSN is now benchmarked with a stable repeated sample around ~`2.0 ms/op` on this fixture;
	- after fixing the restart benchmark harness, removing one extra deep copy during snapshot materialization, reducing dictionary-string allocation in the binary decoder, and decoding table rows directly into positional slices, the persisted-snapshot path is now much closer to replay-only on repeated runs while still allocating materially less, but it is still slower on this fixture, so snapshot-load work remains open rather than justified for closure;
	- the repeated longer-benchtime sample now shows persisted-snapshot restart stabilizing around ~`3.91 ms/op` versus ~`3.33–3.41 ms/op` for replay-only, while cutting restart allocations to ~`1.12 MB/op` and ~`6.3k allocs/op`;
	- the focused split shows the persisted restart cost is still dominated by snapshot-directory read/decompress/decode/materialization (~`302–377 µs/op`), while the in-memory `replayFromSnapshots` restore step itself is comparatively small (~`60 µs/op`);
	- inside the snapshot-directory load, binary decode remains the largest measured in-process component, but the direct positional-row decode path pushed it down materially to roughly `79–166 µs/op`, `186–458 KB/op`, and `1.1k–1.7k allocs/op`, ahead of raw file I/O (~`87 µs/op`), zstd decompression (~`74 µs/op`), and snapshot materialization (~`49–57 µs/op`);
	- delta-chain merge is negligible on the current fixture because the harness is effectively loading a single persisted snapshot file, so this AB slice is presently about file read + decode efficiency rather than cross-file snapshot chaining.
	- an index-rich snapshot fixture (primary key + hash + btree persisted) raises full snapshot-directory load to about `493 µs/op`, but decode still stays in the same order of magnitude (~`167 µs/op`), so persisted index payload is not causing a new dominant hotspot beyond the existing file-read + decode path.
	- removing the extra raw-file accumulation pass in `readAllSnapshotsFromDir()` did not materially shift the measured timings on the current fixture, which further suggests the remaining opportunity is inside file-read/decompress/decode itself rather than in the old two-pass control flow.
	- a direct full-snapshot decode experiment using `decodeSnapshotsBinary()` did not show a compelling win over the current raw-decode + materialize path on either the base or index-rich fixture, so a larger refactor toward that direct path is not yet justified by the current evidence.
	- the positional-row decode change clearly improved isolated decode and snapshot-directory load benchmarks, but the end-to-end persisted-restart timing is still noisy on the current short benchtime harness and needs repeated confirmation before any closure claim.
- Current read-path interpretation:
	- on the simple covered ordered-read shape, `btree-index-only` is about $10\times$ faster than `btree-order` and materially reduces allocations;
	- adding `OFFSET` to the covered ordered-read shape still keeps `btree-index-only` comfortably fast (about $1.7\times$ slower than the zero-offset variant, but still far below row-fetch ordered reads);
	- on the selective covered shape, adding bounded early-stop to the index-only path moved it ahead of `btree-order` as well (~`274 µs/op` vs ~`406 µs/op`), while keeping allocations materially lower.
	- on the composite covered ordered-read shape, enabling composite index-only coverage moved the path to ~`36 µs/op` versus ~`76–78 µs/op` for the non-covered row-fetch path, with materially lower allocations.