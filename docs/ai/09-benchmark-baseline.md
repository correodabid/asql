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

Scope note on 2026-03-14: the current `BenchmarkEngineRestartFromPersistedSnapshot` fixture calls `WaitPendingSnapshots()` before shutdown, so it benchmarks a head snapshot plus effectively `0` trailing WAL records rather than a snapshot plus a non-zero replay tail.

Restart-tail/cadence spot checks on 2026-03-14 using `-benchtime=1x`:

- Fixed 500-record snapshot anchor, varying replay tail (`BenchmarkEngineRestartReplayTailSweep`):
	- `replay_only_tail_0`: `8,684,291 ns/op`
	- `persisted_snapshot_tail_0`: `8,390,041 ns/op`
	- `replay_only_tail_500`: `16,025,499 ns/op`
	- `persisted_snapshot_tail_500`: `14,568,958 ns/op`
	- `replay_only_tail_5000`: `35,608,000 ns/op`
	- `persisted_snapshot_tail_5000`: `50,201,501 ns/op`
	- `replay_only_tail_10000`: `94,399,250 ns/op`
	- `persisted_snapshot_tail_10000`: `108,807,917 ns/op`
- Adaptive-cadence anchor samples with a fixed 500-record tail (`BenchmarkEngineRestartSnapshotCadenceSweep`):
	- `replay_only_total_1000`: `16,160,000 ns/op`
	- `persisted_snapshot_total_1000_tail_500`: `12,671,750 ns/op`
	- `replay_only_total_10500`: `95,817,917 ns/op`
	- `persisted_snapshot_total_10500_tail_500`: `57,602,874 ns/op`
	- `replay_only_total_50500`: `307,145,457 ns/op`
	- `persisted_snapshot_total_50500_tail_500`: `289,921,333 ns/op`
- Workload-shape samples at the default 500-record anchor (`BenchmarkEngineRestartWorkloadSweep`):
	- Repeated sample on 2026-03-14 using `-benchmem -benchtime=100ms -count=3`:
		- `insert_heavy/replay_only`: `7,965,869–8,379,576 ns/op`, `4,477,891–4,477,941 B/op`, `29,927 allocs/op`
		- `insert_heavy/persisted_snapshot`: `9,769,205–10,802,304 ns/op`, `3,962,344–3,993,404 B/op`, `25,952–25,962 allocs/op`
		- `update_heavy/replay_only`: `277,177,125–302,429,916 ns/op`, `616,015,192–616,016,792 B/op`, `2,541,964–2,541,977 allocs/op`
		- `update_heavy/persisted_snapshot`: `265,988,584–281,389,960 ns/op`, `615,521,352–615,522,216 B/op`, `2,537,514–2,537,522 allocs/op`
		- `delete_heavy/replay_only`: `288,364,708–291,897,833 ns/op`, `617,715,624–617,720,528 B/op`, `2,672,291–2,672,326 allocs/op`
		- `delete_heavy/persisted_snapshot`: `274,890,166–281,764,417 ns/op`, `616,057,856–616,062,480 B/op`, `2,660,287–2,660,301 allocs/op`
- Workload-cadence spot checks using `-benchtime=1x` (`BenchmarkEngineRestartWorkloadCadenceSweep`):
	- `insert_heavy`:
		- `replay_only_total_1000`: `16,729,292 ns/op`
		- `persisted_snapshot_total_1000_tail_500`: `23,245,208 ns/op`
		- `replay_only_total_10500`: `94,894,459 ns/op`
		- `persisted_snapshot_total_10500_tail_500`: `57,443,874 ns/op`
	- `update_heavy`:
		- `replay_only_total_1000`: `275,465,791 ns/op`
		- `persisted_snapshot_total_1000_tail_500`: `276,737,665 ns/op`
		- `replay_only_total_10500`: `5,179,752,334 ns/op`
		- `persisted_snapshot_total_10500_tail_500`: `4,774,485,000 ns/op`
	- `delete_heavy`:
		- `replay_only_total_1500`: `293,173,750 ns/op`
		- `persisted_snapshot_total_1500_tail_500`: `271,649,376 ns/op`
		- `replay_only_total_11000`: `3,812,350,749 ns/op`
		- `persisted_snapshot_total_11000_tail_500`: `3,327,926,250 ns/op`

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
	- the current end-to-end restart comparison is a head-snapshot best-case (`0` post-snapshot replay records), so it is good for measuring raw snapshot-load overhead but not for choosing snapshot cadence by itself;
	- after fixing the restart benchmark harness, removing one extra deep copy during snapshot materialization, reducing dictionary-string allocation in the binary decoder, and decoding table rows directly into positional slices, the persisted-snapshot path is now much closer to replay-only on repeated runs while still allocating materially less, but it is still slower on this fixture, so snapshot-load work remains open rather than justified for closure;
	- the repeated longer-benchtime sample now shows persisted-snapshot restart stabilizing around ~`3.91 ms/op` versus ~`3.33–3.41 ms/op` for replay-only, while cutting restart allocations to ~`1.12 MB/op` and ~`6.3k allocs/op`;
	- new spot-check benchmarks now separate two different questions: how much a fixed snapshot anchor helps as the replay tail grows, and how the current adaptive cadence behaves when the engine skips roughly one full interval then replays only the last ~`500` records;
	- on the current M1 spot checks, a snapshot plus ~`500` replayed records clearly beats replay-only at ~`1k` total rows (`12.7 ms` vs `16.2 ms`) and around the current medium anchor of ~`10.5k` total rows (`57.6 ms` vs `95.8 ms`), while the win narrows again by ~`50.5k` total rows (`289.9 ms` vs `307.1 ms`), so cadence is materially workload-size dependent and should be tuned with these sweeps rather than inferred from the old best-case restart fixture alone;
	- the new workload-shape sweep also confirms that “same tail length” is not enough by itself: with a default 500-record anchor, append-heavy restart remained in the ~`16–21 ms` range while update-heavy and delete-heavy shapes landed around ~`270–278 ms`, so mutation mix has to be treated as a first-class input to any eventual snapshot-cadence policy;
	- repeated workload runs now tighten that result: at the default 500-record anchor, persisted snapshots are clearly worse for `insert_heavy` (~`9.8–10.8 ms` vs ~`8.0–8.4 ms` replay-only) but modestly better for `update_heavy` (~`266–281 ms` vs ~`277–302 ms`) and `delete_heavy` (~`275–282 ms` vs ~`288–292 ms`), while also shaving a small amount of memory and allocations in all three shapes;
	- the new workload-cadence sweep suggests the crossover depends strongly on mutation mix: `insert_heavy` still loses with a 500-record anchor at ~`1k` total mutations but flips strongly in favor of snapshots by ~`10.5k`, while `update_heavy` and `delete_heavy` are already roughly break-even or better at the small anchor and gain a larger absolute win by the medium anchor;
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