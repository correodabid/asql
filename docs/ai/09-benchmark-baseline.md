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
	- Repeated sample on 2026-03-14 using `-benchmem -benchtime=1x -count=2`:
		- `insert_heavy`
			- `replay_only_total_1000`: `8,703,959–15,581,751 ns/op`, `4,477,088–4,478,240 B/op`, `29,924–29,927 allocs/op`
			- `persisted_snapshot_total_1000_tail_500`: `22,139,541–22,287,918 ns/op`, `4,012,696–4,023,560 B/op`, `25,979–25,991 allocs/op`
			- `replay_only_total_10500`: `95,856,417–101,541,959 ns/op`, `62,068,584–62,364,488 B/op`, `361,314–361,328 allocs/op`
			- `persisted_snapshot_total_10500_tail_500`: `61,285,543–62,267,500 ns/op`, `35,567,080–35,567,240 B/op`, `216,175–216,176 allocs/op`
		- `update_heavy`
			- `replay_only_total_1000`: `282,722,001–293,694,791 ns/op`, `616,012,952–616,014,072 B/op`, `2,541,942–2,541,955 allocs/op`
			- `persisted_snapshot_total_1000_tail_500`: `277,817,458–282,720,417 ns/op`, `615,520,904–615,522,824 B/op`, `2,537,509–2,537,528 allocs/op`
			- `replay_only_total_10500`: `4,968,812,708–5,241,935,333 ns/op`, `12,084,745,184–12,084,747,232 B/op`, `50,458,480–50,458,505 allocs/op`
			- `persisted_snapshot_total_10500_tail_500`: `4,755,729,542–4,995,082,458 ns/op`, `12,055,915,168–12,055,922,384 B/op`, `50,291,848–50,291,864 allocs/op`
		- `delete_heavy`
			- `replay_only_total_1500`: `291,345,541–303,675,209 ns/op`, `617,717,752–617,718,720 B/op`, `2,672,305–2,672,312 allocs/op`
			- `persisted_snapshot_total_1500_tail_500`: `278,913,751–281,358,458 ns/op`, `616,060,056–616,060,208 B/op`, `2,660,308–2,660,313 allocs/op`
			- `replay_only_total_11000`: `3,497,548,041–3,544,605,791 ns/op`, `8,438,490,400–8,438,490,864 B/op`, `36,303,196–36,303,203 allocs/op`
			- `persisted_snapshot_total_11000_tail_500`: `3,314,063,625–3,434,278,375 ns/op`, `8,408,820,560–8,408,820,720 B/op`, `36,132,048–36,132,054 allocs/op`

- Natural policy-driven restart repeated sample after implementing mutation-mix-aware persisted checkpoints (`BenchmarkEngineRestartNaturalWorkloadCadenceSweep`, `-benchmem -benchtime=1x -count=2`):
	- `insert_heavy`
		- `replay_only_total_1000`: `15,953,290–18,086,999 ns/op`, `4,477,888–4,488,640 B/op`, `29,928–29,939 allocs/op`
		- `policy_persisted_total_1000_tail_500`: `11,542,874–13,014,082 ns/op`, `2,094,512–2,094,704 B/op`, `11,310 allocs/op`
		- `replay_only_total_10500`: `94,088,751–96,111,791 ns/op`, `62,167,448–62,266,056 B/op`, `361,319–361,326 allocs/op`
		- `policy_persisted_total_10500_tail_500`: `50,912,334–51,357,292 ns/op`, `24,960,192–24,969,080 B/op`, `115,895–115,897 allocs/op`
	- `update_heavy`
		- `replay_only_total_1000`: `286,147,167–290,157,876 ns/op`, `616,013,736–616,016,648 B/op`, `2,541,945–2,541,974 allocs/op`
		- `policy_persisted_total_1000_tail_500`: `11,578,166–12,223,459 ns/op`, `1,622,624–1,622,640 B/op`, `8,806 allocs/op`
		- `replay_only_total_10500`: `5,125,040,167–5,259,311,416 ns/op`, `12,084,649,328–12,084,651,632 B/op`, `50,458,480–50,458,505 allocs/op`
		- `policy_persisted_total_10500_tail_500`: `4,789,426,459–4,794,677,000 ns/op`, `12,055,916,320–12,055,916,608 B/op`, `50,291,852–50,291,862 allocs/op`
	- `delete_heavy`
		- `replay_only_total_1500`: `295,197,000–311,076,417 ns/op`, `617,718,120–617,719,904 B/op`, `2,672,316–2,672,317 allocs/op`
		- `policy_persisted_total_1500_tail_500`: `13,082,083–16,060,834 ns/op`, `2,140,400–2,140,416 B/op`, `11,809 allocs/op`
		- `replay_only_total_11000`: `3,518,639,292–3,553,196,959 ns/op`, `8,438,595,312–8,438,688,208 B/op`, `36,303,215–36,303,220 allocs/op`
		- `policy_persisted_total_11000_tail_500`: `3,354,867,793–3,375,866,541 ns/op`, `8,408,820,000–8,408,822,032 B/op`, `36,132,044–36,132,066 allocs/op`
	- Longer repeated confirmation on 2026-03-14 for the heaviest shapes (`-benchmem -benchtime=1x -count=3`):
		- `update_heavy`
			- `replay_only_total_1000`: `285,637,377–311,128,750 ns/op`, `616,016,184–616,016,896 B/op`, `2,541,969–2,541,978 allocs/op`
			- `policy_persisted_total_1000_tail_500`: `11,371,792–12,445,791 ns/op`, `1,622,624 B/op`, `8,806 allocs/op`
			- `replay_only_total_10500`: `4,972,909,501–5,183,491,917 ns/op`, `12,084,551,856–12,084,648,688 B/op`, `50,458,480–50,458,510 allocs/op`
			- `policy_persisted_total_10500_tail_500`: `4,797,593,583–4,856,035,793 ns/op`, `12,055,915,184–12,055,924,480 B/op`, `50,291,846–50,291,886 allocs/op`
		- `delete_heavy`
			- `replay_only_total_1500`: `288,345,501–310,098,999 ns/op`, `617,716,664–617,718,856 B/op`, `2,672,301–2,672,316 allocs/op`
			- `policy_persisted_total_1500_tail_500`: `15,398,500–16,506,167 ns/op`, `2,140,400–2,140,416 B/op`, `11,809 allocs/op`
			- `replay_only_total_11000`: `3,545,556,417–3,714,091,708 ns/op`, `8,438,490,032–8,438,491,488 B/op`, `36,303,195–36,303,207 allocs/op`
			- `policy_persisted_total_11000_tail_500`: `3,345,657,959–3,441,926,292 ns/op`, `8,408,820,176–8,408,821,872 B/op`, `36,132,050–36,132,064 allocs/op`

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
	- the repeated workload-cadence runs strengthen that policy signal: with a 500-record anchor, `insert_heavy` is consistently worse at the small checkpoint (~`22.1–22.3 ms` vs ~`8.7–15.6 ms`) but clearly better by the medium checkpoint (~`61.3–62.3 ms` vs ~`95.9–101.5 ms`), while `update_heavy` and `delete_heavy` are already break-even-to-better at the small anchor and widen their absolute savings by the medium anchor;
	- memory follows the same pattern: by the medium anchor, persisted snapshots cut restart allocation volume materially for `insert_heavy` (~`35.6 MB` vs ~`62.1–62.4 MB`) and trim it modestly for `update_heavy`/`delete_heavy`, so the cadence choice affects both wall-clock restart time and heap pressure;
	- that policy has now been implemented for persisted checkpoints: the existing volume-based anchors in [internal/engine/executor/snapshot.go](internal/engine/executor/snapshot.go#L10-L40) remain the baseline, but the engine now tracks a rolling recent mutation-pressure window (`insert=1`, `update=4`, `delete=3`) and halves the persisted-checkpoint mutation interval when weighted update/delete pressure dominates, with a floor of `250` mutations;
	- disk and memory policy are still treated separately at the policy level: the new heuristic only targets persisted checkpoint cadence, while in-memory snapshot retention and the base in-memory capture anchors remain unchanged for now;
	- repeated natural-cadence runs now make that result much more credible: without forcing a final flush, the policy-driven persisted path consistently cuts small-anchor restart from ~`16–18 ms` to ~`11.5–13.0 ms` for `insert_heavy`, from ~`286–290 ms` to ~`11.6–12.2 ms` for `update_heavy`, and from ~`295–311 ms` to ~`13.1–16.1 ms` for `delete_heavy` when a runtime checkpoint lands near the tail;
	- at the medium anchor, the policy still preserves meaningful wins with less dramatic variance: `insert_heavy` drops from ~`94–96 ms` to ~`51 ms`, `update_heavy` from ~`5.13–5.26 s` to ~`4.79 s`, and `delete_heavy` from ~`3.52–3.55 s` to ~`3.35–3.38 s`;
	- allocation pressure also drops sharply in the natural small-anchor cases because restart no longer has to replay the long tail: for example `update_heavy` falls from ~`616 MB` / ~`2.54M` allocs to ~`1.62 MB` / ~`8.8k` allocs, and `delete_heavy` from ~`618 MB` / ~`2.67M` allocs to ~`2.14 MB` / ~`11.8k` allocs;
	- the extra three-run confirmation on `update_heavy` and `delete_heavy` keeps those ranges tight enough to treat the effect as stable rather than anecdotal, especially for the small-anchor “checkpoint lands near the tail” case where the policy changes restart behavior by more than an order of magnitude;
	- taken together, the deterministic test coverage plus the repeated natural-restart runs are now strong enough to treat restart/cadence policy as a closed evidence slice, so the remaining snapshot work should focus on snapshot-directory load efficiency rather than on further cadence tuning;
	- the focused split shows the persisted restart cost is still dominated by snapshot-directory read/decompress/decode/materialization (~`302–377 µs/op`), while the in-memory `replayFromSnapshots` restore step itself is comparatively small (~`60 µs/op`);
	- inside the snapshot-directory load, binary decode remains the largest measured in-process component, but the direct positional-row decode path plus contiguous row-slab allocation pushed it down further to roughly `70.9–72.0 µs/op`, `187.6 KB/op`, and `586 allocs/op` on the base fixture, while the full directory load improved to roughly `324–364 µs/op` with `~1.77k allocs/op`;
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