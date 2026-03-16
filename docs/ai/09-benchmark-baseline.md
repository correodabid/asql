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

Status note (2026-03-15): the repeated `BenchmarkEngineReplayToLSN` harness now warms cached replay records, primes one replay to the target `LSN` before timing, and stops the timer before teardown so it measures repeated exact-target `ReplayToLSN` calls instead of first-build plus cleanup cost.

Repeated sample on 2026-03-15 using `go test ./internal/engine/executor -run '^$' -bench '^BenchmarkEngineReplayToLSN$' -benchmem -count=5`:

- `BenchmarkEngineReplayToLSN-8`: `2.130–2.179 ns/op`, `0 B/op`, `0 allocs/op`

### WAL (`internal/storage/wal`)

- `BenchmarkFileLogStoreAppend-8`: `24,561 ns/op`, `573 B/op`, `6 allocs/op`
- `BenchmarkFileLogStoreReadFrom-8`: `15,634,191 ns/op`, `5,593,017 B/op`, `60,029 allocs/op`
- `BenchmarkFileLogStoreRecover-8`: `13,377,896 ns/op`, `5,592,932 B/op`, `60,028 allocs/op`

### Restart-path validation (`internal/engine/executor`)

Harness note on 2026-03-14: the restart benchmark now clones the seeded WAL/snapshot fixture per iteration before timing starts, so persisted-snapshot runs no longer accumulate extra checkpoint files between iterations.

Repeated sample on 2026-03-14 using isolated fixtures and `go test ./internal/engine/executor -run '^$' -bench '^BenchmarkEngineRestart...$' -benchmem -benchtime=200ms -count=2`:

- `BenchmarkEngineRestartReplayOnly-8`: `3,330,478–3,413,274 ns/op`, `2,303,626–2,303,673 B/op`, `15,573 allocs/op`
- `BenchmarkEngineRestartFromPersistedSnapshot-8`: `3,910,771–3,918,000 ns/op`, `1,122,907–1,123,694 B/op`, `6,309 allocs/op`

Status note (2026-03-16): snapshot restart now skips duplicate shutdown checkpoint writes when the latest `LSN` is already on disk, and timestamp-index load no longer decodes the on-disk file into a full entry slice before compressing it.

Repeated sample on 2026-03-16 using `go test ./internal/engine/executor -run '^$' -bench '^(BenchmarkEngine(ReadPersistedSnapshotsFromDir|RestartReplayOnly|RestartFromPersistedSnapshot))$' -benchmem -benchtime=200ms -count=2`:

- `BenchmarkEngineRestartReplayOnly-8`: `1,940,649–2,046,391 ns/op`, `1,718,720–1,718,802 B/op`, `12,809 allocs/op`
- `BenchmarkEngineRestartFromPersistedSnapshot-8`: `2,224,551–2,476,452 ns/op`, `755,898–771,614 B/op`, `2,973–2,978 allocs/op`
- `BenchmarkEngineReadPersistedSnapshotsFromDir-8`: `239,629–242,662 ns/op`, `353,609–360,022 B/op`, `1,175–1,177 allocs/op`
- `BenchmarkEngineReplayFromPersistedSnapshots-8`: `452.1–456.5 ns/op`, `640 B/op`, `8 allocs/op`

Follow-up sample on 2026-03-16 after fast-pathing single-file snapshot directories through direct binary decode:

- `BenchmarkEngineRestartFromPersistedSnapshot-8`: `2,263,819–2,437,330 ns/op`, `767,349–772,868 B/op`, `2,974–2,975 allocs/op`
- `BenchmarkEngineReadPersistedSnapshotsFromDir-8`: `235,134–237,537 ns/op`, `352,351–354,822 B/op`, `1,172 allocs/op`

Follow-up sample on 2026-03-16 after decoding single-file full snapshots directly into runtime state with `go test ./internal/engine/executor -run '^$' -bench 'BenchmarkEngine(RestartFromPersistedSnapshot|ReadPersistedSnapshotsFromDir|DecodeFullSnapshotDirect)(Indexed)?$' -benchmem`:

- `BenchmarkEngineRestartFromPersistedSnapshot-8`: `2,267,591 ns/op`, `678,646 B/op`, `2,418 allocs/op`
- `BenchmarkEngineReadPersistedSnapshotsFromDir-8`: `175,658 ns/op`, `241,742 B/op`, `609 allocs/op`
- `BenchmarkEngineDecodeFullSnapshotDirect-8`: `94,210 ns/op`, `299,802 B/op`, `1,151 allocs/op`
- `BenchmarkEngineDecodeFullSnapshotDirectIndexed-8`: `180,197 ns/op`, `485,234 B/op`, `1,713 allocs/op`
- `BenchmarkEngineReadPersistedSnapshotsFromDirIndexed-8`: `304,087 ns/op`, `474,543 B/op`, `1,174 allocs/op`

Repeated comparison on 2026-03-16 using `go test ./internal/engine/executor -run '^$' -bench '^(BenchmarkEngine(RestartReplayOnly|RestartFromPersistedSnapshot|ReadPersistedSnapshotsFromDir))$' -benchmem -benchtime=200ms -count=2`:

- `BenchmarkEngineRestartReplayOnly-8`: `1,976,196–2,412,504 ns/op`, `1,718,644–1,718,743 B/op`, `12,809 allocs/op`
- `BenchmarkEngineRestartFromPersistedSnapshot-8`: `2,577,547–2,728,238 ns/op`, `638,328–658,626 B/op`, `2,405–2,411 allocs/op`
- `BenchmarkEngineReadPersistedSnapshotsFromDir-8`: `196,872–198,813 ns/op`, `240,459–240,518 B/op`, `608 allocs/op`

Follow-up cadence/tail spot checks on 2026-03-16 using `-benchtime=1x -count=1`:

- Fixed 500-record snapshot anchor, varying replay tail (`BenchmarkEngineRestartReplayTailSweep`):
	- `replay_only_tail_0`: `5,897,250 ns/op`, `1,645,048 B/op`, `11,817 allocs/op`
	- `persisted_snapshot_tail_0`: `6,142,457 ns/op`, `682,456 B/op`, `2,241 allocs/op`
	- `replay_only_tail_50`: `6,002,709 ns/op`, `1,718,920 B/op`, `12,810 allocs/op`
	- `persisted_snapshot_tail_50`: `7,821,750 ns/op`, `1,124,128 B/op`, `6,497 allocs/op`
	- `replay_only_tail_100`: `5,228,250 ns/op`, `1,800,104 B/op`, `13,809 allocs/op`
	- `persisted_snapshot_tail_100`: `8,295,625 ns/op`, `1,249,824 B/op`, `7,653 allocs/op`
	- `replay_only_tail_500`: `11,062,957 ns/op`, `3,417,960 B/op`, `24,883 allocs/op`
	- `persisted_snapshot_tail_500`: `15,532,751 ns/op`, `3,036,032 B/op`, `19,921 allocs/op`
	- `replay_only_tail_1000`: `11,822,583 ns/op`, `5,191,776 B/op`, `39,444 allocs/op`
	- `persisted_snapshot_tail_1000`: `22,111,125 ns/op`, `5,110,952 B/op`, `35,981 allocs/op`
	- `replay_only_tail_5000`: `23,328,166 ns/op`, `26,575,856 B/op`, `178,498 allocs/op`
	- `persisted_snapshot_tail_5000`: `37,523,960 ns/op`, `31,855,616 B/op`, `202,083 allocs/op`
	- `replay_only_tail_10000`: `136,021,875 ns/op`, `49,212,544 B/op`, `308,767 allocs/op`
	- `persisted_snapshot_tail_10000`: `96,407,625 ns/op`, `59,327,800 B/op`, `348,823 allocs/op`
- Adaptive-cadence anchor samples with a fixed 500-record tail (`BenchmarkEngineRestartSnapshotCadenceSweep`):
	- `replay_only_total_1000`: `11,488,125 ns/op`, `3,417,912 B/op`, `24,884 allocs/op`
	- `persisted_snapshot_total_1000_tail_500`: `15,539,708 ns/op`, `3,036,016 B/op`, `19,921 allocs/op`
	- `replay_only_total_10500`: `69,258,501 ns/op`, `49,112,904 B/op`, `308,761 allocs/op`
	- `persisted_snapshot_total_10500_tail_500`: `42,842,625 ns/op`, `24,531,768 B/op`, `143,575 allocs/op`
	- `replay_only_total_50500`: `201,905,458 ns/op`, `221,515,432 B/op`, `1,259,855 allocs/op`
	- `persisted_snapshot_total_50500_tail_500`: `194,355,625 ns/op`, `118,840,328 B/op`, `664,072 allocs/op`

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

Follow-up after the direct runtime-state full-snapshot decoder on 2026-03-16:

- `BenchmarkEngineDecodeFullSnapshotDirect-8`: `94,210 ns/op`, `299,802 B/op`, `1,151 allocs/op`
- `BenchmarkEngineDecodeFullSnapshotDirectIndexed-8`: `180,197 ns/op`, `485,234 B/op`, `1,713 allocs/op`
- `BenchmarkEngineReadPersistedSnapshotsFromDirIndexed-8`: `304,087 ns/op`, `474,543 B/op`, `1,174 allocs/op`

Replay-throughput repeated sample:

- 2026-03-14 baseline:
	- `BenchmarkEngineReplayToLSN-8`: ~`1,998,773,750–2,000,865,708 ns/op`, `2,693,952–2,704,560 B/op`, `16,664–16,676 allocs/op`
- 2026-03-15 after skipping snapshot maintenance for bounded `ReplayToLSN` rebuilds:
	- `BenchmarkEngineReplayToLSN-8`: ~`1,995,093,250–1,998,993,334 ns/op`, `1,412,816–1,418,160 B/op`, `12,085–12,091 allocs/op`

### Indexed-read validation (`internal/engine/executor`)

Repeated sample on 2026-03-14 using `go test ./internal/engine/executor -run '^$' -bench ... -benchmem -benchtime=200ms -count=3`:

- `BenchmarkEngineReadIndexedRangeBTree-8`: ~`269,974–272,623 ns/op`, `228,249 B/op`, `656 allocs/op` (`btree-order`)
- `BenchmarkEngineReadIndexOnlyOrderBTree-8`: ~`31,255–31,704 ns/op`, `152,296 B/op`, `220 allocs/op` (`btree-index-only`)
- `BenchmarkEngineReadIndexOnlyOrderOffsetBTree-8`: ~`56,048–57,085 ns/op`, `192,432–192,433 B/op`, `222 allocs/op` (`btree-index-only`)

Selective covered-vs-non-covered repeated sample on 2026-03-14:

- `BenchmarkEngineReadIndexOnlySelectiveCoveredBTree-8`: ~`34,910–48,723 ns/op`, `153,048 B/op`, `235 allocs/op` (`btree-index-only`)
- `BenchmarkEngineReadSelectiveNonCoveredBTree-8`: ~`305,344–353,132 ns/op`, `228,536 B/op`, `656 allocs/op` (`btree-order`)

Composite-order repeated sample on 2026-03-14:

- `BenchmarkEngineReadCompositeCoveredIndexOnlyBTree-8`: ~`33,641–33,963 ns/op`, `152,608 B/op`, `226 allocs/op` (`btree-index-only`)
- `BenchmarkEngineReadCompositeNonCoveredBTree-8`: ~`76,383–77,565 ns/op`, `305,872 B/op`, `733 allocs/op` (`btree-order`)

### Failover / recovery validation (`test/integration`)

Repeated sample on 2026-03-15:

- Control-plane promotion using `go test ./test/integration -run '^$' -bench '^BenchmarkFailoverCoordinatorPromotion$' -benchmem -benchtime=200ms -count=3`:
	- `BenchmarkFailoverCoordinatorPromotion-8`: ~`654.1–805.0 ns/op`, `1,592 B/op`, `16 allocs/op`
- Recovery replay using `go test ./test/integration -run '^$' -bench 'BenchmarkFailover(RecoveryReplay|RecoveryReplaySweep)$' -benchmem -benchtime=1x -count=3`:
	- `BenchmarkFailoverRecoveryReplay-8`: ~`143,333–179,334 ns/op`, `95,064 B/op`, `245 allocs/op`
	- `BenchmarkFailoverRecoveryReplaySweep/small_total_40-8`: ~`438,000 ns/op`, `242,136 B/op`, `1,248 allocs/op`
	- `BenchmarkFailoverRecoveryReplaySweep/medium_total_640-8`: ~`8,669,333–10,771,083 ns/op`, `2,766,008–2,770,520 B/op`, `18,034–18,038 allocs/op`
	- `BenchmarkFailoverRecoveryReplaySweep/large_total_4608-8`: ~`44,389,084–56,834,041 ns/op`, `29,590,344–29,688,128 B/op`, `184,668–184,674 allocs/op`
- Large-scenario phase split using `go test ./test/integration -run '^$' -bench '^BenchmarkFailoverRecoveryReplayLargePhases$' -benchmem -benchtime=1x -count=3`:
	- previous split before the latest replay hot-path cleanup:
		- `store_reopen`: ~`12,521,959–20,042,500 ns/op`, `2,586,480–2,586,752 B/op`, `14,020–14,022 allocs/op`
		- `engine_replay`: ~`23,028,875–26,327,249 ns/op`, `27,102,704–27,202,496 B/op`, `170,647–170,660 allocs/op`
	- current split after skipping non-entity mutation tracking work and per-insert perf instrumentation during replay:
		- `store_reopen`: ~`13,457,083–21,864,708 ns/op`, `2,587,072–2,587,088 B/op`, `14,023 allocs/op`
		- `engine_replay`: ~`19,781,042–24,530,917 ns/op`, `23,834,288–23,932,480 B/op`, `161,430–161,432 allocs/op`
	- current split after persisting segment headers for clean uncompressed reopen:
		- `store_reopen`: ~`38,083–73,158 ns/op`, `4,688 B/op`, `30 allocs/op`
		- `engine_replay`: ~`2,098,562–4,252,816 ns/op`, `23,932,512–23,933,712 B/op`, `161,433–161,438 allocs/op`
		- end-to-end `BenchmarkFailoverRecoveryReplaySweep/large_total_4608-8`: ~`4,210,120–4,311,958 ns/op`, `23,739,048–24,131,616 B/op`, `161,452–161,466 allocs/op`

## Notes

- This baseline is deterministic in workload shape and command path.
- Values are hardware/OS dependent and serve as a regression reference, not cross-machine SLA numbers.
- The restart/replay numbers above are useful internal evidence but are not closure-grade AB evidence yet.
- Current restart-path interpretation:
	- replay-to-LSN is now benchmarked with a stable repeated sample around ~`2.0 ms/op` on this fixture;
	- skipping snapshot capture/persistence/eviction during bounded `ReplayToLSN` did not materially move wall-clock time on the current fixture, but it did cut replay allocation volume from roughly ~`2.69 MB/op` and ~`16.7k allocs/op` to ~`1.41 MB/op` and ~`12.1k allocs/op`;
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
	- the first direct full-snapshot decode experiment using `decodeSnapshotsBinary()` was not compelling, but a later decoder that loads single-file full snapshots directly into runtime state does materially improve the measured hot path: base `BenchmarkEngineReadPersistedSnapshotsFromDir` is now ~`175.7 µs/op` with `609 allocs/op`, and the indexed variant is ~`304.1 µs/op` with `1,174 allocs/op`.
	- the latest spot check now overlaps independent per-file read/decompress/decode work before the deterministic in-order merge in `readAllSnapshotsFromDir()`, and on the current two-file fixture that moved `BenchmarkEngineReadPersistedSnapshotsFromDir` from about ~`407 µs/op` to ~`343 µs/op` and `BenchmarkEngineReadPersistedSnapshotsFromDirIndexed` from about ~`564 µs/op` to ~`504 µs/op` at `-benchmem -benchtime=100ms -count=1`.
	- a follow-up restore-path change now preserves decoded btree entries in restore-ready `indexEntry` form instead of copying them again during `marshalableToTableState`; the base fixture stayed roughly flat, but the indexed snapshot-directory load improved its heap profile from about ~`842 KB/op` to ~`746 KB/op` while landing around ~`499 µs/op`, which suggests the remaining indexed restart cost is no longer dominated by btree-entry rematerialization.
	- another follow-up now reuses snapshot domain state directly during `restoreSnapshot()` / `replayFromSnapshots()` while still cloning the catalog for mutation safety; focused regression coverage confirms post-restore writes do not mutate the captured snapshot, and `BenchmarkEngineReplayFromPersistedSnapshots` fell to roughly ~`690 ns/op`, `640 B/op`, `8 allocs/op` on the current fixture, making the in-memory restore step effectively negligible compared with snapshot-directory read/decompress/decode/materialization.
	- a fresher repeated restart comparison after the direct runtime-state decoder keeps the same interpretation but at a much lower cost level: persisted-snapshot restart is now ~`2.58–2.73 ms/op` versus ~`1.98–2.41 ms/op` for replay-only, while still using far less heap (~`638–659 KB/op` vs ~`1.72 MB/op`) and far fewer allocations (`~2.4k` vs `12.8k`).
	- the new `-benchtime=1x` sweep results keep the policy crossover intact after the decoder refactor: persisted snapshots still lose on the small `total_1000` / short-tail cases, but they remain clearly better on the medium cadence point (`total_10500`) and now edge ahead again on the large `total_50500` spot check. Because those runs are still single-iteration samples, they are directionally useful but not yet closure-grade by themselves.
	- a fresh representative sweep check after the same changes keeps the broader restart interpretation stable: with a fixed 500-record snapshot anchor and a long 5k replay tail, persisted snapshot restart still loses (~`50.4 ms/op`, ~`38.7 MB/op`, ~`229.7k allocs/op`) to replay-only (~`39.3 ms/op`, ~`33.8 MB/op`, ~`206.0k allocs/op`), while the medium cadence case that replays only the last ~`500` records at `total_10500` still wins clearly (~`61.3 ms/op`, ~`34.6 MB/op`, ~`196.2k allocs/op` vs ~`94.5 ms/op`, ~`62.1 MB/op`, ~`361.3k allocs/op`).
	- the positional-row decode change clearly improved isolated decode and snapshot-directory load benchmarks, but the end-to-end persisted-restart timing is still noisy on the current short benchtime harness and needs repeated confirmation before any closure claim.
- Current failover/recovery interpretation:
	- the benchmark suite now includes both failover promotion and multi-scenario recovery replay, so Epic AB is no longer missing failover/recovery coverage at the benchmark-suite level;
	- moving correctness validation outside the timed region tightened the baseline so the recovery benchmark now measures reopen + replay more directly instead of mixing in post-recovery query validation;
	- recovery replay currently scales in the expected direction across the `small_total_40`, `medium_total_640`, and `large_total_4608` scenarios, and the large-case phase split shows the cost is shared between WAL reopen/discovery and engine replay/apply, with replay/apply still the larger slice even after the latest cleanup;
	- the latest replay hot-path cleanup was intentionally narrow: replay now skips entity-tracking row materialization on tables that do not participate in any entity and avoids per-insert mutation perf timing during WAL replay; on the large-case phase split that cut the replay/apply slice from roughly ~`23.0–26.3 ms` / ~`170.6k allocs` to ~`19.8–24.5 ms` / ~`161.4k allocs`;
	- a follow-up WAL reopen optimization now persists compact segment headers for clean uncompressed segments and trusts them on reopen, collapsing the large-case `store_reopen` slice from roughly ~`13.5–21.9 ms` to ~`38–73 µs`; that shifts the failover recovery bottleneck decisively into engine replay/apply;
	- closure decision: this failover-recovery slice is now strong enough to treat as closed for Epic AB. The benchmark suite is broad enough, the bottleneck was identified, two targeted optimizations landed, and the largest recovery scenario now replays end-to-end in roughly ~`2.1–4.3 ms` on the current fixture. Further work can be deferred unless a new higher-volume or more realistic failover workload shows fresh adoption friction.
- Current read-path interpretation:
	- on the simple covered ordered-read shape, `btree-index-only` is about $10\times$ faster than `btree-order` and materially reduces allocations;
	- adding `OFFSET` to the covered ordered-read shape still keeps `btree-index-only` comfortably fast (about $1.7\times$ slower than the zero-offset variant, but still far below row-fetch ordered reads);
	- on the selective covered shape, adding bounded early-stop to the index-only path moved it ahead of `btree-order` as well (~`274 µs/op` vs ~`406 µs/op`), while keeping allocations materially lower.
	- on the composite covered ordered-read shape, enabling composite index-only coverage moved the path to ~`36 µs/op` versus ~`76–78 µs/op` for the non-covered row-fetch path, with materially lower allocations.
	- on the user-visible “entity + related tables” shape, the new scaling benchmarks now keep the indexed path in the ~`23–27 µs/op` range at `orders_10000`, while the optimized unindexed path lands around ~`1.8–1.9 ms/op`; that keeps the index-vs-no-index gap explicit, but it also confirms the join executor no longer pays the old broader materialization cost once root pruning and safe right-side filtering kick in.
	- the unindexed related-read + right-side filter shape now stays around ~`1.87 ms/op` at `orders_10000`, which is consistent with the executor changes that prefilter root/right rows before full join expansion.
	- repeated exact-`LSN` historical reads were a real bottleneck before caching: the new `BenchmarkEngineReadHistoricalAsOfLSNScaling` initially measured about ~`2.10 ms/op`, ~`1.19 MB/op`, ~`8.1k allocs/op` for `rows_1000` and ~`8.57 ms/op`, ~`12.09 MB/op`, ~`80k allocs/op` for `rows_10000`.
	- after adding a head-`LSN`-keyed WAL-record cache plus a small exact-`LSN` historical-state cache, those repeated historical reads improved to about ~`0.59 ms/op`, ~`373 KB/op`, ~`2.7k allocs/op` for `rows_1000` and ~`3.12 ms/op`, ~`3.79 MB/op`, ~`26.8k allocs/op` for `rows_10000`.
	- that historical improvement is intentionally narrow and correctness-explicit: caches are invalidated on commit and WAL GC, so the current win is for repeated reads at the same target `LSN`, not yet for arbitrary neighboring historical windows.