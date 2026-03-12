package executor

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	latencyRingSize     = 1024
	throughputWindowSec = 60
)

// PerfStatsSnapshot is the exported immutable snapshot of engine performance metrics.
type PerfStatsSnapshot struct {
	TotalCommits           uint64 `json:"total_commits"`
	TotalReads             uint64 `json:"total_reads"`
	TotalRollbacks         uint64 `json:"total_rollbacks"`
	TotalBegins            uint64 `json:"total_begins"`
	TotalTimeTravelQueries uint64 `json:"total_time_travel_queries"`
	TotalSnapshots         uint64 `json:"total_snapshots"`
	TotalReplays           uint64 `json:"total_replays"`
	TotalFsyncErrors       uint64 `json:"total_fsync_errors"`
	TotalAuditErrors       uint64 `json:"total_audit_errors"`
	ActiveTransactions     int64  `json:"active_transactions"`
	CommitBatchCount       uint64 `json:"commit_batch_count"`
	CommitBatchMaxJobs     uint64 `json:"commit_batch_max_jobs"`
	CommitBatchMaxWalRecords uint64 `json:"commit_batch_max_wal_records"`

	CommitLatencyP50 float64 `json:"commit_latency_p50_ms"`
	CommitLatencyP95 float64 `json:"commit_latency_p95_ms"`
	CommitLatencyP99 float64 `json:"commit_latency_p99_ms"`
	CommitQueueWaitP50 float64 `json:"commit_queue_wait_p50_ms"`
	CommitQueueWaitP95 float64 `json:"commit_queue_wait_p95_ms"`
	CommitQueueWaitP99 float64 `json:"commit_queue_wait_p99_ms"`
	CommitWriteHoldP50 float64 `json:"commit_write_hold_p50_ms"`
	CommitWriteHoldP95 float64 `json:"commit_write_hold_p95_ms"`
	CommitWriteHoldP99 float64 `json:"commit_write_hold_p99_ms"`
	CommitApplyP50 float64 `json:"commit_apply_p50_ms"`
	CommitApplyP95 float64 `json:"commit_apply_p95_ms"`
	CommitApplyP99 float64 `json:"commit_apply_p99_ms"`
	ReadLatencyP50   float64 `json:"read_latency_p50_ms"`
	ReadLatencyP95   float64 `json:"read_latency_p95_ms"`
	ReadLatencyP99   float64 `json:"read_latency_p99_ms"`

	TimeTravelLatencyP50 float64 `json:"time_travel_latency_p50_ms"`
	TimeTravelLatencyP95 float64 `json:"time_travel_latency_p95_ms"`
	TimeTravelLatencyP99 float64 `json:"time_travel_latency_p99_ms"`
	FsyncLatencyP50      float64 `json:"fsync_latency_p50_ms"`
	FsyncLatencyP95      float64 `json:"fsync_latency_p95_ms"`
	FsyncLatencyP99      float64 `json:"fsync_latency_p99_ms"`
	ReplayDurationMs     float64 `json:"replay_duration_ms"`
	SnapshotDurationMs   float64 `json:"snapshot_duration_ms"`

	CommitThroughput float64 `json:"commit_throughput_per_sec"`
	ReadThroughput   float64 `json:"read_throughput_per_sec"`
	CommitBatchAvgJobs float64 `json:"commit_batch_avg_jobs"`
	CommitBatchAvgWalRecords float64 `json:"commit_batch_avg_wal_records"`

	WALFileSize      int64 `json:"wal_file_size_bytes"`
	SnapshotFileSize int64 `json:"snapshot_file_size_bytes"`
	AuditFileSize    int64 `json:"audit_file_size_bytes"`
}

// latencyRing is a fixed-size circular buffer for latency samples.
// All durations are stored in milliseconds.
type latencyRing struct {
	samples [latencyRingSize]float64
	pos     int
	count   int
}

func (r *latencyRing) record(d time.Duration) {
	r.samples[r.pos] = float64(d.Microseconds()) / 1000.0
	r.pos = (r.pos + 1) % latencyRingSize
	if r.count < latencyRingSize {
		r.count++
	}
}

func (r *latencyRing) percentiles() (p50, p95, p99 float64) {
	if r.count == 0 {
		return 0, 0, 0
	}
	buf := make([]float64, r.count)
	copy(buf, r.samples[:r.count])
	sort.Float64s(buf)

	p50 = buf[percentileIndex(r.count, 50)]
	p95 = buf[percentileIndex(r.count, 95)]
	p99 = buf[percentileIndex(r.count, 99)]
	return
}

func percentileIndex(count, percentile int) int {
	idx := count * percentile / 100
	if idx >= count {
		idx = count - 1
	}
	return idx
}

// throughputWindow tracks event counts over a rolling window with 1-second granularity.
type throughputWindow struct {
	buckets  [throughputWindowSec]uint64
	baseTime int64 // unix seconds of bucket[0]
}

func (w *throughputWindow) increment() {
	now := time.Now().Unix()
	if w.baseTime == 0 {
		w.baseTime = now
	}
	offset := int(now - w.baseTime)
	if offset < 0 {
		return
	}
	if offset >= throughputWindowSec {
		shift := offset - throughputWindowSec + 1
		if shift >= throughputWindowSec {
			w.buckets = [throughputWindowSec]uint64{}
		} else {
			copy(w.buckets[:], w.buckets[shift:])
			for i := throughputWindowSec - shift; i < throughputWindowSec; i++ {
				w.buckets[i] = 0
			}
		}
		w.baseTime += int64(shift)
		offset = int(now - w.baseTime)
	}
	if offset >= 0 && offset < throughputWindowSec {
		w.buckets[offset]++
	}
}

func (w *throughputWindow) rate() float64 {
	now := time.Now().Unix()
	if w.baseTime == 0 {
		return 0
	}
	var total uint64
	for i := 0; i < throughputWindowSec; i++ {
		bucketTime := w.baseTime + int64(i)
		if bucketTime > now {
			break
		}
		if now-bucketTime >= int64(throughputWindowSec) {
			continue
		}
		total += w.buckets[i]
	}
	return float64(total) / float64(throughputWindowSec)
}

// perfStats holds all engine performance counters, latency rings, and throughput windows.
type perfStats struct {
	mu sync.Mutex

	totalCommits           uint64
	totalReads             uint64
	totalRollbacks         uint64
	totalBegins            uint64
	totalTimeTravelQueries uint64
	activeTransactions     int64

	commitLatency     latencyRing
	commitQueueWait   latencyRing
	commitWriteHold   latencyRing
	commitApply       latencyRing
	readLatency       latencyRing
	timeTravelLatency latencyRing
	fsyncLatency      latencyRing

	commitThroughput throughputWindow
	readThroughput   throughputWindow

	commitBatchCount           uint64
	totalCommitBatchJobs       uint64
	maxCommitBatchJobs         uint64
	totalCommitBatchWalRecords uint64
	maxCommitBatchWalRecords   uint64
	totalSnapshots             uint64
	totalReplays               uint64
	totalFsyncErrors           uint64
	totalAuditErrors           uint64
	lastReplayDurationMs       float64
	lastSnapshotDurationMs     float64
}

func newPerfStats() *perfStats {
	return &perfStats{}
}

func (p *perfStats) recordCommit(d time.Duration) {
	p.mu.Lock()
	p.totalCommits++
	p.commitLatency.record(d)
	p.commitThroughput.increment()
	p.mu.Unlock()
}

func (p *perfStats) recordCommitBatch(jobCount, walRecordCount int, maxQueueWait, writeHold, applyDur time.Duration) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.commitBatchCount++
	p.totalCommitBatchJobs += uint64(jobCount)
	if uint64(jobCount) > p.maxCommitBatchJobs {
		p.maxCommitBatchJobs = uint64(jobCount)
	}
	p.totalCommitBatchWalRecords += uint64(walRecordCount)
	if uint64(walRecordCount) > p.maxCommitBatchWalRecords {
		p.maxCommitBatchWalRecords = uint64(walRecordCount)
	}
	p.commitQueueWait.record(maxQueueWait)
	p.commitWriteHold.record(writeHold)
	p.commitApply.record(applyDur)

	return p.commitBatchCount
}

func (p *perfStats) recordRead(d time.Duration) {
	p.mu.Lock()
	p.totalReads++
	p.readLatency.record(d)
	p.readThroughput.increment()
	p.mu.Unlock()
}

func (p *perfStats) recordTimeTravel(d time.Duration) {
	p.mu.Lock()
	p.totalTimeTravelQueries++
	p.timeTravelLatency.record(d)
	p.mu.Unlock()
}

func (p *perfStats) recordFsync(d time.Duration, err error) {
	p.mu.Lock()
	p.fsyncLatency.record(d)
	if err != nil {
		p.totalFsyncErrors++
	}
	p.mu.Unlock()
}

func (p *perfStats) recordReplay(d time.Duration) {
	p.mu.Lock()
	p.totalReplays++
	p.lastReplayDurationMs = float64(d.Microseconds()) / 1000.0
	p.mu.Unlock()
}

func (p *perfStats) recordSnapshotPersist(d time.Duration) {
	p.mu.Lock()
	p.totalSnapshots++
	p.lastSnapshotDurationMs = float64(d.Microseconds()) / 1000.0
	p.mu.Unlock()
}

func (p *perfStats) recordAuditError() {
	p.mu.Lock()
	p.totalAuditErrors++
	p.mu.Unlock()
}

func (p *perfStats) recordBegin() {
	p.mu.Lock()
	p.totalBegins++
	p.mu.Unlock()
	atomic.AddInt64(&p.activeTransactions, 1)
}

func (p *perfStats) recordEndTx() {
	atomic.AddInt64(&p.activeTransactions, -1)
}

func (p *perfStats) recordRollback() {
	p.mu.Lock()
	p.totalRollbacks++
	p.mu.Unlock()
	p.recordEndTx()
}

func (p *perfStats) snapshot() PerfStatsSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	cp50, cp95, cp99 := p.commitLatency.percentiles()
	cqp50, cqp95, cqp99 := p.commitQueueWait.percentiles()
	cwp50, cwp95, cwp99 := p.commitWriteHold.percentiles()
	cap50, cap95, cap99 := p.commitApply.percentiles()
	rp50, rp95, rp99 := p.readLatency.percentiles()
	tp50, tp95, tp99 := p.timeTravelLatency.percentiles()
	fp50, fp95, fp99 := p.fsyncLatency.percentiles()
	avgBatchJobs := 0.0
	avgBatchWalRecords := 0.0
	if p.commitBatchCount > 0 {
		avgBatchJobs = float64(p.totalCommitBatchJobs) / float64(p.commitBatchCount)
		avgBatchWalRecords = float64(p.totalCommitBatchWalRecords) / float64(p.commitBatchCount)
	}

	return PerfStatsSnapshot{
		TotalCommits:           p.totalCommits,
		TotalReads:             p.totalReads,
		TotalRollbacks:         p.totalRollbacks,
		TotalBegins:            p.totalBegins,
		TotalTimeTravelQueries: p.totalTimeTravelQueries,
		TotalSnapshots:         p.totalSnapshots,
		TotalReplays:           p.totalReplays,
		TotalFsyncErrors:       p.totalFsyncErrors,
		TotalAuditErrors:       p.totalAuditErrors,
		ActiveTransactions:     atomic.LoadInt64(&p.activeTransactions),
		CommitBatchCount:       p.commitBatchCount,
		CommitBatchMaxJobs:     p.maxCommitBatchJobs,
		CommitBatchMaxWalRecords: p.maxCommitBatchWalRecords,

		CommitLatencyP50: cp50,
		CommitLatencyP95: cp95,
		CommitLatencyP99: cp99,
		CommitQueueWaitP50: cqp50,
		CommitQueueWaitP95: cqp95,
		CommitQueueWaitP99: cqp99,
		CommitWriteHoldP50: cwp50,
		CommitWriteHoldP95: cwp95,
		CommitWriteHoldP99: cwp99,
		CommitApplyP50: cap50,
		CommitApplyP95: cap95,
		CommitApplyP99: cap99,
		ReadLatencyP50:   rp50,
		ReadLatencyP95:   rp95,
		ReadLatencyP99:   rp99,

		TimeTravelLatencyP50: tp50,
		TimeTravelLatencyP95: tp95,
		TimeTravelLatencyP99: tp99,
		FsyncLatencyP50:      fp50,
		FsyncLatencyP95:      fp95,
		FsyncLatencyP99:      fp99,
		ReplayDurationMs:     p.lastReplayDurationMs,
		SnapshotDurationMs:   p.lastSnapshotDurationMs,

		CommitThroughput: p.commitThroughput.rate(),
		ReadThroughput:   p.readThroughput.rate(),
		CommitBatchAvgJobs: avgBatchJobs,
		CommitBatchAvgWalRecords: avgBatchWalRecords,
	}
}
