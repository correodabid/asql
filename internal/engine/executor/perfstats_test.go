package executor

import (
	"testing"
	"time"
)

func TestLatencyRing_Empty(t *testing.T) {
	var ring latencyRing
	p50, p95, p99 := ring.percentiles()
	if p50 != 0 || p95 != 0 || p99 != 0 {
		t.Fatalf("empty ring should return 0 percentiles, got p50=%f p95=%f p99=%f", p50, p95, p99)
	}
}

func TestLatencyRing_SingleSample(t *testing.T) {
	var ring latencyRing
	ring.record(10 * time.Millisecond)

	p50, p95, p99 := ring.percentiles()
	if p50 != 10 || p95 != 10 || p99 != 10 {
		t.Fatalf("single sample ring should return 10ms for all percentiles, got p50=%f p95=%f p99=%f", p50, p95, p99)
	}
}

func TestLatencyRing_OrderedPercentiles(t *testing.T) {
	var ring latencyRing
	for i := 1; i <= 100; i++ {
		ring.record(time.Duration(i) * time.Millisecond)
	}

	p50, p95, p99 := ring.percentiles()

	if p50 >= p95 {
		t.Fatalf("p50 (%f) should be less than p95 (%f)", p50, p95)
	}
	if p95 >= p99 {
		t.Fatalf("p95 (%f) should be less than p99 (%f)", p95, p99)
	}

	// With 100 samples 1..100ms: p50 ~ 50ms, p95 ~ 95ms, p99 ~ 99ms
	if p50 < 49 || p50 > 51 {
		t.Fatalf("p50 should be around 50ms, got %f", p50)
	}
	if p95 < 94 || p95 > 96 {
		t.Fatalf("p95 should be around 95ms, got %f", p95)
	}
	if p99 < 98 || p99 > 100 {
		t.Fatalf("p99 should be around 99ms, got %f", p99)
	}
}

func TestLatencyRing_CircularOverwrite(t *testing.T) {
	var ring latencyRing

	// Fill with 1ms samples
	for i := 0; i < latencyRingSize; i++ {
		ring.record(1 * time.Millisecond)
	}

	// Overwrite all with 100ms samples
	for i := 0; i < latencyRingSize; i++ {
		ring.record(100 * time.Millisecond)
	}

	p50, _, _ := ring.percentiles()
	if p50 != 100 {
		t.Fatalf("after full overwrite, p50 should be 100ms, got %f", p50)
	}
	if ring.count != latencyRingSize {
		t.Fatalf("count should be capped at %d, got %d", latencyRingSize, ring.count)
	}
}

func TestThroughputWindow_Empty(t *testing.T) {
	var w throughputWindow
	rate := w.rate()
	if rate != 0 {
		t.Fatalf("empty window should return 0 rate, got %f", rate)
	}
}

func TestThroughputWindow_SingleIncrement(t *testing.T) {
	var w throughputWindow
	w.increment()

	rate := w.rate()
	expected := 1.0 / float64(throughputWindowSec)
	if rate < expected-0.01 || rate > expected+0.01 {
		t.Fatalf("single increment rate should be ~%f, got %f", expected, rate)
	}
}

func TestThroughputWindow_MultipleIncrements(t *testing.T) {
	var w throughputWindow
	for i := 0; i < 60; i++ {
		w.increment()
	}

	rate := w.rate()
	// All 60 increments in same second bucket, rate = 60/60 = 1.0
	expected := 60.0 / float64(throughputWindowSec)
	if rate < expected-0.1 || rate > expected+0.1 {
		t.Fatalf("60 increments rate should be ~%f, got %f", expected, rate)
	}
}

func TestPercentileIndex(t *testing.T) {
	tests := []struct {
		count      int
		percentile int
		expected   int
	}{
		{100, 50, 50},
		{100, 95, 95},
		{100, 99, 99},
		{1, 50, 0},
		{1, 99, 0},
		{10, 50, 5},
		{10, 95, 9},
		{10, 99, 9},
	}

	for _, tc := range tests {
		idx := percentileIndex(tc.count, tc.percentile)
		if idx != tc.expected {
			t.Errorf("percentileIndex(%d, %d) = %d, want %d", tc.count, tc.percentile, idx, tc.expected)
		}
	}
}

func TestPerfStats_RecordCommit(t *testing.T) {
	ps := newPerfStats()
	ps.recordBegin()
	ps.recordCommit(5 * time.Millisecond)
	ps.recordEndTx()

	snap := ps.snapshot()

	if snap.TotalCommits != 1 {
		t.Fatalf("expected 1 commit, got %d", snap.TotalCommits)
	}
	if snap.TotalBegins != 1 {
		t.Fatalf("expected 1 begin, got %d", snap.TotalBegins)
	}
	if snap.ActiveTransactions != 0 {
		t.Fatalf("expected 0 active tx, got %d", snap.ActiveTransactions)
	}
	if snap.CommitLatencyP50 == 0 {
		t.Fatal("expected non-zero commit latency p50")
	}
}

func TestPerfStats_RecordRead(t *testing.T) {
	ps := newPerfStats()
	ps.recordRead(2 * time.Millisecond)
	ps.recordRead(4 * time.Millisecond)

	snap := ps.snapshot()

	if snap.TotalReads != 2 {
		t.Fatalf("expected 2 reads, got %d", snap.TotalReads)
	}
	if snap.ReadLatencyP50 == 0 {
		t.Fatal("expected non-zero read latency p50")
	}
}

func TestPerfStats_RecordRollback(t *testing.T) {
	ps := newPerfStats()
	ps.recordBegin()
	ps.recordRollback()

	snap := ps.snapshot()

	if snap.TotalBegins != 1 {
		t.Fatalf("expected 1 begin, got %d", snap.TotalBegins)
	}
	if snap.TotalRollbacks != 1 {
		t.Fatalf("expected 1 rollback, got %d", snap.TotalRollbacks)
	}
	if snap.ActiveTransactions != 0 {
		t.Fatalf("expected 0 active tx after rollback, got %d", snap.ActiveTransactions)
	}
}

func TestPerfStats_RecordTimeTravel(t *testing.T) {
	ps := newPerfStats()
	ps.recordTimeTravel(10 * time.Millisecond)

	snap := ps.snapshot()

	if snap.TotalTimeTravelQueries != 1 {
		t.Fatalf("expected 1 time travel query, got %d", snap.TotalTimeTravelQueries)
	}
	if snap.TimeTravelLatencyP50 == 0 {
		t.Fatal("expected non-zero time travel latency p50")
	}
}

func TestPerfStats_ActiveTransactions(t *testing.T) {
	ps := newPerfStats()

	ps.recordBegin()
	ps.recordBegin()
	ps.recordBegin()

	snap := ps.snapshot()
	if snap.ActiveTransactions != 3 {
		t.Fatalf("expected 3 active tx, got %d", snap.ActiveTransactions)
	}

	ps.recordEndTx()
	ps.recordRollback()

	snap = ps.snapshot()
	if snap.ActiveTransactions != 1 {
		t.Fatalf("expected 1 active tx, got %d", snap.ActiveTransactions)
	}
}

func TestPerfStats_SnapshotIsolation(t *testing.T) {
	ps := newPerfStats()
	ps.recordCommit(5 * time.Millisecond)

	snap1 := ps.snapshot()

	ps.recordCommit(10 * time.Millisecond)

	snap2 := ps.snapshot()

	if snap1.TotalCommits != 1 {
		t.Fatalf("snap1 should have 1 commit, got %d", snap1.TotalCommits)
	}
	if snap2.TotalCommits != 2 {
		t.Fatalf("snap2 should have 2 commits, got %d", snap2.TotalCommits)
	}
}

func TestPerfStats_RecordCommitBatch(t *testing.T) {
	ps := newPerfStats()
	count := ps.recordCommitBatch(3, 11, 7*time.Millisecond, 9*time.Millisecond, 12*time.Millisecond)
	if count != 1 {
		t.Fatalf("expected batch count 1, got %d", count)
	}

	snap := ps.snapshot()
	if snap.CommitBatchCount != 1 {
		t.Fatalf("expected 1 commit batch, got %d", snap.CommitBatchCount)
	}
	if snap.CommitBatchAvgJobs != 3 {
		t.Fatalf("expected avg jobs 3, got %f", snap.CommitBatchAvgJobs)
	}
	if snap.CommitBatchMaxJobs != 3 {
		t.Fatalf("expected max jobs 3, got %d", snap.CommitBatchMaxJobs)
	}
	if snap.CommitBatchAvgWalRecords != 11 {
		t.Fatalf("expected avg wal records 11, got %f", snap.CommitBatchAvgWalRecords)
	}
	if snap.CommitBatchMaxWalRecords != 11 {
		t.Fatalf("expected max wal records 11, got %d", snap.CommitBatchMaxWalRecords)
	}
	if snap.CommitQueueWaitP50 == 0 || snap.CommitWriteHoldP50 == 0 || snap.CommitApplyP50 == 0 {
		t.Fatal("expected non-zero commit batch latency metrics")
	}
}
