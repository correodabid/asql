package executor

import "testing"

func TestCommitQueueCoalesceAddsPendingJobs(t *testing.T) {
	first := &commitJob{}
	second := &commitJob{}
	third := &commitJob{}
	cq := &commitQueue{jobs: []*commitJob{second, third}}

	jobs := cq.coalesce([]*commitJob{first})
	if len(jobs) != 3 {
		t.Fatalf("coalesced jobs = %d, want 3", len(jobs))
	}
	if jobs[0] != first || jobs[1] != second || jobs[2] != third {
		t.Fatalf("coalesced jobs order mismatch")
	}
	if len(cq.jobs) != 0 {
		t.Fatalf("pending jobs left = %d, want 0", len(cq.jobs))
	}
}

func TestCommitQueueCoalesceRespectsBatchLimit(t *testing.T) {
	base := make([]*commitJob, commitQueueMaxBatchJobs-1)
	for i := range base {
		base[i] = &commitJob{}
	}
	extraA := &commitJob{}
	extraB := &commitJob{}
	cq := &commitQueue{jobs: []*commitJob{extraA, extraB}}

	jobs := cq.coalesce(base)
	if len(jobs) != commitQueueMaxBatchJobs {
		t.Fatalf("coalesced jobs = %d, want %d", len(jobs), commitQueueMaxBatchJobs)
	}
	if jobs[len(jobs)-1] != extraA {
		t.Fatalf("expected first extra job to be appended at the batch limit")
	}
	if len(cq.jobs) != 1 || cq.jobs[0] != extraB {
		t.Fatalf("expected one pending job to remain after hitting batch limit")
	}
}
