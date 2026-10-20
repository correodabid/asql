package determinism_test

import (
	"context"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

// requireUniqueTxIDs fails if two transactions in records share a TxID.
func requireUniqueTxIDs(t *testing.T, label string, records []ports.WALRecord) {
	t.Helper()
	seen := make(map[string]uint64)
	for _, r := range records {
		if r.Type != "BEGIN" {
			continue
		}
		if lsn, dup := seen[r.TxID]; dup {
			t.Fatalf("%s: %s begins at LSN %d and again at LSN %d", label, r.TxID, lsn, r.LSN)
		}
		seen[r.TxID] = r.LSN
	}
	if len(seen) == 0 {
		t.Fatalf("%s: no transactions", label)
	}
}

// TestTxIDsAreUniqueAcrossRestarts: a TxID names one transaction for the
// life of the WAL, whether the engine restarts from a checkpoint with no WAL
// after it, from the WAL alone, or after a crash left a transaction without
// its COMMIT.
func TestTxIDsAreUniqueAcrossRestarts(t *testing.T) {
	ctx := context.Background()
	step := 0
	n, _ := runWorkload(t, t.TempDir(), nodeOptions{}, func(n *node) {
		step++
		switch step {
		case 1:
			// The first restart checkpoints the head, so the second starts
			// from a snapshot with no WAL after it.
			n.restart()
			n.restart()
		case 2:
			n.crash()
			n.open()
		default:
			n.close()
			orphan := uncommittedRecords(t, n, orphanTx)
			n.open()
			if err := n.store.AppendReplicatedBatch(ctx, orphan); err != nil {
				t.Fatal(err)
			}
			n.crash()
			n.open()
		}
	})
	defer n.close()
	if step < 3 {
		t.Fatalf("the workload has %d phase boundaries, want at least 3", step)
	}
	requireUniqueTxIDs(t, "after restarts", n.records())
}

// TestTxIDsAreUniqueAcrossLeaders: a follower promoted to leader resumes
// after the TxIDs it applied.
func TestTxIDsAreUniqueAcrossLeaders(t *testing.T) {
	ctx := context.Background()
	phases := workloadPhases()
	leader := openNode(t, t.TempDir(), nodeOptions{})
	for _, phase := range phases[:2] {
		leader.run(phase...)
	}
	records := leader.records()
	leader.close()

	follower := openNode(t, t.TempDir(), nodeOptions{})
	defer follower.close()
	if err := follower.store.AppendReplicatedBatch(ctx, records); err != nil {
		t.Fatal(err)
	}
	if err := follower.engine.ApplyCommittedRecords(ctx, records[len(records)-1].LSN, records); err != nil {
		t.Fatal(err)
	}
	for _, phase := range phases[2:] {
		follower.run(phase...)
	}
	requireUniqueTxIDs(t, "after a change of leader", follower.records())
}
