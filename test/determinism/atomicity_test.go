package determinism_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/ports"
)

// orphanTx touches rows that the rest of the workload reads, so any of its
// mutations leaking into replay shows up in the fingerprint.
var orphanTx = tx{domains: []string{"inventory"}, statements: []string{
	"UPDATE items SET qty = 999 WHERE id = 1",
	"INSERT INTO items (id, sku, qty) VALUES (100, 'SKU-100', 1)",
	"INSERT INTO item_moves (item_id, delta) VALUES (1, 77)",
}}

// uncommittedRecords returns the WAL records that txn writes on top of n's
// closed data directory, without its COMMIT: a transaction whose batch a
// crash cut short, or whose COMMIT a new Raft leader replaced.
func uncommittedRecords(t *testing.T, n *node, txn tx) []ports.WALRecord {
	t.Helper()
	dir := t.TempDir()
	for _, sub := range []string{"wal", "audit", "snap"} {
		copyDir(t, filepath.Join(n.dir, sub), filepath.Join(dir, sub))
	}
	c := openNode(t, dir, nodeOptions{})
	head := c.engine.HeadLSN()
	c.run(txn)
	var records []ports.WALRecord
	for _, r := range c.records() {
		if r.LSN > head {
			records = append(records, r)
		}
	}
	c.close()
	if len(records) < 3 || records[len(records)-1].Type != "COMMIT" {
		t.Fatalf("unexpected transaction records: %+v", records)
	}
	return records[:len(records)-1]
}

// TestUncommittedTransactionIsNeverApplied: a transaction whose COMMIT never
// reached the WAL must stay invisible on every path, both while it is the
// WAL's tail and once later transactions follow it. The replay paths also
// read the WAL as engines that predate unique TxIDs wrote it, where the
// next transaction reused the orphan's TxID.
func TestUncommittedTransactionIsNeverApplied(t *testing.T) {
	ctx := context.Background()
	phases := workloadPhases()
	n := openNode(t, t.TempDir(), nodeOptions{})
	n.run(phases[0]...)
	n.run(phases[1]...)
	before := n.fingerprint(workloadProbes)
	n.close()

	orphan := uncommittedRecords(t, n, orphanTx)
	n.open()
	if err := n.store.AppendReplicatedBatch(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	n.crash()
	n.open()
	// The head moves past the orphan: later commits are numbered after it.
	isHead := func(section string) bool { return section == "head" }
	requireSame(t, "replay with an uncommitted tail", before.without(isHead), n.fingerprint(workloadProbes).without(isHead))
	if got, want := n.engine.HeadLSN(), orphan[len(orphan)-1].LSN; got != want {
		t.Fatalf("head after replay = %d, want the WAL's last LSN %d", got, want)
	}

	for _, phase := range phases[2:] {
		n.run(phase...)
	}
	live := n.fingerprint(workloadProbes)
	res, err := n.engine.Query(ctx, "SELECT id FROM items WHERE id = 100 OR qty = 999", []string{"inventory"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("uncommitted rows are visible: %v", res.Rows)
	}

	n.restart()
	requireSame(t, "restart after an uncommitted transaction", live, n.fingerprint(workloadProbes))
	records := legacyTxIDs(n.records())
	n.close()
	// The timeline shows the renumbered TxIDs; every commit is still checked
	// by the as-of-LSN sections.
	isTimeline := func(section string) bool { return section == "timeline" }
	live = live.without(isTimeline)

	fresh := openNode(t, t.TempDir(), nodeOptions{})
	if err := fresh.store.AppendReplicatedBatch(ctx, records); err != nil {
		t.Fatal(err)
	}
	fresh.restart()
	requireSame(t, "fresh replay after an uncommitted transaction", live, fresh.fingerprint(workloadProbes).without(isTimeline))
	fresh.close()

	follower := openNode(t, t.TempDir(), nodeOptions{})
	defer follower.close()
	for i := 0; i < len(records); i += 5 {
		batch := records[i:min(len(records), i+5)]
		if err := follower.store.AppendReplicatedBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
		if err := follower.engine.ApplyCommittedRecords(ctx, batch[len(batch)-1].LSN, batch); err != nil {
			t.Fatal(err)
		}
	}
	requireSame(t, "follower after an uncommitted transaction", live, follower.fingerprint(workloadProbes).without(isTimeline))
}

// legacyTxIDs renumbers records' TxIDs the way engines that restarted their
// TxID counter on every start wrote them: IDs repeat, and the transaction
// after an unfinished one takes its TxID.
func legacyTxIDs(records []ports.WALRecord) []ports.WALRecord {
	out := append([]ports.WALRecord(nil), records...)
	num, open := 0, false
	for i := range out {
		if out[i].Type == "BEGIN" {
			if !open {
				num = num%3 + 1
			}
			open = true
		}
		if out[i].Type != "SECURITY" {
			out[i].TxID = fmt.Sprintf("tx-%d", num)
		}
		if out[i].Type == "COMMIT" {
			open = false
		}
	}
	return out
}

// TestReplayLimitInsideTransactionDefersIt: in cluster mode startup replay
// stops at the last durable commit index, which can fall inside a
// transaction. The transaction must be deferred whole and applied once Raft
// delivers the rest.
func TestReplayLimitInsideTransactionDefersIt(t *testing.T) {
	ctx := context.Background()
	leader, live := runWorkload(t, t.TempDir(), nodeOptions{}, nil)
	records := leader.records()
	leader.close()

	cuts := 0
	for i := 1; i+1 < len(records); i++ {
		if records[i].Type != "MUTATION" || records[i+1].Type == "BEGIN" || i%7 != 0 {
			continue
		}
		cuts++
		limit := records[i].LSN
		dir := t.TempDir()
		copyDir(t, filepath.Dir(leader.walPath), filepath.Join(dir, "wal"))
		follower := openNode(t, dir, nodeOptions{engine: []executor.EngineOption{executor.WithReplayLimit(limit)}})
		if head := follower.engine.HeadLSN(); head >= limit {
			t.Fatalf("limit %d: head %d is inside the cut transaction", limit, head)
		}
		if err := follower.engine.CatchUp(ctx, records[len(records)-1].LSN); err != nil {
			t.Fatal(err)
		}
		requireSame(t, fmt.Sprintf("replay limited to LSN %d, then caught up", limit), live, follower.fingerprint(workloadProbes))
		follower.close()
	}
	if cuts == 0 {
		t.Fatal("the workload has no transaction to cut")
	}
}
