package executor

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/storage/wal"
)

func replayLimitIDs(t *testing.T, eng *Engine) string {
	t.Helper()
	res, err := eng.Query(context.Background(), "SELECT id FROM results ORDER BY id", []string{"lab"})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, 0, len(res.Rows))
	for _, row := range res.Rows {
		ids = append(ids, row["id"].NumberValue)
	}
	return fmt.Sprint(ids)
}

func TestReplayLimitDefersTailUntilCatchUp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, eng := newLedgerTestEngine(t, dir)
	execLedgerTx(t, eng, "CREATE TABLE results (id INT, result TEXT)")
	execLedgerTx(t, eng, "INSERT INTO results (id, result) VALUES (1, 'a')")
	limit := store.LastLSN()
	captureLedgerTestSnapshot(eng)
	execLedgerTx(t, eng, "INSERT INTO results (id, result) VALUES (2, 'b')")
	// A snapshot beyond the limit, as a pre-watermark node could have written.
	captureLedgerTestSnapshot(eng)
	eng.WaitPendingSnapshots()
	last := store.LastLSN()
	_ = store.Close()

	store, err := wal.NewSegmentedLogStore(filepath.Join(dir, "asql.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	eng, err = New(ctx, store, filepath.Join(dir, "snap"), WithReplayLimit(limit))
	if err != nil {
		t.Fatal(err)
	}
	if got := replayLimitIDs(t, eng); got != "[1]" || eng.HeadLSN() != limit {
		t.Fatalf("replay limit not honored: ids=%s head=%d limit=%d", got, eng.HeadLSN(), limit)
	}
	if err := eng.CatchUp(ctx, last); err != nil {
		t.Fatal(err)
	}
	if got := replayLimitIDs(t, eng); got != "[1 2]" || eng.HeadLSN() != last {
		t.Fatalf("catch-up after limited replay: ids=%s head=%d", got, eng.HeadLSN())
	}
}

// An empty transaction (BEGIN + COMMIT) is the leader's no-op: it must
// replay and apply without error and without changing state.
func TestEmptyTransactionReplaysAsNoOp(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, eng := newLedgerTestEngine(t, dir)
	execLedgerTx(t, eng, "CREATE TABLE results (id INT, result TEXT)")
	execLedgerTx(t, eng, "INSERT INTO results (id, result) VALUES (1, 'a')")
	head := store.LastLSN()
	noop := []ports.WALRecord{
		{LSN: head + 1, Term: 2, TxID: "raft-noop-2", Type: "BEGIN"},
		{LSN: head + 2, Term: 2, TxID: "raft-noop-2", Type: "COMMIT"},
	}
	if err := store.AppendReplicatedBatch(ctx, noop); err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyCommittedRecords(ctx, head+2, noop); err != nil {
		t.Fatalf("apply no-op: %v", err)
	}
	if got := replayLimitIDs(t, eng); got != "[1]" || eng.HeadLSN() != head+2 {
		t.Fatalf("after applying no-op: ids=%s head=%d", got, eng.HeadLSN())
	}
	execLedgerTx(t, eng, "INSERT INTO results (id, result) VALUES (2, 'b')")
	eng.WaitPendingSnapshots()
	_ = store.Close()

	store, err := wal.NewSegmentedLogStore(filepath.Join(dir, "asql.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	eng, err = New(ctx, store, "")
	if err != nil {
		t.Fatalf("replay with no-op: %v", err)
	}
	if got := replayLimitIDs(t, eng); got != "[1 2]" {
		t.Fatalf("replay with no-op: ids=%s", got)
	}
}
