package executor

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

// TestTxTimestampIsTheCommitTime: DEFAULT TX_TIMESTAMP is the time the
// commit records (ADR 0007), the same for every row of a transaction, typed
// as the column is, and it survives replay. INSERT ... RETURNING, which hands
// the row back before the commit, takes the time the statement runs.
func TestTxTimestampIsTheCommitTime(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	// Readings: the RETURNING statement, then each commit batch.
	clock := &scriptedClock{times: []time.Time{at(0), at(1), at(2), at(3)}}
	n := openClockNode(t, dir, clock)
	n.mustTx("CREATE TABLE ev (id INT PRIMARY KEY, at TIMESTAMP DEFAULT TX_TIMESTAMP, txt TEXT DEFAULT TX_TIMESTAMP)")
	n.mustTx("INSERT INTO ev (id) VALUES (1)", "INSERT INTO ev (id) VALUES (2), (3)")
	commitLSN := n.engine.HeadLSN()

	commitTime := commitTimes(t, n.engine)[commitLSN]
	check := func(label string) {
		t.Helper()
		res, err := n.engine.Query(ctx, "SELECT id, at, txt FROM ev", []string{"app"})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Rows) != 3 {
			t.Fatalf("%s: %d rows", label, len(res.Rows))
		}
		for _, row := range res.Rows {
			if row["at"] != (ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: commitTime}) {
				t.Fatalf("%s: row %d at = %+v, want the commit time %s", label, row["id"].NumberValue, row["at"], time.UnixMicro(commitTime).UTC())
			}
			if want := time.UnixMicro(commitTime).UTC().Format(time.RFC3339Nano); row["txt"].StringValue != want {
				t.Fatalf("%s: row %d txt = %q, want %q", label, row["id"].NumberValue, row["txt"].StringValue, want)
			}
		}
		// The row's TX_TIMESTAMP is a date at which it exists.
		if lsn, err := n.engine.LSNForCommitTime(commitTime); err != nil || lsn != commitLSN {
			t.Fatalf("%s: TX_TIMESTAMP resolves to LSN %d (err %v), want %d", label, lsn, err, commitLSN)
		}
	}
	check("live")
	n.close()
	if err := os.RemoveAll(n.snapDir); err != nil {
		t.Fatal(err)
	}
	n = openClockNode(t, dir, &scriptedClock{times: []time.Time{at(30), at(40)}})
	defer n.close()
	check("replay")

	// RETURNING: the time the statement ran.
	session := n.engine.NewSession()
	for _, sql := range []string{"BEGIN DOMAIN app", "INSERT INTO ev (id) VALUES (4) RETURNING at"} {
		res, err := n.engine.Execute(ctx, session, sql)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Rows) == 1 && res.Rows[0]["at"].NumberValue != at(30).UnixMicro() {
			t.Fatalf("RETURNING at = %+v, want the statement's time %s", res.Rows[0]["at"], at(30))
		}
	}
	if _, err := n.engine.Execute(ctx, session, "COMMIT"); err != nil {
		t.Fatal(err)
	}
}
