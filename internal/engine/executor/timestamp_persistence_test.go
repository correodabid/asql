package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	auditstore "github.com/correodabid/asql/internal/storage/audit"
	"github.com/correodabid/asql/internal/storage/wal"
)

// auditedNode is an appendOnlyNode with an audit store, as the servers run.
type auditedNode struct {
	*appendOnlyNode
	auditPath string
	audit     *auditstore.Store
}

func openAuditedNode(t *testing.T, dir string) *auditedNode {
	t.Helper()
	n := &auditedNode{
		appendOnlyNode: &appendOnlyNode{t: t, walPath: filepath.Join(dir, "wal"), snapDir: filepath.Join(dir, "snap")},
		auditPath:      filepath.Join(dir, "audit", "audit"),
	}
	n.open()
	return n
}

func (n *auditedNode) open() {
	n.t.Helper()
	for _, dir := range []string{n.snapDir, filepath.Dir(n.auditPath)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			n.t.Fatal(err)
		}
	}
	store, err := wal.NewSegmentedLogStore(n.walPath, wal.AlwaysSync{})
	if err != nil {
		n.t.Fatal(err)
	}
	audit, err := auditstore.New(n.auditPath)
	if err != nil {
		n.t.Fatal(err)
	}
	engine, err := New(context.Background(), store, n.snapDir, WithAuditStore(audit))
	if err != nil {
		n.t.Fatal(err)
	}
	n.store, n.audit, n.engine = store, audit, engine
}

func (n *auditedNode) close() {
	n.engine.WaitPendingSnapshots()
	_ = n.audit.Close()
	_ = n.store.Close()
}

// TestTimestampsSurviveRestart: timestamp values are kept by the WAL, the
// snapshots and the row history, not only by the live state.
func TestTimestampsSurviveRestart(t *testing.T) {
	ctx := context.Background()
	n := openAuditedNode(t, t.TempDir())
	n.mustTx("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)")
	n.mustTx("INSERT INTO ev (id, ts) VALUES (1, '2024-03-15T10:30:45Z'), (2, '2024-12-01T23:59:59.5Z')")
	n.mustTx("UPDATE ev SET ts = '2025-01-02T03:04:05.000006Z' WHERE id = 2")

	want := map[int64]int64{
		1: time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC).UnixMicro(),
		2: time.Date(2025, 1, 2, 3, 4, 5, 6000, time.UTC).UnixMicro(),
	}
	check := func(label string) {
		t.Helper()
		res, err := n.engine.Query(ctx, "SELECT id, ts FROM ev", []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(res.Rows) != len(want) {
			t.Fatalf("%s: %d rows, want %d", label, len(res.Rows), len(want))
		}
		for _, row := range res.Rows {
			if ts := row["ts"]; ts.Kind != ast.LiteralTimestamp || ts.NumberValue != want[row["id"].NumberValue] {
				t.Fatalf("%s: row %d has ts %+v, want %d", label, row["id"].NumberValue, ts, want[row["id"].NumberValue])
			}
		}
		history, err := n.engine.RowHistory(ctx, "SELECT * FROM ev FOR HISTORY WHERE id = 2", []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(history.Rows) != 2 || history.Rows[1]["ts"].NumberValue != want[2] {
			t.Fatalf("%s: history of row 2 is %+v", label, history.Rows)
		}
	}

	check("live")
	// The second restart reads the snapshot the first one wrote.
	for _, label := range []string{"restart", "restart from a snapshot"} {
		n.close()
		n.open()
		check(label)
	}
	// Without snapshots or row history, everything comes from the WAL.
	n.close()
	for _, dir := range []string{n.snapDir, filepath.Dir(n.auditPath)} {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	n.open()
	defer n.close()
	check("replay of the WAL alone")
}
