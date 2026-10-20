package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/sqlerr"
	"github.com/correodabid/asql/internal/storage/wal"
)

type appendOnlyNode struct {
	t       *testing.T
	walPath string
	snapDir string
	store   *wal.SegmentedLogStore
	engine  *Engine
}

func openAppendOnlyNode(t *testing.T, dir string) *appendOnlyNode {
	t.Helper()
	n := &appendOnlyNode{t: t, walPath: filepath.Join(dir, "wal"), snapDir: filepath.Join(dir, "snap")}
	n.open()
	return n
}

func (n *appendOnlyNode) open() {
	n.t.Helper()
	if err := os.MkdirAll(n.snapDir, 0o755); err != nil {
		n.t.Fatal(err)
	}
	store, err := wal.NewSegmentedLogStore(n.walPath, wal.AlwaysSync{})
	if err != nil {
		n.t.Fatal(err)
	}
	engine, err := New(context.Background(), store, n.snapDir)
	if err != nil {
		n.t.Fatal(err)
	}
	n.store, n.engine = store, engine
}

func (n *appendOnlyNode) close() {
	n.engine.WaitPendingSnapshots()
	_ = n.store.Close()
}

// tx runs statements in one transaction and returns the first error.
func (n *appendOnlyNode) tx(statements ...string) error {
	ctx := context.Background()
	session := n.engine.NewSession()
	if _, err := n.engine.Execute(ctx, session, "BEGIN DOMAIN app"); err != nil {
		return err
	}
	for _, sql := range statements {
		if _, err := n.engine.Execute(ctx, session, sql); err != nil {
			_, _ = n.engine.Execute(ctx, session, "ROLLBACK")
			return err
		}
	}
	_, err := n.engine.Execute(ctx, session, "COMMIT")
	return err
}

func (n *appendOnlyNode) mustTx(statements ...string) {
	n.t.Helper()
	if err := n.tx(statements...); err != nil {
		n.t.Fatalf("%v: %v", statements, err)
	}
}

func requireAppendOnlyError(t *testing.T, label string, err error) {
	t.Helper()
	var sqlErr *sqlerr.SQLError
	if !errors.As(err, &sqlErr) || sqlErr.Code != "42501" || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("%s: error %v, want an append-only violation (42501)", label, err)
	}
}

func (n *appendOnlyNode) count(sql string) int {
	n.t.Helper()
	res, err := n.engine.Query(context.Background(), sql, []string{"app"})
	if err != nil {
		n.t.Fatal(err)
	}
	return len(res.Rows)
}

// TestAppendOnlyTableAcceptsOnlyNewRows: an append-only table takes INSERT
// and new columns, and rejects every change to existing rows and the table
// itself, for good.
func TestAppendOnlyTableAcceptsOnlyNewRows(t *testing.T) {
	n := openAppendOnlyNode(t, t.TempDir())
	n.mustTx("CREATE TABLE artifacts (id INT PRIMARY KEY, kind TEXT, note TEXT) APPEND ONLY")
	n.mustTx("INSERT INTO artifacts (id, kind, note) VALUES (1, 'report', 'a')")
	n.mustTx("INSERT INTO artifacts (id, kind, note) VALUES (1, 'other', 'b') ON CONFLICT (id) DO NOTHING")
	n.mustTx("ALTER TABLE artifacts ADD COLUMN source TEXT")
	n.mustTx("INSERT INTO artifacts (id, kind, note, source) VALUES (2, 'report', 'c', 'x')")

	for _, sql := range []string{
		"UPDATE artifacts SET kind = 'forged' WHERE id = 1",
		"DELETE FROM artifacts WHERE id = 1",
		"TRUNCATE TABLE artifacts",
		"DROP TABLE artifacts",
		"ALTER TABLE artifacts DROP COLUMN note",
		"INSERT INTO artifacts (id, kind, note) VALUES (1, 'forged', 'z') ON CONFLICT (id) DO UPDATE SET kind = EXCLUDED.kind",
	} {
		requireAppendOnlyError(t, sql, n.tx(sql))
	}
	// Setting it again is a no-op, and nothing undoes it.
	n.mustTx("ALTER TABLE artifacts SET APPEND ONLY")
	if got := n.count("SELECT * FROM artifacts WHERE kind = 'report'"); got != 2 {
		t.Fatalf("rows after rejected changes = %d, want 2", got)
	}

	// The flag survives a restart from a snapshot and one from the WAL alone.
	// The first restart checkpoints the head, so the second replays nothing.
	n.close()
	n.open()
	n.close()
	n.open()
	requireAppendOnlyError(t, "after restart", n.tx("DELETE FROM artifacts WHERE id = 2"))
	n.close()
	if err := os.RemoveAll(n.snapDir); err != nil {
		t.Fatal(err)
	}
	n.open()
	defer n.close()
	requireAppendOnlyError(t, "after replay from the WAL", n.tx("UPDATE artifacts SET note = 'x' WHERE id = 2"))
	if !schemaTableAppendOnly(n.engine.SchemaSnapshot(nil), "app", "artifacts") {
		t.Fatal("schema snapshot does not report the table as append-only")
	}
}

func schemaTableAppendOnly(schema SchemaSnapshot, domain, table string) bool {
	for _, d := range schema.Domains {
		for _, tbl := range d.Tables {
			if d.Name == domain && tbl.Name == table {
				return tbl.AppendOnly
			}
		}
	}
	return false
}

// TestAppendOnlyIsCheckedAtCommit: a change queued before the table became
// append-only is rejected when it commits after.
func TestAppendOnlyIsCheckedAtCommit(t *testing.T) {
	ctx := context.Background()
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	n.mustTx("CREATE TABLE findings (id INT PRIMARY KEY, verdict TEXT)", "INSERT INTO findings (id, verdict) VALUES (1, 'draft')")

	early := n.engine.NewSession()
	for _, sql := range []string{"BEGIN DOMAIN app", "UPDATE findings SET verdict = 'rewritten' WHERE id = 1"} {
		if _, err := n.engine.Execute(ctx, early, sql); err != nil {
			t.Fatal(err)
		}
	}
	n.mustTx("ALTER TABLE findings SET APPEND ONLY")
	_, err := n.engine.Execute(ctx, early, "COMMIT")
	requireAppendOnlyError(t, "commit after SET APPEND ONLY", err)

	if got := n.count("SELECT * FROM findings WHERE verdict = 'draft'"); got != 1 {
		t.Fatalf("the rejected UPDATE changed the row")
	}

	// Also within the transaction that creates the table: the queued UPDATE
	// is caught when the commit applies it.
	requireAppendOnlyError(t, "same transaction", n.tx(
		"CREATE TABLE published (id INT PRIMARY KEY, v TEXT) APPEND ONLY",
		"INSERT INTO published (id, v) VALUES (1, 'a')",
		"UPDATE published SET v = 'b' WHERE id = 1",
	))
}
