package executor

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/storage/wal"
)

func newRowConflictEngine(t *testing.T) *Engine {
	t.Helper()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	engine, err := New(context.Background(), store, "")
	if err != nil {
		t.Fatal(err)
	}
	commitAll(t, engine, "CREATE TABLE jobs (id INT PRIMARY KEY, state TEXT, owner TEXT)")
	commitAll(t, engine,
		"INSERT INTO jobs (id, state, owner) VALUES (1, 'pending', '')",
		"INSERT INTO jobs (id, state, owner) VALUES (2, 'pending', '')",
		"INSERT INTO jobs (id, state, owner) VALUES (3, 'done', '')",
	)
	return engine
}

func commitAll(t *testing.T, engine *Engine, statements ...string) {
	t.Helper()
	if err := commitTx(engine, begin(t, engine, statements...)); err != nil {
		t.Fatalf("%v: %v", statements, err)
	}
}

// begin opens a transaction and queues statements in it.
func begin(t *testing.T, engine *Engine, statements ...string) *Session {
	t.Helper()
	session := engine.NewSession()
	for _, sql := range append([]string{"BEGIN DOMAIN work"}, statements...) {
		if _, err := engine.Execute(context.Background(), session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	return session
}

func commitTx(engine *Engine, session *Session) error {
	_, err := engine.Execute(context.Background(), session, "COMMIT")
	return err
}

func claim(id int, owner string) string {
	return fmt.Sprintf("UPDATE jobs SET state = 'running', owner = '%s' WHERE id = %d AND state = 'pending'", owner, id)
}

func requireConflict(t *testing.T, label string, err error) {
	t.Helper()
	if !errors.Is(err, errWriteConflict) {
		t.Fatalf("%s: error %v, want a write conflict (40001)", label, err)
	}
}

func requireCommits(t *testing.T, label string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
}

// TestWriteConflictsAreDetectedPerRow: concurrent UPDATEs and DELETEs of
// different rows of one table all commit; the later of two on the same row
// gets 40001, including when the earlier one made its WHERE stop matching.
func TestWriteConflictsAreDetectedPerRow(t *testing.T) {
	t.Run("different rows commit", func(t *testing.T) {
		engine := newRowConflictEngine(t)
		a, b := begin(t, engine, claim(1, "a")), begin(t, engine, claim(2, "b"))
		c := begin(t, engine, "DELETE FROM jobs WHERE id = 3")
		requireCommits(t, "a", commitTx(engine, a))
		requireCommits(t, "b", commitTx(engine, b))
		requireCommits(t, "c", commitTx(engine, c))
		res, _ := engine.Query(context.Background(), "SELECT id FROM jobs WHERE state = 'running'", []string{"work"})
		if len(res.Rows) != 2 {
			t.Fatalf("running jobs = %d, want 2", len(res.Rows))
		}
	})
	t.Run("same row conflicts", func(t *testing.T) {
		engine := newRowConflictEngine(t)
		a, b := begin(t, engine, claim(1, "a")), begin(t, engine, claim(1, "b"))
		requireCommits(t, "b", commitTx(engine, b))
		// a's WHERE no longer matches row 1, but the row a selected changed.
		requireConflict(t, "a", commitTx(engine, a))
		res, _ := engine.Query(context.Background(), "SELECT owner FROM jobs WHERE id = 1", []string{"work"})
		if len(res.Rows) != 1 || res.Rows[0]["owner"].StringValue != "b" {
			t.Fatalf("job 1 = %v, want owned by b", res.Rows)
		}
	})
	t.Run("a row that starts matching conflicts", func(t *testing.T) {
		engine := newRowConflictEngine(t)
		a := begin(t, engine, "UPDATE jobs SET owner = 'sweeper' WHERE state = 'pending'")
		b := begin(t, engine, "INSERT INTO jobs (id, state, owner) VALUES (4, 'pending', '')")
		c := begin(t, engine, "INSERT INTO jobs (id, state, owner) VALUES (5, 'done', '')")
		requireCommits(t, "c", commitTx(engine, c)) // does not match a's WHERE
		requireCommits(t, "b", commitTx(engine, b))
		requireConflict(t, "a", commitTx(engine, a))
	})
	t.Run("delete of the same row conflicts", func(t *testing.T) {
		engine := newRowConflictEngine(t)
		a, b := begin(t, engine, "DELETE FROM jobs WHERE id = 2"), begin(t, engine, claim(2, "b"))
		requireCommits(t, "a", commitTx(engine, a))
		requireConflict(t, "b", commitTx(engine, b))
	})
	t.Run("an UPDATE without WHERE conflicts with any change", func(t *testing.T) {
		engine := newRowConflictEngine(t)
		a, b := begin(t, engine, "UPDATE jobs SET owner = 'all'"), begin(t, engine, claim(2, "b"))
		requireCommits(t, "b", commitTx(engine, b))
		requireConflict(t, "a", commitTx(engine, a))
	})
}

// TestWriteConflictsFallBackToTheTableWhenHistoryWasTrimmed: when the
// bounded changeLog dropped changes made since a transaction began, any
// change to the table is a conflict.
func TestWriteConflictsFallBackToTheTableWhenHistoryWasTrimmed(t *testing.T) {
	engine := newRowConflictEngine(t)
	a := begin(t, engine, claim(1, "a"))
	for batch := 0; batch*500 < maxChangeLogPerTable+changeLogTrimBatch; batch++ {
		values := make([]string, 0, 500)
		for i := 0; i < 500; i++ {
			values = append(values, fmt.Sprintf("(%d, 'done', '')", 100+batch*500+i))
		}
		commitAll(t, engine, "INSERT INTO jobs (id, state, owner) VALUES "+strings.Join(values, ", "))
	}
	requireConflict(t, "a", commitTx(engine, a))
}
