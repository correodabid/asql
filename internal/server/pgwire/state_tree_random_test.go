package pgwire

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/wal"
)

// TestStateTreeIncrementalMatchesFromScratch: after every one of many
// random changes to artifacts and their parent edges (inserts, deletes,
// updates, a column added, the group declared halfway), the root the
// builder keeps incrementally equals the root of a tree built from scratch
// from the same state.
func TestStateTreeIncrementalMatchesFromScratch(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()
	tx := func(sqls ...string) error {
		if _, err := engine.Execute(ctx, session, "BEGIN DOMAIN lab"); err != nil {
			return err
		}
		for _, sql := range sqls {
			if _, err := engine.Execute(ctx, session, sql); err != nil {
				_, _ = engine.Execute(ctx, session, "ROLLBACK")
				return err
			}
		}
		_, err := engine.Execute(ctx, session, "COMMIT")
		return err
	}
	if err := tx("CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT) VERIFIABLE",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) VERIFIABLE"); err != nil {
		t.Fatal(err)
	}
	builder := newStateTreeBuilder(engine, [32]byte{9})
	rng := rand.New(rand.NewPCG(7, 11))
	id := func() string { return fmt.Sprintf("a%02d", rng.IntN(30)) }
	steps := 0
	for step := range 400 {
		var sql string
		switch op := rng.IntN(10); {
		case step == 150:
			sql = "ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)"
		case step == 250:
			sql = "ALTER TABLE artifacts ADD COLUMN sha TEXT DEFAULT 'none'"
		case op < 3:
			sql = fmt.Sprintf("INSERT INTO artifacts (id, kind) VALUES ('%s', 'k%d')", id(), step)
		case op < 4:
			sql = fmt.Sprintf("DELETE FROM artifacts WHERE id = '%s'", id())
		case op < 5:
			sql = fmt.Sprintf("UPDATE artifacts SET kind = 'u%d' WHERE id = '%s'", step, id())
		case op < 8:
			sql = fmt.Sprintf("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('%s', '%s', %d)", id(), id(), step)
		case op < 9:
			sql = fmt.Sprintf("DELETE FROM artifact_parents WHERE child_id = '%s'", id())
		default:
			sql = fmt.Sprintf("UPDATE artifact_parents SET ordinal = %d WHERE child_id = '%s'", step, id())
		}
		if err := tx(sql); err != nil {
			continue // a duplicate key or a missing row: nothing changed
		}
		steps++
		if _, _, _, err := builder.State(ctx); err != nil {
			t.Fatal(err)
		}
		fresh, err := buildStateTree(builder.key, engine.VerifiableView())
		if err != nil {
			t.Fatal(err)
		}
		if builder.tree.Root() != fresh.Root() || builder.tree.Len() != fresh.Len() {
			t.Fatalf("step %d (%s): incremental root differs from scratch (%d vs %d leaves)", step, sql, builder.tree.Len(), fresh.Len())
		}
	}
	if steps < 200 {
		t.Fatalf("only %d of 400 changes applied", steps)
	}
}
