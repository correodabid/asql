package executor

import (
	"errors"
	"testing"

	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// TestReservedRowVersionColumn: a client cannot declare, rename or drop
// _lsn, while replay, which applies statements without that check, still
// takes a CREATE TABLE that declares it, as WALs written before may hold.
func TestReservedRowVersionColumn(t *testing.T) {
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	n.mustTx("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
	for _, sql := range []string{
		"CREATE TABLE u (id INT PRIMARY KEY, _lsn TEXT)",
		"ALTER TABLE t ADD COLUMN _lsn TEXT",
		"ALTER TABLE t DROP COLUMN _lsn",
		"ALTER TABLE t RENAME COLUMN v TO _lsn",
		"ALTER TABLE t RENAME COLUMN _lsn TO v",
	} {
		var sqlErr *sqlerr.SQLError
		if err := n.tx(sql); !errors.As(err, &sqlErr) || sqlErr.Code != "42701" {
			t.Fatalf("%s: %v, want 42701", sql, err)
		}
	}

	state := n.engine.readState.Load().cloneForMutation(map[string]struct{}{"app": {}})
	if err := n.engine.applyMutationToState(state, "app", "CREATE TABLE legacy (id INT PRIMARY KEY, _lsn TEXT)", n.engine.HeadLSN()+1); err != nil {
		t.Fatalf("replaying a CREATE TABLE that declares _lsn: %v", err)
	}
}
