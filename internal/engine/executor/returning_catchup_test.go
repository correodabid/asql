package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

// readinessCommitter is a Raft committer whose leader is not ready yet.
type readinessCommitter struct {
	fakeRaftCommitter
	waits int
}

func (committer *readinessCommitter) WaitLeaderReady(context.Context) error {
	committer.waits++
	return errors.New("leader not ready")
}

// TestInsertReturningDoesNotApplyUncommittedWAL covers a cluster node whose
// WAL ends with an entry that is not committed: INSERT ... RETURNING must not
// replay it into the engine to resolve defaults. If the entry is later
// replaced by a new leader, nothing would roll the engine back.
func TestInsertReturningDoesNotApplyUncommittedWAL(t *testing.T) {
	ctx := context.Background()
	source := &raftPathLogStore{}
	sourceEngine, err := New(ctx, source, "")
	if err != nil {
		t.Fatal(err)
	}
	session := sourceEngine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"COMMIT",
		"BEGIN DOMAIN accounts",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	} {
		if _, err := sourceEngine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
	committed := source.records[len(source.records)-1].LSN
	if _, err := sourceEngine.Execute(ctx, session, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceEngine.Execute(ctx, session, "INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')"); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceEngine.Execute(ctx, session, "COMMIT"); err != nil {
		t.Fatal(err)
	}

	// The node's WAL holds the uncommitted tail; startup replay stops at the
	// commit index.
	store := &raftPathLogStore{records: append([]ports.WALRecord(nil), source.records...)}
	engine, err := New(ctx, store, "", WithReplayLimit(committed))
	if err != nil {
		t.Fatal(err)
	}
	committer := &readinessCommitter{}
	engine.SetRaftCommitter(committer)
	if got := engine.RowCount("accounts", "users"); got != 1 {
		t.Fatalf("rows after bounded replay = %d, want 1", got)
	}

	session = engine.NewSession()
	if _, err := engine.Execute(ctx, session, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Execute(ctx, session, "INSERT INTO users (id, email) VALUES (3, 'three@asql.dev') RETURNING id"); err != nil {
		t.Fatal(err)
	}
	if got := engine.RowCount("accounts", "users"); got != 1 {
		t.Fatalf("INSERT ... RETURNING applied uncommitted WAL entries: rows = %d, want 1", got)
	}
	if committer.waits == 0 {
		t.Fatal("expected INSERT ... RETURNING to wait for leader readiness")
	}
}
