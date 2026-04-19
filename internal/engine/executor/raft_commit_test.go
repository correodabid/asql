package executor

import (
	"context"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

type raftPathLogStore struct {
	batchNoSyncCalls int
	syncCalls        int
	records          []ports.WALRecord
}

func (store *raftPathLogStore) Append(_ context.Context, record ports.WALRecord) (uint64, error) {
	record.LSN = uint64(len(store.records) + 1)
	store.records = append(store.records, record)
	return record.LSN, nil
}

func (store *raftPathLogStore) ReadFrom(_ context.Context, fromLSN uint64, limit int) ([]ports.WALRecord, error) {
	result := make([]ports.WALRecord, 0)
	for _, record := range store.records {
		if record.LSN < fromLSN {
			continue
		}
		result = append(result, record)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (store *raftPathLogStore) AppendBatchNoSync(_ context.Context, records []ports.WALRecord) ([]uint64, error) {
	store.batchNoSyncCalls++
	lsns := make([]uint64, len(records))
	for i, record := range records {
		record.LSN = uint64(len(store.records) + 1)
		store.records = append(store.records, record)
		lsns[i] = record.LSN
	}
	return lsns, nil
}

func (store *raftPathLogStore) Sync() error {
	store.syncCalls++
	return nil
}

type fakeRaftCommitter struct {
	applyCalls      int
	applyBatchCalls int
	nextLSN         uint64
}

func (committer *fakeRaftCommitter) Apply(_ context.Context, _ string, _ string, _ []byte) (uint64, error) {
	committer.applyCalls++
	committer.nextLSN++
	return committer.nextLSN, nil
}

func (committer *fakeRaftCommitter) ApplyBatch(_ context.Context, records []ports.RaftRecord) ([]uint64, error) {
	committer.applyBatchCalls++
	lsns := make([]uint64, len(records))
	for i := range records {
		committer.nextLSN++
		lsns[i] = committer.nextLSN
	}
	return lsns, nil
}

func TestCommitUsesRaftCommitterInsteadOfDirectWALAndSync(t *testing.T) {
	ctx := context.Background()
	store := &raftPathLogStore{}
	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	committer := &fakeRaftCommitter{}
	engine.SetRaftCommitter(committer)

	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	if committer.applyBatchCalls != 1 {
		t.Fatalf("expected one raft batch commit, got %d", committer.applyBatchCalls)
	}
	if committer.applyCalls != 0 {
		t.Fatalf("expected no single-record raft applies, got %d", committer.applyCalls)
	}
	if store.batchNoSyncCalls != 0 {
		t.Fatalf("expected direct wal AppendBatchNoSync path to be bypassed, got %d calls", store.batchNoSyncCalls)
	}
	if store.syncCalls != 0 {
		t.Fatalf("expected group fsync path to be bypassed in raft mode, got %d sync calls", store.syncCalls)
	}
	if got := engine.RowCount("accounts", "users"); got != 1 {
		t.Fatalf("unexpected row count: got %d want 1", got)
	}
	if headLSN := engine.readState.Load().headLSN; headLSN == 0 {
		t.Fatal("expected raft commit path to advance head LSN")
	}
}