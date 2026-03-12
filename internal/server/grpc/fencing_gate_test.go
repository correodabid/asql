package grpc

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/engine/executor"
	"asql/internal/storage/wal"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommitTxFencingRejectsPartialFields(t *testing.T) {
	service := newTestServiceWithLeadership(t)

	begin, err := service.BeginTx(context.Background(), &BeginTxRequest{Mode: "domain", Domains: []string{"orders"}})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	_, err = service.CommitTx(context.Background(), &CommitTxRequest{TxID: begin.TxID, NodeID: "node-a"})
	if err == nil {
		t.Fatal("expected invalid argument for partial fencing fields")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestCommitTxFencingRejectsStaleLeaderToken(t *testing.T) {
	service := newTestServiceWithLeadership(t)

	state, err := service.leadership.TryAcquireLeadership("orders", "node-a", 10)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	begin, err := service.BeginTx(context.Background(), &BeginTxRequest{Mode: "domain", Domains: []string{"orders"}})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	_, err = service.CommitTx(context.Background(), &CommitTxRequest{
		TxID:         begin.TxID,
		Group:        "orders",
		NodeID:       "node-a",
		FencingToken: state.FencingToken + "-stale",
	})
	if err == nil {
		t.Fatal("expected permission denied for stale fencing token")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", status.Code(err))
	}
}

func TestCommitTxFencingAcceptsActiveLeaderToken(t *testing.T) {
	service := newTestServiceWithLeadership(t)

	state, err := service.leadership.TryAcquireLeadership("orders", "node-a", 10)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	begin, err := service.BeginTx(context.Background(), &BeginTxRequest{Mode: "domain", Domains: []string{"orders"}})
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	_, err = service.Execute(context.Background(), &ExecuteRequest{TxID: begin.TxID, SQL: "CREATE TABLE users (id INT, email TEXT)"})
	if err != nil {
		t.Fatalf("execute create: %v", err)
	}

	commit, err := service.CommitTx(context.Background(), &CommitTxRequest{
		TxID:         begin.TxID,
		Group:        "orders",
		NodeID:       "node-a",
		FencingToken: state.FencingToken,
	})
	if err != nil {
		t.Fatalf("commit tx: %v", err)
	}
	if commit.Status != "COMMIT" {
		t.Fatalf("unexpected commit status: %s", commit.Status)
	}
}

func newTestServiceWithLeadership(t *testing.T) *service {
	t.Helper()

	walStore, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "fencing-test.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	t.Cleanup(func() { _ = walStore.Close() })

	engine, err := executor.New(context.Background(), walStore, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	clock := &leadershipTestClock{now: time.Unix(100, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 30*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	return newService(engine, slog.Default(), leadership)
}
