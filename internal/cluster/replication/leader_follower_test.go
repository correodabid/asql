package replication

import (
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	grpcapi "github.com/correodabid/asql/internal/server/grpc"
	"github.com/correodabid/asql/internal/storage/wal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestLeaderFollowerCatchUp(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()

	leader, err := grpcapi.New(grpcapi.Config{
		Address:     "bufnet",
		DataDirPath: filepath.Join(temp, "leader-data"),
		Logger:      slog.Default(),
	})
	if err != nil {
		t.Fatalf("new leader server: %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() {
		leader.Stop()
		_ = listener.Close()
	})

	go func() {
		_ = leader.ServeOnListener(listener)
	}()

	connection, err := grpc.DialContext(
		ctx,
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	)
	if err != nil {
		t.Fatalf("dial leader: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	begin := new(grpcapi.BeginTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &grpcapi.BeginTxRequest{Mode: "domain", Domains: []string{"accounts"}}, begin); err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	execute(t, ctx, connection, begin.TxID, "CREATE TABLE users (id INT, email TEXT)")
	execute(t, ctx, connection, begin.TxID, "INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')")

	commit := new(grpcapi.CommitTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/CommitTx", &grpcapi.CommitTxRequest{TxID: begin.TxID}, commit); err != nil {
		t.Fatalf("commit tx: %v", err)
	}

	followerStore, err := wal.NewSegmentedLogStore(filepath.Join(temp, "follower.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new follower wal: %v", err)
	}
	t.Cleanup(func() { _ = followerStore.Close() })

	before, err := LagStatusFromGRPC(ctx, connection, followerStore)
	if err != nil {
		t.Fatalf("lag status before catch-up: %v", err)
	}
	if before.LeaderLSN == 0 {
		t.Fatalf("expected leader lsn > 0, got %d", before.LeaderLSN)
	}
	if before.Lag == 0 {
		t.Fatalf("expected lag > 0 before catch-up, got %+v", before)
	}

	if err := CatchUpFromGRPC(ctx, connection, followerStore, 32); err != nil {
		t.Fatalf("replication catch-up: %v", err)
	}

	after, err := LagStatusFromGRPC(ctx, connection, followerStore)
	if err != nil {
		t.Fatalf("lag status after catch-up: %v", err)
	}
	if after.Lag != 0 {
		t.Fatalf("expected lag = 0 after catch-up, got %+v", after)
	}

	followerEngine, err := executor.New(ctx, followerStore, "")
	if err != nil {
		t.Fatalf("new follower engine: %v", err)
	}

	if got := followerEngine.RowCount("accounts", "users"); got != 1 {
		t.Fatalf("unexpected follower row count: got %d want 1", got)
	}
}

func execute(t *testing.T, ctx context.Context, connection *grpc.ClientConn, txID, sql string) {
	t.Helper()

	response := new(grpcapi.ExecuteResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/Execute", &grpcapi.ExecuteRequest{TxID: txID, SQL: sql}, response); err != nil {
		t.Fatalf("execute invoke for %q: %v", sql, err)
	}
}
