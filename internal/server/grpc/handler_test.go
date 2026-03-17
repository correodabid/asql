package grpc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"path/filepath"
	"testing"

	"asql/internal/engine/executor"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestASQLServiceBlackBox(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()

	server, err := New(Config{
		Address:     "bufnet",
		DataDirPath: filepath.Join(temp, "data"),
		Logger:      slog.Default(),
	})
	if err != nil {
		t.Fatalf("new grpc server: %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() {
		server.grpcServer.Stop()
		_ = listener.Close()
	})

	go func() {
		_ = server.grpcServer.Serve(listener)
	}()

	connection, err := grpc.DialContext(
		ctx,
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	beginResp := new(BeginTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &BeginTxRequest{
		Mode:    "domain",
		Domains: []string{"accounts"},
	}, beginResp); err != nil {
		t.Fatalf("begin tx invoke: %v", err)
	}

	if beginResp.TxID == "" {
		t.Fatal("expected tx id from begin tx")
	}

	mustExecute(t, ctx, connection, beginResp.TxID, "CREATE TABLE users (id INT, email TEXT)")
	mustExecute(t, ctx, connection, beginResp.TxID, "INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')")

	commitResp := new(CommitTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/CommitTx", &CommitTxRequest{TxID: beginResp.TxID}, commitResp); err != nil {
		t.Fatalf("commit tx invoke: %v", err)
	}

	if commitResp.Status != "COMMIT" {
		t.Fatalf("unexpected commit status: %s", commitResp.Status)
	}

	timeTravelResp := new(TimeTravelQueryResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/TimeTravelQuery", &TimeTravelQueryRequest{
		SQL:     "SELECT id, email FROM users",
		Domains: []string{"accounts"},
		LSN:     4,
	}, timeTravelResp); err != nil {
		t.Fatalf("time travel invoke: %v", err)
	}

	if len(timeTravelResp.Rows) != 1 {
		t.Fatalf("unexpected time travel rows: got %d want 1", len(timeTravelResp.Rows))
	}

	schemaSnapshot := new(SchemaSnapshotResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/SchemaSnapshot", &SchemaSnapshotRequest{Domains: []string{"accounts"}}, schemaSnapshot); err != nil {
		t.Fatalf("schema snapshot invoke: %v", err)
	}
	if schemaSnapshot.Status != "SNAPSHOT" {
		t.Fatalf("unexpected schema snapshot status: %s", schemaSnapshot.Status)
	}
	if len(schemaSnapshot.Domains) != 1 {
		t.Fatalf("expected one domain in snapshot, got %d", len(schemaSnapshot.Domains))
	}
	if len(schemaSnapshot.Domains[0].Tables) != 1 {
		t.Fatalf("expected one table in snapshot, got %d", len(schemaSnapshot.Domains[0].Tables))
	}
	if schemaSnapshot.Domains[0].Tables[0].Name != "users" {
		t.Fatalf("unexpected table in snapshot: %s", schemaSnapshot.Domains[0].Tables[0].Name)
	}

	migrationPreflight := new(MigrationPreflightResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/MigrationPreflight", &MigrationPreflightRequest{
		Domain:     "accounts",
		ForwardSQL: []string{"ALTER TABLE users ADD COLUMN status TEXT"},
	}, migrationPreflight); err != nil {
		t.Fatalf("migration preflight invoke: %v", err)
	}
	if migrationPreflight.Status != "PREFLIGHT" {
		t.Fatalf("unexpected migration preflight status: %s", migrationPreflight.Status)
	}
	if !migrationPreflight.AutoRollback {
		t.Fatalf("expected auto rollback in migration preflight, got %+v", migrationPreflight)
	}
	if !migrationPreflight.RollbackSafe || !migrationPreflight.RollbackChecked {
		t.Fatalf("expected rollback-safe generated plan, got %+v", migrationPreflight)
	}
	if len(migrationPreflight.RollbackSQL) != 1 || migrationPreflight.RollbackSQL[0] != "ALTER TABLE users DROP COLUMN status" {
		t.Fatalf("unexpected rollback sql: %+v", migrationPreflight.RollbackSQL)
	}

	replayResp := new(ReplayToLSNResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ReplayToLSN", &ReplayToLSNRequest{LSN: 4}, replayResp); err != nil {
		t.Fatalf("replay invoke: %v", err)
	}

	if replayResp.AppliedLSN != 4 {
		t.Fatalf("unexpected replay lsn: got %d want 4", replayResp.AppliedLSN)
	}

	statsResp := new(ScanStrategyStatsResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ScanStrategyStats", &ScanStrategyStatsRequest{}, statsResp); err != nil {
		t.Fatalf("scan strategy stats invoke: %v", err)
	}

	if len(statsResp.Counts) == 0 {
		t.Fatal("expected non-empty scan strategy stats after time travel query")
	}

	routeResp := new(EvaluateReadRouteResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/EvaluateReadRoute", &EvaluateReadRouteRequest{
		Consistency: "bounded-stale",
		LeaderLSN:   20,
		FollowerLSN: 10,
		HasFollower: true,
		MaxLag:      5,
	}, routeResp); err != nil {
		t.Fatalf("evaluate read route invoke: %v", err)
	}

	if routeResp.Mode != "bounded-stale" {
		t.Fatalf("unexpected mode: got %q want bounded-stale", routeResp.Mode)
	}
	if routeResp.Route != "leader" {
		t.Fatalf("unexpected route: got %q want leader", routeResp.Route)
	}
	if routeResp.Lag != 10 {
		t.Fatalf("unexpected lag: got %d want 10", routeResp.Lag)
	}
	if routeResp.FallbackReason != "lag_exceeded" {
		t.Fatalf("unexpected fallback reason: got %q want lag_exceeded", routeResp.FallbackReason)
	}

	routingStats := new(ReadRoutingStatsResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ReadRoutingStats", &ReadRoutingStatsRequest{}, routingStats); err != nil {
		t.Fatalf("read routing stats invoke: %v", err)
	}

	if routingStats.Counts["requests_total"] == 0 {
		t.Fatal("expected read routing requests_total counter > 0")
	}
	if routingStats.Counts["fallback_lag_exceeded"] == 0 {
		t.Fatal("expected fallback_lag_exceeded counter > 0")
	}

	explainFirst := new(ExplainQueryResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ExplainQuery", &ExplainQueryRequest{
		SQL:     "SELECT id, email FROM users ORDER BY id DESC LIMIT 1",
		Domains: []string{"accounts"},
	}, explainFirst); err != nil {
		t.Fatalf("explain query invoke (first): %v", err)
	}

	explainSecond := new(ExplainQueryResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ExplainQuery", &ExplainQueryRequest{
		SQL:     "SELECT id, email FROM users ORDER BY id DESC LIMIT 1",
		Domains: []string{"accounts"},
	}, explainSecond); err != nil {
		t.Fatalf("explain query invoke (second): %v", err)
	}

	if explainFirst.Status != "EXPLAIN" {
		t.Fatalf("unexpected explain status: %s", explainFirst.Status)
	}
	if len(explainFirst.Rows) != 1 || len(explainSecond.Rows) != 1 {
		t.Fatalf("expected one explain row in each call, got first=%d second=%d", len(explainFirst.Rows), len(explainSecond.Rows))
	}

	planShapeFirst, okFirst := explainFirst.Rows[0]["plan_shape"].(string)
	planShapeSecond, okSecond := explainSecond.Rows[0]["plan_shape"].(string)
	if !okFirst || !okSecond {
		t.Fatalf("expected plan_shape string in explain rows, got first=%T second=%T", explainFirst.Rows[0]["plan_shape"], explainSecond.Rows[0]["plan_shape"])
	}
	if planShapeFirst != planShapeSecond {
		t.Fatalf("non-deterministic explain plan shape:\nfirst=%s\nsecond=%s", planShapeFirst, planShapeSecond)
	}

	leadershipErr := connection.Invoke(ctx, "/asql.v1.ASQLService/LeadershipState", &LeadershipStateRequest{}, new(LeadershipStateResponse))
	if leadershipErr == nil {
		t.Fatal("expected invalid argument for empty group")
	}
	if status.Code(leadershipErr) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument for empty group, got %v", status.Code(leadershipErr))
	}
}

func TestNewRejectsLegacyClusterMode(t *testing.T) {
	server, err := New(Config{
		Address:     "bufnet",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      slog.Default(),
		NodeID:      "node-a",
		Peers:       []string{"node-b@127.0.0.1:6434"},
		Groups:      []string{"default"},
	})
	if !errors.Is(err, errLegacyClusterModeDisabled) {
		t.Fatalf("expected legacy cluster mode disabled error, got server=%v err=%v", server, err)
	}
}

func mustExecute(t *testing.T, ctx context.Context, connection *grpc.ClientConn, txID, sql string) {
	t.Helper()

	response := new(ExecuteResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/Execute", &ExecuteRequest{TxID: txID, SQL: sql}, response); err != nil {
		t.Fatalf("execute invoke for %q: %v", sql, err)
	}
}

func TestASQLServiceBlackBoxAuthToken(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()

	server, err := New(Config{
		Address:     "bufnet",
		DataDirPath: filepath.Join(temp, "data"),
		Logger:      slog.Default(),
		AuthToken:   "secret-token",
	})
	if err != nil {
		t.Fatalf("new grpc server: %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() {
		server.grpcServer.Stop()
		_ = listener.Close()
	})

	go func() {
		_ = server.grpcServer.Serve(listener)
	}()

	connection, err := grpc.DialContext(
		ctx,
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	beginResp := new(BeginTxResponse)
	err = connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &BeginTxRequest{Mode: "domain", Domains: []string{"accounts"}}, beginResp)
	if err == nil {
		t.Fatal("expected unauthenticated error when no token is present")
	}

	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated error code, got %v", status.Code(err))
	}

	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer secret-token")
	beginResp = new(BeginTxResponse)
	err = connection.Invoke(authCtx, "/asql.v1.ASQLService/BeginTx", &BeginTxRequest{Mode: "domain", Domains: []string{"accounts"}}, beginResp)
	if err != nil {
		t.Fatalf("expected authenticated request to pass: %v", err)
	}

	if beginResp.TxID == "" {
		t.Fatal("expected tx id after authenticated begin tx")
	}
}

func TestASQLServiceBlackBoxDatabasePrincipalAuthorization(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()

	server, err := New(Config{
		Address:     "bufnet",
		DataDirPath: filepath.Join(temp, "data"),
		Logger:      slog.Default(),
	})
	if err != nil {
		t.Fatalf("new grpc server: %v", err)
	}

	if err := server.engine.BootstrapAdminPrincipal(ctx, "admin", "admin-secret"); err != nil {
		t.Fatalf("bootstrap admin principal: %v", err)
	}
	if err := server.engine.CreateUser(ctx, "analyst", "analyst-secret"); err != nil {
		t.Fatalf("create analyst principal: %v", err)
	}
	if err := server.engine.CreateUser(ctx, "historian", "historian-secret"); err != nil {
		t.Fatalf("create historian principal: %v", err)
	}
	if err := server.engine.GrantPrivilege(ctx, "historian", executor.PrincipalPrivilegeSelectHistory); err != nil {
		t.Fatalf("grant historian select_history: %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() {
		server.grpcServer.Stop()
		_ = listener.Close()
	})

	go func() {
		_ = server.grpcServer.Serve(listener)
	}()

	connection, err := grpc.DialContext(
		ctx,
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &BeginTxRequest{Mode: "domain", Domains: []string{"accounts"}}, new(BeginTxResponse)); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected unauthenticated begin without database principal metadata, got %v", err)
	}

	adminCtx := withDatabasePrincipal(ctx, "admin", "admin-secret")
	adminBegin := new(BeginTxResponse)
	if err := connection.Invoke(adminCtx, "/asql.v1.ASQLService/BeginTx", &BeginTxRequest{Mode: "domain", Domains: []string{"accounts"}}, adminBegin); err != nil {
		t.Fatalf("begin admin schema tx: %v", err)
	}
	mustExecute(t, adminCtx, connection, adminBegin.TxID, "CREATE TABLE users (id INT, email TEXT)")
	mustExecute(t, adminCtx, connection, adminBegin.TxID, "INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')")
	adminCommit := new(CommitTxResponse)
	if err := connection.Invoke(adminCtx, "/asql.v1.ASQLService/CommitTx", &CommitTxRequest{TxID: adminBegin.TxID}, adminCommit); err != nil {
		t.Fatalf("commit admin schema tx: %v", err)
	}

	analystCtx := withDatabasePrincipal(ctx, "analyst", "analyst-secret")
	queryResp := new(QueryResponse)
	if err := connection.Invoke(analystCtx, "/asql.v1.ASQLService/Query", &QueryRequest{SQL: "SELECT id, email FROM users", Domains: []string{"accounts"}}, queryResp); err != nil {
		t.Fatalf("analyst current read: %v", err)
	}
	if len(queryResp.Rows) != 1 {
		t.Fatalf("unexpected analyst current-read row count: got %d want 1", len(queryResp.Rows))
	}

	analystBegin := new(BeginTxResponse)
	if err := connection.Invoke(analystCtx, "/asql.v1.ASQLService/BeginTx", &BeginTxRequest{Mode: "domain", Domains: []string{"accounts"}}, analystBegin); err != nil {
		t.Fatalf("begin analyst tx: %v", err)
	}
	insertErr := connection.Invoke(analystCtx, "/asql.v1.ASQLService/Execute", &ExecuteRequest{TxID: analystBegin.TxID, SQL: "INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')"}, new(ExecuteResponse))
	if status.Code(insertErr) != codes.PermissionDenied {
		t.Fatalf("expected analyst insert to be permission denied, got %v", insertErr)
	}
	rollbackResp := new(RollbackTxResponse)
	if err := connection.Invoke(analystCtx, "/asql.v1.ASQLService/RollbackTx", &RollbackTxRequest{TxID: analystBegin.TxID}, rollbackResp); err != nil {
		t.Fatalf("rollback analyst tx: %v", err)
	}

	timeTravelErr := connection.Invoke(analystCtx, "/asql.v1.ASQLService/TimeTravelQuery", &TimeTravelQueryRequest{SQL: "SELECT id, email FROM users", Domains: []string{"accounts"}, LSN: adminCommit.CommitLSN}, new(TimeTravelQueryResponse))
	if status.Code(timeTravelErr) != codes.PermissionDenied {
		t.Fatalf("expected analyst time-travel query to be permission denied, got %v", timeTravelErr)
	}

	historianCtx := withDatabasePrincipal(ctx, "historian", "historian-secret")
	historyResp := new(TimeTravelQueryResponse)
	if err := connection.Invoke(historianCtx, "/asql.v1.ASQLService/TimeTravelQuery", &TimeTravelQueryRequest{SQL: "SELECT id, email FROM users", Domains: []string{"accounts"}, LSN: adminCommit.CommitLSN}, historyResp); err != nil {
		t.Fatalf("historian time-travel query: %v", err)
	}
	if len(historyResp.Rows) != 1 {
		t.Fatalf("unexpected historian time-travel row count: got %d want 1", len(historyResp.Rows))
	}

	replayErr := connection.Invoke(analystCtx, "/asql.v1.ASQLService/ReplayToLSN", &ReplayToLSNRequest{LSN: adminCommit.CommitLSN}, new(ReplayToLSNResponse))
	if status.Code(replayErr) != codes.PermissionDenied {
		t.Fatalf("expected analyst replay to be permission denied, got %v", replayErr)
	}
	if err := connection.Invoke(adminCtx, "/asql.v1.ASQLService/ReplayToLSN", &ReplayToLSNRequest{LSN: adminCommit.CommitLSN}, new(ReplayToLSNResponse)); err != nil {
		t.Fatalf("admin replay to lsn: %v", err)
	}
	if err := connection.Invoke(adminCtx, "/asql.v1.ASQLService/ExplainQuery", &ExplainQueryRequest{SQL: "SELECT id, email FROM users", Domains: []string{"accounts"}}, new(ExplainQueryResponse)); err != nil {
		t.Fatalf("admin explain query: %v", err)
	}
}

func withDatabasePrincipal(ctx context.Context, principal, password string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, principalMetadataKey, principal, passwordMetadataKey, password)
}
