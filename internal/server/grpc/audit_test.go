package grpc

import (
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestAuditEventsForTxAndAdminAPIs(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	handler := newAuditCaptureHandler()
	logger := slog.New(handler)

	server, err := New(Config{
		Address:     "bufnet",
		DataDirPath: filepath.Join(tempDir, "data"),
		Logger:      logger,
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

	begin := new(BeginTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &BeginTxRequest{Mode: "domain", Domains: []string{"audit"}}, begin); err != nil {
		t.Fatalf("begin tx invoke: %v", err)
	}

	mustExecute(t, ctx, connection, begin.TxID, "CREATE TABLE events (id INT, payload TEXT)")
	mustExecute(t, ctx, connection, begin.TxID, "INSERT INTO events (id, payload) VALUES (1, 'ok')")

	commit := new(CommitTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/CommitTx", &CommitTxRequest{TxID: begin.TxID}, commit); err != nil {
		t.Fatalf("commit tx invoke: %v", err)
	}

	timeTravel := new(TimeTravelQueryResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/TimeTravelQuery", &TimeTravelQueryRequest{SQL: "SELECT id, payload FROM events", Domains: []string{"audit"}, LSN: 4}, timeTravel); err != nil {
		t.Fatalf("time travel query invoke: %v", err)
	}

	replay := new(ReplayToLSNResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ReplayToLSN", &ReplayToLSNRequest{LSN: 4}, replay); err != nil {
		t.Fatalf("replay invoke: %v", err)
	}

	stats := new(ScanStrategyStatsResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ScanStrategyStats", &ScanStrategyStatsRequest{}, stats); err != nil {
		t.Fatalf("scan strategy stats invoke: %v", err)
	}

	if !handler.hasAuditOperation("tx.begin", "success") {
		t.Fatal("missing tx.begin audit event")
	}
	if !handler.hasAuditOperation("tx.execute", "success") {
		t.Fatal("missing tx.execute audit event")
	}
	if !handler.hasAuditOperation("tx.commit", "success") {
		t.Fatal("missing tx.commit audit event")
	}
	if !handler.hasAuditOperation("admin.time_travel_query", "success") {
		t.Fatal("missing admin.time_travel_query audit event")
	}
	if !handler.hasAuditOperation("admin.replay_to_lsn", "success") {
		t.Fatal("missing admin.replay_to_lsn audit event")
	}
	if !handler.hasAuditOperation("admin.scan_strategy_stats", "success") {
		t.Fatal("missing admin.scan_strategy_stats audit event")
	}
}

func TestAuditEventsIncludeFencingRejection(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	handler := newAuditCaptureHandler()
	logger := slog.New(handler)

	server, err := New(Config{
		Address:     "bufnet",
		DataDirPath: filepath.Join(tempDir, "data-fencing"),
		Logger:      logger,
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

	begin := new(BeginTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &BeginTxRequest{Mode: "domain", Domains: []string{"audit"}}, begin); err != nil {
		t.Fatalf("begin tx invoke: %v", err)
	}

	mustExecute(t, ctx, connection, begin.TxID, "CREATE TABLE events (id INT, payload TEXT)")

	commitErr := connection.Invoke(ctx, "/asql.v1.ASQLService/CommitTx", &CommitTxRequest{
		TxID:         begin.TxID,
		Group:        "audit",
		NodeID:       "node-stale",
		FencingToken: "stale-token",
	}, new(CommitTxResponse))
	if commitErr == nil {
		t.Fatal("expected commit fencing rejection error")
	}

	if !handler.hasAuditOperation("ha.fencing_rejection", "failure") {
		t.Fatal("missing ha.fencing_rejection audit event")
	}
}

type auditCaptureHandler struct {
	mu      sync.Mutex
	records []map[string]any
}

func newAuditCaptureHandler() *auditCaptureHandler {
	return &auditCaptureHandler{records: make([]map[string]any, 0)}
}

func (handler *auditCaptureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (handler *auditCaptureHandler) Handle(_ context.Context, record slog.Record) error {
	entry := map[string]any{"message": record.Message}
	record.Attrs(func(attr slog.Attr) bool {
		entry[attr.Key] = attr.Value.Any()
		return true
	})

	handler.mu.Lock()
	handler.records = append(handler.records, entry)
	handler.mu.Unlock()

	return nil
}

func (handler *auditCaptureHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return handler
}

func (handler *auditCaptureHandler) WithGroup(string) slog.Handler {
	return handler
}

func (handler *auditCaptureHandler) hasAuditOperation(operation, status string) bool {
	handler.mu.Lock()
	defer handler.mu.Unlock()

	for _, entry := range handler.records {
		event, _ := entry["event"].(string)
		op, _ := entry["operation"].(string)
		state, _ := entry["status"].(string)
		if event == "audit" && op == operation && state == status {
			return true
		}
	}

	return false
}
