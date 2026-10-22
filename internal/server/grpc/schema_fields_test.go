package grpc

import (
	"context"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// TestSchemaSnapshotAndTimelineCarryEveryField: over gRPC, the schema
// snapshot reports composite primary keys, NOT NULL, APPEND ONLY,
// VERIFIABLE and verifiable groups, and timeline commits their commit time.
func TestSchemaSnapshotAndTimelineCarryEveryField(t *testing.T) {
	ctx := context.Background()
	server, err := New(Config{Address: "bufnet", DataDirPath: filepath.Join(t.TempDir(), "data"), Logger: slog.Default()})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() {
		server.grpcServer.Stop()
		_ = listener.Close()
	})
	go func() { _ = server.grpcServer.Serve(listener) }()
	connection, err := grpc.DialContext(ctx, "passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	tx := func(sqls ...string) {
		t.Helper()
		begin := new(BeginTxResponse)
		if err := connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &BeginTxRequest{Mode: "domain", Domains: []string{"lab"}}, begin); err != nil {
			t.Fatal(err)
		}
		for _, sql := range sqls {
			mustExecute(t, ctx, connection, begin.TxID, sql)
		}
		if err := connection.Invoke(ctx, "/asql.v1.ASQLService/CommitTx", &CommitTxRequest{TxID: begin.TxID}, new(CommitTxResponse)); err != nil {
			t.Fatal(err)
		}
	}
	tx("CREATE TABLE artifacts (id TEXT PRIMARY KEY, note TEXT NOT NULL) VERIFIABLE",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE",
		"ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)")
	tx("INSERT INTO artifacts (id, note) VALUES ('a', 'n')")

	snapshot := new(SchemaSnapshotResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/SchemaSnapshot", &SchemaSnapshotRequest{Domains: []string{"lab"}}, snapshot); err != nil {
		t.Fatal(err)
	}
	tables := map[string]SchemaSnapshotTable{}
	for _, table := range snapshot.Domains[0].Tables {
		tables[table.Name] = table
	}
	edges := tables["artifact_parents"]
	if strings.Join(edges.PrimaryKey, ",") != "child_id,parent_id" || !edges.AppendOnly || !edges.Verifiable || len(edges.VerifiableGroups) != 1 ||
		edges.VerifiableGroups[0].References != "artifacts" || !tables["artifacts"].Verifiable {
		t.Fatalf("schema over gRPC: %+v", tables)
	}
	for _, c := range append(edges.Columns, tables["artifacts"].Columns...) {
		want := c.Name != "ordinal" && !strings.HasPrefix(c.Name, "_")
		if c.NotNull != want {
			t.Fatalf("column %s not_null = %v, want %v", c.Name, c.NotNull, want)
		}
	}

	timeline := new(TimelineCommitsResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/TimelineCommits", &TimelineCommitsRequest{}, timeline); err != nil {
		t.Fatal(err)
	}
	if len(timeline.Commits) == 0 {
		t.Fatal("no commits")
	}
	for _, c := range timeline.Commits {
		if c.CommitTime == nil {
			t.Fatalf("commit at LSN %d has no commit time over gRPC", c.LSN)
		}
	}
}
