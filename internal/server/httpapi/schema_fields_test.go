package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/wal"
)

// schemaFieldsSQL declares every table option the schema reports.
var schemaFieldsSQL = []string{
	"BEGIN DOMAIN lab",
	"CREATE TABLE artifacts (id TEXT PRIMARY KEY, note TEXT NOT NULL, kind TEXT) VERIFIABLE",
	"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE",
	"ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)",
	"COMMIT",
	"BEGIN DOMAIN lab",
	"INSERT INTO artifacts (id, note, kind) VALUES ('a', 'n', 'source')",
	"COMMIT",
}

// checkSchemaFields checks the tables schemaFieldsSQL declares.
func checkSchemaFields(t *testing.T, domains []SchemaSnapshotDomain) {
	t.Helper()
	tables := map[string]SchemaSnapshotTable{}
	for _, d := range domains {
		for _, table := range d.Tables {
			tables[table.Name] = table
		}
	}
	edges, artifacts := tables["artifact_parents"], tables["artifacts"]
	if strings.Join(edges.PrimaryKey, ",") != "child_id,parent_id" || !edges.AppendOnly || !edges.Verifiable {
		t.Fatalf("artifact_parents: %+v", edges)
	}
	if len(edges.VerifiableGroups) != 1 || edges.VerifiableGroups[0].References != "artifacts" ||
		strings.Join(edges.VerifiableGroups[0].Columns, ",") != "child_id" || strings.Join(edges.VerifiableGroups[0].ReferencedColumns, ",") != "id" {
		t.Fatalf("artifact_parents groups: %+v", edges.VerifiableGroups)
	}
	if strings.Join(artifacts.PrimaryKey, ",") != "id" || artifacts.AppendOnly || !artifacts.Verifiable {
		t.Fatalf("artifacts: %+v", artifacts)
	}
	notNull := map[string]bool{}
	primary := map[string]bool{}
	for _, table := range []SchemaSnapshotTable{edges, artifacts} {
		for _, c := range table.Columns {
			notNull[table.Name+"."+c.Name], primary[table.Name+"."+c.Name] = c.NotNull, c.PrimaryKey
		}
	}
	for column, want := range map[string]bool{
		"artifact_parents.child_id": true, "artifact_parents.parent_id": true, "artifact_parents.ordinal": false,
		"artifacts.id": true, "artifacts.note": true, "artifacts.kind": false,
	} {
		if notNull[column] != want {
			t.Fatalf("%s not_null = %v, want %v", column, notNull[column], want)
		}
	}
	// The per-column flag stays on single-column keys only.
	if !primary["artifacts.id"] || primary["artifact_parents.child_id"] || primary["artifact_parents.parent_id"] {
		t.Fatalf("per-column primary_key flags: %v", primary)
	}
}

func TestSchemaSnapshotAndTimelineCarryEveryField(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walStore, err := wal.NewSegmentedLogStore(filepath.Join(baseDir, "wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := executor.New(ctx, walStore, filepath.Join(baseDir, "snaps"))
	if err != nil {
		_ = walStore.Close()
		t.Fatal(err)
	}
	svc := newService(engine, walStore, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, "")
	mux := http.NewServeMux()
	svc.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		server.Close()
		close(svc.cleanupClose)
		svc.stopAuditWriter()
		engine.WaitPendingSnapshots()
		_ = walStore.Close()
	})
	session := engine.NewSession()
	for _, sql := range schemaFieldsSQL {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	var snapshot SchemaSnapshotResponse
	if status, _, body := doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/schema-snapshot", "", "", &SchemaSnapshotRequest{Domains: []string{"lab"}}, &snapshot); status != http.StatusOK {
		t.Fatalf("schema snapshot: %d %s", status, body)
	}
	checkSchemaFields(t, snapshot.Domains)

	var timeline TimelineCommitsResponse
	if status, _, body := doJSONRequest(t, server.URL, http.MethodPost, "/api/v1/timeline-commits", "", "", &TimelineCommitsRequest{}, &timeline); status != http.StatusOK {
		t.Fatalf("timeline commits: %d %s", status, body)
	}
	if len(timeline.Commits) == 0 {
		t.Fatal("no commits")
	}
	for _, c := range timeline.Commits {
		if c.CommitTime == nil || c.CommitTime.IsZero() {
			t.Fatalf("commit at LSN %d has no commit time", c.LSN)
		}
	}
}
