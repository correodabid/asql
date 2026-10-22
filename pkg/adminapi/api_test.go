package adminapi

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestSchemaAndTimelineTypesRoundTrip: every field the engine reports for
// a table, a column and a commit survives JSON, under the names
// asql_admin.schema_snapshot and timeline_commits use.
func TestSchemaAndTimelineTypesRoundTrip(t *testing.T) {
	commitTime := time.Date(2026, 10, 22, 9, 30, 0, 123456000, time.UTC)
	in := struct {
		Table  SchemaSnapshotTable
		Commit TimelineCommitEntry
	}{
		Table: SchemaSnapshotTable{
			Name: "artifact_parents",
			Columns: []SchemaSnapshotColumn{
				{Name: "child_id", Type: "TEXT", NotNull: true},
				{Name: "parent_id", Type: "TEXT", NotNull: true},
				{Name: "ordinal", Type: "INT"},
			},
			PrimaryKey: []string{"child_id", "parent_id"},
			AppendOnly: true,
			Verifiable: true,
			VerifiableGroups: []SchemaSnapshotVerifiableGroup{
				{Columns: []string{"child_id"}, References: "artifacts", ReferencedColumns: []string{"id"}},
			},
		},
		Commit: TimelineCommitEntry{LSN: 7, TxID: "tx-1", Timestamp: 7, CommitTime: &commitTime,
			Tables: []TimelineCommitMutationEntry{{Domain: "lab", Table: "artifacts", Operation: "INSERT"}}},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{`"not_null":true`, `"primary_key":["child_id","parent_id"]`, `"append_only":true`,
		`"verifiable":true`, `"verifiable_groups":[{"columns":["child_id"],"references":"artifacts","referenced_columns":["id"]}]`,
		`"commit_time":"2026-10-22T09:30:00.123456Z"`} {
		if !strings.Contains(string(data), name) {
			t.Fatalf("JSON lacks %s: %s", name, data)
		}
	}
	// The columns of a composite key leave the per-column flag false.
	if strings.Contains(string(data), `"primary_key":true`) {
		t.Fatalf("a composite key column carries primary_key: %s", data)
	}
	out := in
	out.Table, out.Commit = SchemaSnapshotTable{}, TimelineCommitEntry{}
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip changed it:\n%+v\n%+v", in, out)
	}

	// A commit written before commit times were recorded has none.
	data, _ = json.Marshal(TimelineCommitEntry{LSN: 1})
	if strings.Contains(string(data), "commit_time") {
		t.Fatalf("an untimed commit carries commit_time: %s", data)
	}
}
