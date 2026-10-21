package fixtures

import (
	"strings"
	"testing"
)

// A composite primary key survives fixture export as a table constraint, and
// its implicit index is not exported as a CREATE INDEX.
func TestExportKeepsCompositePrimaryKey(t *testing.T) {
	statements := buildCreateTableStatements(exportTable{
		Domain: "lab",
		Snapshot: schemaSnapshotTable{
			Name: "artifact_parents",
			Columns: []schemaSnapshotColumn{
				{Name: "child_id", Type: "TEXT"}, {Name: "parent_id", Type: "TEXT"}, {Name: "ordinal", Type: "INT"},
			},
			Indexes:    []schemaSnapshotIndex{{Name: "__pk_artifact_parents", Columns: []string{"child_id", "parent_id"}, Method: "hash"}},
			PrimaryKey: []string{"child_id", "parent_id"},
		},
	})
	if len(statements) != 1 || !strings.HasSuffix(statements[0], ", PRIMARY KEY (child_id, parent_id))") {
		t.Fatalf("statements: %q", statements)
	}
}
