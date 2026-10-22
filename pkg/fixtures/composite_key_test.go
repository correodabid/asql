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

// APPEND ONLY and VERIFIABLE follow the column list, and a verifiable group
// is declared with ALTER TABLE.
func TestExportKeepsTableOptions(t *testing.T) {
	table := exportTable{
		Domain: "lab",
		Snapshot: schemaSnapshotTable{
			Name:       "artifact_parents",
			Columns:    []schemaSnapshotColumn{{Name: "child_id", Type: "TEXT"}, {Name: "parent_id", Type: "TEXT"}},
			PrimaryKey: []string{"child_id", "parent_id"},
			AppendOnly: true,
			Verifiable: true,
			VerifiableGroups: []schemaSnapshotGroup{
				{Columns: []string{"child_id"}, References: "artifacts", ReferencedColumns: []string{"id"}},
			},
		},
	}
	if statements := buildCreateTableStatements(table); len(statements) != 1 || !strings.HasSuffix(statements[0], ", PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE") {
		t.Fatalf("create: %q", statements)
	}
	if got := buildGroupStatements(table); len(got) != 1 || got[0] != "ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)" {
		t.Fatalf("groups: %q", got)
	}
}
