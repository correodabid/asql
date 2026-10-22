package executor

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/sqlerr"
)

func schemaGroups(t *testing.T, n *appendOnlyNode, table string) []SchemaVerifiableGroup {
	t.Helper()
	for _, d := range n.engine.SchemaSnapshot(nil).Domains {
		for _, tbl := range d.Tables {
			if d.Name == "app" && tbl.Name == table {
				return tbl.VerifiableGroups
			}
		}
	}
	t.Fatalf("table %s not found", table)
	return nil
}

func viewGroupsOf(n *appendOnlyNode, table string) []VerifiableGroup {
	for _, tbl := range n.engine.VerifiableView().Tables() {
		if tbl.Domain == "app" && tbl.Name == table {
			return tbl.Groups
		}
	}
	return nil
}

func requireGroupState(t *testing.T, label string, err error, code, contains string) {
	t.Helper()
	var sqlErr *sqlerr.SQLError
	if !errors.As(err, &sqlErr) || sqlErr.Code != code || !strings.Contains(err.Error(), contains) {
		t.Fatalf("%s: %v, want %s containing %q", label, err, code, contains)
	}
}

const lineageSchema = "CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT) VERIFIABLE"

// TestVerifiableGroups: a group is declared on an edge table and references
// another verifiable table's primary key; it survives restarts, follows
// RENAME COLUMN and holds the columns it names.
func TestVerifiableGroups(t *testing.T) {
	n := openAppendOnlyNode(t, t.TempDir())
	n.mustTx(lineageSchema,
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE",
		// A composite referenced key.
		"CREATE TABLE versions (doc TEXT, rev INT, body TEXT, PRIMARY KEY (doc, rev)) VERIFIABLE",
		"CREATE TABLE version_notes (id INT PRIMARY KEY, doc TEXT, rev INT, note TEXT) VERIFIABLE")
	n.mustTx("ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)")
	n.mustTx("ALTER TABLE version_notes ADD VERIFIABLE GROUP (doc, rev) REFERENCES versions (doc, rev)")
	// A table may have several groups, and declare them in the transaction
	// that creates the tables.
	n.mustTx("CREATE TABLE links (a TEXT, b TEXT, PRIMARY KEY (a, b)) VERIFIABLE",
		"ALTER TABLE links ADD VERIFIABLE GROUP (a) REFERENCES artifacts (id)",
		"ALTER TABLE links ADD VERIFIABLE GROUP (b) REFERENCES artifacts (id)")

	check := func(when string) {
		t.Helper()
		want := map[string][]SchemaVerifiableGroup{
			"artifact_parents": {{Columns: []string{"child_id"}, References: "artifacts", ReferencedColumns: []string{"id"}}},
			"version_notes":    {{Columns: []string{"doc", "rev"}, References: "versions", ReferencedColumns: []string{"doc", "rev"}}},
			"links": {
				{Columns: []string{"a"}, References: "artifacts", ReferencedColumns: []string{"id"}},
				{Columns: []string{"b"}, References: "artifacts", ReferencedColumns: []string{"id"}},
			},
		}
		for table, groups := range want {
			got := schemaGroups(t, n, table)
			if len(got) != len(groups) {
				t.Fatalf("%s: %s groups %+v, want %+v", when, table, got, groups)
			}
			view := viewGroupsOf(n, table)
			for i := range groups {
				if !slices.Equal(got[i].Columns, groups[i].Columns) || got[i].References != groups[i].References || !slices.Equal(got[i].ReferencedColumns, groups[i].ReferencedColumns) {
					t.Fatalf("%s: %s group %d = %+v, want %+v", when, table, i, got[i], groups[i])
				}
				if !slices.Equal(view[i].Columns, groups[i].Columns) || view[i].RefTable != groups[i].References || !slices.Equal(view[i].RefColumns, groups[i].ReferencedColumns) {
					t.Fatalf("%s: %s view group %d = %+v", when, table, i, view[i])
				}
			}
		}
	}
	check("live")
	n.close()
	n.open()
	n.close()
	n.open()
	check("after a restart from a snapshot")
	n.close()
	if err := os.RemoveAll(n.snapDir); err != nil {
		t.Fatal(err)
	}
	n.open()
	defer n.close()
	check("after replay from the WAL")

	// The columns a group names stay.
	requireGroupState(t, "drop a group column", n.tx("ALTER TABLE version_notes DROP COLUMN doc"), "42P16", "group")
	requireGroupState(t, "drop a referenced table", n.tx("DROP TABLE versions"), "42P16", "referenced by the group")
	requireGroupState(t, "drop a referenced table with CASCADE", n.tx("DROP TABLE versions CASCADE"), "42P16", "referenced by the group")
	if err := n.tx("ALTER TABLE versions DROP COLUMN rev"); err == nil {
		t.Fatal("dropped a referenced primary key column")
	}
	// RENAME follows, in the group's table and in the referenced one.
	n.mustTx("ALTER TABLE version_notes RENAME COLUMN doc TO document")
	n.mustTx("ALTER TABLE versions RENAME COLUMN rev TO revision")
	got := schemaGroups(t, n, "version_notes")
	if len(got) != 1 || !slices.Equal(got[0].Columns, []string{"document", "rev"}) || !slices.Equal(got[0].ReferencedColumns, []string{"doc", "revision"}) {
		t.Fatalf("after renames: %+v", got)
	}
	// Dropping the group's own table drops its groups, and then the
	// referenced table can go.
	n.mustTx("DROP TABLE version_notes")
	n.mustTx("DROP TABLE versions")
}

func TestVerifiableGroupRules(t *testing.T) {
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	n.mustTx(lineageSchema,
		"CREATE TABLE edges (child_id TEXT, parent_id TEXT, n INT, PRIMARY KEY (child_id, parent_id)) VERIFIABLE",
		"CREATE TABLE plain_edges (child_id TEXT PRIMARY KEY)",
		"CREATE TABLE plain_artifacts (id TEXT PRIMARY KEY)",
		"CREATE TABLE pairs (a TEXT, b INT, PRIMARY KEY (a, b)) VERIFIABLE")
	n.mustTx("ALTER TABLE edges ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)")
	for sql, want := range map[string][2]string{
		"ALTER TABLE plain_edges ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)":  {"42P16", "plain_edges is not VERIFIABLE"},
		"ALTER TABLE edges ADD VERIFIABLE GROUP (parent_id) REFERENCES plain_artifacts (id)": {"42P16", "plain_artifacts is not VERIFIABLE"},
		"ALTER TABLE edges ADD VERIFIABLE GROUP (parent_id) REFERENCES artifacts (kind)":     {"42P16", "not the primary key"},
		"ALTER TABLE edges ADD VERIFIABLE GROUP (parent_id, n) REFERENCES artifacts (id)":    {"42P16", "2 columns"},
		"ALTER TABLE edges ADD VERIFIABLE GROUP (n) REFERENCES artifacts (id)":               {"42P16", "INT"},
		"ALTER TABLE edges ADD VERIFIABLE GROUP (nope) REFERENCES artifacts (id)":            {"42703", "nope"},
		"ALTER TABLE edges ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)":        {"42P16", "already has the group"},
		"ALTER TABLE edges ADD VERIFIABLE GROUP (child_id, n) REFERENCES pairs (b, a)":       {"42P16", "not the primary key"},
		"ALTER TABLE edges ADD VERIFIABLE GROUP (child_id) REFERENCES missing (id)":          {"42P01", "not found"},
	} {
		requireGroupState(t, sql, n.tx(sql), want[0], want[1])
	}
	// Checked at commit too: a table made non-verifiable is impossible, but
	// one created in the same transaction without VERIFIABLE is caught.
	requireGroupState(t, "same transaction", n.tx(
		"CREATE TABLE later (child_id TEXT PRIMARY KEY)",
		"ALTER TABLE later ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)",
	), "42P16", "later is not VERIFIABLE")
	for _, sql := range []string{
		"ALTER TABLE edges ADD VERIFIABLE GROUP child_id REFERENCES artifacts (id)",
		"ALTER TABLE edges ADD VERIFIABLE GROUP (child_id) artifacts (id)",
		"ALTER TABLE edges ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts",
		"ALTER TABLE edges ADD VERIFIABLE GROUP () REFERENCES artifacts (id)",
	} {
		if err := n.tx(sql); err == nil {
			t.Fatalf("%s: accepted", sql)
		}
	}
	if issues := migrationIssuesFor(t, n, "ALTER TABLE pairs ADD VERIFIABLE GROUP (a) REFERENCES artifacts (id)"); !strings.Contains(issues, "cannot be undone") {
		t.Fatalf("migration rollback issues: %s", issues)
	}
}

func migrationIssuesFor(t *testing.T, n *appendOnlyNode, sql string) string {
	t.Helper()
	_, issues, err := n.engine.generateRollbackSQL("app", []string{sql})
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(issues, "; ")
}
