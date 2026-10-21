package executor

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/sqlerr"
)

func requireSQLState(t *testing.T, label string, err error, code string) {
	t.Helper()
	var sqlErr *sqlerr.SQLError
	if !errors.As(err, &sqlErr) || sqlErr.Code != code {
		t.Fatalf("%s: error %v, want SQLSTATE %s", label, err, code)
	}
}

func schemaTablePrimaryKey(schema SchemaSnapshot, domain, table string) string {
	for _, d := range schema.Domains {
		for _, tbl := range d.Tables {
			if d.Name == domain && tbl.Name == table {
				return strings.Join(tbl.PrimaryKey, ",")
			}
		}
	}
	return ""
}

// TestCompositePrimaryKey: PRIMARY KEY (a, b) makes the tuple unique and its
// columns NOT NULL; ON CONFLICT on the key uses it; it survives restarts.
func TestCompositePrimaryKey(t *testing.T) {
	n := openAppendOnlyNode(t, t.TempDir())
	n.mustTx(
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY",
		"CREATE TABLE links (a TEXT, b TEXT, note TEXT, PRIMARY KEY (b, a))",
	)
	// The same child with other parents, and the same parent with other
	// children, are distinct keys.
	n.mustTx(
		"INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c1', 'p1', 1), ('c1', 'p2', 2), ('c2', 'p1', 1)",
		"INSERT INTO links (a, b, note) VALUES ('x', 'y', 'first'), ('y', 'x', 'other')",
	)

	requireSQLState(t, "repeated key", n.tx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c1', 'p2', 9)"), "23505")
	requireSQLState(t, "repeated key within one statement", n.tx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c3', 'p1', 1), ('c3', 'p1', 2)"), "23505")
	requireSQLState(t, "repeated key within one transaction", n.tx(
		"INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c4', 'p1', 1)",
		"INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c4', 'p1', 2)",
	), "23505")
	requireSQLState(t, "NULL key column", n.tx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c5', NULL, 1)"), "23502")
	requireSQLState(t, "missing key column", n.tx("INSERT INTO artifact_parents (child_id, ordinal) VALUES ('c5', 1)"), "23502")

	// ON CONFLICT on the key, in either column order.
	n.mustTx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c1', 'p1', 7) ON CONFLICT (child_id, parent_id) DO NOTHING")
	n.mustTx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c1', 'p1', 7) ON CONFLICT (parent_id, child_id) DO NOTHING")
	n.mustTx("INSERT INTO links (a, b, note) VALUES ('x', 'y', 'second') ON CONFLICT (a, b) DO UPDATE SET note = EXCLUDED.note")
	if got := n.count("SELECT * FROM links WHERE note = 'second'"); got != 1 {
		t.Fatalf("ON CONFLICT DO UPDATE: %d rows updated, want 1", got)
	}
	// An UPDATE that would repeat a key fails.
	requireSQLState(t, "UPDATE to a repeated key", n.tx("UPDATE links SET a = 'x', b = 'y' WHERE note = 'other'"), "23505")
	requireSQLState(t, "UPDATE a key column to NULL", n.tx("UPDATE links SET a = NULL WHERE note = 'other'"), "23502")
	if err := n.tx("ALTER TABLE links DROP COLUMN a"); err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("DROP COLUMN of a key column: %v", err)
	}

	check := func(when string) {
		t.Helper()
		if got := n.count("SELECT * FROM artifact_parents"); got != 3 {
			t.Fatalf("%s: %d rows, want 3", when, got)
		}
		schema := n.engine.SchemaSnapshot(nil)
		if got := schemaTablePrimaryKey(schema, "app", "artifact_parents"); got != "child_id,parent_id" {
			t.Fatalf("%s: schema primary key %q", when, got)
		}
		if got := schemaTablePrimaryKey(schema, "app", "links"); got != "b,a" {
			t.Fatalf("%s: links primary key %q, want key order b,a", when, got)
		}
		requireSQLState(t, when, n.tx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c2', 'p1', 5)"), "23505")
		n.mustTx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c2', 'p1', 5) ON CONFLICT (child_id, parent_id) DO NOTHING")
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
}

// TestAddPrimaryKey: ALTER TABLE ... ADD PRIMARY KEY on a table with rows
// takes effect only if the rows already satisfy it.
func TestAddPrimaryKey(t *testing.T) {
	n := openAppendOnlyNode(t, t.TempDir())
	n.mustTx(
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT) APPEND ONLY",
		"CREATE TABLE dup (a TEXT, b TEXT)",
		"CREATE TABLE nulls (a TEXT, b TEXT)",
		"CREATE TABLE single (id TEXT, v INT)",
	)
	n.mustTx(
		"INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c1', 'p1', 1), ('c1', 'p2', 2), ('c2', 'p1', 1)",
		"INSERT INTO dup (a, b) VALUES ('x', 'y'), ('x', 'y')",
		"INSERT INTO nulls (a, b) VALUES ('x', NULL)",
		"INSERT INTO single (id, v) VALUES ('a', 1), ('b', 2)",
	)

	// On an append-only table: it changes no row.
	n.mustTx("ALTER TABLE artifact_parents ADD PRIMARY KEY (child_id, parent_id)")
	requireSQLState(t, "repeated key after ADD PRIMARY KEY", n.tx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c1', 'p1', 3)"), "23505")
	n.mustTx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c2', 'p2', 2)")

	requireSQLState(t, "rows with a repeated key", n.tx("ALTER TABLE dup ADD PRIMARY KEY (a, b)"), "23505")
	requireSQLState(t, "rows with a NULL key column", n.tx("ALTER TABLE nulls ADD PRIMARY KEY (a, b)"), "23502")
	// Nothing changed: the rows still go in.
	n.mustTx("INSERT INTO dup (a, b) VALUES ('x', 'y')", "INSERT INTO nulls (a, b) VALUES (NULL, NULL)")
	if got := schemaTablePrimaryKey(n.engine.SchemaSnapshot(nil), "app", "dup"); got != "" {
		t.Fatalf("a failed ADD PRIMARY KEY left key %q", got)
	}

	if err := n.tx("ALTER TABLE artifact_parents ADD PRIMARY KEY (child_id)"); err == nil || !strings.Contains(err.Error(), "already has a primary key") {
		t.Fatalf("a second primary key: %v", err)
	}
	if err := n.tx("ALTER TABLE dup ADD PRIMARY KEY (a, missing)"); err == nil {
		t.Fatal("ADD PRIMARY KEY on a missing column accepted")
	}

	// One column: the usual single-column key.
	n.mustTx("ALTER TABLE single ADD PRIMARY KEY (id)")
	requireSQLState(t, "single-column key", n.tx("INSERT INTO single (id, v) VALUES ('a', 3)"), "23505")

	check := func(when string) {
		t.Helper()
		schema := n.engine.SchemaSnapshot(nil)
		if got := schemaTablePrimaryKey(schema, "app", "artifact_parents"); got != "child_id,parent_id" {
			t.Fatalf("%s: artifact_parents key %q", when, got)
		}
		if got := schemaTablePrimaryKey(schema, "app", "single"); got != "id" {
			t.Fatalf("%s: single key %q", when, got)
		}
		requireSQLState(t, when, n.tx("INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('c2', 'p2', 9)"), "23505")
		requireSQLState(t, when, n.tx("INSERT INTO single (id, v) VALUES ('b', 9)"), "23505")
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
}
