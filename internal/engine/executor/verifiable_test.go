package executor

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/sqlerr"
)

func schemaTableVerifiable(schema SchemaSnapshot, domain, table string) bool {
	for _, d := range schema.Domains {
		for _, tbl := range d.Tables {
			if d.Name == domain && tbl.Name == table {
				return tbl.Verifiable
			}
		}
	}
	return false
}

// TestVerifiableTables: VERIFIABLE marks a table for the ledger's state tree
// (ADR 0008) when it is created or later, combines with APPEND ONLY, does not
// restrict what the table accepts, and survives restarts.
func TestVerifiableTables(t *testing.T) {
	n := openAppendOnlyNode(t, t.TempDir())
	n.mustTx(
		"CREATE TABLE results (id INT PRIMARY KEY, result TEXT) VERIFIABLE",
		"CREATE TABLE evidence (id INT PRIMARY KEY, sha TEXT) APPEND ONLY VERIFIABLE",
		"CREATE TABLE findings (id INT PRIMARY KEY, verdict TEXT) VERIFIABLE APPEND ONLY",
		"CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)",
	)
	n.mustTx("INSERT INTO notes (id, body) VALUES (1, 'a')", "INSERT INTO results (id, result) VALUES (1, 'WITHIN_SPEC')")
	n.mustTx("ALTER TABLE notes SET VERIFIABLE")
	// Setting it again is a no-op.
	n.mustTx("ALTER TABLE notes SET VERIFIABLE")

	// Verifiable alone restricts nothing.
	n.mustTx("UPDATE results SET result = 'OUT_OF_SPEC' WHERE id = 1", "DELETE FROM notes WHERE id = 1")
	// With APPEND ONLY, the append-only rules still apply.
	requireAppendOnlyError(t, "verifiable and append-only", n.tx("DELETE FROM evidence WHERE id = 1"))
	requireAppendOnlyError(t, "append-only and verifiable", n.tx("DROP TABLE findings"))

	check := func(when string) {
		t.Helper()
		schema := n.engine.SchemaSnapshot(nil)
		for table, want := range map[string]bool{"results": true, "evidence": true, "findings": true, "notes": true} {
			if got := schemaTableVerifiable(schema, "app", table); got != want {
				t.Fatalf("%s: %s verifiable = %v, want %v", when, table, got, want)
			}
		}
		if schemaTableAppendOnly(schema, "app", "results") || !schemaTableAppendOnly(schema, "app", "findings") {
			t.Fatalf("%s: VERIFIABLE changed which tables are append-only", when)
		}
	}
	check("live")

	// From a snapshot (the first restart checkpoints the head, so the second
	// replays nothing), then from the WAL alone.
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

func TestVerifiableOptionsParse(t *testing.T) {
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	for _, sql := range []string{
		"CREATE TABLE a (id INT PRIMARY KEY) VERIFIABLE VERIFIABLE",
		"CREATE TABLE b (id INT PRIMARY KEY) APPEND ONLY APPEND ONLY",
		"CREATE TABLE c (id INT PRIMARY KEY) VERIFY",
		"CREATE TABLE d (id INT PRIMARY KEY) APPEND VERIFIABLE",
		"ALTER TABLE missing SET VERIFIABLE",
	} {
		if err := n.tx(sql); err == nil {
			t.Fatalf("%s: accepted", sql)
		}
	}
}

// TestVerifiableNeedsAPrimaryKey: rows are proven by primary key, so a table
// without one cannot be VERIFIABLE, when it is created, set later, or set in
// the transaction that creates it.
func TestVerifiableNeedsAPrimaryKey(t *testing.T) {
	n := openAppendOnlyNode(t, t.TempDir())
	defer n.close()
	n.mustTx("CREATE TABLE edges (child_id TEXT, parent_id TEXT, ordinal INT) APPEND ONLY")
	for _, statements := range [][]string{
		{"CREATE TABLE loose (a TEXT, b TEXT) VERIFIABLE"},
		{"ALTER TABLE edges SET VERIFIABLE"},
		{"CREATE TABLE later (a TEXT, b TEXT)", "ALTER TABLE later SET VERIFIABLE"},
	} {
		var sqlErr *sqlerr.SQLError
		if err := n.tx(statements...); !errors.As(err, &sqlErr) || sqlErr.Code != "42P16" || !strings.Contains(err.Error(), "primary key") {
			t.Fatalf("%v: %v, want 42P16 about the primary key", statements, err)
		}
	}
	if schemaTableVerifiable(n.engine.SchemaSnapshot(nil), "app", "edges") {
		t.Fatal("a table without a primary key became verifiable")
	}

	// The integration's path: a composite key added to the existing
	// append-only edge table, then VERIFIABLE; and a composite key at
	// creation.
	n.mustTx("INSERT INTO edges (child_id, parent_id, ordinal) VALUES ('b', 'a', 1)")
	n.mustTx("ALTER TABLE edges ADD PRIMARY KEY (child_id, parent_id)")
	n.mustTx("ALTER TABLE edges SET VERIFIABLE")
	n.mustTx("CREATE TABLE links (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE")
	schema := n.engine.SchemaSnapshot(nil)
	if !schemaTableVerifiable(schema, "app", "edges") || !schemaTableVerifiable(schema, "app", "links") {
		t.Fatal("tables with a composite primary key are not verifiable")
	}
	// The failed transactions left nothing behind.
	for _, d := range n.engine.SchemaSnapshot(nil).Domains {
		for _, tbl := range d.Tables {
			if tbl.Name == "loose" || tbl.Name == "later" {
				t.Fatalf("table %s exists after its transaction failed", tbl.Name)
			}
		}
	}
}
