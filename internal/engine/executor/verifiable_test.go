package executor

import (
	"os"
	"testing"
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
