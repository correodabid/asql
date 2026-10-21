package executor

import (
	"context"
	"os"
	"testing"
)

// TestJSONPathComparedWithColumnsAndSubqueries (ASQL-14): a JSON path
// compared with a column, on either side, or with a subquery matches the
// rows whose path value equals it. It used to fail to parse with 42601. A
// path the payload does not have is NULL and matches nothing, not even !=.
// Writes with such a predicate find their rows live and on replay, which
// re-parses them from the WAL.
func TestJSONPathComparedWithColumnsAndSubqueries(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	n := openAppendOnlyNode(t, dir)
	n.mustTx("CREATE TABLE artifacts (id TEXT PRIMARY KEY, producer_name TEXT, payload JSON, score INT)")
	n.mustTx("CREATE INDEX artifacts_producer ON artifacts (producer_name) USING HASH")
	n.mustTx(`INSERT INTO artifacts (id, producer_name, payload, score) VALUES
		('a', 'lab', '{"same": "a", "source_id": "b"}', 1),
		('b', 'b', '{"same": "x", "source_id": "c"}', 2),
		('c', 'ops', '{"same": "ops", "source_id": "a"}', 3),
		('d', 'lab', '{"other": 1}', 4)`)
	n.mustTx("CREATE TABLE refs (id INT PRIMARY KEY, target TEXT)")
	n.mustTx("INSERT INTO refs (id, target) VALUES (1, 'c'), (2, 'nothing')")

	ids := func(sql string) string {
		t.Helper()
		res, err := n.engine.Query(ctx, sql, []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		out := ""
		for _, row := range res.Rows {
			out += row["id"].StringValue
		}
		return out
	}
	for where, want := range map[string]string{
		// The ticket's four queries.
		"payload->>'source_id' = 'b'":                                       "a",
		"id = payload->>'same'":                                             "a",
		"payload->>'same' = id":                                             "a",
		"producer_name = payload->>'same'":                                  "c",
		"payload->>'same' = producer_name":                                  "c",
		"payload->>'same' != id":                                            "bc",
		"id < payload->>'same'":                                             "bc",
		"payload->>'same' > id":                                             "bc",
		"payload->>'source_id' = (SELECT target FROM refs WHERE id = 1)":    "b",
		"payload->>'source_id' = (SELECT target FROM refs WHERE id = 2)":    "",
		"id = (SELECT payload->>'source_id' FROM artifacts WHERE id = 'c')": "a",
	} {
		sql := "SELECT id FROM artifacts WHERE " + where + " ORDER BY id"
		if got := ids(sql); got != want {
			t.Fatalf("%s: ids %q, want %q", sql, got, want)
		}
	}

	n.mustTx("UPDATE artifacts SET score = 99 WHERE producer_name = payload->>'same'")
	n.mustTx("DELETE FROM artifacts WHERE payload->>'same' = id")
	check := func(when string) {
		t.Helper()
		if got := ids("SELECT id FROM artifacts WHERE score = 99 ORDER BY id"); got != "c" {
			t.Fatalf("%s: UPDATE changed %q, want c", when, got)
		}
		if got := ids("SELECT id FROM artifacts ORDER BY id"); got != "bcd" {
			t.Fatalf("%s: DELETE left %q, want bcd", when, got)
		}
	}
	check("live")

	// Replay the WAL alone.
	n.close()
	if err := os.RemoveAll(n.snapDir); err != nil {
		t.Fatal(err)
	}
	n = openAppendOnlyNode(t, dir)
	defer n.close()
	check("after replay")
}
