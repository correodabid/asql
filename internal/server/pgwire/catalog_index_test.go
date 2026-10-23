package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestCatalogListsIndexes: pg_index and pg_indexes list the primary key,
// single or composite, each UNIQUE column and the indexes made with
// CREATE INDEX, and pg_constraint points key constraints at their index.
func TestCatalogListsIndexes(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, sql := range []string{"BEGIN DOMAIN lab",
		"CREATE TABLE artifacts (id TEXT PRIMARY KEY, code TEXT UNIQUE, kind TEXT)",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id))",
		"CREATE INDEX artifacts_by_kind ON artifacts (kind) USING BTREE",
		"CREATE INDEX parents_by_parent ON artifact_parents (parent_id)",
		"COMMIT"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	rowsOf := func(sql string) []map[string]any {
		t.Helper()
		rows, err := conn.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		defer rows.Close()
		var out []map[string]any
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				t.Fatal(err)
			}
			row := map[string]any{}
			for i, f := range rows.FieldDescriptions() {
				row[f.Name] = values[i]
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}

	defs := map[string]string{}
	for _, row := range rowsOf("SELECT * FROM pg_catalog.pg_indexes WHERE schemaname = 'lab'") {
		if row["schemaname"] != "lab" {
			t.Fatalf("index %v in schema %v, want lab", row["indexname"], row["schemaname"])
		}
		defs[row["indexname"].(string)] = row["indexdef"].(string)
	}
	want := map[string]string{
		"artifacts_pkey":        "CREATE UNIQUE INDEX artifacts_pkey ON lab.artifacts USING btree (id)",
		"artifacts_code_key":    "CREATE UNIQUE INDEX artifacts_code_key ON lab.artifacts USING btree (code)",
		"artifacts_by_kind":     "CREATE INDEX artifacts_by_kind ON lab.artifacts USING btree (kind)",
		"artifact_parents_pkey": "CREATE UNIQUE INDEX artifact_parents_pkey ON lab.artifact_parents USING btree (child_id, parent_id)",
		"parents_by_parent":     "CREATE INDEX parents_by_parent ON lab.artifact_parents USING hash (parent_id)",
	}
	if len(defs) != len(want) {
		t.Fatalf("pg_indexes = %v, want %v", defs, want)
	}
	for name, def := range want {
		if defs[name] != def {
			t.Fatalf("pg_indexes %s = %q, want %q", name, defs[name], def)
		}
	}

	var names []string
	for _, row := range rowsOf("SELECT * FROM pg_indexes WHERE tablename = 'artifact_parents'") {
		names = append(names, row["indexname"].(string))
	}
	if got := strings.Join(names, ","); got != "artifact_parents_pkey,parents_by_parent" {
		t.Fatalf("pg_indexes for artifact_parents = %s", got)
	}

	// pg_index, for the composite key's table: the key with both columns.
	var relOID int64
	for _, row := range rowsOf("SELECT * FROM pg_catalog.pg_class") {
		if row["relname"] == "artifact_parents" {
			relOID = row["oid"].(int64)
		}
	}
	indexes := rowsOf(fmt.Sprintf("SELECT * FROM pg_catalog.pg_index i WHERE i.indrelid = %d", relOID))
	if len(indexes) != 2 {
		t.Fatalf("pg_index for artifact_parents = %v, want the key and parents_by_parent", indexes)
	}
	key, byParent := indexes[0], indexes[1]
	if key["indisprimary"] != true || key["indisunique"] != true || fmt.Sprint(key["indkey"]) != "1 2" || key["indrelid"] != relOID {
		t.Fatalf("composite key index = %v", key)
	}
	if byParent["indisprimary"] != false || byParent["indisunique"] != false || fmt.Sprint(byParent["indkey"]) != "2" {
		t.Fatalf("parents_by_parent index = %v", byParent)
	}

	var conindid any
	for _, row := range rowsOf("SELECT * FROM pg_catalog.pg_constraint") {
		if row["conrelid"] == relOID && row["contype"] == "p" {
			conindid = row["conindid"]
		}
	}
	if conindid != key["indexrelid"] {
		t.Fatalf("pg_constraint conindid = %v, want the key's index %v", conindid, key["indexrelid"])
	}
}
