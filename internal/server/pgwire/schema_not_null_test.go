package pgwire

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestSchemaReportsNotNullAndCompositeKeys: schema_snapshot reports NOT
// NULL for declared columns and every primary key column, and the
// pg_catalog emulation uses it, with a composite key as one constraint.
func TestSchemaReportsNotNullAndCompositeKeys(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, sql := range []string{"BEGIN DOMAIN lab",
		"CREATE TABLE artifacts (id TEXT PRIMARY KEY, note TEXT NOT NULL, kind TEXT)",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id))",
		"COMMIT"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	want := map[string]bool{
		"artifacts.id": true, "artifacts.note": true, "artifacts.kind": false,
		"artifact_parents.child_id": true, "artifact_parents.parent_id": true, "artifact_parents.ordinal": false,
	}

	var raw string
	if err := conn.QueryRow(ctx, "SELECT snapshot FROM asql_admin.schema_snapshot").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var snap struct {
		Domains []struct {
			Name   string `json:"name"`
			Tables []struct {
				Name    string `json:"name"`
				Columns []struct {
					Name    string `json:"name"`
					NotNull bool   `json:"not_null"`
				} `json:"columns"`
			} `json:"tables"`
		} `json:"domains"`
	}
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, d := range snap.Domains {
		for _, table := range d.Tables {
			for _, c := range table.Columns {
				got[table.Name+"."+c.Name] = c.NotNull
			}
		}
	}
	for column, w := range want {
		if got[column] != w {
			t.Fatalf("schema_snapshot %s not_null = %v, want %v\n%s", column, got[column], w, raw)
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
		return out
	}
	for _, row := range rowsOf("SELECT * FROM information_schema.columns") {
		column := row["table_name"].(string) + "." + row["column_name"].(string)
		if w, ok := want[column]; ok && (row["is_nullable"] == "NO") != w {
			t.Fatalf("information_schema %s is_nullable = %v, want NOT NULL %v", column, row["is_nullable"], w)
		}
	}
	notNullAttrs := 0
	for _, row := range rowsOf("SELECT * FROM pg_catalog.pg_attribute") {
		if row["attname"] == "child_id" || row["attname"] == "parent_id" || row["attname"] == "note" {
			if row["attnotnull"] != true {
				t.Fatalf("pg_attribute %v attnotnull = %v", row["attname"], row["attnotnull"])
			}
			notNullAttrs++
		}
	}
	if notNullAttrs != 3 {
		t.Fatalf("%d not-null attributes found, want 3", notNullAttrs)
	}
	var pkeys []string
	for _, row := range rowsOf("SELECT * FROM pg_catalog.pg_constraint") {
		if row["contype"] == "p" {
			pkeys = append(pkeys, row["conname"].(string)+row["conkey"].(string))
		}
	}
	if joined := strings.Join(pkeys, " "); !strings.Contains(joined, "artifact_parents_pkey{1,2}") || !strings.Contains(joined, "artifacts_pkey{1}") {
		t.Fatalf("primary key constraints: %v", pkeys)
	}
}
