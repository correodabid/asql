package studioapp

import (
	"strings"
	"testing"
)

func TestBuildSchemaDDLScriptDeterministicOrder(t *testing.T) {
	request := schemaDDLRequest{
		Domain: "accounts",
		Tables: []schemaDDLTable{
			{
				Name: "users",
				Columns: []schemaDDLColumn{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "email", Type: "TEXT", Nullable: false, Unique: true},
				},
			},
			{
				Name: "orders",
				Columns: []schemaDDLColumn{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "user_id", Type: "INT", Nullable: false, References: &schemaDDLReference{Table: "users", Column: "id"}},
				},
			},
		},
	}

	first, err := BuildSchemaDDLScript(request)
	if err != nil {
		t.Fatalf("first build failed: %v", err)
	}
	second, err := BuildSchemaDDLScript(request)
	if err != nil {
		t.Fatalf("second build failed: %v", err)
	}
	if first.DDL != second.DDL {
		t.Fatalf("expected deterministic DDL output\nfirst:\n%s\nsecond:\n%s", first.DDL, second.DDL)
	}

	if !strings.Contains(first.DDL, "BEGIN DOMAIN accounts;") {
		t.Fatalf("expected domain begin statement, got:\n%s", first.DDL)
	}
	if !strings.Contains(first.DDL, "CREATE TABLE orders") {
		t.Fatalf("expected orders table in ddl, got:\n%s", first.DDL)
	}
	if !strings.Contains(first.DDL, "CREATE TABLE users") {
		t.Fatalf("expected users table in ddl, got:\n%s", first.DDL)
	}
	if !strings.Contains(first.DDL, "ALTER TABLE orders ADD CONSTRAINT fk_orders_user_id__users_id FOREIGN KEY (user_id) REFERENCES users(id);") {
		t.Fatalf("expected deterministic fk statement, got:\n%s", first.DDL)
	}
}

func TestBuildSchemaDDLScriptRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		request schemaDDLRequest
	}{
		{
			name:    "no tables",
			request: schemaDDLRequest{Domain: "accounts"},
		},
		{
			name: "invalid table name",
			request: schemaDDLRequest{
				Domain: "accounts",
				Tables: []schemaDDLTable{{Name: "bad-table", Columns: []schemaDDLColumn{{Name: "id", Type: "INT"}}}},
			},
		},
		{
			name: "invalid type",
			request: schemaDDLRequest{
				Domain: "accounts",
				Tables: []schemaDDLTable{{Name: "users", Columns: []schemaDDLColumn{{Name: "id", Type: "INT;DROP"}}}},
			},
		},
		{
			name: "fk missing ref table",
			request: schemaDDLRequest{
				Domain: "accounts",
				Tables: []schemaDDLTable{{
					Name:    "orders",
					Columns: []schemaDDLColumn{{Name: "user_id", Type: "INT", References: &schemaDDLReference{Table: "users", Column: "id"}}},
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := BuildSchemaDDLScript(tt.request); err == nil {
				t.Fatalf("expected error for %s", tt.name)
			}
		})
	}
}
