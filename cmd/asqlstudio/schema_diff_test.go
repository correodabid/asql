package main

import (
	"strings"
	"testing"
)

func TestBuildSchemaDiffAddTableAndColumnSafe(t *testing.T) {
	base := schemaDDLRequest{
		Domain: "accounts",
		Tables: []schemaDDLTable{{
			Name: "users",
			Columns: []schemaDDLColumn{{
				Name:       "id",
				Type:       "INT",
				PrimaryKey: true,
			}},
		}},
	}
	target := schemaDDLRequest{
		Domain: "accounts",
		Tables: []schemaDDLTable{
			{
				Name: "users",
				Columns: []schemaDDLColumn{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "email", Type: "TEXT", Nullable: true},
				},
			},
			{
				Name: "orders",
				Columns: []schemaDDLColumn{
					{Name: "id", Type: "INT", PrimaryKey: true},
				},
			},
		},
	}

	diff, err := BuildSchemaDiff(base, target)
	if err != nil {
		t.Fatalf("build schema diff failed: %v", err)
	}
	if !diff.Safe {
		t.Fatalf("expected safe diff, got unsafe with warnings %v", diff.Warnings)
	}
	if len(diff.Statements) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(diff.Statements))
	}
	if !strings.Contains(diff.Statements[0], "CREATE TABLE orders") && !strings.Contains(diff.Statements[1], "CREATE TABLE orders") {
		t.Fatalf("expected create table statement in diff statements: %v", diff.Statements)
	}
}

func TestBuildSchemaDiffDetectsDestructiveChanges(t *testing.T) {
	base := schemaDDLRequest{
		Domain: "accounts",
		Tables: []schemaDDLTable{{
			Name: "users",
			Columns: []schemaDDLColumn{{
				Name:       "id",
				Type:       "INT",
				PrimaryKey: true,
			}, {
				Name: "email",
				Type: "TEXT",
			}},
		}},
	}
	target := schemaDDLRequest{
		Domain: "accounts",
		Tables: []schemaDDLTable{{
			Name: "users",
			Columns: []schemaDDLColumn{{
				Name:       "id",
				Type:       "INT",
				PrimaryKey: true,
			}},
		}},
	}

	diff, err := BuildSchemaDiff(base, target)
	if err != nil {
		t.Fatalf("build schema diff failed: %v", err)
	}
	if diff.Safe {
		t.Fatalf("expected unsafe diff due to dropped column")
	}
	if len(diff.Warnings) == 0 {
		t.Fatalf("expected warnings for destructive change")
	}

	hasDropColumn := false
	for _, op := range diff.Operations {
		if op.Type == "drop_column" {
			hasDropColumn = true
			break
		}
	}
	if !hasDropColumn {
		t.Fatalf("expected drop_column operation, got %v", diff.Operations)
	}
}
