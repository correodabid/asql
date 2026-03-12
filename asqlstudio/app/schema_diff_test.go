package studioapp

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

func TestBuildSchemaDiffWarnsOnVersionedForeignKeyChange(t *testing.T) {
	base := schemaDDLRequest{
		Domain: "manufacturing",
		Tables: []schemaDDLTable{
			{
				Name: "batch_orders",
				Columns: []schemaDDLColumn{
					{Name: "id", Type: "TEXT", PrimaryKey: true},
					{Name: "recipe_id", Type: "TEXT"},
					{Name: "recipe_version", Type: "INT"},
				},
				VersionedForeignKeys: []schemaDDLVersionedFK{{
					Column:           "recipe_id",
					LSNColumn:        "recipe_version",
					ReferencesDomain: "recipe",
					ReferencesTable:  "master_recipes",
					ReferencesColumn: "id",
				}},
			},
		},
	}
	target := schemaDDLRequest{
		Domain: "manufacturing",
		Tables: []schemaDDLTable{
			{
				Name: "batch_orders",
				Columns: []schemaDDLColumn{
					{Name: "id", Type: "TEXT", PrimaryKey: true},
					{Name: "recipe_id", Type: "TEXT"},
					{Name: "captured_recipe_version", Type: "INT"},
				},
				VersionedForeignKeys: []schemaDDLVersionedFK{{
					Column:           "recipe_id",
					LSNColumn:        "captured_recipe_version",
					ReferencesDomain: "recipe",
					ReferencesTable:  "master_recipes",
					ReferencesColumn: "id",
				}},
			},
		},
	}

	diff, err := BuildSchemaDiff(base, target)
	if err != nil {
		t.Fatalf("build schema diff failed: %v", err)
	}
	if diff.Safe {
		t.Fatal("expected unsafe diff when versioned foreign key semantics change")
	}

	hasVFKWarning := false
	for _, warning := range diff.Warnings {
		if strings.Contains(warning, "VERSIONED FOREIGN KEY changed") {
			hasVFKWarning = true
			break
		}
	}
	if !hasVFKWarning {
		t.Fatalf("expected VERSIONED FOREIGN KEY warning, got %v", diff.Warnings)
	}

	hasVFKOperation := false
	for _, op := range diff.Operations {
		if op.Type == "modify_versioned_foreign_key" {
			hasVFKOperation = true
			break
		}
	}
	if !hasVFKOperation {
		t.Fatalf("expected modify_versioned_foreign_key operation, got %v", diff.Operations)
	}
}

func TestBuildSchemaDiffWarnsOnEntityChange(t *testing.T) {
	base := schemaDDLRequest{
		Domain: "recipe",
		Tables: []schemaDDLTable{
			{Name: "master_recipes", Columns: []schemaDDLColumn{{Name: "id", Type: "TEXT", PrimaryKey: true}}},
			{Name: "recipe_operations", Columns: []schemaDDLColumn{{Name: "id", Type: "TEXT", PrimaryKey: true}}},
			{Name: "recipe_parameters", Columns: []schemaDDLColumn{{Name: "id", Type: "TEXT", PrimaryKey: true}}},
		},
		Entities: []schemaDDLEntity{{
			Name:      "master_recipe_entity",
			RootTable: "master_recipes",
			Tables:    []string{"master_recipes", "recipe_operations"},
		}},
	}
	target := schemaDDLRequest{
		Domain: base.Domain,
		Tables: base.Tables,
		Entities: []schemaDDLEntity{{
			Name:      "master_recipe_entity",
			RootTable: "master_recipes",
			Tables:    []string{"master_recipes", "recipe_operations", "recipe_parameters"},
		}},
	}

	diff, err := BuildSchemaDiff(base, target)
	if err != nil {
		t.Fatalf("build schema diff failed: %v", err)
	}
	if diff.Safe {
		t.Fatal("expected unsafe diff when entity definition changes")
	}

	hasEntityWarning := false
	for _, warning := range diff.Warnings {
		if strings.Contains(warning, "entity master_recipe_entity changed") {
			hasEntityWarning = true
			break
		}
	}
	if !hasEntityWarning {
		t.Fatalf("expected entity warning, got %v", diff.Warnings)
	}

	hasEntityOperation := false
	for _, op := range diff.Operations {
		if op.Type == "modify_entity" {
			hasEntityOperation = true
			break
		}
	}
	if !hasEntityOperation {
		t.Fatalf("expected modify_entity operation, got %v", diff.Operations)
	}
}
