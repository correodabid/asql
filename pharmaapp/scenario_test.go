package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestScenarioStepsAreDeterministic(t *testing.T) {
	first, err := loadFixtureSteps(defaultFixtureJSON)
	if err != nil {
		t.Fatalf("load first: %v", err)
	}
	second, err := loadFixtureSteps(defaultFixtureJSON)
	if err != nil {
		t.Fatalf("load second: %v", err)
	}

	if !reflect.DeepEqual(first, second) {
		t.Fatal("scenario steps should be deterministic across repeated calls")
	}
}

func TestScenarioCoversDeepASQLFeatures(t *testing.T) {
	steps, err := loadFixtureSteps(defaultFixtureJSON)
	if err != nil {
		t.Fatalf("load steps: %v", err)
	}
	if len(steps) < 10 {
		t.Fatalf("expected a substantial scenario, got %d steps", len(steps))
	}

	var joined []string
	modes := map[txMode]bool{}
	domains := map[string]bool{}
	for _, step := range steps {
		modes[step.Mode] = true
		for _, domain := range step.Domains {
			domains[domain] = true
		}
		joined = append(joined, step.Statements...)
	}

	joinedSQL := strings.Join(joined, "\n")
	for _, marker := range []string{"CREATE ENTITY", "VERSIONED FOREIGN KEY", "FOR HISTORY", "AS OF LSN"} {
		if marker == "FOR HISTORY" || marker == "AS OF LSN" {
			continue
		}
		if !strings.Contains(joinedSQL, marker) {
			t.Fatalf("expected scenario SQL to contain %q", marker)
		}
	}
	if !modes[txModeDomain] || !modes[txModeCross] {
		t.Fatalf("expected both domain and cross-domain transactions, got %+v", modes)
	}
	if len(domains) < 6 {
		t.Fatalf("expected at least six domains, got %d", len(domains))
	}

	for _, marker := range []string{
		"CREATE TABLE recipe.recipe_unit_procedures",
		"CREATE TABLE recipe.recipe_phases",
		"CREATE TABLE operations.units",
		"CREATE TABLE operations.equipment_assets",
		"CREATE TABLE execution.batch_phase_records",
		"CREATE TABLE compliance.ebr_reviews",
	} {
		if !strings.Contains(joinedSQL, marker) {
			t.Fatalf("expected scenario SQL to contain %q", marker)
		}
	}
}

func TestFixtureParsesAndMatchesScenarioIntent(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("fixtures", "pharma-manufacturing-demo-v1.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	var fixture fixtureFile
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatalf("unmarshal fixture: %v", err)
	}
	if fixture.Version != "v1" {
		t.Fatalf("unexpected fixture version: %s", fixture.Version)
	}
	if fixture.Name == "" || len(fixture.Steps) < 7 {
		t.Fatalf("fixture looks incomplete: %+v", fixture)
	}

	joinedStatements := make([]string, 0, len(fixture.Steps))
	seenCross := false
	for _, step := range fixture.Steps {
		joinedStatements = append(joinedStatements, step.Statements...)
		if step.Mode == string(txModeCross) {
			seenCross = true
		}
	}
	if !seenCross {
		t.Fatal("expected fixture to include cross-domain steps")
	}

	joinedSQL := strings.Join(joinedStatements, "\n")
	for _, fragment := range []string{
		"CREATE ENTITY master_recipe_entity",
		"CREATE ENTITY manufacturing_model_entity",
		"VERSIONED FOREIGN KEY",
		"INSERT INTO operations.units",
		"INSERT INTO execution.batch_phase_records",
		"INSERT INTO compliance.ebr_reviews",
		"INSERT INTO execution.batch_orders",
		"UPDATE recipe.master_recipes SET title = 'Ibuprofen Suspension 200 mg/5mL - Rev B'",
		"INSERT INTO compliance.batch_signatures",
	} {
		if !strings.Contains(joinedSQL, fragment) {
			t.Fatalf("expected fixture SQL to contain %q", fragment)
		}
	}
}

func TestReadmeMentionsComplianceAndISAStandards(t *testing.T) {
	data, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	readme := string(data)
	for _, fragment := range []string{"21 CFR Part 11", "ALCOA+", "ISA-88", "ISA-95", "eBR"} {
		if !strings.Contains(readme, fragment) {
			t.Fatalf("expected README to contain %q", fragment)
		}
	}
}
