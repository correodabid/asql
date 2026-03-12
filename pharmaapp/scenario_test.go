package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type fixtureFile struct {
	Version     string        `json:"version"`
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Steps       []fixtureStep `json:"steps"`
}

type fixtureStep struct {
	Name       string   `json:"name"`
	Mode       string   `json:"mode"`
	Domains    []string `json:"domains"`
	Statements []string `json:"statements"`
}

func TestScenarioStepsAreDeterministic(t *testing.T) {
	first := allSteps()
	second := allSteps()

	if !reflect.DeepEqual(first, second) {
		t.Fatal("scenario steps should be deterministic across repeated calls")
	}
}

func TestScenarioCoversDeepASQLFeatures(t *testing.T) {
	steps := allSteps()
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
	if len(domains) < 5 {
		t.Fatalf("expected at least five domains, got %d", len(domains))
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
		"VERSIONED FOREIGN KEY",
		"INSERT INTO execution.batch_orders",
		"UPDATE recipe.master_recipes SET title = 'Ibuprofen Suspension 200 mg/5mL - Rev B'",
		"INSERT INTO compliance.batch_signatures",
	} {
		if !strings.Contains(joinedSQL, fragment) {
			t.Fatalf("expected fixture SQL to contain %q", fragment)
		}
	}
}
