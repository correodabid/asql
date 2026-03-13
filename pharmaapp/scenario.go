package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
)

//go:embed fixtures/pharma-manufacturing-demo-v1.json
var defaultFixtureJSON []byte

type txMode string

const (
	txModeDomain txMode = "domain"
	txModeCross  txMode = "cross"
)

type scenarioStep struct {
	Name       string
	Mode       txMode
	Domains    []string
	Statements []string
}

func (s scenarioStep) BeginSQL() (string, error) {
	if len(s.Domains) == 0 {
		return "", fmt.Errorf("step %q has no domains", s.Name)
	}

	switch s.Mode {
	case txModeDomain:
		if len(s.Domains) != 1 {
			return "", fmt.Errorf("domain step %q must have exactly one domain", s.Name)
		}
		return "BEGIN DOMAIN " + s.Domains[0], nil
	case txModeCross:
		return "BEGIN CROSS DOMAIN " + strings.Join(s.Domains, ", "), nil
	default:
		return "", fmt.Errorf("step %q has unsupported mode %q", s.Name, s.Mode)
	}
}

// fixtureFile mirrors the ASQL fixture JSON schema for local parsing,
// avoiding a dependency on internal/fixtures and its engine imports.
type fixtureFile struct {
	Version string        `json:"version"`
	Name    string        `json:"name"`
	Steps   []fixtureStep `json:"steps"`
}

type fixtureStep struct {
	Name       string   `json:"name"`
	Mode       string   `json:"mode"`
	Domains    []string `json:"domains"`
	Statements []string `json:"statements"`
}

// loadFixtureSteps parses raw JSON fixture data into scenario steps.
func loadFixtureSteps(data []byte) ([]scenarioStep, error) {
	var f fixtureFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse fixture: %w", err)
	}
	if len(f.Steps) == 0 {
		return nil, fmt.Errorf("fixture %q has no steps", f.Name)
	}
	steps := make([]scenarioStep, len(f.Steps))
	for i, s := range f.Steps {
		steps[i] = scenarioStep{
			Name:       s.Name,
			Mode:       txMode(s.Mode),
			Domains:    s.Domains,
			Statements: s.Statements,
		}
	}
	return steps, nil
}

// isSchemaStep returns true when every statement in the step is DDL (CREATE ...).
func isSchemaStep(s scenarioStep) bool {
	for _, stmt := range s.Statements {
		upper := strings.ToUpper(strings.TrimSpace(stmt))
		if !strings.HasPrefix(upper, "CREATE ") {
			return false
		}
	}
	return len(s.Statements) > 0
}

// splitSteps separates all steps into schema (DDL-only) and workflow (DML) groups.
func splitSteps(all []scenarioStep) (schema, workflow []scenarioStep) {
	for _, s := range all {
		if isSchemaStep(s) {
			schema = append(schema, s)
		} else {
			workflow = append(workflow, s)
		}
	}
	return
}
