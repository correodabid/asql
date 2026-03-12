package main

import (
	"strings"
	"testing"
)

func TestBeginStatement(t *testing.T) {
	got, err := beginStatement("cross", []string{"patients", "compliance"})
	if err != nil {
		t.Fatalf("beginStatement() error = %v", err)
	}
	want := "BEGIN CROSS DOMAIN patients, compliance"
	if got != want {
		t.Fatalf("beginStatement() = %q, want %q", got, want)
	}
}

func TestBeginStatementRejectsInvalidDomainMode(t *testing.T) {
	if _, err := beginStatement("domain", []string{"patients", "compliance"}); err == nil {
		t.Fatal("expected error for multi-domain single-domain transaction")
	}
}

func TestSchemaPlansExposePatientCentricVersionedFlows(t *testing.T) {
	plans := schemaPlans()
	if len(plans) < 6 {
		t.Fatalf("expected multiple schema plans, got %d", len(plans))
	}

	foundPatientEntity := false
	foundVersionedOrders := false
	foundComplianceAudit := false
	for _, plan := range plans {
		for _, statement := range plan.statements {
			if strings.Contains(statement.query, "CREATE ENTITY patient_entity") {
				foundPatientEntity = true
			}
			if strings.Contains(statement.query, "CREATE TABLE orders.lab_orders") && strings.Contains(statement.query, "VERSIONED FOREIGN KEY") {
				foundVersionedOrders = true
			}
			if strings.Contains(statement.query, "CREATE TABLE compliance.audit_events") {
				foundComplianceAudit = true
			}
		}
	}

	if !foundPatientEntity {
		t.Fatal("expected patient entity declaration in schema")
	}
	if !foundVersionedOrders {
		t.Fatal("expected versioned foreign key usage in orders schema")
	}
	if !foundComplianceAudit {
		t.Fatal("expected compliance audit table in schema")
	}
}

func TestPrefixedIDSanitizesValues(t *testing.T) {
	got := prefixedID("sig", "Episode ANA 001")
	if got != "sig-episode-ana-001" {
		t.Fatalf("prefixedID() = %q", got)
	}
}
