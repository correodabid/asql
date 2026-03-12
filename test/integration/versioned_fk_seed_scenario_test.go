package integration

import (
	"context"
	"path/filepath"
	"testing"

	"asql/internal/engine/executor"
	"asql/internal/engine/parser/ast"
	"asql/internal/storage/wal"
)

func mustExecSeedScenario(t *testing.T, ctx context.Context, engine *executor.Engine, session *executor.Session, sql string) {
	t.Helper()
	if _, err := engine.Execute(ctx, session, sql); err != nil {
		t.Fatalf("Execute(%q): %v", sql, err)
	}
}

func TestHealthcareSeedScenarioAvoidsManualLSNPlumbing(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "healthcare-seed-vfk.wal")

	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()

	for _, sql := range []string{
		"BEGIN DOMAIN patients",
		"CREATE TABLE patients.patients (id TEXT PRIMARY KEY, medical_record_no TEXT, full_name TEXT)",
		"CREATE ENTITY patient_entity (ROOT patients)",
		"COMMIT",
		"BEGIN CROSS DOMAIN clinical, patients",
		"CREATE TABLE clinical.admissions (id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'ADMITTED', patient_lsn INT, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_lsn)",
		"CREATE ENTITY admission_entity (ROOT admissions)",
		"COMMIT",
		"BEGIN CROSS DOMAIN billing, clinical, patients",
		"CREATE TABLE billing.invoices (id TEXT PRIMARY KEY, invoice_number TEXT UNIQUE, patient_id TEXT NOT NULL, admission_id TEXT NOT NULL, patient_lsn INT, admission_lsn INT, total_cents INT NOT NULL DEFAULT 0, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_lsn, VERSIONED FOREIGN KEY (admission_id) REFERENCES clinical.admissions(id) AS OF admission_lsn)",
		"CREATE TABLE billing.invoice_items (id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES invoices(id), description TEXT NOT NULL, amount_cents INT NOT NULL DEFAULT 0)",
		"CREATE ENTITY invoice_entity (ROOT invoices, INCLUDES invoice_items)",
		"COMMIT",
		"BEGIN CROSS DOMAIN clinical, patients",
		"INSERT INTO patients.patients (id, medical_record_no, full_name) VALUES ('patient-1', 'MRN-001', 'Ana López')",
		"INSERT INTO clinical.admissions (id, patient_id, status) VALUES ('admission-1', 'patient-1', 'ADMITTED')",
		"COMMIT",
		"BEGIN CROSS DOMAIN billing, clinical, patients",
		"INSERT INTO billing.invoices (id, invoice_number, patient_id, admission_id, total_cents) VALUES ('invoice-1', 'INV-001', 'patient-1', 'admission-1', 125000)",
		"INSERT INTO billing.invoice_items (id, invoice_id, description, amount_cents) VALUES ('invoice-item-1', 'invoice-1', 'Emergency admission', 125000)",
		"COMMIT",
		"BEGIN CROSS DOMAIN billing, clinical, patients",
		"UPDATE patients.patients SET full_name = 'Ana López García' WHERE id = 'patient-1'",
		"UPDATE clinical.admissions SET status = 'IN_CARE' WHERE id = 'admission-1'",
		"INSERT INTO billing.invoices (id, invoice_number, patient_id, admission_id, total_cents) VALUES ('invoice-2', 'INV-002', 'patient-1', 'admission-1', 8500)",
		"INSERT INTO billing.invoice_items (id, invoice_id, description, amount_cents) VALUES ('invoice-item-2', 'invoice-2', 'Dietary adjustment', 8500)",
		"COMMIT",
	} {
		mustExecSeedScenario(t, ctx, engine, session, sql)
	}

	admissions, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, patient_lsn FROM admissions WHERE id = 'admission-1'", []string{"clinical"}, ^uint64(0))
	if err != nil {
		t.Fatalf("select admissions: %v", err)
	}
	if len(admissions.Rows) != 1 {
		t.Fatalf("expected 1 admission row, got %d", len(admissions.Rows))
	}
	if got := admissions.Rows[0]["patient_lsn"]; got.Kind != ast.LiteralNumber || got.NumberValue != 1 {
		t.Fatalf("expected admission patient_lsn=1, got %+v", got)
	}

	invoices, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT invoice_number, patient_lsn, admission_lsn, total_cents FROM invoices ORDER BY invoice_number", []string{"billing"}, ^uint64(0))
	if err != nil {
		t.Fatalf("select invoices: %v", err)
	}
	if len(invoices.Rows) != 2 {
		t.Fatalf("expected 2 invoices, got %d", len(invoices.Rows))
	}
	if got := invoices.Rows[0]["patient_lsn"].NumberValue; got != 1 {
		t.Fatalf("expected invoice-1 patient_lsn=1, got %d", got)
	}
	if got := invoices.Rows[0]["admission_lsn"].NumberValue; got != 1 {
		t.Fatalf("expected invoice-1 admission_lsn=1, got %d", got)
	}
	if got := invoices.Rows[1]["patient_lsn"].NumberValue; got != 2 {
		t.Fatalf("expected invoice-2 patient_lsn=2 after same-tx update, got %d", got)
	}
	if got := invoices.Rows[1]["admission_lsn"].NumberValue; got != 2 {
		t.Fatalf("expected invoice-2 admission_lsn=2 after same-tx update, got %d", got)
	}

	patientVersion, ok, err := engine.EntityVersion("patients", "patient_entity", "patient-1")
	if err != nil {
		t.Fatalf("patient entity version: %v", err)
	}
	if !ok || patientVersion != 2 {
		t.Fatalf("expected patient version 2, got ok=%v version=%d", ok, patientVersion)
	}
	admissionVersion, ok, err := engine.EntityVersion("clinical", "admission_entity", "admission-1")
	if err != nil {
		t.Fatalf("admission entity version: %v", err)
	}
	if !ok || admissionVersion != 2 {
		t.Fatalf("expected admission version 2, got ok=%v version=%d", ok, admissionVersion)
	}

	replayed, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new replayed engine: %v", err)
	}
	replayedInvoices, err := replayed.TimeTravelQueryAsOfLSN(ctx, "SELECT invoice_number, patient_lsn, admission_lsn FROM invoices ORDER BY invoice_number", []string{"billing"}, ^uint64(0))
	if err != nil {
		t.Fatalf("select replayed invoices: %v", err)
	}
	if len(replayedInvoices.Rows) != 2 {
		t.Fatalf("expected 2 replayed invoices, got %d", len(replayedInvoices.Rows))
	}
	if replayedInvoices.Rows[0]["patient_lsn"].NumberValue != 1 || replayedInvoices.Rows[0]["admission_lsn"].NumberValue != 1 {
		t.Fatalf("unexpected replayed invoice-1 references: %+v", replayedInvoices.Rows[0])
	}
	if replayedInvoices.Rows[1]["patient_lsn"].NumberValue != 2 || replayedInvoices.Rows[1]["admission_lsn"].NumberValue != 2 {
		t.Fatalf("unexpected replayed invoice-2 references: %+v", replayedInvoices.Rows[1])
	}
}
