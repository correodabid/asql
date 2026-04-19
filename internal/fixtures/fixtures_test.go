package fixtures

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	pgwire "github.com/correodabid/asql/internal/server/pgwire"
	"github.com/correodabid/asql/internal/storage/wal"

	"github.com/jackc/pgx/v5"
)

func TestLoadFileAndValidateDemoFixture(t *testing.T) {
	fixture, err := LoadFile(filepath.Join("..", "..", "fixtures", "healthcare-billing-demo-v1.json"))
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}

	if err := ValidateDryRun(context.Background(), fixture); err != nil {
		t.Fatalf("dry-run validate fixture: %v", err)
	}
}

func TestLoadFileAndValidateEcommerceLargeFixture(t *testing.T) {
	fixture, err := LoadFile(filepath.Join("..", "..", "fixtures", "ecommerce-large-demo-v1.json"))
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}

	if err := ValidateDryRun(context.Background(), fixture); err != nil {
		t.Fatalf("dry-run validate fixture: %v", err)
	}
}

func TestValidateSpecRejectsNondeterministicStatements(t *testing.T) {
	fixture := &File{
		Version: CurrentVersion,
		Name:    "bad-fixture",
		Steps: []Step{{
			Name:       "create",
			Mode:       "domain",
			Domains:    []string{"demo"},
			Statements: []string{"INSERT INTO users (id, created_at) VALUES ('1', NOW())"},
		}},
	}

	err := ValidateSpec(fixture)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "non-deterministic token") {
		t.Fatalf("expected non-deterministic token error, got %v", err)
	}
}

func TestValidateDryRunCatchesBrokenReferences(t *testing.T) {
	fixture := &File{
		Version: CurrentVersion,
		Name:    "broken-refs",
		Steps: []Step{
			{
				Name:    "schema",
				Mode:    "domain",
				Domains: []string{"demo"},
				Statements: []string{
					"CREATE TABLE parents (id TEXT PRIMARY KEY)",
					"CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parents(id))",
				},
			},
			{
				Name:    "seed",
				Mode:    "domain",
				Domains: []string{"demo"},
				Statements: []string{
					"INSERT INTO children (id, parent_id) VALUES ('child-1', 'missing-parent')",
				},
			},
		},
	}

	err := ValidateDryRun(context.Background(), fixture)
	if err == nil {
		t.Fatal("expected dry-run validation error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "constraint") {
		t.Fatalf("expected constraint error, got %v", err)
	}
}

func TestApplySeedsEngineDeterministically(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "fixture-apply.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	defer store.Close()

	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	fixture := &File{
		Version: CurrentVersion,
		Name:    "simple-seed",
		Steps: []Step{
			{
				Name:    "schema",
				Mode:    "domain",
				Domains: []string{"patients"},
				Statements: []string{
					"CREATE TABLE patients.patients (id TEXT PRIMARY KEY, full_name TEXT)",
				},
			},
			{
				Name:    "rows",
				Mode:    "domain",
				Domains: []string{"patients"},
				Statements: []string{
					"INSERT INTO patients.patients (id, full_name) VALUES ('patient-1', 'Ana Lopez')",
					"INSERT INTO patients.patients (id, full_name) VALUES ('patient-2', 'Bob Ruiz')",
				},
			},
		},
	}

	if err := Apply(ctx, fixture, newEngineExecutor(engine)); err != nil {
		t.Fatalf("apply fixture: %v", err)
	}

	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, full_name FROM patients ORDER BY id", []string{"patients"}, ^uint64(0))
	if err != nil {
		t.Fatalf("query rows: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if got := result.Rows[0]["full_name"]; got.Kind != ast.LiteralString || got.StringValue != "Ana Lopez" {
		t.Fatalf("unexpected first row full_name: %+v", got)
	}
}

func TestExportFromPGWireRoundTripsFixture(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := pgwire.New(pgwire.Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "fixture-export-data"),
		Logger:      slog.New(slog.NewTextHandler(os.Stdout, nil)),
	})
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- server.ServeOnListener(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("pgwire server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire shutdown")
		}
	})

	conn, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	for _, sql := range []string{
		"BEGIN DOMAIN patients",
		"CREATE TABLE patients.patients (id TEXT PRIMARY KEY, full_name TEXT)",
		"CREATE ENTITY patient_entity (ROOT patients)",
		"COMMIT",
		"BEGIN CROSS DOMAIN billing, patients",
		"CREATE TABLE billing.invoices (id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, patient_lsn INT, total_cents INT, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_lsn)",
		"COMMIT",
		"BEGIN CROSS DOMAIN billing, patients",
		"INSERT INTO patients.patients (id, full_name) VALUES ('patient-1', 'Ana Lopez')",
		"INSERT INTO billing.invoices (id, patient_id, total_cents) VALUES ('invoice-1', 'patient-1', 125000)",
		"COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	exported, err := ExportFromPGWire(ctx, conn, ExportOptions{
		Domains:     []string{"patients", "billing"},
		Name:        "export-roundtrip",
		Description: "Roundtrip export test",
	})
	if err != nil {
		t.Fatalf("export fixture: %v", err)
	}
	if err := ValidateDryRun(ctx, exported); err != nil {
		t.Fatalf("dry-run exported fixture: %v", err)
	}

	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "roundtrip-export.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new wal: %v", err)
	}
	defer store.Close()

	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if err := Apply(ctx, exported, newEngineExecutor(engine)); err != nil {
		t.Fatalf("apply exported fixture: %v", err)
	}

	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, patient_id, total_cents FROM invoices ORDER BY id", []string{"billing"}, ^uint64(0))
	if err != nil {
		t.Fatalf("query invoices: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 invoice row, got %d", len(result.Rows))
	}
	if result.Rows[0]["patient_id"].StringValue != "patient-1" {
		t.Fatalf("unexpected exported patient_id: %+v", result.Rows[0]["patient_id"])
	}
	if result.Rows[0]["total_cents"].NumberValue != 125000 {
		t.Fatalf("unexpected exported total_cents: %+v", result.Rows[0]["total_cents"])
	}
}

func TestSaveFilePersistsExportedFixture(t *testing.T) {
	fixture := &File{
		Version:     CurrentVersion,
		Name:        "save-fixture",
		Description: "Save file test",
		Steps: []Step{{
			Name:       "schema",
			Mode:       "domain",
			Domains:    []string{"demo"},
			Statements: []string{"CREATE TABLE demo.items (id TEXT PRIMARY KEY)"},
		}},
	}
	path := filepath.Join(t.TempDir(), "saved-fixture.json")
	if err := SaveFile(path, fixture); err != nil {
		t.Fatalf("save fixture: %v", err)
	}
	loaded, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load saved fixture: %v", err)
	}
	if loaded.Name != fixture.Name || len(loaded.Steps) != 1 {
		t.Fatalf("unexpected loaded fixture: %+v", loaded)
	}
}
