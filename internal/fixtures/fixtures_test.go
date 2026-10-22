package fixtures

import (
	"context"
	"encoding/json"
	"io"
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
	fixture, err := LoadFile(filepath.Join("testdata", "demo.json"))
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

// TestExportKeepsTableOptions: APPEND ONLY, VERIFIABLE, composite primary
// keys and verifiable groups survive export and load, with every row.
func TestExportKeepsTableOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := pgwire.New(pgwire.Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "fixture-options-data"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ServeOnListener(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		<-errCh
	})
	conn, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, sql := range []string{
		"BEGIN DOMAIN lab",
		"CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT NOT NULL, note TEXT) VERIFIABLE",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE",
		"CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)",
		"COMMIT",
		"BEGIN DOMAIN lab",
		"ALTER TABLE artifact_parents ADD VERIFIABLE GROUP (child_id) REFERENCES artifacts (id)",
		"COMMIT",
		"BEGIN DOMAIN lab",
		"INSERT INTO artifacts (id, kind) VALUES ('aa', 'source'), ('bb', 'run'), ('cc', 'source')",
		"INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('bb', 'cc', 2), ('bb', 'aa', 1)",
		"INSERT INTO notes (id, body) VALUES (1, 'x')",
		"COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	var raw string
	if err := conn.QueryRow(ctx, "SELECT snapshot FROM asql_admin.schema_snapshot").Scan(&raw); err != nil {
		t.Fatal(err)
	}

	exported, err := ExportFromPGWire(ctx, conn, ExportOptions{Domains: []string{"lab"}, Name: "options"})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if err := ValidateSpec(exported); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if err := ValidateDryRun(ctx, exported); err != nil {
		t.Fatalf("dry run: %v", err)
	}
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "options.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(ctx, exported, newEngineExecutor(engine)); err != nil {
		t.Fatalf("apply: %v", err)
	}

	// The source's options, as asql_admin.schema_snapshot reports them.
	type group struct {
		Columns           []string `json:"columns"`
		References        string   `json:"references"`
		ReferencedColumns []string `json:"referenced_columns"`
	}
	type column struct {
		Name       string `json:"name"`
		Type       string `json:"type"`
		PrimaryKey bool   `json:"primary_key"`
		Unique     bool   `json:"unique"`
		NotNull    bool   `json:"not_null"`
	}
	type options struct {
		AppendOnly bool     `json:"append_only"`
		Verifiable bool     `json:"verifiable"`
		PrimaryKey []string `json:"primary_key"`
		Groups     []group  `json:"verifiable_groups"`
		Columns    []column `json:"columns"`
	}
	var source struct {
		Domains []struct {
			Name   string `json:"name"`
			Tables []struct {
				Name string `json:"name"`
				options
			} `json:"tables"`
		} `json:"domains"`
	}
	if err := json.Unmarshal([]byte(raw), &source); err != nil {
		t.Fatal(err)
	}
	want := map[string]options{}
	for _, d := range source.Domains {
		for _, tbl := range d.Tables {
			if d.Name == "lab" {
				want[tbl.Name] = tbl.options
			}
		}
	}
	got := map[string]options{}
	for _, d := range engine.SchemaSnapshot([]string{"lab"}).Domains {
		for _, tbl := range d.Tables {
			var groups []group
			for _, g := range tbl.VerifiableGroups {
				groups = append(groups, group{Columns: g.Columns, References: g.References, ReferencedColumns: g.ReferencedColumns})
			}
			var columns []column
			for _, c := range tbl.Columns {
				columns = append(columns, column{Name: c.Name, Type: c.Type, PrimaryKey: c.PrimaryKey, Unique: c.Unique, NotNull: c.NotNull})
			}
			got[tbl.Name] = options{AppendOnly: tbl.AppendOnly, Verifiable: tbl.Verifiable, PrimaryKey: tbl.PrimaryKey, Groups: groups, Columns: columns}
		}
	}
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got)
	if string(wantJSON) != string(gotJSON) {
		t.Fatalf("options after export and load:\n got %s\nwant %s", gotJSON, wantJSON)
	}
	if !got["artifact_parents"].AppendOnly || !got["artifact_parents"].Verifiable || len(got["artifact_parents"].Groups) != 1 || got["notes"].Verifiable {
		t.Fatalf("unexpected options: %s", gotJSON)
	}
	for _, step := range exported.Steps {
		for _, statement := range step.Statements {
			if strings.Contains(statement, "_lsn") {
				t.Fatalf("the export mentions _lsn: %s", statement)
			}
		}
	}
	// One _lsn per table, and NOT NULL outside the key kept.
	for table, opts := range got {
		lsn := 0
		for _, c := range opts.Columns {
			if c.Name == "_lsn" {
				lsn++
			}
			if table == "artifacts" && c.Name == "kind" && !c.NotNull {
				t.Fatalf("artifacts.kind lost NOT NULL: %s", gotJSON)
			}
		}
		if lsn != 1 {
			t.Fatalf("%s has %d _lsn columns after export and load", table, lsn)
		}
	}
	for table, rows := range map[string]int{"artifacts": 3, "artifact_parents": 2, "notes": 1} {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT * FROM "+table, []string{"lab"}, ^uint64(0))
		if err != nil || len(result.Rows) != rows {
			t.Fatalf("%s: %d rows, %v; want %d", table, len(result.Rows), err, rows)
		}
	}
}

// TestOldExportedFixturesStillLoad: fixtures exported by v0.7.0 and
// earlier declare _lsn in CREATE TABLE and insert it; they load with the
// engine's own _lsn only.
func TestOldExportedFixturesStillLoad(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "old-fixture.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &File{Version: CurrentVersion, Name: "exported-by-v0.7.0", Steps: []Step{
		{Name: "schema", Mode: "domain", Domains: []string{"lab"}, Statements: []string{
			"CREATE TABLE lab.artifacts (id TEXT PRIMARY KEY, kind TEXT, _lsn TEXT)",
		}},
		{Name: "data", Mode: "domain", Domains: []string{"lab"}, Statements: []string{
			"INSERT INTO lab.artifacts (id, kind, _lsn) VALUES ('aa', 'source', '7')",
		}},
	}}
	if err := Apply(ctx, fixture, newEngineExecutor(engine)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, d := range engine.SchemaSnapshot([]string{"lab"}).Domains {
		for _, tbl := range d.Tables {
			n := 0
			for _, c := range tbl.Columns {
				if c.Name == "_lsn" {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("%s has %d _lsn columns", tbl.Name, n)
			}
		}
	}
	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM artifacts", []string{"lab"}, ^uint64(0))
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("rows: %v, %v", result.Rows, err)
	}
}
