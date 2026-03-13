package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"asql/internal/engine/executor"
	"asql/internal/platform/datadir"
	auditstore "asql/internal/storage/audit"
	"asql/internal/storage/wal"
)

func TestRunLocalAuditCommandReport(t *testing.T) {
	ctx := context.Background()
	dataDir := seedAuditDataDir(t, ctx)

	var output bytes.Buffer
	if err := runLocalAuditCommand(ctx, &output, "audit-report", auditCommandOptions{DataDir: dataDir}); err != nil {
		t.Fatalf("audit-report: %v", err)
	}

	var report auditReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatalf("decode report: %v\noutput=%s", err, output.String())
	}
	if report.Status != "reported" {
		t.Fatalf("unexpected status: %+v", report)
	}
	if report.Policy.RetentionMode != "retain_forever" || !report.Policy.RetainForever {
		t.Fatalf("unexpected policy: %+v", report.Policy)
	}
	if report.EntryCount == 0 {
		t.Fatalf("expected audit entries, got %+v", report)
	}
	if report.Operations["UPDATE"] == 0 {
		t.Fatalf("expected UPDATE operation count, got %+v", report.Operations)
	}
	if report.Domains["accounts"] == 0 {
		t.Fatalf("expected accounts domain count, got %+v", report.Domains)
	}
}

func TestRunLocalAuditCommandExport(t *testing.T) {
	ctx := context.Background()
	dataDir := seedAuditDataDir(t, ctx)
	outputPath := filepath.Join(t.TempDir(), "audit-export.jsonl")

	var output bytes.Buffer
	if err := runLocalAuditCommand(ctx, &output, "audit-export", auditCommandOptions{
		DataDir: dataDir,
		Output:  outputPath,
		Format:  "jsonl",
		Domains: []string{"accounts"},
	}); err != nil {
		t.Fatalf("audit-export: %v", err)
	}

	var result auditExportResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatalf("decode export result: %v\noutput=%s", err, output.String())
	}
	if result.Status != "exported" {
		t.Fatalf("unexpected export status: %+v", result)
	}
	if result.EntryCount == 0 || result.SHA256 == "" {
		t.Fatalf("expected entry_count and sha256, got %+v", result)
	}
	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read export output: %v", err)
	}
	if !strings.Contains(string(content), "\"domain\":\"accounts\"") {
		t.Fatalf("expected accounts audit entry, got %s", string(content))
	}
	if !strings.Contains(string(content), "\"operation\":\"UPDATE\"") {
		t.Fatalf("expected UPDATE audit entry, got %s", string(content))
	}
}

func seedAuditDataDir(t *testing.T, ctx context.Context) string {
	t.Helper()
	dataDir := filepath.Join(t.TempDir(), "audit-data")
	dd, err := datadir.New(dataDir)
	if err != nil {
		t.Fatalf("new data dir: %v", err)
	}
	walStore, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	t.Cleanup(func() { _ = walStore.Close() })
	store, err := auditstore.New(dd.AuditBasePath())
	if err != nil {
		t.Fatalf("new audit store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	engine, err := executor.New(ctx, walStore, dd.SnapDir(), executor.WithAuditStore(store))
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute setup %q: %v", sql, err)
		}
	}
	mutation := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN accounts",
		"UPDATE users SET email = 'two@asql.dev' WHERE id = 1",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, mutation, sql); err != nil {
			t.Fatalf("execute mutation %q: %v", sql, err)
		}
	}
	waitForAuditEntries(t, ctx, store, 1)
	return dataDir
}

func waitForAuditEntries(t *testing.T, ctx context.Context, store *auditstore.Store, minEntries int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := store.ReadAll(ctx)
		if err == nil && len(entries) >= minEntries {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	entries, err := store.ReadAll(ctx)
	if err != nil {
		t.Fatalf("read audit entries: %v", err)
	}
	t.Fatalf("expected at least %d audit entries, got %d", minEntries, len(entries))
}
