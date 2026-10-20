package pgwire

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func complianceConfig(dataDir, ledgerMode string, compliance bool) Config {
	return Config{
		Address:     "127.0.0.1:0",
		DataDirPath: dataDir,
		Logger:      slog.New(slog.NewTextHandler(os.Stdout, nil)),
		LedgerMode:  ledgerMode,
		Compliance:  compliance,
	}
}

// writeCommit starts a server on dataDir, commits one transaction and stops.
func writeCommit(t *testing.T, config Config, table string) {
	t.Helper()
	server, err := New(config)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	defer server.Stop()
	ctx := context.Background()
	session := server.engine.NewSession()
	for _, sql := range []string{"BEGIN DOMAIN shop", "CREATE TABLE " + table + " (id INT PRIMARY KEY)", "COMMIT"} {
		if _, err := server.engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}
}

func TestComplianceRefusesHistoryTheLedgerDoesNotVouchFor(t *testing.T) {
	if _, err := New(complianceConfig(t.TempDir(), "off", true)); err == nil || !strings.Contains(err.Error(), "ledger cannot be off") {
		t.Fatalf("compliance with the ledger off: err=%v", err)
	}

	// A WAL written with the ledger on from the start passes, across restarts.
	anchored := filepath.Join(t.TempDir(), "data")
	writeCommit(t, complianceConfig(anchored, "chain", true), "a")
	writeCommit(t, complianceConfig(anchored, "chain", true), "b")

	// One written with the ledger off is refused, and still served without
	// compliance.
	unanchored := filepath.Join(t.TempDir(), "data")
	writeCommit(t, complianceConfig(unanchored, "off", false), "a")
	if _, err := New(complianceConfig(unanchored, "chain", true)); err == nil || !strings.Contains(err.Error(), "no ledger anchor") {
		t.Fatalf("compliance over unanchored segments: err=%v", err)
	}
	writeCommit(t, complianceConfig(unanchored, "chain", false), "b")
}
