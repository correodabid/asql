package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/platform/datadir"
	"github.com/correodabid/asql/internal/storage/wal"
)

func TestVerifyLedgerCommand(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	dd, err := datadir.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	store, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := executor.New(ctx, store, dd.SnapDir())
	if err != nil {
		t.Fatal(err)
	}
	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN lab",
		"CREATE TABLE batch_results (id INT, result TEXT)",
		"INSERT INTO batch_results (id, result) VALUES (1, 'OUT_OF_SPEC')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	engine.WaitPendingSnapshots()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := runVerifyLedger(&out, verifyLedgerOptions{DataDir: dir}); err != nil {
		t.Fatalf("verify clean data dir: %v\n%s", err, out.String())
	}
	var result verifyLedgerOutput
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "ok" || !result.FromGenesis || result.Records != 4 || result.Failure != nil {
		t.Fatalf("unexpected report: %+v", result)
	}

	// Flip a byte of the stored CREATE TABLE payload; the record CRC now fails,
	// which verify-ledger reports at the exact LSN.
	seg := filepath.Join(dd.WALDir(), filepath.Base(dd.WALBasePath())+".000001")
	data, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-10] ^= 0xff
	if err := os.WriteFile(seg, data, 0o644); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	err = runVerifyLedger(&out, verifyLedgerOptions{DataDir: dir})
	if !errors.Is(err, errLedgerVerifyFailed) {
		t.Fatalf("expected verification failure, got %v", err)
	}
	result = verifyLedgerOutput{}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "mismatch" || result.Failure == nil {
		t.Fatalf("expected mismatch report: %s", out.String())
	}

	if err := runVerifyLedger(&out, verifyLedgerOptions{}); err == nil {
		t.Fatal("expected error without -data-dir")
	}
}
