package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pgwire "github.com/correodabid/asql/internal/server/pgwire"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/statetree"

	"github.com/jackc/pgx/v5"
)

func TestProveAndVerifyRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var keygen bytes.Buffer
	keyPath := filepath.Join(dir, "node.key")
	if err := runLedgerKeygen(&keygen, keyPath); err != nil {
		t.Fatal(err)
	}
	pubPath := filepath.Join(dir, "node.pub")
	server, err := pgwire.New(pgwire.Config{
		Address: "127.0.0.1:0", DataDirPath: filepath.Join(dir, "data"), NodeID: "node-a",
		LedgerMode: "signed", LedgerKeyFile: keyPath, LedgerDigestInterval: 20 * time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- server.ServeOnListener(serveCtx, listener) }()
	defer func() {
		cancel()
		server.Stop()
		<-done
	}()
	addr := listener.Addr().String()

	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, sql := range []string{
		"BEGIN DOMAIN lab",
		"CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT) VERIFIABLE",
		"CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY VERIFIABLE",
		"COMMIT",
		"BEGIN DOMAIN lab",
		"INSERT INTO artifacts (id, kind) VALUES ('aa', 'source'), ('bb', 'extraction')",
		"INSERT INTO artifact_parents (child_id, parent_id, ordinal) VALUES ('bb', 'aa', 1)",
		"COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	var head int64
	if err := conn.QueryRow(ctx, "SELECT current_lsn()").Scan(&head); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		var lsn, version int64
		if err := conn.QueryRow(ctx, "SELECT lsn, version FROM asql_admin.ledger_digest").Scan(&lsn, &version); err == nil && lsn == head {
			if version != ledgerdigest.Version3 {
				t.Fatalf("digest version %d with a state key from ledger-keygen", version)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no digest of the head")
		}
		time.Sleep(10 * time.Millisecond)
	}

	keysPath := filepath.Join(dir, "lineage.json")
	if err := os.WriteFile(keysPath, []byte(`[{"domain": "lab", "table": "artifacts", "pk": ["bb"]},
		{"domain": "lab", "table": "artifact_parents", "pk": ["bb", "aa"]},
		{"domain": "lab", "table": "artifacts", "pk": ["aa"]}]`), 0o644); err != nil {
		t.Fatal(err)
	}
	proofPath := filepath.Join(dir, "proof.json")
	if err := runProveRows(ctx, io.Discard, proveRowsOptions{PgwireAddr: addr, KeysPath: keysPath, OutputPath: proofPath}); err != nil {
		t.Fatalf("prove-rows: %v", err)
	}
	verify := func(path string) (verifyRowsOutput, error) {
		t.Helper()
		var out bytes.Buffer
		err := runVerifyRows(&out, verifyRowsOptions{ProofPath: path, PubKeyPath: pubPath})
		var report verifyRowsOutput
		if out.Len() > 0 {
			if jerr := json.Unmarshal(out.Bytes(), &report); jerr != nil {
				t.Fatalf("decode report: %v\n%s", jerr, out.String())
			}
		}
		return report, err
	}
	report, err := verify(proofPath)
	if err != nil || report.Status != "ok" || uint64(head) != report.LSN {
		t.Fatalf("verify-rows: %+v, %v", report, err)
	}
	if got := strings.Join(report.Rows, "; "); got != "lab.artifacts(id=bb); lab.artifact_parents(child_id=bb, parent_id=aa); lab.artifacts(id=aa)" {
		t.Fatalf("rows as cited: %s", got)
	}

	data, _ := os.ReadFile(proofPath)
	var proof statetree.RowsProof
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	proof.Rows[1].Row["ordinal"] = proof.Rows[1].Row["child_id"]
	edited := filepath.Join(dir, "edited.json")
	writeJSON(t, edited, proof)
	if report, err := verify(edited); !errors.Is(err, errRowsVerifyFailed) || report.Status != "mismatch" {
		t.Fatalf("an edited row: %+v, %v", report, err)
	}
	if err := runVerifyRows(io.Discard, verifyRowsOptions{ProofPath: proofPath}); err == nil {
		t.Fatal("verify-rows without -pubkey must be refused")
	}
}
