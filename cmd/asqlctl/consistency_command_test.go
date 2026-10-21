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
	"testing"
	"time"

	pgwire "github.com/correodabid/asql/internal/server/pgwire"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"

	"github.com/jackc/pgx/v5"
)

func TestProveAndVerifyConsistency(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	keyPath, pubPath := filepath.Join(dir, "node.key"), filepath.Join(dir, "node.pub")
	if _, err := ledgerdigest.GenerateKeyFiles(keyPath, pubPath); err != nil {
		t.Fatal(err)
	}
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
	// commit runs one transaction and waits for the digest of its head.
	commit := func(sqls ...string) uint64 {
		t.Helper()
		for _, sql := range append(append([]string{"BEGIN DOMAIN lab"}, sqls...), "COMMIT") {
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
			var lsn int64
			err := conn.QueryRow(ctx, "SELECT lsn FROM asql_admin.ledger_digest").Scan(&lsn)
			if err == nil && lsn == head {
				return uint64(head)
			}
			if time.Now().After(deadline) {
				t.Fatalf("no digest at LSN %d: last %d, %v", head, lsn, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	from := commit("CREATE TABLE results (id INT PRIMARY KEY, result TEXT)")
	commit("INSERT INTO results (id, result) VALUES (1, 'WITHIN_SPEC')")
	to := commit("INSERT INTO results (id, result) VALUES (2, 'OUT_OF_SPEC')")

	proofPath := filepath.Join(dir, "proof.json")
	var out bytes.Buffer
	if err := runProveConsistency(ctx, &out, proveConsistencyOptions{PgwireAddr: addr, FromLSN: from, ToLSN: to, OutputPath: proofPath}); err != nil {
		t.Fatalf("prove-consistency: %v", err)
	}
	verify := func(path string) (verifyConsistencyOutput, error) {
		t.Helper()
		var out bytes.Buffer
		err := runVerifyConsistency(&out, verifyConsistencyOptions{ProofPath: path, PubKeyPath: pubPath})
		var report verifyConsistencyOutput
		if out.Len() > 0 {
			if jerr := json.Unmarshal(out.Bytes(), &report); jerr != nil {
				t.Fatalf("decode report: %v\n%s", jerr, out.String())
			}
		}
		return report, err
	}
	report, err := verify(proofPath)
	if err != nil || report.Status != "ok" || report.FromLSN != from || report.ToLSN != to || report.ToNode != "node-a" {
		t.Fatalf("verify-consistency: %+v, %v", report, err)
	}

	// An edited proof fails, and so does a missing key.
	data, err := os.ReadFile(proofPath)
	if err != nil {
		t.Fatal(err)
	}
	var proof ledgerdigest.ConsistencyProof
	if err := json.Unmarshal(data, &proof); err != nil {
		t.Fatal(err)
	}
	if len(proof.Proof) == 0 {
		t.Fatal("a proof between different digests has no hashes")
	}
	proof.Proof[0][0] ^= 1
	edited := filepath.Join(dir, "edited.json")
	writeJSON(t, edited, proof)
	if report, err := verify(edited); !errors.Is(err, errConsistencyVerifyFailed) || report.Status != "mismatch" {
		t.Fatalf("an edited proof: %+v, %v", report, err)
	}
	if err := runVerifyConsistency(io.Discard, verifyConsistencyOptions{ProofPath: proofPath}); err == nil {
		t.Fatal("verify-consistency without -pubkey must be refused")
	}
}
