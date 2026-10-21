package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/platform/datadir"
	pgwire "github.com/correodabid/asql/internal/server/pgwire"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/wal"

	"github.com/jackc/pgx/v5"
)

// attestFixture runs a node with ledger=signed, writes to it, attests a
// query at the head and at an earlier LSN, and stops it. It returns the
// data directory, the node's signer, the trusted public key file and the
// two attested result files.
func attestFixture(t *testing.T) (dataDir string, signer *ledgerdigest.Ed25519Signer, pubPath, headResult, earlyResult string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	keyPath, pubPath := filepath.Join(dir, "node.key"), filepath.Join(dir, "node.pub")
	if _, err := ledgerdigest.GenerateKeyFiles(keyPath, pubPath); err != nil {
		t.Fatal(err)
	}
	signer, err := ledgerdigest.LoadSigner(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	dataDir = filepath.Join(dir, "data")
	server, err := pgwire.New(pgwire.Config{
		Address: "127.0.0.1:0", DataDirPath: dataDir, NodeID: "node-a",
		LedgerMode: "signed", LedgerKeyFile: keyPath,
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
	addr := listener.Addr().String()

	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	exec := func(sqls ...string) {
		t.Helper()
		for _, sql := range sqls {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}
	exec("BEGIN DOMAIN lab", "CREATE TABLE results (id INT PRIMARY KEY, result TEXT, value FLOAT)", "COMMIT",
		"BEGIN DOMAIN lab", "INSERT INTO results (id, result, value) VALUES (1, 'WITHIN_SPEC', 0.5)", "COMMIT")
	var early int64
	if err := conn.QueryRow(ctx, "SELECT current_lsn()").Scan(&early); err != nil {
		t.Fatal(err)
	}
	exec("BEGIN DOMAIN lab", "INSERT INTO results (id, result, value) VALUES (2, 'OUT_OF_SPEC', 1.5)", "COMMIT")
	_ = conn.Close(ctx)

	const query = "SELECT id, result, value FROM lab.results ORDER BY id"
	headResult, earlyResult = filepath.Join(dir, "head.json"), filepath.Join(dir, "early.json")
	for path, lsn := range map[string]uint64{headResult: 0, earlyResult: uint64(early)} {
		var out bytes.Buffer
		if err := runAttestQuery(ctx, &out, attestQueryOptions{PgwireAddr: addr, SQL: query, LSN: lsn, OutputPath: path}); err != nil {
			t.Fatalf("attest-query at LSN %d: %v", lsn, err)
		}
	}
	cancel()
	server.Stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop")
	}
	return dataDir, signer, pubPath, headResult, earlyResult
}

func readAttested(t *testing.T, path string) ledgerdigest.AttestedResult {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r ledgerdigest.AttestedResult
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func writeJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func verifyResultReport(t *testing.T, opts verifyResultOptions) (verifyResultOutput, error) {
	t.Helper()
	var out bytes.Buffer
	err := runVerifyResult(context.Background(), &out, opts)
	var report verifyResultOutput
	if jsonErr := json.Unmarshal(out.Bytes(), &report); jsonErr != nil {
		t.Fatalf("report: %v (%v)\n%s", jsonErr, err, out.String())
	}
	return report, err
}

// headDigest signs, with the node's key, a digest of the WAL head of the
// stopped node, as its checkpointer does.
func headDigest(t *testing.T, signer *ledgerdigest.Ed25519Signer, dataDir, path string) {
	t.Helper()
	dd, err := datadir.New(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	store, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	head := store.LedgerHead()
	_ = store.Close()
	d, err := ledgerdigest.Sign(signer, ledgerdigest.Digest{LSN: head.LSN, Chain: head.Chain, NodeID: "node-a", SignedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	writeJSON(t, path, d)
}

// treeDigest hashes the names and contents of the files under dir.
func treeDigest(t *testing.T, dir string) string {
	t.Helper()
	h := sha256.New()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "%s %d\n", path, len(data))
		h.Write(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestAttestQueryAndVerifyResult(t *testing.T) {
	dataDir, signer, pubPath, headResult, earlyResult := attestFixture(t)
	digestPath := filepath.Join(t.TempDir(), "digest.json")
	headDigest(t, signer, dataDir, digestPath)
	// Re-execution runs on a copy: the data directory is left as it was.
	before := treeDigest(t, dataDir)
	defer func() {
		if treeDigest(t, dataDir) != before {
			t.Error("verify-result changed the data directory")
		}
	}()

	// A result checks out with the key alone, and fully with the digest and
	// the data; the early one too, through the WAL that links it to the
	// later digest.
	for _, path := range []string{headResult, earlyResult} {
		report, err := verifyResultReport(t, verifyResultOptions{ResultPath: path, PubKeyPath: pubPath})
		if err != nil || report.Signature.Status != checkPass || report.Rows.Status != checkPass || report.Reexecution.Status != checkNotChecked {
			t.Fatalf("%s with the key alone: %+v, err %v", filepath.Base(path), report, err)
		}
		report, err = verifyResultReport(t, verifyResultOptions{ResultPath: path, PubKeyPath: pubPath, DigestPath: digestPath, DataDir: dataDir})
		if err != nil || report.Status != "ok" || report.Chain.Status != checkPass || report.Reexecution.Status != checkPass {
			t.Fatalf("%s with digest and data: %+v, err %v", filepath.Base(path), report, err)
		}
	}
	if r := readAttested(t, earlyResult); len(r.Rows) != 1 {
		t.Fatalf("early result has %d rows, want 1", len(r.Rows))
	}

	resign := func(a ledgerdigest.Attestation) ledgerdigest.Attestation {
		signed, err := ledgerdigest.SignAttestation(signer, a)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	for name, tc := range map[string]struct {
		tamper func(r *ledgerdigest.AttestedResult)
		// the first check that must fail
		failing string
	}{
		"a row value":   {func(r *ledgerdigest.AttestedResult) { r.Rows[1][1].StringValue = "WITHIN_SPEC" }, "rows"},
		"a dropped row": {func(r *ledgerdigest.AttestedResult) { r.Rows = r.Rows[:1] }, "rows"},
		"the hash":      {func(r *ledgerdigest.AttestedResult) { r.Attestation.ResultHash[0] ^= 1 }, "signature"},
		"the signature": {func(r *ledgerdigest.AttestedResult) { r.Attestation.Sig[0] ^= 1 }, "signature"},
		"the LSN":       {func(r *ledgerdigest.AttestedResult) { r.Attestation.LSN-- }, "signature"},
		"the chain":     {func(r *ledgerdigest.AttestedResult) { r.Attestation.Chain[0] ^= 1 }, "signature"},
		// Signed by the node over a false chain: only the WAL or a digest
		// shows it.
		"a re-signed chain": {func(r *ledgerdigest.AttestedResult) {
			r.Attestation.Chain[0] ^= 1
			r.Attestation = resign(r.Attestation)
		}, "chain"},
		// Signed by the node over rows it made up, hash included: only
		// re-executing shows it.
		"re-signed rows and hash": {func(r *ledgerdigest.AttestedResult) {
			r.Rows[1][1].StringValue = "WITHIN_SPEC"
			sum, err := ledgerdigest.HashResult(r.Attestation.Columns, r.Rows)
			if err != nil {
				t.Fatal(err)
			}
			r.Attestation.ResultHash = sum
			r.Attestation = resign(r.Attestation)
		}, "reexecution"},
	} {
		tampered := readAttested(t, headResult)
		tc.tamper(&tampered)
		path := filepath.Join(t.TempDir(), "tampered.json")
		writeJSON(t, path, tampered)
		report, err := verifyResultReport(t, verifyResultOptions{ResultPath: path, PubKeyPath: pubPath, DigestPath: digestPath, DataDir: dataDir})
		if err == nil || report.Status != "mismatch" {
			t.Fatalf("%s: verified: %+v", name, report)
		}
		checks := map[string]verifyResultCheck{"signature": report.Signature, "rows": report.Rows, "chain": report.Chain, "reexecution": report.Reexecution}
		if checks[tc.failing].Status != checkFail {
			t.Fatalf("%s: %s check is %+v, want invalid; report %+v", name, tc.failing, checks[tc.failing], report)
		}
		for _, before := range []string{"signature", "rows", "chain", "reexecution"} {
			if before == tc.failing {
				break
			}
			if checks[before].Status != checkPass && !(before == "rows" && tc.failing == "signature") {
				t.Fatalf("%s: %s check is %+v, want valid", name, before, checks[before])
			}
		}
	}

	// A false chain, signed by the node, is caught by the WAL alone and by a
	// digest at the same LSN alone.
	forged := readAttested(t, headResult)
	forged.Attestation.Chain[0] ^= 1
	forged.Attestation = resign(forged.Attestation)
	forgedPath := filepath.Join(t.TempDir(), "forged.json")
	writeJSON(t, forgedPath, forged)
	for label, opts := range map[string]verifyResultOptions{
		"data only":   {ResultPath: forgedPath, PubKeyPath: pubPath, DataDir: dataDir},
		"digest only": {ResultPath: forgedPath, PubKeyPath: pubPath, DigestPath: digestPath},
	} {
		if report, err := verifyResultReport(t, opts); err == nil || report.Chain.Status != checkFail {
			t.Fatalf("re-signed chain, %s: %+v", label, report)
		}
	}

	// A key the verifier does not trust.
	untrusted := filepath.Join(t.TempDir(), "untrusted.pub")
	if _, err := ledgerdigest.GenerateKeyFiles(filepath.Join(t.TempDir(), "u.key"), untrusted); err != nil {
		t.Fatal(err)
	}
	if report, err := verifyResultReport(t, verifyResultOptions{ResultPath: headResult, PubKeyPath: untrusted}); err == nil ||
		!strings.Contains(report.Signature.Message, "not trusted") {
		t.Fatalf("untrusted key: %+v", report)
	}
}
