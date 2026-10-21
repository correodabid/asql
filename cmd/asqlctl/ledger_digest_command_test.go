package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/platform/datadir"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/logtree"
	"github.com/correodabid/asql/internal/storage/wal"
)

// makeLedgerDataDir writes a data directory holding one row per result, and
// returns it with a digest of its chain head signed by signer.
func makeLedgerDataDir(t *testing.T, signer *ledgerdigest.Ed25519Signer, results ...string) (string, ledgerdigest.Digest) {
	t.Helper()
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
	statements := []string{"BEGIN DOMAIN lab", "CREATE TABLE batch_results (id INT, result TEXT)", "COMMIT"}
	for i, result := range results {
		statements = append(statements, "BEGIN DOMAIN lab", fmt.Sprintf("INSERT INTO batch_results (id, result) VALUES (%d, '%s')", i+1, result), "COMMIT")
	}
	session := engine.NewSession()
	for _, sql := range statements {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	head := store.LedgerHead()
	digest, err := ledgerdigest.Sign(signer, ledgerdigest.Digest{LSN: head.LSN, Chain: head.Chain, NodeID: "node-a", SignedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	engine.WaitPendingSnapshots()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, digest
}

func writeDigestFile(t *testing.T, d ledgerdigest.Digest) string {
	t.Helper()
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "digest.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func newLedgerKey(t *testing.T) (*ledgerdigest.Ed25519Signer, string) {
	t.Helper()
	var out bytes.Buffer
	priv := filepath.Join(t.TempDir(), "node.key")
	if err := runLedgerKeygen(&out, priv); err != nil {
		t.Fatal(err)
	}
	var keygen ledgerKeygenOutput
	if err := json.Unmarshal(out.Bytes(), &keygen); err != nil {
		t.Fatal(err)
	}
	signer, err := ledgerdigest.LoadSigner(keygen.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if keygen.KeyID != signer.KeyID() || !strings.HasSuffix(keygen.PublicKey, "node.pub") {
		t.Fatalf("unexpected keygen output %+v", keygen)
	}
	return signer, keygen.PublicKey
}

func verifyWithDigest(t *testing.T, opts verifyLedgerOptions) (verifyLedgerOutput, error) {
	t.Helper()
	var out bytes.Buffer
	err := runVerifyLedger(&out, opts)
	var result verifyLedgerOutput
	if out.Len() > 0 {
		if jerr := json.Unmarshal(out.Bytes(), &result); jerr != nil {
			t.Fatalf("decode report: %v\n%s", jerr, out.String())
		}
	}
	return result, err
}

func TestVerifyLedgerWithSignedDigest(t *testing.T) {
	signer, pubPath := newLedgerKey(t)
	dir, digest := makeLedgerDataDir(t, signer, "OUT_OF_SPEC", "WITHIN_SPEC")
	digestPath := writeDigestFile(t, digest)

	result, err := verifyWithDigest(t, verifyLedgerOptions{DataDir: dir, DigestPath: digestPath, PubKeyPath: pubPath})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if result.Status != "ok" || result.Digest == nil || result.Digest.Signature != "valid" || result.Digest.Chain != "match" || result.Digest.LSN != result.LastLSN {
		t.Fatalf("unexpected report: %+v digest=%+v", result, result.Digest)
	}

	if _, err := verifyWithDigest(t, verifyLedgerOptions{DataDir: dir, DigestPath: digestPath}); err == nil || !strings.Contains(err.Error(), "-pubkey") {
		t.Fatalf("a digest without trusted keys must be refused, got %v", err)
	}
}

func TestVerifyLedgerDigestRejectsUntrustedOrForgedDigests(t *testing.T) {
	signer, pubPath := newLedgerKey(t)
	_, otherPub := newLedgerKey(t)
	dir, digest := makeLedgerDataDir(t, signer, "OUT_OF_SPEC")

	result, err := verifyWithDigest(t, verifyLedgerOptions{DataDir: dir, DigestPath: writeDigestFile(t, digest), PubKeyPath: otherPub})
	if !errors.Is(err, errLedgerVerifyFailed) || result.Failure == nil || result.Failure.Kind != verifyDigestSignature || result.Digest.Signature != "invalid" {
		t.Fatalf("digest from an untrusted key: err=%v report=%+v", err, result.Failure)
	}

	forged := digest
	forged.LSN--
	result, err = verifyWithDigest(t, verifyLedgerOptions{DataDir: dir, DigestPath: writeDigestFile(t, forged), PubKeyPath: pubPath})
	if !errors.Is(err, errLedgerVerifyFailed) || result.Failure == nil || result.Failure.Kind != verifyDigestSignature {
		t.Fatalf("edited digest: err=%v report=%+v", err, result.Failure)
	}
}

// TestVerifyLedgerDigestDetectsAReplacedHistory covers what anchors on disk
// cannot: a WAL replaced by another internally consistent history, and a WAL
// cleanly truncated after the digest was taken. Both pass verify-ledger on
// their own; the digest exposes them.
func TestVerifyLedgerDigestDetectsAReplacedHistory(t *testing.T) {
	signer, pubPath := newLedgerKey(t)
	_, digest := makeLedgerDataDir(t, signer, "OUT_OF_SPEC", "WITHIN_SPEC")
	forgedDir, _ := makeLedgerDataDir(t, signer, "WITHIN_SPEC", "WITHIN_SPEC")
	digestPath := writeDigestFile(t, digest)

	if result, err := verifyWithDigest(t, verifyLedgerOptions{DataDir: forgedDir}); err != nil || result.Status != "ok" {
		t.Fatalf("the replaced history is self-consistent and must verify without a digest: %v %+v", err, result.Failure)
	}
	result, err := verifyWithDigest(t, verifyLedgerOptions{DataDir: forgedDir, DigestPath: digestPath, PubKeyPath: pubPath})
	if !errors.Is(err, errLedgerVerifyFailed) || result.Failure == nil || result.Failure.Kind != wal.VerifyChainMismatch || result.Digest.Chain != "mismatch" {
		t.Fatalf("replaced history: err=%v failure=%+v digest=%+v", err, result.Failure, result.Digest)
	}

	// Truncate committed records after the digest; the remaining WAL and its
	// anchors stay consistent.
	dir, digest := makeLedgerDataDir(t, signer, "OUT_OF_SPEC", "WITHIN_SPEC")
	dd, _ := datadir.New(dir)
	store, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.TruncateAfter(context.Background(), digest.LSN-3); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	if err := os.RemoveAll(dd.SnapDir()); err != nil {
		t.Fatal(err)
	}
	if result, err := verifyWithDigest(t, verifyLedgerOptions{DataDir: dir}); err != nil || result.Status != "ok" {
		t.Fatalf("the truncated WAL must verify without a digest: %v %+v", err, result.Failure)
	}
	result, err = verifyWithDigest(t, verifyLedgerOptions{DataDir: dir, DigestPath: writeDigestFile(t, digest), PubKeyPath: pubPath})
	if !errors.Is(err, errLedgerVerifyFailed) || result.Failure == nil || result.Failure.Kind != verifyDigestBeyondWAL {
		t.Fatalf("truncated WAL: err=%v failure=%+v", err, result.Failure)
	}
}

// TestVerifyLedgerChecksAVersion2DigestsTreeRoot rebuilds the log tree from
// the WAL and compares it with the root a version 2 digest signs (ADR 0008).
func TestVerifyLedgerChecksAVersion2DigestsTreeRoot(t *testing.T) {
	signer, pubPath := newLedgerKey(t)
	dir, v1 := makeLedgerDataDir(t, signer, "OUT_OF_SPEC", "WITHIN_SPEC")
	dd, _ := datadir.New(dir)
	store, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	leaves, err := store.LedgerLeaves(context.Background(), 1, v1.LSN)
	_ = store.Close()
	if err != nil || uint64(len(leaves)) != v1.LSN {
		t.Fatalf("%d leaves, %v; want %d", len(leaves), err, v1.LSN)
	}
	var tree logtree.Frontier
	for _, leaf := range leaves {
		tree.Add(logtree.Hash(leaf))
	}

	sign := func(root [32]byte) string {
		d := v1
		d.Version, d.TreeRoot = ledgerdigest.Version2, root
		d, err := ledgerdigest.Sign(signer, d)
		if err != nil {
			t.Fatal(err)
		}
		return writeDigestFile(t, d)
	}
	result, err := verifyWithDigest(t, verifyLedgerOptions{DataDir: dir, DigestPath: sign(tree.Root()), PubKeyPath: pubPath})
	if err != nil || result.Digest.Chain != "match" || result.Digest.TreeRoot != "match" {
		t.Fatalf("version 2 digest: err=%v digest=%+v", err, result.Digest)
	}

	wrong := tree.Root()
	wrong[0] ^= 1
	result, err = verifyWithDigest(t, verifyLedgerOptions{DataDir: dir, DigestPath: sign(wrong), PubKeyPath: pubPath})
	if !errors.Is(err, errLedgerVerifyFailed) || result.Failure == nil || result.Failure.Kind != verifyDigestTree || result.Digest.TreeRoot != "mismatch" {
		t.Fatalf("a signed tree root that is not the WAL's: err=%v failure=%+v digest=%+v", err, result.Failure, result.Digest)
	}

	result, err = verifyWithDigest(t, verifyLedgerOptions{DataDir: dir, DigestPath: writeDigestFile(t, v1), PubKeyPath: pubPath})
	if err != nil || result.Digest.TreeRoot != "" {
		t.Fatalf("version 1 digest: err=%v tree_root %q, want none", err, result.Digest.TreeRoot)
	}
}

func keygen(t *testing.T, privPath string) (ledgerKeygenOutput, error) {
	t.Helper()
	var out bytes.Buffer
	err := runLedgerKeygen(&out, privPath)
	var result ledgerKeygenOutput
	if out.Len() > 0 {
		if jerr := json.Unmarshal(out.Bytes(), &result); jerr != nil {
			t.Fatalf("decode keygen output: %v\n%s", jerr, out.String())
		}
	}
	return result, err
}

func TestLedgerKeygenWritesTheStateKey(t *testing.T) {
	priv := filepath.Join(t.TempDir(), "node.key")
	result, err := keygen(t, priv)
	if err != nil {
		t.Fatal(err)
	}
	if result.StateKey != priv+".state" || strings.Join(result.Created, ",") != "private_key,public_key,state_key" {
		t.Fatalf("keygen output %+v", result)
	}
	if _, err := ledgerdigest.LoadStateKey(result.StateKey); err != nil {
		t.Fatalf("the state key it wrote: %v", err)
	}

	// A second run overwrites nothing.
	before := readFiles(t, priv, result.PublicKey, result.StateKey)
	if _, err := keygen(t, priv); err == nil || !strings.Contains(err.Error(), "never overwritten") {
		t.Fatalf("keygen over existing keys: %v", err)
	}
	if after := readFiles(t, priv, result.PublicKey, result.StateKey); after != before {
		t.Fatal("a second keygen changed the keys")
	}
}

// A node set up before ADR 0008 has a key pair and no state key: keygen
// adds the state key and leaves the pair alone.
func TestLedgerKeygenAddsAStateKeyToAnExistingKey(t *testing.T) {
	dir := t.TempDir()
	priv, pub := filepath.Join(dir, "node.key"), filepath.Join(dir, "node.pub")
	if _, err := ledgerdigest.GenerateKeyFiles(priv, pub); err != nil {
		t.Fatal(err)
	}
	signer, err := ledgerdigest.LoadSigner(priv)
	if err != nil {
		t.Fatal(err)
	}
	before := readFiles(t, priv, pub)
	result, err := keygen(t, priv)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(result.Created, ",") != "state_key" || result.KeyID != signer.KeyID() {
		t.Fatalf("keygen next to an existing key: %+v", result)
	}
	if readFiles(t, priv, pub) != before {
		t.Fatal("keygen changed the existing key pair")
	}
	if _, err := ledgerdigest.LoadStateKey(priv + ".state"); err != nil {
		t.Fatal(err)
	}

	// Something that is not a private key is not given a state key.
	bogus := filepath.Join(dir, "bogus.key")
	if err := os.WriteFile(bogus, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := keygen(t, bogus); err == nil || !strings.Contains(err.Error(), "not a usable private key") {
		t.Fatalf("keygen next to a bogus key: %v", err)
	}
	if _, err := os.Stat(bogus + ".state"); !os.IsNotExist(err) {
		t.Fatalf("a state key was written next to a bogus key: %v", err)
	}
}

func readFiles(t *testing.T, paths ...string) string {
	t.Helper()
	var all strings.Builder
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		all.Write(data)
	}
	return all.String()
}
