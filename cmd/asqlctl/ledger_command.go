package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/platform/datadir"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/logtree"
	"github.com/correodabid/asql/internal/storage/wal"
)

// errLedgerVerifyFailed makes verify-ledger exit non-zero after it has
// printed a report that found a mismatch.
var errLedgerVerifyFailed = errors.New("wal ledger verification failed")

func isLedgerCommand(command string) bool {
	return strings.EqualFold(strings.TrimSpace(command), "verify-ledger")
}

func isLedgerKeygenCommand(command string) bool {
	return strings.EqualFold(strings.TrimSpace(command), "ledger-keygen")
}

// Digest failure kinds, reported next to the WAL verifier's own kinds.
const (
	verifyDigestSignature  = "digest-signature"
	verifyDigestBeyondWAL  = "digest-beyond-wal"
	verifyDigestNotCovered = "digest-not-covered"
	verifyDigestTree       = "digest-tree-mismatch"
)

type verifyLedgerOptions struct {
	DataDir      string
	FromSnapshot bool
	// DigestPath is a signed digest (JSON, as GET /api/v1/ledger/digest
	// returns it). PubKeyPath holds the trusted public keys that verify it.
	DigestPath string
	PubKeyPath string
}

type verifyLedgerOutput struct {
	Status             string               `json:"status"`
	DataDir            string               `json:"data_dir"`
	FromGenesis        bool                 `json:"from_genesis"`
	Segments           int                  `json:"segments"`
	UnanchoredSegments int                  `json:"unanchored_segments"`
	Records            uint64               `json:"records"`
	StartLSN           uint64               `json:"start_lsn"`
	LastLSN            uint64               `json:"last_lsn"`
	Head               string               `json:"head"`
	SnapshotAnchors    int                  `json:"snapshot_anchors"`
	AnchorsChecked     int                  `json:"anchors_checked"`
	Digest             *verifyLedgerDigest  `json:"digest,omitempty"`
	Failure            *verifyLedgerFailure `json:"failure,omitempty"`
	Warnings           []string             `json:"warnings,omitempty"`
}

type verifyLedgerDigest struct {
	LSN      uint64 `json:"lsn"`
	NodeID   string `json:"node_id"`
	KeyID    string `json:"key_id"`
	SignedAt string `json:"signed_at"`
	// Signature is "valid" or "invalid"; Chain is "match", "mismatch" or
	// "not-checked". TreeRoot, for version 2 digests (ADR 0008), compares the
	// signed log tree root with the one rebuilt from the WAL: "match",
	// "mismatch" or "not-checked" (the WAL does not start at LSN 1).
	Signature string `json:"signature"`
	Chain     string `json:"chain"`
	TreeRoot  string `json:"tree_root,omitempty"`
}

type verifyLedgerFailure struct {
	Kind string `json:"kind"`
	// The modification lies in LSNs (after_lsn, at_lsn]; exact when
	// at_lsn == after_lsn+1.
	AfterLSN uint64 `json:"after_lsn"`
	AtLSN    uint64 `json:"at_lsn"`
	Exact    bool   `json:"exact"`
	Segment  string `json:"segment,omitempty"`
	Anchor   string `json:"anchor,omitempty"`
	Message  string `json:"message"`
}

// runVerifyLedger recomputes the WAL ledger chain of a local data directory
// (ADR 0005) and checks it against segment header and snapshot anchors.
// With FromSnapshot, a WAL truncated by WAL GC is verified from its first
// retained segment and must meet a snapshot anchor.
//
// With a digest, its signature is checked against the trusted public keys
// and its chain value against the recomputed chain. Unlike anchors on disk, a
// digest kept elsewhere exposes a WAL that was rewritten together with its
// anchors.
func runVerifyLedger(out io.Writer, opts verifyLedgerOptions) error {
	if strings.TrimSpace(opts.DataDir) == "" {
		return errors.New("verify-ledger requires -data-dir")
	}
	var digest *ledgerdigest.Digest
	var keys ledgerdigest.Keyring
	if opts.DigestPath != "" {
		if opts.PubKeyPath == "" {
			return errors.New("verify-ledger -digest requires -pubkey with the trusted public key(s); a digest does not carry the key that verifies it")
		}
		var err error
		if digest, err = readDigestFile(opts.DigestPath); err != nil {
			return err
		}
		if keys, err = ledgerdigest.LoadKeyring(opts.PubKeyPath); err != nil {
			return err
		}
	} else if opts.PubKeyPath != "" {
		return errors.New("verify-ledger -pubkey is only used with -digest")
	}
	dd, err := datadir.New(opts.DataDir)
	if err != nil {
		return err
	}
	anchors, err := executor.SnapshotLedgerAnchors(dd.SnapDir())
	if err != nil {
		return fmt.Errorf("read snapshot anchors: %w", err)
	}
	snapshotAnchors := len(anchors)

	var digestOut *verifyLedgerDigest
	var digestFailure *wal.VerifyFailure
	var digestSource string
	if digest != nil {
		digestOut = &verifyLedgerDigest{
			LSN:       digest.LSN,
			NodeID:    digest.NodeID,
			KeyID:     digest.KeyID,
			SignedAt:  digest.SignedAt.UTC().Format(time.RFC3339Nano),
			Signature: "valid",
			Chain:     "not-checked",
		}
		if err := digest.Verify(keys); err != nil {
			digestOut.Signature = "invalid"
			digestFailure = &wal.VerifyFailure{Kind: verifyDigestSignature, AtLSN: digest.LSN, Anchor: "digest", Message: err.Error()}
		} else {
			// First, so that it is the anchor checked when another one shares
			// its LSN.
			digestSource = fmt.Sprintf("signed digest (key %s, node %q)", digest.KeyID, digest.NodeID)
			anchors = append([]wal.LedgerAnchor{{LSN: digest.LSN, Chain: digest.Chain, Source: digestSource}}, anchors...)
		}
	}

	// Rebuild the log tree up to a version 2 digest's LSN while verifying.
	var tree *logtree.Frontier
	verifyOpts := wal.VerifyOptions{Anchors: anchors, FromAnchor: opts.FromSnapshot}
	if digestOut != nil && digestFailure == nil && digest.HasTree() {
		digestOut.TreeRoot = "not-checked"
		tree = &logtree.Frontier{}
		verifyOpts.OnLeaf = func(lsn uint64, leaf wal.ChainHash) {
			if lsn <= digest.LSN && uint64(tree.Size())+1 == lsn {
				tree.Add(logtree.Hash(leaf))
			}
		}
	}
	report, err := wal.VerifyLedger(dd.WALBasePath(), verifyOpts)
	if err != nil {
		return err
	}
	if digestOut != nil && digestFailure == nil {
		switch {
		case report.Failure != nil && report.Failure.Anchor == digestSource:
			digestOut.Chain = "mismatch"
		case report.Failure != nil && report.Failure.AtLSN <= digest.LSN:
			// The WAL failed before the digest could be checked.
		case report.Failure != nil:
			digestOut.Chain = "match"
		case digest.LSN > report.LastLSN:
			digestFailure = &wal.VerifyFailure{Kind: verifyDigestBeyondWAL, AfterLSN: report.LastLSN, AtLSN: digest.LSN, Anchor: "digest",
				Message: fmt.Sprintf("the WAL ends at LSN %d but the signed digest covers LSN %d: committed records were removed", report.LastLSN, digest.LSN)}
		case digest.LSN < report.StartLSN:
			digestFailure = &wal.VerifyFailure{Kind: verifyDigestNotCovered, AtLSN: digest.LSN, Anchor: "digest",
				Message: fmt.Sprintf("the retained WAL starts after LSN %d, so the digest at LSN %d cannot be checked", report.StartLSN, digest.LSN)}
		default:
			digestOut.Chain = "match"
		}
		if tree != nil && digestOut.Chain == "match" && uint64(tree.Size()) == digest.LSN {
			if root := tree.Root(); root == logtree.Hash(digest.TreeRoot) {
				digestOut.TreeRoot = "match"
			} else {
				digestOut.TreeRoot = "mismatch"
				digestFailure = &wal.VerifyFailure{Kind: verifyDigestTree, AtLSN: digest.LSN, Anchor: "digest",
					Message: fmt.Sprintf("the signed log tree root %x is not the root of the WAL's records 1..%d, %x", digest.TreeRoot, digest.LSN, root)}
			}
		}
	}
	if report.Failure == nil && digestFailure != nil {
		report.Failure = digestFailure
	}

	result := verifyLedgerOutput{
		Status:             "ok",
		DataDir:            opts.DataDir,
		FromGenesis:        report.FromGenesis,
		Segments:           report.Segments,
		UnanchoredSegments: report.UnanchoredSegments,
		Records:            report.Records,
		StartLSN:           report.StartLSN,
		LastLSN:            report.LastLSN,
		Head:               report.Head.String(),
		SnapshotAnchors:    snapshotAnchors,
		AnchorsChecked:     report.AnchorsChecked,
		Digest:             digestOut,
		Warnings:           report.Warnings,
	}
	if f := report.Failure; f != nil {
		result.Status = "mismatch"
		result.Failure = &verifyLedgerFailure{
			Kind:     f.Kind,
			AfterLSN: f.AfterLSN,
			AtLSN:    f.AtLSN,
			Exact:    f.Exact(),
			Segment:  f.Segment,
			Anchor:   f.Anchor,
			Message:  f.Message,
		}
	}
	if err := printJSONTo(out, result); err != nil {
		return err
	}
	if report.Failure != nil {
		return fmt.Errorf("%w: %s", errLedgerVerifyFailed, report.Failure)
	}
	return nil
}

// readDigestFile reads one signed digest in its JSON form.
func readDigestFile(path string) (*ledgerdigest.Digest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read digest: %w", err)
	}
	var d ledgerdigest.Digest
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("parse digest %s: %w", path, err)
	}
	return &d, nil
}

type ledgerKeygenOutput struct {
	PrivateKey string `json:"private_key"`
	PublicKey  string `json:"public_key"`
	KeyID      string `json:"key_id"`
}

// runLedgerKeygen writes a new Ed25519 key pair for ledger=signed: the
// private key at privPath (mode 0600) and the public key next to it with a
// .pub extension.
func runLedgerKeygen(out io.Writer, privPath string) error {
	if strings.TrimSpace(privPath) == "" {
		return errors.New("ledger-keygen requires -output (private key path)")
	}
	pubPath := strings.TrimSuffix(privPath, filepath.Ext(privPath)) + ".pub"
	if pubPath == privPath {
		pubPath = privPath + ".pub"
	}
	pub, err := ledgerdigest.GenerateKeyFiles(privPath, pubPath)
	if err != nil {
		return err
	}
	return printJSONTo(out, ledgerKeygenOutput{PrivateKey: privPath, PublicKey: pubPath, KeyID: ledgerdigest.KeyID(pub)})
}
