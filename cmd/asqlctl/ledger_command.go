package main

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/platform/datadir"
	"github.com/correodabid/asql/internal/storage/wal"
)

// errLedgerVerifyFailed makes verify-ledger exit non-zero after it has
// printed a report that found a mismatch.
var errLedgerVerifyFailed = errors.New("wal ledger verification failed")

func isLedgerCommand(command string) bool {
	return strings.EqualFold(strings.TrimSpace(command), "verify-ledger")
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
	Failure            *verifyLedgerFailure `json:"failure,omitempty"`
	Warnings           []string             `json:"warnings,omitempty"`
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
// With fromSnapshot, a WAL truncated by WAL GC is verified from its first
// retained segment and must meet a snapshot anchor.
func runVerifyLedger(out io.Writer, dataDir string, fromSnapshot bool) error {
	if strings.TrimSpace(dataDir) == "" {
		return errors.New("verify-ledger requires -data-dir")
	}
	dd, err := datadir.New(dataDir)
	if err != nil {
		return err
	}
	anchors, err := executor.SnapshotLedgerAnchors(dd.SnapDir())
	if err != nil {
		return fmt.Errorf("read snapshot anchors: %w", err)
	}
	report, err := wal.VerifyLedger(dd.WALBasePath(), wal.VerifyOptions{Anchors: anchors, FromAnchor: fromSnapshot})
	if err != nil {
		return err
	}

	result := verifyLedgerOutput{
		Status:             "ok",
		DataDir:            dataDir,
		FromGenesis:        report.FromGenesis,
		Segments:           report.Segments,
		UnanchoredSegments: report.UnanchoredSegments,
		Records:            report.Records,
		StartLSN:           report.StartLSN,
		LastLSN:            report.LastLSN,
		Head:               report.Head.String(),
		SnapshotAnchors:    len(anchors),
		AnchorsChecked:     report.AnchorsChecked,
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
