package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/platform/buildinfo"
	"github.com/correodabid/asql/internal/platform/datadir"
	pgwire "github.com/correodabid/asql/internal/server/pgwire"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/wal"

	"github.com/jackc/pgx/v5"
)

// Signed query results (ADR 0006): attest-query asks a node for a result
// with a signed attestation, and verify-result checks one.

// errResultVerifyFailed makes verify-result exit non-zero after printing a
// report in which a check failed.
var errResultVerifyFailed = errors.New("result verification failed")

func isAttestCommand(command string) bool {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "attest-query", "verify-result":
		return true
	}
	return false
}

type attestQueryOptions struct {
	PgwireAddr string
	Principal  string
	Password   string
	AuthToken  string
	SQL        string
	LSN        uint64
	OutputPath string
}

// runAttestQuery runs SELECT * FROM asql_admin.attest_query on a node and
// writes the attested result as JSON.
func runAttestQuery(ctx context.Context, out io.Writer, opts attestQueryOptions) error {
	if strings.TrimSpace(opts.SQL) == "" {
		return errors.New("attest-query requires -sql")
	}
	connString := buildConnString(shellConfig{PgwireAddr: opts.PgwireAddr, AuthToken: opts.AuthToken})
	if opts.Principal != "" {
		connString = fmt.Sprintf("postgres://%s:%s@%s/asql?sslmode=disable&default_query_exec_mode=simple_protocol",
			opts.Principal, opts.Password, opts.PgwireAddr)
	}
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("connect pgwire: %w", err)
	}
	defer conn.Close(ctx)

	query := "SELECT * FROM asql_admin.attest_query WHERE sql = '" + strings.ReplaceAll(opts.SQL, "'", "''") + "'"
	if opts.LSN > 0 {
		query += fmt.Sprintf(" AND lsn = %d", opts.LSN)
	}
	result, err := queryAttestedResult(ctx, conn, query)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if opts.OutputPath == "" {
		_, err = out.Write(data)
		return err
	}
	if err := os.WriteFile(opts.OutputPath, data, 0o644); err != nil {
		return err
	}
	return printJSONTo(out, map[string]any{
		"status": "attested", "output": opts.OutputPath,
		"lsn": result.Attestation.LSN, "row_count": result.Attestation.RowCount, "key_id": result.Attestation.KeyID,
	})
}

// queryAttestedResult rebuilds the attested result from the row attest_query
// returns, whose columns are named as the attestation's JSON fields.
func queryAttestedResult(ctx context.Context, conn *pgx.Conn, query string) (ledgerdigest.AttestedResult, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return ledgerdigest.AttestedResult{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return ledgerdigest.AttestedResult{}, err
		}
		return ledgerdigest.AttestedResult{}, errors.New("attest_query returned no row")
	}
	values, err := rows.Values()
	if err != nil {
		return ledgerdigest.AttestedResult{}, err
	}
	fields := map[string]json.RawMessage{}
	var encodedRows json.RawMessage
	for i, field := range rows.FieldDescriptions() {
		var raw json.RawMessage
		if text, ok := values[i].(string); ok && (field.Name == "domains" || field.Name == "columns" || field.Name == "rows") {
			raw = json.RawMessage(text)
		} else if raw, err = json.Marshal(values[i]); err != nil {
			return ledgerdigest.AttestedResult{}, err
		}
		if field.Name == "rows" {
			encodedRows = raw
			continue
		}
		fields[field.Name] = raw
	}
	if err := rows.Err(); err != nil {
		return ledgerdigest.AttestedResult{}, err
	}
	attestation, err := json.Marshal(fields)
	if err != nil {
		return ledgerdigest.AttestedResult{}, err
	}
	var result ledgerdigest.AttestedResult
	if err := json.Unmarshal(attestation, &result.Attestation); err != nil {
		return ledgerdigest.AttestedResult{}, fmt.Errorf("attest_query row: %w", err)
	}
	if err := json.Unmarshal(encodedRows, &result.Rows); err != nil {
		return ledgerdigest.AttestedResult{}, fmt.Errorf("attest_query rows: %w", err)
	}
	return result, nil
}

type verifyResultOptions struct {
	ResultPath string
	PubKeyPath string
	DigestPath string
	DataDir    string
}

// Outcomes of each verify-result check.
const (
	checkPass       = "valid"
	checkFail       = "invalid"
	checkNotChecked = "not-checked"
	// checkVersionMismatch: re-execution gave other rows on another engine
	// version, which is not guaranteed to give the same ones (ADR 0006).
	checkVersionMismatch = "version-mismatch"
)

type verifyResultCheck struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type verifyResultOutput struct {
	Status        string            `json:"status"`
	LSN           uint64            `json:"lsn"`
	NodeID        string            `json:"node_id"`
	KeyID         string            `json:"key_id"`
	SQL           string            `json:"sql"`
	RowCount      uint64            `json:"row_count"`
	EngineVersion string            `json:"engine_version"`
	Signature     verifyResultCheck `json:"signature"`
	Rows          verifyResultCheck `json:"rows"`
	Chain         verifyResultCheck `json:"chain"`
	Reexecution   verifyResultCheck `json:"reexecution"`
}

// runVerifyResult checks an attested result, step by step (ADR 0006 § 4):
//
//  1. the signature, against the trusted keys only;
//  2. the rows, against the attested hash, row count and columns;
//  3. the chain value: against a digest at the same LSN, or, with the data
//     directory, against the WAL's chain at the LSN, which must extend to
//     the digest when there is one;
//  4. with the data directory: the query re-executed at the LSN, on a
//     scratch copy, gives the attested hash.
//
// Each outcome is reported apart, so a verifier without the data still
// checks 1 to 3. Step 4 is what shows the answer is right, not only signed.
func runVerifyResult(ctx context.Context, out io.Writer, opts verifyResultOptions) error {
	if opts.ResultPath == "" {
		return errors.New("verify-result requires -result")
	}
	if opts.PubKeyPath == "" {
		return errors.New("verify-result requires -pubkey with the trusted public key(s); an attestation does not carry the key that verifies it")
	}
	data, err := os.ReadFile(opts.ResultPath)
	if err != nil {
		return err
	}
	var result ledgerdigest.AttestedResult
	if err := json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("read attested result: %w", err)
	}
	keys, err := ledgerdigest.LoadKeyring(opts.PubKeyPath)
	if err != nil {
		return err
	}
	var digest *ledgerdigest.Digest
	if opts.DigestPath != "" {
		if digest, err = readDigestFile(opts.DigestPath); err != nil {
			return err
		}
	}

	a := result.Attestation
	report := verifyResultOutput{
		LSN: a.LSN, NodeID: a.NodeID, KeyID: a.KeyID, SQL: a.SQL, RowCount: a.RowCount, EngineVersion: a.EngineVersion,
		Signature:   verifyResultCheck{Status: checkPass},
		Rows:        verifyResultCheck{Status: checkPass},
		Chain:       verifyResultCheck{Status: checkNotChecked},
		Reexecution: verifyResultCheck{Status: checkNotChecked},
	}
	if err := a.Verify(keys); err != nil {
		report.Signature = verifyResultCheck{Status: checkFail, Message: err.Error()}
	}
	if err := a.CheckRows(result.Rows); err != nil {
		report.Rows = verifyResultCheck{Status: checkFail, Message: err.Error()}
	}

	if digest != nil {
		report.Chain = checkChainAgainstDigest(a, *digest, keys)
	}
	if opts.DataDir != "" {
		chain, reexecution := checkAgainstData(ctx, a, digest, opts.DataDir)
		if report.Chain.Status != checkFail {
			report.Chain = chain
		}
		report.Reexecution = reexecution
	}

	report.Status = "ok"
	for _, check := range []verifyResultCheck{report.Signature, report.Rows, report.Chain, report.Reexecution} {
		if check.Status == checkFail || check.Status == checkVersionMismatch {
			report.Status = "mismatch"
		}
	}
	if err := printJSONTo(out, report); err != nil {
		return err
	}
	if report.Status != "ok" {
		return errResultVerifyFailed
	}
	return nil
}

// checkChainAgainstDigest checks a's chain value with a digest alone, which
// it can only do when the digest is at the same LSN.
func checkChainAgainstDigest(a ledgerdigest.Attestation, digest ledgerdigest.Digest, keys ledgerdigest.Keyring) verifyResultCheck {
	if err := digest.Verify(keys); err != nil {
		return verifyResultCheck{Status: checkFail, Message: "digest: " + err.Error()}
	}
	switch {
	case digest.LSN < a.LSN:
		return verifyResultCheck{Status: checkFail, Message: fmt.Sprintf("the digest is at LSN %d, before the attested LSN %d, so it does not cover it", digest.LSN, a.LSN)}
	case digest.LSN == a.LSN && digest.Chain != a.Chain:
		return verifyResultCheck{Status: checkFail, Message: fmt.Sprintf("the attested chain at LSN %d differs from the digest's", a.LSN)}
	case digest.LSN == a.LSN:
		return verifyResultCheck{Status: checkPass, Message: "matches the digest at the same LSN"}
	}
	return verifyResultCheck{Status: checkNotChecked, Message: fmt.Sprintf("the digest is at LSN %d, after the attested LSN; -data-dir checks that the WAL links them", digest.LSN)}
}

// checkAgainstData checks a against a copy of the data directory: the WAL's
// chain at a.LSN, tied to the digest when there is one, and the query
// re-executed there.
func checkAgainstData(ctx context.Context, a ledgerdigest.Attestation, digest *ledgerdigest.Digest, dataDir string) (verifyResultCheck, verifyResultCheck) {
	fail := func(err error) (verifyResultCheck, verifyResultCheck) {
		c := verifyResultCheck{Status: checkFail, Message: err.Error()}
		return c, c
	}
	scratch, err := os.MkdirTemp("", "asql-verify-result-")
	if err != nil {
		return fail(err)
	}
	defer os.RemoveAll(scratch)
	copyDir := filepath.Join(scratch, "data")
	if err := copyTree(dataDir, copyDir); err != nil {
		return fail(fmt.Errorf("copy data directory: %w", err))
	}
	dd, err := datadir.New(copyDir)
	if err != nil {
		return fail(err)
	}

	chainCheck := verifyResultCheck{Status: checkPass, Message: "matches the WAL"}
	if digest != nil {
		anchors := []wal.LedgerAnchor{{LSN: digest.LSN, Chain: digest.Chain, Source: "signed digest"}}
		report, err := wal.VerifyLedger(dd.WALBasePath(), wal.VerifyOptions{Anchors: anchors, FromAnchor: true})
		switch {
		case err != nil:
			chainCheck = verifyResultCheck{Status: checkFail, Message: err.Error()}
		case !report.OK:
			chainCheck = verifyResultCheck{Status: checkFail, Message: "the WAL does not match the digest: " + report.Failure.String()}
		case digest.LSN > report.LastLSN:
			chainCheck = verifyResultCheck{Status: checkFail, Message: fmt.Sprintf("the WAL ends at LSN %d, before the digest's LSN %d", report.LastLSN, digest.LSN)}
		default:
			chainCheck.Message = fmt.Sprintf("matches the WAL, which matches the digest at LSN %d", digest.LSN)
		}
	}

	store, err := wal.NewSegmentedLogStore(dd.WALBasePath(), wal.AlwaysSync{})
	if err != nil {
		return fail(err)
	}
	defer store.Close()
	if chain, err := store.ChainAt(a.LSN); err != nil {
		chainCheck = verifyResultCheck{Status: checkFail, Message: err.Error()}
	} else if chain != a.Chain {
		chainCheck = verifyResultCheck{Status: checkFail, Message: fmt.Sprintf("the WAL's chain at LSN %d is %x, the attestation's %x", a.LSN, chain, a.Chain)}
	}

	engine, err := executor.New(ctx, store, dd.SnapDir())
	if err != nil {
		return chainCheck, verifyResultCheck{Status: checkFail, Message: "open the copy: " + err.Error()}
	}
	defer engine.WaitPendingSnapshots()
	res, err := engine.TimeTravelQueryAsOfLSN(ctx, a.SQL, a.Domains, a.LSN)
	if err != nil {
		return chainCheck, verifyResultCheck{Status: checkFail, Message: "re-execute: " + err.Error()}
	}
	_, rows, err := pgwire.AttestedRows(a.SQL, res.Rows)
	if err != nil {
		return chainCheck, verifyResultCheck{Status: checkFail, Message: "re-execute: " + err.Error()}
	}
	if err := a.CheckRows(rows); err != nil {
		if a.EngineVersion != buildinfo.EngineVersion() {
			return chainCheck, verifyResultCheck{Status: checkVersionMismatch,
				Message: fmt.Sprintf("re-executed with engine %s, attested with %s, which is not guaranteed to give the same rows: %v", buildinfo.EngineVersion(), a.EngineVersion, err)}
		}
		return chainCheck, verifyResultCheck{Status: checkFail, Message: "re-executed at LSN " + fmt.Sprint(a.LSN) + ": " + err.Error()}
	}
	return chainCheck, verifyResultCheck{Status: checkPass, Message: fmt.Sprintf("the query re-executed at LSN %d gives the attested rows", a.LSN)}
}

// copyTree copies the regular files and directories of src to dst.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o600)
	})
}
