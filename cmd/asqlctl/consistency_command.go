package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/correodabid/asql/internal/storage/ledgerdigest"

	"github.com/jackc/pgx/v5"
)

// Consistency proofs (ADR 0008): prove-consistency asks a node for a proof
// that the log signed in one digest extends the log signed in an earlier
// one, and verify-consistency checks it with the trusted keys alone.

// errConsistencyVerifyFailed makes verify-consistency exit non-zero after
// printing a report in which a check failed.
var errConsistencyVerifyFailed = errors.New("consistency verification failed")

func isConsistencyCommand(command string) bool {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "prove-consistency", "verify-consistency":
		return true
	}
	return false
}

type proveConsistencyOptions struct {
	PgwireAddr string
	Principal  string
	Password   string
	AuthToken  string
	FromLSN    uint64
	ToLSN      uint64 // 0: the latest digest
	OutputPath string
}

// runProveConsistency runs SELECT * FROM asql_admin.prove_consistency on a
// node and writes the proof, with both digests, as JSON.
func runProveConsistency(ctx context.Context, out io.Writer, opts proveConsistencyOptions) error {
	if opts.FromLSN == 0 {
		return errors.New("prove-consistency requires -from-lsn, the LSN of a signed digest")
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

	query := fmt.Sprintf("SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = %d", opts.FromLSN)
	if opts.ToLSN > 0 {
		query += fmt.Sprintf(" AND to_lsn = %d", opts.ToLSN)
	}
	proof, err := queryConsistencyProof(ctx, conn, query)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(proof, "", "  ")
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
		"status": "proved", "output": opts.OutputPath,
		"from_lsn": proof.From.LSN, "to_lsn": proof.To.LSN, "proof_hashes": len(proof.Proof),
	})
}

// queryConsistencyProof rebuilds the proof from the row prove_consistency
// returns, whose columns are named as the proof's JSON fields and hold
// their JSON.
func queryConsistencyProof(ctx context.Context, conn *pgx.Conn, query string) (ledgerdigest.ConsistencyProof, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return ledgerdigest.ConsistencyProof{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return ledgerdigest.ConsistencyProof{}, err
		}
		return ledgerdigest.ConsistencyProof{}, errors.New("prove_consistency returned no row")
	}
	values, err := rows.Values()
	if err != nil {
		return ledgerdigest.ConsistencyProof{}, err
	}
	fields := map[string]json.RawMessage{}
	for i, field := range rows.FieldDescriptions() {
		text, ok := values[i].(string)
		if !ok {
			return ledgerdigest.ConsistencyProof{}, fmt.Errorf("prove_consistency: column %s is %T, want JSON text", field.Name, values[i])
		}
		fields[field.Name] = json.RawMessage(text)
	}
	if err := rows.Err(); err != nil {
		return ledgerdigest.ConsistencyProof{}, err
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return ledgerdigest.ConsistencyProof{}, err
	}
	var proof ledgerdigest.ConsistencyProof
	if err := json.Unmarshal(data, &proof); err != nil {
		return ledgerdigest.ConsistencyProof{}, fmt.Errorf("decode consistency proof: %w", err)
	}
	return proof, nil
}

type verifyConsistencyOptions struct {
	ProofPath  string
	PubKeyPath string
}

type verifyConsistencyOutput struct {
	Status  string `json:"status"`
	FromLSN uint64 `json:"from_lsn"`
	ToLSN   uint64 `json:"to_lsn"`
	// FromNode and ToNode name the nodes that signed each digest.
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
	Error    string `json:"error,omitempty"`
}

// runVerifyConsistency checks a proof written by prove-consistency: both
// digests' signatures against the trusted keys and the RFC 6962 proof
// between their tree roots. It needs no data directory.
func runVerifyConsistency(out io.Writer, opts verifyConsistencyOptions) error {
	if strings.TrimSpace(opts.ProofPath) == "" {
		return errors.New("verify-consistency requires -proof")
	}
	if strings.TrimSpace(opts.PubKeyPath) == "" {
		return errors.New("verify-consistency requires -pubkey with the trusted public key(s); a digest does not carry the key that verifies it")
	}
	data, err := os.ReadFile(opts.ProofPath)
	if err != nil {
		return fmt.Errorf("read proof: %w", err)
	}
	var proof ledgerdigest.ConsistencyProof
	if err := json.Unmarshal(data, &proof); err != nil {
		return fmt.Errorf("parse proof %s: %w", opts.ProofPath, err)
	}
	keys, err := ledgerdigest.LoadKeyring(opts.PubKeyPath)
	if err != nil {
		return err
	}
	result := verifyConsistencyOutput{
		Status:  "ok",
		FromLSN: proof.From.LSN, ToLSN: proof.To.LSN,
		FromNode: proof.From.NodeID, ToNode: proof.To.NodeID,
	}
	verifyErr := proof.Verify(keys)
	if verifyErr != nil {
		result.Status, result.Error = "mismatch", verifyErr.Error()
	}
	if err := printJSONTo(out, result); err != nil {
		return err
	}
	if verifyErr != nil {
		return fmt.Errorf("%w: %v", errConsistencyVerifyFailed, verifyErr)
	}
	return nil
}
