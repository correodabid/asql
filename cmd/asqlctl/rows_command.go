package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/statetree"

	"github.com/jackc/pgx/v5"
)

// Row proofs (ADR 0008): prove-rows asks a node to prove a set of rows of
// VERIFIABLE tables, cited by primary key, against a signed digest, and
// verify-rows checks the proof with the trusted keys alone.

// errRowsVerifyFailed makes verify-rows exit non-zero after printing a
// report in which a check failed.
var errRowsVerifyFailed = errors.New("row proof verification failed")

func isRowsCommand(command string) bool {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "prove-rows", "verify-rows":
		return true
	}
	return false
}

type proveRowsOptions struct {
	PgwireAddr string
	Principal  string
	Password   string
	AuthToken  string
	// KeysPath is a JSON file: [{"domain": …, "table": …, "pk": [...]}, …].
	KeysPath   string
	LSN        uint64 // 0: the latest digest
	OutputPath string
}

// runProveRows runs SELECT * FROM asql_admin.prove_rows on a node and
// writes the proof, with the digest and the rows, as JSON.
func runProveRows(ctx context.Context, out io.Writer, opts proveRowsOptions) error {
	if strings.TrimSpace(opts.KeysPath) == "" {
		return errors.New(`prove-rows requires -keys, a JSON file: [{"domain": …, "table": …, "pk": [...]}, …]`)
	}
	keys, err := os.ReadFile(opts.KeysPath)
	if err != nil {
		return fmt.Errorf("read keys: %w", err)
	}
	if !json.Valid(keys) {
		return fmt.Errorf("keys %s is not valid JSON", opts.KeysPath)
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

	query := "SELECT * FROM asql_admin.prove_rows WHERE keys = '" + strings.ReplaceAll(string(keys), "'", "''") + "'"
	if opts.LSN > 0 {
		query += fmt.Sprintf(" AND lsn = %d", opts.LSN)
	}
	proof, err := queryRowsProof(ctx, conn, query)
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
		"lsn": proof.Digest.LSN, "rows": len(proof.Rows), "proof_hashes": len(proof.Proof.Siblings),
	})
}

// queryRowsProof rebuilds the proof from the row prove_rows returns, whose
// columns are named as the proof's JSON fields and hold their JSON.
func queryRowsProof(ctx context.Context, conn *pgx.Conn, query string) (statetree.RowsProof, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return statetree.RowsProof{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return statetree.RowsProof{}, err
		}
		return statetree.RowsProof{}, errors.New("prove_rows returned no row")
	}
	values, err := rows.Values()
	if err != nil {
		return statetree.RowsProof{}, err
	}
	fields := map[string]json.RawMessage{}
	for i, field := range rows.FieldDescriptions() {
		text, ok := values[i].(string)
		if !ok {
			return statetree.RowsProof{}, fmt.Errorf("prove_rows: column %s is %T, want JSON text", field.Name, values[i])
		}
		fields[field.Name] = json.RawMessage(text)
	}
	if err := rows.Err(); err != nil {
		return statetree.RowsProof{}, err
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return statetree.RowsProof{}, err
	}
	var proof statetree.RowsProof
	if err := json.Unmarshal(data, &proof); err != nil {
		return statetree.RowsProof{}, fmt.Errorf("decode row proof: %w", err)
	}
	return proof, nil
}

type verifyRowsOptions struct {
	ProofPath  string
	PubKeyPath string
}

type verifyRowsOutput struct {
	Status string `json:"status"`
	LSN    uint64 `json:"lsn"`
	NodeID string `json:"node_id"`
	// Rows names each proven row as a report cites it: table and key.
	Rows  []string `json:"rows"`
	Error string   `json:"error,omitempty"`
}

// runVerifyRows checks a proof written by prove-rows: the digest's
// signature against the trusted keys and every row against its state root.
// It needs no data directory and no secret.
func runVerifyRows(out io.Writer, opts verifyRowsOptions) error {
	if strings.TrimSpace(opts.ProofPath) == "" {
		return errors.New("verify-rows requires -proof")
	}
	if strings.TrimSpace(opts.PubKeyPath) == "" {
		return errors.New("verify-rows requires -pubkey with the trusted public key(s); a digest does not carry the key that verifies it")
	}
	data, err := os.ReadFile(opts.ProofPath)
	if err != nil {
		return fmt.Errorf("read proof: %w", err)
	}
	var proof statetree.RowsProof
	if err := json.Unmarshal(data, &proof); err != nil {
		return fmt.Errorf("parse proof %s: %w", opts.ProofPath, err)
	}
	keys, err := ledgerdigest.LoadKeyring(opts.PubKeyPath)
	if err != nil {
		return err
	}
	result := verifyRowsOutput{Status: "ok", LSN: proof.Digest.LSN, NodeID: proof.Digest.NodeID}
	for _, row := range proof.Rows {
		parts := make([]string, len(row.PrimaryKeyColumns))
		for i, column := range row.PrimaryKeyColumns {
			value := ""
			if i < len(row.PrimaryKey) {
				value = literalText(row.PrimaryKey[i])
			}
			parts[i] = column + "=" + value
		}
		result.Rows = append(result.Rows, fmt.Sprintf("%s.%s(%s)", row.Domain, row.Table, strings.Join(parts, ", ")))
	}
	verifyErr := proof.Verify(keys)
	if verifyErr != nil {
		result.Status, result.Error = "mismatch", verifyErr.Error()
	}
	if err := printJSONTo(out, result); err != nil {
		return err
	}
	if verifyErr != nil {
		return fmt.Errorf("%w: %v", errRowsVerifyFailed, verifyErr)
	}
	return nil
}

// literalText renders a key value for the report.
func literalText(value ast.Literal) string {
	switch value.Kind {
	case ast.LiteralNumber, ast.LiteralTimestamp:
		return strconv.FormatInt(value.NumberValue, 10)
	case ast.LiteralFloat:
		return strconv.FormatFloat(value.FloatValue, 'g', -1, 64)
	case ast.LiteralBoolean:
		return strconv.FormatBool(value.BoolValue)
	case ast.LiteralNull, "":
		return "NULL"
	default:
		return value.StringValue
	}
}
