package pgwire

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/sqlerr"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/statetree"
)

// Row proofs (ADR 0008): SELECT * FROM asql_admin.prove_rows WHERE keys =
// '<JSON list of {domain, table, pk}>' [AND lsn = N] proves that the state
// signed by the version 3 digest at LSN N (the latest one by default) held
// those rows, in one batch proof that reveals nothing about other rows.
var (
	errProveRows    = sqlerr.New("22023", "prove_rows")
	errProveRowsOff = sqlerr.New("55000", "prove_rows: row proofs need ledger mode signed and a state key")

	reProveRowsKeys = regexp.MustCompile(`(?is)\bkeys\s*=\s*'((?:[^']|'')*)'`)
	reProveRowsLSN  = regexp.MustCompile(`(?i)\blsn\s*=\s*(\d+)`)
)

// proveRowsColumns are named as the JSON fields of statetree.RowsProof;
// each holds that field's JSON.
var proveRowsColumns = []string{"digest", "rows", "proof"}

// proveRowsKey is one row asked for: its table and primary key values, in
// key order, as JSON scalars.
type proveRowsKey struct {
	Domain string            `json:"domain"`
	Table  string            `json:"table"`
	PK     []json.RawMessage `json:"pk"`
}

func (server *Server) adminProveRows(ctx context.Context, sql, principal string) (interceptResult, bool) {
	row, err := server.proveRows(ctx, sql, principal)
	if err != nil {
		return interceptResult{err: err}, true
	}
	return interceptResult{result: executor.Result{Status: "OK", Rows: []map[string]ast.Literal{row}}, columns: proveRowsColumns}, true
}

func (server *Server) proveRows(ctx context.Context, sql, principal string) (map[string]ast.Literal, error) {
	if server.ledger == nil || server.ledger.stateTree == nil {
		return nil, errProveRowsOff
	}
	m := reProveRowsKeys.FindStringSubmatchIndex(sql)
	if m == nil {
		return nil, fmt.Errorf(`%w: WHERE keys = '[{"domain": …, "table": …, "pk": [...]}, …]' is required`, errProveRows)
	}
	var keys []proveRowsKey
	if err := json.Unmarshal([]byte(strings.ReplaceAll(sql[m[2]:m[3]], "''", "'")), &keys); err != nil {
		return nil, fmt.Errorf("%w: keys: %v", errProveRows, err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: keys is empty", errProveRows)
	}
	// The LSN is read from the rest of the WHERE, not from the keys.
	where := sql[:m[0]] + sql[m[1]:]

	digest, err := server.ledger.stateDigest(where)
	if err != nil {
		return nil, err
	}

	// The privileges of reading the tables, at that LSN.
	tables := map[[2]string]bool{}
	for _, k := range keys {
		tables[[2]string{strings.ToLower(k.Domain), strings.ToLower(k.Table)}] = true
	}
	for t := range tables {
		if _, err := server.engine.AuthorizeSQL(principal, "SELECT * FROM "+t[0]+"."+t[1], []string{t[0]}); err != nil {
			return nil, err
		}
	}
	if digest.LSN != server.engine.HeadLSN() {
		if err := server.authorizeHistoricalReadPrincipal(principal, historicalReadAuditDetail{queryKind: "prove_rows", targetKind: "lsn", targetLSN: digest.LSN}, true); err != nil {
			return nil, err
		}
	}

	tree, view, err := server.ledger.stateTree.TreeAt(ctx, digest.LSN)
	if err != nil {
		return nil, err
	}
	if tree.Root() != digest.StateRoot {
		return nil, fmt.Errorf("prove_rows: the state tree at LSN %d does not match its digest", digest.LSN)
	}
	keyColumns := map[[2]string][]string{}
	for _, t := range view.Tables() {
		keyColumns[[2]string{t.Domain, t.Name}] = t.PrimaryKey
	}

	proof := statetree.RowsProof{Digest: digest}
	treeKeys := make([]statetree.Hash, 0, len(keys))
	leaves := map[statetree.Hash]statetree.Hash{}
	for i, k := range keys {
		domain, table := strings.ToLower(k.Domain), strings.ToLower(k.Table)
		columns, ok := keyColumns[[2]string{domain, table}]
		if !ok {
			return nil, fmt.Errorf("%w: key %d: %s.%s is not a VERIFIABLE table at LSN %d", errProveRows, i, domain, table, digest.LSN)
		}
		values, err := proveRowsValues(k.PK)
		if err != nil {
			return nil, fmt.Errorf("%w: key %d: %v", errProveRows, i, err)
		}
		row, found, err := view.Row(domain, table, values)
		if err != nil {
			return nil, fmt.Errorf("%w: key %d: %v", errProveRows, i, err)
		}
		if !found {
			return nil, fmt.Errorf("%w: key %d: %s.%s has no row with that primary key at LSN %d", errProveRows, i, domain, table, digest.LSN)
		}
		key, leaf, err := stateTreeEntry(*server.ledger.stateKey, domain, table, row)
		if err != nil {
			return nil, err
		}
		if _, dup := leaves[key]; dup {
			return nil, fmt.Errorf("%w: key %d: the same row is asked for twice", errProveRows, i)
		}
		leaves[key] = leaf
		treeKeys = append(treeKeys, key)
		proof.Rows = append(proof.Rows, statetree.ProvenRow{
			Domain: domain, Table: table, PrimaryKeyColumns: columns, PrimaryKey: row.PrimaryKey,
			Row: row.Row, Salt: statetree.Salt(*server.ledger.stateKey, key, row.LSN),
		})
	}
	entries, batch, err := tree.Prove(treeKeys)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if leaves[e.Key] != e.Leaf {
			return nil, fmt.Errorf("prove_rows: a row at LSN %d does not match the state tree", digest.LSN)
		}
	}
	proof.Proof = batch

	data, err := json.Marshal(proof)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	row := map[string]ast.Literal{}
	for _, column := range proveRowsColumns {
		row[column] = litS(string(fields[column]))
	}
	return row, nil
}

// stateDigest returns the version 3 digest a row proof is checked against:
// the one signed at the LSN the WHERE names, or the latest.
func (ls *ledgerSigning) stateDigest(where string) (ledgerdigest.Digest, error) {
	var d ledgerdigest.Digest
	if m := reProveRowsLSN.FindStringSubmatch(where); m != nil {
		lsn, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			return d, fmt.Errorf("%w: lsn: %v", errProveRows, err)
		}
		if d, err = ls.digestAt(lsn); err != nil {
			return d, fmt.Errorf("%w: %v", errProveRows, strings.TrimPrefix(err.Error(), "prove_consistency: "))
		}
	} else {
		latest, ok := ls.log.Latest()
		if !ok {
			return d, fmt.Errorf("%w: no digest signed yet", errProveRows)
		}
		d = latest
	}
	if !d.HasState() {
		return d, fmt.Errorf("%w: the digest at LSN %d is version %d and signs no state root", errProveRows, d.LSN, d.Version)
	}
	return d, nil
}

// proveRowsValues turns JSON scalars into key values.
func proveRowsValues(raw []json.RawMessage) ([]ast.Literal, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("pk is empty")
	}
	values := make([]ast.Literal, len(raw))
	for i, r := range raw {
		var v any
		if err := json.Unmarshal(r, &v); err != nil {
			return nil, err
		}
		switch x := v.(type) {
		case string:
			values[i] = ast.Literal{Kind: ast.LiteralString, StringValue: x}
		case float64:
			if x != math.Trunc(x) {
				return nil, fmt.Errorf("pk value %d: %v is not an integer", i, x)
			}
			values[i] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(x)}
		case bool:
			values[i] = ast.Literal{Kind: ast.LiteralBoolean, BoolValue: x}
		default:
			return nil, fmt.Errorf("pk value %d must be a string, an integer or a boolean", i)
		}
	}
	return values, nil
}
