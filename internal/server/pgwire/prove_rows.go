package pgwire

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
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
// groups is last: v0.5.0 returned the first three only.
var proveRowsColumns = []string{"digest", "rows", "proof", "groups"}

// proveRowsKey is one row asked for, by its table and primary key values in
// key order, or one group asked for complete, by its table and the
// referenced row's key values (with group_columns when the table has more
// than one group). Values are JSON scalars.
type proveRowsKey struct {
	Domain       string            `json:"domain"`
	Table        string            `json:"table"`
	PK           []json.RawMessage `json:"pk"`
	Group        []json.RawMessage `json:"group"`
	GroupColumns []string          `json:"group_columns"`
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
	viewTables := map[[2]string]executor.VerifiableTable{}
	for _, t := range view.Tables() {
		viewTables[[2]string{t.Domain, t.Name}] = t
	}

	proof := statetree.RowsProof{Digest: digest}
	treeKeys := make([]statetree.Hash, 0, len(keys))
	leaves := map[statetree.Hash]statetree.Hash{}
	addRow := func(domain, table string, columns []string, row executor.VerifiableRow) (bool, error) {
		key, leaf, err := stateTreeEntry(*server.ledger.stateKey, domain, table, row)
		if err != nil {
			return false, err
		}
		if _, dup := leaves[key]; dup {
			return false, nil
		}
		leaves[key] = leaf
		treeKeys = append(treeKeys, key)
		proof.Rows = append(proof.Rows, statetree.ProvenRow{
			Domain: domain, Table: table, PrimaryKeyColumns: columns, PrimaryKey: row.PrimaryKey,
			Row: row.Row, Salt: statetree.Salt(*server.ledger.stateKey, key, row.LSN),
		})
		return true, nil
	}
	groupsAsked := map[statetree.Hash]bool{}
	for i, k := range keys {
		domain, table := strings.ToLower(k.Domain), strings.ToLower(k.Table)
		vt, ok := viewTables[[2]string{domain, table}]
		if !ok {
			return nil, fmt.Errorf("%w: key %d: %s.%s is not a VERIFIABLE table at LSN %d", errProveRows, i, domain, table, digest.LSN)
		}
		if k.Group != nil {
			if k.PK != nil {
				return nil, fmt.Errorf("%w: key %d: give pk or group, not both", errProveRows, i)
			}
			if err := server.proveGroup(view, vt, k, i, digest.LSN, groupsAsked, &proof, &treeKeys, addRow); err != nil {
				return nil, err
			}
			continue
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
		added, err := addRow(domain, table, vt.PrimaryKey, row)
		if err != nil {
			return nil, err
		}
		if !added {
			return nil, fmt.Errorf("%w: key %d: the same row is asked for twice", errProveRows, i)
		}
	}
	_, batch, err := tree.Prove(treeKeys)
	if err != nil {
		return nil, err
	}
	proof.Proof = batch
	// Never hand out a proof that does not verify against the digest.
	if err := proof.CheckRoot(); err != nil {
		return nil, fmt.Errorf("prove_rows: the proof at LSN %d does not verify: %w", digest.LSN, err)
	}

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
		value := fields[column]
		if len(value) == 0 {
			value = json.RawMessage("[]") // no groups asked for
		}
		row[column] = litS(string(value))
	}
	return row, nil
}

// proveGroup adds a group asked for complete, and every row of it, to a
// row proof. Rows already in the proof are not repeated.
func (server *Server) proveGroup(view executor.VerifiableView, vt executor.VerifiableTable, k proveRowsKey, i int, lsn uint64,
	asked map[statetree.Hash]bool, proof *statetree.RowsProof, treeKeys *[]statetree.Hash,
	addRow func(string, string, []string, executor.VerifiableRow) (bool, error)) error {
	var group *executor.VerifiableGroup
	for j := range vt.Groups {
		g := vt.Groups[j]
		if len(k.GroupColumns) == 0 && len(vt.Groups) == 1 || slices.Equal(k.GroupColumns, g.Columns) {
			group = &g
			break
		}
	}
	if group == nil {
		if len(vt.Groups) == 0 {
			return fmt.Errorf("%w: key %d: %s.%s has no verifiable group", errProveRows, i, vt.Domain, vt.Name)
		}
		return fmt.Errorf("%w: key %d: name the group with group_columns: %s.%s has %d groups", errProveRows, i, vt.Domain, vt.Name, len(vt.Groups))
	}
	values, err := proveRowsValues(k.Group)
	if err != nil {
		return fmt.Errorf("%w: key %d: group: %v", errProveRows, i, err)
	}
	// The group exists only for a row of the referenced table.
	ref, found, err := view.Row(vt.Domain, group.RefTable, values)
	if err != nil {
		return fmt.Errorf("%w: key %d: %v", errProveRows, i, err)
	}
	if !found {
		return fmt.Errorf("%w: key %d: %s.%s has no row with that primary key at LSN %d, so it has no group", errProveRows, i, vt.Domain, group.RefTable, lsn)
	}
	key, err := statetree.GroupKey(vt.Domain, vt.Name, group.Columns, group.RefTable, ref.PrimaryKey)
	if err != nil {
		return err
	}
	if asked[key] {
		return fmt.Errorf("%w: key %d: the same group is asked for twice", errProveRows, i)
	}
	asked[key] = true
	err = view.Rows(vt.Domain, vt.Name, func(row executor.VerifiableRow) error {
		if !statetree.InGroup(row.Row, group.Columns, ref.PrimaryKey) {
			return nil
		}
		_, err := addRow(vt.Domain, vt.Name, vt.PrimaryKey, row)
		return err
	})
	if err != nil {
		return err
	}
	proof.Groups = append(proof.Groups, statetree.ProvenGroup{
		Domain: vt.Domain, Table: vt.Name, Columns: group.Columns, RefTable: group.RefTable, Values: ref.PrimaryKey,
	})
	*treeKeys = append(*treeKeys, key)
	return nil
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
