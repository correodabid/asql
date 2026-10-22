package statetree

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
)

// ErrNoState reports a digest that signs no state root.
var ErrNoState = errors.New("row proof: the digest signs no state root (version 3 needed)")

// ProvenRow is a row a RowsProof shows, with what its leaf is built from.
type ProvenRow struct {
	Domain string
	Table  string
	// PrimaryKeyColumns and PrimaryKey are the row's primary key, in key
	// order: how a report cites the row.
	PrimaryKeyColumns []string
	PrimaryKey        []ast.Literal
	// Row is every column of the row.
	Row  map[string]ast.Literal
	Salt Hash
}

// Entry returns the row's key and leaf.
func (r ProvenRow) Entry() (Entry, error) {
	if len(r.PrimaryKeyColumns) != len(r.PrimaryKey) || len(r.PrimaryKey) == 0 {
		return Entry{}, fmt.Errorf("row proof: %s.%s: %d key columns for %d key values", r.Domain, r.Table, len(r.PrimaryKeyColumns), len(r.PrimaryKey))
	}
	for i, column := range r.PrimaryKeyColumns {
		value, ok := r.Row[column]
		if !ok || value != r.PrimaryKey[i] {
			return Entry{}, fmt.Errorf("row proof: %s.%s: the row's %s is not its key value", r.Domain, r.Table, column)
		}
	}
	key, err := RowKey(r.Domain, r.Table, r.PrimaryKey)
	if err != nil {
		return Entry{}, err
	}
	leaf, err := RowLeaf(r.Salt, r.Row)
	if err != nil {
		return Entry{}, err
	}
	return Entry{Key: key, Leaf: leaf}, nil
}

// ProvenGroup is a group a RowsProof shows complete: every row of Table
// whose Columns equal Values is among the proof's rows, and no other one
// exists. For artifact_parents GROUP (child_id) REFERENCES artifacts (id)
// and Values ["X"], the proof's rows are all of X's parent edges, none if
// X has no parents.
type ProvenGroup struct {
	Domain   string
	Table    string
	Columns  []string
	RefTable string
	Values   []ast.Literal
}

// Entry returns the group's key and the leaf its members among rows give.
func (g ProvenGroup) Entry(rows []ProvenRow, entries []Entry) (Entry, error) {
	key, err := GroupKey(g.Domain, g.Table, g.Columns, g.RefTable, g.Values)
	if err != nil {
		return Entry{}, err
	}
	var members []Entry
	for i, row := range rows {
		if row.Domain == g.Domain && row.Table == g.Table && InGroup(row.Row, g.Columns, g.Values) {
			members = append(members, entries[i])
		}
	}
	return Entry{Key: key, Leaf: GroupLeaf(members)}, nil
}

// RowsProof shows that a node's state at a signed digest's LSN held these
// rows (ADR 0008), and that each of Groups has no rows besides the ones
// shown: the digest, the rows with their salts, and one batch proof under
// the digest's state root. It reveals nothing about other rows.
type RowsProof struct {
	Digest ledgerdigest.Digest
	Rows   []ProvenRow
	Groups []ProvenGroup
	Proof  Proof
}

// Verify checks the digest's signature against keys and the rows against
// its state root.
func (p RowsProof) Verify(keys ledgerdigest.Keyring) error {
	if err := p.Digest.Verify(keys); err != nil {
		return err
	}
	return p.CheckRoot()
}

// CheckRoot checks the rows and groups against the digest's state root,
// without checking the digest's signature: Verify does both.
func (p RowsProof) CheckRoot() error {
	if !p.Digest.HasState() {
		return ErrNoState
	}
	if len(p.Rows) == 0 && len(p.Groups) == 0 {
		return errors.New("row proof: no rows")
	}
	entries := make([]Entry, len(p.Rows))
	for i, row := range p.Rows {
		entry, err := row.Entry()
		if err != nil {
			return err
		}
		entries[i] = entry
	}
	all := entries
	for _, g := range p.Groups {
		entry, err := g.Entry(p.Rows, entries)
		if err != nil {
			return err
		}
		all = append(all, entry)
	}
	return Verify(p.Digest.StateRoot, all, p.Proof)
}

type provenRowJSON struct {
	Domain            string                     `json:"domain"`
	Table             string                     `json:"table"`
	PrimaryKeyColumns []string                   `json:"primary_key_columns"`
	PrimaryKey        []json.RawMessage          `json:"primary_key"`
	Row               map[string]json.RawMessage `json:"row"`
	Salt              string                     `json:"salt"`
}

type proofJSON struct {
	Depths   []uint16 `json:"depths"`
	Siblings []string `json:"siblings"`
}

type provenGroupJSON struct {
	Domain   string            `json:"domain"`
	Table    string            `json:"table"`
	Columns  []string          `json:"columns"`
	RefTable string            `json:"references"`
	Values   []json.RawMessage `json:"values"`
}

type rowsProofJSON struct {
	Digest ledgerdigest.Digest `json:"digest"`
	Rows   []provenRowJSON     `json:"rows"`
	Groups []provenGroupJSON   `json:"groups,omitempty"`
	Proof  proofJSON           `json:"proof"`
}

// MarshalJSON writes values in ADR 0006's typed JSON form and hashes in hex.
func (p RowsProof) MarshalJSON() ([]byte, error) {
	out := rowsProofJSON{Digest: p.Digest, Rows: make([]provenRowJSON, len(p.Rows))}
	for i, row := range p.Rows {
		j := provenRowJSON{Domain: row.Domain, Table: row.Table, PrimaryKeyColumns: row.PrimaryKeyColumns,
			Row: make(map[string]json.RawMessage, len(row.Row)), Salt: hex.EncodeToString(row.Salt[:])}
		for _, value := range row.PrimaryKey {
			raw, err := marshalValue(value)
			if err != nil {
				return nil, err
			}
			j.PrimaryKey = append(j.PrimaryKey, raw)
		}
		for column, value := range row.Row {
			raw, err := marshalValue(value)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", column, err)
			}
			j.Row[column] = raw
		}
		out.Rows[i] = j
	}
	for _, g := range p.Groups {
		j := provenGroupJSON{Domain: g.Domain, Table: g.Table, Columns: g.Columns, RefTable: g.RefTable}
		for _, value := range g.Values {
			raw, err := marshalValue(value)
			if err != nil {
				return nil, err
			}
			j.Values = append(j.Values, raw)
		}
		out.Groups = append(out.Groups, j)
	}
	out.Proof.Depths = p.Proof.Depths
	out.Proof.Siblings = make([]string, len(p.Proof.Siblings))
	for i, h := range p.Proof.Siblings {
		out.Proof.Siblings[i] = hex.EncodeToString(h[:])
	}
	return json.Marshal(out)
}

// UnmarshalJSON is the inverse of MarshalJSON.
func (p *RowsProof) UnmarshalJSON(data []byte) error {
	var in rowsProofJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	out := RowsProof{Digest: in.Digest, Rows: make([]ProvenRow, len(in.Rows)), Proof: Proof{Depths: in.Proof.Depths}}
	for i, j := range in.Rows {
		row := ProvenRow{Domain: j.Domain, Table: j.Table, PrimaryKeyColumns: j.PrimaryKeyColumns, Row: make(map[string]ast.Literal, len(j.Row))}
		for _, raw := range j.PrimaryKey {
			value, err := ledgerdigest.UnmarshalValue(raw)
			if err != nil {
				return fmt.Errorf("row proof: row %d primary key: %w", i, err)
			}
			row.PrimaryKey = append(row.PrimaryKey, value)
		}
		for column, raw := range j.Row {
			value, err := ledgerdigest.UnmarshalValue(raw)
			if err != nil {
				return fmt.Errorf("row proof: row %d column %s: %w", i, column, err)
			}
			row.Row[column] = value
		}
		salt, err := decodeHash(j.Salt)
		if err != nil {
			return fmt.Errorf("row proof: row %d salt: %w", i, err)
		}
		row.Salt = salt
		out.Rows[i] = row
	}
	for i, j := range in.Groups {
		g := ProvenGroup{Domain: j.Domain, Table: j.Table, Columns: j.Columns, RefTable: j.RefTable}
		for _, raw := range j.Values {
			value, err := ledgerdigest.UnmarshalValue(raw)
			if err != nil {
				return fmt.Errorf("row proof: group %d value: %w", i, err)
			}
			g.Values = append(g.Values, value)
		}
		out.Groups = append(out.Groups, g)
	}
	for _, s := range in.Proof.Siblings {
		h, err := decodeHash(s)
		if err != nil {
			return fmt.Errorf("row proof: sibling: %w", err)
		}
		out.Proof.Siblings = append(out.Proof.Siblings, h)
	}
	*p = out
	return nil
}

func marshalValue(value ast.Literal) (json.RawMessage, error) {
	v, err := ledgerdigest.MarshalValue(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func decodeHash(s string) (Hash, error) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 {
		return Hash{}, errors.New("must be 32 bytes of hex")
	}
	var h Hash
	copy(h[:], b)
	return h, nil
}
