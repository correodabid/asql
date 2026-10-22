package statetree

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
)

// TestGroupProofs: a state of artifacts and parent edges with one group
// leaf per artifact. A proof shows bb's parents complete, and aa and cc
// with none; hiding a parent of a group shown complete fails.
func TestGroupProofs(t *testing.T) {
	_, keys, signer := lineageProof(t)
	stateKey := [32]byte{42}
	artifact := func(id string) ProvenRow {
		return ProvenRow{Domain: "lab", Table: "artifacts", PrimaryKeyColumns: []string{"id"}, PrimaryKey: []ast.Literal{str(id)},
			Row: map[string]ast.Literal{"id": str(id), "kind": str("k-" + id)}}
	}
	edge := func(child, parent string, ordinal int64) ProvenRow {
		return ProvenRow{Domain: "lab", Table: "artifact_parents", PrimaryKeyColumns: []string{"child_id", "parent_id"},
			PrimaryKey: []ast.Literal{str(child), str(parent)},
			Row:        map[string]ast.Literal{"child_id": str(child), "parent_id": str(parent), "ordinal": num(ordinal)}}
	}
	rows := []ProvenRow{artifact("aa"), artifact("bb"), artifact("cc"), artifact("zz"),
		edge("bb", "aa", 1), edge("bb", "cc", 2), edge("zz", "aa", 1)}
	var tree Tree
	entries := map[string]Entry{}
	for i := range rows {
		key, err := RowKey(rows[i].Domain, rows[i].Table, rows[i].PrimaryKey)
		if err != nil {
			t.Fatal(err)
		}
		rows[i].Salt = Salt(stateKey, key, uint64(100+i))
		e, err := rows[i].Entry()
		if err != nil {
			t.Fatal(err)
		}
		entries[keyText(rows[i])] = e
		tree = tree.Set(e.Key, e.Leaf)
	}
	group := func(id string) ProvenGroup {
		return ProvenGroup{Domain: "lab", Table: "artifact_parents", Columns: []string{"child_id"}, RefTable: "artifacts", Values: []ast.Literal{str(id)}}
	}
	// One group leaf per artifact, as the node builds them.
	for _, id := range []string{"aa", "bb", "cc", "zz"} {
		var members []Entry
		for _, r := range rows {
			if r.Table == "artifact_parents" && InGroup(r.Row, []string{"child_id"}, []ast.Literal{str(id)}) {
				members = append(members, entries[keyText(r)])
			}
		}
		key, err := GroupKey("lab", "artifact_parents", []string{"child_id"}, "artifacts", []ast.Literal{str(id)})
		if err != nil {
			t.Fatal(err)
		}
		tree = tree.Set(key, GroupLeaf(members))
	}
	digest, err := ledgerdigest.Sign(signer, ledgerdigest.Digest{Version: ledgerdigest.Version3, LSN: 200, TreeRoot: [32]byte{1}, StateRoot: tree.Root(), NodeID: "node-a", SignedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	prove := func(shown []ProvenRow, groups []ProvenGroup) RowsProof {
		t.Helper()
		p := RowsProof{Digest: digest, Rows: shown, Groups: groups}
		var treeKeys []Hash
		for _, r := range shown {
			treeKeys = append(treeKeys, entries[keyText(r)].Key)
		}
		for _, g := range groups {
			key, _ := GroupKey(g.Domain, g.Table, g.Columns, g.RefTable, g.Values)
			treeKeys = append(treeKeys, key)
		}
		_, proof, err := tree.Prove(treeKeys)
		if err != nil {
			t.Fatal(err)
		}
		p.Proof = proof
		return p
	}

	// bb's lineage: bb, its two parents and their edges; bb's parents
	// complete, and aa and cc have none.
	lineage := prove([]ProvenRow{rows[1], rows[0], rows[2], rows[4], rows[5]}, []ProvenGroup{group("bb"), group("aa"), group("cc")})
	data, _ := json.Marshal(lineage)
	var back RowsProof
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if err := back.Verify(keys); err != nil {
		t.Fatalf("complete lineage: %v\n%s", err, data)
	}

	// Hiding the second parent of bb, whose parents are shown complete.
	hidden := prove([]ProvenRow{rows[1], rows[0], rows[4]}, []ProvenGroup{group("bb")})
	hidden.Rows = []ProvenRow{rows[1], rows[0], rows[4]}
	if err := hidden.Verify(keys); !errors.Is(err, ErrBadProof) {
		t.Fatalf("a hidden parent: %v, want ErrBadProof", err)
	}
	// Presenting zz as a source: its parents shown complete, with none.
	if err := prove([]ProvenRow{rows[3]}, []ProvenGroup{group("zz")}).Verify(keys); !errors.Is(err, ErrBadProof) {
		t.Fatalf("zz presented without its parent: %v, want ErrBadProof", err)
	}
	// An empty group alone proves "no parents".
	if err := prove(nil, []ProvenGroup{group("aa")}).Verify(keys); err != nil {
		t.Fatalf("aa has no parents: %v", err)
	}
	// Edges outside a group shown complete do not need their siblings.
	if err := prove([]ProvenRow{rows[4]}, nil).Verify(keys); err != nil {
		t.Fatalf("one edge without its group: %v", err)
	}

	// The group key covers the table, columns, reference and values.
	base, _ := GroupKey("lab", "artifact_parents", []string{"child_id"}, "artifacts", []ast.Literal{str("bb")})
	for name, other := range map[string]func() (Hash, error){
		"table": func() (Hash, error) {
			return GroupKey("lab", "artifact_children", []string{"child_id"}, "artifacts", []ast.Literal{str("bb")})
		},
		"column": func() (Hash, error) {
			return GroupKey("lab", "artifact_parents", []string{"parent_id"}, "artifacts", []ast.Literal{str("bb")})
		},
		"reference": func() (Hash, error) {
			return GroupKey("lab", "artifact_parents", []string{"child_id"}, "sources", []ast.Literal{str("bb")})
		},
		"value": func() (Hash, error) {
			return GroupKey("lab", "artifact_parents", []string{"child_id"}, "artifacts", []ast.Literal{str("cc")})
		},
	} {
		if k, err := other(); err != nil || k == base {
			t.Fatalf("changing the %s gives the same group key (%v)", name, err)
		}
	}
	if GroupLeaf(nil) == GroupLeaf([]Entry{{}}) {
		t.Fatal("an empty group and a group of one have the same leaf")
	}
}

func keyText(r ProvenRow) string {
	data, _ := json.Marshal(r.PrimaryKey)
	return r.Table + string(data)
}
