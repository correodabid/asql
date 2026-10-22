package statetree

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
)

func str(s string) ast.Literal { return ast.Literal{Kind: ast.LiteralString, StringValue: s} }
func num(n int64) ast.Literal  { return ast.Literal{Kind: ast.LiteralNumber, NumberValue: n} }

// lineageProof builds a state of artifacts and their parent edges, signs a
// version 3 digest of it and proves a lineage: two artifacts and the edge
// between them, across both tables.
func lineageProof(t *testing.T) (RowsProof, ledgerdigest.Keyring, *ledgerdigest.Ed25519Signer) {
	t.Helper()
	dir := t.TempDir()
	if _, err := ledgerdigest.GenerateKeyFiles(filepath.Join(dir, "node.key"), filepath.Join(dir, "node.pub")); err != nil {
		t.Fatal(err)
	}
	signer, err := ledgerdigest.LoadSigner(filepath.Join(dir, "node.key"))
	if err != nil {
		t.Fatal(err)
	}
	keys := ledgerdigest.Keyring{}
	keys.Add(signer.PublicKey())
	stateKey := [32]byte{42}

	rows := []ProvenRow{
		{Domain: "lab", Table: "artifacts", PrimaryKeyColumns: []string{"id"}, PrimaryKey: []ast.Literal{str("aa")}, Row: map[string]ast.Literal{"id": str("aa"), "kind": str("source")}},
		{Domain: "lab", Table: "artifacts", PrimaryKeyColumns: []string{"id"}, PrimaryKey: []ast.Literal{str("bb")}, Row: map[string]ast.Literal{"id": str("bb"), "kind": str("extraction")}},
		{Domain: "lab", Table: "artifacts", PrimaryKeyColumns: []string{"id"}, PrimaryKey: []ast.Literal{str("zz")}, Row: map[string]ast.Literal{"id": str("zz"), "kind": str("unpublished")}},
		{Domain: "lab", Table: "artifact_parents", PrimaryKeyColumns: []string{"child_id", "parent_id"}, PrimaryKey: []ast.Literal{str("bb"), str("aa")},
			Row: map[string]ast.Literal{"child_id": str("bb"), "parent_id": str("aa"), "ordinal": num(1)}},
	}
	var tree Tree
	for i := range rows {
		key, err := RowKey(rows[i].Domain, rows[i].Table, rows[i].PrimaryKey)
		if err != nil {
			t.Fatal(err)
		}
		rows[i].Salt = Salt(stateKey, key, uint64(10+i))
		leaf, err := RowLeaf(rows[i].Salt, rows[i].Row)
		if err != nil {
			t.Fatal(err)
		}
		tree = tree.Set(key, leaf)
	}
	digest, err := ledgerdigest.Sign(signer, ledgerdigest.Digest{Version: ledgerdigest.Version3, LSN: 20, TreeRoot: [32]byte{1}, StateRoot: tree.Root(), NodeID: "node-a", SignedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	// The lineage: aa, bb and the edge bb <- aa; zz is not shown.
	shown := []ProvenRow{rows[0], rows[1], rows[3]}
	var treeKeys []Hash
	for _, r := range shown {
		e, err := r.Entry()
		if err != nil {
			t.Fatal(err)
		}
		treeKeys = append(treeKeys, e.Key)
	}
	_, proof, err := tree.Prove(treeKeys)
	if err != nil {
		t.Fatal(err)
	}
	return RowsProof{Digest: digest, Rows: shown, Proof: proof}, keys, signer
}

func TestRowsProof(t *testing.T) {
	proof, keys, signer := lineageProof(t)
	data, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	var back RowsProof
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if err := back.Verify(keys); err != nil {
		t.Fatalf("after a JSON round trip: %v\n%s", err, data)
	}

	edit := func(name string, mutate func(*RowsProof), want error) {
		t.Helper()
		var p RowsProof
		_ = json.Unmarshal(data, &p)
		mutate(&p)
		if err := p.Verify(keys); err == nil || (want != nil && !errors.Is(err, want)) {
			t.Fatalf("%s: %v, want %v", name, err, want)
		}
	}
	edit("a changed ordinal", func(p *RowsProof) { p.Rows[2].Row["ordinal"] = num(2) }, ErrBadProof)
	edit("a changed value type", func(p *RowsProof) { p.Rows[2].Row["ordinal"] = str("1") }, ErrBadProof)
	edit("an added column", func(p *RowsProof) { p.Rows[0].Row["note"] = str("x") }, ErrBadProof)
	edit("another salt", func(p *RowsProof) { p.Rows[0].Salt[0] ^= 1 }, ErrBadProof)
	edit("a row dropped from the lineage", func(p *RowsProof) { p.Rows = p.Rows[:2] }, ErrBadProof)
	edit("a row cited by another key", func(p *RowsProof) { p.Rows[1].PrimaryKey = []ast.Literal{str("cc")} }, nil)
	edit("the edge's key order swapped", func(p *RowsProof) {
		p.Rows[2].PrimaryKeyColumns = []string{"parent_id", "child_id"}
		p.Rows[2].PrimaryKey = []ast.Literal{str("aa"), str("bb")}
	}, ErrBadProof)
	edit("an edited state root", func(p *RowsProof) { p.Digest.StateRoot[0] ^= 1 }, ledgerdigest.ErrBadSignature)
	edit("no rows", func(p *RowsProof) { p.Rows = nil }, nil)

	v2, err := ledgerdigest.Sign(signer, ledgerdigest.Digest{Version: ledgerdigest.Version2, LSN: 20, NodeID: "node-a", SignedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	edit("a version 2 digest", func(p *RowsProof) { p.Digest = v2 }, ErrNoState)
	if err := back.Verify(ledgerdigest.Keyring{}); !errors.Is(err, ledgerdigest.ErrUnknownKey) {
		t.Fatalf("without the node's key: %v", err)
	}
}
