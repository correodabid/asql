package statetree

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

// referenceRoot is the root as the package comment defines it, written
// from the rules and not from the tree: empty is zero, a lone key is
// H(0x02 || key || leaf), anything else H(0x01 || left || right) splitting
// on the key bit at each level.
func referenceRoot(entries map[Hash]Hash) Hash {
	keys := make([]Hash, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	var root func(keys []Hash, depth int) Hash
	root = func(keys []Hash, depth int) Hash {
		switch len(keys) {
		case 0:
			return Hash{}
		case 1:
			leaf := entries[keys[0]]
			return sha256.Sum256(append(append([]byte{0x02}, keys[0][:]...), leaf[:]...))
		}
		var left, right []Hash
		for _, k := range keys {
			if k[depth/8]&(0x80>>(depth%8)) == 0 {
				left = append(left, k)
			} else {
				right = append(right, k)
			}
		}
		l, r := root(left, depth+1), root(right, depth+1)
		return sha256.Sum256(append(append([]byte{0x01}, l[:]...), r[:]...))
	}
	return root(keys, 0)
}

func hashOf(s string) Hash { return sha256.Sum256([]byte(s)) }

// nearKeys returns keys sharing a long prefix, so branches with an empty
// side appear.
func nearKey(base Hash, flipBit int) Hash {
	k := base
	k[flipBit/8] ^= 0x80 >> (flipBit % 8)
	return k
}

func TestTreeMatchesTheHashingRules(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	model := map[Hash]Hash{}
	var tree Tree
	base := hashOf("base")
	pool := []Hash{base, nearKey(base, 255), nearKey(base, 200), nearKey(base, 9)}
	for i := range 60 {
		pool = append(pool, hashOf(fmt.Sprint("key ", i)))
	}
	for step := range 2000 {
		key := pool[rng.IntN(len(pool))]
		if rng.IntN(3) == 0 {
			tree = tree.Delete(key)
			delete(model, key)
		} else {
			leaf := hashOf(fmt.Sprint("leaf ", step))
			tree = tree.Set(key, leaf)
			model[key] = leaf
		}
		if tree.Root() != referenceRoot(model) || tree.Len() != len(model) {
			t.Fatalf("step %d: root or size differs from the rules (%d keys, tree says %d)", step, len(model), tree.Len())
		}
	}

	entries := make([]Entry, 0, len(model))
	for k, v := range model {
		entries = append(entries, Entry{Key: k, Leaf: v})
		if got, ok := tree.Get(k); !ok || got != v {
			t.Fatalf("Get(%x) = %x, %v", k, got, ok)
		}
	}
	built, err := Build(entries)
	if err != nil {
		t.Fatal(err)
	}
	if built.Root() != tree.Root() || built.Len() != tree.Len() {
		t.Fatal("Build and incremental updates give different trees")
	}
	if _, err := Build(append(entries, entries[0])); err == nil {
		t.Fatal("Build accepted a duplicate key")
	}
	if (Tree{}).Root() != (Hash{}) {
		t.Fatal("the empty tree's root is not zero")
	}
}

func TestOldTreesStayValid(t *testing.T) {
	var tree Tree
	for i := range 50 {
		tree = tree.Set(hashOf(fmt.Sprint(i)), hashOf("v1"))
	}
	before, root := tree, tree.Root()
	for i := range 25 {
		tree = tree.Set(hashOf(fmt.Sprint(i)), hashOf("v2")).Delete(hashOf(fmt.Sprint(i + 25)))
	}
	if before.Root() != root || before.Len() != 50 {
		t.Fatal("updating a tree changed an earlier one")
	}
	if _, _, err := before.Prove([]Hash{hashOf("30")}); err != nil {
		t.Fatalf("the earlier tree can no longer prove its keys: %v", err)
	}
}

func TestBatchProofs(t *testing.T) {
	var tree Tree
	base := hashOf("base")
	keys := []Hash{base, nearKey(base, 255), nearKey(base, 3)}
	for i := range 200 {
		keys = append(keys, hashOf(fmt.Sprint("row ", i)))
	}
	for i, k := range keys {
		tree = tree.Set(k, hashOf(fmt.Sprint("leaf ", i)))
	}
	root := tree.Root()

	for _, batch := range [][]Hash{
		{keys[0]},
		{keys[0], keys[1]}, // siblings at the last level
		{keys[2], keys[0], keys[10], keys[100]},
		keys, // every key: no sibling but empty ones
	} {
		entries, proof, err := tree.Prove(batch)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != len(batch) {
			t.Fatalf("%d entries for %d keys", len(entries), len(batch))
		}
		if err := Verify(root, entries, proof); err != nil {
			t.Fatalf("a batch of %d: %v", len(batch), err)
		}
		// Shared paths: the batch proof is smaller than one proof per key.
		if len(batch) > 1 {
			single := 0
			for _, k := range batch {
				_, p, _ := tree.Prove([]Hash{k})
				single += len(p.Siblings)
			}
			if len(proof.Siblings) >= single {
				t.Fatalf("a batch of %d has %d siblings, one proof per key %d", len(batch), len(proof.Siblings), single)
			}
		}

		forged := append([]Entry(nil), entries...)
		forged[0].Leaf[0] ^= 1
		if err := Verify(root, forged, proof); !errors.Is(err, ErrBadProof) {
			t.Fatalf("a changed leaf: %v", err)
		}
		if len(proof.Siblings) > 0 {
			edited := Proof{Depths: proof.Depths, Siblings: append([]Hash(nil), proof.Siblings...)}
			edited.Siblings[0][5] ^= 1
			if err := Verify(root, entries, edited); !errors.Is(err, ErrBadProof) {
				t.Fatalf("an edited sibling: %v", err)
			}
			short := Proof{Depths: proof.Depths, Siblings: proof.Siblings[:len(proof.Siblings)-1]}
			if err := Verify(root, entries, short); !errors.Is(err, ErrBadProof) {
				t.Fatalf("a missing sibling: %v", err)
			}
			long := Proof{Depths: proof.Depths, Siblings: append(append([]Hash(nil), proof.Siblings...), Hash{})}
			if err := Verify(root, entries, long); !errors.Is(err, ErrBadProof) {
				t.Fatalf("an extra sibling: %v", err)
			}
		}
		if len(entries) > 1 {
			// Dropping a row from the batch does not leave a valid proof.
			if err := Verify(root, entries[1:], proof); !errors.Is(err, ErrBadProof) {
				t.Fatalf("a row dropped from the batch: %v", err)
			}
		}
	}

	if _, _, err := tree.Prove([]Hash{hashOf("absent")}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a key with no row: %v", err)
	}
	if _, _, err := tree.Prove([]Hash{keys[0], keys[0]}); err == nil {
		t.Fatal("a key asked for twice was accepted")
	}
	single, proof, _ := tree.Prove([]Hash{keys[5]})
	if err := Verify(tree.Set(keys[6], hashOf("changed")).Root(), single, proof); !errors.Is(err, ErrBadProof) {
		t.Fatalf("a proof against another root: %v", err)
	}
}

func TestEncodings(t *testing.T) {
	pk := []ast.Literal{{Kind: ast.LiteralString, StringValue: "dd"}, {Kind: ast.LiteralString, StringValue: "cc"}}
	key, err := RowKey("lab", "artifact_parents", pk)
	if err != nil {
		t.Fatal(err)
	}
	// Each part of the key changes it, and the order of a composite key
	// matters.
	for name, other := range map[string]func() (Hash, error){
		"domain": func() (Hash, error) { return RowKey("lab2", "artifact_parents", pk) },
		"table":  func() (Hash, error) { return RowKey("lab", "artifacts", pk) },
		"order":  func() (Hash, error) { return RowKey("lab", "artifact_parents", []ast.Literal{pk[1], pk[0]}) },
		"type": func() (Hash, error) {
			return RowKey("lab", "artifact_parents", []ast.Literal{{Kind: ast.LiteralNumber, NumberValue: 1}, pk[1]})
		},
		"boundary": func() (Hash, error) { return RowKey("la", "bartifact_parents", pk) },
	} {
		if k, err := other(); err != nil || k == key {
			t.Fatalf("changing the %s gives the same key (%v)", name, err)
		}
	}

	row := map[string]ast.Literal{
		"child_id":  {Kind: ast.LiteralString, StringValue: "dd"},
		"parent_id": {Kind: ast.LiteralString, StringValue: "cc"},
		"ordinal":   {Kind: ast.LiteralNumber, NumberValue: 2},
	}
	salt := Salt([32]byte{7}, key, 42)
	leaf, err := RowLeaf(salt, row)
	if err != nil {
		t.Fatal(err)
	}
	// The leaf covers values outside the key, such as ordinal, the salt,
	// and the column names.
	moved := map[string]ast.Literal{"child_id": row["child_id"], "parent_id": row["parent_id"], "ordinal": {Kind: ast.LiteralNumber, NumberValue: 3}}
	renamed := map[string]ast.Literal{"child_id": row["child_id"], "parent_id": row["parent_id"], "position": row["ordinal"]}
	for name, l := range map[string]func() (Hash, error){
		"ordinal":     func() (Hash, error) { return RowLeaf(salt, moved) },
		"column name": func() (Hash, error) { return RowLeaf(salt, renamed) },
		"salt":        func() (Hash, error) { return RowLeaf(Salt([32]byte{7}, key, 43), row) },
		"state key":   func() (Hash, error) { return RowLeaf(Salt([32]byte{8}, key, 42), row) },
	} {
		if h, err := l(); err != nil || h == leaf {
			t.Fatalf("changing the %s gives the same leaf (%v)", name, err)
		}
	}

	// The same bytes written out by hand, from the package comment.
	str := func(s string) []byte { return append([]byte{0, 0, 0, byte(len(s))}, s...) }
	text := func(s string) []byte { return append([]byte{3}, str(s)...) } // ADR 0006 text
	rawKey := []byte{0x01}
	rawKey = append(rawKey, str("lab")...)
	rawKey = append(rawKey, str("artifact_parents")...)
	rawKey = append(rawKey, 0, 0, 0, 2)
	rawKey = append(append(rawKey, text("dd")...), text("cc")...)
	if sha256.Sum256(rawKey) != key {
		t.Fatal("RowKey differs from the documented bytes")
	}
	mac := hmac.New(sha256.New, append([]byte{7}, make([]byte, 31)...))
	mac.Write(append(key[:], 0, 0, 0, 0, 0, 0, 0, 42))
	if !bytes.Equal(mac.Sum(nil), salt[:]) {
		t.Fatal("Salt differs from the documented HMAC")
	}
	rawLeaf := append([]byte{0x00, 0x01}, salt[:]...)
	rawLeaf = append(rawLeaf, 0, 0, 0, 3)
	rawLeaf = append(append(rawLeaf, str("child_id")...), text("dd")...)
	rawLeaf = append(append(rawLeaf, str("ordinal")...), 1, 0, 0, 0, 0, 0, 0, 0, 2) // ADR 0006 int
	rawLeaf = append(append(rawLeaf, str("parent_id")...), text("cc")...)
	if sha256.Sum256(rawLeaf) != leaf {
		t.Fatal("RowLeaf differs from the documented bytes")
	}

	// Pinned vectors: a change here changes every root a node has signed.
	for name, got := range map[string]Hash{"key": key, "salt": salt, "leaf": leaf} {
		want := map[string]string{
			"key":  vectorKey,
			"salt": vectorSalt,
			"leaf": vectorLeaf,
		}[name]
		if hex.EncodeToString(got[:]) != want {
			t.Errorf("%s vector = %x, want %s", name, got, want)
		}
	}
}

const (
	vectorKey  = "c3361431cfc4e7d36cba17b1293ae06fdc9e2c4232bda7ebd49d5f15e4a9df21"
	vectorSalt = "1fcd117cf4402c12367355305de34d2f43d8913b2faf68d9f3c4e9e2dfbf3b34"
	vectorLeaf = "ebe1c92b8276eafacf291512fe0606bd4f26b20d717937dc2f15c1023e676067"
)

// Proof verification does not depend on the order entries are given in.
func TestVerifyTakesEntriesInAnyOrder(t *testing.T) {
	var tree Tree
	var keys []Hash
	for i := range 20 {
		k := hashOf(fmt.Sprint(i))
		keys = append(keys, k)
		tree = tree.Set(k, hashOf(fmt.Sprint("v", i)))
	}
	entries, proof, err := tree.Prove(keys[:7])
	if err != nil {
		t.Fatal(err)
	}
	reversed := append([]Entry(nil), entries...)
	sort.Slice(reversed, func(i, j int) bool { return bytes.Compare(reversed[i].Key[:], reversed[j].Key[:]) > 0 })
	if err := Verify(tree.Root(), reversed, proof); err != nil {
		t.Fatal(err)
	}
}
