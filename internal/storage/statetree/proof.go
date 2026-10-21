package statetree

import (
	"bytes"
	"fmt"
	"sort"
)

// Proof is a batch proof that a set of keys have given leaves under a root.
// Walking down from the root over the keys' paths, in key order, Depths
// holds the level at which each key's subtree holds it alone, and Siblings
// the hash of each subtree beside the paths that holds none of the keys,
// 32 zero bytes when it is empty. Paths share their upper nodes, so each
// sibling appears once.
type Proof struct {
	Depths   []uint16
	Siblings []Hash
}

// Prove returns the leaves of keys, sorted by key, and a proof of them.
// Every key must be in the tree.
func (t Tree) Prove(keys []Hash) ([]Entry, Proof, error) {
	sorted := sortedKeys(keys)
	for i := 1; i < len(sorted); i++ {
		if sorted[i] == sorted[i-1] {
			return nil, Proof{}, fmt.Errorf("state tree: key %x asked for twice", sorted[i])
		}
	}
	var p proofBuilder
	if err := p.prove(t.root, 0, sorted); err != nil {
		return nil, Proof{}, err
	}
	return p.entries, p.proof, nil
}

type proofBuilder struct {
	entries []Entry
	proof   Proof
}

func (p *proofBuilder) prove(n *node, depth int, keys []Hash) error {
	if n == nil {
		return fmt.Errorf("%w: %x", ErrNotFound, keys[0])
	}
	if n.leaf {
		if len(keys) != 1 || n.key != keys[0] {
			for _, key := range keys {
				if key != n.key {
					return fmt.Errorf("%w: %x", ErrNotFound, key)
				}
			}
		}
		p.entries = append(p.entries, Entry{Key: n.key, Leaf: n.item})
		p.proof.Depths = append(p.proof.Depths, uint16(depth))
		return nil
	}
	sides := splitKeys(keys, depth)
	for side, keys := range sides {
		if len(keys) == 0 {
			p.proof.Siblings = append(p.proof.Siblings, n.child[side].hashOrEmpty())
			continue
		}
		if err := p.prove(n.child[side], depth+1, keys); err != nil {
			return err
		}
	}
	return nil
}

// Verify checks that proof shows entries under root. It needs nothing but
// its arguments.
func Verify(root Hash, entries []Entry, proof Proof) error {
	if len(entries) == 0 {
		return fmt.Errorf("%w: no entries", ErrBadProof)
	}
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].Key[:], sorted[j].Key[:]) < 0 })
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Key == sorted[i-1].Key {
			return fmt.Errorf("%w: key %x appears twice", ErrBadProof, sorted[i].Key)
		}
	}
	if len(proof.Depths) != len(sorted) {
		return fmt.Errorf("%w: %d depths for %d entries", ErrBadProof, len(proof.Depths), len(sorted))
	}
	v := verifier{proof: proof}
	got, err := v.hash(sorted, 0)
	if err != nil {
		return err
	}
	if v.sibling != len(proof.Siblings) {
		return fmt.Errorf("%w: %d siblings left over", ErrBadProof, len(proof.Siblings)-v.sibling)
	}
	if got != root {
		return fmt.Errorf("%w: it gives root %x, want %x", ErrBadProof, got, root)
	}
	return nil
}

type verifier struct {
	proof          Proof
	entry, sibling int
}

func (v *verifier) hash(entries []Entry, depth int) (Hash, error) {
	if len(entries) == 1 {
		switch at := int(v.proof.Depths[v.entry]); {
		case at == depth:
			v.entry++
			return aloneHash(entries[0].Key, entries[0].Leaf), nil
		case at < depth:
			return Hash{}, fmt.Errorf("%w: key %x is alone at level %d, above %d", ErrBadProof, entries[0].Key, at, depth)
		}
	}
	if depth >= Depth {
		return Hash{}, fmt.Errorf("%w: path longer than %d levels", ErrBadProof, Depth)
	}
	keys := make([]Hash, len(entries))
	for i, e := range entries {
		keys[i] = e.Key
	}
	sides := splitKeys(keys, depth)
	var hashes [2]Hash
	offset := 0
	for side, sideKeys := range sides {
		if len(sideKeys) == 0 {
			if v.sibling >= len(v.proof.Siblings) {
				return Hash{}, fmt.Errorf("%w: too few siblings", ErrBadProof)
			}
			hashes[side] = v.proof.Siblings[v.sibling]
			v.sibling++
			continue
		}
		h, err := v.hash(entries[offset:offset+len(sideKeys)], depth+1)
		if err != nil {
			return Hash{}, err
		}
		hashes[side] = h
		offset += len(sideKeys)
	}
	return nodeHash(hashes[0], hashes[1]), nil
}

func sortedKeys(keys []Hash) []Hash {
	sorted := append([]Hash(nil), keys...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i][:], sorted[j][:]) < 0 })
	return sorted
}

// splitKeys splits sorted keys by their bit at depth.
func splitKeys(sorted []Hash, depth int) [2][]Hash {
	mid := sort.Search(len(sorted), func(i int) bool { return bit(sorted[i], depth) == 1 })
	return [2][]Hash{sorted[:mid], sorted[mid:]}
}
