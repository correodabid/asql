package statetree

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

// Depth is the number of key bits.
const Depth = 256

var (
	// ErrNotFound reports a key with no row in the tree.
	ErrNotFound = errors.New("state tree: no row with that key")
	// ErrBadProof reports a proof that does not verify.
	ErrBadProof = errors.New("state tree: proof does not verify")
)

// Entry is a key and its leaf.
type Entry struct {
	Key  Hash
	Leaf Hash
}

// node is immutable once built, so a Tree is a value: an update returns a
// new tree that shares every untouched node with the old one, and the trees
// of earlier checkpoints stay valid.
type node struct {
	hash Hash
	// A leaf holds one key; a branch splits on the bit at its depth and
	// always has at least two keys below it (the tree is compact).
	leaf      bool
	key, item Hash
	child     [2]*node
}

func newLeaf(key, leaf Hash) *node {
	return &node{hash: aloneHash(key, leaf), leaf: true, key: key, item: leaf}
}

func newBranch(left, right *node) *node {
	return &node{hash: nodeHash(left.hashOrEmpty(), right.hashOrEmpty()), child: [2]*node{left, right}}
}

func (n *node) hashOrEmpty() Hash {
	if n == nil {
		return Hash{}
	}
	return n.hash
}

// Tree is a state tree. The zero value is empty.
type Tree struct {
	root *node
	size int
}

// Root returns the tree's root: 32 zero bytes when it is empty.
func (t Tree) Root() Hash { return t.root.hashOrEmpty() }

// Len returns the number of keys.
func (t Tree) Len() int { return t.size }

// Get returns key's leaf.
func (t Tree) Get(key Hash) (Hash, bool) {
	n := t.root
	for depth := 0; n != nil; depth++ {
		if n.leaf {
			if n.key == key {
				return n.item, true
			}
			return Hash{}, false
		}
		n = n.child[bit(key, depth)]
	}
	return Hash{}, false
}

// Set returns the tree with key's leaf set.
func (t Tree) Set(key, leaf Hash) Tree {
	root, added := set(t.root, 0, key, leaf)
	if added {
		t.size++
	}
	t.root = root
	return t
}

func set(n *node, depth int, key, leaf Hash) (*node, bool) {
	switch {
	case n == nil:
		return newLeaf(key, leaf), true
	case n.leaf && n.key == key:
		return newLeaf(key, leaf), false
	case n.leaf:
		return split(n, newLeaf(key, leaf), depth), true
	}
	b := bit(key, depth)
	child, added := set(n.child[b], depth+1, key, leaf)
	children := n.child
	children[b] = child
	return newBranch(children[0], children[1]), added
}

// split joins two leaves with different keys below depth, with a branch
// at each level their keys share.
func split(a, b *node, depth int) *node {
	ba, bb := bit(a.key, depth), bit(b.key, depth)
	if ba != bb {
		if ba == 0 {
			return newBranch(a, b)
		}
		return newBranch(b, a)
	}
	inner := split(a, b, depth+1)
	if ba == 0 {
		return newBranch(inner, nil)
	}
	return newBranch(nil, inner)
}

// Delete returns the tree without key.
func (t Tree) Delete(key Hash) Tree {
	root, removed := remove(t.root, 0, key)
	if removed {
		t.size--
	}
	t.root = root
	return t
}

func remove(n *node, depth int, key Hash) (*node, bool) {
	if n == nil {
		return nil, false
	}
	if n.leaf {
		if n.key == key {
			return nil, true
		}
		return n, false
	}
	b := bit(key, depth)
	child, removed := remove(n.child[b], depth+1, key)
	if !removed {
		return n, false
	}
	children := n.child
	children[b] = child
	// Keep the tree compact: a branch left with a single leaf below it
	// becomes that leaf.
	switch {
	case children[0] == nil && children[1] == nil:
		return nil, true
	case children[0] == nil && children[1].leaf:
		return children[1], true
	case children[1] == nil && children[0].leaf:
		return children[0], true
	}
	return newBranch(children[0], children[1]), true
}

// Build returns the tree of entries, in O(n log n). Keys must be distinct.
func Build(entries []Entry) (Tree, error) {
	sorted := append([]Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].Key[:], sorted[j].Key[:]) < 0 })
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Key == sorted[i-1].Key {
			return Tree{}, fmt.Errorf("state tree: duplicate key %x", sorted[i].Key)
		}
	}
	return Tree{root: build(sorted, 0), size: len(sorted)}, nil
}

func build(sorted []Entry, depth int) *node {
	switch len(sorted) {
	case 0:
		return nil
	case 1:
		return newLeaf(sorted[0].Key, sorted[0].Leaf)
	}
	// Sorted keys have their bit at depth 0 first, then 1.
	mid := sort.Search(len(sorted), func(i int) bool { return bit(sorted[i].Key, depth) == 1 })
	return newBranch(build(sorted[:mid], depth+1), build(sorted[mid:], depth+1))
}
