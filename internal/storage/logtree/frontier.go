package logtree

import "golang.org/x/mod/sumdb/tlog"

// Frontier computes the root of a growing tree in O(log n) memory, for
// verifiers that stream the WAL instead of keeping a tree.
type Frontier struct {
	size  int64
	nodes []Hash // roots of the complete subtrees, largest first
}

// Add appends the next record's leaf.
func (f *Frontier) Add(leaf Hash) {
	f.nodes = append(f.nodes, leaf)
	// Every trailing one bit of the old size completes a subtree.
	for n := f.size; n&1 == 1; n >>= 1 {
		last := len(f.nodes) - 1
		f.nodes = append(f.nodes[:last-1], tlog.NodeHash(f.nodes[last-1], f.nodes[last]))
	}
	f.size++
}

// Size returns the number of records added.
func (f *Frontier) Size() int64 { return f.size }

// Root returns the RFC 6962 root of the records added so far.
func (f *Frontier) Root() Hash {
	if len(f.nodes) == 0 {
		return Hash{}
	}
	root := f.nodes[len(f.nodes)-1]
	for i := len(f.nodes) - 2; i >= 0; i-- {
		root = tlog.NodeHash(f.nodes[i], root)
	}
	return root
}
