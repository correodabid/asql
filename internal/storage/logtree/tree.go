// Package logtree implements the log tree of ADR 0008: an RFC 6962 Merkle
// tree over the WAL ledger leaves in LSN order, with inclusion proofs for a
// record and consistency proofs between two tree sizes.
//
// The ledger leaves of ADR 0005 are already RFC 6962 record hashes
// (SHA-256(0x00 || fields)), and record i of the tree is the WAL record at
// LSN i+1, so a tree of size n covers LSNs 1..n. The hashing and the proofs
// are those of golang.org/x/mod/sumdb/tlog, which sum.golang.org runs on.
//
// The tree is metadata about the log, built off the commit path: nothing the
// engine computes reads it.
package logtree

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/mod/sumdb/tlog"
)

// FileName is the file, in the ledger directory, that holds the tree's
// stored hashes in tlog's stored-hash order: every leaf and every complete
// interior node, 32 bytes each, about two hashes per record. The tiles of
// tlog are a view over the same hashes.
const FileName = "log-tree.hashes"

// Hash is a tree node or leaf hash.
type Hash = tlog.Hash

var (
	// ErrOutOfRange reports a proof asked for beyond the tree's size.
	ErrOutOfRange = errors.New("log tree: out of range")
	// ErrBadProof reports a proof that does not verify.
	ErrBadProof = errors.New("log tree: proof does not verify")
)

// Tree is an append-only RFC 6962 tree persisted in one file. Appends are
// serialised by the caller (one builder); proofs may run concurrently with
// them.
type Tree struct {
	mu   sync.RWMutex
	file *os.File
	size int64 // records whose hashes are all written and synced
}

// Open opens or creates the tree in dir. A torn tail left by a crash, hashes
// of a record that was not fully written, is cut off.
func Open(dir string) (*Tree, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("log tree: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(dir, FileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("log tree: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("log tree: %w", err)
	}
	stored := info.Size() / tlog.HashSize
	size := sizeForStoredHashes(stored)
	if keep := tlog.StoredHashCount(size) * tlog.HashSize; keep != info.Size() {
		if err := file.Truncate(keep); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("log tree: cut torn tail: %w", err)
		}
	}
	return &Tree{file: file, size: size}, nil
}

// sizeForStoredHashes returns the largest tree size whose stored hashes fit
// in stored hashes.
func sizeForStoredHashes(stored int64) int64 {
	// StoredHashCount(n) = 2n - popcount(n) >= n, so n is at most stored.
	lo, hi := int64(0), stored
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if tlog.StoredHashCount(mid) <= stored {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

// Close closes the tree's file.
func (t *Tree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.file.Close()
}

// Size returns the number of records in the tree, all of them durable.
func (t *Tree) Size() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.size
}

// Append adds leaves as the next records and syncs them, so Size never
// covers hashes a crash could lose.
func (t *Tree) Append(leaves []Hash) error {
	if len(leaves) == 0 {
		return nil
	}
	t.mu.RLock()
	size := t.size
	t.mu.RUnlock()

	// New interior nodes may need hashes appended earlier in this batch.
	pending := make([]Hash, 0, 2*len(leaves))
	base := tlog.StoredHashCount(size)
	reader := tlog.HashReaderFunc(func(indexes []int64) ([]Hash, error) {
		out := make([]Hash, len(indexes))
		var onDisk []int64
		var at []int
		for i, index := range indexes {
			if index >= base {
				out[i] = pending[index-base]
				continue
			}
			onDisk = append(onDisk, index)
			at = append(at, i)
		}
		if len(onDisk) > 0 {
			read, err := t.readHashes(onDisk)
			if err != nil {
				return nil, err
			}
			for j, h := range read {
				out[at[j]] = h
			}
		}
		return out, nil
	})
	for i, leaf := range leaves {
		hashes, err := tlog.StoredHashesForRecordHash(size+int64(i), leaf, reader)
		if err != nil {
			return fmt.Errorf("log tree: %w", err)
		}
		pending = append(pending, hashes...)
	}

	buf := make([]byte, 0, len(pending)*tlog.HashSize)
	for _, h := range pending {
		buf = append(buf, h[:]...)
	}
	if _, err := t.file.WriteAt(buf, base*tlog.HashSize); err != nil {
		return fmt.Errorf("log tree: write: %w", err)
	}
	if err := t.file.Sync(); err != nil {
		return fmt.Errorf("log tree: sync: %w", err)
	}
	t.mu.Lock()
	t.size = size + int64(len(leaves))
	t.mu.Unlock()
	return nil
}

// readHashes reads stored hashes by index from the file.
func (t *Tree) readHashes(indexes []int64) ([]Hash, error) {
	out := make([]Hash, len(indexes))
	for i, index := range indexes {
		if _, err := t.file.ReadAt(out[i][:], index*tlog.HashSize); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("log tree: stored hash %d missing", index)
			}
			return nil, fmt.Errorf("log tree: read: %w", err)
		}
	}
	return out, nil
}

// hashReader reads hashes of a tree of the given size, which must not exceed
// the durable size.
func (t *Tree) hashReader(size int64) (tlog.HashReader, error) {
	if size < 0 || size > t.Size() {
		return nil, fmt.Errorf("%w: size %d, tree has %d records", ErrOutOfRange, size, t.Size())
	}
	return tlog.HashReaderFunc(t.readHashes), nil
}

// Leaf returns the leaf hash of the record at lsn.
func (t *Tree) Leaf(lsn uint64) (Hash, error) {
	if lsn == 0 || int64(lsn) > t.Size() {
		return Hash{}, fmt.Errorf("%w: LSN %d, tree has %d records", ErrOutOfRange, lsn, t.Size())
	}
	hashes, err := t.readHashes([]int64{tlog.StoredHashIndex(0, int64(lsn)-1)})
	if err != nil {
		return Hash{}, err
	}
	return hashes[0], nil
}

// Root returns the root of the tree of size records, which covers LSNs
// 1..size.
func (t *Tree) Root(size int64) (Hash, error) {
	r, err := t.hashReader(size)
	if err != nil {
		return Hash{}, err
	}
	return tlog.TreeHash(size, r)
}

// InclusionProof proves that the record at lsn is in the tree of size
// records.
func (t *Tree) InclusionProof(lsn uint64, size int64) ([]Hash, error) {
	r, err := t.hashReader(size)
	if err != nil {
		return nil, err
	}
	if lsn == 0 || int64(lsn) > size {
		return nil, fmt.Errorf("%w: LSN %d is not in a tree of %d records", ErrOutOfRange, lsn, size)
	}
	return tlog.ProveRecord(size, int64(lsn)-1, r)
}

// ConsistencyProof proves that the tree of oldSize records is a prefix of
// the tree of newSize records.
func (t *Tree) ConsistencyProof(oldSize, newSize int64) ([]Hash, error) {
	r, err := t.hashReader(newSize)
	if err != nil {
		return nil, err
	}
	if oldSize < 1 || oldSize > newSize {
		return nil, fmt.Errorf("%w: old size %d, new size %d", ErrOutOfRange, oldSize, newSize)
	}
	return tlog.ProveTree(newSize, oldSize, r)
}

// VerifyInclusion checks a proof that leaf is the record at lsn in the tree
// of size records with the given root. It needs nothing but its arguments.
func VerifyInclusion(proof []Hash, size int64, root Hash, lsn uint64, leaf Hash) error {
	if lsn == 0 || int64(lsn) > size {
		return fmt.Errorf("%w: LSN %d is not in a tree of %d records", ErrOutOfRange, lsn, size)
	}
	if err := tlog.CheckRecord(proof, size, root, int64(lsn)-1, leaf); err != nil {
		return fmt.Errorf("%w: %v", ErrBadProof, err)
	}
	return nil
}

// VerifyConsistency checks a proof that the tree of oldSize records with
// oldRoot is a prefix of the tree of newSize records with newRoot.
func VerifyConsistency(proof []Hash, oldSize int64, oldRoot Hash, newSize int64, newRoot Hash) error {
	if oldSize < 1 || oldSize > newSize {
		return fmt.Errorf("%w: old size %d, new size %d", ErrOutOfRange, oldSize, newSize)
	}
	if err := tlog.CheckTree(proof, newSize, newRoot, oldSize, oldRoot); err != nil {
		return fmt.Errorf("%w: %v", ErrBadProof, err)
	}
	return nil
}
