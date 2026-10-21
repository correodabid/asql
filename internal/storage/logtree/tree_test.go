package logtree

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/mod/sumdb/tlog"
)

// leafFor returns a distinct RFC 6962 leaf hash for record i.
func leafFor(i int) Hash {
	return tlog.RecordHash([]byte(fmt.Sprintf("record %d", i)))
}

// rfc6962Root is the Merkle Tree Hash of RFC 6962 § 2.1 written from the
// RFC, independent of tlog: MTH({}) = SHA-256(), MTH(D[n]) =
// SHA-256(0x01 || MTH(D[0:k]) || MTH(D[k:n])) with k the largest power of
// two below n, over leaf hashes.
func rfc6962Root(leaves []Hash) Hash {
	switch len(leaves) {
	case 0:
		return sha256.Sum256(nil)
	case 1:
		return leaves[0]
	}
	k := 1
	for k*2 < len(leaves) {
		k *= 2
	}
	left, right := rfc6962Root(leaves[:k]), rfc6962Root(leaves[k:])
	return sha256.Sum256(append(append([]byte{0x01}, left[:]...), right[:]...))
}

func TestTreeMatchesRFC6962AndItsProofsVerify(t *testing.T) {
	tree, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()

	const n = 300
	leaves := make([]Hash, n)
	for i := range leaves {
		leaves[i] = leafFor(i)
	}
	// Uneven batches, so interior nodes span batches.
	rng := rand.New(rand.NewPCG(1, 2))
	for appended := 0; appended < n; {
		k := min(n-appended, 1+rng.IntN(40))
		if err := tree.Append(leaves[appended : appended+k]); err != nil {
			t.Fatal(err)
		}
		appended += k
	}
	if tree.Size() != n {
		t.Fatalf("size %d, want %d", tree.Size(), n)
	}

	for size := int64(1); size <= n; size++ {
		root, err := tree.Root(size)
		if err != nil {
			t.Fatal(err)
		}
		if want := rfc6962Root(leaves[:size]); root != want {
			t.Fatalf("root of %d records is %x, RFC 6962 gives %x", size, root, want)
		}
	}

	head, _ := tree.Root(n)
	for _, lsn := range []uint64{1, 2, 3, 128, 129, 256, 257, n} {
		leaf, err := tree.Leaf(lsn)
		if err != nil || leaf != leaves[lsn-1] {
			t.Fatalf("leaf at LSN %d: %x, %v", lsn, leaf, err)
		}
		proof, err := tree.InclusionProof(lsn, n)
		if err != nil {
			t.Fatal(err)
		}
		if err := VerifyInclusion(proof, n, head, lsn, leaves[lsn-1]); err != nil {
			t.Fatalf("inclusion of LSN %d: %v", lsn, err)
		}
		if err := VerifyInclusion(proof, n, head, lsn, leafFor(n+1)); !errors.Is(err, ErrBadProof) {
			t.Fatalf("inclusion of a wrong leaf at LSN %d: %v", lsn, err)
		}
		if lsn < n {
			if err := VerifyInclusion(proof, n, head, lsn+1, leaves[lsn-1]); !errors.Is(err, ErrBadProof) {
				t.Fatalf("inclusion at the wrong LSN %d: %v", lsn+1, err)
			}
		}
	}

	for _, sizes := range [][2]int64{{1, n}, {7, 8}, {8, 9}, {100, 256}, {255, 257}, {n, n}} {
		oldRoot, _ := tree.Root(sizes[0])
		newRoot, _ := tree.Root(sizes[1])
		proof, err := tree.ConsistencyProof(sizes[0], sizes[1])
		if err != nil {
			t.Fatal(err)
		}
		if err := VerifyConsistency(proof, sizes[0], oldRoot, sizes[1], newRoot); err != nil {
			t.Fatalf("consistency %d -> %d: %v", sizes[0], sizes[1], err)
		}
		// A different history of the old size does not verify.
		forged := append([]Hash(nil), leaves[:sizes[0]]...)
		forged[0] = leafFor(n + 1)
		if err := VerifyConsistency(proof, sizes[0], rfc6962Root(forged), sizes[1], newRoot); !errors.Is(err, ErrBadProof) {
			t.Fatalf("consistency %d -> %d from a forged old root: %v", sizes[0], sizes[1], err)
		}
	}

	for _, err := range []error{
		func() error { _, err := tree.Root(n + 1); return err }(),
		func() error { _, err := tree.InclusionProof(0, n); return err }(),
		func() error { _, err := tree.InclusionProof(11, 10); return err }(),
		func() error { _, err := tree.ConsistencyProof(0, n); return err }(),
		func() error { _, err := tree.ConsistencyProof(11, 10); return err }(),
		func() error { _, err := tree.Leaf(n + 1); return err }(),
	} {
		if !errors.Is(err, ErrOutOfRange) {
			t.Fatalf("out of range: %v", err)
		}
	}
}

func TestTreeReopensAndCutsATornTail(t *testing.T) {
	dir := t.TempDir()
	tree, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	leaves := make([]Hash, 23)
	for i := range leaves {
		leaves[i] = leafFor(i)
	}
	if err := tree.Append(leaves); err != nil {
		t.Fatal(err)
	}
	if err := tree.Close(); err != nil {
		t.Fatal(err)
	}

	// A crash in the middle of a record's hashes: record index 23 (binary
	// 10111) completes three interior nodes; only its leaf is written.
	path := filepath.Join(dir, FileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	extra := leafFor(23)
	if _, err := f.Write(append(extra[:], 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	tree, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	if tree.Size() != 23 {
		t.Fatalf("size after a torn tail %d, want 23", tree.Size())
	}
	if info, _ := os.Stat(path); info.Size() != tlog.StoredHashCount(23)*tlog.HashSize {
		t.Fatalf("file is %d bytes, want %d", info.Size(), tlog.StoredHashCount(23)*tlog.HashSize)
	}
	more := append(append([]Hash(nil), leaves...), leafFor(23), leafFor(24))
	if err := tree.Append(more[23:]); err != nil {
		t.Fatal(err)
	}
	if root, _ := tree.Root(25); root != rfc6962Root(more) {
		t.Fatal("root after reopening and appending does not match RFC 6962")
	}
}

func TestSizeForStoredHashes(t *testing.T) {
	for n := int64(0); n < 2000; n++ {
		count := tlog.StoredHashCount(n)
		if got := sizeForStoredHashes(count); got != n {
			t.Fatalf("sizeForStoredHashes(StoredHashCount(%d)) = %d", n, got)
		}
		if next := tlog.StoredHashCount(n + 1); next-count > 1 {
			if got := sizeForStoredHashes(next - 1); got != n {
				t.Fatalf("a partial record after %d records gives size %d", n, got)
			}
		}
	}
}
