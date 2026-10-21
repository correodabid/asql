package ledgerdigest

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/storage/logtree"

	"golang.org/x/mod/sumdb/tlog"
)

// treeOf returns a log tree of n records whose leaves depend on history.
func treeOf(t *testing.T, history string, n int) *logtree.Tree {
	t.Helper()
	tree, err := logtree.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tree.Close() })
	leaves := make([]logtree.Hash, n)
	for i := range leaves {
		leaves[i] = tlog.RecordHash([]byte(fmt.Sprintf("%s %d", history, i)))
	}
	if err := tree.Append(leaves); err != nil {
		t.Fatal(err)
	}
	return tree
}

func signedAt(t *testing.T, signer *Ed25519Signer, tree *logtree.Tree, lsn uint64) Digest {
	t.Helper()
	root, err := tree.Root(int64(lsn))
	if err != nil {
		t.Fatal(err)
	}
	d, err := Sign(signer, Digest{Version: Version2, LSN: lsn, TreeRoot: root, NodeID: "node-a", SignedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestConsistencyProof(t *testing.T) {
	signer := testSigner(t)
	keys := Keyring{}
	keys.Add(signer.PublicKey())
	tree := treeOf(t, "real", 100)
	prove := func(from, to uint64) ConsistencyProof {
		t.Helper()
		proof, err := tree.ConsistencyProof(int64(from), int64(to))
		if err != nil {
			t.Fatal(err)
		}
		return ConsistencyProof{From: signedAt(t, signer, tree, from), To: signedAt(t, signer, tree, to), Proof: proof}
	}

	for _, sizes := range [][2]uint64{{1, 100}, {37, 64}, {64, 65}, {100, 100}} {
		p := prove(sizes[0], sizes[1])
		data, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var back ConsistencyProof
		if err := json.Unmarshal(data, &back); err != nil {
			t.Fatal(err)
		}
		if err := back.Verify(keys); err != nil {
			t.Fatalf("%d -> %d after a JSON round trip: %v\n%s", sizes[0], sizes[1], err, data)
		}
	}

	// The node rewrote its history after LSN 37 and signs the new head: no
	// proof links it to the digest it published at LSN 37.
	forked := treeOf(t, "rewritten", 100)
	p := prove(37, 100)
	p.To = signedAt(t, signer, forked, 100)
	if err := p.Verify(keys); !errors.Is(err, logtree.ErrBadProof) {
		t.Fatalf("a rewritten history: %v, want ErrBadProof", err)
	}
	if proof, err := forked.ConsistencyProof(37, 100); err != nil {
		t.Fatal(err)
	} else if p.Proof = proof; p.Verify(keys) == nil {
		t.Fatal("the rewritten tree's own proof links it to the published digest")
	}

	p = prove(37, 100)
	p.Proof[0][0] ^= 1
	if err := p.Verify(keys); !errors.Is(err, logtree.ErrBadProof) {
		t.Fatalf("an edited proof: %v, want ErrBadProof", err)
	}

	p = prove(37, 100)
	p.From.TreeRoot[0] ^= 1
	if err := p.Verify(keys); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("an edited digest: %v, want ErrBadSignature", err)
	}

	p = prove(37, 100)
	v1, err := Sign(signer, Digest{LSN: 37, NodeID: "node-a", SignedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	p.From = v1
	if err := p.Verify(keys); !errors.Is(err, ErrNoTree) {
		t.Fatalf("a version 1 digest: %v, want ErrNoTree", err)
	}

	p = prove(37, 100)
	p.From, p.To = p.To, p.From
	if err := p.Verify(keys); err == nil || !strings.Contains(err.Error(), "after") {
		t.Fatalf("from after to: %v", err)
	}

	other := testSigner(t)
	p = prove(37, 100)
	p.To = signedAt(t, other, tree, 100)
	if err := p.Verify(keys); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("a digest signed by an untrusted key: %v, want ErrUnknownKey", err)
	}
}
