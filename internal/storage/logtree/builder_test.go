package logtree

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/storage/wal"
)

func openWAL(t *testing.T, base string) *wal.SegmentedLogStore {
	t.Helper()
	// Small segments, so reads cross sealed and compacted segments.
	store, err := wal.NewSegmentedLogStore(base, wal.AlwaysSync{}, wal.WithSegmentSize(4<<10))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func appendRecords(t *testing.T, store *wal.SegmentedLogStore, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		record := ports.WALRecord{Term: 1, TxID: fmt.Sprintf("tx-%d", i), Type: "MUTATION", Timestamp: uint64(i), Payload: []byte(fmt.Sprintf("payload %d", i))}
		if _, err := store.Append(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
}

func walSource(store *wal.SegmentedLogStore) Source {
	return Source{
		CommittedLSN: store.LastLSN,
		Leaves: func(ctx context.Context, from, to uint64) ([]Hash, error) {
			leaves, err := store.LedgerLeaves(ctx, from, to)
			out := make([]Hash, len(leaves))
			for i, l := range leaves {
				out[i] = Hash(l)
			}
			return out, err
		},
	}
}

// walLeaves recomputes every leaf from the WAL records.
func walLeaves(t *testing.T, store *wal.SegmentedLogStore) []Hash {
	t.Helper()
	records, err := store.ReadFrom(context.Background(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	leaves := make([]Hash, len(records))
	for i, r := range records {
		leaf, err := wal.LedgerLeaf(r)
		if err != nil {
			t.Fatal(err)
		}
		leaves[i] = Hash(leaf)
	}
	return leaves
}

func TestBuilderFollowsTheWALWhileItIsWritten(t *testing.T) {
	dir := t.TempDir()
	store := openWAL(t, filepath.Join(dir, "asql.wal"))
	defer store.Close()
	appendRecords(t, store, 1, 500)
	if err := store.CompactSealedSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	tree, err := Open(filepath.Join(dir, "ledger"))
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	builder, err := NewBuilder(tree, walSource(store), 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		appendRecords(t, store, 501, 500)
	}()
	for range 20 {
		if err := builder.CatchUp(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if err := builder.CatchUp(context.Background()); err != nil {
		t.Fatal(err)
	}

	leaves := walLeaves(t, store)
	if len(leaves) != 1000 || tree.Size() != 1000 {
		t.Fatalf("WAL has %d records, tree %d; want 1000", len(leaves), tree.Size())
	}
	if root, _ := tree.Root(1000); root != rfc6962Root(leaves) {
		t.Fatal("the tree's root is not the RFC 6962 root of the WAL's leaves")
	}
}

func TestBuilderResumesAfterARestart(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "asql.wal")
	store := openWAL(t, base)
	appendRecords(t, store, 1, 100)
	tree, err := Open(filepath.Join(dir, "ledger"))
	if err != nil {
		t.Fatal(err)
	}
	builder, _ := NewBuilder(tree, walSource(store), 0, nil)
	if err := builder.CatchUp(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = tree.Close()
	appendRecords(t, store, 101, 50)
	_ = store.Close()

	store = openWAL(t, base)
	defer store.Close()
	tree, err = Open(filepath.Join(dir, "ledger"))
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	builder, _ = NewBuilder(tree, walSource(store), 0, nil)
	if err := builder.CatchUp(context.Background()); err != nil {
		t.Fatal(err)
	}
	if root, _ := tree.Root(150); tree.Size() != 150 || root != rfc6962Root(walLeaves(t, store)) {
		t.Fatalf("after a restart the tree has %d records and does not match the WAL", tree.Size())
	}
}

func TestBuilderRefusesAWALThatDoesNotStartAtLSN1(t *testing.T) {
	dir := t.TempDir()
	store := openWAL(t, filepath.Join(dir, "asql.wal"))
	defer store.Close()
	appendRecords(t, store, 1, 300)
	if err := store.TruncateBefore(context.Background(), 200); err != nil {
		t.Fatal(err)
	}
	tree, err := Open(filepath.Join(dir, "ledger"))
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	builder, _ := NewBuilder(tree, walSource(store), 0, nil)
	if err := builder.CatchUp(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a WAL truncated before the tree was built: %v, want ErrUnavailable", err)
	}
	if !errors.Is(builder.Err(), ErrUnavailable) || tree.Size() != 0 {
		t.Fatalf("builder error %v, tree size %d", builder.Err(), tree.Size())
	}
}

func TestBuilderRefusesATreeFromAnotherWAL(t *testing.T) {
	dir := t.TempDir()
	other := openWAL(t, filepath.Join(dir, "other.wal"))
	appendRecords(t, other, 1000, 10)
	tree, err := Open(filepath.Join(dir, "ledger"))
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	builder, _ := NewBuilder(tree, walSource(other), 0, nil)
	if err := builder.CatchUp(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = other.Close()

	store := openWAL(t, filepath.Join(dir, "asql.wal"))
	defer store.Close()
	appendRecords(t, store, 1, 20)
	builder, _ = NewBuilder(tree, walSource(store), 0, nil)
	if err := builder.CatchUp(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("a tree built from another WAL: %v, want ErrUnavailable", err)
	}
	if tree.Size() != 10 {
		t.Fatalf("the refused tree grew to %d records", tree.Size())
	}
}
