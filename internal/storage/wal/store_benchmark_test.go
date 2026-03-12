package wal

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"asql/internal/engine/ports"
)

func BenchmarkFileLogStoreAppend(b *testing.B) {
	ctx := context.Background()
	store := newBenchmarkStore(b)
	defer func() { _ = store.Close() }()

	payload := []byte("payload")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.Append(ctx, ports.WALRecord{TxID: fmt.Sprintf("tx-%d", i), Type: "MUTATION", Timestamp: uint64(i + 1), Payload: payload})
		if err != nil {
			b.Fatalf("append: %v", err)
		}
	}
}

func BenchmarkFileLogStoreReadFrom(b *testing.B) {
	ctx := context.Background()
	store := newBenchmarkStore(b)
	defer func() { _ = store.Close() }()
	seedWAL(b, ctx, store, 5000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records, err := store.ReadFrom(ctx, 1, 0)
		if err != nil {
			b.Fatalf("read from: %v", err)
		}
		if len(records) != 5000 {
			b.Fatalf("unexpected records size: got %d want 5000", len(records))
		}
	}
}

func BenchmarkFileLogStoreRecover(b *testing.B) {
	ctx := context.Background()
	store := newBenchmarkStore(b)
	defer func() { _ = store.Close() }()
	seedWAL(b, ctx, store, 5000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records, err := store.Recover(ctx)
		if err != nil {
			b.Fatalf("recover: %v", err)
		}
		if len(records) != 5000 {
			b.Fatalf("unexpected records size: got %d want 5000", len(records))
		}
	}
}

func newBenchmarkStore(b *testing.B) *FileLogStore {
	b.Helper()

	path := filepath.Join(b.TempDir(), "wal-bench.log")
	store, err := NewFileLogStore(path, EveryN{N: 256})
	if err != nil {
		b.Fatalf("new file log store: %v", err)
	}

	return store
}

func seedWAL(b *testing.B, ctx context.Context, store *FileLogStore, rows int) {
	b.Helper()

	for i := 0; i < rows; i++ {
		_, err := store.Append(ctx, ports.WALRecord{TxID: "seed", Type: "MUTATION", Timestamp: uint64(i + 1), Payload: []byte("seed")})
		if err != nil {
			b.Fatalf("seed append: %v", err)
		}
	}
}
