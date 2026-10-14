package wal

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/ports"
)

func BenchmarkLedgerLeaf(b *testing.B) {
	for _, size := range []int{0, 256, 1024, 4096} {
		b.Run(fmt.Sprintf("payload=%d", size), func(b *testing.B) {
			rec := ports.WALRecord{Term: 1, Timestamp: 42, TxID: "tx-000042", Type: "MUTATION", Payload: bytes.Repeat([]byte{7}, size)}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := LedgerLeaf(rec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkLedgerChainStep(b *testing.B) {
	leaf, _ := LedgerLeaf(ports.WALRecord{Type: "COMMIT"})
	chain := GenesisChain
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		chain = ChainStep(chain, uint64(i), leaf)
	}
}

// BenchmarkSegmentedAppendTxNoSync appends one transaction (BEGIN + one
// MUTATION + COMMIT) per op without fsync: the CPU-bound path where the
// ledger's relative overhead is largest (ADR 0005 performance gate).
func BenchmarkSegmentedAppendTxNoSync(b *testing.B) {
	ctx := context.Background()
	for _, size := range []int{128, 1024} {
		for _, mode := range []LedgerMode{LedgerOff, LedgerChain} {
			b.Run(fmt.Sprintf("payload=%d/ledger=%s", size, mode), func(b *testing.B) {
				store, err := NewSegmentedLogStore(filepath.Join(b.TempDir(), "bench.wal"), EveryN{N: 1 << 62}, WithLedgerMode(mode))
				if err != nil {
					b.Fatal(err)
				}
				defer store.Close()
				payload := bytes.Repeat([]byte("abcdefgh"), size/8)
				tx := []ports.WALRecord{
					{TxID: "tx-bench", Type: "BEGIN", Timestamp: 1},
					{TxID: "tx-bench", Type: "MUTATION", Timestamp: 2, Payload: payload},
					{TxID: "tx-bench", Type: "COMMIT", Timestamp: 3},
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := store.AppendBatchNoSync(ctx, tx); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
