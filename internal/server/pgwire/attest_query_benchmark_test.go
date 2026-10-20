package pgwire

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/storage/ledgerdigest"
)

// BenchmarkAttestQuery compares attest_query with the AS OF LSN query it
// attests (ADR 0006), at the head and at an old LSN whose chain value is no
// longer in memory and is rehashed from its segment's anchor.
func BenchmarkAttestQuery(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	keyPath := filepath.Join(dir, "node.key")
	if _, err := ledgerdigest.GenerateKeyFiles(keyPath, filepath.Join(dir, "node.pub")); err != nil {
		b.Fatal(err)
	}
	server, err := New(Config{
		Address: "127.0.0.1:0", DataDirPath: filepath.Join(dir, "data"), NodeID: "node-a",
		LedgerMode: "signed", LedgerKeyFile: keyPath, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(server.Stop)

	session := server.engine.NewSession()
	exec := func(sql string) {
		if _, err := server.engine.Execute(ctx, session, sql); err != nil {
			b.Fatalf("%s: %v", sql, err)
		}
	}
	exec("BEGIN DOMAIN bench")
	exec("CREATE TABLE results (id INT PRIMARY KEY, result TEXT)")
	exec("COMMIT")
	var old uint64
	// 2,000 commits of 3 records: the oldest fall out of the 4,096 chain
	// values kept in memory.
	for i := 0; i < 2000; i++ {
		exec("BEGIN DOMAIN bench")
		exec(fmt.Sprintf("INSERT INTO results (id, result) VALUES (%d, 'r-%d')", i, i))
		exec("COMMIT")
		if i == 100 {
			old = server.engine.HeadLSN()
		}
	}
	if _, ok := server.walStore.LedgerChainAt(old); ok {
		b.Fatalf("LSN %d is still in memory", old)
	}

	const query = "SELECT id, result FROM bench.results WHERE id < 50"
	for _, at := range []struct {
		name string
		lsn  uint64
	}{{"head", server.engine.HeadLSN()}, {"old", old}} {
		b.Run("as-of/"+at.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := server.engine.TimeTravelQueryAsOfLSN(ctx, query, nil, at.lsn); err != nil {
					b.Fatal(err)
				}
			}
		})
		sql := fmt.Sprintf("SELECT * FROM asql_admin.attest_query WHERE sql = '%s' AND lsn = %d", query, at.lsn)
		b.Run("attest/"+at.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := server.attestQuery(ctx, sql, nil, "asql"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
