package pgwire

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/wal"
)

// BenchmarkStateTreeCheckpoint measures one checkpoint of the state tree
// builder over a verifiable table of 100,000 rows, after 10 rows changed.
func BenchmarkStateTreeCheckpoint(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	store, err := wal.NewSegmentedLogStore(filepath.Join(dir, "wal"), wal.AlwaysSync{})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	engine, err := executor.New(ctx, store, "")
	if err != nil {
		b.Fatal(err)
	}
	session := engine.NewSession()
	exec := func(sql string) {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			b.Fatalf("%s: %v", sql, err)
		}
	}
	exec("BEGIN DOMAIN lab")
	exec("CREATE TABLE artifacts (id TEXT PRIMARY KEY, kind TEXT, sha TEXT) VERIFIABLE")
	exec("COMMIT")
	const rows, batch = 100_000, 5_000
	for i := 0; i < rows; i += batch {
		var values []string
		for j := i; j < i+batch; j++ {
			values = append(values, fmt.Sprintf("('a%06d', 'extraction', 'sha-%d')", j, j))
		}
		exec("BEGIN DOMAIN lab")
		exec("INSERT INTO artifacts (id, kind, sha) VALUES " + strings.Join(values, ", "))
		exec("COMMIT")
	}
	builder := newStateTreeBuilder(engine, [32]byte{1})
	if _, _, _, err := builder.State(ctx); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		exec("BEGIN DOMAIN lab")
		for j := 0; j < 10; j++ {
			exec(fmt.Sprintf("UPDATE artifacts SET kind = 'k%d' WHERE id = 'a%06d'", n, (n*10+j)%rows))
		}
		exec("COMMIT")
		b.StartTimer()
		if _, _, _, err := builder.State(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
