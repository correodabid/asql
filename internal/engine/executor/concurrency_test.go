package executor

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/correodabid/asql/internal/storage/wal"
)

// TestConcurrentReadsDoNotBlock verifies that multiple goroutines can execute
// read queries simultaneously without blocking each other. Because reads use
// an atomic load (lock-free), they must never be serialised by a mutex.
func TestConcurrentReadsDoNotBlock(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent-reads.wal")

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN accounts")
	exec("CREATE TABLE users (id INT, email TEXT)")
	for i := 1; i <= 100; i++ {
		exec(fmt.Sprintf("INSERT INTO users (id, email) VALUES (%d, 'user-%d@asql.dev')", i, i))
	}
	exec("COMMIT")

	headLSN := engine.readState.Load().headLSN

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users ORDER BY id ASC", []string{"accounts"}, headLSN)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: %v", id, err)
				return
			}
			if len(result.Rows) != 100 {
				errs <- fmt.Errorf("goroutine %d: expected 100 rows, got %d", id, len(result.Rows))
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

// TestConcurrentReadsConsistentSnapshot verifies that a reader goroutine sees a
// consistent snapshot even while a writer commits new rows. The reader should
// see exactly the rows that existed at the LSN it queried — not more, not fewer.
func TestConcurrentReadsConsistentSnapshot(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "consistent-snapshot.wal")

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	// Setup: create table with initial rows.
	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN accounts")
	exec("CREATE TABLE counters (id INT, val INT)")
	for i := 1; i <= 10; i++ {
		exec(fmt.Sprintf("INSERT INTO counters (id, val) VALUES (%d, %d)", i, i*10))
	}
	exec("COMMIT")

	// Capture LSN after initial 10 rows.
	snapshotLSN := engine.readState.Load().headLSN

	// Writer goroutine: continuously insert more rows.
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for i := 11; i <= 50; i++ {
			s := engine.NewSession()
			if _, err := engine.Execute(ctx, s, "BEGIN DOMAIN accounts"); err != nil {
				return
			}
			if _, err := engine.Execute(ctx, s, fmt.Sprintf("INSERT INTO counters (id, val) VALUES (%d, %d)", i, i*10)); err != nil {
				return
			}
			if _, err := engine.Execute(ctx, s, "COMMIT"); err != nil {
				return
			}
		}
	}()

	// Reader goroutines: query at the snapshotLSN and expect exactly 10 rows.
	const readers = 10
	var readerWg sync.WaitGroup
	errs := make(chan error, readers)

	for r := 0; r < readers; r++ {
		readerWg.Add(1)
		go func(id int) {
			defer readerWg.Done()
			result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, val FROM counters", []string{"accounts"}, snapshotLSN)
			if err != nil {
				errs <- fmt.Errorf("reader %d: %v", id, err)
				return
			}
			if len(result.Rows) != 10 {
				errs <- fmt.Errorf("reader %d at LSN %d: expected 10 rows, got %d", id, snapshotLSN, len(result.Rows))
			}
		}(r)
	}

	readerWg.Wait()
	writerWg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

// TestWriteSerialisation verifies that concurrent writes to the same domain
// are serialised by writeMu: all committed inserts must be visible in the
// final state, and no rows are lost. Transactions that hit a write conflict
// are retried, which is the expected optimistic-concurrency pattern.
func TestWriteSerialisation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "write-serial.wal")

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	if _, err := engine.Execute(ctx, session, "BEGIN DOMAIN accounts"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := engine.Execute(ctx, session, "CREATE TABLE events (id INT, source TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := engine.Execute(ctx, session, "COMMIT"); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Launch multiple writers, each inserting rows in separate transactions.
	// Write conflicts are retried (optimistic concurrency).
	const writers = 10
	const rowsPerWriter = 5
	const maxRetries = 50
	var wg sync.WaitGroup
	errs := make(chan error, writers*rowsPerWriter)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < rowsPerWriter; i++ {
				id := writerID*1000 + i
				sql := fmt.Sprintf("INSERT INTO events (id, source) VALUES (%d, 'writer-%d')", id, writerID)

				var committed bool
				for attempt := 0; attempt < maxRetries; attempt++ {
					s := engine.NewSession()
					if _, err := engine.Execute(ctx, s, "BEGIN DOMAIN accounts"); err != nil {
						errs <- fmt.Errorf("writer %d begin: %v", writerID, err)
						return
					}
					if _, err := engine.Execute(ctx, s, sql); err != nil {
						errs <- fmt.Errorf("writer %d insert: %v", writerID, err)
						return
					}
					if _, err := engine.Execute(ctx, s, "COMMIT"); err != nil {
						// Write conflict — retry with fresh snapshot.
						continue
					}
					committed = true
					break
				}
				if !committed {
					errs <- fmt.Errorf("writer %d row %d: failed after %d retries", writerID, i, maxRetries)
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	headLSN := engine.readState.Load().headLSN
	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM events", []string{"accounts"}, headLSN)
	if err != nil {
		t.Fatalf("final select: %v", err)
	}

	expected := writers * rowsPerWriter
	if len(result.Rows) != expected {
		t.Errorf("expected %d rows, got %d", expected, len(result.Rows))
	}
}

// TestAlterTableCOWSafety verifies that a concurrent reader still sees the old
// schema (without the new column) while an ALTER TABLE ADD COLUMN commit is
// in progress, and after the commit the new column is visible.
func TestAlterTableCOWSafety(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "alter-cow.wal")

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN accounts")
	exec("CREATE TABLE users (id INT, email TEXT)")
	exec("INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')")
	exec("COMMIT")

	// Capture the LSN before ALTER TABLE.
	preLSN := engine.readState.Load().headLSN

	// Apply ALTER TABLE ADD COLUMN.
	session = engine.NewSession()
	exec("BEGIN DOMAIN accounts")
	exec("ALTER TABLE users ADD COLUMN status TEXT")
	exec("INSERT INTO users (id, email, status) VALUES (2, 'two@asql.dev', 'active')")
	exec("COMMIT")

	postLSN := engine.readState.Load().headLSN

	// Query at the pre-ALTER LSN: should see 1 row without status column.
	pre, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users", []string{"accounts"}, preLSN)
	if err != nil {
		t.Fatalf("pre-alter query: %v", err)
	}
	if len(pre.Rows) != 1 {
		t.Errorf("pre-alter: expected 1 row, got %d", len(pre.Rows))
	}

	// Query at the post-ALTER LSN: should see 2 rows with status column.
	post, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email, status FROM users ORDER BY id ASC", []string{"accounts"}, postLSN)
	if err != nil {
		t.Fatalf("post-alter query: %v", err)
	}
	if len(post.Rows) != 2 {
		t.Errorf("post-alter: expected 2 rows, got %d", len(post.Rows))
	}

	// Concurrent reads at pre and post should both succeed with no data races.
	const goroutines = 10
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*2)

	for g := 0; g < goroutines; g++ {
		wg.Add(2)
		go func(id int) {
			defer wg.Done()
			r, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM users", []string{"accounts"}, preLSN)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d pre: %v", id, err)
				return
			}
			if len(r.Rows) != 1 {
				errs <- fmt.Errorf("goroutine %d pre: expected 1 row, got %d", id, len(r.Rows))
			}
		}(g)
		go func(id int) {
			defer wg.Done()
			r, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM users", []string{"accounts"}, postLSN)
			if err != nil {
				errs <- fmt.Errorf("goroutine %d post: %v", id, err)
				return
			}
			if len(r.Rows) != 2 {
				errs <- fmt.Errorf("goroutine %d post: expected 2 rows, got %d", id, len(r.Rows))
			}
		}(g)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

// TestConcurrentWritesDifferentDomains verifies that writes to different
// domains can proceed via writeMu without causing data corruption.
func TestConcurrentWritesDifferentDomains(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "multi-domain-writes.wal")

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	// Setup two domains.
	domains := []string{"billing", "shipping"}
	for _, d := range domains {
		s := engine.NewSession()
		if _, err := engine.Execute(ctx, s, fmt.Sprintf("BEGIN DOMAIN %s", d)); err != nil {
			t.Fatalf("begin %s: %v", d, err)
		}
		if _, err := engine.Execute(ctx, s, "CREATE TABLE events (id INT, data TEXT)"); err != nil {
			t.Fatalf("create %s: %v", d, err)
		}
		if _, err := engine.Execute(ctx, s, "COMMIT"); err != nil {
			t.Fatalf("commit %s: %v", d, err)
		}
	}

	const rowsPerDomain = 20
	var wg sync.WaitGroup
	errs := make(chan error, len(domains)*rowsPerDomain)

	for _, d := range domains {
		wg.Add(1)
		go func(domain string) {
			defer wg.Done()
			for i := 1; i <= rowsPerDomain; i++ {
				s := engine.NewSession()
				if _, err := engine.Execute(ctx, s, fmt.Sprintf("BEGIN DOMAIN %s", domain)); err != nil {
					errs <- fmt.Errorf("begin %s/%d: %v", domain, i, err)
					return
				}
				sql := fmt.Sprintf("INSERT INTO events (id, data) VALUES (%d, '%s-%d')", i, domain, i)
				if _, err := engine.Execute(ctx, s, sql); err != nil {
					errs <- fmt.Errorf("insert %s/%d: %v", domain, i, err)
					return
				}
				if _, err := engine.Execute(ctx, s, "COMMIT"); err != nil {
					errs <- fmt.Errorf("commit %s/%d: %v", domain, i, err)
					return
				}
			}
		}(d)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	headLSN := engine.readState.Load().headLSN
	for _, d := range domains {
		result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id FROM events", []string{d}, headLSN)
		if err != nil {
			t.Fatalf("select %s: %v", d, err)
		}
		if len(result.Rows) != rowsPerDomain {
			t.Errorf("domain %s: expected %d rows, got %d", d, rowsPerDomain, len(result.Rows))
		}
	}
}
