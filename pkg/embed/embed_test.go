package embed_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/correodabid/asql/pkg/embed"
)

func start(t *testing.T, opts embed.Options) *embed.Server {
	t.Helper()
	srv, err := embed.Start(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// closeWithin fails the test if Close takes longer than d.
func closeWithin(t *testing.T, srv *embed.Server, d time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- srv.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(d):
		t.Fatalf("Close did not return within %s", d)
	}
}

// TestEmbeddedServerKeepsItsDataAcrossRestarts: an embedded server serves
// pgwire, closes with clients still connected, and a later Start on the same
// directory recovers the data.
func TestEmbeddedServerKeepsItsDataAcrossRestarts(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")

	srv := start(t, embed.Options{DataDir: dir, AuthToken: "secret"})
	conn, err := pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"BEGIN DOMAIN app",
		"CREATE TABLE artifacts (id INT PRIMARY KEY, kind TEXT)",
		"INSERT INTO artifacts (id, kind) VALUES (1, 'report')",
		"COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	// conn stays open: Close must not wait for it.
	closeWithin(t, srv, 5*time.Second)
	if err := srv.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	_ = conn.Close(ctx)

	srv = start(t, embed.Options{DataDir: dir, AuthToken: "secret"})
	defer closeWithin(t, srv, 5*time.Second)
	conn, err = pgx.Connect(ctx, srv.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var kind string
	if err := conn.QueryRow(ctx, "SELECT kind FROM app.artifacts WHERE id = 1").Scan(&kind); err != nil || kind != "report" {
		t.Fatalf("after restart: kind=%q err=%v", kind, err)
	}
}

// TestEmbeddedServersAreIndependent: each server has its own data, as a
// contract test that needs an empty store per test does.
func TestEmbeddedServersAreIndependent(t *testing.T) {
	ctx := context.Background()
	a := start(t, embed.Options{DataDir: filepath.Join(t.TempDir(), "a")})
	defer closeWithin(t, a, 5*time.Second)
	b := start(t, embed.Options{DataDir: filepath.Join(t.TempDir(), "b")})
	defer closeWithin(t, b, 5*time.Second)
	if a.Addr() == b.Addr() {
		t.Fatalf("both servers listen on %s", a.Addr())
	}

	connA, err := pgx.Connect(ctx, a.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connA.Close(ctx) }()
	for _, sql := range []string{"BEGIN DOMAIN app", "CREATE TABLE t (id INT PRIMARY KEY)", "COMMIT"} {
		if _, err := connA.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	connB, err := pgx.Connect(ctx, b.DSN())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connB.Close(ctx) }()
	if _, err := connB.Exec(ctx, "SELECT * FROM app.t"); err == nil {
		t.Fatal("server b sees a table created on server a")
	}
}

func TestStartRequiresADataDir(t *testing.T) {
	if _, err := embed.Start(context.Background(), embed.Options{}); err == nil {
		t.Fatal("Start without DataDir succeeded")
	}
}

// TestASecondServerOnTheSameDataDirFails (ASQL-11): a data directory has one
// server at a time; the next Start on it fails until the first is closed.
func TestASecondServerOnTheSameDataDirFails(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	first := start(t, embed.Options{DataDir: dir})
	if _, err := embed.Start(context.Background(), embed.Options{DataDir: dir}); !errors.Is(err, embed.ErrDataDirInUse) {
		t.Fatalf("second Start: %v, want ErrDataDirInUse", err)
	}
	closeWithin(t, first, 5*time.Second)
	again := start(t, embed.Options{DataDir: dir})
	closeWithin(t, again, 5*time.Second)
}
