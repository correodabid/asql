package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pgwire "asql/internal/server/pgwire"

	"github.com/jackc/pgx/v5"
)

// ── Unit tests (no server needed) ─────────────────────────────────────────────

func TestPrintTable(t *testing.T) {
	var buf bytes.Buffer
	cols := []string{"id", "name", "email"}
	rows := [][]string{
		{"1", "Alice", "alice@example.com"},
		{"2", "Bob", "bob@example.com"},
	}
	printTable(&buf, cols, rows)
	out := buf.String()

	// Should contain header.
	if !strings.Contains(out, "id") {
		t.Error("table output missing 'id' column header")
	}
	if !strings.Contains(out, "name") {
		t.Error("table output missing 'name' column header")
	}
	// Should contain data.
	if !strings.Contains(out, "Alice") {
		t.Error("table output missing 'Alice'")
	}
	if !strings.Contains(out, "bob@example.com") {
		t.Error("table output missing 'bob@example.com'")
	}
	// Should contain separator.
	if !strings.Contains(out, "---") {
		t.Error("table output missing separator")
	}
}

func TestPrintTableEmpty(t *testing.T) {
	var buf bytes.Buffer
	printTable(&buf, nil, nil)
	if buf.Len() != 0 {
		t.Errorf("expected empty output for nil columns, got %q", buf.String())
	}
}

func TestPrompt(t *testing.T) {
	s := &shellSession{}

	got := s.prompt(false)
	if got != "asql=> " {
		t.Errorf("default prompt = %q, want %q", got, "asql=> ")
	}

	got = s.prompt(true)
	if got != "    -> " {
		t.Errorf("continuation prompt = %q, want %q", got, "    -> ")
	}

	s.inTx = true
	got = s.prompt(false)
	if got != "asql=# " {
		t.Errorf("tx prompt = %q, want %q", got, "asql=# ")
	}

	s.domain = "accounts"
	got = s.prompt(false)
	if got != "accounts=# " {
		t.Errorf("domain tx prompt = %q, want %q", got, "accounts=# ")
	}

	s.inTx = false
	got = s.prompt(false)
	if got != "accounts=> " {
		t.Errorf("domain no-tx prompt = %q, want %q", got, "accounts=> ")
	}
}

func TestHandleMetaQuit(t *testing.T) {
	var buf bytes.Buffer
	s := &shellSession{out: &buf}

	if !s.handleMeta(`\q`) {
		t.Error(`\q should return quit=true`)
	}
	if !s.handleMeta(`\quit`) {
		t.Error(`\quit should return quit=true`)
	}
}

func TestHandleMetaHelp(t *testing.T) {
	var buf bytes.Buffer
	s := &shellSession{out: &buf}

	s.handleMeta(`\?`)
	out := buf.String()
	if !strings.Contains(out, `\q`) {
		t.Error("help should mention \\q")
	}
	if !strings.Contains(out, `\timing`) {
		t.Error("help should mention \\timing")
	}
}

func TestHandleMetaTiming(t *testing.T) {
	var buf bytes.Buffer
	s := &shellSession{out: &buf}

	s.handleMeta(`\timing`)
	if !s.timing {
		t.Error("timing should be on after first \\timing")
	}

	s.handleMeta(`\timing`)
	if s.timing {
		t.Error("timing should be off after second \\timing")
	}
}

func TestHandleMetaConninfo(t *testing.T) {
	var buf bytes.Buffer
	s := &shellSession{
		cfg: shellConfig{PgwireAddr: "10.0.0.1:5433"},
		out: &buf,
	}

	s.handleMeta(`\conninfo`)
	if !strings.Contains(buf.String(), "10.0.0.1:5433") {
		t.Errorf("conninfo should show address, got %q", buf.String())
	}
}

func TestHandleMetaUnknown(t *testing.T) {
	var buf bytes.Buffer
	s := &shellSession{out: &buf}

	quit := s.handleMeta(`\bogus`)
	if quit {
		t.Error("unknown meta-command should not quit")
	}
	if !strings.Contains(buf.String(), "Unknown command") {
		t.Errorf("expected 'Unknown command', got %q", buf.String())
	}
}

// ── Integration test (spins up a real pgwire server) ──────────────────────────

func TestShellIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	walPath := filepath.Join(t.TempDir(), "data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := pgwire.New(pgwire.Config{
		Address:     "127.0.0.1:0",
		DataDirPath: walPath,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- server.ServeOnListener(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("pgwire server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire shutdown")
		}
	})

	addr := listener.Addr().String()
	connStr := fmt.Sprintf("postgres://asql@%s/asql?sslmode=disable&default_query_exec_mode=simple_protocol", addr)
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })

	var buf bytes.Buffer
	sess := &shellSession{
		cfg:  shellConfig{PgwireAddr: addr},
		conn: conn,
		ctx:  ctx,
		out:  &buf,
	}

	// Test: execute DDL + DML + SELECT workflow.
	t.Run("create_and_query", func(t *testing.T) {
		buf.Reset()
		sess.executeAndDisplay("BEGIN DOMAIN testdomain;")
		t.Logf("BEGIN output: %q", buf.String())
		if !sess.inTx {
			t.Error("expected inTx=true after BEGIN DOMAIN")
		}
		if sess.domain != "testdomain" {
			t.Errorf("expected domain='testdomain', got %q", sess.domain)
		}

		buf.Reset()
		sess.executeAndDisplay("CREATE TABLE items (id INT, name TEXT);")
		t.Logf("CREATE output: %q", buf.String())
		if strings.Contains(buf.String(), "ERROR") {
			t.Errorf("CREATE TABLE failed: %s", buf.String())
		}

		buf.Reset()
		sess.executeAndDisplay("INSERT INTO items (id, name) VALUES (1, 'widget');")
		t.Logf("INSERT output: %q", buf.String())
		if strings.Contains(buf.String(), "ERROR") {
			t.Errorf("INSERT failed: %s", buf.String())
		}

		buf.Reset()
		sess.executeAndDisplay("COMMIT;")
		t.Logf("COMMIT output: %q", buf.String())

		// After committing, query with domain-qualified name.
		buf.Reset()
		sess.executeAndDisplay("SELECT id, name FROM testdomain.items;")
		t.Logf("SELECT output: %q", buf.String())
		out := buf.String()
		if !strings.Contains(out, "widget") {
			t.Errorf("SELECT should contain 'widget', got %s", out)
		}
		if !strings.Contains(out, "(1 row)") {
			t.Errorf("SELECT should show '(1 row)', got %s", out)
		}
	})

	// Test: timing display.
	t.Run("timing", func(t *testing.T) {
		sess.timing = true
		buf.Reset()
		// Use a query that doesn't need a tx (catalog interception).
		sess.executeAndDisplay("SELECT current_database();")
		out := buf.String()
		t.Logf("timing output: %q", out)
		if !strings.Contains(out, "Time:") {
			t.Errorf("expected timing output, got %s", out)
		}
		sess.timing = false
	})

	// Test: \dt meta-command (tables listing).
	t.Run("meta_dt", func(t *testing.T) {
		buf.Reset()
		sess.metaDescribeTables([]string{`\dt`})
		out := buf.String()
		if !strings.Contains(out, "items") {
			t.Errorf("\\dt should list 'items' table, got %s", out)
		}
	})

	// Test: \l meta-command (domains listing).
	t.Run("meta_l", func(t *testing.T) {
		buf.Reset()
		sess.metaListDomains()
		out := buf.String()
		if !strings.Contains(out, "testdomain") {
			t.Errorf("\\l should list 'testdomain', got %s", out)
		}
	})

	// Test: \cluster meta-command.
	t.Run("meta_cluster", func(t *testing.T) {
		buf.Reset()
		sess.metaCluster()
		out := buf.String()
		if !strings.Contains(out, "Node Role:") {
			t.Errorf("\\cluster should show 'Node Role:', got %s", out)
		}
	})

	// Test: error handling.
	t.Run("error_handling", func(t *testing.T) {
		buf.Reset()
		sess.executeAndDisplay("SELECT * FROM nonexistent_table;")
		if !strings.Contains(buf.String(), "ERROR") {
			t.Errorf("expected ERROR for nonexistent table, got %s", buf.String())
		}
	})
}
