package pgwire

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/correodabid/asql/internal/storage/ledgerdigest"
)

// syncBuffer is a log sink safe for the server's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestLedgerStateKeyLoading(t *testing.T) {
	newServer := func(t *testing.T, config Config) (*Server, string, error) {
		t.Helper()
		var logs syncBuffer
		config.Logger = slog.New(slog.NewTextHandler(&logs, nil))
		server, err := New(config)
		if err == nil {
			t.Cleanup(server.Stop)
		}
		return server, logs.String(), err
	}

	t.Run("missing default file: starts and warns", func(t *testing.T) {
		config, _ := signedLedgerConfig(t)
		server, logs, err := newServer(t, config)
		if err != nil {
			t.Fatal(err)
		}
		if server.ledger.stateKey != nil || !strings.Contains(logs, "no state key") {
			t.Fatalf("state key %v; logs:\n%s", server.ledger.stateKey, logs)
		}
	})

	t.Run("default file next to the signing key", func(t *testing.T) {
		config, _ := signedLedgerConfig(t)
		path := ledgerdigest.DefaultStateKeyPath(config.LedgerKeyFile)
		if err := ledgerdigest.GenerateStateKeyFile(path); err != nil {
			t.Fatal(err)
		}
		want, _ := ledgerdigest.LoadStateKey(path)
		server, logs, err := newServer(t, config)
		if err != nil {
			t.Fatal(err)
		}
		if server.ledger.stateKey == nil || *server.ledger.stateKey != want || strings.Contains(logs, "no state key") {
			t.Fatalf("state key not loaded from %s; logs:\n%s", path, logs)
		}
	})

	t.Run("file named by the flag", func(t *testing.T) {
		config, _ := signedLedgerConfig(t)
		config.LedgerStateKeyFile = filepath.Join(t.TempDir(), "elsewhere.state")
		if err := ledgerdigest.GenerateStateKeyFile(config.LedgerStateKeyFile); err != nil {
			t.Fatal(err)
		}
		// A different default file is ignored.
		if err := ledgerdigest.GenerateStateKeyFile(ledgerdigest.DefaultStateKeyPath(config.LedgerKeyFile)); err != nil {
			t.Fatal(err)
		}
		want, _ := ledgerdigest.LoadStateKey(config.LedgerStateKeyFile)
		server, _, err := newServer(t, config)
		if err != nil {
			t.Fatal(err)
		}
		if server.ledger.stateKey == nil || *server.ledger.stateKey != want {
			t.Fatal("the state key named by the flag was not the one loaded")
		}
	})

	for name, setup := range map[string]func(t *testing.T, config *Config){
		"flag names a missing file": func(t *testing.T, config *Config) {
			config.LedgerStateKeyFile = filepath.Join(t.TempDir(), "missing.state")
		},
		"default file is unusable": func(t *testing.T, config *Config) {
			if err := os.WriteFile(ledgerdigest.DefaultStateKeyPath(config.LedgerKeyFile), []byte("junk"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name+": does not start", func(t *testing.T) {
			config, _ := signedLedgerConfig(t)
			setup(t, &config)
			if _, _, err := newServer(t, config); err == nil || !strings.Contains(err.Error(), "ledger state key") {
				t.Fatalf("err %v, want a ledger state key error", err)
			}
		})
	}

	t.Run("only with ledger mode signed", func(t *testing.T) {
		config, _ := signedLedgerConfig(t)
		config.LedgerMode, config.LedgerKeyFile = "chain", ""
		config.LedgerStateKeyFile = filepath.Join(t.TempDir(), "x.state")
		if err := config.Validate(); err == nil || !strings.Contains(err.Error(), "state key") {
			t.Fatalf("validate: %v", err)
		}
	})
}
