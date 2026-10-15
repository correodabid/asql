package pgwire

import (
	"log/slog"
	"testing"
)

func TestConfigValidateLedgerMode(t *testing.T) {
	base := Config{Address: ":0", DataDirPath: t.TempDir(), Logger: slog.Default()}
	for _, mode := range []string{"", "chain", "off"} {
		cfg := base
		cfg.LedgerMode = mode
		if err := cfg.Validate(); err != nil {
			t.Fatalf("ledger mode %q: %v", mode, err)
		}
	}
	cfg := base
	cfg.LedgerMode = "signed"
	if err := cfg.Validate(); err == nil {
		t.Fatal("ledger mode signed must require a key file")
	}
	cfg.LedgerKeyFile = "node.key"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("ledger mode signed with a key: %v", err)
	}
	cfg = base
	cfg.LedgerKeyFile = "node.key"
	if err := cfg.Validate(); err == nil {
		t.Fatal("a key file without ledger mode signed must be rejected")
	}
	cfg = base
	cfg.LedgerMode = "bogus"
	if err := cfg.Validate(); err == nil {
		t.Fatal("unknown ledger mode accepted")
	}
}
