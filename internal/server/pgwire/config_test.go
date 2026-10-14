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
		t.Fatal("ledger mode signed must be rejected until signing lands")
	}
}
