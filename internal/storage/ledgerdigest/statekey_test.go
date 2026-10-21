package ledgerdigest

import (
	"encoding/pem"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestStateKeyGenerateAndLoad(t *testing.T) {
	dir := t.TempDir()
	if got := DefaultStateKeyPath(filepath.Join(dir, "node.key")); got != filepath.Join(dir, "node.key.state") {
		t.Fatalf("default path %s", got)
	}
	a, b := filepath.Join(dir, "a.state"), filepath.Join(dir, "b.state")
	for _, path := range []string{a, b} {
		if err := GenerateStateKeyFile(path); err != nil {
			t.Fatal(err)
		}
	}
	keyA, err := LoadStateKey(a)
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := LoadStateKey(b)
	if err != nil {
		t.Fatal(err)
	}
	if keyA == keyB || keyA == (StateKey{}) {
		t.Fatal("two generated state keys are equal or zero")
	}
	if again, _ := LoadStateKey(a); again != keyA {
		t.Fatal("loading the same file twice gives different keys")
	}
	if runtime.GOOS != "windows" {
		if info, _ := os.Stat(a); info.Mode().Perm() != 0o600 {
			t.Fatalf("state key file mode %04o, want 0600", info.Mode().Perm())
		}
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 2 {
		t.Fatalf("generation left %d files, want the 2 keys", len(entries))
	}

	// Never overwritten.
	before, _ := os.ReadFile(a)
	if err := GenerateStateKeyFile(a); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("regenerating over an existing key: %v", err)
	}
	if after, _ := os.ReadFile(a); string(after) != string(before) {
		t.Fatal("an existing state key was changed")
	}
}

func TestLoadStateKeyRejectsBadFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, data []byte, mode os.FileMode) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		return path
	}
	key := make([]byte, 32)
	key[0] = 1
	block := func(typ string, b []byte) []byte { return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: b}) }

	good := write("good.state", block(stateKeyPEMType, key), 0o600)
	if _, err := LoadStateKey(good); err != nil {
		t.Fatalf("a valid file: %v", err)
	}
	cases := map[string]struct {
		path string
		want string
	}{
		"missing":       {filepath.Join(dir, "missing.state"), "no such file"},
		"wrong type":    {write("type.state", block("PRIVATE KEY", key), 0o600), "expected a PEM"},
		"short":         {write("short.state", block(stateKeyPEMType, key[:31]), 0o600), "31 bytes"},
		"trailing data": {write("trailing.state", append(block(stateKeyPEMType, key), "extra"...), 0o600), "unexpected data"},
		"all zeros":     {write("zeros.state", block(stateKeyPEMType, make([]byte, 32)), 0o600), "all zeros"},
		"not PEM":       {write("hex.state", []byte(strings.Repeat("ab", 32)+"\n"), 0o600), "expected a PEM"},
	}
	if runtime.GOOS != "windows" {
		cases["readable by others"] = struct {
			path string
			want string
		}{write("open.state", block(stateKeyPEMType, key), 0o644), "too open"}
	}
	for name, tc := range cases {
		if _, err := LoadStateKey(tc.path); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want an error with %q", name, err, tc.want)
		}
	}
}
