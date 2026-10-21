package ledgerdigest

import (
	"bytes"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// StateKey is the node secret that salts the leaves of the state tree
// (ADR 0008): salt = HMAC-SHA-256(state key, key || commit LSN). It is
// separate from the signing key, so rotating that key does not change the
// tree, and replicas share it as they share the signing key.
type StateKey [32]byte

// stateKeyPEMType is the PEM block type of a state key file: one block
// holding the 32 secret bytes.
const stateKeyPEMType = "ASQL LEDGER STATE KEY"

// StateKeySuffix names the state key file next to a ledger private key:
// node.key -> node.key.state.
const StateKeySuffix = ".state"

// DefaultStateKeyPath returns where the state key of the ledger private key
// at keyPath lives unless configured otherwise.
func DefaultStateKeyPath(keyPath string) string { return keyPath + StateKeySuffix }

// LoadStateKey reads a state key file. Like the private key, it must not be
// readable by group or others.
func LoadStateKey(path string) (StateKey, error) {
	info, err := os.Stat(path)
	if err != nil {
		return StateKey{}, fmt.Errorf("ledger state key: %w", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return StateKey{}, fmt.Errorf("ledger state key %s: permissions %04o are too open; use chmod 600", path, info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return StateKey{}, fmt.Errorf("ledger state key: %w", err)
	}
	block, rest := pem.Decode(data)
	if block == nil || block.Type != stateKeyPEMType {
		return StateKey{}, fmt.Errorf("ledger state key %s: expected a PEM %q block", path, stateKeyPEMType)
	}
	if len(block.Bytes) != len(StateKey{}) {
		return StateKey{}, fmt.Errorf("ledger state key %s: %d bytes, want %d", path, len(block.Bytes), len(StateKey{}))
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return StateKey{}, fmt.Errorf("ledger state key %s: unexpected data after the key", path)
	}
	var key StateKey
	copy(key[:], block.Bytes)
	if key == (StateKey{}) {
		return StateKey{}, fmt.Errorf("ledger state key %s: the key is all zeros", path)
	}
	return key, nil
}

// GenerateStateKeyFile writes a new random state key at path (mode 0600).
// It never overwrites a file: replacing the key of a running tree would
// break every proof built with it. The file appears complete or not at all.
func GenerateStateKeyFile(path string) error {
	var key StateKey
	if _, err := rand.Read(key[:]); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("create state key: %s already exists", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("create state key: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create state key: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func(err error) error { _ = tmp.Close(); return errors.Join(err, os.Remove(tmpPath)) }
	if err := tmp.Chmod(0o600); err != nil {
		return cleanup(fmt.Errorf("create state key: %w", err))
	}
	if err := pem.Encode(tmp, &pem.Block{Type: stateKeyPEMType, Bytes: key[:]}); err != nil {
		return cleanup(fmt.Errorf("create state key: %w", err))
	}
	if err := tmp.Sync(); err != nil {
		return cleanup(fmt.Errorf("create state key: %w", err))
	}
	if err := tmp.Close(); err != nil {
		return errors.Join(fmt.Errorf("create state key: %w", err), os.Remove(tmpPath))
	}
	// A hard link fails if path appeared meanwhile, unlike a rename.
	if err := os.Link(tmpPath, path); err != nil {
		return errors.Join(fmt.Errorf("create state key: %w", err), os.Remove(tmpPath))
	}
	return os.Remove(tmpPath)
}
