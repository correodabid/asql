package ledgerdigest

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"runtime"
)

// Ed25519Signer implements ports.LedgerSigner with an in-process key.
type Ed25519Signer struct {
	key   ed25519.PrivateKey
	keyID string
}

// NewEd25519Signer wraps a private key.
func NewEd25519Signer(key ed25519.PrivateKey) *Ed25519Signer {
	return &Ed25519Signer{key: key, keyID: KeyID(key.Public().(ed25519.PublicKey))}
}

// KeyID implements ports.LedgerSigner.
func (s *Ed25519Signer) KeyID() string { return s.keyID }

// PublicKey returns the verification key.
func (s *Ed25519Signer) PublicKey() ed25519.PublicKey { return s.key.Public().(ed25519.PublicKey) }

// Sign implements ports.LedgerSigner. Ed25519 signing is deterministic.
func (s *Ed25519Signer) Sign(message []byte) ([]byte, error) {
	return ed25519.Sign(s.key, message), nil
}

// LoadSigner reads an Ed25519 private key in PEM-encoded PKCS #8, the format
// `openssl genpkey -algorithm ed25519` and `asqlctl -command ledger-keygen`
// write. The file must not be readable by group or others.
func LoadSigner(path string) (*Ed25519Signer, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("ledger key: %w", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("ledger key %s: permissions %04o are too open; use chmod 600", path, info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ledger key: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("ledger key %s: expected a PEM \"PRIVATE KEY\" block (PKCS #8)", path)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("ledger key %s: %w", path, err)
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("ledger key %s: not an Ed25519 key", path)
	}
	return NewEd25519Signer(key), nil
}

// LoadKeyring reads trusted Ed25519 public keys: one or more PEM "PUBLIC KEY"
// blocks (PKIX), as `openssl pkey -pubout` and ledger-keygen write. A file
// may hold several keys, e.g. one per cluster node.
func LoadKeyring(path string) (Keyring, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ledger public keys: %w", err)
	}
	keys := Keyring{}
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			break
		}
		if block.Type != "PUBLIC KEY" {
			continue
		}
		parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("ledger public keys %s: %w", path, err)
		}
		pub, ok := parsed.(ed25519.PublicKey)
		if !ok {
			return nil, fmt.Errorf("ledger public keys %s: not an Ed25519 key", path)
		}
		keys.Add(pub)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("ledger public keys %s: no PEM \"PUBLIC KEY\" block found", path)
	}
	return keys, nil
}

// GenerateKeyFiles creates a new Ed25519 key pair: the private key at
// privPath (mode 0600, never overwritten) and the public key at pubPath.
func GenerateKeyFiles(privPath, pubPath string) (ed25519.PublicKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(privPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create private key: %w", err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: privDER}); err != nil {
		_ = f.Close()
		return nil, errors.Join(err, os.Remove(privPath))
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o644); err != nil {
		return nil, fmt.Errorf("write public key: %w", err)
	}
	return pub, nil
}
