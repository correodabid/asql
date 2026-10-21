package ledgerdigest

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func testSigner(t *testing.T) *Ed25519Signer {
	t.Helper()
	dir := t.TempDir()
	if _, err := GenerateKeyFiles(filepath.Join(dir, "node.key"), filepath.Join(dir, "node.pub")); err != nil {
		t.Fatal(err)
	}
	signer, err := LoadSigner(filepath.Join(dir, "node.key"))
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

func testDigest(t *testing.T, signer *Ed25519Signer) Digest {
	t.Helper()
	d, err := Sign(signer, Digest{
		LSN:       42,
		Chain:     [32]byte{1, 2, 3},
		Term:      3,
		Timestamp: 40,
		NodeID:    "node-a",
		SignedAt:  time.Date(2026, 10, 15, 12, 0, 0, 123456789, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDigestSignVerifyAndJSONRoundTrip(t *testing.T) {
	signer := testSigner(t)
	keys := Keyring{}
	keys.Add(signer.PublicKey())
	d := testDigest(t, signer)
	if d.Version != Version1 || d.KeyID != signer.KeyID() {
		t.Fatalf("digest version/key id = %d/%q", d.Version, d.KeyID)
	}
	if err := d.Verify(keys); err != nil {
		t.Fatalf("verify: %v", err)
	}

	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var back Digest
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if err := back.Verify(keys); err != nil {
		t.Fatalf("verify after JSON round trip: %v\n%s", err, data)
	}
	if back.LSN != d.LSN || back.Chain != d.Chain || !back.SignedAt.Equal(d.SignedAt) {
		t.Fatalf("round trip changed the digest: %+v", back)
	}
}

func TestDigestVerifyRejectsAnyChange(t *testing.T) {
	signer := testSigner(t)
	keys := Keyring{}
	keys.Add(signer.PublicKey())
	for name, mutate := range map[string]func(*Digest){
		"lsn":       func(d *Digest) { d.LSN++ },
		"chain":     func(d *Digest) { d.Chain[31] ^= 1 },
		"term":      func(d *Digest) { d.Term++ },
		"timestamp": func(d *Digest) { d.Timestamp++ },
		"node_id":   func(d *Digest) { d.NodeID = "node-b" },
		"signed_at": func(d *Digest) { d.SignedAt = d.SignedAt.Add(time.Nanosecond) },
		"sig":       func(d *Digest) { d.Sig[0] ^= 1 },
	} {
		d := testDigest(t, signer)
		mutate(&d)
		if err := d.Verify(keys); !errors.Is(err, ErrBadSignature) {
			t.Errorf("%s changed: err=%v, want ErrBadSignature", name, err)
		}
	}
}

func TestVersion2DigestSignsTheTreeRoot(t *testing.T) {
	signer := testSigner(t)
	keys := Keyring{}
	keys.Add(signer.PublicKey())
	v1 := testDigest(t, signer)
	v1Bytes, _ := v1.CanonicalBytes()

	d := v1
	d.Version, d.TreeRoot = Version2, [32]byte{9, 8, 7}
	d, err := Sign(signer, d)
	if err != nil {
		t.Fatal(err)
	}
	if !d.HasTree() || d.Version != Version2 || v1.HasTree() {
		t.Fatalf("version %d, HasTree %v; version 1 HasTree %v", d.Version, d.HasTree(), v1.HasTree())
	}
	if err := d.Verify(keys); err != nil {
		t.Fatal(err)
	}
	// The tree root is signed between the chain and the term; version 1
	// bytes do not change.
	v2Bytes, _ := d.CanonicalBytes()
	if len(v2Bytes) != len(v1Bytes)+32 {
		t.Fatalf("version 2 encodes %d bytes, version 1 %d", len(v2Bytes), len(v1Bytes))
	}

	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var back Digest
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if err := back.Verify(keys); err != nil || back.TreeRoot != d.TreeRoot {
		t.Fatalf("after a JSON round trip: %v, tree root %x\n%s", err, back.TreeRoot, data)
	}
	if data, _ := json.Marshal(v1); strings.Contains(string(data), "tree_root") {
		t.Fatalf("a version 1 digest carries a tree root: %s", data)
	}

	for name, mutate := range map[string]func(*Digest){
		"tree root":              func(d *Digest) { d.TreeRoot[0] ^= 1 },
		"downgrade to version 1": func(d *Digest) { d.Version = Version1 },
	} {
		forged := d
		mutate(&forged)
		if err := forged.Verify(keys); !errors.Is(err, ErrBadSignature) {
			t.Errorf("%s: err=%v, want ErrBadSignature", name, err)
		}
	}
	var stripped map[string]any
	_ = json.Unmarshal(data, &stripped)
	delete(stripped, "tree_root")
	data, _ = json.Marshal(stripped)
	if err := json.Unmarshal(data, &back); err == nil {
		t.Fatal("a version 2 digest without its tree root decodes")
	}
}

func TestDigestVerifyNeedsATrustedKey(t *testing.T) {
	signer, other := testSigner(t), testSigner(t)
	d := testDigest(t, signer)
	keys := Keyring{}
	keys.Add(other.PublicKey())
	if err := d.Verify(keys); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("err=%v, want ErrUnknownKey", err)
	}
	// Re-signing a forged digest with another key does not help: the key id
	// names a key the verifier does not trust.
	forged := d
	forged.Chain[0] ^= 1
	forged, err := Sign(other, forged)
	if err != nil {
		t.Fatal(err)
	}
	trusted := Keyring{}
	trusted.Add(signer.PublicKey())
	if err := forged.Verify(trusted); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("forged digest: err=%v, want ErrUnknownKey", err)
	}
	// A key id that collides with a trusted one still fails the signature.
	forged.KeyID = signer.KeyID()
	if err := forged.Verify(trusted); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("forged digest with a trusted key id: err=%v, want ErrBadSignature", err)
	}
}

func TestLoadKeyringReadsSeveralKeys(t *testing.T) {
	dir := t.TempDir()
	var all []byte
	var pubs []ed25519.PublicKey
	for _, name := range []string{"a", "b"} {
		pub, err := GenerateKeyFiles(filepath.Join(dir, name+".key"), filepath.Join(dir, name+".pub"))
		if err != nil {
			t.Fatal(err)
		}
		pubs = append(pubs, pub)
		data, _ := os.ReadFile(filepath.Join(dir, name+".pub"))
		all = append(all, data...)
	}
	path := filepath.Join(dir, "cluster.pub")
	if err := os.WriteFile(path, all, 0o644); err != nil {
		t.Fatal(err)
	}
	keys, err := LoadKeyring(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, pub := range pubs {
		if _, ok := keys[KeyID(pub)]; !ok {
			t.Fatalf("key %s missing from keyring", KeyID(pub))
		}
	}
}

func TestLoadSignerRejectsOpenPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permissions")
	}
	dir := t.TempDir()
	key := filepath.Join(dir, "node.key")
	if _, err := GenerateKeyFiles(key, filepath.Join(dir, "node.pub")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(key, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSigner(key); err == nil {
		t.Fatal("expected a world-readable key to be rejected")
	}
	if _, err := GenerateKeyFiles(key, filepath.Join(dir, "node.pub")); err == nil {
		t.Fatal("expected keygen to refuse to overwrite a private key")
	}
}

// TestLoadsStandardPEMKeys uses the example key pair of RFC 8410 §10.1 and
// §10.3, in the PKCS #8 / PKIX encoding that OpenSSL writes.
func TestLoadsStandardPEMKeys(t *testing.T) {
	dir := t.TempDir()
	priv := filepath.Join(dir, "rfc8410.key")
	pub := filepath.Join(dir, "rfc8410.pub")
	if err := os.WriteFile(priv, []byte("-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEINTuctv5E1hK1bbY8fdp+K06/nwoy/HU++CXqI9EdVhC\n-----END PRIVATE KEY-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pub, []byte("-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAGb9ECWmEzf6FQbrBZ9w7lshQhqowtrbLDFw4rXAxZuE=\n-----END PUBLIC KEY-----\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	signer, err := LoadSigner(priv)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := LoadKeyring(pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := testDigest(t, signer).Verify(keys); err != nil {
		t.Fatalf("RFC 8410 key pair: %v", err)
	}
}
