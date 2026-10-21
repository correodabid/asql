// Package ledgerdigest implements signed WAL ledger digests (ADR 0005,
// ledger=signed): the digest format and its canonical encoding, Ed25519 keys,
// the sidecar digest log, and the checkpointer that signs committed chain
// heads. It also implements result attestations (ADR 0006), signed with the
// same keys under their own domain.
//
// A digest commits to the ledger chain value at a committed LSN. Verifying
// one needs a public key obtained outside the digest itself: a digest never
// carries the key that verifies it, or anyone could re-sign a forged one.
package ledgerdigest

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
)

// Digest format versions. Both are Ed25519 over the canonical encoding
// below; version 2 (ADR 0008) also signs the log tree root at LSN.
const (
	Version1 = 1
	Version2 = 2
)

// signingDomain separates digest signatures from any other use of the key.
const signingDomain = "asql-ledger-digest-v1"

var (
	// ErrBadSignature reports a digest whose signature does not verify.
	ErrBadSignature = errors.New("ledger digest: signature does not verify")
	// ErrUnknownKey reports a digest signed by a key the verifier does not
	// trust.
	ErrUnknownKey = errors.New("ledger digest: signing key is not trusted")
)

// Digest is a signed statement that the WAL ledger chain had value Chain at
// LSN. LSN is always committed: a digest never covers entries that Raft
// could still replace.
type Digest struct {
	Version uint8
	LSN     uint64
	Chain   [32]byte
	// Term and Timestamp are those of the WAL record at LSN. Timestamp is
	// the logical WAL timestamp, not wall-clock time.
	Term      uint64
	Timestamp uint64
	// TreeRoot is the root of the RFC 6962 log tree of LSNs 1..LSN
	// (ADR 0008). Only version 2 digests carry it.
	TreeRoot [32]byte
	NodeID   string
	KeyID    string
	// SignedAt is the signer's wall clock. It is signed but only asserted by
	// the node: proving that a state existed by some time needs the digest
	// published or time-stamped elsewhere.
	SignedAt time.Time
	Sig      []byte
}

// CanonicalBytes returns the exact bytes that are signed. All integers are
// big-endian; strings are length-prefixed. Version 2 inserts the tree root
// after the chain; version 1 bytes are unchanged.
func (d Digest) CanonicalBytes() ([]byte, error) {
	if len(d.NodeID) > math.MaxUint16 || len(d.KeyID) > math.MaxUint16 {
		return nil, errors.New("ledger digest: node or key id too long")
	}
	b := make([]byte, 0, len(signingDomain)+1+1+8+32+32+8+8+8+2+len(d.NodeID)+2+len(d.KeyID))
	b = append(b, signingDomain...)
	b = append(b, 0)
	b = append(b, d.Version)
	b = binary.BigEndian.AppendUint64(b, d.LSN)
	b = append(b, d.Chain[:]...)
	if d.Version >= Version2 {
		b = append(b, d.TreeRoot[:]...)
	}
	b = binary.BigEndian.AppendUint64(b, d.Term)
	b = binary.BigEndian.AppendUint64(b, d.Timestamp)
	b = binary.BigEndian.AppendUint64(b, uint64(d.SignedAt.UnixNano()))
	b = binary.BigEndian.AppendUint16(b, uint16(len(d.NodeID)))
	b = append(b, d.NodeID...)
	b = binary.BigEndian.AppendUint16(b, uint16(len(d.KeyID)))
	b = append(b, d.KeyID...)
	return b, nil
}

// Verify checks d's signature against a trusted key set indexed by key id.
func (d Digest) Verify(keys Keyring) error {
	if d.Version != Version1 && d.Version != Version2 {
		return fmt.Errorf("ledger digest: unsupported version %d", d.Version)
	}
	pub, ok := keys[d.KeyID]
	if !ok {
		return fmt.Errorf("%w: key id %q", ErrUnknownKey, d.KeyID)
	}
	msg, err := d.CanonicalBytes()
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, msg, d.Sig) {
		return ErrBadSignature
	}
	return nil
}

// KeyID derives a key id from an Ed25519 public key: the first 8 bytes of
// its SHA-256, in hex.
func KeyID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:8])
}

// Keyring maps key ids to trusted public keys.
type Keyring map[string]ed25519.PublicKey

// HasTree reports whether d signs a log tree root.
func (d Digest) HasTree() bool { return d.Version >= Version2 }

// Add trusts pub under its derived key id.
func (k Keyring) Add(pub ed25519.PublicKey) { k[KeyID(pub)] = pub }

// digestJSON is the transport form: what GET /api/v1/ledger/digest returns,
// what the sidecar log stores, and what verify-ledger --digest reads.
type digestJSON struct {
	Version   uint8  `json:"version"`
	LSN       uint64 `json:"lsn"`
	Chain     string `json:"chain"`
	Term      uint64 `json:"term"`
	Timestamp uint64 `json:"timestamp"`
	TreeRoot  string `json:"tree_root,omitempty"`
	NodeID    string `json:"node_id"`
	KeyID     string `json:"key_id"`
	SignedAt  string `json:"signed_at"`
	Sig       string `json:"sig"`
}

// MarshalJSON encodes the chain in hex, the signature in base64 and
// SignedAt as RFC 3339 with nanoseconds, so it round-trips exactly.
func (d Digest) MarshalJSON() ([]byte, error) {
	var treeRoot string
	if d.HasTree() {
		treeRoot = hex.EncodeToString(d.TreeRoot[:])
	}
	return json.Marshal(digestJSON{
		Version:   d.Version,
		LSN:       d.LSN,
		Chain:     hex.EncodeToString(d.Chain[:]),
		Term:      d.Term,
		Timestamp: d.Timestamp,
		TreeRoot:  treeRoot,
		NodeID:    d.NodeID,
		KeyID:     d.KeyID,
		SignedAt:  d.SignedAt.UTC().Format(time.RFC3339Nano),
		Sig:       base64.StdEncoding.EncodeToString(d.Sig),
	})
}

// UnmarshalJSON is the inverse of MarshalJSON.
func (d *Digest) UnmarshalJSON(data []byte) error {
	var j digestJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	chain, err := hex.DecodeString(j.Chain)
	if err != nil || len(chain) != 32 {
		return fmt.Errorf("ledger digest: chain must be 32 bytes of hex")
	}
	signedAt, err := time.Parse(time.RFC3339Nano, j.SignedAt)
	if err != nil {
		return fmt.Errorf("ledger digest: signed_at: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(j.Sig)
	if err != nil {
		return fmt.Errorf("ledger digest: sig: %w", err)
	}
	var treeRoot []byte
	if j.Version >= Version2 {
		if treeRoot, err = hex.DecodeString(j.TreeRoot); err != nil || len(treeRoot) != 32 {
			return fmt.Errorf("ledger digest: tree_root must be 32 bytes of hex")
		}
	}
	*d = Digest{
		Version:   j.Version,
		LSN:       j.LSN,
		Term:      j.Term,
		Timestamp: j.Timestamp,
		NodeID:    j.NodeID,
		KeyID:     j.KeyID,
		SignedAt:  signedAt,
		Sig:       sig,
	}
	copy(d.Chain[:], chain)
	copy(d.TreeRoot[:], treeRoot)
	return nil
}

// Sign fills in d's key id and signs it. A zero version means version 1;
// version 2 also signs d.TreeRoot.
func Sign(signer ports.LedgerSigner, d Digest) (Digest, error) {
	if d.Version == 0 {
		d.Version = Version1
	}
	d.KeyID = signer.KeyID()
	msg, err := d.CanonicalBytes()
	if err != nil {
		return Digest{}, err
	}
	sig, err := signer.Sign(msg)
	if err != nil {
		return Digest{}, fmt.Errorf("ledger digest: sign: %w", err)
	}
	d.Sig = sig
	return d, nil
}
