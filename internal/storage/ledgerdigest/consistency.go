package ledgerdigest

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/correodabid/asql/internal/storage/logtree"
)

// ErrNoTree reports a version 1 digest where a log tree root is needed.
var ErrNoTree = errors.New("ledger digest: version 1 digests sign no log tree root")

// ConsistencyProof shows that the log signed by To extends the log signed by
// From unchanged (ADR 0008): the RFC 6962 consistency proof between their
// tree roots, which reveals no record. It carries both digests, so it is
// checked with the trusted keys alone.
type ConsistencyProof struct {
	From  Digest         `json:"from_digest"`
	To    Digest         `json:"to_digest"`
	Proof []logtree.Hash `json:"-"`
}

type consistencyProofJSON struct {
	From  Digest   `json:"from_digest"`
	To    Digest   `json:"to_digest"`
	Proof []string `json:"proof"`
}

// MarshalJSON encodes the proof hashes in hex.
func (p ConsistencyProof) MarshalJSON() ([]byte, error) {
	return json.Marshal(consistencyProofJSON{From: p.From, To: p.To, Proof: EncodeHashes(p.Proof)})
}

// UnmarshalJSON is the inverse of MarshalJSON.
func (p *ConsistencyProof) UnmarshalJSON(data []byte) error {
	var j consistencyProofJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	proof, err := DecodeHashes(j.Proof)
	if err != nil {
		return fmt.Errorf("consistency proof: %w", err)
	}
	*p = ConsistencyProof{From: j.From, To: j.To, Proof: proof}
	return nil
}

// Verify checks both digests' signatures against keys and the proof
// between their tree roots.
func (p ConsistencyProof) Verify(keys Keyring) error {
	for _, d := range []Digest{p.From, p.To} {
		if err := d.Verify(keys); err != nil {
			return fmt.Errorf("digest at LSN %d: %w", d.LSN, err)
		}
		if !d.HasTree() {
			return fmt.Errorf("digest at LSN %d: %w", d.LSN, ErrNoTree)
		}
	}
	if p.From.LSN > p.To.LSN {
		return fmt.Errorf("consistency proof: from LSN %d is after to LSN %d", p.From.LSN, p.To.LSN)
	}
	return logtree.VerifyConsistency(p.Proof, int64(p.From.LSN), logtree.Hash(p.From.TreeRoot), int64(p.To.LSN), logtree.Hash(p.To.TreeRoot))
}

// EncodeHashes returns hashes in hex.
func EncodeHashes(hashes []logtree.Hash) []string {
	out := make([]string, len(hashes))
	for i, h := range hashes {
		out[i] = hex.EncodeToString(h[:])
	}
	return out
}

// DecodeHashes is the inverse of EncodeHashes.
func DecodeHashes(encoded []string) ([]logtree.Hash, error) {
	out := make([]logtree.Hash, len(encoded))
	for i, s := range encoded {
		b, err := hex.DecodeString(s)
		if err != nil || len(b) != len(out[i]) {
			return nil, fmt.Errorf("hash %d must be 32 bytes of hex", i)
		}
		copy(out[i][:], b)
	}
	return out, nil
}
