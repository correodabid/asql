package ledgerdigest

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

var vectorRows = [][]ast.Literal{
	{{Kind: ast.LiteralNumber, NumberValue: 1}, {Kind: ast.LiteralString, StringValue: "alpha"}, {Kind: ast.LiteralFloat, FloatValue: 0.5}, {Kind: ast.LiteralBoolean, BoolValue: true}},
	{{Kind: ast.LiteralNumber, NumberValue: -7}, {Kind: ast.LiteralNull}, {Kind: ast.LiteralTimestamp, NumberValue: 1710498645000000}, {Kind: ast.LiteralJSON, StringValue: `{"a":1}`}},
}

var vectorColumns = []string{"id", "name", "value", "flag"}

func vectorSigner() *Ed25519Signer {
	return NewEd25519Signer(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize)))
}

func vectorAttestation(t *testing.T) Attestation {
	t.Helper()
	sum, err := HashResult(vectorColumns, vectorRows)
	if err != nil {
		t.Fatal(err)
	}
	a, err := SignAttestation(vectorSigner(), Attestation{
		LSN:           42,
		Chain:         [32]byte{1, 2, 3},
		Term:          3,
		Timestamp:     40,
		Domains:       []string{"lab", "shop"},
		SQL:           "SELECT id, name FROM lab.artifacts",
		ResultHash:    sum,
		RowCount:      uint64(len(vectorRows)),
		Columns:       vectorColumns,
		EngineVersion: "v1.2.3",
		NodeID:        "node-a",
		SignedAt:      time.Date(2026, 10, 20, 12, 0, 0, 123456789, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestAttestationFormatVectors pins the result encoding, the signed bytes
// and the signature: changing any of them breaks every attestation already
// issued.
func TestAttestationFormatVectors(t *testing.T) {
	sum, err := HashResult(vectorColumns, vectorRows)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hex.EncodeToString(sum[:]), "d1826c8bce7e9c4b5e9e67ca7a037c2ec5e19a8aff16c749b230406ecf89dd8b"; got != want {
		t.Fatalf("result hash vector changed:\n got %s\nwant %s", got, want)
	}
	a := vectorAttestation(t)
	msg, err := a.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(msg)
	if got, want := hex.EncodeToString(digest[:]), "cf6f833cd4c4218e60a4c55d1a562b208d5e7a124e648185eee92f4a4acba831"; got != want {
		t.Fatalf("signed bytes vector changed:\n got %s\nwant %s", got, want)
	}
	if got, want := hex.EncodeToString(a.Sig), "6c35209a307e29fc7202122b7ba4aae6a1c2ec460954c53ee2e78f11260f62e6c333e986f0ea606aa4dbd2307edd4e56fb3d88030885e82ec6878d41e0423409"; got != want {
		t.Fatalf("signature vector changed:\n got %s\nwant %s", got, want)
	}
}

func TestAttestationSignVerifyAndJSONRoundTrip(t *testing.T) {
	keys := Keyring{}
	keys.Add(vectorSigner().PublicKey())
	a := vectorAttestation(t)
	if err := a.Verify(keys); err != nil {
		t.Fatal(err)
	}
	if err := a.CheckRows(vectorRows); err != nil {
		t.Fatal(err)
	}

	data, err := json.Marshal(AttestedResult{Attestation: a, Rows: vectorRows})
	if err != nil {
		t.Fatal(err)
	}
	var back AttestedResult
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if err := back.Attestation.Verify(keys); err != nil {
		t.Fatalf("verify after JSON round trip: %v\n%s", err, data)
	}
	if err := back.Attestation.CheckRows(back.Rows); err != nil {
		t.Fatalf("rows after JSON round trip: %v\n%s", err, data)
	}
}

func TestAttestationVerifyRejectsAnyChange(t *testing.T) {
	keys := Keyring{}
	keys.Add(vectorSigner().PublicKey())
	for name, mutate := range map[string]func(*Attestation){
		"lsn":            func(a *Attestation) { a.LSN++ },
		"chain":          func(a *Attestation) { a.Chain[31] ^= 1 },
		"term":           func(a *Attestation) { a.Term++ },
		"timestamp":      func(a *Attestation) { a.Timestamp++ },
		"domains":        func(a *Attestation) { a.Domains = []string{"lab"} },
		"sql":            func(a *Attestation) { a.SQL += " WHERE id = 1" },
		"result_hash":    func(a *Attestation) { a.ResultHash[0] ^= 1 },
		"row_count":      func(a *Attestation) { a.RowCount++ },
		"columns":        func(a *Attestation) { a.Columns = []string{"id", "name", "value", "other"} },
		"engine_version": func(a *Attestation) { a.EngineVersion = "v1.2.4" },
		"node_id":        func(a *Attestation) { a.NodeID = "node-b" },
		"signed_at":      func(a *Attestation) { a.SignedAt = a.SignedAt.Add(time.Nanosecond) },
		"sig":            func(a *Attestation) { a.Sig[0] ^= 1 },
	} {
		a := vectorAttestation(t)
		a.Columns = append([]string(nil), a.Columns...)
		mutate(&a)
		if err := a.Verify(keys); !errors.Is(err, ErrBadSignature) {
			t.Fatalf("%s changed: verify = %v, want ErrBadSignature", name, err)
		}
	}
	if err := vectorAttestation(t).Verify(Keyring{}); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("untrusted key: %v", err)
	}
}

// TestDigestAndAttestationSignaturesDoNotCross: both are signed with the
// ledger key, under different domains.
func TestDigestAndAttestationSignaturesDoNotCross(t *testing.T) {
	signer := vectorSigner()
	keys := Keyring{}
	keys.Add(signer.PublicKey())
	a := vectorAttestation(t)
	d, err := Sign(signer, Digest{LSN: a.LSN, Chain: a.Chain, Term: a.Term, Timestamp: a.Timestamp, NodeID: a.NodeID, SignedAt: a.SignedAt})
	if err != nil {
		t.Fatal(err)
	}
	a.Sig = d.Sig
	if err := a.Verify(keys); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("a digest signature passed as an attestation: %v", err)
	}
}

// TestCheckRowsTellsTypesApart: the hash covers each value's kind, so 1,
// 1.0 and '1' are different results.
func TestCheckRowsTellsTypesApart(t *testing.T) {
	a := vectorAttestation(t)
	for name, mutate := range map[string]func([][]ast.Literal){
		"value":         func(r [][]ast.Literal) { r[0][0].NumberValue = 2 },
		"int to float":  func(r [][]ast.Literal) { r[0][0] = ast.Literal{Kind: ast.LiteralFloat, FloatValue: 1} },
		"int to text":   func(r [][]ast.Literal) { r[0][0] = ast.Literal{Kind: ast.LiteralString, StringValue: "1"} },
		"ts to int":     func(r [][]ast.Literal) { r[1][2].Kind = ast.LiteralNumber },
		"null to text":  func(r [][]ast.Literal) { r[1][1] = ast.Literal{Kind: ast.LiteralString} },
		"json to text":  func(r [][]ast.Literal) { r[1][3].Kind = ast.LiteralString },
		"bool":          func(r [][]ast.Literal) { r[0][3].BoolValue = false },
		"row order":     func(r [][]ast.Literal) { r[0], r[1] = r[1], r[0] },
		"text boundary": func(r [][]ast.Literal) { r[0][1].StringValue = "alph" },
	} {
		rows := make([][]ast.Literal, len(vectorRows))
		for i := range vectorRows {
			rows[i] = append([]ast.Literal(nil), vectorRows[i]...)
		}
		mutate(rows)
		if err := a.CheckRows(rows); !errors.Is(err, ErrResultMismatch) {
			t.Fatalf("%s: CheckRows = %v, want ErrResultMismatch", name, err)
		}
	}
	if err := a.CheckRows(vectorRows[:1]); !errors.Is(err, ErrResultMismatch) {
		t.Fatalf("missing row: %v", err)
	}
	if _, err := HashResult([]string{"a"}, [][]ast.Literal{{}}); err == nil {
		t.Fatal("row shorter than the columns: no error")
	}
}

// TestRowsJSONKeepsEveryValueExact: the transport form round-trips values
// a plain JSON number would not.
func TestRowsJSONKeepsEveryValueExact(t *testing.T) {
	rows := Rows{{
		{Kind: ast.LiteralNumber, NumberValue: math.MaxInt64},
		{Kind: ast.LiteralNumber, NumberValue: math.MinInt64},
		{Kind: ast.LiteralFloat, FloatValue: math.Copysign(0, -1)},
		{Kind: ast.LiteralFloat, FloatValue: 0.1},
		{Kind: ast.LiteralFloat, FloatValue: math.Inf(1)},
		{Kind: ast.LiteralString, StringValue: "ñ \"quoted\"\n"},
		{Kind: ast.LiteralNull},
	}}
	columns := []string{"a", "b", "c", "d", "e", "f", "g"}
	want, err := HashResult(columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	var back Rows
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if got, err := HashResult(columns, back); err != nil || got != want {
		t.Fatalf("hash after JSON round trip changed (err %v): %s", err, data)
	}
	for _, bad := range []string{`[[1]]`, `[[["x","1"]]]`, `[[["i","1.5"]]]`, `[[["b","true"]]]`, `[[["i"]]]`} {
		if err := json.Unmarshal([]byte(bad), &back); err == nil {
			t.Fatalf("%s: no error", bad)
		}
	}
}
