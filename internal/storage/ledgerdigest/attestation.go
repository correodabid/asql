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
	"hash"
	"math"
	"strconv"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/ports"
)

// Result attestations (ADR 0006): a signed statement that a SELECT over the
// history ending at a committed chain value returned a result, identified by
// its hash. They use the node's ledger key under their own signing domain,
// so a digest signature can never pass for an attestation or the reverse.

// AttestationVersion is the attestation format version.
const AttestationVersion = 1

const (
	resultDomain      = "asql-result-v1"
	attestationDomain = "asql-result-attestation-v1"
)

// Value kinds of the canonical result encoding.
const (
	resultNull      byte = 0
	resultInt       byte = 1
	resultFloat     byte = 2
	resultText      byte = 3
	resultBool      byte = 4
	resultTimestamp byte = 5
	resultJSON      byte = 6
)

// ErrResultMismatch reports rows that do not match an attestation.
var ErrResultMismatch = errors.New("result attestation: rows do not match")

// HashResult returns the SHA-256 of the canonical encoding of a result:
//
//	"asql-result-v1" 0x00 || u32 column count || each column (u32 length, bytes)
//	|| u64 row count || each row: each value in column order
//
// A value is a kind byte followed by i64 for int and timestamp (Unix µs),
// IEEE-754 bits for float, one byte for bool, nothing for null, and a u32
// length and the bytes for text and JSON. Integers are big-endian.
func HashResult(columns []string, rows [][]ast.Literal) ([32]byte, error) {
	h := sha256.New()
	if err := encodeResult(h, columns, rows); err != nil {
		return [32]byte{}, err
	}
	var sum [32]byte
	h.Sum(sum[:0])
	return sum, nil
}

func encodeResult(h hash.Hash, columns []string, rows [][]ast.Literal) error {
	var b []byte
	b = append(b, resultDomain...)
	b = append(b, 0)
	b = binary.BigEndian.AppendUint32(b, uint32(len(columns)))
	for _, column := range columns {
		b = appendString32(b, column)
	}
	b = binary.BigEndian.AppendUint64(b, uint64(len(rows)))
	h.Write(b)
	for i, row := range rows {
		if len(row) != len(columns) {
			return fmt.Errorf("result attestation: row %d has %d values for %d columns", i, len(row), len(columns))
		}
		b = b[:0]
		for j, value := range row {
			var err error
			if b, err = appendResultValue(b, value); err != nil {
				return fmt.Errorf("result attestation: row %d column %s: %w", i, columns[j], err)
			}
		}
		h.Write(b)
	}
	return nil
}

// AppendValue appends value's canonical encoding (the one HashResult uses)
// to b. The state tree of ADR 0008 encodes keys and rows with it.
func AppendValue(b []byte, value ast.Literal) ([]byte, error) {
	return appendResultValue(b, value)
}

func appendResultValue(b []byte, value ast.Literal) ([]byte, error) {
	switch value.Kind {
	case ast.LiteralNull, "":
		return append(b, resultNull), nil
	case ast.LiteralNumber:
		return binary.BigEndian.AppendUint64(append(b, resultInt), uint64(value.NumberValue)), nil
	case ast.LiteralFloat:
		return binary.BigEndian.AppendUint64(append(b, resultFloat), math.Float64bits(value.FloatValue)), nil
	case ast.LiteralString:
		return appendString32(append(b, resultText), value.StringValue), nil
	case ast.LiteralBoolean:
		v := byte(0)
		if value.BoolValue {
			v = 1
		}
		return append(b, resultBool, v), nil
	case ast.LiteralTimestamp:
		return binary.BigEndian.AppendUint64(append(b, resultTimestamp), uint64(value.NumberValue)), nil
	case ast.LiteralJSON:
		return appendString32(append(b, resultJSON), value.StringValue), nil
	default:
		return nil, fmt.Errorf("unsupported value kind %q", value.Kind)
	}
}

func appendString32(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

// Attestation is a signed statement that SQL, run in Domains over the
// history ending at Chain (the chain value at LSN), returned the result
// whose hash is ResultHash. The rows travel next to it, not inside it.
type Attestation struct {
	Version uint8
	LSN     uint64
	Chain   [32]byte
	// Term and Timestamp are those of the WAL record at LSN, as in a digest.
	Term      uint64
	Timestamp uint64
	// Domains are sorted.
	Domains    []string
	SQL        string
	ResultHash [32]byte
	RowCount   uint64
	Columns    []string
	// EngineVersion is signed because another version is not guaranteed to
	// return the same rows: verifiers report a mismatch, not a failure.
	EngineVersion string
	NodeID        string
	KeyID         string
	// SignedAt is the signer's wall clock, asserted by the node only.
	SignedAt time.Time
	Sig      []byte
}

// CanonicalBytes returns the exact bytes that are signed:
//
//	"asql-result-attestation-v1" 0x00 || version || u64 lsn || chain (32)
//	|| u64 term || u64 timestamp || domains || sql || result hash (32)
//	|| u64 row count || columns || engine version || node id || key id
//	|| u64 signed_at (Unix ns)
//
// Strings are a u32 length and the bytes; lists a u32 count and their
// strings. Integers are big-endian.
func (a Attestation) CanonicalBytes() ([]byte, error) {
	if len(a.SQL) > math.MaxUint32 {
		return nil, errors.New("result attestation: sql too long")
	}
	b := make([]byte, 0, 256+len(a.SQL))
	b = append(b, attestationDomain...)
	b = append(b, 0)
	b = append(b, a.Version)
	b = binary.BigEndian.AppendUint64(b, a.LSN)
	b = append(b, a.Chain[:]...)
	b = binary.BigEndian.AppendUint64(b, a.Term)
	b = binary.BigEndian.AppendUint64(b, a.Timestamp)
	b = appendList32(b, a.Domains)
	b = appendString32(b, a.SQL)
	b = append(b, a.ResultHash[:]...)
	b = binary.BigEndian.AppendUint64(b, a.RowCount)
	b = appendList32(b, a.Columns)
	b = appendString32(b, a.EngineVersion)
	b = appendString32(b, a.NodeID)
	b = appendString32(b, a.KeyID)
	b = binary.BigEndian.AppendUint64(b, uint64(a.SignedAt.UnixNano()))
	return b, nil
}

func appendList32(b []byte, list []string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(list)))
	for _, s := range list {
		b = appendString32(b, s)
	}
	return b
}

// SignAttestation fills in a's version and key id and signs it.
func SignAttestation(signer ports.LedgerSigner, a Attestation) (Attestation, error) {
	a.Version = AttestationVersion
	a.KeyID = signer.KeyID()
	msg, err := a.CanonicalBytes()
	if err != nil {
		return Attestation{}, err
	}
	sig, err := signer.Sign(msg)
	if err != nil {
		return Attestation{}, fmt.Errorf("result attestation: sign: %w", err)
	}
	a.Sig = sig
	return a, nil
}

// Verify checks a's signature against a trusted key set.
func (a Attestation) Verify(keys Keyring) error {
	if a.Version != AttestationVersion {
		return fmt.Errorf("result attestation: unsupported version %d", a.Version)
	}
	pub, ok := keys[a.KeyID]
	if !ok {
		return fmt.Errorf("%w: key id %q", ErrUnknownKey, a.KeyID)
	}
	msg, err := a.CanonicalBytes()
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, msg, a.Sig) {
		return ErrBadSignature
	}
	return nil
}

// CheckRows checks rows against the shape and hash a attests.
func (a Attestation) CheckRows(rows [][]ast.Literal) error {
	if uint64(len(rows)) != a.RowCount {
		return fmt.Errorf("%w: %d rows, the attestation has %d", ErrResultMismatch, len(rows), a.RowCount)
	}
	sum, err := HashResult(a.Columns, rows)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrResultMismatch, err)
	}
	if sum != a.ResultHash {
		return fmt.Errorf("%w: result hash %x, the attestation has %x", ErrResultMismatch, sum, a.ResultHash)
	}
	return nil
}

type attestationJSON struct {
	Version       uint8    `json:"version"`
	LSN           uint64   `json:"lsn"`
	Chain         string   `json:"chain"`
	Term          uint64   `json:"term"`
	Timestamp     uint64   `json:"timestamp"`
	Domains       []string `json:"domains"`
	SQL           string   `json:"sql"`
	ResultHash    string   `json:"result_hash"`
	RowCount      uint64   `json:"row_count"`
	Columns       []string `json:"columns"`
	EngineVersion string   `json:"engine_version"`
	NodeID        string   `json:"node_id"`
	KeyID         string   `json:"key_id"`
	SignedAt      string   `json:"signed_at"`
	Sig           string   `json:"sig"`
}

// MarshalJSON encodes hashes in hex, the signature in base64 and SignedAt
// as RFC 3339 with nanoseconds, as digests do.
func (a Attestation) MarshalJSON() ([]byte, error) {
	return json.Marshal(attestationJSON{
		Version:       a.Version,
		LSN:           a.LSN,
		Chain:         hex.EncodeToString(a.Chain[:]),
		Term:          a.Term,
		Timestamp:     a.Timestamp,
		Domains:       nonNil(a.Domains),
		SQL:           a.SQL,
		ResultHash:    hex.EncodeToString(a.ResultHash[:]),
		RowCount:      a.RowCount,
		Columns:       nonNil(a.Columns),
		EngineVersion: a.EngineVersion,
		NodeID:        a.NodeID,
		KeyID:         a.KeyID,
		SignedAt:      a.SignedAt.UTC().Format(time.RFC3339Nano),
		Sig:           base64.StdEncoding.EncodeToString(a.Sig),
	})
}

func nonNil(list []string) []string {
	if list == nil {
		return []string{}
	}
	return list
}

// UnmarshalJSON is the inverse of MarshalJSON.
func (a *Attestation) UnmarshalJSON(data []byte) error {
	var j attestationJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	chain, err := decodeHash32(j.Chain)
	if err != nil {
		return fmt.Errorf("result attestation: chain: %w", err)
	}
	resultHash, err := decodeHash32(j.ResultHash)
	if err != nil {
		return fmt.Errorf("result attestation: result_hash: %w", err)
	}
	signedAt, err := time.Parse(time.RFC3339Nano, j.SignedAt)
	if err != nil {
		return fmt.Errorf("result attestation: signed_at: %w", err)
	}
	sig, err := base64.StdEncoding.DecodeString(j.Sig)
	if err != nil {
		return fmt.Errorf("result attestation: sig: %w", err)
	}
	*a = Attestation{
		Version:       j.Version,
		LSN:           j.LSN,
		Chain:         chain,
		Term:          j.Term,
		Timestamp:     j.Timestamp,
		Domains:       j.Domains,
		SQL:           j.SQL,
		ResultHash:    resultHash,
		RowCount:      j.RowCount,
		Columns:       j.Columns,
		EngineVersion: j.EngineVersion,
		NodeID:        j.NodeID,
		KeyID:         j.KeyID,
		SignedAt:      signedAt,
		Sig:           sig,
	}
	return nil
}

func decodeHash32(s string) ([32]byte, error) {
	var h [32]byte
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != 32 {
		return h, errors.New("must be 32 bytes of hex")
	}
	copy(h[:], raw)
	return h, nil
}

// Rows are the rows of an attested result in their JSON transport form: an
// array of arrays of typed cells, so the result hash can be recomputed from
// them exactly. A cell is null or [kind, value]: ["i", "42"] (int, as a
// decimal string so 64 bits survive JSON readers), ["f", "1.5"] (float,
// shortest representation that parses back to the same bits), ["s", "text"],
// ["b", true], ["t", "1710498645000000"] (timestamp, Unix µs) and
// ["j", "{\"a\":1}"] (JSON text as stored).
type Rows [][]ast.Literal

// MarshalJSON implements json.Marshaler.
func (rows Rows) MarshalJSON() ([]byte, error) {
	out := make([][]any, len(rows))
	for i, row := range rows {
		cells := make([]any, len(row))
		for j, value := range row {
			cell, err := marshalCell(value)
			if err != nil {
				return nil, fmt.Errorf("result rows: row %d value %d: %w", i, j, err)
			}
			cells[j] = cell
		}
		out[i] = cells
	}
	return json.Marshal(out)
}

// MarshalValue returns value's typed JSON form: null, or [kind, value]
// with kind i, f, s, b, t or j. Row proofs (ADR 0008) use it too.
func MarshalValue(value ast.Literal) (any, error) { return marshalCell(value) }

// UnmarshalValue is the inverse of MarshalValue.
func UnmarshalValue(cell json.RawMessage) (ast.Literal, error) { return unmarshalCell(cell) }

func marshalCell(value ast.Literal) (any, error) {
	switch value.Kind {
	case ast.LiteralNull, "":
		return nil, nil
	case ast.LiteralNumber:
		return []any{"i", strconv.FormatInt(value.NumberValue, 10)}, nil
	case ast.LiteralFloat:
		return []any{"f", strconv.FormatFloat(value.FloatValue, 'g', -1, 64)}, nil
	case ast.LiteralString:
		return []any{"s", value.StringValue}, nil
	case ast.LiteralBoolean:
		return []any{"b", value.BoolValue}, nil
	case ast.LiteralTimestamp:
		return []any{"t", strconv.FormatInt(value.NumberValue, 10)}, nil
	case ast.LiteralJSON:
		return []any{"j", value.StringValue}, nil
	default:
		return nil, fmt.Errorf("unsupported value kind %q", value.Kind)
	}
}

// UnmarshalJSON implements json.Unmarshaler.
func (rows *Rows) UnmarshalJSON(data []byte) error {
	var raw [][]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("result rows: %w", err)
	}
	out := make(Rows, len(raw))
	for i, cells := range raw {
		row := make([]ast.Literal, len(cells))
		for j, cell := range cells {
			value, err := unmarshalCell(cell)
			if err != nil {
				return fmt.Errorf("result rows: row %d value %d: %w", i, j, err)
			}
			row[j] = value
		}
		out[i] = row
	}
	*rows = out
	return nil
}

func unmarshalCell(cell json.RawMessage) (ast.Literal, error) {
	if string(cell) == "null" {
		return ast.Literal{Kind: ast.LiteralNull}, nil
	}
	var pair []json.RawMessage
	if err := json.Unmarshal(cell, &pair); err != nil || len(pair) != 2 {
		return ast.Literal{}, errors.New("a cell is null or [kind, value]")
	}
	var kind string
	if err := json.Unmarshal(pair[0], &kind); err != nil {
		return ast.Literal{}, errors.New("cell kind must be a string")
	}
	if kind == "b" {
		var v bool
		if err := json.Unmarshal(pair[1], &v); err != nil {
			return ast.Literal{}, errors.New("bool cell value must be true or false")
		}
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: v}, nil
	}
	var text string
	if err := json.Unmarshal(pair[1], &text); err != nil {
		return ast.Literal{}, fmt.Errorf("%s cell value must be a string", kind)
	}
	switch kind {
	case "i", "t":
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return ast.Literal{}, fmt.Errorf("%s cell value: %w", kind, err)
		}
		if kind == "t" {
			return ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: n}, nil
		}
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: n}, nil
	case "f":
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return ast.Literal{}, fmt.Errorf("f cell value: %w", err)
		}
		return ast.Literal{Kind: ast.LiteralFloat, FloatValue: f}, nil
	case "s":
		return ast.Literal{Kind: ast.LiteralString, StringValue: text}, nil
	case "j":
		return ast.Literal{Kind: ast.LiteralJSON, StringValue: text}, nil
	default:
		return ast.Literal{}, fmt.Errorf("unknown cell kind %q", kind)
	}
}

// AttestedResult is an attestation with its rows: what asqlctl attest-query
// writes and verify-result reads.
type AttestedResult struct {
	Attestation Attestation `json:"attestation"`
	Rows        Rows        `json:"rows"`
}
