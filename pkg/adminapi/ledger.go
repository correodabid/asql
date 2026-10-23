package adminapi

import (
	"encoding/json"
	"time"
)

// ─── Ledger types (ADR 0005, 0006, 0008) ────────────────────────────────────
//
// These are the JSON transport forms of the ledger's signed objects: what
// GET /api/v1/ledger/digest returns, what asqlctl reads and writes, and
// what each JSON column of asql_admin.prove_consistency and
// asql_admin.prove_rows holds. Hashes are hex, signatures base64.
//
// Values in proofs and attested results are typed JSON cells: null, or
// [kind, value] with kind "i" (int, as a decimal string), "f" (float),
// "s" (text), "b" (boolean), "t" (timestamp, Unix µs as a decimal string)
// or "j" (JSON text as stored). They are kept as raw JSON so a value
// survives a round trip exactly.

// LedgerDigest is a signed checkpoint of the WAL. Version 2 adds TreeRoot,
// the root of the log tree of LSNs 1..LSN; version 3 adds StateRoot, the
// root of the state tree at LSN. Earlier versions leave them empty.
type LedgerDigest struct {
	Version   uint8     `json:"version"`
	LSN       uint64    `json:"lsn"`
	Chain     string    `json:"chain"`
	Term      uint64    `json:"term"`
	Timestamp uint64    `json:"timestamp"`
	TreeRoot  string    `json:"tree_root,omitempty"`
	StateRoot string    `json:"state_root,omitempty"`
	NodeID    string    `json:"node_id"`
	KeyID     string    `json:"key_id"`
	SignedAt  time.Time `json:"signed_at"`
	Sig       string    `json:"sig"`
}

// ConsistencyProof proves that the log signed by To extends the log signed
// by From (asql_admin.prove_consistency, asqlctl prove-consistency).
type ConsistencyProof struct {
	From  LedgerDigest `json:"from_digest"`
	To    LedgerDigest `json:"to_digest"`
	Proof []string     `json:"proof"`
}

// RowsProof proves that the state signed by Digest, a version 3 digest,
// held Rows, and that each of Groups is complete among them
// (asql_admin.prove_rows, asqlctl prove-rows). It reveals nothing about
// other rows.
type RowsProof struct {
	Digest LedgerDigest  `json:"digest"`
	Rows   []ProvenRow   `json:"rows"`
	Groups []ProvenGroup `json:"groups,omitempty"`
	Proof  StateProof    `json:"proof"`
}

// ProvenRow is one row of a RowsProof, cited by its primary key in key
// order. Row holds every column, the engine's _lsn included.
type ProvenRow struct {
	Domain            string                     `json:"domain"`
	Table             string                     `json:"table"`
	PrimaryKeyColumns []string                   `json:"primary_key_columns"`
	PrimaryKey        []json.RawMessage          `json:"primary_key"`
	Row               map[string]json.RawMessage `json:"row"`
	Salt              string                     `json:"salt"`
}

// ProvenGroup is a verifiable group a RowsProof shows complete: every row
// of Table whose Columns equal Values is among the proof's rows, and no
// other one exists.
type ProvenGroup struct {
	Domain     string            `json:"domain"`
	Table      string            `json:"table"`
	Columns    []string          `json:"columns"`
	References string            `json:"references"`
	Values     []json.RawMessage `json:"values"`
}

// StateProof is the batch proof of a RowsProof's leaves in the state tree.
type StateProof struct {
	Depths   []uint16 `json:"depths"`
	Siblings []string `json:"siblings"`
}

// ResultAttestation is a node's signature over a query's result at an LSN
// (asql_admin.attest_query, whose columns are named as these fields).
type ResultAttestation struct {
	Version       uint8     `json:"version"`
	LSN           uint64    `json:"lsn"`
	Chain         string    `json:"chain"`
	Term          uint64    `json:"term"`
	Timestamp     uint64    `json:"timestamp"`
	Domains       []string  `json:"domains"`
	SQL           string    `json:"sql"`
	ResultHash    string    `json:"result_hash"`
	RowCount      uint64    `json:"row_count"`
	Columns       []string  `json:"columns"`
	EngineVersion string    `json:"engine_version"`
	NodeID        string    `json:"node_id"`
	KeyID         string    `json:"key_id"`
	SignedAt      time.Time `json:"signed_at"`
	Sig           string    `json:"sig"`
}

// AttestedResult is an attestation with the rows it signs, as
// asqlctl attest-query writes it and verify-result reads it.
type AttestedResult struct {
	Attestation ResultAttestation   `json:"attestation"`
	Rows        [][]json.RawMessage `json:"rows"`
}
