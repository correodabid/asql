package adminapi

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/logtree"
	"github.com/correodabid/asql/internal/storage/statetree"
)

// TestLedgerTypesMatchTransportForm: the public ledger types decode what
// the server encodes and encode it back unchanged, so a client can read a
// digest, a proof or an attested result and hand it on to asqlctl.
func TestLedgerTypesMatchTransportForm(t *testing.T) {
	signedAt := time.Date(2026, 10, 23, 6, 0, 0, 123456789, time.UTC)
	digest := func(lsn uint64, b byte) ledgerdigest.Digest {
		return ledgerdigest.Digest{
			Version: ledgerdigest.Version3, LSN: lsn, Chain: [32]byte{b}, Term: 2, Timestamp: lsn * 10,
			TreeRoot: [32]byte{b, 1}, StateRoot: [32]byte{b, 2}, NodeID: "node-a", KeyID: "k1",
			SignedAt: signedAt, Sig: []byte{b, 9, 9},
		}
	}
	text := ast.Literal{Kind: ast.LiteralString, StringValue: "X"}
	cases := []struct {
		name   string
		server any
		client any
	}{
		{"digest v3", digest(7, 1), &LedgerDigest{}},
		{"digest v1", ledgerdigest.Digest{Version: ledgerdigest.Version1, LSN: 3, Chain: [32]byte{3}, SignedAt: signedAt, Sig: []byte{1}}, &LedgerDigest{}},
		{"consistency proof", ledgerdigest.ConsistencyProof{From: digest(7, 1), To: digest(9, 2), Proof: []logtree.Hash{{4}, {5}}}, &ConsistencyProof{}},
		{"rows proof", statetree.RowsProof{
			Digest: digest(9, 2),
			Rows: []statetree.ProvenRow{{
				Domain: "lab", Table: "artifacts", PrimaryKeyColumns: []string{"id"}, PrimaryKey: []ast.Literal{text},
				Row: map[string]ast.Literal{
					"id":    text,
					"n":     {Kind: ast.LiteralNumber, NumberValue: 1 << 60},
					"note":  {Kind: ast.LiteralNull},
					"_lsn":  {Kind: ast.LiteralNumber, NumberValue: 9},
					"ok":    {Kind: ast.LiteralBoolean, BoolValue: true},
					"ratio": {Kind: ast.LiteralFloat, FloatValue: 0.1},
				},
				Salt: statetree.Hash{6},
			}},
			Groups: []statetree.ProvenGroup{{Domain: "lab", Table: "artifact_parents", Columns: []string{"child_id"}, RefTable: "artifacts", Values: []ast.Literal{text}}},
			Proof:  statetree.Proof{Depths: []uint16{3, 1}, Siblings: []statetree.Hash{{7}, {8}}},
		}, &RowsProof{}},
		{"attested result", ledgerdigest.AttestedResult{
			Attestation: ledgerdigest.Attestation{
				Version: 1, LSN: 9, Chain: [32]byte{2}, Term: 2, Timestamp: 90, Domains: []string{"lab"},
				SQL: "SELECT id FROM artifacts", ResultHash: [32]byte{3}, RowCount: 1, Columns: []string{"id"},
				EngineVersion: "v0.8.0", NodeID: "node-a", KeyID: "k1", SignedAt: signedAt, Sig: []byte{4},
			},
			Rows: ledgerdigest.Rows{{text}},
		}, &AttestedResult{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sent, err := json.Marshal(c.server)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(sent, c.client); err != nil {
				t.Fatalf("decode %s: %v", sent, err)
			}
			back, err := json.Marshal(c.client)
			if err != nil {
				t.Fatal(err)
			}
			var want, got any
			if err := json.Unmarshal(sent, &want); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(back, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip changed the JSON\nserver %s\nclient %s", sent, back)
			}
			// And the server's types read what the client writes.
			decoded := reflect.New(reflect.TypeOf(c.server))
			if err := json.Unmarshal(back, decoded.Interface()); err != nil {
				t.Fatalf("server decode %s: %v", back, err)
			}
			if !reflect.DeepEqual(decoded.Elem().Interface(), c.server) {
				t.Fatalf("server decode = %+v, want %+v", decoded.Elem().Interface(), c.server)
			}
		})
	}
}
