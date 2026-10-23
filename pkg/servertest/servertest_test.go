package servertest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/pkg/adminapi"
	"github.com/correodabid/asql/pkg/servertest"
)

func TestStartForTestingServesAnEmptyDatabase(t *testing.T) {
	ctx := context.Background()
	srv := servertest.StartForTesting(t, servertest.Options{})
	conn, err := pgx.Connect(ctx, srv.DSN)
	if err != nil {
		t.Fatal(err)
	}
	// Left open on purpose: the cleanup must not wait for it.
	for _, sql := range []string{"BEGIN DOMAIN app", "CREATE TABLE t (id INT PRIMARY KEY)", "COMMIT"} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
}

// TestStartForTestingSignedLedger: a SignedLedger server signs at a known
// LSN on demand, with the node ID it was given, and what it signs verifies
// against the public key it hands back.
func TestStartForTestingSignedLedger(t *testing.T) {
	ctx := context.Background()
	srv := servertest.StartForTesting(t, servertest.Options{SignedLedger: true, NodeID: "studio-test"})
	conn, err := pgx.Connect(ctx, srv.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	exec := func(sqls ...string) {
		t.Helper()
		for _, sql := range sqls {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("exec %q: %v", sql, err)
			}
		}
	}
	keys := ledgerdigest.Keyring{}
	pubs, err := ledgerdigest.LoadKeyring(srv.LedgerPublicKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	for id, pub := range pubs {
		if id != srv.LedgerKeyID || !pub.Equal(srv.LedgerPublicKey) {
			t.Fatalf("public key file holds %s, want %s", id, srv.LedgerKeyID)
		}
		keys[id] = pub
	}
	latest := func() (adminapi.LedgerDigest, ledgerdigest.Digest) {
		t.Helper()
		rows, err := conn.Query(ctx, "SELECT * FROM asql_admin.ledger_digest")
		if err != nil {
			t.Fatal(err)
		}
		values, err := pgx.CollectExactlyOneRow(rows, pgx.RowToMap)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		var public adminapi.LedgerDigest
		var d ledgerdigest.Digest
		if err := json.Unmarshal(raw, &public); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
		if err := d.Verify(keys); err != nil {
			t.Fatalf("digest at LSN %d: %v", d.LSN, err)
		}
		return public, d
	}

	exec("BEGIN DOMAIN app", "CREATE TABLE t (id INT PRIMARY KEY, note TEXT) VERIFIABLE", "COMMIT")
	exec("BEGIN DOMAIN app", "INSERT INTO t (id, note) VALUES (1, 'a')", "COMMIT")
	// The cadence may sign this commit first; either way the head is
	// signed once SignLedgerDigest returns.
	srv.SignLedgerDigest(t)
	if srv.SignLedgerDigest(t) {
		t.Fatal("SignLedgerDigest signed again with nothing committed")
	}
	first, _ := latest()
	var head uint64
	if err := conn.QueryRow(ctx, "SELECT current_lsn()").Scan(&head); err != nil {
		t.Fatal(err)
	}
	if first.LSN != head || first.Version != ledgerdigest.Version3 || first.StateRoot == "" {
		t.Fatalf("digest = %+v, want version 3 with a state root at the head, LSN %d", first, head)
	}
	if first.NodeID != "studio-test" || first.KeyID != srv.LedgerKeyID {
		t.Fatalf("digest node_id %q key_id %q, want studio-test and %s", first.NodeID, first.KeyID, srv.LedgerKeyID)
	}

	exec("BEGIN DOMAIN app", "INSERT INTO t (id, note) VALUES (2, 'b')", "COMMIT")
	srv.SignLedgerDigest(t)
	second, _ := latest()
	if second.LSN <= first.LSN {
		t.Fatalf("second digest at LSN %d, first at %d", second.LSN, first.LSN)
	}
	var fromDigest, toDigest, proof string
	query := fmt.Sprintf("SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = %d AND to_lsn = %d", first.LSN, second.LSN)
	if err := conn.QueryRow(ctx, query).Scan(&fromDigest, &toDigest, &proof); err != nil {
		t.Fatal(err)
	}
	var p ledgerdigest.ConsistencyProof
	raw := fmt.Sprintf(`{"from_digest":%s,"to_digest":%s,"proof":%s}`, fromDigest, toDigest, proof)
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if err := p.Verify(keys); err != nil {
		t.Fatalf("consistency proof: %v", err)
	}
}
