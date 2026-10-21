package pgwire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/storage/ledgerdigest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// waitForDigestAtHead waits until the checkpointer signs the committed head.
func waitForDigestAtHead(t *testing.T, server *Server) ledgerdigest.Digest {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if d, ok := server.ledger.log.Latest(); ok && d.LSN == server.engine.HeadLSN() {
			return d
		}
		if time.Now().After(deadline) {
			t.Fatal("no digest of the committed head")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func queryConsistency(ctx context.Context, conn *pgx.Conn, sql string) (ledgerdigest.ConsistencyProof, error) {
	var from, to, proof string
	if err := conn.QueryRow(ctx, sql).Scan(&from, &to, &proof); err != nil {
		return ledgerdigest.ConsistencyProof{}, err
	}
	var p ledgerdigest.ConsistencyProof
	err := json.Unmarshal([]byte(`{"from_digest":`+from+`,"to_digest":`+to+`,"proof":`+proof+`}`), &p)
	return p, err
}

func TestProveConsistency(t *testing.T) {
	config, keys := signedLedgerConfig(t)
	server, pgwireAddr, _, cleanup := startAdminSmokeServer(t, config)
	defer cleanup()
	ctx := context.Background()
	execLedgerWrites(t, pgwireAddr, 1, 3)
	early := waitForDigestAtHead(t, server)
	execLedgerWrites(t, pgwireAddr, 4, 9)
	late := waitForDigestAtHead(t, server)

	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })

		for sql, want := range map[string][2]uint64{
			fmt.Sprintf("SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = %d AND to_lsn = %d", early.LSN, late.LSN): {early.LSN, late.LSN},
			// Without to_lsn, up to the latest digest.
			fmt.Sprintf("SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = %d", early.LSN):                          {early.LSN, late.LSN},
			fmt.Sprintf("SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = %d AND to_lsn = %d", late.LSN, late.LSN): {late.LSN, late.LSN},
		} {
			p, err := queryConsistency(ctx, conn, sql)
			if err != nil {
				t.Fatalf("%s: %s: %v", mode, sql, err)
			}
			if p.From.LSN != want[0] || p.To.LSN != want[1] {
				t.Fatalf("%s: %s: proof %d -> %d, want %d -> %d", mode, sql, p.From.LSN, p.To.LSN, want[0], want[1])
			}
			if err := p.Verify(keys); err != nil {
				t.Fatalf("%s: %s: %v", mode, sql, err)
			}
			if p.From.LSN == early.LSN && !bytes.Equal(p.From.Sig, early.Sig) {
				t.Fatalf("%s: the proof's from digest is not the one the node published", mode)
			}
		}

		for sql, want := range map[string]string{
			"SELECT * FROM asql_admin.prove_consistency":                                                                       "from_lsn",
			fmt.Sprintf("SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = %d", early.LSN-1):                         fmt.Sprintf("next one is at LSN %d", early.LSN),
			fmt.Sprintf("SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = %d AND to_lsn = %d", late.LSN, early.LSN): "after",
			fmt.Sprintf("SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = %d", late.LSN+100):                        "no digest was signed",
		} {
			_, err := queryConsistency(ctx, conn, sql)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "22023" || !strings.Contains(pgErr.Message, want) {
				t.Fatalf("%s: %s: %v, want 22023 with %q", mode, sql, err, want)
			}
		}
	}
}

func TestProveConsistencyNeedsSignedLedger(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	_, err = queryConsistency(ctx, conn, "SELECT * FROM asql_admin.prove_consistency WHERE from_lsn = 1")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55000" {
		t.Fatalf("without ledger=signed: %v, want 55000", err)
	}
}
