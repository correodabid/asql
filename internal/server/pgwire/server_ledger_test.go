package pgwire

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/platform/datadir"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/logtree"
	"github.com/correodabid/asql/internal/storage/wal"

	"github.com/jackc/pgx/v5"
)

// signedLedgerConfig returns a config for ledger=signed with a fresh key, and
// the keyring that verifies it.
func signedLedgerConfig(t *testing.T) (Config, ledgerdigest.Keyring) {
	t.Helper()
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "node.key")
	pub, err := ledgerdigest.GenerateKeyFiles(keyPath, filepath.Join(dir, "node.pub"))
	if err != nil {
		t.Fatal(err)
	}
	keys := ledgerdigest.Keyring{}
	keys.Add(pub)
	return Config{
		Address:              "127.0.0.1:0",
		AdminHTTPAddr:        "127.0.0.1:0",
		DataDirPath:          filepath.Join(dir, "data"),
		Logger:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		NodeID:               "node-a",
		LedgerMode:           "signed",
		LedgerKeyFile:        keyPath,
		LedgerDigestInterval: 20 * time.Millisecond,
	}, keys
}

func execLedgerWrites(t *testing.T, pgwireAddr string, from, to int) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if from == 1 {
		for _, sql := range []string{"BEGIN DOMAIN lab", "CREATE TABLE results (id INT PRIMARY KEY, result TEXT)", "COMMIT"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("exec %q: %v", sql, err)
			}
		}
	}
	for id := from; id <= to; id++ {
		for _, sql := range []string{"BEGIN DOMAIN lab", fmt.Sprintf("INSERT INTO results (id, result) VALUES (%d, 'WITHIN_SPEC')", id), "COMMIT"} {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("exec %q: %v", sql, err)
			}
		}
	}
}

func TestSignedLedgerDigestsMatchTheWAL(t *testing.T) {
	config, keys := signedLedgerConfig(t)
	server, pgwireAddr, adminAddr, cleanup := startAdminSmokeServer(t, config)
	execLedgerWrites(t, pgwireAddr, 1, 10)

	// The checkpointer signs while the server runs, not only on shutdown.
	digestLog := server.ledger.log
	deadline := time.Now().Add(2 * time.Second)
	for {
		if d, ok := digestLog.Latest(); ok && d.LSN == server.engine.HeadLSN() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no digest of the committed head while running")
		}
		time.Sleep(10 * time.Millisecond)
	}
	checkDigestSurfaces(t, server, pgwireAddr, adminAddr, keys)
	execLedgerWrites(t, pgwireAddr, 11, 12)
	cleanup()

	dd, err := datadir.New(config.DataDirPath)
	if err != nil {
		t.Fatal(err)
	}
	digestLog, err = ledgerdigest.OpenLog(dd.LedgerDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer digestLog.Close()
	latest, ok := digestLog.Latest()
	if !ok {
		t.Fatal("no digest after shutdown")
	}
	if err := latest.Verify(keys); err != nil {
		t.Fatalf("latest digest: %v", err)
	}
	// Rebuild the log tree from the WAL alone: roots[n] is its root at LSN n.
	var tree logtree.Frontier
	roots := []logtree.Hash{{}}
	report, err := wal.VerifyLedger(dd.WALBasePath(), wal.VerifyOptions{
		Anchors: []wal.LedgerAnchor{{LSN: latest.LSN, Chain: latest.Chain, Source: "digest"}},
		OnLeaf: func(_ uint64, leaf wal.ChainHash) {
			tree.Add(logtree.Hash(leaf))
			roots = append(roots, tree.Root())
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Shutdown signs the final head, so the newest digest covers the whole WAL.
	if !report.OK || latest.LSN != report.LastLSN || latest.NodeID != "node-a" {
		t.Fatalf("digest LSN %d, WAL last LSN %d, ok=%v failure=%v", latest.LSN, report.LastLSN, report.OK, report.Failure)
	}
	// Every digest signs the log tree root at its LSN (ADR 0008).
	for _, lsn := range []uint64{1, 3, latest.LSN / 2, latest.LSN} {
		d, ok, err := digestLog.Covering(lsn)
		if err != nil || !ok {
			t.Fatalf("no digest covers LSN %d: %v", lsn, err)
		}
		if d.Version != ledgerdigest.Version2 || logtree.Hash(d.TreeRoot) != roots[d.LSN] {
			t.Fatalf("digest at LSN %d: version %d, tree root %x; the WAL's is %x", d.LSN, d.Version, d.TreeRoot, roots[d.LSN])
		}
	}
	// The node's own tree holds the whole WAL.
	nodeTree, err := logtree.Open(dd.LedgerDir())
	if err != nil {
		t.Fatal(err)
	}
	defer nodeTree.Close()
	if root, err := nodeTree.Root(int64(latest.LSN)); err != nil || root != roots[latest.LSN] {
		t.Fatalf("node's tree at LSN %d: %x, %v; want %x", latest.LSN, root, err, roots[latest.LSN])
	}
}

// checkDigestSurfaces reads digests through admin HTTP and SQL and checks
// they match the log.
func checkDigestSurfaces(t *testing.T, server *Server, pgwireAddr, adminAddr string, keys ledgerdigest.Keyring) {
	t.Helper()
	latest, _ := server.ledger.log.Latest()
	covering, ok, err := server.ledger.log.Covering(3)
	if err != nil || !ok {
		t.Fatalf("no digest covers LSN 3: %v", err)
	}

	get := func(path string, want int) ledgerdigest.Digest {
		t.Helper()
		resp, err := http.Get("http://" + adminAddr + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != want {
			t.Fatalf("GET %s: status %d, want %d: %s", path, resp.StatusCode, want, body)
		}
		var d ledgerdigest.Digest
		if want == http.StatusOK {
			if err := json.Unmarshal(body, &d); err != nil {
				t.Fatalf("GET %s: %v: %s", path, err, body)
			}
			if err := d.Verify(keys); err != nil {
				t.Fatalf("GET %s: %v", path, err)
			}
		}
		return d
	}
	if d := get("/api/v1/ledger/digest", http.StatusOK); d.LSN != latest.LSN || d.Chain != latest.Chain {
		t.Fatalf("latest digest over HTTP: LSN %d, want %d", d.LSN, latest.LSN)
	}
	if d := get("/api/v1/ledger/digest?lsn=3", http.StatusOK); d.LSN != covering.LSN {
		t.Fatalf("covering digest over HTTP: LSN %d, want %d", d.LSN, covering.LSN)
	}
	get(fmt.Sprintf("/api/v1/ledger/digest?lsn=%d", latest.LSN+1000), http.StatusNotFound)
	get("/api/v1/ledger/digest?lsn=x", http.StatusBadRequest)

	ctx := context.Background()
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		for sql, want := range map[string]ledgerdigest.Digest{
			"SELECT * FROM asql_admin.ledger_digest":                latest,
			"SELECT * FROM asql_admin.ledger_digest WHERE lsn >= 3": covering,
		} {
			var (
				version, lsn, term, timestamp       int64
				chain, nodeID, keyID, signedAt, sig string
				treeRoot, stateRoot                 *string
			)
			if err := conn.QueryRow(ctx, sql).Scan(&version, &lsn, &chain, &term, &timestamp, &nodeID, &keyID, &signedAt, &sig, &treeRoot, &stateRoot); err != nil {
				t.Fatalf("%s (%s): %v", sql, mode, err)
			}
			if uint64(lsn) != want.LSN || chain != hex.EncodeToString(want.Chain[:]) || keyID != want.KeyID || sig != base64.StdEncoding.EncodeToString(want.Sig) {
				t.Fatalf("%s (%s): lsn=%d chain=%s, want lsn=%d", sql, mode, lsn, chain, want.LSN)
			}
			if version != ledgerdigest.Version2 || treeRoot == nil || *treeRoot != hex.EncodeToString(want.TreeRoot[:]) {
				t.Fatalf("%s (%s): version %d, tree_root %v; want version 2 with %x", sql, mode, version, treeRoot, want.TreeRoot)
			}
			if stateRoot != nil {
				t.Fatalf("%s (%s): a node without a state key returned state_root %s", sql, mode, *stateRoot)
			}
		}
		_ = conn.Close(ctx)
	}
}

func TestLedgerDigestSurfacesWithoutSigning(t *testing.T) {
	config, _ := signedLedgerConfig(t)
	config.LedgerMode, config.LedgerKeyFile = "chain", ""
	_, pgwireAddr, adminAddr, cleanup := startAdminSmokeServer(t, config)
	defer cleanup()

	resp, err := http.Get("http://" + adminAddr + "/api/v1/ledger/digest")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+pgwireAddr+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	rows, err := conn.Query(ctx, "SELECT * FROM asql_admin.ledger_digest")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil || n != 0 {
		t.Fatalf("rows=%d err=%v, want no rows", n, err)
	}
	if got := len(rows.FieldDescriptions()); got != len(ledgerDigestColumns) {
		t.Fatalf("columns = %d, want %d", got, len(ledgerDigestColumns))
	}
}
