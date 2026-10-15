package pgwire

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/platform/datadir"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
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
	server, pgwireAddr, _, cleanup := startAdminSmokeServer(t, config)
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
	report, err := wal.VerifyLedger(dd.WALBasePath(), wal.VerifyOptions{
		Anchors: []wal.LedgerAnchor{{LSN: latest.LSN, Chain: latest.Chain, Source: "digest"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Shutdown signs the final head, so the newest digest covers the whole WAL.
	if !report.OK || latest.LSN != report.LastLSN || latest.NodeID != "node-a" {
		t.Fatalf("digest LSN %d, WAL last LSN %d, ok=%v failure=%v", latest.LSN, report.LastLSN, report.OK, report.Failure)
	}
}
