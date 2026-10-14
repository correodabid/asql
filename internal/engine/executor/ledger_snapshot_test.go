package executor

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/storage/wal"
)

func newLedgerTestEngine(t *testing.T, dir string, opts ...EngineOption) (*wal.SegmentedLogStore, *Engine) {
	t.Helper()
	snapDir := filepath.Join(dir, "snap")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := wal.NewSegmentedLogStore(filepath.Join(dir, "asql.wal"), wal.AlwaysSync{}, wal.WithSegmentSize(4096))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	eng, err := New(context.Background(), store, snapDir, opts...)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	return store, eng
}

func execLedgerTx(t *testing.T, eng *Engine, stmts ...string) {
	t.Helper()
	ctx := context.Background()
	s := eng.NewSession()
	for _, sql := range append(append([]string{"BEGIN DOMAIN lab"}, stmts...), "COMMIT") {
		if _, err := eng.Execute(ctx, s, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
}

// captureLedgerTestSnapshot adds an in-memory snapshot at the current head,
// which WaitPendingSnapshots then persists (and, with WAL GC, truncates to).
func captureLedgerTestSnapshot(eng *Engine) uint64 {
	state := eng.readState.Load()
	eng.writeMu.Lock()
	catalog := cloneCatalog(eng.catalog)
	eng.writeMu.Unlock()
	snap := captureSnapshotWithCatalog(state, catalog)
	eng.snapshots.mu.Lock()
	eng.snapshots.add(snap)
	eng.snapshots.mu.Unlock()
	return snap.lsn
}

func TestSnapshotRecordsLedgerAnchor(t *testing.T) {
	dir := t.TempDir()
	store, eng := newLedgerTestEngine(t, dir)
	execLedgerTx(t, eng, "CREATE TABLE results (id INT, result TEXT)")
	for i := 0; i < 50; i++ {
		execLedgerTx(t, eng, fmt.Sprintf("INSERT INTO results (id, result) VALUES (%d, 'r%d')", i, i))
	}
	captureLedgerTestSnapshot(eng)
	eng.WaitPendingSnapshots()
	head := store.LedgerHead()
	_ = store.Close()

	anchors, err := SnapshotLedgerAnchors(filepath.Join(dir, "snap"))
	if err != nil {
		t.Fatal(err)
	}
	if len(anchors) == 0 {
		t.Fatal("persisted snapshot carries no ledger anchor")
	}
	last := anchors[len(anchors)-1]
	if last.LSN != head.LSN || last.Chain != head.Chain {
		t.Fatalf("snapshot anchor %d/%s, ledger head %d/%s", last.LSN, last.Chain, head.LSN, head.Chain)
	}
	report, err := wal.VerifyLedger(filepath.Join(dir, "asql.wal"), wal.VerifyOptions{Anchors: anchors})
	if err != nil || !report.OK {
		t.Fatalf("verify with snapshot anchors: err=%v failure=%v", err, report.Failure)
	}
}

func TestWALGCKeepsLedgerVerifiableFromSnapshotAnchor(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "asql.wal")
	store, eng := newLedgerTestEngine(t, dir, WithWALGC())
	execLedgerTx(t, eng, "CREATE TABLE results (id INT, result TEXT)")
	for i := 0; i < 200; i++ {
		execLedgerTx(t, eng, fmt.Sprintf("INSERT INTO results (id, result) VALUES (%d, 'value-%d')", i, i))
		if i == 150 {
			captureLedgerTestSnapshot(eng)
		}
	}
	eng.WaitPendingSnapshots()
	_ = store.Close()

	catalog, err := wal.CatalogSegments(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog) == 0 || catalog[0].FirstLSN <= 1 {
		t.Fatalf("expected WAL GC to remove the head of the log: %+v", catalog)
	}
	anchors, err := SnapshotLedgerAnchors(filepath.Join(dir, "snap"))
	if err != nil || len(anchors) == 0 {
		t.Fatalf("snapshot anchors: %v, %v", anchors, err)
	}

	report, err := wal.VerifyLedger(base, wal.VerifyOptions{Anchors: anchors})
	if err != nil {
		t.Fatal(err)
	}
	if report.OK || report.Failure.Kind != wal.VerifyNotGenesis {
		t.Fatalf("verification claiming genesis must fail explicitly after GC: %+v", report.Failure)
	}
	report, err = wal.VerifyLedger(base, wal.VerifyOptions{Anchors: anchors, FromAnchor: true})
	if err != nil || !report.OK {
		t.Fatalf("verification from snapshot anchor: err=%v failure=%v", err, report.Failure)
	}

	// The engine restarts over the truncated, anchored WAL.
	store, eng = newLedgerTestEngine(t, dir, WithWALGC())
	defer store.Close()
	if h := store.LedgerHead(); !h.Valid || !h.Rooted {
		t.Fatalf("ledger head after GC restart: %+v", h)
	}
	res, err := eng.Query(context.Background(), "SELECT result FROM results WHERE id = 199", []string{"lab"})
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("query after restart: %v %v", res, err)
	}
}

// Snapshots written before ADR 0005 (format v12) still decode, without an anchor.
func TestSnapshotV12DecodesWithoutLedgerAnchor(t *testing.T) {
	_, eng := newLedgerTestEngine(t, t.TempDir())
	execLedgerTx(t, eng, "CREATE TABLE results (id INT, result TEXT)", "INSERT INTO results (id, result) VALUES (1, 'a')")
	state := eng.readState.Load()
	snap := captureSnapshotWithCatalog(state, cloneCatalog(eng.catalog))
	snap.ledgerChain[0], snap.hasLedgerChain = 7, true

	v13, err := encodeSnapshotFileBinary(&snap, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Rewrite as v12: drop the 33-byte anchor after is_full and fix the CRC.
	off := 5
	n := int(binary.BigEndian.Uint32(v13[off:]))
	off += 4
	for i := 0; i < n; i++ {
		off += 2 + int(binary.BigEndian.Uint16(v13[off:]))
	}
	off += 4 + 8 + 8 + 1 // num_snapshots, lsn, logicalTS, is_full
	v12 := append(append([]byte{}, v13[:off]...), v13[off+33:len(v13)-4]...)
	v12[4] = 12
	v12 = binary.BigEndian.AppendUint32(v12, crc32.Checksum(v12, snapCRC32C))

	for name, data := range map[string][]byte{"v13": v13, "v12": v12} {
		entries, err := decodeSnapshotFileBinaryRaw(data)
		if err != nil || len(entries) != 1 {
			t.Fatalf("%s: decode: %v", name, err)
		}
		if got, want := entries[0].hasLedgerChain, name == "v13"; got != want || entries[0].lsn != snap.lsn {
			t.Fatalf("%s: hasLedgerChain=%v lsn=%d", name, got, entries[0].lsn)
		}
		snaps, ok, err := decodeSingleFullSnapshotBinary(data)
		if err != nil || !ok || len(snaps[0].state.domains["lab"].tables["results"].rows) != 1 {
			t.Fatalf("%s: full decode: ok=%v err=%v", name, ok, err)
		}
	}
}
