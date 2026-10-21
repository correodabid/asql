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

// WAL GC keeps every record from the floor on, even when a snapshot covers
// them: the log tree (ADR 0008) has not added them yet.
func TestWALGCStopsAtTheFloor(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "asql.wal")
	store, eng := newLedgerTestEngine(t, dir, WithWALGC())
	eng.SetWALGCFloor(func() uint64 { return 40 })
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
	if len(catalog) == 0 || catalog[0].FirstLSN > 40 {
		t.Fatalf("WAL GC removed records at or after the floor, LSN 40: %+v", catalog[0])
	}
}

// Snapshots written before ADR 0005 (format v12) still decode, without an
// anchor, and ones written before v14 without a TxID high-water mark.
func TestOlderSnapshotFormatsDecode(t *testing.T) {
	_, eng := newLedgerTestEngine(t, t.TempDir())
	execLedgerTx(t, eng, "CREATE TABLE results (id INT, result TEXT)", "INSERT INTO results (id, result) VALUES (1, 'a')")
	state := eng.readState.Load()
	snap := captureSnapshotWithCatalog(state, cloneCatalog(eng.catalog))
	snap.ledgerChain[0], snap.hasLedgerChain = 7, true
	snap.lastTxNum = 42

	snap.state.domains["lab"].tables["results"].appendOnly = true
	snap.state.domains["lab"].tables["results"].verifiable = true

	v16, err := encodeSnapshotFileBinary(&snap, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Older versions lack fields after is_full: the 33-byte anchor (v12),
	// the 8-byte last_tx_num after it (v12, v13), the append-only table
	// list after that (v12-v14: a count and one table, 12 bytes) and the
	// verifiable table list after that (v12-v15, also 12 bytes). Drop them
	// and fix the CRC.
	off := 5
	n := int(binary.BigEndian.Uint32(v16[off:]))
	off += 4
	for i := 0; i < n; i++ {
		off += 2 + int(binary.BigEndian.Uint16(v16[off:]))
	}
	off += 4 + 8 + 8 + 1 // num_snapshots, lsn, logicalTS, is_full
	older := func(version byte, from, to int) []byte {
		data := append(append([]byte{}, v16[:from]...), v16[to:len(v16)-4]...)
		data[4] = version
		return binary.BigEndian.AppendUint32(data, crc32.Checksum(data, snapCRC32C))
	}
	formats := map[string][]byte{
		"v16": v16,
		"v15": older(15, off+53, off+65),
		"v14": older(14, off+41, off+65),
		"v13": older(13, off+33, off+65),
		"v12": older(12, off, off+65),
	}

	for name, data := range formats {
		entries, err := decodeSnapshotFileBinaryRaw(data)
		if err != nil || len(entries) != 1 {
			t.Fatalf("%s: decode: %v", name, err)
		}
		if got, want := entries[0].hasLedgerChain, name != "v12"; got != want || entries[0].lsn != snap.lsn {
			t.Fatalf("%s: hasLedgerChain=%v lsn=%d", name, got, entries[0].lsn)
		}
		wantTxNum := uint64(0)
		if name == "v14" || name == "v15" || name == "v16" {
			wantTxNum = 42
		}
		wantAppendOnly := name == "v15" || name == "v16"
		wantVerifiable := name == "v16"
		if entries[0].lastTxNum != wantTxNum {
			t.Fatalf("%s: lastTxNum=%d, want %d", name, entries[0].lastTxNum, wantTxNum)
		}
		snaps, ok, err := decodeSingleFullSnapshotBinary(data)
		results := snaps[0].state.domains["lab"].tables["results"]
		if err != nil || !ok || len(results.rows) != 1 || snaps[0].lastTxNum != wantTxNum || results.appendOnly != wantAppendOnly || results.verifiable != wantVerifiable {
			t.Fatalf("%s: full decode: ok=%v err=%v", name, ok, err)
		}
		if len(entries[0].appendOnly) > 0 != wantAppendOnly || len(entries[0].verifiable) > 0 != wantVerifiable {
			t.Fatalf("%s: append-only tables %v, verifiable tables %v", name, entries[0].appendOnly, entries[0].verifiable)
		}
	}
}
