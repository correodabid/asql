package executor

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/klauspost/compress/s2"

	"github.com/correodabid/asql/internal/storage/wal"
)

// TestLedgerRewriteNeedsAnExternalDigest documents the limit of the ledger
// without signed, externally held digests (ADR 0005 § Threat model). An
// attacker with write access to the data directory edits a committed value,
// recomputes the record CRCs, the whole chain, the segment header anchors,
// and regenerates the snapshots. Everything on disk is then self-consistent
// and verify-ledger passes. Only a digest exported before the rewrite, and
// kept where the attacker cannot change it, exposes the forgery.
func TestLedgerRewriteNeedsAnExternalDigest(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "asql.wal")
	snapDir := filepath.Join(dir, "snaps")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	open := func() (*wal.SegmentedLogStore, *Engine) {
		store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{}, wal.WithSegmentSize(1024))
		if err != nil {
			t.Fatalf("open wal: %v", err)
		}
		eng, err := New(ctx, store, snapDir)
		if err != nil {
			t.Fatalf("open engine: %v", err)
		}
		return store, eng
	}
	exec := func(eng *Engine, statements ...string) {
		s := eng.NewSession()
		for _, sql := range statements {
			if _, err := eng.Execute(ctx, s, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}

	store, eng := open()
	exec(eng, "BEGIN DOMAIN lab", "CREATE TABLE batch_results (id INT, result TEXT)", "COMMIT")
	exec(eng, "BEGIN DOMAIN lab", "INSERT INTO batch_results (id, result) VALUES (1, 'OUT_OF_SPEC')", "COMMIT")
	for id := 2; id <= 20; id++ {
		exec(eng, "BEGIN DOMAIN lab", fmt.Sprintf("INSERT INTO batch_results (id, result) VALUES (%d, 'WITHIN_SPEC')", id), "COMMIT")
	}
	// The operator exports a digest and keeps it out of the attacker's reach.
	exported := store.LedgerHead()
	if !exported.Valid {
		t.Fatal("ledger head not available")
	}
	eng.WaitPendingSnapshots()
	_ = store.Close()

	rewriteLedger(t, walPath, []byte("OUT_OF_SPEC"), []byte("WITHIN_SPEC"))
	// Snapshots are derived state: drop them and let the engine regenerate
	// them, with anchors, from the forged WAL.
	if err := os.RemoveAll(snapDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	store, eng = open()
	result, err := eng.Query(ctx, "SELECT result FROM batch_results WHERE id = 1", []string{"lab"})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["result"].StringValue != "WITHIN_SPEC" {
		t.Fatalf("forged history not replayed: rows=%v err=%v", result.Rows, err)
	}
	eng.WaitPendingSnapshots()
	_ = store.Close()

	// Everything on disk agrees with itself: verify-ledger passes.
	anchors, err := SnapshotLedgerAnchors(snapDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(anchors) == 0 {
		t.Fatal("expected regenerated snapshot anchors")
	}
	report, err := wal.VerifyLedger(walPath, wal.VerifyOptions{Anchors: anchors})
	if err != nil {
		t.Fatal(err)
	}
	if !report.OK || report.Segments < 2 || report.AnchorsChecked == 0 {
		t.Fatalf("expected the rewritten ledger to verify: ok=%v segments=%d anchors=%d failure=%v",
			report.OK, report.Segments, report.AnchorsChecked, report.Failure)
	}

	// The digest exported before the rewrite does not match.
	report, err = wal.VerifyLedger(walPath, wal.VerifyOptions{Anchors: []wal.LedgerAnchor{
		{LSN: exported.LSN, Chain: exported.Chain, Source: "exported digest"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if report.OK || report.Failure == nil || report.Failure.Kind != wal.VerifyChainMismatch {
		t.Fatalf("exported digest did not expose the rewrite: ok=%v failure=%v", report.OK, report.Failure)
	}
	t.Logf("verify-ledger against the exported digest: %s", report.Failure)
}

// rewriteLedger replaces old with new in every mutation payload and then
// rewrites what the ledger anchors on disk: record CRCs, and each segment
// header's chain values, DataSize and CRC. It uses only what an attacker
// with the code has: the record format and the public chain functions.
func rewriteLedger(t *testing.T, walPath string, old, new []byte) {
	t.Helper()
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{}, wal.WithSegmentSize(1024))
	if err != nil {
		t.Fatal(err)
	}
	records, err := store.ReadFrom(ctx, 1, 0)
	_ = store.Close()
	if err != nil {
		t.Fatal(err)
	}
	// chainAt[lsn] is the forged chain value after lsn.
	chainAt := map[uint64]wal.ChainHash{0: wal.GenesisChain}
	chain := wal.GenesisChain
	for _, r := range records {
		r.Payload = bytes.ReplaceAll(r.Payload, old, new)
		leaf, err := wal.LedgerLeaf(r)
		if err != nil {
			t.Fatal(err)
		}
		chain = wal.ChainStep(chain, r.LSN, leaf)
		chainAt[r.LSN] = chain
	}

	segs, _ := filepath.Glob(walPath + ".*")
	sort.Strings(segs)
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	rewritten := 0
	for _, seg := range segs {
		data, err := os.ReadFile(seg)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) < 128 || string(data[:4]) != "WALS" || data[4] != 4 {
			t.Fatalf("%s: expected a raw segment with a v4 header", seg)
		}
		header := append([]byte{}, data[:128]...)
		frames := data[128:]
		var out []byte
		var first, last uint64
		for off := 0; off+4 <= len(frames); {
			n := int(binary.BigEndian.Uint32(frames[off:]))
			if n == 0 || off+4+n > len(frames) {
				break
			}
			body := frames[off+4 : off+4+n]
			off += 4 + n
			lsn := binary.BigEndian.Uint64(body[1:9])
			if first == 0 {
				first = lsn
			}
			last = lsn
			txLen := int(binary.BigEndian.Uint16(body[25:]))
			pStart := 27 + txLen + 1
			if payload := body[pStart : len(body)-4]; len(payload) > 0 {
				raw, err := s2.Decode(nil, payload)
				if err == nil && bytes.Contains(raw, old) {
					nb := append(append([]byte{}, body[:pStart]...), s2.Encode(nil, bytes.ReplaceAll(raw, old, new))...)
					body = binary.BigEndian.AppendUint32(nb, crc32.Checksum(nb, castagnoli))
					rewritten++
				}
			}
			out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
			out = append(out, body...)
		}
		if first != 0 {
			before, ok1 := chainAt[first-1]
			at, ok2 := chainAt[last]
			if !ok1 || !ok2 {
				t.Fatalf("%s: no chain value for LSN range %d-%d", seg, first, last)
			}
			copy(header[39:71], before[:])
			copy(header[71:103], at[:])
		}
		binary.BigEndian.PutUint64(header[30:38], uint64(len(out)))
		binary.BigEndian.PutUint32(header[124:], crc32.Checksum(header[:124], castagnoli))
		if err := os.WriteFile(seg, append(header, out...), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if rewritten == 0 {
		t.Fatal("did not find the committed value in the WAL")
	}
}
