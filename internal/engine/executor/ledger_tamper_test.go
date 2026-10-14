package executor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/s2"

	"github.com/correodabid/asql/internal/storage/wal"
)

// TestWALTamperIsDetectedByLedger is the acceptance test for ADR 0005. An
// attacker with write access to the data directory edits a committed value
// and recomputes the record CRC32C and the segment header's size and CRC.
// Before the ledger the engine replayed the forged history without error;
// now verify-ledger locates the modification and the store refuses to open.
func TestWALTamperIsDetectedByLedger(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "asql.wal")

	open := func() (*wal.SegmentedLogStore, *Engine) {
		store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
		if err != nil {
			t.Fatalf("open wal: %v", err)
		}
		eng, err := New(ctx, store, "")
		if err != nil {
			t.Fatalf("open engine: %v", err)
		}
		return store, eng
	}
	exec := func(eng *Engine, s *Session, sql string) {
		if _, err := eng.Execute(ctx, s, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	store, eng := open()
	s := eng.NewSession()
	exec(eng, s, "BEGIN DOMAIN lab")
	exec(eng, s, "CREATE TABLE batch_results (id INT, result TEXT)")
	exec(eng, s, "COMMIT")
	s = eng.NewSession()
	exec(eng, s, "BEGIN DOMAIN lab")
	exec(eng, s, "INSERT INTO batch_results (id, result) VALUES (1, 'OUT_OF_SPEC')")
	exec(eng, s, "COMMIT")
	eng.WaitPendingSnapshots()
	_ = store.Close()

	// Forge: rewrite OUT_OF_SPEC -> WITHIN_SPEC inside the WAL and fix the CRC.
	segs, _ := filepath.Glob(walPath + ".*")
	forged := false
	var forgedLSN uint64
	for _, seg := range segs {
		data, err := os.ReadFile(seg)
		if err != nil || len(data) < 64 || string(data[:4]) != "WALS" {
			continue
		}
		headerSize := 64
		if data[4] == 4 { // v4 header carries the ledger anchors
			headerSize = 128
		}
		header, frames := data[:headerSize], data[headerSize:]
		var out []byte
		for off := 0; off+4 <= len(frames); {
			n := int(binary.BigEndian.Uint32(frames[off:]))
			if n == 0 || off+4+n > len(frames) {
				out = append(out, frames[off:]...)
				break
			}
			body := frames[off+4 : off+4+n]
			off += 4 + n
			txLen := int(binary.BigEndian.Uint16(body[25:]))
			pStart := 27 + txLen + 1
			payload := body[pStart : len(body)-4]
			if len(payload) > 0 {
				raw, err := s2.Decode(nil, payload)
				if err == nil && bytes.Contains(raw, []byte("OUT_OF_SPEC")) {
					raw = bytes.Replace(raw, []byte("OUT_OF_SPEC"), []byte("WITHIN_SPEC"), 1)
					nb := append(append([]byte{}, body[:pStart]...), s2.Encode(nil, raw)...)
					nb = binary.BigEndian.AppendUint32(nb, crc32.Checksum(nb, crc32.MakeTable(crc32.Castagnoli)))
					body = nb
					forged = true
					forgedLSN = binary.BigEndian.Uint64(body[1:9])
				}
			}
			out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
			out = append(out, body...)
		}
		if forged {
			// Keep the header consistent with the rewritten frames: DataSize
			// and header CRC32C. The chain anchors are left untouched.
			header = append([]byte{}, header...)
			binary.BigEndian.PutUint64(header[30:38], uint64(len(out)))
			binary.BigEndian.PutUint32(header[headerSize-4:], crc32.Checksum(header[:headerSize-4], crc32.MakeTable(crc32.Castagnoli)))
			if err := os.WriteFile(seg, append(header, out...), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !forged {
		t.Fatal("did not find the committed value in the WAL")
	}

	// verify-ledger detects the modification and locates it.
	report, err := wal.VerifyLedger(walPath, wal.VerifyOptions{})
	if err != nil {
		t.Fatalf("verify ledger: %v", err)
	}
	if report.OK {
		t.Fatal("forged WAL passed ledger verification")
	}
	f := report.Failure
	t.Logf("verify-ledger: %s", f)
	if f.Kind != wal.VerifyChainMismatch {
		t.Fatalf("failure kind %q, want %q", f.Kind, wal.VerifyChainMismatch)
	}
	if forgedLSN <= f.AfterLSN || forgedLSN > f.AtLSN {
		t.Fatalf("forged LSN %d outside reported interval (%d, %d]", forgedLSN, f.AfterLSN, f.AtLSN)
	}

	// Reopen: the store refuses to replay the forged history.
	if _, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{}); !errors.Is(err, wal.ErrLedgerMismatch) {
		t.Fatalf("open forged wal: err=%v, want ErrLedgerMismatch", err)
	}
}
