package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/s2"

	"github.com/correodabid/asql/internal/engine/ports"
)

// The hash format is a persistent contract: these vectors must never change.
// They were computed independently from the ADR 0005 construction.
func TestLedgerFormatVectors(t *testing.T) {
	if got, want := GenesisChain.String(), "e119e4f89e679843a77523dd96c9aedc8dd930d16c2c44faedb2cec2d6204264"; got != want {
		t.Fatalf("genesis chain changed:\n got %s\nwant %s", got, want)
	}
	leaf, err := LedgerLeaf(ports.WALRecord{Term: 2, Timestamp: 7, TxID: "tx-1", Type: "MUTATION", Payload: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := leaf.String(), "e151aacadc8e6d357d14ae07de6cab1d05ca6f630f3b21c76f2ad337bdb7aafc"; got != want {
		t.Fatalf("leaf vector changed:\n got %s\nwant %s", got, want)
	}
	if got, want := ChainStep(GenesisChain, 1, leaf).String(), "d5e0fa7a89911198ed35cbf907e48e24952c53aff36f7309773b0e26fb01fcca"; got != want {
		t.Fatalf("chain vector changed:\n got %s\nwant %s", got, want)
	}
}

func TestLedgerLeafExcludesLSNButChainIncludesIt(t *testing.T) {
	rec := ports.WALRecord{TxID: "tx", Type: "BEGIN", Timestamp: 1}
	a, _ := LedgerLeaf(rec)
	rec.LSN = 99
	b, _ := LedgerLeaf(rec)
	if a != b {
		t.Fatal("leaf must not depend on LSN")
	}
	if ChainStep(GenesisChain, 1, a) == ChainStep(GenesisChain, 2, a) {
		t.Fatal("chain step must depend on LSN")
	}
	if _, err := LedgerLeaf(ports.WALRecord{Type: "NOPE"}); err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func ledgerTx(i int, payloadSize int) []ports.WALRecord {
	txID := fmt.Sprintf("tx-%d", i)
	payload := bytes.Repeat([]byte{byte(i)}, payloadSize)
	ts := uint64(i * 3)
	return []ports.WALRecord{
		{TxID: txID, Type: "BEGIN", Timestamp: ts + 1, Term: 1},
		{TxID: txID, Type: "MUTATION", Timestamp: ts + 2, Term: 1, Payload: payload},
		{TxID: txID, Type: "COMMIT", Timestamp: ts + 3, Term: 1},
	}
}

// expectedChain recomputes the chain over records starting at LSN 1.
func expectedChain(t testing.TB, records []ports.WALRecord) ChainHash {
	t.Helper()
	chain := GenesisChain
	for i, r := range records {
		leaf, err := LedgerLeaf(r)
		if err != nil {
			t.Fatal(err)
		}
		chain = ChainStep(chain, uint64(i+1), leaf)
	}
	return chain
}

func newLedgerStore(t testing.TB, dir string, opts ...SegmentOption) *SegmentedLogStore {
	t.Helper()
	store, err := NewSegmentedLogStore(filepath.Join(dir, "asql.wal"), AlwaysSync{}, opts...)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func TestLedgerAllAppendPathsAgree(t *testing.T) {
	ctx := context.Background()
	var all []ports.WALRecord
	for i := 0; i < 20; i++ {
		all = append(all, ledgerTx(i, 64*i)...)
	}
	want := expectedChain(t, all)

	type appendFn func(store ports.LogStore, recs []ports.WALRecord) error
	paths := map[string]appendFn{
		"Append": func(s ports.LogStore, recs []ports.WALRecord) error {
			for _, r := range recs {
				if _, err := s.Append(ctx, r); err != nil {
					return err
				}
			}
			return nil
		},
		"AppendBatch": func(s ports.LogStore, recs []ports.WALRecord) error {
			_, err := s.(ports.BatchAppender).AppendBatch(ctx, recs)
			return err
		},
		"AppendBatchNoSync": func(s ports.LogStore, recs []ports.WALRecord) error {
			_, err := s.(ports.NoSyncBatchAppender).AppendBatchNoSync(ctx, recs)
			return err
		},
	}
	for name, fn := range paths {
		for _, kind := range []string{"segmented", "file"} {
			t.Run(name+"/"+kind, func(t *testing.T) {
				dir := t.TempDir()
				var store ports.LogStore
				var head func() LedgerHead
				if kind == "segmented" {
					s := newLedgerStore(t, dir, WithSegmentSize(2048))
					defer s.Close()
					store, head = s, s.LedgerHead
				} else {
					s, err := NewFileLogStore(filepath.Join(dir, "wal.log"), AlwaysSync{})
					if err != nil {
						t.Fatal(err)
					}
					defer s.Close()
					store, head = s, s.LedgerHead
				}
				for i := 0; i < len(all); i += 3 {
					if err := fn(store, all[i:i+3]); err != nil {
						t.Fatal(err)
					}
				}
				h := head()
				if !h.Valid || !h.Rooted || h.LSN != uint64(len(all)) || h.Chain != want {
					t.Fatalf("head = %+v, want chain %s at LSN %d", h, want, len(all))
				}
			})
		}
	}
}

func TestLedgerReplicationHeadsMatchAndDivergenceIsDetected(t *testing.T) {
	ctx := context.Background()
	leader := newLedgerStore(t, t.TempDir(), WithSegmentSize(4096))
	defer leader.Close()
	follower := newLedgerStore(t, t.TempDir(), WithSegmentSize(4096))
	defer follower.Close()
	diverged := newLedgerStore(t, t.TempDir(), WithSegmentSize(4096))
	defer diverged.Close()

	// Mixed leader traffic.
	for i := 0; i < 30; i++ {
		tx := ledgerTx(i, 100)
		if i%2 == 0 {
			for _, r := range tx {
				if _, err := leader.Append(ctx, r); err != nil {
					t.Fatal(err)
				}
			}
		} else if _, err := leader.AppendBatch(ctx, tx); err != nil {
			t.Fatal(err)
		}
	}
	records, err := leader.ReadFrom(ctx, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Follower receives a mix of single and batched replicated appends.
	for i := 0; i < len(records); {
		n := 1 + i%5
		if i+n > len(records) {
			n = len(records) - i
		}
		if n == 1 {
			err = follower.AppendReplicated(ctx, records[i])
		} else {
			err = follower.AppendReplicatedBatch(ctx, records[i:i+n])
		}
		if err != nil {
			t.Fatal(err)
		}
		i += n
	}
	if lh, fh := leader.LedgerHead(), follower.LedgerHead(); lh != fh {
		t.Fatalf("leader head %+v != follower head %+v", lh, fh)
	}

	// A follower that received one altered record diverges from the leader.
	forged := append([]ports.WALRecord(nil), records...)
	forged[40].Payload = append([]byte(nil), forged[40].Payload...)
	forged[40].Payload[0] ^= 0xff
	if err := diverged.AppendReplicatedBatch(ctx, forged); err != nil {
		t.Fatal(err)
	}
	if leader.LedgerHead().Chain == diverged.LedgerHead().Chain {
		t.Fatal("diverged follower must have a different chain head")
	}
	for _, lsn := range []uint64{40, 41} {
		lc, _ := leader.LedgerChainAt(lsn)
		dc, _ := diverged.LedgerChainAt(lsn)
		if (lc == dc) != (lsn < 41) {
			t.Fatalf("LSN %d: leader/diverged chain equality = %v", lsn, lc == dc)
		}
	}
}

func appendTxs(t testing.TB, store *SegmentedLogStore, from, to, payload int) []ports.WALRecord {
	t.Helper()
	var all []ports.WALRecord
	for i := from; i < to; i++ {
		tx := ledgerTx(i, payload)
		if _, err := store.AppendBatch(context.Background(), tx); err != nil {
			t.Fatal(err)
		}
		all = append(all, tx...)
	}
	return all
}

func TestLedgerRestartRestoresHeadFromSealedAnchors(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	all := appendTxs(t, store, 0, 40, 200)
	if store.SegmentCount() < 3 {
		t.Fatalf("expected rotation, got %d segments", store.SegmentCount())
	}
	before := store.LedgerHead()
	_ = store.Close()

	store = newLedgerStore(t, dir, WithSegmentSize(1024))
	defer store.Close()
	if got := store.LedgerHead(); got != before || got.Chain != expectedChain(t, all) {
		t.Fatalf("head after restart %+v, want %+v", got, before)
	}
	// Appending after restart continues the same chain.
	all = append(all, appendTxs(t, store, 40, 45, 200)...)
	if store.LedgerHead().Chain != expectedChain(t, all) {
		t.Fatal("chain diverged after restart")
	}
	report, err := VerifyLedger(filepath.Join(dir, "asql.wal"), VerifyOptions{})
	if err != nil || !report.OK || !report.FromGenesis || report.Records != uint64(len(all)) {
		t.Fatalf("verify: err=%v report=%+v failure=%v", err, report, report.Failure)
	}
	if report.AnchorsChecked < report.Segments {
		t.Fatalf("expected segment anchors to be checked: %+v", report)
	}
}

func TestLedgerCompactionInvariance(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	all := appendTxs(t, store, 0, 40, 300)
	want := store.LedgerHead()
	if err := store.CompactSealedSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	compressed := 0
	for _, p := range store.SegmentPaths() {
		data, _ := os.ReadFile(p)
		if string(data[:4]) == walzMagic {
			compressed++
		}
	}
	if compressed == 0 {
		t.Fatal("expected WALZ segments after compaction")
	}
	if got := store.LedgerHead(); got != want {
		t.Fatalf("head changed by compaction: %+v vs %+v", got, want)
	}
	_ = store.Close()

	report, err := VerifyLedger(filepath.Join(dir, "asql.wal"), VerifyOptions{})
	if err != nil || !report.OK || report.Head != expectedChain(t, all) {
		t.Fatalf("verify after compaction: err=%v failure=%v report=%+v", err, report.Failure, report)
	}
	store = newLedgerStore(t, dir, WithSegmentSize(1024))
	defer store.Close()
	if got := store.LedgerHead(); got != want {
		t.Fatalf("head after compaction+restart %+v, want %+v", got, want)
	}
}

// encodeLegacyV4Frame encodes a record in the pre-Raft v4 format (no Term).
func encodeLegacyV4Frame(r diskRecord) []byte {
	typeEnum, _ := typeStringToEnum(r.Type)
	payload := r.Payload
	if len(payload) > 0 {
		payload = s2.Encode(nil, payload)
	}
	body := []byte{binaryVersionLegacy}
	body = binary.BigEndian.AppendUint64(body, r.LSN)
	body = binary.BigEndian.AppendUint64(body, r.Timestamp)
	body = binary.BigEndian.AppendUint16(body, uint16(len(r.TxID)))
	body = append(body, r.TxID...)
	body = append(body, typeEnum)
	body = append(body, payload...)
	body = binary.BigEndian.AppendUint32(body, crc32.Checksum(body, crc32cTable))
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...)
}

func TestLedgerV4ToV5MigrationInvariance(t *testing.T) {
	var records []ports.WALRecord
	for i := 0; i < 10; i++ {
		for _, r := range ledgerTx(i, 50) {
			r.Term = 0 // v4 records decode with Term=0
			records = append(records, r)
		}
	}
	want := expectedChain(t, records)

	// A legacy monolithic WAL of v4 records, migrated on open.
	dir := t.TempDir()
	var legacy []byte
	for i, r := range records {
		legacy = append(legacy, encodeLegacyV4Frame(diskRecord{LSN: uint64(i + 1), TxID: r.TxID, Type: r.Type, Timestamp: r.Timestamp, Payload: r.Payload})...)
	}
	if err := os.WriteFile(filepath.Join(dir, "asql.wal"), legacy, 0o644); err != nil {
		t.Fatal(err)
	}
	store := newLedgerStore(t, dir)
	if got := store.LedgerHead(); got.Chain != want || got.LSN != uint64(len(records)) {
		t.Fatalf("v4 chain head %+v, want %s", got, want)
	}
	// New v5 records extend the same chain.
	more := ledgerTx(10, 50)
	if _, err := store.AppendBatch(context.Background(), more); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	// The same logical history written as v5 yields the same chain.
	v5 := newLedgerStore(t, t.TempDir())
	defer v5.Close()
	if _, err := v5.AppendBatch(context.Background(), append(append([]ports.WALRecord(nil), records...), more...)); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyLedger(filepath.Join(dir, "asql.wal"), VerifyOptions{})
	if err != nil || !report.OK || report.Head != v5.LedgerHead().Chain {
		t.Fatalf("v4->v5 verify: err=%v failure=%v head=%s want=%s", err, report.Failure, report.Head, v5.LedgerHead().Chain)
	}
	if report.UnanchoredSegments != 1 {
		t.Fatalf("legacy segment should be reported unanchored: %+v", report)
	}
}

func TestLedgerWALGCVerifiesFromSnapshotAnchor(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	base := filepath.Join(dir, "asql.wal")
	store := newLedgerStore(t, dir, WithSegmentSize(1024))
	appendTxs(t, store, 0, 30, 200)
	snapLSN := store.LastLSN() - 4
	snapChain, ok := store.LedgerChainAt(snapLSN)
	if !ok {
		t.Fatal("chain at snapshot LSN must be available")
	}
	appendTxs(t, store, 30, 35, 200)
	if err := store.TruncateBefore(ctx, snapLSN); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	anchor := LedgerAnchor{LSN: snapLSN, Chain: snapChain, Source: "snapshot"}

	report, err := VerifyLedger(base, VerifyOptions{Anchors: []LedgerAnchor{anchor}})
	if err != nil {
		t.Fatal(err)
	}
	if report.OK || report.Failure.Kind != VerifyNotGenesis {
		t.Fatalf("verification from genesis must fail explicitly after GC: %+v", report.Failure)
	}

	report, err = VerifyLedger(base, VerifyOptions{Anchors: []LedgerAnchor{anchor}, FromAnchor: true})
	if err != nil || !report.OK || report.FromGenesis {
		t.Fatalf("verification from snapshot anchor: err=%v failure=%v", err, report.Failure)
	}

	// A wrong snapshot anchor is detected.
	bad := anchor
	bad.Chain[0] ^= 1
	report, _ = VerifyLedger(base, VerifyOptions{Anchors: []LedgerAnchor{bad}, FromAnchor: true})
	if report.OK || report.Failure.Kind != VerifyChainMismatch || report.Failure.AtLSN != snapLSN {
		t.Fatalf("expected mismatch at snapshot anchor: %+v", report.Failure)
	}

	// The truncated store restarts with its head intact.
	store = newLedgerStore(t, dir, WithSegmentSize(1024))
	defer store.Close()
	if h := store.LedgerHead(); !h.Valid || !h.Rooted || h.LSN != 105 {
		t.Fatalf("head after GC restart: %+v", h)
	}
}

func TestLedgerOffWritesV3HeadersAndChainResumesFromGenesis(t *testing.T) {
	dir := t.TempDir()
	store := newLedgerStore(t, dir, WithLedgerMode(LedgerOff))
	all := appendTxs(t, store, 0, 5, 10)
	if h := store.LedgerHead(); h.Valid {
		t.Fatalf("ledger off must report an invalid head: %+v", h)
	}
	if _, ok := store.LedgerChainAt(1); ok {
		t.Fatal("ledger off must not serve chain values")
	}
	path := store.SegmentPaths()[0]
	_ = store.Close()
	data, _ := os.ReadFile(path)
	if data[4] != segmentHeaderVerV3 {
		t.Fatalf("ledger off must write v3 headers, got v%d", data[4])
	}

	store = newLedgerStore(t, dir)
	defer store.Close()
	if h := store.LedgerHead(); !h.Valid || h.Chain != expectedChain(t, all) {
		t.Fatalf("enabling the ledger must rehash from genesis: %+v", h)
	}
}

func TestParseLedgerMode(t *testing.T) {
	for in, want := range map[string]LedgerMode{"": LedgerChain, "chain": LedgerChain, "off": LedgerOff} {
		got, err := ParseLedgerMode(in)
		if err != nil || got != want {
			t.Fatalf("ParseLedgerMode(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParseLedgerMode("signed"); err == nil {
		t.Fatal("signed is not available yet")
	}
}

func TestLedgerAddsNoAllocationsToAppend(t *testing.T) {
	ctx := context.Background()
	tx := ledgerTx(1, 256)
	allocs := func(mode LedgerMode) float64 {
		store := newLedgerStore(t, t.TempDir(), WithLedgerMode(mode))
		defer store.Close()
		_, _ = store.AppendBatchNoSync(ctx, tx) // warm pools
		return testing.AllocsPerRun(200, func() {
			if _, err := store.AppendBatchNoSync(ctx, tx); err != nil {
				t.Fatal(err)
			}
		})
	}
	off, chain := allocs(LedgerOff), allocs(LedgerChain)
	if chain > off {
		t.Fatalf("ledger adds allocations to AppendBatchNoSync: off=%v chain=%v", off, chain)
	}
}

// --- tamper helpers ---

type segmentFile struct {
	path   string
	header segmentHeader
	recs   []diskRecord
}

func readSegmentFile(t *testing.T, path string) segmentFile {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := decodeSegmentHeader(data)
	if err != nil {
		t.Fatal(err)
	}
	recs, _, err := scanRecordsFromBytes(data[h.size() : h.size()+int64(h.DataSize)])
	if err != nil {
		t.Fatal(err)
	}
	return segmentFile{path: path, header: h, recs: recs}
}

// write re-encodes the records with valid CRCs. The header's size, LSN range,
// count and CRC are fixed up too, so only the chain can reveal the change.
// Its chain anchors are left alone unless the caller forges them.
func (sf segmentFile) write(t *testing.T) {
	t.Helper()
	var frames []byte
	for _, r := range sf.recs {
		frames = append(frames, encodeDiskRecord(r)...)
	}
	h := sf.header
	h.DataSize = uint64(len(frames))
	h.RecordCount = uint32(len(sf.recs))
	h.FirstLSN = sf.recs[0].LSN
	h.LastLSN = sf.recs[len(sf.recs)-1].LSN
	if err := os.WriteFile(sf.path, append(encodeSegmentHeader(h), frames...), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerTamperVariants(t *testing.T) {
	const tampered = uint64(17) // a MUTATION record (LSN 3k+2)
	cases := []struct {
		name   string
		mutate func(sf *segmentFile)
		kind   string
		exact  bool
	}{
		{name: "edit payload", kind: VerifyChainMismatch, mutate: func(sf *segmentFile) {
			sf.recs[tampered-1].Payload = bytes.Replace(sf.recs[tampered-1].Payload, []byte{5}, []byte{6}, 1)
		}},
		{name: "edit timestamp", kind: VerifyChainMismatch, mutate: func(sf *segmentFile) {
			sf.recs[tampered-1].Timestamp++
		}},
		{name: "drop record", kind: VerifyLSNSequence, exact: true, mutate: func(sf *segmentFile) {
			sf.recs = append(sf.recs[:tampered-1], sf.recs[tampered:]...)
		}},
		{name: "reorder records", kind: VerifyLSNSequence, exact: true, mutate: func(sf *segmentFile) {
			sf.recs[tampered-1], sf.recs[tampered] = sf.recs[tampered], sf.recs[tampered-1]
		}},
		{name: "reorder and renumber", kind: VerifyChainMismatch, mutate: func(sf *segmentFile) {
			a, b := sf.recs[tampered-1], sf.recs[tampered]
			a.LSN, b.LSN = b.LSN, a.LSN
			sf.recs[tampered-1], sf.recs[tampered] = b, a
		}},
		{name: "truncate and append", kind: VerifyChainMismatch, mutate: func(sf *segmentFile) {
			sf.recs = sf.recs[:tampered-1]
			for i, r := range ledgerTx(99, 30) {
				r.LSN = tampered + uint64(i)
				sf.recs = append(sf.recs, diskRecord{LSN: r.LSN, Term: r.Term, TxID: r.TxID, Type: r.Type, Timestamp: r.Timestamp, Payload: r.Payload})
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			base := filepath.Join(dir, "asql.wal")
			store := newLedgerStore(t, dir)
			appendTxs(t, store, 0, 10, 40)
			path := store.SegmentPaths()[0]
			_ = store.Close()

			sf := readSegmentFile(t, path)
			tc.mutate(&sf)
			sf.write(t)

			report, err := VerifyLedger(base, VerifyOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if report.OK {
				t.Fatal("tampering went undetected")
			}
			f := report.Failure
			if f.Kind != tc.kind {
				t.Fatalf("failure kind %q, want %q (%s)", f.Kind, tc.kind, f)
			}
			if tampered <= f.AfterLSN || tampered > f.AtLSN {
				t.Fatalf("tampered LSN %d outside reported interval (%d, %d]", tampered, f.AfterLSN, f.AtLSN)
			}
			if tc.exact && (!f.Exact() || f.AtLSN != tampered) {
				t.Fatalf("expected exact LSN %d, got %s", tampered, f)
			}

			// The store refuses to open the modified active segment.
			if _, err := NewSegmentedLogStore(base, AlwaysSync{}); !errors.Is(err, ErrLedgerMismatch) {
				t.Fatalf("open after tamper: err=%v, want ErrLedgerMismatch", err)
			}
		})
	}
}

// An attacker who also recomputes the header anchors is caught by an
// anchor held outside the WAL, such as a snapshot.
func TestLedgerTamperWithForgedHeaderCaughtBySnapshotAnchor(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "asql.wal")
	store := newLedgerStore(t, dir)
	appendTxs(t, store, 0, 10, 40)
	anchorChain, _ := store.LedgerChainAt(21)
	path := store.SegmentPaths()[0]
	_ = store.Close()

	sf := readSegmentFile(t, path)
	sf.recs[16].Payload = []byte("forged")
	var records []ports.WALRecord
	for _, r := range sf.recs {
		records = append(records, ports.WALRecord{Term: r.Term, TxID: r.TxID, Type: r.Type, Timestamp: r.Timestamp, Payload: r.Payload})
	}
	sf.header.ChainLast = expectedChain(t, records)
	sf.write(t)

	if report, _ := VerifyLedger(base, VerifyOptions{}); !report.OK {
		t.Fatalf("a fully re-forged chain is consistent without external anchors: %v", report.Failure)
	}
	report, _ := VerifyLedger(base, VerifyOptions{Anchors: []LedgerAnchor{{LSN: 21, Chain: anchorChain, Source: "snapshot"}}})
	if report.OK || report.Failure.AtLSN != 21 || report.Failure.AfterLSN != 0 {
		t.Fatalf("expected snapshot anchor mismatch at LSN 21: %+v", report.Failure)
	}
}
