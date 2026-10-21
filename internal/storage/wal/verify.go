package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LedgerAnchor is a chain value recorded outside the WAL frames, for example
// in a snapshot, against which verification checks the recomputed chain.
type LedgerAnchor struct {
	LSN    uint64
	Chain  ChainHash
	Source string
}

// VerifyOptions configures VerifyLedger.
type VerifyOptions struct {
	// Anchors are external chain values, typically read from snapshots.
	Anchors []LedgerAnchor
	// FromAnchor allows a WAL whose first retained record is not LSN 1 (after
	// WAL GC) to be verified starting from the first segment's
	// chainBeforeFirstLSN. At least one external anchor must then fall inside
	// the verified range, tying the retained suffix to a snapshot. Without
	// FromAnchor such a WAL fails verification, because it cannot be checked
	// from genesis.
	FromAnchor bool
	// OnLeaf, when set, receives every verified record's ledger leaf in LSN
	// order, so a caller can rebuild the log tree (ADR 0008) in the same
	// pass.
	OnLeaf func(lsn uint64, leaf ChainHash)
}

// Verification failure kinds.
const (
	VerifyChainMismatch = "chain-mismatch"
	VerifyLSNSequence   = "lsn-sequence"
	VerifyDecode        = "decode"
	VerifyNotGenesis    = "not-genesis"
	VerifyNoAnchor      = "no-anchor"
)

// VerifyFailure locates the first point where the WAL stops matching the
// chain. The modification lies in the LSN interval (AfterLSN, AtLSN]: AfterLSN
// is the last LSN confirmed by an anchor (or the verification start) and AtLSN
// is where the mismatch was observed. When AtLSN == AfterLSN+1 the LSN is exact.
type VerifyFailure struct {
	Kind     string
	AfterLSN uint64
	AtLSN    uint64
	Segment  string
	Anchor   string
	Message  string
}

// Exact reports whether the failure pins down a single LSN.
func (f *VerifyFailure) Exact() bool { return f.AtLSN == f.AfterLSN+1 }

func (f *VerifyFailure) String() string {
	loc := fmt.Sprintf("LSN %d", f.AtLSN)
	if !f.Exact() {
		loc = fmt.Sprintf("LSN range %d-%d", f.AfterLSN+1, f.AtLSN)
	}
	return fmt.Sprintf("%s at %s: %s", f.Kind, loc, f.Message)
}

// VerifyReport is the outcome of VerifyLedger.
type VerifyReport struct {
	OK          bool
	FromGenesis bool
	Segments    int
	// UnanchoredSegments counts segments without ledger anchors in their
	// header (written before the ledger existed or with the ledger off).
	// Their records are still covered by the recomputed chain.
	UnanchoredSegments int
	Records            uint64
	StartLSN           uint64 // chain value at StartLSN is the verification start
	LastLSN            uint64
	Head               ChainHash
	AnchorsChecked     int
	Failure            *VerifyFailure
	Warnings           []string
}

type verifySegment struct {
	name   string
	header segmentHeader
	hasHdr bool
	exact  bool // header DataSize matched the file
	frames []byte
}

// VerifyLedger recomputes the ledger chain over every record of the
// segmented WAL at basePath and checks it against segment header anchors and
// opts.Anchors. It never modifies files. Integrity failures are reported in
// the returned VerifyReport; the error is reserved for I/O problems.
func VerifyLedger(basePath string, opts VerifyOptions) (VerifyReport, error) {
	var report VerifyReport
	names, err := listSegmentFiles(basePath)
	if err != nil {
		return report, err
	}
	if len(names) == 0 {
		return report, fmt.Errorf("no wal segments found at %s", basePath)
	}

	external := make(map[uint64][]LedgerAnchor, len(opts.Anchors))
	for _, a := range opts.Anchors {
		external[a.LSN] = append(external[a.LSN], a)
	}
	externalUsed := 0

	v := &verifier{report: &report}
	for i, name := range names {
		seg, err := readVerifySegment(filepath.Join(filepath.Dir(basePath), name))
		if err != nil {
			return report, err
		}
		seg.name = name
		report.Segments++
		if seg.header.Version != segmentHeaderVerV4 || seg.header.LedgerFlags&(ledgerFlagBefore|ledgerFlagLast) == 0 {
			report.UnanchoredSegments++
		}
		if !seg.exact && seg.hasHdr {
			report.Warnings = append(report.Warnings,
				fmt.Sprintf("%s: header DataSize does not match the file; its chainAtLastLSN anchor is not checked", name))
		}

		records, err := v.decodeFrames(seg)
		if err != nil || report.Failure != nil {
			break
		}

		if i == 0 && !v.start(seg, records, opts) {
			break
		}

		before := seg.header.LedgerFlags&ledgerFlagBefore != 0
		if i > 0 && before && seg.header.Version == segmentHeaderVerV4 {
			if !v.check(seg.header.ChainBefore, seg.name, "segment header chainBeforeFirstLSN") {
				break
			}
		}

		for _, r := range records {
			if r.LSN != v.lsn+1 {
				v.fail(VerifyLSNSequence, v.lsn, v.lsn+1, seg.name, "",
					fmt.Sprintf("expected LSN %d, found LSN %d (record dropped, duplicated or reordered)", v.lsn+1, r.LSN))
				break
			}
			typeEnum, err := typeStringToEnum(r.Type)
			if err != nil {
				v.fail(VerifyDecode, v.lsn, r.LSN, seg.name, "", err.Error())
				break
			}
			leaf := ledgerLeaf(r.Term, r.Timestamp, r.TxID, typeEnum, r.Payload)
			v.chain = ChainStep(v.chain, r.LSN, leaf)
			if opts.OnLeaf != nil {
				opts.OnLeaf(r.LSN, leaf)
			}
			v.lsn = r.LSN
			report.Records++
			for _, a := range external[r.LSN] {
				externalUsed++
				if !v.check(a.Chain, seg.name, a.Source) {
					break
				}
			}
			if report.Failure != nil {
				break
			}
		}
		if report.Failure != nil {
			break
		}

		if seg.exact && seg.header.Version == segmentHeaderVerV4 && seg.header.LedgerFlags&ledgerFlagLast != 0 {
			if seg.header.LastLSN != v.lsn {
				v.fail(VerifyChainMismatch, v.good, max(v.lsn, seg.header.LastLSN), seg.name, "segment header",
					fmt.Sprintf("header lastLSN is %d but the segment ends at LSN %d", seg.header.LastLSN, v.lsn))
				break
			}
			if !v.check(seg.header.ChainLast, seg.name, "segment header chainAtLastLSN") {
				break
			}
		}
	}

	report.LastLSN = v.lsn
	report.Head = v.chain
	if report.Failure == nil && !report.FromGenesis && opts.FromAnchor && externalUsed == 0 {
		v.fail(VerifyNoAnchor, report.StartLSN, report.LastLSN, "", "",
			"no snapshot anchor falls inside the retained WAL; the retained suffix is not tied to a snapshot")
	}
	for _, a := range opts.Anchors {
		if a.LSN <= report.StartLSN || a.LSN > report.LastLSN {
			report.Warnings = append(report.Warnings,
				fmt.Sprintf("anchor %s at LSN %d is outside the verified range", a.Source, a.LSN))
		}
	}
	report.OK = report.Failure == nil
	return report, nil
}

type verifier struct {
	report *VerifyReport
	chain  ChainHash
	lsn    uint64 // LSN of chain
	good   uint64 // last LSN confirmed by an anchor or the start
}

func (v *verifier) fail(kind string, after, at uint64, segment, anchor, msg string) {
	v.report.Failure = &VerifyFailure{Kind: kind, AfterLSN: after, AtLSN: at, Segment: segment, Anchor: anchor, Message: msg}
}

func (v *verifier) check(want ChainHash, segment, anchor string) bool {
	v.report.AnchorsChecked++
	if want != v.chain {
		v.fail(VerifyChainMismatch, v.good, v.lsn, segment, anchor,
			fmt.Sprintf("recomputed chain %s does not match %s %s", v.chain, anchor, want))
		return false
	}
	v.good = v.lsn
	return true
}

// start positions the chain before the first retained record.
func (v *verifier) start(seg *verifySegment, records []diskRecord, opts VerifyOptions) bool {
	first := uint64(1)
	if len(records) > 0 {
		first = records[0].LSN
	} else if seg.header.FirstLSN > 0 {
		first = seg.header.FirstLSN
	}
	hasBefore := seg.header.Version == segmentHeaderVerV4 && seg.header.LedgerFlags&ledgerFlagBefore != 0
	if first == 1 {
		v.chain, v.lsn, v.good = GenesisChain, 0, 0
		v.report.FromGenesis = true
		if hasBefore {
			return v.check(seg.header.ChainBefore, seg.name, "segment header chainBeforeFirstLSN")
		}
		return true
	}
	v.report.StartLSN = first - 1
	if !opts.FromAnchor {
		v.fail(VerifyNotGenesis, 0, first, seg.name, "",
			fmt.Sprintf("WAL history starts at LSN %d: earlier records were truncated (WAL GC), so the chain cannot be verified from genesis; verify the retained suffix from the snapshot anchor instead", first))
		return false
	}
	for _, a := range opts.Anchors {
		if a.LSN == first-1 {
			v.chain, v.lsn, v.good = a.Chain, first-1, first-1
			return true
		}
	}
	if !hasBefore {
		v.fail(VerifyNoAnchor, 0, first, seg.name, "",
			fmt.Sprintf("first retained segment has no chainBeforeFirstLSN and no anchor at LSN %d", first-1))
		return false
	}
	if seg.header.LedgerFlags&ledgerFlagReanchored != 0 {
		v.report.Warnings = append(v.report.Warnings,
			fmt.Sprintf("%s: chain was re-anchored after an unanchored truncated prefix", seg.name))
	}
	v.chain, v.lsn, v.good = seg.header.ChainBefore, first-1, first-1
	return true
}

func (v *verifier) decodeFrames(seg *verifySegment) ([]diskRecord, error) {
	data := seg.frames
	var records []diskRecord
	lsn := v.lsn
	for len(data) > 0 {
		if len(data) < 4 {
			v.report.Warnings = append(v.report.Warnings, fmt.Sprintf("%s: ignored truncated trailing frame", seg.name))
			break
		}
		n := int(binary.BigEndian.Uint32(data[:4]))
		if len(data) < 4+n {
			v.report.Warnings = append(v.report.Warnings, fmt.Sprintf("%s: ignored truncated trailing frame", seg.name))
			break
		}
		r, err := decodeDiskRecord(data[4 : 4+n])
		if err != nil {
			at := lsn + 1
			if n >= 9 {
				at = binary.BigEndian.Uint64(data[5:13])
			}
			v.fail(VerifyDecode, lsn, at, seg.name, "", err.Error())
			return nil, nil
		}
		lsn = r.LSN
		records = append(records, r)
		data = data[4+n:]
	}
	return records, nil
}

func readVerifySegment(path string) (*verifySegment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read wal segment: %w", err)
	}
	seg := &verifySegment{}
	if len(data) >= 4 && string(data[:4]) == walzMagic {
		raw, err := walzDecoder.DecodeAll(data[4:], nil)
		if err != nil {
			return nil, fmt.Errorf("decode walz segment %s: %w", filepath.Base(path), err)
		}
		seg.frames, seg.header, seg.hasHdr = splitSegmentHeaderFromBytes(raw)
		seg.exact = seg.hasHdr
		return seg, nil
	}
	header, err := decodeSegmentHeader(data)
	if err != nil {
		seg.frames = data
		return seg, nil
	}
	seg.header, seg.hasHdr = header, true
	size := header.size()
	end := size + int64(header.DataSize)
	if end >= size && end <= int64(len(data)) {
		seg.exact = true
		seg.frames = data[size:end]
	} else {
		seg.frames = data[size:]
	}
	return seg, nil
}

// listSegmentFiles returns the segment file names for basePath in sequence order.
func listSegmentFiles(basePath string) ([]string, error) {
	dir := filepath.Dir(basePath)
	base := filepath.Base(basePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read wal directory: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), base+".") {
			continue
		}
		suffix := strings.TrimPrefix(e.Name(), base+".")
		if len(suffix) != 6 || strings.Trim(suffix, "0123456789") != "" {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}
