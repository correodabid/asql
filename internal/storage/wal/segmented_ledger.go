package wal

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
)

// ErrLedgerMismatch reports that WAL contents no longer match a persisted
// ledger anchor: the log was modified after it was written.
var ErrLedgerMismatch = errors.New("wal ledger mismatch")

// commitLedgerLocked advances the chain over records just written at
// firstLSN and records the new head on the active segment. It runs under
// store.mu after a successful write and before lastLSN is published. lb holds
// leaves computed before the lock was taken; it is nil when the ledger is off.
func (store *SegmentedLogStore) commitLedgerLocked(firstLSN uint64, lb *leafBuf) {
	active := store.activeSegment()
	if lb == nil {
		// Ledger off: the segment no longer has a known chain value at its end.
		active.ledgerFlags &^= ledgerFlagLast
		return
	}
	store.ledger.advance(firstLSN, lb)
	store.ledger.commit(firstLSN, lb)
	active.chainLast = store.ledger.head
	active.ledgerFlags |= ledgerFlagLast
}

// LedgerHead returns the current chain head.
func (store *SegmentedLogStore) LedgerHead() LedgerHead {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.ledger.headInfo()
}

// UnanchoredSegments returns the sequence numbers of segments that hold
// records but no ledger anchor: written with the ledger off, or before it
// existed (v3 headers). Their records are still covered by the chain, but
// only the chain recomputed from them vouches for them.
func (store *SegmentedLogStore) UnanchoredSegments() []uint32 {
	store.mu.Lock()
	defer store.mu.Unlock()
	var seqs []uint32
	for _, seg := range store.segments {
		if seg.firstLSN > 0 && seg.ledgerFlags&ledgerFlagBefore == 0 {
			seqs = append(seqs, seg.seqNum)
		}
	}
	return seqs
}

// LedgerChainAt returns the chain value at lsn while it is still among the
// recent points kept in memory. Implements ports.LedgerReader.
func (store *SegmentedLogStore) LedgerChainAt(lsn uint64) ([32]byte, bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	chain, ok := store.ledger.chainAt(lsn)
	return chain, ok
}

// restoreLedger rebuilds the chain head on open. The head is restored from
// the newest trusted anchor, normally the last sealed segment header, and only
// the segments after it, normally just the active one, are rehashed. Rehashed
// segments are checked against their own header anchors, so a modified active
// segment refuses to open instead of being silently re-anchored.
func (store *SegmentedLogStore) restoreLedger() error {
	if store.ledgerMode != LedgerChain || len(store.segments) == 0 {
		return nil
	}
	segs := store.segments
	last := len(segs) - 1

	// beforeLSN[i] is the LSN whose chain value a segment's chainBefore holds.
	beforeLSN := make([]uint64, len(segs))
	var prevLast uint64
	for i, seg := range segs {
		if seg.firstLSN > 0 {
			beforeLSN[i] = seg.firstLSN - 1
		} else {
			beforeLSN[i] = prevLast
		}
		if seg.lastLSN > 0 {
			prevLast = seg.lastLSN
		}
	}

	startIdx := -1
	var chain ChainHash
	var chainLSN uint64
	rooted := true
	for i := last; i >= 0 && startIdx < 0; i-- {
		seg := segs[i]
		reanchored := seg.ledgerFlags&ledgerFlagReanchored != 0
		switch {
		case i < last && seg.ledgerFlags&ledgerFlagLast != 0 && seg.lastLSN > 0:
			startIdx, chain, chainLSN, rooted = i+1, seg.chainLast, seg.lastLSN, !reanchored
		case seg.ledgerFlags&ledgerFlagBefore != 0:
			startIdx, chain, chainLSN, rooted = i, seg.chainBefore, beforeLSN[i], !reanchored
		}
	}
	if startIdx < 0 {
		startIdx, chain = 0, GenesisChain
		if first := beforeLSN[0]; first > 0 {
			// The prefix was truncated before any anchor was written. The chain
			// restarts here and is flagged as not extending back to genesis.
			chainLSN, rooted = first, false
			slog.Warn("wal ledger: history before first retained LSN is unanchored; re-anchoring chain",
				"first_lsn", first+1)
		}
	}
	store.ledger.reset(chainLSN, chain, rooted)

	for i := startIdx; i <= last; i++ {
		if err := store.rehashSegment(segs[i], i == last); err != nil {
			return err
		}
	}
	return nil
}

// rehashSegment advances the chain over every record of seg and checks the
// segment's header anchors.
func (store *SegmentedLogStore) rehashSegment(seg *segment, active bool) error {
	ls := &store.ledger
	if seg.ledgerFlags&ledgerFlagBefore != 0 && seg.chainBefore != ls.head {
		return fmt.Errorf("%w: segment %06d chainBeforeFirstLSN does not match chain at LSN %d; run asqlctl verify-ledger",
			ErrLedgerMismatch, seg.seqNum, ls.headLSN)
	}
	if active && seg.headerSize == segmentHeaderSizeV4 && seg.ledgerFlags&ledgerFlagBefore == 0 {
		seg.chainBefore = ls.head
		seg.ledgerFlags |= ledgerFlagBefore
		if !ls.rooted {
			seg.ledgerFlags |= ledgerFlagReanchored
		}
	}

	records, _, _, err := scanSegmentRecords(seg.file, seg.headerSize)
	if err != nil {
		return fmt.Errorf("wal ledger: rehash segment %06d: %w", seg.seqNum, err)
	}
	if _, err := seg.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("wal ledger: seek segment %06d end: %w", seg.seqNum, err)
	}
	for _, r := range records {
		if r.LSN != ls.headLSN+1 {
			return fmt.Errorf("%w: segment %06d: LSN %d follows %d", ErrLedgerMismatch, seg.seqNum, r.LSN, ls.headLSN)
		}
		typeEnum, err := typeStringToEnum(r.Type)
		if err != nil {
			return fmt.Errorf("wal ledger: segment %06d LSN %d: %w", seg.seqNum, r.LSN, err)
		}
		ls.step(r.LSN, ledgerLeaf(r.Term, r.Timestamp, r.TxID, typeEnum, r.Payload))
	}

	if seg.ledgerFlags&ledgerFlagLast != 0 && seg.headerExact &&
		(seg.lastLSN != ls.headLSN || seg.chainLast != ls.head) {
		return fmt.Errorf("%w: segment %06d: chain at LSN %d does not match chainAtLastLSN (header LSN %d); the segment was modified after it was written; run asqlctl verify-ledger",
			ErrLedgerMismatch, seg.seqNum, ls.headLSN, seg.lastLSN)
	}
	if active && seg.headerSize == segmentHeaderSizeV4 {
		seg.chainLast = ls.head
		seg.ledgerFlags |= ledgerFlagLast
	}
	return nil
}
