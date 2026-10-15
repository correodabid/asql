package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/correodabid/asql/internal/engine/ports"
)

// TruncateAfter removes every WAL record with LSN > afterLSN.
//
// It exists for Raft log rollback: a follower that holds entries from an old
// term which the current leader never had must drop that conflicting suffix
// before it can accept the leader's entries. Those entries were never
// committed, so the engine never applied them; this is the only operation
// that removes records from the tail of the log. Callers must never pass an
// afterLSN below the commit index.
//
// Segments that lie entirely above afterLSN are deleted. The segment holding
// afterLSN is rewritten atomically without the later records, as a raw
// segment even if it had been compacted to WALZ, and becomes the active
// segment. The ledger head is then restored at afterLSN.
func (store *SegmentedLogStore) TruncateAfter(ctx context.Context, afterLSN uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	store.syncMu.Lock()
	defer store.syncMu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return errors.New("wal store is closed")
	}
	if afterLSN >= store.lastLSN {
		return nil
	}
	slog.Warn("wal: truncating log suffix", "after_lsn", afterLSN, "last_lsn", store.lastLSN)

	// Drop segments that hold only records above afterLSN.
	for len(store.segments) > 0 {
		seg := store.segments[len(store.segments)-1]
		if seg.firstLSN != 0 && seg.firstLSN <= afterLSN {
			break
		}
		path := store.segmentPath(seg.seqNum)
		if err := seg.file.Close(); err != nil {
			return fmt.Errorf("close segment %06d: %w", seg.seqNum, err)
		}
		seg.file = nil
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove segment %06d: %w", seg.seqNum, err)
		}
		store.totalSize -= seg.size
		store.segments = store.segments[:len(store.segments)-1]
	}

	// Cut the segment that holds afterLSN.
	if n := len(store.segments); n > 0 && store.segments[n-1].lastLSN > afterLSN {
		if err := store.rewriteSegmentThrough(store.segments[n-1], afterLSN); err != nil {
			return err
		}
	}

	store.lastLSN = afterLSN
	if len(store.recentRecords) > store.recentStart {
		window := store.recentRecords[store.recentStart:]
		filtered := make([]ports.WALRecord, 0, len(window))
		for _, record := range window {
			if record.LSN <= afterLSN {
				filtered = append(filtered, record)
			}
		}
		store.recentRecords = filtered
		store.recentStart = 0
	}

	// Rebuild the ledger head at afterLSN from the newest trusted anchor.
	store.ledger = newLedgerState(store.ledgerMode)
	if len(store.segments) == 0 && afterLSN > 0 && store.ledger.active() {
		// Nothing below the cut is retained: re-anchor, as restoreLedger does
		// for a truncated, unanchored prefix.
		store.ledger.reset(afterLSN, GenesisChain, false)
	}
	if err := store.restoreLedger(); err != nil {
		return fmt.Errorf("restore ledger after truncation: %w", err)
	}

	// Appends need a raw active segment.
	if n := len(store.segments); n == 0 || store.segments[n-1].compressed {
		if err := store.createSegment(); err != nil {
			return fmt.Errorf("create active segment after truncation: %w", err)
		}
	}

	// Persist the active header now: it may have been sealed, and its ledger
	// anchors changed.
	active := store.activeSegment()
	if err := store.persistSegmentHeader(active, false); err != nil {
		return fmt.Errorf("persist active segment header after truncation: %w", err)
	}
	if err := syncWithRetry(active.file); err != nil {
		return fmt.Errorf("sync active segment after truncation: %w", err)
	}
	active.dirty = false
	return nil
}

// rewriteSegmentThrough replaces seg's file with one that holds only its
// records up to and including afterLSN. Must be called under mu and syncMu.
func (store *SegmentedLogStore) rewriteSegmentThrough(seg *segment, afterLSN uint64) error {
	records, _, _, err := scanSegmentRecords(seg.file, seg.headerSize)
	if err != nil {
		return fmt.Errorf("read segment %06d for truncation: %w", seg.seqNum, err)
	}
	var frames []byte
	var kept uint32
	for _, r := range records {
		if r.LSN > afterLSN {
			break
		}
		frames = append(frames, encodeDiskRecord(r)...)
		kept++
	}

	if seg.compressed {
		// A WALZ segment's header lives inside the compressed payload; the
		// rewritten raw segment gets an outer header again.
		seg.headerSize = segmentHeaderSizeV3
		if store.ledgerMode == LedgerChain {
			seg.headerSize = segmentHeaderSizeV4
		}
	}
	seg.lastLSN = afterLSN
	seg.recordCount = kept
	seg.compressed = false
	seg.headerExact = true
	// The chain value at the new end is recomputed by restoreLedger.
	seg.ledgerFlags &^= ledgerFlagLast
	oldSize := seg.size
	seg.size = seg.headerSize + int64(len(frames))

	path := store.segmentPath(seg.seqNum)
	tmpPath := path + ".truncatetmp"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create truncation temp file: %w", err)
	}
	success := false
	defer func() {
		if !success {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()
	if seg.headerSize > 0 {
		if _, err := tmp.Write(encodeSegmentHeader(store.segmentHeaderFor(seg, false))); err != nil {
			return fmt.Errorf("write truncated segment header: %w", err)
		}
	}
	if _, err := tmp.Write(frames); err != nil {
		return fmt.Errorf("write truncated segment: %w", err)
	}
	if err := PlatformSync(tmp); err != nil {
		return fmt.Errorf("sync truncated segment: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename truncated segment: %w", err)
	}
	success = true
	if _, err := tmp.Seek(0, io.SeekEnd); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("seek truncated segment end: %w", err)
	}

	_ = seg.file.Close()
	seg.file = tmp
	seg.dirty = false
	store.totalSize += seg.size - oldSize
	return nil
}
