package wal

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/correodabid/asql/internal/engine/ports"
)

// LedgerLeaves returns the ledger leaves of the records at LSNs
// fromLSN..toLSN, in order, for the log tree of ADR 0008. It returns fewer
// when the WAL ends before toLSN, and none when it does not hold fromLSN
// (truncated before it, or not written yet). A gap after fromLSN is an
// error.
//
// It never reads a segment file under the store lock, so it does not stall
// appends: records still in the recent-read cache are copied under the lock,
// and older ones are read through a separate handle on their segment. A
// segment compacted or removed meanwhile makes it fail; the caller retries.
func (store *SegmentedLogStore) LedgerLeaves(ctx context.Context, fromLSN, toLSN uint64) ([]ChainHash, error) {
	if fromLSN == 0 || toLSN < fromLSN {
		return nil, fmt.Errorf("ledger leaves: bad range %d..%d", fromLSN, toLSN)
	}
	type segmentRef struct {
		path       string
		headerSize int64
	}
	store.mu.Lock()
	limit := int(toLSN - fromLSN + 1)
	cached, ok := store.readRecentLocked(fromLSN, limit)
	var refs []segmentRef
	if !ok {
		for i := store.findSegmentIndexForLSN(fromLSN); i < len(store.segments); i++ {
			seg := store.segments[i]
			if seg.firstLSN > toLSN {
				break
			}
			refs = append(refs, segmentRef{path: store.segmentPath(seg.seqNum), headerSize: seg.headerSize})
		}
	}
	store.mu.Unlock()

	records := cached
	for _, ref := range refs {
		if len(records) >= limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		next := fromLSN + uint64(len(records))
		read, err := readSegmentRecordsFrom(ref.path, ref.headerSize, next, limit-len(records))
		if err != nil {
			return nil, err
		}
		records = append(records, read...)
	}

	if len(records) > 0 && records[0].LSN > fromLSN {
		// Truncated before fromLSN: the WAL does not hold it.
		return nil, nil
	}
	leaves := make([]ChainHash, 0, len(records))
	for i, record := range records {
		if want := fromLSN + uint64(i); record.LSN != want {
			return nil, fmt.Errorf("ledger leaves: expected LSN %d, found LSN %d", want, record.LSN)
		}
		leaf, err := LedgerLeaf(record)
		if err != nil {
			return nil, fmt.Errorf("ledger leaves: LSN %d: %w", record.LSN, err)
		}
		leaves = append(leaves, leaf)
	}
	return leaves, nil
}

// readSegmentRecordsFrom reads up to limit records from LSN fromLSN on out of
// one segment file through its own handle.
func readSegmentRecordsFrom(path string, headerSize int64, fromLSN uint64, limit int) ([]ports.WALRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("ledger leaves: %w", err)
	}
	defer f.Close()
	// A segment compacted after the lock was released starts with the WALZ
	// magic and carries its header inside the compressed body.
	var peek [len(walzMagic)]byte
	if _, err := io.ReadFull(f, peek[:]); err == nil && string(peek[:]) == walzMagic {
		headerSize = 0
	}
	records, _, err := scanSegmentRecordsFromLSN(f, fromLSN, limit, headerSize)
	if err != nil {
		return nil, fmt.Errorf("ledger leaves: read %s: %w", path, err)
	}
	return records, nil
}
