package raft

import (
	"context"
	"fmt"
	"sync"

	"asql/internal/engine/ports"
)

// Entry is a single Raft log entry, mirroring a WAL record with Raft metadata.
//
// Index = WAL LSN.  ASQL reuses the WAL as the Raft log, so every committed Raft entry
// is also a persisted WAL record.  This avoids double-writes: the WAL IS the Raft log.
type Entry struct {
	Index   uint64 // = WAL LSN
	Term    uint64 // Raft term in which this entry was created
	TxID    string
	Type    string
	Payload []byte
}

// Log is the interface the RaftNode uses to read/write log entries.
// The implementation wraps a WAL store so that Raft entries and WAL records
// are a single unified, append-only, durable sequence.
type Log interface {
	// LastIndex returns the highest committed log index (LSN), or 0 if empty.
	LastIndex() uint64
	// LastTerm returns the Raft term of the last entry, or 0 if empty.
	LastTerm() uint64
	// Term returns the Raft term stored at a specific index, or ErrIndexOutOfRange.
	Term(index uint64) (uint64, error)
	// Entries returns a slice of entries in [from, to) order.
	Entries(ctx context.Context, from, to uint64) ([]Entry, error)
	// AppendLeader appends a new entry as leader, assigning the next LSN.
	// Returns the appended entry (with Index populated).
	AppendLeader(ctx context.Context, term uint64, typ, txID string, payload []byte) (Entry, error)
	// AppendLeaderBatch appends multiple entries as leader with a single
	// write+sync.  Falls back to per-record AppendLeader when the store
	// doesn't support batch writes.
	AppendLeaderBatch(ctx context.Context, term uint64, records []BatchRecord) ([]Entry, error)
	// AppendFollower applies a batch of entries received via AppendEntries.
	// Each entry's Index must equal lastIndex+1 of the previous entry.
	AppendFollower(ctx context.Context, entries []Entry) error
	// TruncateAfter removes all entries with Index > afterIndex.
	// Used when a leader sends entries that conflict with our log (§5.3).
	TruncateAfter(ctx context.Context, afterIndex uint64) error
}

// walLog adapts a segmented WAL store to the Log interface.
// It keeps an in-memory index of (LSN → Term) for fast term lookups without
// re-reading the full WAL on every vote check.
//
// Invariants:
//   - All entries are stored as ports.WALRecord with Term populated.
//   - Index == LSN everywhere in this package.
type walLog struct {
	mu    sync.Mutex
	store walAppender
	terms []termEntry // compact in-memory index; rebuilt on open
}

type overlapMatcher interface {
	MatchEntriesPrefix(entries []Entry) (appendFrom int, conflictIndex uint64, err error)
}

// walAppender is the subset of SegmentedLogStore used by walLog.
// Declared as an interface so it can be substituted in tests.
type walAppender interface {
	Append(ctx context.Context, r ports.WALRecord) (uint64, error)
	AppendReplicated(ctx context.Context, r ports.WALRecord) error
	TruncateBefore(ctx context.Context, beforeLSN uint64) error
	ReadFrom(ctx context.Context, fromLSN uint64, limit int) ([]ports.WALRecord, error)
	LastLSN() uint64
}

// termEntry associates an LSN range [from, from+count) with a single term.
// This compresses the in-memory index: consecutive entries with the same term
// are stored as one range entry instead of one entry per LSN.
type termEntry struct {
	from  uint64 // first LSN in this range (inclusive)
	count uint64 // number of consecutive entries with this term
	term  uint64
}

// NewWALLog wraps a walAppender (typically *wal.SegmentedLogStore) and pre-loads
// the term index by reading all existing WAL records.
func NewWALLog(ctx context.Context, store walAppender) (Log, error) {
	wl := &walLog{store: store}
	if err := wl.rebuildIndex(ctx); err != nil {
		return nil, fmt.Errorf("raft: build log index: %w", err)
	}
	return wl, nil
}

// rebuildIndex scans the WAL from LSN 1 and populates the in-memory term index.
func (wl *walLog) rebuildIndex(ctx context.Context) error {
	records, err := wl.store.ReadFrom(ctx, 1, 0)
	if err != nil {
		return err
	}
	wl.terms = wl.terms[:0]
	for _, r := range records {
		wl.appendTermRange(r.LSN, r.Term)
	}
	return nil
}

// appendTermRange extends the compact term index with a new (lsn, term) pair.
func (wl *walLog) appendTermRange(lsn, term uint64) {
	if len(wl.terms) > 0 {
		last := &wl.terms[len(wl.terms)-1]
		if last.term == term && last.from+last.count == lsn {
			last.count++
			return
		}
	}
	wl.terms = append(wl.terms, termEntry{from: lsn, count: 1, term: term})
}

// LastIndex returns the current highest LSN.
func (wl *walLog) LastIndex() uint64 {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	return wl.store.LastLSN()
}

// LastTerm returns the term of the last WAL entry.
func (wl *walLog) LastTerm() uint64 {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	last := wl.store.LastLSN()
	if last == 0 {
		return 0
	}
	t, _ := wl.termLocked(last)
	return t
}

// Term returns the Raft term for a given LSN.
func (wl *walLog) Term(index uint64) (uint64, error) {
	wl.mu.Lock()
	defer wl.mu.Unlock()
	return wl.termLocked(index)
}

func (wl *walLog) termLocked(index uint64) (uint64, error) {
	// Binary search the compact term index.
	lo, hi := 0, len(wl.terms)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		te := wl.terms[mid]
		if index < te.from {
			hi = mid - 1
		} else if index >= te.from+te.count {
			lo = mid + 1
		} else {
			return te.term, nil
		}
	}
	// Fall through: not in compact index — this means index is out of range.
	last := wl.store.LastLSN()
	if index > last {
		return 0, fmt.Errorf("%w: index=%d last=%d", ErrIndexOutOfRange, index, last)
	}
	// index ≤ last but not in index: record has term=0 (pre-Raft WAL record).
	return 0, nil
}

func (wl *walLog) findTermRangeLocked(index uint64) (pos int, found bool) {
	lo, hi := 0, len(wl.terms)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		te := wl.terms[mid]
		if index < te.from {
			hi = mid - 1
		} else if index >= te.from+te.count {
			lo = mid + 1
		} else {
			return mid, true
		}
	}
	return lo, false
}

// MatchEntriesPrefix returns how many leading entries already exist locally
// with identical terms, plus the first conflicting index when a mismatch is
// found. It scans compact term ranges under a single walLog lock to avoid the
// repeated lock/unlock and binary-search cost of calling Term() per entry.
func (wl *walLog) MatchEntriesPrefix(entries []Entry) (appendFrom int, conflictIndex uint64, err error) {
	if len(entries) == 0 {
		return 0, 0, nil
	}

	wl.mu.Lock()
	defer wl.mu.Unlock()

	lastIndex := wl.store.LastLSN()
	for appendFrom < len(entries) && entries[appendFrom].Index <= lastIndex {
		idx := entries[appendFrom].Index
		pos, found := wl.findTermRangeLocked(idx)
		if !found {
			nextIndexed := lastIndex + 1
			if pos < len(wl.terms) {
				nextIndexed = wl.terms[pos].from
			}
			for appendFrom < len(entries) && entries[appendFrom].Index <= lastIndex && entries[appendFrom].Index < nextIndexed {
				if entries[appendFrom].Term != 0 {
					return appendFrom, entries[appendFrom].Index, nil
				}
				appendFrom++
			}
			continue
		}

		te := wl.terms[pos]
		if entries[appendFrom].Term != te.term {
			return appendFrom, entries[appendFrom].Index, nil
		}

		rangeEnd := te.from + te.count - 1
		for appendFrom < len(entries) && entries[appendFrom].Index <= lastIndex && entries[appendFrom].Index <= rangeEnd {
			if entries[appendFrom].Term != te.term {
				return appendFrom, entries[appendFrom].Index, nil
			}
			appendFrom++
		}
	}

	return appendFrom, 0, nil
}

// Entries returns entries [from, to).
func (wl *walLog) Entries(ctx context.Context, from, to uint64) ([]Entry, error) {
	if from >= to {
		return nil, nil
	}
	limit := int(to - from)
	records, err := wl.store.ReadFrom(ctx, from, limit)
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, len(records))
	for i, r := range records {
		entries[i] = Entry{
			Index:   r.LSN,
			Term:    r.Term,
			TxID:    r.TxID,
			Type:    r.Type,
			Payload: r.Payload,
		}
	}
	return entries, nil
}

// AppendLeader appends a new entry as leader, assigning the next monotonic LSN.
func (wl *walLog) AppendLeader(ctx context.Context, term uint64, typ, txID string, payload []byte) (Entry, error) {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	lsn, err := wl.store.Append(ctx, ports.WALRecord{
		Term:    term,
		TxID:    txID,
		Type:    typ,
		Payload: payload,
	})
	if err != nil {
		return Entry{}, err
	}
	wl.appendTermRange(lsn, term)
	return Entry{Index: lsn, Term: term, TxID: txID, Type: typ, Payload: payload}, nil
}

// batchAppender is the optional batch-write interface.
type batchAppender interface {
	AppendBatch(ctx context.Context, records []ports.WALRecord) ([]uint64, error)
}

// AppendLeaderBatch appends multiple entries as leader in a single
// write+sync, dramatically reducing fsync overhead for commit batches.
// Falls back to per-record Append when the store doesn't support
// AppendBatch.
func (wl *walLog) AppendLeaderBatch(ctx context.Context, term uint64, records []BatchRecord) ([]Entry, error) {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	// Fast path: if the store supports batch appending,
	// do a single write+sync for all records.
	if ba, ok := wl.store.(batchAppender); ok {
		walRecords := make([]ports.WALRecord, len(records))
		for i, r := range records {
			walRecords[i] = ports.WALRecord{
				Term:    term,
				TxID:    r.TxID,
				Type:    r.Type,
				Payload: r.Payload,
			}
		}
		lsns, err := ba.AppendBatch(ctx, walRecords)
		if err != nil {
			return nil, err
		}
		entries := make([]Entry, len(records))
		for i, r := range records {
			wl.appendTermRange(lsns[i], term)
			entries[i] = Entry{Index: lsns[i], Term: term, TxID: r.TxID, Type: r.Type, Payload: r.Payload}
		}
		return entries, nil
	}

	// Fallback: append one at a time.
	entries := make([]Entry, len(records))
	for i, r := range records {
		lsn, err := wl.store.Append(ctx, ports.WALRecord{
			Term:    term,
			TxID:    r.TxID,
			Type:    r.Type,
			Payload: r.Payload,
		})
		if err != nil {
			return nil, err
		}
		wl.appendTermRange(lsn, term)
		entries[i] = Entry{Index: lsn, Term: term, TxID: r.TxID, Type: r.Type, Payload: r.Payload}
	}
	return entries, nil
}

// AppendFollower applies a batch sent by the leader via AppendEntries.
// Entries must be contiguous starting from lastLSN+1.
// replicatedBatchAppender is an optional interface for stores that can batch
// multiple replicated appends into a single write+sync.
type replicatedBatchAppender interface {
	AppendReplicatedBatch(ctx context.Context, records []ports.WALRecord) error
}

func (wl *walLog) AppendFollower(ctx context.Context, entries []Entry) error {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	// Fast path: use batch append if the store supports it.
	if ba, ok := wl.store.(replicatedBatchAppender); ok && len(entries) > 0 {
		records := make([]ports.WALRecord, len(entries))
		for i, e := range entries {
			records[i] = ports.WALRecord{
				LSN:     e.Index,
				Term:    e.Term,
				TxID:    e.TxID,
				Type:    e.Type,
				Payload: e.Payload,
			}
		}
		if err := ba.AppendReplicatedBatch(ctx, records); err != nil {
			return err
		}
		for _, e := range entries {
			wl.appendTermRange(e.Index, e.Term)
		}
		return nil
	}

	// Fallback: per-entry append.
	for _, e := range entries {
		if err := wl.store.AppendReplicated(ctx, ports.WALRecord{
			LSN:     e.Index,
			Term:    e.Term,
			TxID:    e.TxID,
			Type:    e.Type,
			Payload: e.Payload,
		}); err != nil {
			return err
		}
		wl.appendTermRange(e.Index, e.Term)
	}
	return nil
}

// TruncateAfter removes all entries with Index > afterIndex (log rollback).
// This is called when a follower discovers a conflicting entry in AppendEntries.
func (wl *walLog) TruncateAfter(ctx context.Context, afterIndex uint64) error {
	wl.mu.Lock()
	defer wl.mu.Unlock()

	if err := wl.store.TruncateBefore(ctx, afterIndex+2); err != nil {
		return err
	}
	// Trim compact term index.
	for i := len(wl.terms) - 1; i >= 0; i-- {
		te := wl.terms[i]
		if te.from > afterIndex {
			wl.terms = wl.terms[:i]
		} else if te.from+te.count-1 > afterIndex {
			// Partial range: trim the count.
			excess := (te.from + te.count - 1) - afterIndex
			wl.terms[i].count -= excess
			wl.terms = wl.terms[:i+1]
			break
		} else {
			break
		}
	}
	return nil
}
