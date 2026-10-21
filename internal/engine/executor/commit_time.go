package executor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
)

// Commit wall-clock times (ADR 0007). The leader writes the time of each
// commit in the payload of its COMMIT record, so replays, replicas and
// snapshots resolve a date to the same commit, and the ledger covers it.
// Older COMMIT records have an empty payload and no time.

// commitHeaderVersion is the first byte of a COMMIT payload; it is followed
// by the commit time in Unix microseconds, big-endian.
const (
	commitHeaderVersion byte = 0x01
	commitHeaderSize         = 1 + 8
)

func encodeCommitHeader(micros int64) []byte {
	b := make([]byte, commitHeaderSize)
	b[0] = commitHeaderVersion
	binary.BigEndian.PutUint64(b[1:], uint64(micros))
	return b
}

// commitTimeOf returns the commit time a COMMIT record carries.
func commitTimeOf(record ports.WALRecord) (int64, bool) {
	if record.Type != walTypeCommit || len(record.Payload) != commitHeaderSize || record.Payload[0] != commitHeaderVersion {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(record.Payload[1:])), true
}

// ErrCommitTimeUnknown reports a date that cannot be resolved to a commit:
// it is earlier than the first commit with a recorded time, and commits
// without one came before it.
var ErrCommitTimeUnknown = errors.New("commit time unknown")

const (
	commitTimeIndexFileName = "commit-time.idx"
	commitTimeIndexMagic    = "ASQLCTI1"
	// header: magic, 1 flag byte (bit 0: commits without a time precede
	// the first entry); then entries of u64 LSN (top bit: untimed) and i64
	// Unix µs.
	commitTimeIndexHeaderSize = len(commitTimeIndexMagic) + 1
	commitTimeIndexEntrySize  = 16
)

type commitTimeEntry struct {
	lsn    uint64
	micros int64
	// untimed marks a commit without a recorded time after commits with
	// one (written by an older version after a downgrade). It carries the
	// time of the commit before it, so times still do not decrease, but a
	// date that resolves to it cannot tell it from the commits around it.
	untimed bool
}

// commitTimeUntimedBit marks an untimed entry's LSN in the index file.
const commitTimeUntimedBit = uint64(1) << 63

// commitTimeIndex maps commit times to commit LSNs. Times never decrease
// with the LSN (ADR 0007 § 2), so a date resolves by binary search.
type commitTimeIndex struct {
	mu      sync.RWMutex
	entries []commitTimeEntry
	// untimed is true when commits without a recorded time come before the
	// first entry, or may (the WAL it was built from was truncated).
	untimed bool
	// scanned is the last LSN the index has seen.
	scanned  uint64
	filePath string
}

func newCommitTimeIndex(snapDir string) *commitTimeIndex {
	index := &commitTimeIndex{}
	if snapDir != "" {
		index.filePath = filepath.Join(filepath.Dir(snapDir), commitTimeIndexFileName)
	}
	return index
}

// resolve returns the LSN of the last commit at or before micros, or 0 when
// no commit is that old and the state then was the empty database.
func (index *commitTimeIndex) resolve(micros int64) (uint64, error) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	pos := sort.Search(len(index.entries), func(i int) bool { return index.entries[i].micros > micros })
	if pos > 0 {
		entry := index.entries[pos-1]
		if entry.untimed {
			return 0, fmt.Errorf("%w: commits up to LSN %d recorded no time, so the state at that time cannot be told",
				ErrCommitTimeUnknown, entry.lsn)
		}
		return entry.lsn, nil
	}
	if !index.untimed {
		return 0, nil
	}
	if len(index.entries) == 0 {
		return 0, fmt.Errorf("%w: no commit has a recorded time yet", ErrCommitTimeUnknown)
	}
	first := index.entries[0]
	return 0, fmt.Errorf("%w: the first commit with a recorded time is LSN %d at %s; earlier commits have none",
		ErrCommitTimeUnknown, first.lsn, time.UnixMicro(first.micros).UTC().Format(time.RFC3339Nano))
}

// last returns the time of the last commit, or 0.
func (index *commitTimeIndex) last() int64 {
	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.entries) == 0 {
		return 0
	}
	return index.entries[len(index.entries)-1].micros
}

func (index *commitTimeIndex) scannedLSN() uint64 {
	index.mu.RLock()
	defer index.mu.RUnlock()
	return index.scanned
}

// timeAt returns the recorded time of the commit at lsn.
func (index *commitTimeIndex) timeAt(lsn uint64) (int64, bool) {
	index.mu.RLock()
	defer index.mu.RUnlock()
	pos := sort.Search(len(index.entries), func(i int) bool { return index.entries[i].lsn >= lsn })
	if pos < len(index.entries) && index.entries[pos].lsn == lsn && !index.entries[pos].untimed {
		return index.entries[pos].micros, true
	}
	return 0, false
}

// observe adds the commits of records, which follow what the index has seen.
// It returns the new entries.
func (index *commitTimeIndex) observe(records []ports.WALRecord) []commitTimeEntry {
	var added []commitTimeEntry
	for _, record := range records {
		if record.LSN <= index.scanned {
			continue
		}
		index.scanned = record.LSN
		if record.Type != walTypeCommit {
			continue
		}
		micros, ok := commitTimeOf(record)
		var entry commitTimeEntry
		switch {
		case ok:
			entry = commitTimeEntry{lsn: record.LSN, micros: micros}
		case len(index.entries) == 0:
			index.untimed = true
			continue
		default:
			entry = commitTimeEntry{lsn: record.LSN, micros: index.entries[len(index.entries)-1].micros, untimed: true}
		}
		index.entries = append(index.entries, entry)
		added = append(added, entry)
	}
	return added
}

// rebuild replaces the index with the commits of records, the whole
// retained WAL.
func (index *commitTimeIndex) rebuild(records []ports.WALRecord) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	index.entries, index.scanned = nil, 0
	// A WAL that does not start at LSN 1 lost its oldest commits to WAL GC.
	index.untimed = len(records) > 0 && records[0].LSN > 1
	index.observe(records)
	return index.writeFileLocked()
}

// append adds the commits of records that follow what the index has seen.
func (index *commitTimeIndex) append(records []ports.WALRecord) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	untimedBefore := index.untimed
	added := index.observe(records)
	if index.filePath == "" || (len(added) == 0 && untimedBefore == index.untimed) {
		return nil
	}
	if untimedBefore != index.untimed {
		return index.writeFileLocked()
	}
	if err := appendCommitTimeFile(index.filePath, added); errors.Is(err, os.ErrNotExist) {
		return index.writeFileLocked()
	} else if err != nil {
		return err
	}
	return nil
}

// truncateAfter drops the commits after lsn.
func (index *commitTimeIndex) truncateAfter(lsn uint64) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	pos := sort.Search(len(index.entries), func(i int) bool { return index.entries[i].lsn > lsn })
	index.entries = index.entries[:pos]
	if index.scanned > lsn {
		index.scanned = lsn
	}
	return index.writeFileLocked()
}

// load reads the persisted index. It returns false when there is none.
func (index *commitTimeIndex) load() (bool, error) {
	if index.filePath == "" {
		return false, nil
	}
	data, err := os.ReadFile(index.filePath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(data) < commitTimeIndexHeaderSize || string(data[:len(commitTimeIndexMagic)]) != commitTimeIndexMagic ||
		(len(data)-commitTimeIndexHeaderSize)%commitTimeIndexEntrySize != 0 {
		return false, nil
	}
	entries := make([]commitTimeEntry, 0, (len(data)-commitTimeIndexHeaderSize)/commitTimeIndexEntrySize)
	for off := commitTimeIndexHeaderSize; off < len(data); off += commitTimeIndexEntrySize {
		lsn := binary.BigEndian.Uint64(data[off:])
		entries = append(entries, commitTimeEntry{
			lsn:     lsn &^ commitTimeUntimedBit,
			micros:  int64(binary.BigEndian.Uint64(data[off+8:])),
			untimed: lsn&commitTimeUntimedBit != 0,
		})
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	index.entries = entries
	index.untimed = data[len(commitTimeIndexMagic)]&1 != 0
	index.scanned = 0
	if len(entries) > 0 {
		index.scanned = entries[len(entries)-1].lsn
	}
	return true, nil
}

func (index *commitTimeIndex) writeFileLocked() error {
	if index.filePath == "" {
		return nil
	}
	b := make([]byte, 0, commitTimeIndexHeaderSize+len(index.entries)*commitTimeIndexEntrySize)
	b = append(b, commitTimeIndexMagic...)
	flags := byte(0)
	if index.untimed {
		flags |= 1
	}
	b = append(b, flags)
	b = appendCommitTimeEntries(b, index.entries)
	tmp := index.filePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, index.filePath)
}

func appendCommitTimeFile(path string, entries []commitTimeEntry) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(appendCommitTimeEntries(nil, entries)); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func appendCommitTimeEntries(b []byte, entries []commitTimeEntry) []byte {
	for _, e := range entries {
		lsn := e.lsn
		if e.untimed {
			lsn |= commitTimeUntimedBit
		}
		b = binary.BigEndian.AppendUint64(b, lsn)
		b = binary.BigEndian.AppendUint64(b, uint64(e.micros))
	}
	return b
}
