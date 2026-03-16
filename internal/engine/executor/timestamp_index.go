package executor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"asql/internal/engine/ports"
	"asql/internal/storage/wal"
)

const (
	timestampIndexFileName = "timestamp-lsn.idx"
	timestampIndexMagic    = "ASQLTSI1"
	timestampIndexHeaderSz = len(timestampIndexMagic)
	timestampIndexEntrySz  = 16
)

var errTimestampIndexCorrupt = errors.New("timestamp index file is corrupt")

type timestampLSNEntry struct {
	timestamp uint64
	lsn       uint64
}

type timestampLSNRange struct {
	startTimestamp uint64
	startLSN       uint64
	count          uint64
}

func (r timestampLSNRange) endTimestamp() uint64 {
	if r.count == 0 {
		return r.startTimestamp
	}
	return r.startTimestamp + r.count - 1
}

func (r timestampLSNRange) endLSN() uint64 {
	if r.count == 0 {
		return r.startLSN
	}
	return r.startLSN + r.count - 1
}

type timestampLSNIndex struct {
	mu        sync.RWMutex
	ranges    []timestampLSNRange
	filePath  string
	persistOK bool
}

func newTimestampLSNIndex(snapDir string) *timestampLSNIndex {
	index := &timestampLSNIndex{persistOK: true}
	if snapDir != "" {
		index.filePath = filepath.Join(filepath.Dir(snapDir), timestampIndexFileName)
	}
	return index
}

func (index *timestampLSNIndex) Resolve(logicalTimestamp uint64) uint64 {
	if index == nil {
		return 0
	}

	index.mu.RLock()
	defer index.mu.RUnlock()

	pos := sort.Search(len(index.ranges), func(i int) bool {
		return index.ranges[i].startTimestamp > logicalTimestamp
	})
	if pos == 0 {
		return 0
	}
	r := index.ranges[pos-1]
	offset := logicalTimestamp - r.startTimestamp
	if offset >= r.count {
		offset = r.count - 1
	}
	return r.startLSN + offset
}

func (index *timestampLSNIndex) LastLSN() uint64 {
	if index == nil {
		return 0
	}

	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.ranges) == 0 {
		return 0
	}
	return index.ranges[len(index.ranges)-1].endLSN()
}

func (index *timestampLSNIndex) load() (bool, error) {
	if index == nil || index.filePath == "" {
		return false, nil
	}

	data, err := os.ReadFile(index.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read timestamp index: %w", err)
	}
	ranges, err := decodeCompressedTimestampIndex(data)
	if err != nil {
		return false, err
	}

	index.mu.Lock()
	index.ranges = ranges
	index.persistOK = true
	index.mu.Unlock()
	return true, nil
}

func (index *timestampLSNIndex) rebuild(records []ports.WALRecord) error {
	if index == nil {
		return nil
	}

	entries := timestampEntriesFromRecords(records)

	index.mu.Lock()
	index.ranges = compressTimestampEntries(entries)
	index.persistOK = true
	index.mu.Unlock()

	if index.filePath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(index.filePath), 0o755); err != nil {
		return fmt.Errorf("mkdir timestamp index dir: %w", err)
	}
	if err := writeTimestampIndexFile(index.filePath, entries); err != nil {
		index.mu.Lock()
		index.persistOK = false
		index.mu.Unlock()
		return err
	}
	return nil
}

func (index *timestampLSNIndex) append(records []ports.WALRecord) error {
	if index == nil || len(records) == 0 {
		return nil
	}

	entries := timestampEntriesFromRecords(records)
	if len(entries) == 0 {
		return nil
	}

	index.mu.Lock()
	defer index.mu.Unlock()

	lastLSN := uint64(0)
	if len(index.ranges) > 0 {
		lastLSN = index.ranges[len(index.ranges)-1].endLSN()
	}
	start := 0
	for start < len(entries) && entries[start].lsn <= lastLSN {
		start++
	}
	if start == len(entries) {
		return nil
	}
	entries = entries[start:]
	index.ranges = appendCompressedTimestampEntries(index.ranges, entries)

	if index.filePath == "" || !index.persistOK {
		return nil
	}
	if err := appendTimestampIndexFile(index.filePath, entries); err != nil {
		index.persistOK = false
		return err
	}
	return nil
}

func decodeTimestampIndex(data []byte) ([]timestampLSNEntry, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < timestampIndexHeaderSz || string(data[:timestampIndexHeaderSz]) != timestampIndexMagic {
		return nil, errTimestampIndexCorrupt
	}
	body := data[timestampIndexHeaderSz:]
	if len(body)%timestampIndexEntrySz != 0 {
		return nil, errTimestampIndexCorrupt
	}
	entries := make([]timestampLSNEntry, 0, len(body)/timestampIndexEntrySz)
	for offset := 0; offset < len(body); offset += timestampIndexEntrySz {
		entries = append(entries, timestampLSNEntry{
			timestamp: binary.LittleEndian.Uint64(body[offset : offset+8]),
			lsn:       binary.LittleEndian.Uint64(body[offset+8 : offset+16]),
		})
	}
	return entries, nil
}

func decodeCompressedTimestampIndex(data []byte) ([]timestampLSNRange, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < timestampIndexHeaderSz || string(data[:timestampIndexHeaderSz]) != timestampIndexMagic {
		return nil, errTimestampIndexCorrupt
	}
	body := data[timestampIndexHeaderSz:]
	if len(body)%timestampIndexEntrySz != 0 {
		return nil, errTimestampIndexCorrupt
	}
	if len(body) == 0 {
		return nil, nil
	}

	ranges := make([]timestampLSNRange, 0, len(body)/timestampIndexEntrySz)
	for offset := 0; offset < len(body); offset += timestampIndexEntrySz {
		entry := timestampLSNEntry{
			timestamp: binary.LittleEndian.Uint64(body[offset : offset+8]),
			lsn:       binary.LittleEndian.Uint64(body[offset+8 : offset+16]),
		}
		ranges = appendCompressedTimestampEntry(ranges, entry)
	}
	return ranges, nil
}

func writeTimestampIndexFile(path string, entries []timestampLSNEntry) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".asql-tsindex-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp timestamp index: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(encodeTimestampIndex(entries)); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write timestamp index: %w", err)
	}
	if err := wal.PlatformSync(tmp); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("sync timestamp index: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close timestamp index: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename timestamp index: %w", err)
	}
	return nil
}

func appendTimestampIndexFile(path string, entries []timestampLSNEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir timestamp index dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open timestamp index: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat timestamp index: %w", err)
	}
	if stat.Size() == 0 {
		if _, err := file.Write([]byte(timestampIndexMagic)); err != nil {
			return fmt.Errorf("write timestamp index header: %w", err)
		}
	}
	if _, err := file.Write(encodeTimestampEntries(entries)); err != nil {
		return fmt.Errorf("append timestamp index entries: %w", err)
	}
	return nil
}

func encodeTimestampIndex(entries []timestampLSNEntry) []byte {
	data := make([]byte, 0, timestampIndexHeaderSz+len(entries)*timestampIndexEntrySz)
	data = append(data, []byte(timestampIndexMagic)...)
	data = append(data, encodeTimestampEntries(entries)...)
	return data
}

func encodeTimestampEntries(entries []timestampLSNEntry) []byte {
	data := make([]byte, len(entries)*timestampIndexEntrySz)
	for i, entry := range entries {
		offset := i * timestampIndexEntrySz
		binary.LittleEndian.PutUint64(data[offset:offset+8], entry.timestamp)
		binary.LittleEndian.PutUint64(data[offset+8:offset+16], entry.lsn)
	}
	return data
}

func timestampEntriesFromRecords(records []ports.WALRecord) []timestampLSNEntry {
	entries := make([]timestampLSNEntry, 0, len(records))
	for _, record := range records {
		entries = append(entries, timestampLSNEntry{timestamp: record.Timestamp, lsn: record.LSN})
	}
	return entries
}

func compressTimestampEntries(entries []timestampLSNEntry) []timestampLSNRange {
	if len(entries) == 0 {
		return nil
	}
	ranges := make([]timestampLSNRange, 0, len(entries))
	for _, entry := range entries {
		ranges = appendCompressedTimestampEntry(ranges, entry)
	}
	return ranges
}

func appendCompressedTimestampEntries(ranges []timestampLSNRange, entries []timestampLSNEntry) []timestampLSNRange {
	for _, entry := range entries {
		ranges = appendCompressedTimestampEntry(ranges, entry)
	}
	return ranges
}

func appendCompressedTimestampEntry(ranges []timestampLSNRange, entry timestampLSNEntry) []timestampLSNRange {
	if len(ranges) == 0 {
		return append(ranges, timestampLSNRange{startTimestamp: entry.timestamp, startLSN: entry.lsn, count: 1})
	}
	last := &ranges[len(ranges)-1]
	if last.endTimestamp()+1 == entry.timestamp && last.endLSN()+1 == entry.lsn {
		last.count++
		return ranges
	}
	return append(ranges, timestampLSNRange{startTimestamp: entry.timestamp, startLSN: entry.lsn, count: 1})
}
