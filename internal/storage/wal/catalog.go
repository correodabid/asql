package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/correodabid/asql/internal/engine/ports"
)

// SegmentCatalogEntry describes one WAL segment file for backup/restore flows.
type SegmentCatalogEntry struct {
	FileName    string
	SeqNum      uint32
	FirstLSN    uint64
	LastLSN     uint64
	RecordCount uint32
	Bytes       int64
	Sealed      bool
}

// readSegmentFileRecords returns the complete records of one segment file.
func readSegmentFileRecords(path string) (segmentHeader, bool, []diskRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return segmentHeader{}, false, nil, err
	}
	defer f.Close()
	header, hasHeader, err := tryReadSegmentHeader(f)
	if err != nil {
		return segmentHeader{}, false, nil, err
	}
	headerSize := int64(0)
	if hasHeader {
		headerSize = header.size()
	}
	records, _, _, err := scanSegmentRecords(f, headerSize)
	return header, hasHeader, records, err
}

// ReadRecordsReadOnly returns every complete record of the segmented store
// at basePath, and the store's size in bytes, without opening it for
// writing. NewSegmentedLogStore repairs a segment's tail on open, which
// would cut the file under a process that is still appending to it; tools
// that read the files of a running node use this instead.
func ReadRecordsReadOnly(basePath string) ([]ports.WALRecord, int64, error) {
	files, err := listSegmentFiles(basePath)
	if err != nil {
		return nil, 0, err
	}
	dir := filepath.Dir(basePath)
	var out []ports.WALRecord
	var size int64
	for _, name := range files {
		path := filepath.Join(dir, name)
		_, _, records, err := readSegmentFileRecords(path)
		if err != nil {
			return nil, 0, fmt.Errorf("read wal segment %s: %w", name, err)
		}
		if info, err := os.Stat(path); err == nil {
			size += info.Size()
		}
		for _, r := range records {
			out = append(out, ports.WALRecord{LSN: r.LSN, Term: r.Term, TxID: r.TxID, Type: r.Type, Timestamp: r.Timestamp, Payload: r.Payload})
		}
	}
	return out, size, nil
}

// CatalogSegments scans all WAL segment files for a segmented store base path.
func CatalogSegments(basePath string) ([]SegmentCatalogEntry, error) {
	dir := filepath.Dir(basePath)
	files, err := listSegmentFiles(basePath)
	if err != nil {
		return nil, err
	}
	result := make([]SegmentCatalogEntry, 0, len(files))
	base := filepath.Base(basePath)
	for _, name := range files {
		var seqNum uint32
		if _, err := fmt.Sscanf(strings.TrimPrefix(name, base+"."), "%06d", &seqNum); err != nil {
			continue
		}
		path := filepath.Join(dir, name)
		header, hasHeader, records, err := readSegmentFileRecords(path)
		if err != nil {
			return nil, fmt.Errorf("scan wal segment %s: %w", name, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat wal segment %s: %w", name, err)
		}
		entry := SegmentCatalogEntry{
			FileName: name,
			SeqNum:   seqNum,
			Bytes:    info.Size(),
		}
		if hasHeader {
			entry.Sealed = header.Sealed
		}
		if len(records) > 0 {
			entry.FirstLSN = records[0].LSN
			entry.LastLSN = records[len(records)-1].LSN
			entry.RecordCount = uint32(len(records))
		} else if hasHeader {
			entry.FirstLSN = header.FirstLSN
			entry.LastLSN = header.LastLSN
			entry.RecordCount = header.RecordCount
		}
		result = append(result, entry)
	}

	return result, nil
}
