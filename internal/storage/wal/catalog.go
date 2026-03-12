package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

// CatalogSegments scans all WAL segment files for a segmented store base path.
func CatalogSegments(basePath string) ([]SegmentCatalogEntry, error) {
	dir := filepath.Dir(basePath)
	base := filepath.Base(basePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read wal directory: %w", err)
	}

	type catalogFile struct {
		seqNum   uint32
		fileName string
	}
	files := make([]catalogFile, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), base+".") {
			continue
		}
		suffix := strings.TrimPrefix(entry.Name(), base+".")
		if len(suffix) != 6 {
			continue
		}
		var seqNum uint32
		if _, err := fmt.Sscanf(suffix, "%06d", &seqNum); err != nil {
			continue
		}
		files = append(files, catalogFile{seqNum: seqNum, fileName: entry.Name()})
	}

	sort.Slice(files, func(i, j int) bool { return files[i].seqNum < files[j].seqNum })
	result := make([]SegmentCatalogEntry, 0, len(files))
	for _, file := range files {
		path := filepath.Join(dir, file.fileName)
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open wal segment %s: %w", file.fileName, err)
		}
		records, _, err := scanSegmentRecords(f)
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, fmt.Errorf("scan wal segment %s: %w", file.fileName, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("stat wal segment %s: %w", file.fileName, err)
		}
		entry := SegmentCatalogEntry{
			FileName: file.fileName,
			SeqNum:   file.seqNum,
			Bytes:    info.Size(),
		}
		if len(records) > 0 {
			entry.FirstLSN = records[0].LSN
			entry.LastLSN = records[len(records)-1].LSN
			entry.RecordCount = uint32(len(records))
		}
		result = append(result, entry)
	}

	return result, nil
}
