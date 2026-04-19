package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/correodabid/asql/internal/engine/ports"
)

const (
	// DefaultSegmentSize is the target size for each WAL segment file (16 MB).
	DefaultSegmentSize int64 = 16 * 1024 * 1024

	// recentReadCacheSize keeps the most recent WAL records in memory so Raft
	// replication can serve hot tail reads without rescanning segment files.
	// The leader may need to serve several rounds worth of outstanding entries
	// while followers are still applying committed WAL. If this tail cache is
	// too small, sustained ingest falls off a cliff once follower lag pushes the
	// requested `nextIndex` behind the cached window and every heartbeat/broadcast
	// starts rescanning segment files from disk again.
	recentReadCacheSize = 262144

	// segmentHeaderSize is the fixed size of the header at the start of each segment file.
	segmentHeaderSize int64 = 64

	// walCompactionIdleDelay delays background WALZ compaction until the store
	// has been idle for a short period so sustained ingest does not compete with
	// compression work on sealed segments.
	walCompactionIdleDelay = 30 * time.Second

	// segmentMagic identifies a segment file with a header.
	segmentMagic = "WALS"

	// segmentHeaderVer is the current header format version.
	segmentHeaderVer = uint8(3)

	// walzMagic is the 4-byte prefix written at the start of a whole-segment
	// zstd-compressed (WALZ) sealed segment file.
	walzMagic = "WALZ"
)

// walzEncoder and walzDecoder are package-level zstd handles for whole-segment compression.
// Initialized once at package init; encoder uses BetterCompression for sealed segments.
var (
	walzEncoder *zstd.Encoder
	walzDecoder *zstd.Decoder
)

func init() {
	var err error
	walzDecoder, err = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		panic(fmt.Sprintf("wal: init zstd decoder: %v", err))
	}
	walzEncoder, err = zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		panic(fmt.Sprintf("wal: init zstd encoder: %v", err))
	}
}

// segmentHeader holds the metadata stored in the first 64 bytes of each segment file.
//
//	Offset  Size  Field
//	0       4B    Magic "WALS"
//	4       1B    Header version (1)
//	5       4B    Segment seqNum
//	9       8B    firstLSN
//	17      8B    lastLSN
//	25      4B    recordCount
//	29      1B    sealed flag (0=active, 1=sealed)
//	30      8B    dataSize (bytes of WAL frames after the header)
//	38      22B   reserved
//	60      4B    CRC32 of bytes [0:60]
type segmentHeader struct {
	SeqNum      uint32
	FirstLSN    uint64
	LastLSN     uint64
	RecordCount uint32
	Sealed      bool
	DataSize    uint64
}

func encodeSegmentHeader(h segmentHeader) []byte {
	buf := make([]byte, segmentHeaderSize)
	copy(buf[0:4], segmentMagic)
	buf[4] = segmentHeaderVer
	binary.BigEndian.PutUint32(buf[5:9], h.SeqNum)
	binary.BigEndian.PutUint64(buf[9:17], h.FirstLSN)
	binary.BigEndian.PutUint64(buf[17:25], h.LastLSN)
	binary.BigEndian.PutUint32(buf[25:29], h.RecordCount)
	if h.Sealed {
		buf[29] = 1
	}
	binary.BigEndian.PutUint64(buf[30:38], h.DataSize)
	// buf[38:60] reserved (zero)
	crc := crc32.Checksum(buf[:60], crc32cTable)
	binary.BigEndian.PutUint32(buf[60:64], crc)
	return buf
}

func decodeSegmentHeader(buf []byte) (segmentHeader, error) {
	if int64(len(buf)) < segmentHeaderSize {
		return segmentHeader{}, errors.New("segment header too short")
	}
	if string(buf[0:4]) != segmentMagic {
		return segmentHeader{}, errors.New("invalid segment magic")
	}
	if buf[4] != segmentHeaderVer {
		return segmentHeader{}, fmt.Errorf("unsupported segment header version: %d", buf[4])
	}
	expected := crc32.Checksum(buf[:60], crc32cTable)
	actual := binary.BigEndian.Uint32(buf[60:64])
	if expected != actual {
		return segmentHeader{}, fmt.Errorf("segment header CRC mismatch: expected=%x got=%x", expected, actual)
	}
	return segmentHeader{
		SeqNum:      binary.BigEndian.Uint32(buf[5:9]),
		FirstLSN:    binary.BigEndian.Uint64(buf[9:17]),
		LastLSN:     binary.BigEndian.Uint64(buf[17:25]),
		RecordCount: binary.BigEndian.Uint32(buf[25:29]),
		Sealed:      buf[29] == 1,
		DataSize:    binary.BigEndian.Uint64(buf[30:38]),
	}, nil
}

// segment represents a single WAL segment file.
type segment struct {
	file        *os.File
	seqNum      uint32 // monotonic segment sequence number
	firstLSN    uint64 // first LSN in this segment (0 if empty)
	lastLSN     uint64 // last LSN in this segment (0 if empty)
	size        int64  // current file size in bytes
	recordCount uint32 // number of WAL records in this segment
	hasHeader   bool   // true if file starts with a segment header
	compressed  bool   // true when the segment is already compacted to WALZ
	dirty       bool   // true when the segment has writes not yet fsynced
}

// SegmentedLogStore stores WAL records across multiple segment files.
// Each segment holds a contiguous range of LSN values. When the active
// segment exceeds SegmentTargetSize, a new segment is created.
//
// File naming: {basePath}.{seqNum:06d}
// Example:    asql.wal.000001, asql.wal.000002, ...
//
// Implements ports.LogStore, ports.BatchAppender, ports.NoSyncBatchAppender,
// ports.Syncer, and ports.Truncator.
type SegmentedLogStore struct {
	mu                  sync.Mutex
	syncMu              sync.Mutex // serializes fsync and segment-removal ops
	basePath            string     // e.g. "/data/asql.wal"
	segments            []*segment // sorted by seqNum, last is active
	totalSize           int64      // cached combined on-disk size of all segment files
	syncStrategy        SyncStrategy
	appendCount         uint64
	lastLSN             uint64
	targetSize          int64  // target max size per segment
	nextSeqNum          uint32 // next segment sequence number to allocate
	closed              bool
	writeCh             chan struct{} // closed & replaced on every successful Append; never nil
	recentRecords       []ports.WALRecord
	recentStart         int
	lastWriteUnixNano   atomic.Int64
	compactionScheduled atomic.Bool
}

// SegmentOption configures the SegmentedLogStore.
type SegmentOption func(*SegmentedLogStore)

// WithSegmentSize sets the target size for each WAL segment.
func WithSegmentSize(size int64) SegmentOption {
	return func(s *SegmentedLogStore) {
		if size > 0 {
			s.targetSize = size
		}
	}
}

// NewSegmentedLogStore creates or opens a segmented WAL store at basePath.
// If a monolithic WAL file exists at basePath (legacy format), it is
// automatically migrated to the first segment.
func NewSegmentedLogStore(basePath string, syncStrategy SyncStrategy, opts ...SegmentOption) (*SegmentedLogStore, error) {
	if syncStrategy == nil {
		syncStrategy = AlwaysSync{}
	}

	store := &SegmentedLogStore{
		basePath:     basePath,
		syncStrategy: syncStrategy,
		targetSize:   DefaultSegmentSize,
		writeCh:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(store)
	}

	// Migrate legacy monolithic WAL if it exists.
	if err := store.migrateLegacy(); err != nil {
		return nil, fmt.Errorf("migrate legacy wal: %w", err)
	}

	// Discover existing segments.
	if err := store.discoverSegments(); err != nil {
		return nil, fmt.Errorf("discover wal segments: %w", err)
	}

	// If no segments exist, create the first one.
	if len(store.segments) == 0 {
		if err := store.createSegment(); err != nil {
			return nil, fmt.Errorf("create initial segment: %w", err)
		}
	}

	return store, nil
}

// segmentPath returns the file path for a segment with the given sequence number.
func (store *SegmentedLogStore) segmentPath(seqNum uint32) string {
	return fmt.Sprintf("%s.%06d", store.basePath, seqNum)
}

// parseSegmentSeqNum extracts the sequence number from a segment filename.
func (store *SegmentedLogStore) parseSegmentSeqNum(name string) (uint32, bool) {
	base := filepath.Base(store.basePath)
	prefix := base + "."
	fname := filepath.Base(name)
	if !strings.HasPrefix(fname, prefix) {
		return 0, false
	}
	suffix := strings.TrimPrefix(fname, prefix)
	if len(suffix) != 6 {
		return 0, false
	}
	n, err := strconv.ParseUint(suffix, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

// migrateLegacy converts a monolithic WAL file to the first segment.
func (store *SegmentedLogStore) migrateLegacy() error {
	info, err := os.Stat(store.basePath)
	if err != nil || info.IsDir() {
		return nil // no legacy file
	}

	// Check if segments already exist — if so, legacy coexistence is an error.
	dir := filepath.Dir(store.basePath)
	base := filepath.Base(store.basePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read wal directory: %w", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), base+".") {
			if _, ok := store.parseSegmentSeqNum(e.Name()); ok {
				return fmt.Errorf("legacy wal file %q coexists with segments — manual resolution required", store.basePath)
			}
		}
	}

	// Rename legacy file to segment 000001.
	segPath := store.segmentPath(1)
	slog.Info("wal: migrating legacy monolithic WAL to segmented format",
		"from", store.basePath, "to", segPath)
	if err := os.Rename(store.basePath, segPath); err != nil {
		return fmt.Errorf("rename legacy wal to segment: %w", err)
	}

	return nil
}

// discoverSegments scans for existing segment files and opens them.
func (store *SegmentedLogStore) discoverSegments() error {
	dir := filepath.Dir(store.basePath)
	base := filepath.Base(store.basePath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read wal directory: %w", err)
	}

	var seqNums []uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasPrefix(e.Name(), base+".") {
			continue
		}
		if seqNum, ok := store.parseSegmentSeqNum(e.Name()); ok {
			seqNums = append(seqNums, seqNum)
		}
	}

	sort.Slice(seqNums, func(i, j int) bool { return seqNums[i] < seqNums[j] })

	for _, seqNum := range seqNums {
		seg, err := store.openSegment(seqNum)
		if err != nil {
			store.closeAllSegments()
			return fmt.Errorf("open segment %06d: %w", seqNum, err)
		}
		store.segments = append(store.segments, seg)
		store.totalSize += seg.size
		if seg.lastLSN > store.lastLSN {
			store.lastLSN = seg.lastLSN
		}
		if seqNum >= store.nextSeqNum {
			store.nextSeqNum = seqNum + 1
		}
	}

	return nil
}

// openSegment opens an existing segment file and scans it to determine LSN bounds.
func (store *SegmentedLogStore) openSegment(seqNum uint32) (*segment, error) {
	path := store.segmentPath(seqNum)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment file: %w", err)
	}

	seg := &segment{
		file:   f,
		seqNum: seqNum,
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat segment: %w", err)
	}
	if header, ok, err := tryReadSegmentHeader(f); err != nil {
		f.Close()
		return nil, err
	} else if ok {
		seg.hasHeader = true
		seg.firstLSN = header.FirstLSN
		seg.lastLSN = header.LastLSN
		seg.recordCount = header.RecordCount
		validEnd := segmentHeaderSize + int64(header.DataSize)
		if validEnd < segmentHeaderSize || validEnd > info.Size() {
			seg.hasHeader = true
		} else {
			if validEnd < info.Size() {
				slog.Warn("wal: truncating bytes beyond persisted segment header",
					"segment", seqNum, "valid_end", validEnd, "file_size", info.Size())
				if err := f.Truncate(validEnd); err != nil {
					f.Close()
					return nil, fmt.Errorf("truncate segment to header size: %w", err)
				}
			}
			seg.size = validEnd
			if _, err := f.Seek(0, io.SeekEnd); err != nil {
				f.Close()
				return nil, fmt.Errorf("seek segment end: %w", err)
			}
			return seg, nil
		}
	}

	// Scan records to get LSN bounds and file size.
	records, validEnd, err := scanSegmentRecords(f, seg.hasHeader)
	if err != nil {
		f.Close()
		return nil, err
	}

	if len(records) > 0 {
		seg.firstLSN = records[0].LSN
		seg.lastLSN = records[len(records)-1].LSN
	}

	// Truncate any partial trailing frame.
	compressed, err := isWALZSegmentFile(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("inspect segment format: %w", err)
	}
	seg.compressed = compressed
	if validEnd < info.Size() {
		slog.Warn("wal: truncating partial frame in segment",
			"segment", seqNum, "valid_end", validEnd, "file_size", info.Size())
		if err := f.Truncate(validEnd); err != nil {
			f.Close()
			return nil, fmt.Errorf("truncate segment: %w", err)
		}
	}
	seg.size = validEnd

	// Seek to end for appending (only relevant for the active segment).
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("seek segment end: %w", err)
	}

	return seg, nil
}

func tryReadSegmentHeader(f *os.File) (segmentHeader, bool, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return segmentHeader{}, false, fmt.Errorf("seek segment start: %w", err)
	}
	buf := make([]byte, segmentHeaderSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return segmentHeader{}, false, nil
		}
		return segmentHeader{}, false, fmt.Errorf("read segment header: %w", err)
	}
	header, err := decodeSegmentHeader(buf)
	if err != nil {
		return segmentHeader{}, false, nil
	}
	return header, true, nil
}

func isWALZSegmentFile(f *os.File) (bool, error) {
	if f == nil {
		return false, nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false, fmt.Errorf("seek segment start: %w", err)
	}
	var peek [4]byte
	n, err := io.ReadFull(f, peek[:])
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return false, nil
		}
		return false, fmt.Errorf("read segment peek: %w", err)
	}
	if n < len(peek) {
		return false, nil
	}
	return string(peek[:]) == walzMagic, nil
}

// scanSegmentRecords reads all valid records from a segment file.
// Returns the records, the valid end offset, and any fatal error.
// Transparently handles WALZ (whole-segment zstd compressed) files.
func scanSegmentRecords(f *os.File, hasHeader bool) ([]diskRecord, int64, error) {
	offset := int64(0)
	if hasHeader {
		offset = segmentHeaderSize
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek segment start: %w", err)
	}

	// Peek at first 4 bytes to detect WALZ format.
	var peek [4]byte
	n, err := io.ReadFull(f, peek[:])
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, offset + int64(n), nil // empty or too-short file
		}
		return nil, 0, fmt.Errorf("read segment peek: %w", err)
	}

	if string(peek[:]) == walzMagic {
		// Whole-segment zstd compressed: read the rest, decompress, scan.
		compressedBody, err := io.ReadAll(f)
		if err != nil {
			return nil, 0, fmt.Errorf("read walz payload: %w", err)
		}
		raw, err := walzDecoder.DecodeAll(compressedBody, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("decode walz segment: %w", err)
		}
		info, err := f.Stat()
		if err != nil {
			return nil, 0, fmt.Errorf("stat walz segment: %w", err)
		}
		// Sealed WALZ segments are always complete — validEnd = full file size.
		records, _, err := scanRecordsFromBytes(stripSegmentHeaderFromBytes(raw))
		return records, info.Size(), err
	}

	// Normal uncompressed segment: seek back and parse length-prefixed records.
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek segment start: %w", err)
	}

	records, validEnd, err := scanRecordsFromFile(f)
	if err != nil {
		return nil, 0, err
	}
	return records, offset + validEnd, nil
}

// scanSegmentRecordsFromLSN reads records from a segment file, filtering by
// fromLSN and respecting limit without materializing the entire segment in the
// common uncompressed case.
func scanSegmentRecordsFromLSN(f *os.File, fromLSN uint64, limit int, hasHeader bool) ([]ports.WALRecord, int64, error) {
	offset := int64(0)
	if hasHeader {
		offset = segmentHeaderSize
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek segment start: %w", err)
	}

	var peek [4]byte
	n, err := io.ReadFull(f, peek[:])
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, offset + int64(n), nil
		}
		return nil, 0, fmt.Errorf("read segment peek: %w", err)
	}

	if string(peek[:]) == walzMagic {
		compressedBody, err := io.ReadAll(f)
		if err != nil {
			return nil, 0, fmt.Errorf("read walz payload: %w", err)
		}
		raw, err := walzDecoder.DecodeAll(compressedBody, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("decode walz segment: %w", err)
		}
		info, err := f.Stat()
		if err != nil {
			return nil, 0, fmt.Errorf("stat walz segment: %w", err)
		}
		records, _, err := scanRecordsFromBytes(stripSegmentHeaderFromBytes(raw))
		if err != nil {
			return nil, 0, err
		}
		result := make([]ports.WALRecord, 0, len(records))
		for _, r := range records {
			if r.LSN < fromLSN {
				continue
			}
			result = append(result, ports.WALRecord{
				LSN:       r.LSN,
				Term:      r.Term,
				TxID:      r.TxID,
				Type:      r.Type,
				Timestamp: r.Timestamp,
				Payload:   r.Payload,
			})
			if limit > 0 && len(result) >= limit {
				break
			}
		}
		return result, info.Size(), nil
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek segment start: %w", err)
	}

	records, validEnd, err := scanRecordsFromFileFiltered(f, fromLSN, limit)
	if err != nil {
		return nil, 0, err
	}
	return records, offset + validEnd, nil
}

func stripSegmentHeaderFromBytes(data []byte) []byte {
	if len(data) < int(segmentHeaderSize) {
		return data
	}
	header, err := decodeSegmentHeader(data[:segmentHeaderSize])
	if err != nil {
		return data
	}
	end := segmentHeaderSize + int64(header.DataSize)
	if end < segmentHeaderSize || end > int64(len(data)) {
		return data[segmentHeaderSize:]
	}
	return data[segmentHeaderSize:end]
}

// scanRecordsFromBytes parses length-prefixed WAL records from a byte slice.
// Used after decompressing a WALZ segment.
func scanRecordsFromBytes(data []byte) ([]diskRecord, int64, error) {
	var records []diskRecord
	var off int64

	for len(data) > 0 {
		if len(data) < 4 {
			slog.Warn("wal: truncated record in walz segment (incomplete length prefix)", "valid_records", len(records))
			break
		}
		length := int(binary.BigEndian.Uint32(data[:4]))
		if len(data) < 4+length {
			slog.Warn("wal: truncated record in walz segment (incomplete payload)", "valid_records", len(records))
			break
		}
		record, err := decodeDiskRecord(data[4 : 4+length])
		if err != nil {
			return nil, 0, err
		}
		records = append(records, record)
		off += int64(4 + length)
		data = data[4+length:]
	}

	return records, off, nil
}

// scanRecordsFromFile parses length-prefixed WAL records from a file (uncompressed path).
func scanRecordsFromFile(f *os.File) ([]diskRecord, int64, error) {
	var records []diskRecord
	lengthBuf := make([]byte, 4)
	var validEnd int64

	for {
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, 0, fmt.Errorf("seek current pos: %w", err)
		}

		if _, err := io.ReadFull(f, lengthBuf); err != nil {
			if errors.Is(err, io.EOF) {
				validEnd = pos
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				slog.Warn("wal: truncated record (incomplete length prefix)", "valid_records", len(records))
				validEnd = pos
				break
			}
			return nil, 0, fmt.Errorf("read length prefix: %w", err)
		}

		length := binary.BigEndian.Uint32(lengthBuf)
		body := make([]byte, length)
		if _, err := io.ReadFull(f, body); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				slog.Warn("wal: truncated record (incomplete payload)", "valid_records", len(records))
				validEnd = pos
				break
			}
			return nil, 0, fmt.Errorf("read record payload: %w", err)
		}

		record, err := decodeDiskRecord(body)
		if err != nil {
			return nil, 0, err
		}

		records = append(records, record)
		validEnd = pos + 4 + int64(length)
	}

	return records, validEnd, nil
}

func scanRecordsFromFileFiltered(f *os.File, fromLSN uint64, limit int) ([]ports.WALRecord, int64, error) {
	var records []ports.WALRecord
	lengthBuf := make([]byte, 4)
	var validEnd int64

	for {
		pos, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, 0, fmt.Errorf("seek current pos: %w", err)
		}

		if _, err := io.ReadFull(f, lengthBuf); err != nil {
			if errors.Is(err, io.EOF) {
				validEnd = pos
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				slog.Warn("wal: truncated record (incomplete length prefix)", "valid_records", len(records))
				validEnd = pos
				break
			}
			return nil, 0, fmt.Errorf("read length prefix: %w", err)
		}

		length := binary.BigEndian.Uint32(lengthBuf)
		body := make([]byte, length)
		if _, err := io.ReadFull(f, body); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				slog.Warn("wal: truncated record (incomplete payload)", "valid_records", len(records))
				validEnd = pos
				break
			}
			return nil, 0, fmt.Errorf("read record payload: %w", err)
		}

		record, err := decodeDiskRecord(body)
		if err != nil {
			return nil, 0, err
		}

		if record.LSN >= fromLSN {
			records = append(records, ports.WALRecord{
				LSN:       record.LSN,
				Term:      record.Term,
				TxID:      record.TxID,
				Type:      record.Type,
				Timestamp: record.Timestamp,
				Payload:   record.Payload,
			})
			if limit > 0 && len(records) >= limit {
				validEnd = pos + 4 + int64(length)
				break
			}
		}

		validEnd = pos + 4 + int64(length)
	}

	return records, validEnd, nil
}

// createSegment creates a new empty segment file.
func (store *SegmentedLogStore) createSegment() error {
	if store.nextSeqNum == 0 {
		store.nextSeqNum = 1
	}
	seqNum := store.nextSeqNum
	store.nextSeqNum++

	path := store.segmentPath(seqNum)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create segment file: %w", err)
	}

	seg := &segment{
		file:      f,
		seqNum:    seqNum,
		hasHeader: true,
		size:      segmentHeaderSize,
	}
	if err := writeSegmentHeader(f, segmentHeader{SeqNum: seqNum}); err != nil {
		_ = f.Close()
		return fmt.Errorf("write initial segment header: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return fmt.Errorf("seek new segment end: %w", err)
	}
	store.totalSize += segmentHeaderSize
	store.segments = append(store.segments, seg)

	slog.Debug("wal: created new segment", "segment", seqNum, "path", path)
	return nil
}

// activeSegment returns the current active (last) segment.
func (store *SegmentedLogStore) activeSegment() *segment {
	if len(store.segments) == 0 {
		return nil
	}
	return store.segments[len(store.segments)-1]
}

// rotateIfNeeded creates a new segment if the active one exceeds targetSize.
// Must be called under mu.
func (store *SegmentedLogStore) rotateIfNeeded() error {
	active := store.activeSegment()
	if active == nil || active.size < store.targetSize {
		return nil
	}

	// Sync only when the segment contains writes not yet persisted by the
	// normal append/group-commit path. In cluster mode with AlwaysSync, every
	// append batch already fsyncs before reaching rotation, so forcing another
	// full-segment sync here only adds periodic stalls.
	if active.dirty {
		if err := store.persistSegmentHeader(active, true); err != nil {
			return fmt.Errorf("persist segment header before rotation: %w", err)
		}
		if err := syncWithRetry(active.file); err != nil {
			return fmt.Errorf("sync segment before rotation: %w", err)
		}
		active.dirty = false
	}
	if err := store.persistSegmentHeader(active, true); err != nil {
		return fmt.Errorf("seal segment header before rotation: %w", err)
	}

	slog.Info("wal: rotating segment",
		"segment", active.seqNum,
		"size", active.size,
		"lsn_range", fmt.Sprintf("%d-%d", active.firstLSN, active.lastLSN))

	if err := store.createSegment(); err != nil {
		return err
	}

	// Asynchronously compress sealed segments to WALZ format to reduce disk usage.
	store.scheduleIdleCompaction()

	return nil
}

// Close closes all segment files.
func (store *SegmentedLogStore) Close() error {
	store.syncMu.Lock()
	defer store.syncMu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return nil
	}
	store.closed = true
	return store.closeAllSegments()
}

func (store *SegmentedLogStore) closeAllSegments() error {
	var firstErr error
	for _, seg := range store.segments {
		if seg.file != nil {
			if err := seg.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			seg.file = nil
		}
	}
	return firstErr
}

// writeToActive writes encoded bytes to the active segment, updating metadata.
func (store *SegmentedLogStore) writeToActive(data []byte, firstLSN, lastLSN uint64, recordsWritten uint32) error {
	active := store.activeSegment()
	if active == nil {
		return errors.New("no active segment")
	}

	if _, err := active.file.Write(data); err != nil {
		return err
	}

	active.size += int64(len(data))
	store.totalSize += int64(len(data))
	if active.firstLSN == 0 {
		active.firstLSN = firstLSN
	}
	active.lastLSN = lastLSN
	active.recordCount += recordsWritten
	active.dirty = true
	store.lastWriteUnixNano.Store(time.Now().UnixNano())
	return nil
}

func writeSegmentHeader(f *os.File, header segmentHeader) error {
	if _, err := f.WriteAt(encodeSegmentHeader(header), 0); err != nil {
		return fmt.Errorf("write segment header: %w", err)
	}
	return nil
}

func (store *SegmentedLogStore) persistSegmentHeader(seg *segment, sealed bool) error {
	if seg == nil || seg.file == nil || !seg.hasHeader {
		return nil
	}
	dataSize := int64(0)
	if seg.size > segmentHeaderSize {
		dataSize = seg.size - segmentHeaderSize
	}
	return writeSegmentHeader(seg.file, segmentHeader{
		SeqNum:      seg.seqNum,
		FirstLSN:    seg.firstLSN,
		LastLSN:     seg.lastLSN,
		RecordCount: seg.recordCount,
		Sealed:      sealed,
		DataSize:    uint64(dataSize),
	})
}

func (store *SegmentedLogStore) scheduleIdleCompaction() {
	if !store.compactionScheduled.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer store.compactionScheduled.Store(false)
		for {
			lastWrite := store.lastWriteUnixNano.Load()
			if lastWrite == 0 {
				break
			}
			since := time.Since(time.Unix(0, lastWrite))
			if since >= walCompactionIdleDelay {
				break
			}
			time.Sleep(walCompactionIdleDelay - since)
		}
		if err := store.CompactSealedSegments(context.Background()); err != nil {
			slog.Warn("wal: background compaction failed", "error", err.Error())
		}
	}()
}

func (store *SegmentedLogStore) appendRecentLocked(records []ports.WALRecord) {
	if len(records) == 0 {
		return
	}
	store.recentRecords = append(store.recentRecords, records...)
	if logicalLen := len(store.recentRecords) - store.recentStart; logicalLen > recentReadCacheSize {
		overflow := logicalLen - recentReadCacheSize
		for i := 0; i < overflow; i++ {
			store.recentRecords[store.recentStart+i] = ports.WALRecord{}
		}
		store.recentStart += overflow
	}
	store.compactRecentLocked()
}

func (store *SegmentedLogStore) compactRecentLocked() {
	if store.recentStart == 0 {
		return
	}
	logicalLen := len(store.recentRecords) - store.recentStart
	if logicalLen < 0 {
		logicalLen = 0
	}
	if store.recentStart < recentReadCacheSize/2 && len(store.recentRecords) <= recentReadCacheSize*2 {
		return
	}
	if logicalLen == 0 {
		store.recentRecords = nil
		store.recentStart = 0
		return
	}
	compacted := make([]ports.WALRecord, logicalLen)
	copy(compacted, store.recentRecords[store.recentStart:])
	store.recentRecords = compacted
	store.recentStart = 0
}

func (store *SegmentedLogStore) readRecentLocked(fromLSN uint64, limit int) ([]ports.WALRecord, bool) {
	window := store.recentRecords[store.recentStart:]
	if len(window) == 0 {
		return nil, false
	}
	if fromLSN < window[0].LSN {
		return nil, false
	}
	start := sort.Search(len(window), func(i int) bool {
		return window[i].LSN >= fromLSN
	})
	if start >= len(window) {
		return nil, true
	}
	end := len(window)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	result := make([]ports.WALRecord, end-start)
	copy(result, window[start:end])
	return result, true
}

// Append appends a single WAL record assigning the next deterministic LSN.
func (store *SegmentedLogStore) Append(ctx context.Context, record ports.WALRecord) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return 0, errors.New("wal store is closed")
	}

	if err := store.rotateIfNeeded(); err != nil {
		return 0, fmt.Errorf("rotate segment: %w", err)
	}

	nextLSN := store.lastLSN + 1
	encoded := encodeDiskRecord(diskRecord{
		LSN:       nextLSN,
		Term:      record.Term,
		TxID:      record.TxID,
		Type:      record.Type,
		Timestamp: record.Timestamp,
		Payload:   record.Payload,
	})

	if err := store.writeToActive(encoded, nextLSN, nextLSN, 1); err != nil {
		return 0, fmt.Errorf("write wal record: %w", err)
	}

	store.appendCount++
	if store.syncStrategy.ShouldSync(store.appendCount) {
		active := store.activeSegment()
		if err := store.persistSegmentHeader(active, false); err != nil {
			return 0, fmt.Errorf("persist wal segment header: %w", err)
		}
		if err := syncWithRetry(active.file); err != nil {
			return 0, fmt.Errorf("sync wal segment: %w", err)
		}
		active.dirty = false
	}

	store.lastLSN = nextLSN
	store.appendRecentLocked([]ports.WALRecord{{
		LSN:       nextLSN,
		Term:      record.Term,
		TxID:      record.TxID,
		Type:      record.Type,
		Timestamp: record.Timestamp,
		Payload:   record.Payload,
	}})
	store.notifyWriters()
	return nextLSN, nil
}
func (store *SegmentedLogStore) AppendBatch(ctx context.Context, records []ports.WALRecord) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return nil, errors.New("wal store is closed")
	}

	if err := store.rotateIfNeeded(); err != nil {
		return nil, fmt.Errorf("rotate segment: %w", err)
	}

	lsns := make([]uint64, len(records))
	var totalSize int
	encodedRecords := make([][]byte, len(records))
	for i, record := range records {
		nextLSN := store.lastLSN + uint64(i) + 1
		lsns[i] = nextLSN
		encoded := encodeDiskRecord(diskRecord{
			LSN:       nextLSN,
			Term:      record.Term,
			TxID:      record.TxID,
			Type:      record.Type,
			Timestamp: record.Timestamp,
			Payload:   record.Payload,
		})
		encodedRecords[i] = encoded
		totalSize += len(encoded)
	}

	buf := make([]byte, 0, totalSize)
	for _, encoded := range encodedRecords {
		buf = append(buf, encoded...)
	}

	firstLSN := lsns[0]
	lastLSN := lsns[len(lsns)-1]
	if err := store.writeToActive(buf, firstLSN, lastLSN, uint32(len(records))); err != nil {
		return nil, fmt.Errorf("write wal batch: %w", err)
	}

	store.appendCount += uint64(len(records))
	if store.syncStrategy.ShouldSync(store.appendCount) {
		active := store.activeSegment()
		if err := store.persistSegmentHeader(active, false); err != nil {
			return nil, fmt.Errorf("persist wal segment header: %w", err)
		}
		if err := syncWithRetry(active.file); err != nil {
			return nil, fmt.Errorf("sync wal segment: %w", err)
		}
		active.dirty = false
	}

	store.lastLSN = lastLSN
	appended := make([]ports.WALRecord, len(records))
	for i, record := range records {
		appended[i] = ports.WALRecord{
			LSN:       lsns[i],
			Term:      record.Term,
			TxID:      record.TxID,
			Type:      record.Type,
			Timestamp: record.Timestamp,
			Payload:   record.Payload,
		}
	}
	store.appendRecentLocked(appended)
	store.notifyWriters()
	return lsns, nil
}

// AppendBatchNoSync appends multiple records without calling fsync.
func (store *SegmentedLogStore) AppendBatchNoSync(ctx context.Context, records []ports.WALRecord) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return nil, errors.New("wal store is closed")
	}

	if err := store.rotateIfNeeded(); err != nil {
		return nil, fmt.Errorf("rotate segment: %w", err)
	}

	lsns := make([]uint64, len(records))
	var totalSize int
	encodedRecords := make([][]byte, len(records))
	for i, record := range records {
		nextLSN := store.lastLSN + uint64(i) + 1
		lsns[i] = nextLSN
		encoded := encodeDiskRecord(diskRecord{
			LSN:       nextLSN,
			Term:      record.Term,
			TxID:      record.TxID,
			Type:      record.Type,
			Timestamp: record.Timestamp,
			Payload:   record.Payload,
		})
		encodedRecords[i] = encoded
		totalSize += len(encoded)
	}

	buf := make([]byte, 0, totalSize)
	for _, encoded := range encodedRecords {
		buf = append(buf, encoded...)
	}

	firstLSN := lsns[0]
	lastLSN := lsns[len(lsns)-1]
	if err := store.writeToActive(buf, firstLSN, lastLSN, uint32(len(records))); err != nil {
		return nil, fmt.Errorf("write wal batch: %w", err)
	}

	store.appendCount += uint64(len(records))
	store.lastLSN = lastLSN
	appended := make([]ports.WALRecord, len(records))
	for i, record := range records {
		appended[i] = ports.WALRecord{
			LSN:       lsns[i],
			Term:      record.Term,
			TxID:      record.TxID,
			Type:      record.Type,
			Timestamp: record.Timestamp,
			Payload:   record.Payload,
		}
	}
	store.appendRecentLocked(appended)
	store.notifyWriters()
	return lsns, nil
}

// Sync forces an fsync on the active segment file.
func (store *SegmentedLogStore) Sync() error {
	store.syncMu.Lock()
	defer store.syncMu.Unlock()

	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return errors.New("wal store is closed")
	}
	active := store.activeSegment()
	if active == nil || active.file == nil {
		store.mu.Unlock()
		return errors.New("no active segment")
	}
	if err := store.persistSegmentHeader(active, false); err != nil {
		store.mu.Unlock()
		return err
	}
	f := active.file
	store.mu.Unlock()

	err := syncWithRetry(f)
	if err != nil {
		return err
	}

	store.mu.Lock()
	for _, seg := range store.segments {
		if seg != nil && seg.file == f {
			seg.dirty = false
			break
		}
	}
	store.mu.Unlock()
	return nil
}

// AppendReplicated appends a record preserving incoming LSN for follower replication.
func (store *SegmentedLogStore) AppendReplicated(ctx context.Context, record ports.WALRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return errors.New("wal store is closed")
	}

	if record.LSN == 0 || record.LSN != store.lastLSN+1 {
		return fmt.Errorf("%w: got=%d expected=%d", errOutOfOrderLSN, record.LSN, store.lastLSN+1)
	}

	if err := store.rotateIfNeeded(); err != nil {
		return fmt.Errorf("rotate segment: %w", err)
	}

	encoded := encodeDiskRecord(diskRecord{
		LSN:       record.LSN,
		Term:      record.Term,
		TxID:      record.TxID,
		Type:      record.Type,
		Timestamp: record.Timestamp,
		Payload:   record.Payload,
	})

	if err := store.writeToActive(encoded, record.LSN, record.LSN, 1); err != nil {
		return fmt.Errorf("write replicated wal record: %w", err)
	}

	store.appendCount++
	if store.syncStrategy.ShouldSync(store.appendCount) {
		active := store.activeSegment()
		if err := store.persistSegmentHeader(active, false); err != nil {
			return fmt.Errorf("persist replicated wal segment header: %w", err)
		}
		if err := syncWithRetry(active.file); err != nil {
			return fmt.Errorf("sync replicated wal segment: %w", err)
		}
		active.dirty = false
	}

	store.lastLSN = record.LSN
	store.appendRecentLocked([]ports.WALRecord{record})
	return nil
}

// AppendReplicatedBatch appends multiple replicated records in a single
// write+sync, preserving incoming LSNs. Records must be in consecutive
// LSN order starting from lastLSN+1.
func (store *SegmentedLogStore) AppendReplicatedBatch(ctx context.Context, records []ports.WALRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return errors.New("wal store is closed")
	}

	// Validate LSN ordering.
	expectedLSN := store.lastLSN + 1
	for i, r := range records {
		if r.LSN != expectedLSN+uint64(i) {
			return fmt.Errorf("%w: got=%d expected=%d (record %d)", errOutOfOrderLSN, r.LSN, expectedLSN+uint64(i), i)
		}
	}

	if err := store.rotateIfNeeded(); err != nil {
		return fmt.Errorf("rotate segment: %w", err)
	}

	// Encode all records into a single buffer.
	var totalSize int
	encodedRecords := make([][]byte, len(records))
	for i, record := range records {
		encoded := encodeDiskRecord(diskRecord{
			LSN:       record.LSN,
			Term:      record.Term,
			TxID:      record.TxID,
			Type:      record.Type,
			Timestamp: record.Timestamp,
			Payload:   record.Payload,
		})
		encodedRecords[i] = encoded
		totalSize += len(encoded)
	}

	buf := make([]byte, 0, totalSize)
	for _, encoded := range encodedRecords {
		buf = append(buf, encoded...)
	}

	firstLSN := records[0].LSN
	lastLSN := records[len(records)-1].LSN
	if err := store.writeToActive(buf, firstLSN, lastLSN, uint32(len(records))); err != nil {
		return fmt.Errorf("write replicated wal batch: %w", err)
	}

	store.appendCount += uint64(len(records))
	if store.syncStrategy.ShouldSync(store.appendCount) {
		active := store.activeSegment()
		if err := store.persistSegmentHeader(active, false); err != nil {
			return fmt.Errorf("persist replicated wal segment header: %w", err)
		}
		if err := syncWithRetry(active.file); err != nil {
			return fmt.Errorf("sync replicated wal segment: %w", err)
		}
		active.dirty = false
	}

	store.lastLSN = lastLSN
	appended := make([]ports.WALRecord, len(records))
	copy(appended, records)
	store.appendRecentLocked(appended)
	return nil
}

// LastLSN returns the latest appended LSN.
func (store *SegmentedLogStore) LastLSN() uint64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.lastLSN
}

// Subscribe returns a channel that is closed the next time a WAL record is
// successfully appended. Callers must call Subscribe() *before* ReadFrom so
// that any concurrent write is never missed:
//
//	ch := store.Subscribe()
//	records, _ := store.ReadFrom(ctx, fromLSN, limit)
//	if len(records) == 0 { <-ch } // wake when next record arrives
func (store *SegmentedLogStore) Subscribe() <-chan struct{} {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.writeCh
}

// notifyWriters closes the current writeCh and allocates a fresh one.
// Must be called while holding store.mu.
func (store *SegmentedLogStore) notifyWriters() {
	old := store.writeCh
	store.writeCh = make(chan struct{})
	close(old)
}

// ReadFrom reads WAL records in ascending LSN order starting from fromLSN.
func (store *SegmentedLogStore) ReadFrom(ctx context.Context, fromLSN uint64, limit int) ([]ports.WALRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if result, ok := store.readRecentLocked(fromLSN, limit); ok {
		return result, nil
	}
	if fromLSN > store.lastLSN {
		return nil, nil
	}

	var result []ports.WALRecord
	for i := store.findSegmentIndexForLSN(fromLSN); i < len(store.segments); i++ {
		seg := store.segments[i]

		records, _, err := scanSegmentRecordsFromLSN(seg.file, fromLSN, limit-len(result), seg.hasHeader)
		if err != nil {
			return nil, fmt.Errorf("read segment %06d: %w", seg.seqNum, err)
		}

		// Restore file position to end for the active segment.
		if _, err := seg.file.Seek(0, io.SeekEnd); err != nil {
			return nil, fmt.Errorf("seek segment end: %w", err)
		}

		for _, r := range records {
			result = append(result, r)
			if limit > 0 && len(result) >= limit {
				return result, nil
			}
		}
	}

	return result, nil
}

// findSegmentIndexForLSN returns the earliest segment that may contain fromLSN.
// Segments are ordered by sequence number and hold monotonically increasing
// LSN ranges, so earlier segments with lastLSN < fromLSN can be skipped.
// Must be called while holding store.mu.
func (store *SegmentedLogStore) findSegmentIndexForLSN(fromLSN uint64) int {
	if len(store.segments) == 0 {
		return 0
	}
	idx := sort.Search(len(store.segments), func(i int) bool {
		lastLSN := store.segments[i].lastLSN
		return lastLSN == 0 || lastLSN >= fromLSN
	})
	if idx >= len(store.segments) {
		return len(store.segments)
	}
	return idx
}

// Recover returns all valid WAL records for startup state rebuild.
func (store *SegmentedLogStore) Recover(ctx context.Context) ([]ports.WALRecord, error) {
	return store.ReadFrom(ctx, 1, 0)
}

// TruncateBefore removes all WAL records with LSN < beforeLSN.
// Segments whose entire LSN range is below beforeLSN are deleted.
// The segment containing beforeLSN is rewritten to remove older records.
func (store *SegmentedLogStore) TruncateBefore(ctx context.Context, beforeLSN uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if beforeLSN <= 1 {
		return nil
	}

	store.syncMu.Lock()
	defer store.syncMu.Unlock()
	store.mu.Lock()
	defer store.mu.Unlock()

	if store.closed {
		return errors.New("wal store is closed")
	}

	var kept []*segment
	var removedCount int

	for _, seg := range store.segments {
		// Keep the active segment (last one) always.
		isActive := seg == store.segments[len(store.segments)-1]

		if !isActive && seg.lastLSN > 0 && seg.lastLSN < beforeLSN {
			// Entire segment is below the truncation point — delete it.
			path := seg.file.Name()
			store.totalSize -= seg.size
			seg.file.Close()
			if err := os.Remove(path); err != nil {
				slog.Warn("wal: failed to remove old segment", "path", path, "error", err.Error())
			} else {
				slog.Info("wal: removed old segment", "segment", seg.seqNum,
					"lsn_range", fmt.Sprintf("%d-%d", seg.firstLSN, seg.lastLSN))
			}
			removedCount++
			continue
		}

		kept = append(kept, seg)
	}

	store.segments = kept

	if removedCount > 0 {
		slog.Info("wal: truncation complete", "removed_segments", removedCount, "remaining_segments", len(kept))
	}

	if len(store.recentRecords) > store.recentStart {
		window := store.recentRecords[store.recentStart:]
		filtered := make([]ports.WALRecord, 0, len(window))
		for _, record := range window {
			if record.LSN >= beforeLSN {
				filtered = append(filtered, record)
			}
		}
		store.recentRecords = filtered
		store.recentStart = 0
	} else {
		store.recentRecords = nil
		store.recentStart = 0
	}

	return nil
}

// CompactSealedSegments rewrites all sealed (non-active) segments to whole-segment
// zstd format (WALZ), reducing on-disk footprint. It is idempotent: already-compressed
// segments are skipped. Compaction errors are logged and non-fatal.
func (store *SegmentedLogStore) CompactSealedSegments(ctx context.Context) error {
	store.mu.Lock()
	if store.closed || len(store.segments) <= 1 {
		store.mu.Unlock()
		return nil
	}
	// Copy pointers to all but the active (last) segment.
	sealed := make([]*segment, len(store.segments)-1)
	copy(sealed, store.segments[:len(store.segments)-1])
	store.mu.Unlock()

	for _, seg := range sealed {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if seg.compressed {
			continue
		}
		if err := store.compactSegment(seg); err != nil {
			slog.Warn("wal: segment compaction failed", "segment", seg.seqNum, "error", err.Error())
		}
	}
	return nil
}

// compactSegment rewrites a single sealed segment to WALZ format.
// It opens a separate file descriptor so concurrent readers of seg.file are unaffected.
// The segment's file handle is atomically swapped to the new compressed file.
// Serialized via syncMu to avoid re-entrant compaction or concurrent truncation.
func (store *SegmentedLogStore) compactSegment(seg *segment) error {
	store.syncMu.Lock()
	defer store.syncMu.Unlock()

	// Get the file path under the main lock (seg.file must not be nil).
	store.mu.Lock()
	if store.closed || seg.file == nil {
		store.mu.Unlock()
		return nil
	}
	path := seg.file.Name()
	store.mu.Unlock()

	// Open a fresh read-only handle so we don't disturb the segment's file position.
	rf, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open segment for compaction: %w", err)
	}
	defer rf.Close()

	// Skip segments already in WALZ format.
	var peek [4]byte
	if _, err := io.ReadFull(rf, peek[:]); err != nil {
		return nil // empty or too short — nothing to compact
	}
	if string(peek[:]) == walzMagic {
		return nil // already compressed
	}
	if _, err := rf.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek segment: %w", err)
	}

	// Read all raw bytes of the uncompressed segment.
	raw, err := io.ReadAll(rf)
	if err != nil {
		return fmt.Errorf("read segment for compaction: %w", err)
	}
	rf.Close()

	// Compress with whole-segment zstd.
	compressed := walzEncoder.EncodeAll(raw, make([]byte, 0, len(raw)/2))

	// Write to a temporary file then atomically rename over the original.
	tmpPath := path + ".walztmp"
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create walz temp file: %w", err)
	}
	success := false
	defer func() {
		tmp.Close()
		if !success {
			os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write([]byte(walzMagic)); err != nil {
		return fmt.Errorf("write walz magic: %w", err)
	}
	if _, err := tmp.Write(compressed); err != nil {
		return fmt.Errorf("write walz payload: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync walz temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close walz temp: %w", err)
	}
	success = true

	// Atomic replace (POSIX rename is atomic on the same filesystem).
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename walz segment: %w", err)
	}

	// Reopen the compressed file and swap the handle in the segment struct.
	newF, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("reopen compressed segment: %w", err)
	}
	newInfo, err := newF.Stat()
	if err != nil {
		newF.Close()
		return fmt.Errorf("stat compressed segment: %w", err)
	}

	store.mu.Lock()
	oldF := seg.file
	oldSize := seg.size
	seg.file = newF
	seg.size = newInfo.Size()
	seg.compressed = true
	store.totalSize += newInfo.Size() - oldSize
	store.mu.Unlock()

	if oldF != nil {
		oldF.Close()
	}

	ratio := 0.0
	if len(raw) > 0 {
		ratio = float64(4+len(compressed)) / float64(len(raw))
	}
	slog.Info("wal: sealed segment compacted to WALZ",
		"segment", seg.seqNum,
		"original_bytes", len(raw),
		"compressed_bytes", 4+len(compressed),
		"ratio", fmt.Sprintf("%.2f", ratio))

	return nil
}

// SegmentCount returns the number of active segment files.
func (store *SegmentedLogStore) SegmentCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.segments)
}

// SegmentPaths returns the file paths of all segment files (for diagnostics).
func (store *SegmentedLogStore) SegmentPaths() []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	paths := make([]string, len(store.segments))
	for i, seg := range store.segments {
		paths[i] = seg.file.Name()
	}
	return paths
}

// TotalSize returns the combined on-disk size of all WAL segment files.
func (store *SegmentedLogStore) TotalSize() (int64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.totalSize, nil
}
