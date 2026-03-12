package executor

import (
	"fmt"
	"os"
	"path/filepath"

	"asql/internal/platform/datadir"
	"asql/internal/storage/wal"
)

// SnapshotCatalogEntry describes one persisted checkpoint file available in a data directory.
type SnapshotCatalogEntry struct {
	FileName  string `json:"file_name"`
	Sequence  uint64 `json:"sequence"`
	LSN       uint64 `json:"lsn"`
	LogicalTS uint64 `json:"logical_ts"`
	Bytes     int64  `json:"bytes"`
	IsFull    bool   `json:"is_full"`
}

// WALRetentionState summarizes the current retained WAL and snapshot window.
type WALRetentionState struct {
	DataDir             string                    `json:"data_dir"`
	RetainWAL           bool                      `json:"retain_wal"`
	HeadLSN             uint64                    `json:"head_lsn"`
	OldestRetainedLSN   uint64                    `json:"oldest_retained_lsn"`
	LastRetainedLSN     uint64                    `json:"last_retained_lsn"`
	SegmentCount        int                       `json:"segment_count"`
	DiskSnapshotCount   int                       `json:"disk_snapshot_count"`
	MemorySnapshotCount int                       `json:"memory_snapshot_count,omitempty"`
	MaxDiskSnapshots    int                       `json:"max_disk_snapshots"`
	Segments            []wal.SegmentCatalogEntry `json:"segments,omitempty"`
}

// CurrentLSN returns the current visible head LSN for the engine.
func (engine *Engine) CurrentLSN() uint64 {
	if engine == nil {
		return 0
	}
	if state := engine.readState.Load(); state != nil {
		return state.headLSN
	}
	return engine.headLSN
}

// SnapshotCatalog returns the catalog of persisted snapshots for the engine's snapshot directory.
func (engine *Engine) SnapshotCatalog() ([]SnapshotCatalogEntry, error) {
	if engine == nil || engine.snapDir == "" {
		return nil, nil
	}
	entries, err := LoadSnapshotCatalog(engine.snapDir)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// LoadSnapshotCatalog inspects a snapshot directory and returns persisted checkpoint metadata.
func LoadSnapshotCatalog(snapDir string) ([]SnapshotCatalogEntry, error) {
	catalog, err := catalogSnapshotFiles(snapDir)
	if err != nil {
		return nil, err
	}
	entries := make([]SnapshotCatalogEntry, 0, len(catalog))
	for _, item := range catalog {
		path := filepath.Join(snapDir, item.FileName)
		bytes, _ := fileSize(path)
		entries = append(entries, SnapshotCatalogEntry{
			FileName:  item.FileName,
			Sequence:  item.Sequence,
			LSN:       item.LSN,
			LogicalTS: item.LogicalTS,
			Bytes:     bytes,
			IsFull:    (item.Sequence-1)%fullSnapshotFrequency == 0,
		})
	}
	return entries, nil
}

// InspectDataDirSnapshotCatalog inspects a persisted ASQL data directory and returns its snapshot catalog.
func InspectDataDirSnapshotCatalog(dataDir string) ([]SnapshotCatalogEntry, error) {
	dd, err := datadir.New(dataDir)
	if err != nil {
		return nil, fmt.Errorf("open data dir: %w", err)
	}
	entries, err := LoadSnapshotCatalog(dd.SnapDir())
	if err != nil {
		return nil, fmt.Errorf("catalog snapshots: %w", err)
	}
	return entries, nil
}

// WALRetentionState returns the engine's current WAL retention summary.
func (engine *Engine) WALRetentionState() WALRetentionState {
	state := WALRetentionState{
		RetainWAL:        true,
		HeadLSN:          0,
		MaxDiskSnapshots: maxDiskSnapshots,
	}
	if engine == nil {
		return state
	}
	state.RetainWAL = engine.retainWAL
	state.HeadLSN = engine.headLSN
	if engine.snapshots != nil {
		engine.snapshots.mu.Lock()
		state.MemorySnapshotCount = len(engine.snapshots.snapshots)
		engine.snapshots.mu.Unlock()
	}
	if entries, err := engine.SnapshotCatalog(); err == nil {
		state.DiskSnapshotCount = len(entries)
	}
	return state
}

// InspectDataDirWALRetention inspects a persisted ASQL data directory and returns WAL retention state.
func InspectDataDirWALRetention(dataDir string) (WALRetentionState, error) {
	dd, err := datadir.New(dataDir)
	if err != nil {
		return WALRetentionState{}, fmt.Errorf("open data dir: %w", err)
	}
	segments, err := wal.CatalogSegments(dd.WALBasePath())
	if err != nil {
		return WALRetentionState{}, fmt.Errorf("catalog wal segments: %w", err)
	}
	catalog, err := LoadSnapshotCatalog(dd.SnapDir())
	if err != nil {
		return WALRetentionState{}, fmt.Errorf("catalog snapshots: %w", err)
	}
	state := WALRetentionState{
		DataDir:           dataDir,
		RetainWAL:         len(catalog) == 0,
		SegmentCount:      len(segments),
		DiskSnapshotCount: len(catalog),
		MaxDiskSnapshots:  maxDiskSnapshots,
		Segments:          segments,
	}
	if len(segments) > 0 {
		state.OldestRetainedLSN = segments[0].FirstLSN
		state.LastRetainedLSN = segments[len(segments)-1].LastLSN
		state.HeadLSN = state.LastRetainedLSN
	}
	return state, nil
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
