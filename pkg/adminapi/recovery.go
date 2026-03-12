package adminapi

// BackupFileMetadata describes one file materialized in a recovery backup.
type BackupFileMetadata struct {
	RelativePath string `json:"relative_path"`
	Bytes        int64  `json:"bytes"`
	SHA256       string `json:"sha256"`
}

// SnapshotBackupMetadata describes one persisted snapshot file in a base backup.
type SnapshotBackupMetadata struct {
	BackupFileMetadata
	Sequence  uint64 `json:"sequence"`
	LSN       uint64 `json:"lsn"`
	LogicalTS uint64 `json:"logical_ts"`
}

// WALSegmentBackupMetadata describes one WAL segment in a base backup.
type WALSegmentBackupMetadata struct {
	BackupFileMetadata
	SeqNum      uint32 `json:"seq_num"`
	FirstLSN    uint64 `json:"first_lsn"`
	LastLSN     uint64 `json:"last_lsn"`
	RecordCount uint32 `json:"record_count"`
}

// BaseBackupManifest is the public admin representation of a recovery backup manifest.
type BaseBackupManifest struct {
	Version        int                        `json:"version"`
	HeadLSN        uint64                     `json:"head_lsn"`
	HeadTimestamp  uint64                     `json:"head_timestamp"`
	Snapshots      []SnapshotBackupMetadata   `json:"snapshots"`
	WALSegments    []WALSegmentBackupMetadata `json:"wal_segments"`
	TimestampIndex *BackupFileMetadata        `json:"timestamp_index,omitempty"`
}

// RestoreResult reports the exact recovery boundary applied to a restored data dir.
type RestoreResult struct {
	AppliedLSN       uint64
	AppliedTimestamp uint64
}

// RecoveryCreateBackupRequest creates a new recovery backup from a data directory.
type RecoveryCreateBackupRequest struct {
	DataDir   string `json:"data_dir,omitempty"`
	BackupDir string `json:"backup_dir"`
}

// RecoveryBackupManifestRequest loads an existing recovery backup manifest.
type RecoveryBackupManifestRequest struct {
	BackupDir string `json:"backup_dir"`
}

// RecoveryVerifyBackupRequest verifies a recovery backup directory.
type RecoveryVerifyBackupRequest struct {
	BackupDir string `json:"backup_dir"`
}

// RecoveryVerifyBackupResponse returns verification status and manifest details.
type RecoveryVerifyBackupResponse struct {
	Status   string             `json:"status"`
	Manifest BaseBackupManifest `json:"manifest"`
}

// RecoveryRestoreLSNRequest restores a backup to a target LSN.
type RecoveryRestoreLSNRequest struct {
	BackupDir     string `json:"backup_dir"`
	TargetDataDir string `json:"target_data_dir"`
	LSN           uint64 `json:"lsn"`
}

// RecoveryRestoreTimestampRequest restores a backup to a target logical timestamp.
type RecoveryRestoreTimestampRequest struct {
	BackupDir        string `json:"backup_dir"`
	TargetDataDir    string `json:"target_data_dir"`
	LogicalTimestamp uint64 `json:"logical_timestamp"`
}

// SnapshotCatalogEntry describes one persisted checkpoint file available in a data directory.
type SnapshotCatalogEntry struct {
	FileName  string `json:"file_name"`
	Sequence  uint64 `json:"sequence"`
	LSN       uint64 `json:"lsn"`
	LogicalTS uint64 `json:"logical_ts"`
	Bytes     int64  `json:"bytes"`
	IsFull    bool   `json:"is_full"`
}

// RecoverySnapshotCatalogRequest inspects a persisted ASQL data directory snapshot catalog.
type RecoverySnapshotCatalogRequest struct {
	DataDir string `json:"data_dir,omitempty"`
}

// RecoverySnapshotCatalogResponse returns a snapshot catalog inspection result.
type RecoverySnapshotCatalogResponse struct {
	Snapshots []SnapshotCatalogEntry `json:"snapshots"`
}

// WALSegmentCatalogEntry describes one WAL segment file for recovery diagnostics.
type WALSegmentCatalogEntry struct {
	FileName    string
	SeqNum      uint32
	FirstLSN    uint64
	LastLSN     uint64
	RecordCount uint32
	Bytes       int64
	Sealed      bool
}

// RecoveryWALRetentionRequest inspects WAL retention for a persisted ASQL data directory.
type RecoveryWALRetentionRequest struct {
	DataDir string `json:"data_dir,omitempty"`
}

// WALRetentionState summarizes the current retained WAL and snapshot window.
type WALRetentionState struct {
	DataDir             string                   `json:"data_dir"`
	RetainWAL           bool                     `json:"retain_wal"`
	HeadLSN             uint64                   `json:"head_lsn"`
	OldestRetainedLSN   uint64                   `json:"oldest_retained_lsn"`
	LastRetainedLSN     uint64                   `json:"last_retained_lsn"`
	SegmentCount        int                      `json:"segment_count"`
	DiskSnapshotCount   int                      `json:"disk_snapshot_count"`
	MemorySnapshotCount int                      `json:"memory_snapshot_count,omitempty"`
	MaxDiskSnapshots    int                      `json:"max_disk_snapshots"`
	Segments            []WALSegmentCatalogEntry `json:"segments,omitempty"`
}
