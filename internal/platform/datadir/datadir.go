// Package datadir manages the .asql/ data directory layout.
//
// All persistent state (WAL segments, snapshots, node metadata) lives
// under a single root directory, conventionally named ".asql".
//
// Directory structure:
//
//	.asql/
//	  wal/
//	    wal.000001        ← WAL segment files
//	    wal.000002
//	  snap/
//	    snap.000001       ← numbered snapshot files (zstd-compressed)
//	    snap.000002
//	    snap.000003
//	  audit/
//	    audit.000001      ← persistent audit log segments
//	    audit.000002
//	  meta/
//	    node.json         ← node identity & persistent config (future)
package datadir

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	// DefaultName is the conventional data directory name.
	DefaultName = ".asql"

	dirWAL   = "wal"
	dirSnap  = "snap"
	dirMeta  = "meta"
	dirAudit = "audit"

	walBaseName   = "wal"   // segment files: wal.000001, wal.000002, …
	auditBaseName = "audit" // audit log segments: audit.000001, …
)

// DataDir represents the root of the ASQL data directory.
type DataDir struct {
	root string
}

// New creates or opens a DataDir at the given root path.
// All required subdirectories are created if they do not exist.
func New(root string) (*DataDir, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve data dir path: %w", err)
	}

	dd := &DataDir{root: abs}

	for _, sub := range []string{dirWAL, dirSnap, dirMeta, dirAudit} {
		if err := os.MkdirAll(filepath.Join(abs, sub), 0o755); err != nil {
			return nil, fmt.Errorf("create %s directory: %w", sub, err)
		}
	}

	return dd, nil
}

// Root returns the absolute path to the data directory.
func (d *DataDir) Root() string { return d.root }

// WALDir returns the path to the wal/ subdirectory.
func (d *DataDir) WALDir() string { return filepath.Join(d.root, dirWAL) }

// WALBasePath returns the base path for WAL segment files.
// The SegmentedLogStore appends ".NNNNNN" suffixes to this path.
func (d *DataDir) WALBasePath() string { return filepath.Join(d.root, dirWAL, walBaseName) }

// SnapDir returns the path to the snap/ subdirectory.
// Snapshot files are stored as numbered files (snap.NNNNNN) inside this directory.
func (d *DataDir) SnapDir() string { return filepath.Join(d.root, dirSnap) }

// AuditDir returns the path to the audit/ subdirectory.
func (d *DataDir) AuditDir() string { return filepath.Join(d.root, dirAudit) }

// AuditBasePath returns the base path for audit log segment files.
// The SegmentedLogStore appends ".NNNNNN" suffixes to this path.
func (d *DataDir) AuditBasePath() string { return filepath.Join(d.root, dirAudit, auditBaseName) }

// MetaDir returns the path to the meta/ subdirectory.
func (d *DataDir) MetaDir() string { return filepath.Join(d.root, dirMeta) }

// String returns the root path (useful for logging).
func (d *DataDir) String() string { return d.root }
