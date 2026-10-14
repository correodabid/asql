package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/storage/wal"
)

// SnapshotLedgerAnchors returns the WAL ledger chain value recorded in each
// persisted snapshot under snapDir (ADR 0005), ordered by LSN. Snapshots
// written before the ledger existed carry no anchor and are skipped.
func SnapshotLedgerAnchors(snapDir string) ([]wal.LedgerAnchor, error) {
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read snapshot dir: %w", err)
	}
	var anchors []wal.LedgerAnchor
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, snapFilePrefix) {
			continue
		}
		if _, err := strconv.ParseUint(strings.TrimPrefix(name, snapFilePrefix), 10, 64); err != nil {
			continue
		}
		raw, err := loadRawSnapshotEntries(filepath.Join(snapDir, name))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		for _, r := range raw {
			if r.hasLedgerChain {
				anchors = append(anchors, wal.LedgerAnchor{LSN: r.lsn, Chain: r.ledgerChain, Source: "snapshot " + name})
			}
		}
	}
	sort.Slice(anchors, func(i, j int) bool { return anchors[i].LSN < anchors[j].LSN })
	return anchors, nil
}
