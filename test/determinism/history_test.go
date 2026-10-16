package determinism_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/storage/audit"
	"github.com/correodabid/asql/internal/storage/wal"
)

// Row history (FOR HISTORY) is an index of the WAL in the audit store. The
// tests in replay_test.go and paths_test.go compare it on every path; these
// cover the audit store falling out of step with the WAL.

// truncateAudit cuts a closed node's audit store back to the commits up to
// lsn, as a crash before its writes reached disk would.
func truncateAudit(t *testing.T, n *node, lsn uint64) {
	t.Helper()
	store, err := audit.New(n.auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.TruncateAfter(context.Background(), lsn); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestLostHistoryIsRegenerated: history the audit store lost is rebuilt from
// the WAL on restart, whether it lags behind the last checkpoint or is gone
// altogether.
func TestLostHistoryIsRegenerated(t *testing.T) {
	n, live := runWorkload(t, t.TempDir(), nodeOptions{}, nil)
	head := n.engine.HeadLSN()
	n.close()

	for _, keep := range []uint64{head / 2, 0} {
		truncateAudit(t, n, keep)
		n.open()
		requireSame(t, "restart after losing history", live, n.fingerprint(workloadProbes))
		n.close()
	}

	if err := os.RemoveAll(filepath.Dir(n.auditPath)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(n.auditPath), 0o755); err != nil {
		t.Fatal(err)
	}
	n.open()
	defer n.close()
	requireSame(t, "restart without an audit store", live, n.fingerprint(workloadProbes))
}

// TestHistoryBeyondTheWALIsDropped: the WAL is synced after the audit store
// is written, so a crash can leave history of a commit the WAL lost. On
// restart it must disappear with the commit.
func TestHistoryBeyondTheWALIsDropped(t *testing.T) {
	ctx := context.Background()
	phases := workloadPhases()
	n := openNode(t, t.TempDir(), nodeOptions{})
	for _, phase := range phases[:3] {
		n.run(phase...)
	}
	want := n.fingerprint(workloadProbes)
	cut := n.engine.HeadLSN()
	n.run(phases[3]...)
	n.close()

	store, err := wal.NewSegmentedLogStore(n.walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.TruncateAfter(ctx, cut); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Checkpoints past the cut describe commits that are gone.
	if err := os.RemoveAll(n.snapDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(n.snapDir, 0o755); err != nil {
		t.Fatal(err)
	}

	n.open()
	defer n.close()
	requireSame(t, "restart after the WAL lost its tail", want, n.fingerprint(workloadProbes))
}
