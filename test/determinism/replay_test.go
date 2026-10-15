package determinism_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// runWorkload runs every phase, calling between after each phase but the
// last, and returns the node with the fingerprint of its live state.
func runWorkload(t *testing.T, dir string, opts nodeOptions, between func(*node)) (*node, fingerprint) {
	t.Helper()
	n := openNode(t, dir, opts)
	phases := workloadPhases()
	for i, phase := range phases {
		n.run(phase...)
		if between != nil && i < len(phases)-1 {
			between(n)
		}
	}
	return n, n.fingerprint(workloadProbes)
}

// TestReplayReproducesLiveState: the state rebuilt on restart, from
// snapshots plus the WAL delta or from the WAL alone, is the state the
// engine served before it stopped (rules 3 and 6).
func TestReplayReproducesLiveState(t *testing.T) {
	dir := t.TempDir()
	n, live := runWorkload(t, dir, nodeOptions{}, nil)
	n.close()

	n.open()
	requireSame(t, "restart with snapshots", live, n.fingerprint(workloadProbes))
	n.close()

	if err := os.RemoveAll(n.snapDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(n.snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(filepath.Join(dir, "timestamp-lsn.idx"))
	n.open()
	defer n.close()
	requireSame(t, "replay from the WAL alone", live, n.fingerprint(workloadProbes))
}

// replayFromWALAlone copies a closed node's durable stores, the WAL and the
// audit log, into a fresh data directory and opens it there. Snapshots and
// the timestamp index are caches and are not copied (rule 6).
func replayFromWALAlone(t *testing.T, src *node, opts nodeOptions) *node {
	t.Helper()
	dir := t.TempDir()
	copyDir(t, filepath.Dir(src.walPath), filepath.Join(dir, "wal"))
	copyDir(t, filepath.Dir(src.auditPath), filepath.Join(dir, "audit"))
	return openNode(t, dir, opts)
}

// TestReplayAcrossRestartsAndCrashes runs the workload with a clean restart
// and a crash between phases. The engine that went through them must serve
// the same state as one that replays the final WAL from scratch.
func TestReplayAcrossRestartsAndCrashes(t *testing.T) {
	step := 0
	n, live := runWorkload(t, t.TempDir(), nodeOptions{}, func(n *node) {
		step++
		if step%2 == 1 {
			n.restart()
			return
		}
		n.crash()
		n.open()
	})
	n.close()

	fresh := replayFromWALAlone(t, n, nodeOptions{})
	defer fresh.close()
	requireSame(t, "fresh replay after restarts and crashes",
		live.without(historyIsUnreliable), fresh.fingerprint(workloadProbes).without(historyIsUnreliable))
}

// TestRepeatedReplaysAreIdentical: replaying the same WAL several times,
// including natural row order, gives the same state every time (rule 3).
func TestRepeatedReplaysAreIdentical(t *testing.T) {
	n, _ := runWorkload(t, t.TempDir(), nodeOptions{}, nil)
	n.close()
	var first fingerprint
	for i := 0; i < 3; i++ {
		replica := replayFromWALAlone(t, n, nodeOptions{})
		f := replica.fingerprint(workloadProbes)
		replica.close()
		if i == 0 {
			first = f
			continue
		}
		requireSame(t, "repeated replay", first, f)
	}
}

// TestTimelineListsEveryCommit checks TimelineCommits against the WAL itself
// after restarts, which reuse transaction ids: one entry per COMMIT record,
// with the mutation count of that transaction.
func TestTimelineListsEveryCommit(t *testing.T) {
	n, _ := runWorkload(t, t.TempDir(), nodeOptions{}, func(n *node) { n.restart() })
	defer n.close()
	type commit struct {
		lsn       uint64
		mutations int
	}
	var want []commit
	mutations := 0
	for _, r := range n.records() {
		switch r.Type {
		case "BEGIN":
			mutations = 0
		case "MUTATION":
			mutations++
		case "COMMIT":
			want = append(want, commit{r.LSN, mutations})
		}
	}
	timeline, err := n.engine.TimelineCommits(context.Background(), 0, n.engine.HeadLSN(), "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(timeline) != len(want) {
		t.Fatalf("timeline lists %d commits, the WAL holds %d", len(timeline), len(want))
	}
	for i, c := range timeline {
		if c.LSN != want[i].lsn || len(c.Tables) != want[i].mutations {
			t.Fatalf("timeline commit %d: LSN %d with %d mutations, want LSN %d with %d", i, c.LSN, len(c.Tables), want[i].lsn, want[i].mutations)
		}
	}
}
