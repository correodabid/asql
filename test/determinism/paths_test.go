package determinism_test

import (
	"context"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/storage/wal"
	"github.com/correodabid/asql/pkg/fixtures"
)

// TestFollowerApplyMatchesLeader: a Raft follower appends the leader's
// records and applies them as they commit, in batches of any size. It must
// end in the leader's state, before and after it restarts.
func TestFollowerApplyMatchesLeader(t *testing.T) {
	leader, live := runWorkload(t, t.TempDir(), nodeOptions{}, nil)
	records := leader.records()
	leader.close()

	follower := openNode(t, t.TempDir(), nodeOptions{})
	ctx := context.Background()
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < len(records); {
		end := min(len(records), i+1+rng.Intn(12))
		batch := records[i:end]
		if err := follower.store.AppendReplicatedBatch(ctx, batch); err != nil {
			t.Fatal(err)
		}
		if err := follower.engine.ApplyCommittedRecords(ctx, batch[len(batch)-1].LSN, batch); err != nil {
			t.Fatal(err)
		}
		i = end
	}
	want := live
	requireSame(t, "follower after applying replicated batches", want, follower.fingerprint(workloadProbes))
	follower.restart()
	defer follower.close()
	requireSame(t, "follower after restart", want, follower.fingerprint(workloadProbes))
}

// TestTimeTravelMatchesPrefixReplay: reading AS OF LSN k on the full engine
// returns what an engine that replayed only the first k records serves as
// its current state.
func TestTimeTravelMatchesPrefixReplay(t *testing.T) {
	leader, _ := runWorkload(t, t.TempDir(), nodeOptions{}, nil)
	ctx := context.Background()
	timeline, err := leader.engine.TimelineCommits(ctx, 0, leader.engine.HeadLSN(), "", 0)
	if err != nil {
		t.Fatal(err)
	}
	schema := leader.engine.SchemaSnapshot(nil)
	leader.close()
	leader.open()
	defer leader.close()

	for i, c := range timeline {
		if i%3 != 0 && i != len(timeline)-1 {
			continue // every third commit keeps the test fast
		}
		prefix := replayFromWALAlone(t, leader, nodeOptions{engine: []executor.EngineOption{executor.WithReplayLimit(c.LSN)}})
		for _, d := range schema.Domains {
			for _, table := range d.Tables {
				sql := "SELECT * FROM " + d.Name + "." + table.Name
				want, wantErr := leader.engine.TimeTravelQueryAsOfLSN(ctx, sql, []string{d.Name}, c.LSN)
				got, gotErr := prefix.engine.Query(ctx, sql, []string{d.Name})
				if (wantErr == nil) != (gotErr == nil) || renderResult(want, wantErr) != renderResult(got, gotErr) {
					t.Fatalf("LSN %d, %s: time travel and prefix replay differ\n  as of: %s\n  prefix: %s",
						c.LSN, sql, renderResult(want, wantErr), renderResult(got, gotErr))
				}
			}
		}
		prefix.close()
	}
}

// TestCompactionAndLedgerModeDoNotChangeReplay: compacting sealed segments to
// WALZ, and running with the ledger off, change on-disk bytes but never the
// replayed state.
func TestCompactionAndLedgerModeDoNotChangeReplay(t *testing.T) {
	small := nodeOptions{segmentSize: 4096}
	n, live := runWorkload(t, t.TempDir(), small, nil)
	if err := n.store.CompactSealedSegments(context.Background()); err != nil {
		t.Fatal(err)
	}
	n.close()
	compacted := replayFromWALAlone(t, n, small)
	requireSame(t, "replay of a compacted WAL", live, compacted.fingerprint(workloadProbes))
	compacted.close()

	off, offLive := runWorkload(t, t.TempDir(), nodeOptions{ledger: wal.LedgerOff}, nil)
	off.close()
	requireSameExceptVolatile(t, "ledger off", live, offLive)
	replayed := replayFromWALAlone(t, off, nodeOptions{ledger: wal.LedgerOff})
	defer replayed.close()
	requireSame(t, "replay with the ledger off", offLive, replayed.fingerprint(workloadProbes))
}

// requireSameExceptVolatile compares two independent runs of the workload:
// UUID_V7 and TX_TIMESTAMP defaults legitimately differ between runs, so
// only sections that cannot contain them are compared.
func requireSameExceptVolatile(t *testing.T, label string, a, b fingerprint) {
	t.Helper()
	requireSame(t, label, a.without(containsVolatile), b.without(containsVolatile))
}

func containsVolatile(section string) bool {
	return strings.Contains(section, "item_moves") || strings.HasPrefix(section, "as-of-lsn")
}

// TestIdenticalWorkloadsWriteIdenticalWALs: two engines running the same
// statements, with no volatile defaults, write byte-identical logical WALs,
// so their ledger chains end at the same value.
func TestIdenticalWorkloadsWriteIdenticalWALs(t *testing.T) {
	var heads []wal.LedgerHead
	var logs [][]string
	for i := 0; i < 2; i++ {
		n := openNode(t, t.TempDir(), nodeOptions{})
		for _, phase := range stableWorkloadPhases() {
			n.run(phase...)
		}
		heads = append(heads, n.store.LedgerHead())
		var lines []string
		for _, r := range n.records() {
			lines = append(lines, renderRecord(r))
		}
		logs = append(logs, lines)
		n.close()
	}
	for i := range logs[0] {
		if i >= len(logs[1]) || logs[0][i] != logs[1][i] {
			other := "<missing>"
			if i < len(logs[1]) {
				other = logs[1][i]
			}
			t.Fatalf("WAL record %d differs between identical runs:\n  run 1: %s\n  run 2: %s", i+1, logs[0][i], other)
		}
	}
	if len(logs[0]) != len(logs[1]) || heads[0] != heads[1] {
		t.Fatalf("identical runs ended differently: %d vs %d records, heads %+v vs %+v", len(logs[0]), len(logs[1]), heads[0], heads[1])
	}
}

// TestFixtureReplaysIdentically applies the demo fixture and checks replay
// against the live state.
func TestFixtureReplaysIdentically(t *testing.T) {
	fixture, err := fixtures.LoadFile(filepath.Join("..", "..", "internal", "fixtures", "testdata", "demo.json"))
	if err != nil {
		t.Fatal(err)
	}
	n := openNode(t, t.TempDir(), nodeOptions{})
	if err := fixtures.Apply(context.Background(), fixture, engineExec{n.engine, n.engine.NewSession()}); err != nil {
		t.Fatal(err)
	}
	p := probes{entities: []entityProbe{
		{"patients", "patient_entity", "patient-1"},
		{"clinical", "admission_entity", "admission-1"},
		{"billing", "invoice_entity", "invoice-1"},
		{"billing", "invoice_entity", "invoice-2"},
	}}
	live := n.fingerprint(p)
	n.close()
	fresh := replayFromWALAlone(t, n, nodeOptions{})
	defer fresh.close()
	requireSame(t, "fixture replay", live, fresh.fingerprint(p))
}

type engineExec struct {
	engine  *executor.Engine
	session *executor.Session
}

func (e engineExec) Exec(ctx context.Context, sql string) error {
	_, err := e.engine.Execute(ctx, e.session, sql)
	return err
}
