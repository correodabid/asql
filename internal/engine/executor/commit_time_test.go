package executor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/storage/wal"
)

// scriptedClock returns the given times in order, then repeats the last.
type scriptedClock struct{ times []time.Time }

func (c *scriptedClock) Now() time.Time {
	now := c.times[0]
	if len(c.times) > 1 {
		c.times = c.times[1:]
	}
	return now
}

func at(minute int) time.Time { return time.Date(2026, 3, 1, 10, minute, 0, 0, time.UTC) }

func openClockNode(t *testing.T, dir string, clock *scriptedClock) *appendOnlyNode {
	t.Helper()
	n := &appendOnlyNode{t: t, walPath: filepath.Join(dir, "wal"), snapDir: filepath.Join(dir, "snap")}
	if err := os.MkdirAll(n.snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := wal.NewSegmentedLogStore(n.walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(context.Background(), store, n.snapDir, WithClock(clock))
	if err != nil {
		t.Fatal(err)
	}
	n.store, n.engine = store, engine
	return n
}

func commitTimes(t *testing.T, engine *Engine) map[uint64]int64 {
	t.Helper()
	timeline, err := engine.TimelineCommits(context.Background(), 0, engine.HeadLSN(), "", 0)
	if err != nil {
		t.Fatal(err)
	}
	times := map[uint64]int64{}
	for _, c := range timeline {
		times[c.LSN] = c.CommitTimeMicros
	}
	return times
}

// TestCommitTimesResolveDates: each commit records the leader's time, which
// never goes back even when the clock does, and a date resolves to the last
// commit at or before it, the same after a restart.
func TestCommitTimesResolveDates(t *testing.T) {
	dir := t.TempDir()
	// The third commit's clock jumps back before the second's.
	clock := &scriptedClock{times: []time.Time{at(0), at(10), at(5), at(20)}}
	n := openClockNode(t, dir, clock)
	n.mustTx("CREATE TABLE ev (id INT PRIMARY KEY)")
	created := n.engine.HeadLSN()
	n.mustTx("INSERT INTO ev (id) VALUES (1)")
	first := n.engine.HeadLSN()
	n.mustTx("INSERT INTO ev (id) VALUES (2)")
	second := n.engine.HeadLSN()
	n.mustTx("INSERT INTO ev (id) VALUES (3)")
	third := n.engine.HeadLSN()

	times := commitTimes(t, n.engine)
	want := map[uint64]time.Time{created: at(0), first: at(10), second: at(10), third: at(20)}
	for lsn, w := range want {
		if times[lsn] != w.UnixMicro() {
			t.Fatalf("commit %d at %s, want %s", lsn, time.UnixMicro(times[lsn]).UTC(), w)
		}
	}

	check := func(label string) {
		t.Helper()
		for date, wantLSN := range map[time.Time]uint64{
			at(0).Add(-time.Second): 0,
			at(0):                   created,
			at(9):                   created,
			at(10):                  second,
			at(15):                  second,
			at(59):                  third,
		} {
			lsn, err := n.engine.LSNForCommitTime(date.UnixMicro())
			if err != nil || lsn != wantLSN {
				t.Fatalf("%s: %s resolves to LSN %d (err %v), want %d", label, date, lsn, err, wantLSN)
			}
		}
	}
	check("live")
	for _, label := range []string{"restart", "restart from a snapshot"} {
		n.close()
		n.open()
		check(label)
	}
	n.close()
	for _, f := range []string{n.snapDir, filepath.Join(dir, commitTimeIndexFileName)} {
		if err := os.RemoveAll(f); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(n.snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	n.open()
	check("replay of the WAL alone")

	// After a restart the next commit still does not go below the last one.
	n.close()
	n = openClockNode(t, dir, &scriptedClock{times: []time.Time{at(1)}})
	defer n.close()
	n.mustTx("INSERT INTO ev (id) VALUES (4)")
	if got := commitTimes(t, n.engine)[n.engine.HeadLSN()]; got != at(20).UnixMicro() {
		t.Fatalf("commit after restart with a clock behind at %s, want %s", time.UnixMicro(got).UTC(), at(20))
	}
}

// TestDatesBeforeRecordedCommitTimesFail: commits written before commits
// recorded their time cannot be placed in time, so a date before the first
// recorded one fails instead of guessing.
func TestDatesBeforeRecordedCommitTimesFail(t *testing.T) {
	dir := t.TempDir()
	// A WAL from before ADR 0007: a transaction whose COMMIT has no payload.
	store, err := wal.NewSegmentedLogStore(filepath.Join(dir, "wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendBatch(context.Background(), []ports.WALRecord{
		{TxID: "legacy-1", Type: walTypeBegin, Timestamp: 1},
		{TxID: "legacy-1", Type: walTypeCommit, Timestamp: 2},
	}); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	n := openClockNode(t, dir, &scriptedClock{times: []time.Time{at(30)}})
	defer n.close()
	n.mustTx("CREATE TABLE ev (id INT PRIMARY KEY)")
	if lsn, err := n.engine.LSNForCommitTime(at(30).UnixMicro()); err != nil || lsn != n.engine.HeadLSN() {
		t.Fatalf("date of the first recorded commit: LSN %d, err %v", lsn, err)
	}
	if _, err := n.engine.LSNForCommitTime(at(29).UnixMicro()); !errors.Is(err, ErrCommitTimeUnknown) {
		t.Fatalf("date before the first recorded commit: %v, want ErrCommitTimeUnknown", err)
	}
}

// TestDatesAmongUntimedCommitsFail: commits an older version wrote after a
// downgrade have no time; a date that falls among them fails instead of
// resolving past them.
func TestDatesAmongUntimedCommitsFail(t *testing.T) {
	dir := t.TempDir()
	n := openClockNode(t, dir, &scriptedClock{times: []time.Time{at(0)}})
	n.mustTx("CREATE TABLE ev (id INT PRIMARY KEY)")
	n.close()

	// An older version, with no commit time, writes a transaction.
	store, err := wal.NewSegmentedLogStore(n.walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendBatch(context.Background(), []ports.WALRecord{
		{TxID: "old-1", Type: walTypeBegin, Timestamp: 100},
		{TxID: "old-1", Type: walTypeCommit, Timestamp: 101},
	}); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	for _, restart := range []string{"index caught up", "index rebuilt"} {
		if restart == "index rebuilt" {
			if err := os.Remove(filepath.Join(dir, commitTimeIndexFileName)); err != nil {
				t.Fatal(err)
			}
		}
		n = openClockNode(t, dir, &scriptedClock{times: []time.Time{at(30)}})
		if lsn, err := n.engine.LSNForCommitTime(at(0).Add(-time.Second).UnixMicro()); err != nil || lsn != 0 {
			t.Fatalf("%s: before every commit: LSN %d, err %v", restart, lsn, err)
		}
		if _, err := n.engine.LSNForCommitTime(at(10).UnixMicro()); !errors.Is(err, ErrCommitTimeUnknown) {
			t.Fatalf("%s: a date after the untimed commit: %v, want ErrCommitTimeUnknown", restart, err)
		}
		n.close()
	}
	n = openClockNode(t, dir, &scriptedClock{times: []time.Time{at(30)}})
	defer n.close()
	n.mustTx("INSERT INTO ev (id) VALUES (1)")
	if lsn, err := n.engine.LSNForCommitTime(at(30).UnixMicro()); err != nil || lsn != n.engine.HeadLSN() {
		t.Fatalf("a date after the next timed commit: LSN %d, err %v", lsn, err)
	}
}
