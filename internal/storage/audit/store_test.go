package audit

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/ports"
)

func entry(lsn uint64, op string) ports.AuditEntry {
	row := map[string]ast.Literal{"id": {Kind: ast.LiteralNumber, NumberValue: int64(lsn)}}
	e := ports.AuditEntry{CommitLSN: lsn, Domain: "d", Table: "t", Operation: op}
	if op == "DELETE" {
		e.OldRow = row
	} else {
		e.NewRow = row
	}
	return e
}

func commitLSNs(t *testing.T, s *Store) []uint64 {
	t.Helper()
	entries, err := s.ReadAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var lsns []uint64
	for _, e := range entries {
		lsns = append(lsns, e.CommitLSN)
	}
	return lsns
}

func requireCovered(t *testing.T, s *Store, want uint64) {
	t.Helper()
	if got, tracked := s.Covered(); got != want || !tracked {
		t.Fatalf("Covered() = %d, %t; want %d, true", got, tracked, want)
	}
}

func requireLSNs(t *testing.T, got []uint64, want ...uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("commit LSNs = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("commit LSNs = %v, want %v", got, want)
		}
	}
}

func openStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCoverageSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "audit")
	s := openStore(t, path)
	requireCovered(t, s, 0)
	if err := s.AppendCommitted(ctx, []ports.AuditEntry{entry(3, "INSERT"), entry(3, "INSERT")}, 3); err != nil {
		t.Fatal(err)
	}
	// A commit without row changes still advances coverage.
	if err := s.AppendCommitted(ctx, nil, 5); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendCommitted(ctx, []ports.AuditEntry{entry(8, "UPDATE")}, 9); err != nil {
		t.Fatal(err)
	}
	requireCovered(t, s, 9)
	_ = s.Close()

	s = openStore(t, path)
	defer s.Close()
	requireCovered(t, s, 9)
	requireLSNs(t, commitLSNs(t, s), 3, 3, 8)
}

// An unfinished write leaves entries after the last mark: readers ignore
// them and the next write drops them.
func TestUnmarkedTailIsIgnoredThenDropped(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "audit")
	s := openStore(t, path)
	if err := s.AppendCommitted(ctx, []ports.AuditEntry{entry(3, "INSERT")}, 3); err != nil {
		t.Fatal(err)
	}
	payload, err := encodeEntry(entry(7, "INSERT"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.inner.AppendBatch(ctx, []ports.WALRecord{{Type: recordType, Payload: payload}}); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s = openStore(t, path)
	defer s.Close()
	requireCovered(t, s, 3)
	requireLSNs(t, commitLSNs(t, s), 3)
	if err := s.AppendCommitted(ctx, []ports.AuditEntry{entry(7, "DELETE")}, 7); err != nil {
		t.Fatal(err)
	}
	requireLSNs(t, commitLSNs(t, s), 3, 7)
	entries, _ := s.ReadAll(ctx)
	if entries[1].Operation != "DELETE" {
		t.Fatalf("the unfinished write's entry survived: %+v", entries[1])
	}
}

func TestTruncateAfterKeepsWholeMarkedGroups(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, filepath.Join(t.TempDir(), "audit"))
	defer s.Close()
	for _, lsn := range []uint64{3, 6, 9} {
		if err := s.AppendCommitted(ctx, []ports.AuditEntry{entry(lsn, "INSERT")}, lsn); err != nil {
			t.Fatal(err)
		}
	}
	// 7 falls inside the group covering (6, 9]: that group goes.
	if err := s.TruncateAfter(ctx, 7); err != nil {
		t.Fatal(err)
	}
	requireCovered(t, s, 6)
	requireLSNs(t, commitLSNs(t, s), 3, 6)
	if err := s.TruncateAfter(ctx, 0); err != nil {
		t.Fatal(err)
	}
	requireCovered(t, s, 0)
	requireLSNs(t, commitLSNs(t, s))
	if err := s.AppendCommitted(ctx, []ports.AuditEntry{entry(4, "INSERT")}, 4); err != nil {
		t.Fatal(err)
	}
	requireLSNs(t, commitLSNs(t, s), 4)
}

// A store written before coverage marks existed is reported untracked and
// read in full.
func TestLegacyStoreIsUntracked(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "audit")
	s := openStore(t, path)
	for _, lsn := range []uint64{4, 2} {
		payload, err := encodeEntry(entry(lsn, "UPDATE"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.inner.AppendBatch(ctx, []ports.WALRecord{{Type: recordType, Payload: payload}}); err != nil {
			t.Fatal(err)
		}
	}
	_ = s.Close()

	s = openStore(t, path)
	defer s.Close()
	if _, tracked := s.Covered(); tracked {
		t.Fatal("legacy store reported as tracked")
	}
	requireLSNs(t, commitLSNs(t, s), 4, 2)
	// Adopting it keeps its entries.
	if err := s.AppendCommitted(ctx, nil, 4); err != nil {
		t.Fatal(err)
	}
	requireCovered(t, s, 4)
	requireLSNs(t, commitLSNs(t, s), 4, 2)
}
