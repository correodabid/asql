package executor

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/domains"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/wal"
)

func TestCaptureSnapshotWithCatalogStripsChangeLogAndClonesRows(t *testing.T) {
	catalog := domains.NewCatalog()
	catalog.RegisterTable("d", "t")

	row := []ast.Literal{{Kind: ast.LiteralString, StringValue: "r1"}}
	state := &readableState{
		domains: map[string]*domainState{
			"d": {
				tables: map[string]*tableState{
					"t": {
						columns:           []string{"id"},
						columnDefinitions: map[string]ast.ColumnDefinition{},
						columnIndex:       map[string]int{"id": 0},
						rows:              [][]ast.Literal{row},
						changeLog: []changeLogEntry{{
							operation: "INSERT",
							newRow: map[string]ast.Literal{
								"id": {Kind: ast.LiteralString, StringValue: "r1"},
							},
						}},
					},
				},
			},
		},
		headLSN:   7,
		logicalTS: 11,
	}

	snap := captureSnapshotWithCatalog(state, cloneCatalog(catalog))
	snapTable := snap.state.domains["d"].tables["t"]
	if len(snapTable.changeLog) != 0 {
		t.Fatalf("expected snapshot changeLog to be stripped, got %d entries", len(snapTable.changeLog))
	}

	state.domains["d"].tables["t"].rows[0][0] = ast.Literal{Kind: ast.LiteralString, StringValue: "mutated"}
	got := snapTable.rows[0][0].StringValue
	if got != "r1" {
		t.Fatalf("expected snapshot row to be isolated from live state, got %q", got)
	}
}

func TestRestoreSnapshotSharedDomainsStayImmutableAcrossWrite(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restore-snapshot-shared.wal")

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN bench",
		"CREATE TABLE entries (id INT PRIMARY KEY, payload TEXT)",
		"INSERT INTO entries (id, payload) VALUES (1, 'one')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	snap := captureSnapshot(engine.readState.Load(), engine.catalog)
	engine.restoreSnapshot(&snap)

	session = engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN bench",
		"INSERT INTO entries (id, payload) VALUES (2, 'two')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute after restore %q: %v", sql, err)
		}
	}

	snapTable := snap.state.domains["bench"].tables["entries"]
	if got := len(snapTable.rows); got != 1 {
		t.Fatalf("expected captured snapshot rows to remain immutable, got %d", got)
	}
	if got := snapTable.rows[0][1].StringValue; got != "one" {
		t.Fatalf("expected captured snapshot payload to remain 'one', got %q", got)
	}

	stateTable := engine.readState.Load().domains["bench"].tables["entries"]
	if got := len(stateTable.rows); got != 2 {
		t.Fatalf("expected restored engine state to include new row, got %d", got)
	}
}

func TestAdaptiveSnapshotInterval(t *testing.T) {
	tests := []struct {
		mutations uint64
		want      int
	}{
		{mutations: 0, want: defaultSnapshotInterval},
		{mutations: snapshotIntervalMediumThreshold - 1, want: defaultSnapshotInterval},
		{mutations: snapshotIntervalMediumThreshold, want: snapshotIntervalMedium},
		{mutations: snapshotIntervalHighThreshold, want: snapshotIntervalHigh},
		{mutations: snapshotIntervalXLThreshold, want: snapshotIntervalXL},
	}

	for _, tt := range tests {
		if got := adaptiveSnapshotInterval(tt.mutations); got != tt.want {
			t.Fatalf("adaptiveSnapshotInterval(%d) = %d, want %d", tt.mutations, got, tt.want)
		}
	}
}

func TestAdaptiveDiskCheckpointWALBytes(t *testing.T) {
	tests := []struct {
		mutations uint64
		want      uint64
	}{
		{mutations: 0, want: diskCheckpointWALBytes},
		{mutations: diskCheckpointWALBytesMediumThreshold - 1, want: diskCheckpointWALBytes},
		{mutations: diskCheckpointWALBytesMediumThreshold, want: diskCheckpointWALBytesMedium},
		{mutations: diskCheckpointWALBytesHighThreshold, want: diskCheckpointWALBytesHigh},
		{mutations: diskCheckpointWALBytesXLThreshold, want: diskCheckpointWALBytesXL},
	}

	for _, tt := range tests {
		if got := adaptiveDiskCheckpointWALBytes(tt.mutations); got != tt.want {
			t.Fatalf("adaptiveDiskCheckpointWALBytes(%d) = %d, want %d", tt.mutations, got, tt.want)
		}
	}
}

func TestAdaptivePersistedCheckpointMutationInterval(t *testing.T) {
	tests := []struct {
		name           string
		mutations      uint64
		recentPressure int
		recentSamples  int
		want           int
	}{
		{
			name:           "insufficient sample keeps base",
			mutations:      0,
			recentPressure: int(mutationPressureUpdate) * (recentMutationMinSampleSize - 1),
			recentSamples:  recentMutationMinSampleSize - 1,
			want:           defaultSnapshotInterval,
		},
		{
			name:           "insert heavy keeps base",
			mutations:      snapshotIntervalMediumThreshold,
			recentPressure: int(mutationPressureInsert) * recentMutationMinSampleSize,
			recentSamples:  recentMutationMinSampleSize,
			want:           snapshotIntervalMedium,
		},
		{
			name:           "mixed update pressure halves interval",
			mutations:      snapshotIntervalMediumThreshold,
			recentPressure: recentMutationMinSampleSize * 3,
			recentSamples:  recentMutationMinSampleSize,
			want:           snapshotIntervalMedium / 2,
		},
		{
			name:           "minimum floor applies",
			mutations:      0,
			recentPressure: recentMutationMinSampleSize * 4,
			recentSamples:  recentMutationMinSampleSize,
			want:           minPersistedCheckpointMutationInterval,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := adaptivePersistedCheckpointMutationInterval(tt.mutations, tt.recentPressure, tt.recentSamples); got != tt.want {
				t.Fatalf("adaptivePersistedCheckpointMutationInterval(%d, %d, %d) = %d, want %d", tt.mutations, tt.recentPressure, tt.recentSamples, got, tt.want)
			}
		})
	}
}

func TestRecordMutationPressureRollingWindow(t *testing.T) {
	engine := &Engine{}

	for i := 0; i < recentMutationWindowSize; i++ {
		engine.recordMutationPressure(mutationPressureInsert)
	}
	if engine.recentMutationCount != recentMutationWindowSize {
		t.Fatalf("recentMutationCount = %d, want %d", engine.recentMutationCount, recentMutationWindowSize)
	}
	if engine.recentMutationPressure != recentMutationWindowSize*int(mutationPressureInsert) {
		t.Fatalf("recentMutationPressure = %d, want %d", engine.recentMutationPressure, recentMutationWindowSize*int(mutationPressureInsert))
	}

	for i := 0; i < recentMutationWindowSize; i++ {
		engine.recordMutationPressure(mutationPressureUpdate)
	}
	if engine.recentMutationCount != recentMutationWindowSize {
		t.Fatalf("recentMutationCount after rollover = %d, want %d", engine.recentMutationCount, recentMutationWindowSize)
	}
	if engine.recentMutationPressure != recentMutationWindowSize*int(mutationPressureUpdate) {
		t.Fatalf("recentMutationPressure after rollover = %d, want %d", engine.recentMutationPressure, recentMutationWindowSize*int(mutationPressureUpdate))
	}
}

func TestSnapshotStoreClosest(t *testing.T) {
	store := newSnapshotStore()

	store.add(engineSnapshot{lsn: 100})
	store.add(engineSnapshot{lsn: 200})
	store.add(engineSnapshot{lsn: 300})

	tests := []struct {
		target  uint64
		wantLSN uint64
		wantNil bool
	}{
		{target: 50, wantNil: true},
		{target: 100, wantLSN: 100},
		{target: 150, wantLSN: 100},
		{target: 200, wantLSN: 200},
		{target: 250, wantLSN: 200},
		{target: 300, wantLSN: 300},
		{target: 999, wantLSN: 300},
	}

	for _, tt := range tests {
		got := store.closest(tt.target)
		if tt.wantNil {
			if got != nil {
				t.Errorf("closest(%d) = lsn %d, want nil", tt.target, got.lsn)
			}
			continue
		}
		if got == nil {
			t.Errorf("closest(%d) = nil, want lsn %d", tt.target, tt.wantLSN)
			continue
		}
		if got.lsn != tt.wantLSN {
			t.Errorf("closest(%d) = lsn %d, want %d", tt.target, got.lsn, tt.wantLSN)
		}
	}
}

func TestSnapshotStoreEmpty(t *testing.T) {
	store := newSnapshotStore()

	if got := store.closest(100); got != nil {
		t.Errorf("closest on empty store should be nil, got lsn %d", got.lsn)
	}
	if store.count() != 0 {
		t.Errorf("count on empty store should be 0, got %d", store.count())
	}
}

// TestSnapshotAcceleratedTimeTravel creates enough records to trigger snapshot
// creation, then verifies that time-travel queries at various LSNs produce
// identical results whether served from a snapshot or from full replay.
func TestSnapshotAcceleratedTimeTravel(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "snap.wal")

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	// Create table
	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN test")
	exec("CREATE TABLE items (id INT, name TEXT)")
	exec("COMMIT")

	// Insert more than defaultSnapshotInterval records to ensure snapshots are created.
	// Each commit produces 3 WAL records (BEGIN + MUTATION + COMMIT),
	// so insertCount transactions produce 3*insertCount mutations total.
	// We need >defaultSnapshotInterval mutations for at least 1 snapshot.
	insertCount := defaultSnapshotInterval + 100 // guarantee at least one snapshot
	lsnAfterCreate := uint64(0)

	for i := 1; i <= insertCount; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN test")
		exec(fmt.Sprintf("INSERT INTO items (id, name) VALUES (%d, 'item-%d')", i, i))
		exec("COMMIT")
		if i == 1 {
			// After: CREATE TABLE (3 records) + 1st insert (3 records) = LSN 6
			// But let's just check the head after first insert commit
		}
	}
	engine.snapshotWg.Wait()

	// Verify snapshots were created
	engine.snapshots.mu.Lock()
	snapCount := engine.snapshots.count()
	engine.snapshots.mu.Unlock()
	headLSN := engine.readState.Load().headLSN

	if snapCount == 0 {
		t.Fatalf("expected at least 1 snapshot, got 0")
	}
	t.Logf("snapshots created: %d, head LSN: %d", snapCount, headLSN)

	// Record LSN after table creation (3 records: BEGIN + CREATE TABLE mutation + COMMIT)
	lsnAfterCreate = 3

	// Time-travel to right after table creation — should be 0 items
	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, name FROM items", []string{"test"}, lsnAfterCreate)
	if err != nil {
		t.Fatalf("time travel to LSN %d: %v", lsnAfterCreate, err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows at LSN %d, got %d", lsnAfterCreate, len(result.Rows))
	}

	// Time-travel to after 10th insert (each insert is 3 records: BEGIN + MUTATION + COMMIT)
	// LSN after 10 inserts = 3 (create) + 10*3 = 33
	lsnAfter10 := uint64(3 + 10*3)
	result, err = engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, name FROM items", []string{"test"}, lsnAfter10)
	if err != nil {
		t.Fatalf("time travel to LSN %d: %v", lsnAfter10, err)
	}
	if len(result.Rows) != 10 {
		t.Errorf("expected 10 rows at LSN %d, got %d", lsnAfter10, len(result.Rows))
	}

	// Time-travel to halfway point — this should benefit from snapshot
	midInsert := insertCount / 2
	lsnAtMid := uint64(3 + midInsert*3)
	result, err = engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, name FROM items", []string{"test"}, lsnAtMid)
	if err != nil {
		t.Fatalf("time travel to LSN %d: %v", lsnAtMid, err)
	}
	if len(result.Rows) != midInsert {
		t.Errorf("expected %d rows at LSN %d, got %d", midInsert, lsnAtMid, len(result.Rows))
	}

	// Time-travel to head — should use fast path (0 rows replayed)
	result, err = engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, name FROM items", []string{"test"}, headLSN)
	if err != nil {
		t.Fatalf("time travel to head LSN %d: %v", headLSN, err)
	}
	if len(result.Rows) != insertCount {
		t.Errorf("expected %d rows at head LSN, got %d", insertCount, len(result.Rows))
	}
}

// TestSnapshotSurvivesReplay verifies snapshots are recreated after engine
// restart (replay from WAL).
func TestSnapshotSurvivesReplay(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replay-snap.wal")

	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new log store: %v", err)
	}

	engine, err := New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	exec := func(sql string) {
		t.Helper()
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	exec("BEGIN DOMAIN test")
	exec("CREATE TABLE counters (id INT, val INT)")
	exec("COMMIT")

	for i := 1; i <= defaultSnapshotInterval+50; i++ {
		session = engine.NewSession()
		exec("BEGIN DOMAIN test")
		exec(fmt.Sprintf("INSERT INTO counters (id, val) VALUES (%d, %d)", i, i*10))
		exec("COMMIT")
	}
	engine.snapshotWg.Wait()

	// Check snapshots exist
	engine.snapshots.mu.Lock()
	origSnaps := engine.snapshots.count()
	engine.snapshots.mu.Unlock()
	origHead := engine.readState.Load().headLSN

	if origSnaps == 0 {
		t.Fatal("expected snapshots before restart")
	}

	// Close and reopen — simulate restart
	_ = store.Close()

	store2, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen log store: %v", err)
	}
	t.Cleanup(func() { _ = store2.Close() })

	engine2, err := New(ctx, store2, "")
	if err != nil {
		t.Fatalf("new engine after restart: %v", err)
	}

	engine2.snapshots.mu.Lock()
	newSnaps := engine2.snapshots.count()
	engine2.snapshots.mu.Unlock()
	newHead := engine2.readState.Load().headLSN

	if newSnaps == 0 {
		t.Fatal("expected snapshots after restart replay")
	}
	if newHead != origHead {
		t.Errorf("head LSN mismatch after restart: got %d want %d", newHead, origHead)
	}

	// Historical query should work via snapshot
	midpoint := defaultSnapshotInterval / 2
	lsnAtMid := uint64(3 + midpoint*3)
	result, err := engine2.TimeTravelQueryAsOfLSN(ctx, "SELECT id, val FROM counters", []string{"test"}, lsnAtMid)
	if err != nil {
		t.Fatalf("time travel after restart: %v", err)
	}
	if len(result.Rows) != midpoint {
		t.Errorf("expected %d rows at LSN %d after restart, got %d", midpoint, lsnAtMid, len(result.Rows))
	}
}
