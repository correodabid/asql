package executor

import (
	"testing"

	"asql/internal/engine/planner"
	"asql/internal/engine/ports"
)

func TestAppendWALRecordCacheAppendsSmallCache(t *testing.T) {
	engine := &Engine{}
	engine.storeWALRecordCache(2, []ports.WALRecord{{LSN: 1}, {LSN: 2}})

	engine.appendWALRecordCache([]ports.WALRecord{{LSN: 3}, {LSN: 4}})

	cached := engine.walRecordsCache.Load()
	if cached == nil {
		t.Fatal("expected wal cache to remain populated")
	}
	if cached.headLSN != 4 {
		t.Fatalf("headLSN=%d want 4", cached.headLSN)
	}
	if len(cached.records) != 4 {
		t.Fatalf("len(records)=%d want 4", len(cached.records))
	}
}

func TestAppendWALRecordCacheInvalidatesLargeCache(t *testing.T) {
	engine := &Engine{}
	records := make([]ports.WALRecord, walRecordCacheMaxIncrementalRecords)
	for i := range records {
		records[i] = ports.WALRecord{LSN: uint64(i + 1)}
	}
	engine.storeWALRecordCache(records[len(records)-1].LSN, records)

	engine.appendWALRecordCache([]ports.WALRecord{{LSN: uint64(len(records) + 1)}})

	if cached := engine.walRecordsCache.Load(); cached != nil {
		t.Fatal("expected large wal cache to be invalidated on append")
	}
}

func TestClearWALRecordCacheAlsoClearsReplayPlanCache(t *testing.T) {
	engine := &Engine{}
	engine.storeWALRecordCache(2, []ports.WALRecord{{LSN: 1}, {LSN: 2}})
	engine.walReplayPlansCache.Store(&walReplayPlanCache{
		headLSN: 2,
		entries: []replayPlanCacheEntry{{plan: planner.Plan{TableName: "entries"}, ok: true}},
	})

	engine.clearWALRecordCache()

	if cached := engine.walReplayPlansCache.Load(); cached != nil {
		t.Fatal("expected replay plan cache to be cleared together with wal cache")
	}
}

func TestAppendWALRecordCacheInvalidationAlsoClearsReplayPlanCache(t *testing.T) {
	engine := &Engine{}
	records := make([]ports.WALRecord, walRecordCacheMaxIncrementalRecords)
	for i := range records {
		records[i] = ports.WALRecord{LSN: uint64(i + 1)}
	}
	engine.storeWALRecordCache(records[len(records)-1].LSN, records)
	engine.walReplayPlansCache.Store(&walReplayPlanCache{
		headLSN: records[len(records)-1].LSN,
		entries: []replayPlanCacheEntry{{plan: planner.Plan{TableName: "entries"}, ok: true}},
	})

	engine.appendWALRecordCache([]ports.WALRecord{{LSN: uint64(len(records) + 1)}})

	if cached := engine.walReplayPlansCache.Load(); cached != nil {
		t.Fatal("expected replay plan cache to be invalidated with wal cache")
	}
}
