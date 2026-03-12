package executor

import "testing"

func TestTrimChangeLogUsesLowWatermark(t *testing.T) {
	table := &tableState{
		changeLog: make([]changeLogEntry, maxChangeLogPerTable+1),
	}
	for i := range table.changeLog {
		table.changeLog[i] = changeLogEntry{commitLSN: uint64(i + 1)}
	}

	trimChangeLog(table)

	wantLen := maxChangeLogPerTable - changeLogTrimBatch
	if len(table.changeLog) != wantLen {
		t.Fatalf("expected trimmed len %d, got %d", wantLen, len(table.changeLog))
	}
	if got := table.changeLog[0].commitLSN; got != uint64(changeLogTrimBatch+2) {
		t.Fatalf("expected oldest retained LSN %d, got %d", changeLogTrimBatch+2, got)
	}
}

func TestTrimChangeLogCompactsOversizedCapacity(t *testing.T) {
	changeLog := make([]changeLogEntry, maxChangeLogPerTable+1, maxChangeLogPerTable*3)
	for i := range changeLog {
		changeLog[i] = changeLogEntry{commitLSN: uint64(i + 1)}
	}
	table := &tableState{changeLog: changeLog}

	trimChangeLog(table)

	if cap(table.changeLog) > maxChangeLogPerTable {
		t.Fatalf("expected compacted capacity <= %d, got %d", maxChangeLogPerTable, cap(table.changeLog))
	}
	if len(table.changeLog) != maxChangeLogPerTable-changeLogTrimBatch {
		t.Fatalf("unexpected trimmed len %d", len(table.changeLog))
	}
}