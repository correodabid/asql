package executor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	auditstore "asql/internal/storage/audit"
	"asql/internal/storage/wal"
)

func TestRowHistoryUsesAuditStoreForQualifiedStringPKAcrossRestart(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	walPath := filepath.Join(baseDir, "engine.wal")
	snapDir := filepath.Join(baseDir, "snaps")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatalf("mkdir snap dir: %v", err)
	}
	auditPath := filepath.Join(baseDir, "audit", "audit")
	if err := os.MkdirAll(filepath.Dir(auditPath), 0o755); err != nil {
		t.Fatalf("mkdir audit dir: %v", err)
	}

	walStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new wal store: %v", err)
	}
	auditLog, err := auditstore.New(auditPath)
	if err != nil {
		_ = walStore.Close()
		t.Fatalf("new audit store: %v", err)
	}

	engine, err := New(ctx, walStore, snapDir, WithAuditStore(auditLog))
	if err != nil {
		_ = walStore.Close()
		_ = auditLog.Close()
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN execution",
		"CREATE TABLE execution.batch_orders (id TEXT PRIMARY KEY, status TEXT, ebr_status TEXT)",
		"INSERT INTO execution.batch_orders (id, status, ebr_status) VALUES ('batch-001', 'PLANNED', 'DRAFT')",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	session = engine.NewSession()
	for _, sql := range []string{
		"BEGIN DOMAIN execution",
		"UPDATE execution.batch_orders SET status = 'IN_EXECUTION', ebr_status = 'EXECUTION_STARTED' WHERE id = 'batch-001'",
		"COMMIT",
	} {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("execute %q: %v", sql, err)
		}
	}

	waitForAuditHistoryEntries(t, ctx, auditLog, 2)

	historyResult, err := engine.RowHistory(ctx, "SELECT * FROM execution.batch_orders FOR HISTORY WHERE id = 'batch-001'", nil)
	if err != nil {
		t.Fatalf("row history before restart: %v", err)
	}
	if len(historyResult.Rows) != 2 {
		t.Fatalf("expected 2 history entries before restart, got %d: %+v", len(historyResult.Rows), historyResult.Rows)
	}

	engine.WaitPendingSnapshots()
	if err := walStore.Close(); err != nil {
		t.Fatalf("close wal store: %v", err)
	}
	if err := auditLog.Close(); err != nil {
		t.Fatalf("close audit store: %v", err)
	}

	reopenedWal, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen wal store: %v", err)
	}
	reopenedAudit, err := auditstore.New(auditPath)
	if err != nil {
		_ = reopenedWal.Close()
		t.Fatalf("reopen audit store: %v", err)
	}

	restarted, err := New(ctx, reopenedWal, snapDir, WithAuditStore(reopenedAudit))
	if err != nil {
		_ = reopenedWal.Close()
		_ = reopenedAudit.Close()
		t.Fatalf("restart engine: %v", err)
	}

	restartedHistory, err := restarted.RowHistory(ctx, "SELECT * FROM execution.batch_orders FOR HISTORY WHERE id = 'batch-001'", nil)
	if err != nil {
		t.Fatalf("row history after restart: %v", err)
	}
	if len(restartedHistory.Rows) != 2 {
		t.Fatalf("expected 2 history entries after restart, got %d: %+v", len(restartedHistory.Rows), restartedHistory.Rows)
	}
	if got := restartedHistory.Rows[0][HistoryOperationColumnName].StringValue; got != "INSERT" {
		t.Fatalf("expected first history op INSERT, got %s", got)
	}
	if got := restartedHistory.Rows[1][HistoryOperationColumnName].StringValue; got != "UPDATE" {
		t.Fatalf("expected second history op UPDATE, got %s", got)
	}

	restarted.WaitPendingSnapshots()
	_ = reopenedWal.Close()
	_ = reopenedAudit.Close()
}

func waitForAuditHistoryEntries(t *testing.T, ctx context.Context, store *auditstore.Store, minEntries int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := store.ReadAll(ctx)
		if err == nil && len(entries) >= minEntries {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	entries, err := store.ReadAll(ctx)
	if err != nil {
		t.Fatalf("read audit entries: %v", err)
	}
	t.Fatalf("expected at least %d audit entries, got %d", minEntries, len(entries))
}
