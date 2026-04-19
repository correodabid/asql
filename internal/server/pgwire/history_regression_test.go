package pgwire

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/engine/executor"

	"github.com/jackc/pgx/v5"
)

type pgwireHistorySnapshot struct {
	Operation string
	ID        int64
	Name      string
}

func TestPGWireForHistoryRegressionStableMetadataAndRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataDir := filepath.Join(t.TempDir(), "history-regression-data")
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: dataDir,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}
	t.Cleanup(server.Stop)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for test: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeOnListener(ctx, listener)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("pgwire server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire server shutdown")
		}
	})

	conn, err := pgx.Connect(ctx, "postgres://asql@"+listener.Addr().String()+"/asql?sslmode=disable&default_query_exec_mode=simple_protocol")
	if err != nil {
		t.Fatalf("connect pgx: %v", err)
	}
	defer conn.Close(ctx)

	for _, sql := range []string{
		"BEGIN DOMAIN historyreg",
		"CREATE TABLE items (id INT PRIMARY KEY, name TEXT)",
		"INSERT INTO items (id, name) VALUES (1, 'Alice')",
		"COMMIT",
		"BEGIN DOMAIN historyreg",
		"UPDATE items SET name = 'Bob' WHERE id = 1",
		"COMMIT",
		"BEGIN DOMAIN historyreg",
		"DELETE FROM items WHERE id = 1",
		"COMMIT",
	} {
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	rows, err := conn.Query(ctx, "SELECT * FROM historyreg.items FOR HISTORY WHERE id = 1")
	if err != nil {
		t.Fatalf("history query: %v", err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	gotColumns := make([]string, 0, len(fields))
	gotOIDs := make([]uint32, 0, len(fields))
	for _, field := range fields {
		gotColumns = append(gotColumns, string(field.Name))
		gotOIDs = append(gotOIDs, field.DataTypeOID)
	}
	wantColumns := []string{executor.HistoryOperationColumnName, executor.HistoryCommitLSNColumnName, "id", "name"}
	wantOIDs := []uint32{25, 20, 20, 25}
	if !reflect.DeepEqual(gotColumns, wantColumns) {
		t.Fatalf("unexpected FOR HISTORY columns: got %v want %v", gotColumns, wantColumns)
	}
	if !reflect.DeepEqual(gotOIDs, wantOIDs) {
		t.Fatalf("unexpected FOR HISTORY type OIDs: got %v want %v", gotOIDs, wantOIDs)
	}

	var snapshots []pgwireHistorySnapshot
	var previousCommitLSN int64
	for rows.Next() {
		var operation string
		var commitLSN int64
		var id int64
		var name string
		if err := rows.Scan(&operation, &commitLSN, &id, &name); err != nil {
			t.Fatalf("scan history row: %v", err)
		}
		if commitLSN <= previousCommitLSN {
			t.Fatalf("expected strictly increasing commit LSNs, got prev=%d current=%d", previousCommitLSN, commitLSN)
		}
		previousCommitLSN = commitLSN
		snapshots = append(snapshots, pgwireHistorySnapshot{Operation: operation, ID: id, Name: name})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate history rows: %v", err)
	}

	wantSnapshots := []pgwireHistorySnapshot{
		{Operation: "INSERT", ID: 1, Name: "Alice"},
		{Operation: "UPDATE", ID: 1, Name: "Bob"},
		{Operation: "DELETE", ID: 1, Name: "Bob"},
	}
	if !reflect.DeepEqual(snapshots, wantSnapshots) {
		t.Fatalf("unexpected FOR HISTORY row snapshot: got %#v want %#v", snapshots, wantSnapshots)
	}
}
