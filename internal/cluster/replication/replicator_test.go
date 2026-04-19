package replication

import (
	"context"
	"path/filepath"
	"testing"

	grpcapi "github.com/correodabid/asql/internal/server/grpc"
	"github.com/correodabid/asql/internal/storage/wal"
)

func TestApplyBatchDetectsDivergence(t *testing.T) {
	ctx := context.Background()
	store, err := wal.NewSegmentedLogStore(filepath.Join(t.TempDir(), "follower.wal"), wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new file log store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	err = applyBatch(ctx, store, []grpcapi.StreamWALResponse{{LSN: 2, TxID: "x", Type: "BEGIN"}})
	if err == nil {
		t.Fatal("expected divergence error for out-of-order lsn")
	}
}
