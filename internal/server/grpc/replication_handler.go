package grpc

import (
	"context"
	"log/slog"
	"time"

	"asql/internal/engine/ports"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type replicationService struct {
	logStore ports.LogStore
	logger   *slog.Logger
}

func newReplicationService(logStore ports.LogStore, logger *slog.Logger) *replicationService {
	return &replicationService{logStore: logStore, logger: logger}
}

// writeNotifier is implemented by WAL stores that can push notifications
// instead of requiring callers to poll. The StreamWAL handler uses it to
// wake up immediately when a new record is written rather than sleeping for
// the fallback poll interval.
type writeNotifier interface {
	Subscribe() <-chan struct{}
}

// StreamWAL streams WAL records to a follower starting from request.FromLSN.
//
// Two modes:
//   - Batch mode (Limit > 0): sends exactly Limit records and closes the stream.
//     Used by CatchUpFromGRPC and integration tests.
//   - Persistent mode (Limit == 0): keeps the stream open and pushes new records
//     as they are written to the log. The stream stays alive until the follower
//     disconnects or the server context is cancelled. Used by
//     persistentStreamReplicator for near-realtime replication.
func (service *replicationService) StreamWAL(request *StreamWALRequest, stream ReplicationService_StreamWALServer) error {
	if request.FromLSN == 0 {
		request.FromLSN = 1
	}
	ctx := stream.Context()

	// Batch mode: read up to Limit records, send, and close.
	if request.Limit > 0 {
		records, err := service.logStore.ReadFrom(ctx, request.FromLSN, int(request.Limit))
		if err != nil {
			service.auditFailure("admin.replication_stream", err.Error(),
				slog.Uint64("from_lsn", request.FromLSN), slog.Uint64("limit", uint64(request.Limit)))
			return status.Error(codes.Internal, err.Error())
		}
		for _, record := range records {
			if err := stream.Send(&StreamWALResponse{
				LSN: record.LSN, Term: record.Term, TxID: record.TxID, Type: record.Type,
				Timestamp: record.Timestamp, Payload: record.Payload,
			}); err != nil {
				service.auditFailure("admin.replication_stream", err.Error(),
					slog.Uint64("from_lsn", request.FromLSN), slog.Uint64("limit", uint64(request.Limit)))
				return err
			}
		}
		service.auditSuccess("admin.replication_stream",
			slog.Uint64("from_lsn", request.FromLSN), slog.Uint64("limit", uint64(request.Limit)),
			slog.Int("records", len(records)))
		return nil
	}

	// Persistent mode: push records to the follower as soon as they are written.
	//
	// Design (zero-poll when idle):
	//   1. Subscribe() snapshots the notification channel BEFORE ReadFrom so
	//      any concurrent write is never missed.
	//   2. Read a page of up to catchUpPageSize records and send them all.
	//   3. If the page was full there may be more records immediately; loop.
	//   4. If the page was partial (0 or < full), the follower is caught up;
	//      block on the notification channel – no CPU wasted while idle.
	//
	// Latency: write→replicated = network RTT + apply time (no artificial delay).
	const (
		catchUpPageSize = 2000
		idleTimeout     = 5 * time.Second // safety net if notification is ever missed
	)
	notifier, hasNotify := service.logStore.(writeNotifier)

	fromLSN := request.FromLSN
	total := 0
	for {
		// Snapshot the notification channel before reading so any write that
		// arrives during ReadFrom is captured and wakes the idle select below.
		var notifyCh <-chan struct{}
		if hasNotify {
			notifyCh = notifier.Subscribe()
		}

		records, err := service.logStore.ReadFrom(ctx, fromLSN, catchUpPageSize)
		if err != nil {
			if ctx.Err() != nil {
				service.auditSuccess("admin.replication_stream",
					slog.Uint64("from_lsn", request.FromLSN), slog.Int("records", total))
				return nil
			}
			service.auditFailure("admin.replication_stream", err.Error(), slog.Uint64("from_lsn", fromLSN))
			return status.Error(codes.Internal, err.Error())
		}
		for _, record := range records {
			if err := stream.Send(&StreamWALResponse{
				LSN: record.LSN, Term: record.Term, TxID: record.TxID, Type: record.Type,
				Timestamp: record.Timestamp, Payload: record.Payload,
			}); err != nil {
				return err
			}
			fromLSN = record.LSN + 1
			total++
		}

		// Full page → might be more records waiting; loop immediately.
		if len(records) == catchUpPageSize {
			if ctx.Err() != nil {
				service.auditSuccess("admin.replication_stream",
					slog.Uint64("from_lsn", request.FromLSN), slog.Int("records", total))
				return nil
			}
			continue
		}

		// Partial (or empty) page → follower is caught up. Wait for the next
		// write notification or context cancellation.
		if notifyCh != nil {
			select {
			case <-ctx.Done():
				service.auditSuccess("admin.replication_stream",
					slog.Uint64("from_lsn", request.FromLSN), slog.Int("records", total))
				return nil
			case <-notifyCh: // wakes immediately on next Append()
			case <-time.After(idleTimeout): // safety net
			}
		} else {
			// Fallback for test mocks that don't implement writeNotifier.
			select {
			case <-ctx.Done():
				service.auditSuccess("admin.replication_stream",
					slog.Uint64("from_lsn", request.FromLSN), slog.Int("records", total))
				return nil
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

func (service *replicationService) LastLSN(_ context.Context, _ *LastLSNRequest) (*LastLSNResponse, error) {
	withLastLSN, ok := service.logStore.(interface{ LastLSN() uint64 })
	if !ok {
		err := "log store does not expose last lsn"
		service.auditFailure("admin.replication_last_lsn", err)
		return nil, status.Error(codes.FailedPrecondition, err)
	}

	lsn := withLastLSN.LastLSN()
	service.auditSuccess("admin.replication_last_lsn", slog.Uint64("lsn", lsn))
	return &LastLSNResponse{LSN: lsn}, nil
}

func (service *replicationService) auditSuccess(operation string, attrs ...slog.Attr) {
	if service.logger == nil {
		return
	}

	args := []any{
		slog.String("event", "audit"),
		slog.String("status", "success"),
		slog.String("operation", operation),
	}
	for _, attr := range attrs {
		args = append(args, attr)
	}

	service.logger.Info("audit_event", args...)
}

func (service *replicationService) auditFailure(operation, reason string, attrs ...slog.Attr) {
	if service.logger == nil {
		return
	}

	args := []any{
		slog.String("event", "audit"),
		slog.String("status", "failure"),
		slog.String("operation", operation),
		slog.String("reason", reason),
	}
	for _, attr := range attrs {
		args = append(args, attr)
	}

	service.logger.Warn("audit_event", args...)
}
