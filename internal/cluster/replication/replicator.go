package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"asql/internal/engine/ports"
	grpcapi "asql/internal/server/grpc"
	"asql/internal/storage/wal"

	grpcgo "google.golang.org/grpc"
)

// CatchUpFromGRPC pulls WAL records from leader and applies them in strict LSN order.
func CatchUpFromGRPC(ctx context.Context, connection *grpcgo.ClientConn, follower *wal.SegmentedLogStore, batchSize uint32) error {
	if batchSize == 0 {
		batchSize = 128
	}

	for {
		fromLSN := follower.LastLSN() + 1
		records, err := fetchWALBatch(ctx, connection, fromLSN, batchSize)
		if err != nil {
			return err
		}

		if len(records) == 0 {
			return nil
		}

		if err := applyBatch(ctx, follower, records); err != nil {
			return err
		}

		if uint32(len(records)) < batchSize {
			return nil
		}
	}
}

func fetchWALBatch(ctx context.Context, connection *grpcgo.ClientConn, fromLSN uint64, limit uint32) ([]grpcapi.StreamWALResponse, error) {
	stream, err := connection.NewStream(
		ctx,
		&grpcgo.StreamDesc{ServerStreams: true},
		"/asql.v1.ReplicationService/StreamWAL",
		grpcgo.ForceCodec(jsonCodec{}),
	)
	if err != nil {
		return nil, fmt.Errorf("open replication stream: %w", err)
	}

	if err := stream.SendMsg(&grpcapi.StreamWALRequest{FromLSN: fromLSN, Limit: limit}); err != nil {
		return nil, fmt.Errorf("send replication request: %w", err)
	}

	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("close replication stream send: %w", err)
	}

	responses := make([]grpcapi.StreamWALResponse, 0)
	for {
		response := grpcapi.StreamWALResponse{}
		err := stream.RecvMsg(&response)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("receive replication record: %w", err)
		}

		responses = append(responses, response)
	}

	return responses, nil
}

func applyBatch(ctx context.Context, follower *wal.SegmentedLogStore, records []grpcapi.StreamWALResponse) error {
	for _, record := range records {
		expected := follower.LastLSN() + 1
		if record.LSN != expected {
			return fmt.Errorf("replication divergence detected: got lsn=%d expected=%d", record.LSN, expected)
		}

		if err := follower.AppendReplicated(ctx, ports.WALRecord{
			LSN:       record.LSN,
			Term:      record.Term,
			TxID:      record.TxID,
			Type:      record.Type,
			Timestamp: record.Timestamp,
			Payload:   record.Payload,
		}); err != nil {
			return fmt.Errorf("apply replicated record lsn=%d: %w", record.LSN, err)
		}
	}

	return nil
}

type jsonCodec struct{}

func (jsonCodec) Name() string {
	return "json"
}

func (jsonCodec) Marshal(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (jsonCodec) Unmarshal(data []byte, value interface{}) error {
	return json.Unmarshal(data, value)
}

type LagStatus struct {
	LeaderLSN   uint64
	FollowerLSN uint64
	Lag         uint64
}

func LeaderLastLSNFromGRPC(ctx context.Context, connection *grpcgo.ClientConn) (uint64, error) {
	response := new(grpcapi.LastLSNResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ReplicationService/LastLSN", &grpcapi.LastLSNRequest{}, response, grpcgo.ForceCodec(jsonCodec{})); err != nil {
		return 0, fmt.Errorf("get leader last lsn: %w", err)
	}

	return response.LSN, nil
}

func LagStatusFromGRPC(ctx context.Context, connection *grpcgo.ClientConn, follower *wal.SegmentedLogStore) (LagStatus, error) {
	if follower == nil {
		return LagStatus{}, fmt.Errorf("follower wal store is required")
	}

	leaderLSN, err := LeaderLastLSNFromGRPC(ctx, connection)
	if err != nil {
		return LagStatus{}, err
	}

	followerLSN := follower.LastLSN()
	lag := uint64(0)
	if leaderLSN > followerLSN {
		lag = leaderLSN - followerLSN
	}

	return LagStatus{LeaderLSN: leaderLSN, FollowerLSN: followerLSN, Lag: lag}, nil
}
