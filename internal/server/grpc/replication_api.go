package grpc

import (
	"context"

	grpcgo "google.golang.org/grpc"
)

type StreamWALRequest struct {
	FromLSN uint64 `json:"from_lsn"`
	Limit   uint32 `json:"limit,omitempty"`
}

type StreamWALResponse struct {
	LSN       uint64 `json:"lsn"`
	Term      uint64 `json:"term,omitempty"`
	TxID      string `json:"tx_id"`
	Type      string `json:"type"`
	Timestamp uint64 `json:"timestamp"`
	Payload   []byte `json:"payload,omitempty"`
}

type LastLSNRequest struct{}

type LastLSNResponse struct {
	LSN uint64 `json:"lsn"`
}

type ReplicationServiceServer interface {
	StreamWAL(*StreamWALRequest, ReplicationService_StreamWALServer) error
	LastLSN(context.Context, *LastLSNRequest) (*LastLSNResponse, error)
}

type ReplicationService_StreamWALServer interface {
	Send(*StreamWALResponse) error
	grpcgo.ServerStream
}

func registerReplicationServiceServer(server *grpcgo.Server, implementation ReplicationServiceServer) {
	server.RegisterService(&grpcgo.ServiceDesc{
		ServiceName: "asql.v1.ReplicationService",
		HandlerType: (*ReplicationServiceServer)(nil),
		Methods: []grpcgo.MethodDesc{
			{MethodName: "LastLSN", Handler: lastLSNHandler},
		},
		Streams: []grpcgo.StreamDesc{
			{
				StreamName:    "StreamWAL",
				Handler:       streamWALHandler,
				ServerStreams: true,
			},
		},
	}, implementation)
}

func streamWALHandler(service interface{}, stream grpcgo.ServerStream) error {
	request := new(StreamWALRequest)
	if err := stream.RecvMsg(request); err != nil {
		return err
	}

	return service.(ReplicationServiceServer).StreamWAL(request, &replicationServiceStreamWALServer{ServerStream: stream})
}

type replicationServiceStreamWALServer struct {
	grpcgo.ServerStream
}

func (server *replicationServiceStreamWALServer) Send(response *StreamWALResponse) error {
	return server.SendMsg(response)
}

func lastLSNHandler(service interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpcgo.UnaryServerInterceptor) (interface{}, error) {
	request := new(LastLSNRequest)
	if err := decode(request); err != nil {
		return nil, err
	}

	if interceptor == nil {
		return service.(ReplicationServiceServer).LastLSN(ctx, request)
	}

	info := &grpcgo.UnaryServerInfo{Server: service, FullMethod: "/asql.v1.ReplicationService/LastLSN"}
	handler := func(innerCtx context.Context, innerReq interface{}) (interface{}, error) {
		return service.(ReplicationServiceServer).LastLSN(innerCtx, innerReq.(*LastLSNRequest))
	}

	return interceptor(ctx, request, info, handler)
}
