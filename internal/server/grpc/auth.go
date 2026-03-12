package grpc

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func unaryAuthInterceptor(token string) grpc.UnaryServerInterceptor {
	if token == "" {
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}

	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := validateAuthorizationHeader(ctx, token); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func streamAuthInterceptor(token string) grpc.StreamServerInterceptor {
	if token == "" {
		return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, stream)
		}
	}

	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := validateAuthorizationHeader(stream.Context(), token); err != nil {
			return err
		}

		return handler(srv, stream)
	}
}

func validateAuthorizationHeader(ctx context.Context, expectedToken string) error {
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "authorization metadata is required")
	}

	values := meta.Get("authorization")
	if len(values) == 0 {
		return status.Error(codes.Unauthenticated, "authorization header is required")
	}

	auth := strings.TrimSpace(values[0])
	const bearerPrefix = "Bearer "
	if !strings.HasPrefix(auth, bearerPrefix) {
		return status.Error(codes.Unauthenticated, "authorization header must use Bearer token")
	}

	receivedToken := strings.TrimSpace(strings.TrimPrefix(auth, bearerPrefix))
	if receivedToken != expectedToken {
		return status.Error(codes.Unauthenticated, "invalid bearer token")
	}

	return nil
}
