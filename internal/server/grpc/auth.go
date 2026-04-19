package grpc

import (
	"context"
	"strings"

	"github.com/correodabid/asql/internal/engine/executor"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	principalMetadataKey = "asql-principal"
	passwordMetadataKey  = "asql-password"
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

func authenticatePrincipalFromMetadata(ctx context.Context, engine *executor.Engine) (string, error) {
	if engine == nil || !engine.HasPrincipalCatalog() {
		return "", nil
	}

	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Errorf(codes.Unauthenticated, "%s metadata is required", principalMetadataKey)
	}

	principal := strings.TrimSpace(firstMetadataValue(meta, principalMetadataKey))
	if principal == "" {
		return "", status.Errorf(codes.Unauthenticated, "%s metadata is required", principalMetadataKey)
	}

	password := strings.TrimSpace(firstMetadataValue(meta, passwordMetadataKey))
	if password == "" {
		return "", status.Errorf(codes.Unauthenticated, "%s metadata is required", passwordMetadataKey)
	}

	info, err := engine.AuthenticatePrincipal(principal, password)
	if err != nil {
		return "", status.Error(codes.Unauthenticated, err.Error())
	}

	return info.Name, nil
}

func firstMetadataValue(meta metadata.MD, key string) string {
	values := meta.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
