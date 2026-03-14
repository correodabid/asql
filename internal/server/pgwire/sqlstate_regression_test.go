package pgwire

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

type startupFrontendEncodable interface {
	Encode(dst []byte) ([]byte, error)
}

func startSQLStateRegressionServer(t *testing.T, config Config) (string, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	server, err := New(config)
	if err != nil {
		t.Fatalf("new pgwire server: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for test: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeOnListener(ctx, listener)
	}()

	cleanup := func() {
		cancel()
		server.Stop()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("pgwire server exited with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for pgwire server shutdown")
		}
	}

	return listener.Addr().String(), cleanup
}

func dialRawStartupClient(t *testing.T, addr string) (net.Conn, *pgproto3.Frontend) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial pgwire: %v", err)
	}
	frontend := pgproto3.NewFrontend(bufio.NewReader(conn), conn)
	return conn, frontend
}

func sendStartupFrontendMessages(t *testing.T, conn net.Conn, msgs ...startupFrontendEncodable) {
	t.Helper()
	for _, msg := range msgs {
		payload, err := msg.Encode(nil)
		if err != nil {
			t.Fatalf("encode frontend message %T: %v", msg, err)
		}
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write frontend message %T: %v", msg, err)
		}
	}
}

func receiveBackendMessageOfType[T any](t *testing.T, frontend *pgproto3.Frontend) T {
	t.Helper()
	msg, err := frontend.Receive()
	if err != nil {
		t.Fatalf("receive backend message: %v", err)
	}
	typed, ok := msg.(T)
	if !ok {
		t.Fatalf("unexpected backend message %T", msg)
	}
	return typed
}

func TestSendFollowerRedirectErrorWrites25006AndHint(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	backend := pgproto3.NewBackend(bufio.NewReader(serverConn), serverConn)
	frontend := pgproto3.NewFrontend(bufio.NewReader(clientConn), clientConn)

	errCh := make(chan error, 1)
	go func() {
		errCh <- sendFollowerRedirectError(backend, "127.0.0.1:5434", nil)
	}()

	errMsg := receiveBackendMessageOfType[*pgproto3.ErrorResponse](t, frontend)
	if errMsg.Code != "25006" {
		t.Fatalf("unexpected SQLSTATE: got %s want 25006", errMsg.Code)
	}
	if errMsg.Hint != "asql_leader=127.0.0.1:5434" {
		t.Fatalf("unexpected redirect hint: got %q", errMsg.Hint)
	}
	if errMsg.Message != "not the leader: redirect writes to 127.0.0.1:5434" {
		t.Fatalf("unexpected redirect message: got %q", errMsg.Message)
	}

	ready := receiveBackendMessageOfType[*pgproto3.ReadyForQuery](t, frontend)
	if ready.TxStatus != 'I' {
		t.Fatalf("unexpected tx status: got %q want %q", ready.TxStatus, byte('I'))
	}

	if err := <-errCh; err != nil {
		t.Fatalf("sendFollowerRedirectError returned error: %v", err)
	}
}

func TestPGWirePasswordAuthenticationWrongPasswordReturns28P01(t *testing.T) {
	addr, cleanup := startSQLStateRegressionServer(t, Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		AuthToken:   "secret-token",
	})
	defer cleanup()

	conn, frontend := dialRawStartupClient(t, addr)
	defer conn.Close()

	sendStartupFrontendMessages(t, conn, &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"user":     "asql",
			"database": "asql",
		},
	})

	receiveBackendMessageOfType[*pgproto3.AuthenticationCleartextPassword](t, frontend)
	sendStartupFrontendMessages(t, conn, &pgproto3.PasswordMessage{Password: "wrong-token"})

	errMsg := receiveBackendMessageOfType[*pgproto3.ErrorResponse](t, frontend)
	if errMsg.Code != "28P01" {
		t.Fatalf("unexpected SQLSTATE: got %s want 28P01", errMsg.Code)
	}
	if errMsg.Severity != "FATAL" {
		t.Fatalf("unexpected severity: got %q want FATAL", errMsg.Severity)
	}
}

func TestPGWirePasswordAuthenticationWrongMessageReturns08P01(t *testing.T) {
	addr, cleanup := startSQLStateRegressionServer(t, Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		AuthToken:   "secret-token",
	})
	defer cleanup()

	conn, frontend := dialRawStartupClient(t, addr)
	defer conn.Close()

	sendStartupFrontendMessages(t, conn, &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"user":     "asql",
			"database": "asql",
		},
	})

	receiveBackendMessageOfType[*pgproto3.AuthenticationCleartextPassword](t, frontend)
	sendStartupFrontendMessages(t, conn, &pgproto3.Query{String: "SHOW server_version"})

	errMsg := receiveBackendMessageOfType[*pgproto3.ErrorResponse](t, frontend)
	if errMsg.Code != "08P01" {
		t.Fatalf("unexpected SQLSTATE: got %s want 08P01", errMsg.Code)
	}
	if errMsg.Severity != "FATAL" {
		t.Fatalf("unexpected severity: got %q want FATAL", errMsg.Severity)
	}
}
