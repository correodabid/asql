package pgwire

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

type frontendEncodable interface {
	Encode(dst []byte) ([]byte, error)
}

type rawProtoClient struct {
	t        *testing.T
	conn     net.Conn
	frontend *pgproto3.Frontend
	processID uint32
	secretKey uint32
}

func startConformanceServer(t *testing.T) (string, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	server, err := New(Config{
		Address:     "127.0.0.1:0",
		DataDirPath: filepath.Join(t.TempDir(), "data"),
		Logger:      logger,
	})
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

func newRawProtoClient(t *testing.T, addr string) *rawProtoClient {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial pgwire: %v", err)
	}

	client := &rawProtoClient{
		t:        t,
		conn:     conn,
		frontend: pgproto3.NewFrontend(bufio.NewReader(conn), conn),
	}

	startup := &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters: map[string]string{
			"user":     "asql",
			"database": "asql",
		},
	}
	client.send(startup)
	startupMessages := client.receiveUntilReady()
	for _, raw := range startupMessages {
		if key, ok := raw.(*pgproto3.BackendKeyData); ok {
			client.processID = key.ProcessID
			client.secretKey = key.SecretKey
		}
	}
	return client
}

func sendCancelRequest(t *testing.T, addr string, processID, secretKey uint32) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial cancel request: %v", err)
	}
	defer conn.Close()
	payload := make([]byte, 16)
	binary.BigEndian.PutUint32(payload[0:4], 16)
	binary.BigEndian.PutUint32(payload[4:8], 80877102)
	binary.BigEndian.PutUint32(payload[8:12], processID)
	binary.BigEndian.PutUint32(payload[12:16], secretKey)
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write cancel request: %v", err)
	}
}

func (client *rawProtoClient) close() {
	client.t.Helper()
	if client == nil || client.conn == nil {
		return
	}
	client.send(&pgproto3.Terminate{})
	_ = client.conn.Close()
}

func (client *rawProtoClient) send(msgs ...frontendEncodable) {
	client.t.Helper()
	for _, msg := range msgs {
		payload, err := msg.Encode(nil)
		if err != nil {
			client.t.Fatalf("encode frontend message %T: %v", msg, err)
		}
		if _, err := client.conn.Write(payload); err != nil {
			client.t.Fatalf("write frontend message %T: %v", msg, err)
		}
	}
}

func (client *rawProtoClient) receiveUntilReady() []any {
	client.t.Helper()
	messages := make([]any, 0, 8)
	for {
		messages = append(messages, client.receive())
		if _, ok := messages[len(messages)-1].(*pgproto3.ReadyForQuery); ok {
			return messages
		}
	}
}

func (client *rawProtoClient) receive() any {
	client.t.Helper()
	msg, err := client.frontend.Receive()
	if err != nil {
		client.t.Fatalf("receive backend message: %v", err)
	}
	return cloneBackendMessage(msg)
	}

func cloneBackendMessage(msg any) any {
	switch typed := msg.(type) {
	case *pgproto3.DataRow:
		values := make([][]byte, len(typed.Values))
		for i, value := range typed.Values {
			if value == nil {
				continue
			}
			values[i] = append([]byte(nil), value...)
		}
		return &pgproto3.DataRow{Values: values}
	case *pgproto3.RowDescription:
		fields := make([]pgproto3.FieldDescription, len(typed.Fields))
		for i, field := range typed.Fields {
			fields[i] = field
			fields[i].Name = append([]byte(nil), field.Name...)
		}
		return &pgproto3.RowDescription{Fields: fields}
	case *pgproto3.ParameterDescription:
		return &pgproto3.ParameterDescription{ParameterOIDs: append([]uint32(nil), typed.ParameterOIDs...)}
	case *pgproto3.CommandComplete:
		return &pgproto3.CommandComplete{CommandTag: append([]byte(nil), typed.CommandTag...)}
	case *pgproto3.BackendKeyData:
		return &pgproto3.BackendKeyData{ProcessID: typed.ProcessID, SecretKey: typed.SecretKey}
	case *pgproto3.CopyData:
		return &pgproto3.CopyData{Data: append([]byte(nil), typed.Data...)}
	case *pgproto3.CopyInResponse:
		return &pgproto3.CopyInResponse{OverallFormat: typed.OverallFormat, ColumnFormatCodes: append([]uint16(nil), typed.ColumnFormatCodes...)}
	case *pgproto3.CopyOutResponse:
		return &pgproto3.CopyOutResponse{OverallFormat: typed.OverallFormat, ColumnFormatCodes: append([]uint16(nil), typed.ColumnFormatCodes...)}
	default:
		return msg
	}
}

func (client *rawProtoClient) simpleQuery(sql string) []any {
	client.t.Helper()
	client.send(&pgproto3.Query{String: sql})
	return client.receiveUntilReady()
}

func TestExtendedQueryPortalResumesAcrossExecuteCalls(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT, email TEXT)")
	client.simpleQuery("INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')")
	client.simpleQuery("INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')")
	client.simpleQuery("INSERT INTO users (id, email) VALUES (3, 'three@asql.dev')")
	client.simpleQuery("COMMIT")

	client.send(
		&pgproto3.Parse{Name: "sel_users", Query: "SELECT id, email FROM accounts.users ORDER BY id ASC"},
		&pgproto3.Describe{ObjectType: 'S', Name: "sel_users"},
		&pgproto3.Bind{DestinationPortal: "portal_users", PreparedStatement: "sel_users"},
		&pgproto3.Describe{ObjectType: 'P', Name: "portal_users"},
		&pgproto3.Execute{Portal: "portal_users", MaxRows: 1},
		&pgproto3.Execute{Portal: "portal_users", MaxRows: 1},
		&pgproto3.Execute{Portal: "portal_users", MaxRows: 0},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	var (
		parseCompleteCount  int
		bindCompleteCount   int
		parameterDescCount  int
		rowDescriptionCount int
		portalSuspended     int
		commandComplete     int
		gotIDs              []string
	)
	for _, raw := range messages {
		switch msg := raw.(type) {
		case *pgproto3.ParseComplete:
			parseCompleteCount++
		case *pgproto3.BindComplete:
			bindCompleteCount++
		case *pgproto3.ParameterDescription:
			parameterDescCount++
		case *pgproto3.RowDescription:
			rowDescriptionCount++
			if len(msg.Fields) != 2 {
				t.Fatalf("unexpected row description field count: got %d want 2", len(msg.Fields))
			}
		case *pgproto3.DataRow:
			if len(msg.Values) != 2 {
				t.Fatalf("unexpected data row field count: got %d want 2", len(msg.Values))
			}
			gotIDs = append(gotIDs, string(msg.Values[0]))
		case *pgproto3.PortalSuspended:
			portalSuspended++
		case *pgproto3.CommandComplete:
			commandComplete++
		}
	}

	if parseCompleteCount != 1 {
		t.Fatalf("unexpected parse complete count: got %d want 1", parseCompleteCount)
	}
	if bindCompleteCount != 1 {
		t.Fatalf("unexpected bind complete count: got %d want 1", bindCompleteCount)
	}
	if parameterDescCount != 1 {
		t.Fatalf("unexpected parameter description count: got %d want 1", parameterDescCount)
	}
	if rowDescriptionCount != 2 {
		t.Fatalf("unexpected row description count: got %d want 2", rowDescriptionCount)
	}
	if portalSuspended != 2 {
		t.Fatalf("unexpected portal suspended count: got %d want 2", portalSuspended)
	}
	if commandComplete != 1 {
		t.Fatalf("unexpected command complete count: got %d want 1", commandComplete)
	}

	wantIDs := []string{"1", "2", "3"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("unexpected resumed row sequence: got %v want %v", gotIDs, wantIDs)
	}
}

func TestExtendedQueryDiscardsMessagesUntilSyncAfterError(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.send(
		&pgproto3.Parse{Name: "bad_stmt", Query: "SELECT $1"},
		&pgproto3.Sync{},
	)
	client.receiveUntilReady()

	client.send(
		&pgproto3.Bind{
			DestinationPortal:    "bad_portal",
			PreparedStatement:    "bad_stmt",
			ParameterFormatCodes: []int16{1},
			Parameters:           [][]byte{{0x00, 0x01}},
		},
		&pgproto3.Describe{ObjectType: 'P', Name: "bad_portal"},
		&pgproto3.Execute{Portal: "bad_portal", MaxRows: 0},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	var errorCount, readyCount, commandCount, rowDescCount, portalSuspendedCount int
	for _, raw := range messages {
		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			errorCount++
			if msg.Code != "22P02" {
				t.Fatalf("unexpected SQLSTATE: got %s want 22P02", msg.Code)
			}
		case *pgproto3.ReadyForQuery:
			readyCount++
		case *pgproto3.CommandComplete:
			commandCount++
		case *pgproto3.RowDescription:
			rowDescCount++
		case *pgproto3.PortalSuspended:
			portalSuspendedCount++
		}
	}

	if errorCount != 1 {
		t.Fatalf("unexpected error count after pipeline recovery: got %d want 1", errorCount)
	}
	if readyCount != 1 {
		t.Fatalf("unexpected ready count after sync: got %d want 1", readyCount)
	}
	if commandCount != 0 || rowDescCount != 0 || portalSuspendedCount != 0 {
		t.Fatalf("expected messages after error to be discarded until sync, got command=%d rowdesc=%d suspended=%d", commandCount, rowDescCount, portalSuspendedCount)
	}
}

func TestCancelRequestCancelsSimpleQueryAndKeepsConnectionUsable(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	if client.processID == 0 || client.secretKey == 0 {
		t.Fatalf("expected backend key data from startup, got pid=%d secret=%d", client.processID, client.secretKey)
	}

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT, email TEXT)")
	for i := 1; i <= 3; i++ {
		client.simpleQuery("INSERT INTO users (id, email) VALUES (" + string(rune('0'+i)) + ", 'u" + string(rune('0'+i)) + "@asql.dev')")
	}
	client.simpleQuery("COMMIT")

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	pgwireStreamHook = func() {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
	}
	defer func() { pgwireStreamHook = nil }()

	client.send(&pgproto3.Query{String: "SELECT id, email FROM accounts.users ORDER BY id ASC"})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for row streaming hook")
	}

	sendCancelRequest(t, addr, client.processID, client.secretKey)
	time.Sleep(100 * time.Millisecond)
	close(release)

	messages := client.receiveUntilReady()
	var canceled bool
	for _, raw := range messages {
		if errMsg, ok := raw.(*pgproto3.ErrorResponse); ok {
			if errMsg.Code == "57014" {
				canceled = true
			}
		}
	}
	if !canceled {
		t.Fatalf("expected query canceled error, got %#v", messages)
	}

	postCancel := client.simpleQuery("SHOW server_version")
	var sawCommandComplete bool
	for _, raw := range postCancel {
		if _, ok := raw.(*pgproto3.CommandComplete); ok {
			sawCommandComplete = true
		}
	}
	if !sawCommandComplete {
		t.Fatalf("expected connection to remain usable after cancel, got %#v", postCancel)
	}
}

func TestExtendedQueryDescribeStatementInfersParameterCount(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.send(
		&pgproto3.Parse{Name: "param_stmt", Query: "SELECT * FROM asql_admin.engine_stats WHERE total_commits >= $1 AND total_reads >= $2"},
		&pgproto3.Describe{ObjectType: 'S', Name: "param_stmt"},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	found := false
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.ParameterDescription); ok {
			found = true
			if len(msg.ParameterOIDs) != 2 {
				t.Fatalf("unexpected inferred parameter count: got %d want 2", len(msg.ParameterOIDs))
			}
		}
	}
	if !found {
		t.Fatal("expected parameter description in describe-statement response")
	}
}

func TestCopyFromStdinInsertsRowsAndAcceptsChunkedCopyData(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT, email TEXT)")
	client.simpleQuery("COMMIT")

	client.send(&pgproto3.Query{String: "COPY accounts.users (id, email) FROM STDIN"})
	if _, ok := client.receive().(*pgproto3.CopyInResponse); !ok {
		t.Fatal("expected CopyInResponse")
	}

	client.send(
		&pgproto3.CopyData{Data: []byte("1\tone")},
		&pgproto3.CopyData{Data: []byte("@asql.dev\n2\ttwo@asql.dev\n")},
		&pgproto3.CopyDone{},
	)
	messages := client.receiveUntilReady()

	var commandTag string
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.CommandComplete); ok {
			commandTag = string(msg.CommandTag)
		}
	}
	if commandTag != "COPY 2" {
		t.Fatalf("unexpected COPY command tag: got %q want %q", commandTag, "COPY 2")
	}

	rows := client.simpleQuery("SELECT id, email FROM accounts.users ORDER BY id ASC")
	var got [][]string
	for _, raw := range rows {
		if msg, ok := raw.(*pgproto3.DataRow); ok {
			got = append(got, []string{string(msg.Values[0]), string(msg.Values[1])})
		}
	}
	if !reflect.DeepEqual(got, [][]string{{"1", "one@asql.dev"}, {"2", "two@asql.dev"}}) {
		t.Fatalf("unexpected rows after COPY FROM: got %v", got)
	}
}

func TestCopyToStdoutStreamsRows(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT, email TEXT)")
	client.simpleQuery("INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')")
	client.simpleQuery("INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')")
	client.simpleQuery("COMMIT")

	client.send(&pgproto3.Query{String: "COPY accounts.users (id, email) TO STDOUT"})
	if _, ok := client.receive().(*pgproto3.CopyOutResponse); !ok {
		t.Fatal("expected CopyOutResponse")
	}
	messages := client.receiveUntilReady()

	var (
		copyRows   []string
		commandTag string
	)
	for _, raw := range messages {
		switch msg := raw.(type) {
		case *pgproto3.CopyData:
			copyRows = append(copyRows, string(msg.Data))
		case *pgproto3.CommandComplete:
			commandTag = string(msg.CommandTag)
		}
	}
	sort.Strings(copyRows)
	wantRows := []string{"1\tone@asql.dev\n", "2\ttwo@asql.dev\n"}
	sort.Strings(wantRows)
	if !reflect.DeepEqual(copyRows, wantRows) {
		t.Fatalf("unexpected COPY TO rows: got %v", copyRows)
	}
	if commandTag != "COPY 2" {
		t.Fatalf("unexpected COPY TO command tag: got %q want %q", commandTag, "COPY 2")
	}
}

func TestCopyFailRollsBackInsertedRows(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT, email TEXT)")
	client.simpleQuery("COMMIT")

	client.send(&pgproto3.Query{String: "COPY accounts.users (id, email) FROM STDIN"})
	if _, ok := client.receive().(*pgproto3.CopyInResponse); !ok {
		t.Fatal("expected CopyInResponse")
	}

	client.send(
		&pgproto3.CopyData{Data: []byte("1\tone@asql.dev\n")},
		&pgproto3.CopyFail{Message: "abort copy"},
	)
	messages := client.receiveUntilReady()

	var sawAbort bool
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.ErrorResponse); ok && msg.Code == "57014" {
			sawAbort = true
		}
	}
	if !sawAbort {
		t.Fatalf("expected copy abort error, got %#v", messages)
	}

	rows := client.simpleQuery("SELECT id, email FROM accounts.users ORDER BY id ASC")
	for _, raw := range rows {
		if _, ok := raw.(*pgproto3.DataRow); ok {
			t.Fatalf("expected COPY rollback to leave table empty, got %#v", rows)
		}
	}
}
