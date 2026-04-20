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
	t         *testing.T
	conn      net.Conn
	frontend  *pgproto3.Frontend
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
			client.secretKey = secretKeyToUint32(key.SecretKey)
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

func TestExtendedQueryTailEntityChangesFollowResumesAcrossExecuteCalls(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	writer := newRawProtoClient(t, addr)
	defer writer.close()

	for _, sql := range []string{
		"BEGIN DOMAIN test",
		"CREATE TABLE items (id INT PRIMARY KEY, status TEXT)",
		"CREATE TABLE item_steps (id INT PRIMARY KEY, item_id INT REFERENCES items(id), label TEXT)",
		"CREATE ENTITY item_aggregate (ROOT items, INCLUDES item_steps)",
		"COMMIT",
		"BEGIN DOMAIN test",
		"INSERT INTO items (id, status) VALUES (1, 'draft')",
		"COMMIT",
	} {
		writer.simpleQuery(sql)
	}

	client.send(
		&pgproto3.Parse{Name: "tail_follow", Query: "TAIL ENTITY CHANGES test.item_aggregate FOR '1' LIMIT 2 FOLLOW"},
		&pgproto3.Bind{DestinationPortal: "tail_follow_portal", PreparedStatement: "tail_follow"},
		&pgproto3.Describe{ObjectType: 'P', Name: "tail_follow_portal"},
		&pgproto3.Execute{Portal: "tail_follow_portal", MaxRows: 1},
		&pgproto3.Sync{},
	)
	firstMessages := client.receiveUntilReady()

	var firstRows [][]string
	var firstSuspended, firstComplete int
	for _, raw := range firstMessages {
		switch msg := raw.(type) {
		case *pgproto3.DataRow:
			values := make([]string, len(msg.Values))
			for i, value := range msg.Values {
				values[i] = string(value)
			}
			firstRows = append(firstRows, values)
		case *pgproto3.PortalSuspended:
			firstSuspended++
		case *pgproto3.CommandComplete:
			firstComplete++
		}
	}
	if len(firstRows) != 1 {
		t.Fatalf("expected first execute to return one row, got %#v", firstRows)
	}
	if firstRows[0][5] != "1" || firstRows[0][6] != `["items"]` {
		t.Fatalf("unexpected first execute row: %#v", firstRows[0])
	}
	if firstSuspended != 1 || firstComplete != 0 {
		t.Fatalf("unexpected first execute status: suspended=%d complete=%d", firstSuspended, firstComplete)
	}

	for _, sql := range []string{
		"BEGIN DOMAIN test",
		"INSERT INTO item_steps (id, item_id, label) VALUES (10, 1, 'mix')",
		"COMMIT",
	} {
		writer.simpleQuery(sql)
	}

	client.send(
		&pgproto3.Execute{Portal: "tail_follow_portal", MaxRows: 1},
		&pgproto3.Sync{},
	)
	secondMessages := client.receiveUntilReady()

	var secondRows [][]string
	var secondSuspended, secondComplete int
	for _, raw := range secondMessages {
		switch msg := raw.(type) {
		case *pgproto3.DataRow:
			values := make([]string, len(msg.Values))
			for i, value := range msg.Values {
				values[i] = string(value)
			}
			secondRows = append(secondRows, values)
		case *pgproto3.PortalSuspended:
			secondSuspended++
		case *pgproto3.CommandComplete:
			secondComplete++
		}
	}
	if len(secondRows) != 1 {
		t.Fatalf("expected second execute to return one row, got %#v", secondRows)
	}
	if secondRows[0][5] != "2" || secondRows[0][6] != `["item_steps"]` {
		t.Fatalf("unexpected second execute row: %#v", secondRows[0])
	}
	if secondSuspended != 0 || secondComplete != 1 {
		t.Fatalf("unexpected second execute status: suspended=%d complete=%d", secondSuspended, secondComplete)
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

func TestExtendedQueryDescribeStatementInfersInsertParameterOIDs(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT PRIMARY KEY, active BOOL, email TEXT)")
	client.simpleQuery("COMMIT")

	client.send(
		&pgproto3.Parse{Name: "insert_stmt", Query: "INSERT INTO accounts.users (id, active, email) VALUES ($1, $2, $3)"},
		&pgproto3.Describe{ObjectType: 'S', Name: "insert_stmt"},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	found := false
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.ParameterDescription); ok {
			found = true
			want := []uint32{20, 16, 25}
			if !reflect.DeepEqual(msg.ParameterOIDs, want) {
				t.Fatalf("unexpected parameter OIDs: got %v want %v", msg.ParameterOIDs, want)
			}
		}
	}
	if !found {
		t.Fatal("expected parameter description in describe-statement response")
	}
}

func TestExtendedQueryDescribeStatementInfersPredicateParameterOIDs(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT PRIMARY KEY, active BOOL, email TEXT)")
	client.simpleQuery("COMMIT")

	client.send(
		&pgproto3.Parse{Name: "select_stmt", Query: "SELECT id, email FROM accounts.users WHERE id >= $1 AND active = $2 ORDER BY id ASC"},
		&pgproto3.Describe{ObjectType: 'S', Name: "select_stmt"},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	found := false
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.ParameterDescription); ok {
			found = true
			want := []uint32{20, 16}
			if !reflect.DeepEqual(msg.ParameterOIDs, want) {
				t.Fatalf("unexpected predicate parameter OIDs: got %v want %v", msg.ParameterOIDs, want)
			}
		}
	}
	if !found {
		t.Fatal("expected parameter description in describe-statement response")
	}
}

func TestExtendedQueryDescribeStatementInfersUpdateParameterOIDs(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT PRIMARY KEY, active BOOL, email TEXT)")
	client.simpleQuery("COMMIT")

	client.send(
		&pgproto3.Parse{Name: "update_stmt", Query: "UPDATE accounts.users SET active = $1, email = $2 WHERE id = $3"},
		&pgproto3.Describe{ObjectType: 'S', Name: "update_stmt"},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	found := false
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.ParameterDescription); ok {
			found = true
			want := []uint32{16, 25, 20}
			if !reflect.DeepEqual(msg.ParameterOIDs, want) {
				t.Fatalf("unexpected update parameter OIDs: got %v want %v", msg.ParameterOIDs, want)
			}
		}
	}
	if !found {
		t.Fatal("expected parameter description in describe-statement response")
	}
}

func TestExtendedQueryDescribeStatementInfersArithmeticUpdateParameterOIDs(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE inventory (id INT PRIMARY KEY, stock INT, active BOOL)")
	client.simpleQuery("COMMIT")

	client.send(
		&pgproto3.Parse{Name: "arith_update_stmt", Query: "UPDATE accounts.inventory SET stock = stock - $1 WHERE id = $2"},
		&pgproto3.Describe{ObjectType: 'S', Name: "arith_update_stmt"},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	found := false
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.ParameterDescription); ok {
			found = true
			want := []uint32{20, 20}
			if !reflect.DeepEqual(msg.ParameterOIDs, want) {
				t.Fatalf("unexpected arithmetic update parameter OIDs: got %v want %v", msg.ParameterOIDs, want)
			}
		}
	}
	if !found {
		t.Fatal("expected parameter description in describe-statement response")
	}
}

func TestExtendedQueryInsertReturningUsesSchemaAwareRowDescription(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT PRIMARY KEY, active BOOL, email TEXT)")
	client.simpleQuery("COMMIT")

	client.send(
		&pgproto3.Parse{Name: "ins_ret", Query: "INSERT INTO accounts.users (id, active, email) VALUES ($1, $2, $3) RETURNING id, active, email"},
		&pgproto3.Describe{ObjectType: 'S', Name: "ins_ret"},
		&pgproto3.Bind{
			DestinationPortal: "ins_ret_portal",
			PreparedStatement: "ins_ret",
			Parameters: [][]byte{
				[]byte("7"),
				[]byte("true"),
				[]byte("seven@asql.dev"),
			},
		},
		&pgproto3.Describe{ObjectType: 'P', Name: "ins_ret_portal"},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	var rowDescs []*pgproto3.RowDescription
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.RowDescription); ok {
			rowDescs = append(rowDescs, msg)
		}
	}
	if len(rowDescs) != 2 {
		t.Fatalf("unexpected row description count: got %d want 2", len(rowDescs))
	}

	for i, desc := range rowDescs {
		if len(desc.Fields) != 3 {
			t.Fatalf("row description %d field count: got %d want 3", i, len(desc.Fields))
		}
		if got := string(desc.Fields[0].Name); got != "id" || desc.Fields[0].DataTypeOID != 20 {
			t.Fatalf("row description %d field 0 = (%q,%d), want (id,20)", i, got, desc.Fields[0].DataTypeOID)
		}
		if got := string(desc.Fields[1].Name); got != "active" || desc.Fields[1].DataTypeOID != 16 {
			t.Fatalf("row description %d field 1 = (%q,%d), want (active,16)", i, got, desc.Fields[1].DataTypeOID)
		}
		if got := string(desc.Fields[2].Name); got != "email" || desc.Fields[2].DataTypeOID != 25 {
			t.Fatalf("row description %d field 2 = (%q,%d), want (email,25)", i, got, desc.Fields[2].DataTypeOID)
		}
	}
}

func TestExtendedQueryQualifiedStarUsesSchemaAwareRowDescription(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT PRIMARY KEY, email TEXT)")
	client.simpleQuery("COMMIT")

	client.send(
		&pgproto3.Parse{Name: "sel_star_alias", Query: "SELECT u.* FROM accounts.users u ORDER BY u.id ASC"},
		&pgproto3.Describe{ObjectType: 'S', Name: "sel_star_alias"},
		&pgproto3.Bind{DestinationPortal: "sel_star_alias_portal", PreparedStatement: "sel_star_alias"},
		&pgproto3.Describe{ObjectType: 'P', Name: "sel_star_alias_portal"},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	var rowDescs []*pgproto3.RowDescription
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.RowDescription); ok {
			rowDescs = append(rowDescs, msg)
		}
	}
	if len(rowDescs) != 2 {
		t.Fatalf("unexpected row description count: got %d want 2", len(rowDescs))
	}
	for i, desc := range rowDescs {
		if len(desc.Fields) != 3 {
			t.Fatalf("row description %d field count: got %d want 3", i, len(desc.Fields))
		}
		if got := string(desc.Fields[0].Name); got != "_lsn" || desc.Fields[0].DataTypeOID != 20 {
			t.Fatalf("row description %d field 0 = (%q,%d), want (_lsn,20)", i, got, desc.Fields[0].DataTypeOID)
		}
		if got := string(desc.Fields[1].Name); got != "id" || desc.Fields[1].DataTypeOID != 20 {
			t.Fatalf("row description %d field 1 = (%q,%d), want (id,20)", i, got, desc.Fields[1].DataTypeOID)
		}
		if got := string(desc.Fields[2].Name); got != "email" || desc.Fields[2].DataTypeOID != 25 {
			t.Fatalf("row description %d field 2 = (%q,%d), want (email,25)", i, got, desc.Fields[2].DataTypeOID)
		}
	}
}

func TestExtendedQueryDerivedTableRowDescriptionFollowsExpandedColumns(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT PRIMARY KEY, email TEXT)")
	client.simpleQuery("COMMIT")

	query := "SELECT * FROM (SELECT u.*, ROW_NUMBER() OVER (ORDER BY u.id ASC) AS rn FROM accounts.users u) s WHERE s.rn = 1 ORDER BY s.id ASC"
	client.send(
		&pgproto3.Parse{Name: "derived_star", Query: query},
		&pgproto3.Describe{ObjectType: 'S', Name: "derived_star"},
		&pgproto3.Bind{DestinationPortal: "derived_star_portal", PreparedStatement: "derived_star"},
		&pgproto3.Describe{ObjectType: 'P', Name: "derived_star_portal"},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	var rowDescs []*pgproto3.RowDescription
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.RowDescription); ok {
			rowDescs = append(rowDescs, msg)
		}
	}
	if len(rowDescs) != 2 {
		t.Fatalf("unexpected row description count: got %d want 2", len(rowDescs))
	}
	for i, desc := range rowDescs {
		if len(desc.Fields) != 4 {
			t.Fatalf("row description %d field count: got %d want 4", i, len(desc.Fields))
		}
		if got := string(desc.Fields[0].Name); got != "_lsn" || desc.Fields[0].DataTypeOID != 20 {
			t.Fatalf("row description %d field 0 = (%q,%d), want (_lsn,20)", i, got, desc.Fields[0].DataTypeOID)
		}
		if got := string(desc.Fields[1].Name); got != "id" || desc.Fields[1].DataTypeOID != 20 {
			t.Fatalf("row description %d field 1 = (%q,%d), want (id,20)", i, got, desc.Fields[1].DataTypeOID)
		}
		if got := string(desc.Fields[2].Name); got != "email" || desc.Fields[2].DataTypeOID != 25 {
			t.Fatalf("row description %d field 2 = (%q,%d), want (email,25)", i, got, desc.Fields[2].DataTypeOID)
		}
		if got := string(desc.Fields[3].Name); got != "rn" || desc.Fields[3].DataTypeOID != 20 {
			t.Fatalf("row description %d field 3 = (%q,%d), want (rn,20)", i, got, desc.Fields[3].DataTypeOID)
		}
	}
}

func TestExtendedQueryMultipleDerivedJoinsDescribeAndExecute(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT PRIMARY KEY, email TEXT)")
	client.simpleQuery("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
	client.simpleQuery("CREATE TABLE sizes (id INT PRIMARY KEY, label TEXT)")
	client.simpleQuery("INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')")
	client.simpleQuery("INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')")
	client.simpleQuery("INSERT INTO users (id, email) VALUES (3, 'three@asql.dev')")
	client.simpleQuery("INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100)")
	client.simpleQuery("INSERT INTO orders (id, user_id, amount) VALUES (20, 1, 200)")
	client.simpleQuery("INSERT INTO orders (id, user_id, amount) VALUES (30, 2, 150)")
	client.simpleQuery("INSERT INTO sizes (id, label) VALUES (1, 'S')")
	client.simpleQuery("INSERT INTO sizes (id, label) VALUES (2, 'M')")
	client.simpleQuery("INSERT INTO sizes (id, label) VALUES (3, 'L')")
	client.simpleQuery("COMMIT")

	query := "SELECT users.email AS email, ranked.amount AS amount, allowed.label AS label FROM accounts.users JOIN (SELECT user_id, amount, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn FROM accounts.orders) ranked ON accounts.users.id = ranked.user_id JOIN (SELECT id, label FROM accounts.sizes WHERE id <= 2) allowed ON accounts.users.id = allowed.id WHERE ranked.rn = 1 ORDER BY accounts.users.id ASC"
	client.send(
		&pgproto3.Parse{Name: "multi_derived_join", Query: query},
		&pgproto3.Describe{ObjectType: 'S', Name: "multi_derived_join"},
		&pgproto3.Bind{DestinationPortal: "multi_derived_join_portal", PreparedStatement: "multi_derived_join"},
		&pgproto3.Describe{ObjectType: 'P', Name: "multi_derived_join_portal"},
		&pgproto3.Execute{Portal: "multi_derived_join_portal", MaxRows: 0},
		&pgproto3.Sync{},
	)
	messages := client.receiveUntilReady()

	var (
		rowDescs []*pgproto3.RowDescription
		rows     [][]string
	)
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.RowDescription); ok {
			rowDescs = append(rowDescs, msg)
			continue
		}
		if msg, ok := raw.(*pgproto3.DataRow); ok {
			values := make([]string, len(msg.Values))
			for i, value := range msg.Values {
				values[i] = string(value)
			}
			rows = append(rows, values)
		}
	}

	if len(rowDescs) != 2 {
		t.Fatalf("unexpected row description count: got %d want 2", len(rowDescs))
	}
	for i, desc := range rowDescs {
		if len(desc.Fields) != 3 {
			t.Fatalf("row description %d field count: got %d want 3", i, len(desc.Fields))
		}
		if got := string(desc.Fields[0].Name); got != "email" || desc.Fields[0].DataTypeOID != 25 {
			t.Fatalf("row description %d field 0 = (%q,%d), want (email,25)", i, got, desc.Fields[0].DataTypeOID)
		}
		if got := string(desc.Fields[1].Name); got != "amount" || desc.Fields[1].DataTypeOID != 20 {
			t.Fatalf("row description %d field 1 = (%q,%d), want (amount,20)", i, got, desc.Fields[1].DataTypeOID)
		}
		if got := string(desc.Fields[2].Name); got != "label" || desc.Fields[2].DataTypeOID != 25 {
			t.Fatalf("row description %d field 2 = (%q,%d), want (label,25)", i, got, desc.Fields[2].DataTypeOID)
		}
	}

	wantRows := [][]string{{"one@asql.dev", "200", "S"}, {"two@asql.dev", "150", "M"}}
	if !reflect.DeepEqual(rows, wantRows) {
		t.Fatalf("unexpected rows: got %v want %v", rows, wantRows)
	}
}

func TestExtendedQueryBinaryBindSupportsInt4Int8AndBool(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT, amount INT, active BOOL)")
	client.simpleQuery("INSERT INTO users (id, amount, active) VALUES (1, 5, true)")
	client.simpleQuery("INSERT INTO users (id, amount, active) VALUES (2, 5000000000, false)")
	client.simpleQuery("INSERT INTO users (id, amount, active) VALUES (3, -7, true)")
	client.simpleQuery("COMMIT")

	int4Bytes := func(v int32) []byte {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(v))
		return buf
	}
	int8Bytes := func(v int64) []byte {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	}
	assertIDs := func(t *testing.T, messages []any, want []string) {
		t.Helper()
		var got []string
		for _, raw := range messages {
			if msg, ok := raw.(*pgproto3.DataRow); ok {
				got = append(got, string(msg.Values[0]))
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("unexpected ids: got %v want %v", got, want)
		}
	}

	client.send(
		&pgproto3.Parse{Name: "bin_int4", Query: "SELECT id FROM accounts.users WHERE amount = $1 ORDER BY id ASC", ParameterOIDs: []uint32{23}},
		&pgproto3.Bind{DestinationPortal: "bin_int4", PreparedStatement: "bin_int4", ParameterFormatCodes: []int16{1}, Parameters: [][]byte{int4Bytes(-7)}},
		&pgproto3.Execute{Portal: "bin_int4", MaxRows: 0},
		&pgproto3.Sync{},
	)
	assertIDs(t, client.receiveUntilReady(), []string{"3"})

	client.send(
		&pgproto3.Parse{Name: "bin_int8", Query: "SELECT id FROM accounts.users WHERE amount = $1 ORDER BY id ASC", ParameterOIDs: []uint32{20}},
		&pgproto3.Bind{DestinationPortal: "bin_int8", PreparedStatement: "bin_int8", ParameterFormatCodes: []int16{1}, Parameters: [][]byte{int8Bytes(5000000000)}},
		&pgproto3.Execute{Portal: "bin_int8", MaxRows: 0},
		&pgproto3.Sync{},
	)
	assertIDs(t, client.receiveUntilReady(), []string{"2"})

	client.send(
		&pgproto3.Parse{Name: "bin_bool", Query: "SELECT id FROM accounts.users WHERE active = $1 ORDER BY id ASC", ParameterOIDs: []uint32{16}},
		&pgproto3.Bind{DestinationPortal: "bin_bool", PreparedStatement: "bin_bool", ParameterFormatCodes: []int16{1}, Parameters: [][]byte{{1}}},
		&pgproto3.Execute{Portal: "bin_bool", MaxRows: 0},
		&pgproto3.Sync{},
	)
	assertIDs(t, client.receiveUntilReady(), []string{"1", "3"})
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

func TestCopyFromStdinCSVInsertsQuotedValues(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT, email TEXT, note TEXT)")
	client.simpleQuery("COMMIT")

	client.send(&pgproto3.Query{String: "COPY accounts.users (id, email, note) FROM STDIN WITH CSV"})
	if _, ok := client.receive().(*pgproto3.CopyInResponse); !ok {
		t.Fatal("expected CopyInResponse")
	}

	client.send(
		&pgproto3.CopyData{Data: []byte("1,one@asql.dev,plain note\n")},
		&pgproto3.CopyData{Data: []byte("2,two@asql.dev,\"quoted, value\"\n")},
		&pgproto3.CopyData{Data: []byte("3,three@asql.dev,\"he said \"\"hi\"\"\"\n")},
		&pgproto3.CopyDone{},
	)
	messages := client.receiveUntilReady()

	var commandTag string
	for _, raw := range messages {
		if msg, ok := raw.(*pgproto3.CommandComplete); ok {
			commandTag = string(msg.CommandTag)
		}
	}
	if commandTag != "COPY 3" {
		t.Fatalf("unexpected COPY command tag: got %q want %q", commandTag, "COPY 3")
	}

	rows := client.simpleQuery("SELECT id, email, note FROM accounts.users ORDER BY id ASC")
	var got [][]string
	for _, raw := range rows {
		if msg, ok := raw.(*pgproto3.DataRow); ok {
			got = append(got, []string{string(msg.Values[0]), string(msg.Values[1]), string(msg.Values[2])})
		}
	}
	if !reflect.DeepEqual(got, [][]string{
		{"1", "one@asql.dev", "plain note"},
		{"2", "two@asql.dev", "quoted, value"},
		{"3", "three@asql.dev", `he said "hi"`},
	}) {
		t.Fatalf("unexpected rows after CSV COPY FROM: got %v", got)
	}
}

func TestCopyToStdoutCSVQuotesValues(t *testing.T) {
	addr, cleanup := startConformanceServer(t)
	defer cleanup()

	client := newRawProtoClient(t, addr)
	defer client.close()

	client.simpleQuery("BEGIN DOMAIN accounts")
	client.simpleQuery("CREATE TABLE users (id INT, email TEXT, note TEXT)")
	client.simpleQuery("INSERT INTO users (id, email, note) VALUES (1, 'one@asql.dev', 'plain note')")
	client.simpleQuery("INSERT INTO users (id, email, note) VALUES (2, 'two@asql.dev', 'quoted, value')")
	client.simpleQuery("INSERT INTO users (id, email, note) VALUES (3, 'three@asql.dev', 'he said \"hi\"')")
	client.simpleQuery("COMMIT")

	client.send(&pgproto3.Query{String: "COPY accounts.users (id, email, note) TO STDOUT WITH CSV"})
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
	wantRows := []string{
		"1,one@asql.dev,plain note\n",
		"2,two@asql.dev,\"quoted, value\"\n",
		"3,three@asql.dev,\"he said \"\"hi\"\"\"\n",
	}
	sort.Strings(wantRows)
	if !reflect.DeepEqual(copyRows, wantRows) {
		t.Fatalf("unexpected CSV COPY TO rows: got %v want %v", copyRows, wantRows)
	}
	if commandTag != "COPY 3" {
		t.Fatalf("unexpected CSV COPY TO command tag: got %q want %q", commandTag, "COPY 3")
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
