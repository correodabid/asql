package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"strings"
	"time"

	api "asql/internal/server/grpc"

	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

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

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:9042", "ASQL gRPC endpoint")
	authToken := flag.String("auth-token", "", "optional bearer token")
	dbPrincipal := flag.String("db-principal", "", "optional durable database principal for gRPC metadata auth")
	dbPassword := flag.String("db-password", "", "optional durable database principal password for gRPC metadata auth")
	domain := flag.String("domain", "app", "transaction domain")
	secondaryDomain := flag.String("secondary-domain", "app_aux", "secondary domain used for cross-domain begin demo")
	table := flag.String("table", "users", "table name")
	id := flag.Int("id", 1, "user id to insert")
	email := flag.String("email", "you@example.com", "email to insert")
	initSchema := flag.Bool("init-schema", false, "initialize table schema in a dedicated transaction (run once on fresh WAL)")
	verifyAdmin := flag.Bool("verify-admin", false, "run time-travel and replay checks after commit")
	verifyAdminStrict := flag.Bool("verify-admin-strict", false, "fail execution if admin verification fails")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	connection, err := grpcgo.DialContext(
		ctx,
		*endpoint,
		grpcgo.WithTransportCredentials(insecure.NewCredentials()),
		grpcgo.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: 3 * time.Second}
			return dialer.DialContext(ctx, "tcp", address)
		}),
		grpcgo.WithDefaultCallOptions(grpcgo.ForceCodec(jsonCodec{})),
	)
	if err != nil {
		fatalf("dial server: %v", err)
	}
	defer connection.Close()

	ctx = withAuth(ctx, *authToken, *dbPrincipal, *dbPassword)

	if *initSchema {
		if err := initializeSchema(ctx, connection, *domain, *table); err != nil {
			fatalf("init schema: %v", err)
		}
		fmt.Println("schema initialized")
	}

	txID, err := beginTx(ctx, connection, "domain", []string{*domain})
	if err != nil {
		fatalf("begin tx: %v", err)
	}
	fmt.Printf("tx started: %s\n", txID)

	insertSQL := fmt.Sprintf("INSERT INTO %s (id, email) VALUES (%d, '%s')", *table, *id, escapeString(*email))
	if err := execute(ctx, connection, txID, insertSQL); err != nil {
		fatalf("queue insert: %v", err)
	}

	if err := commitTx(ctx, connection, txID); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "table not found") {
			fatalf("commit tx: %v (run once with -init-schema)", err)
		}
		fatalf("insert flow: %v", err)
	}
	fmt.Println("tx committed")

	rollbackTxID, err := beginTx(ctx, connection, "domain", []string{*domain})
	if err != nil {
		fatalf("begin rollback demo tx: %v", err)
	}

	rollbackInsertSQL := fmt.Sprintf("INSERT INTO %s (id, email) VALUES (%d, '%s')", *table, *id+1000000, escapeString("rollback@example.com"))
	if err := execute(ctx, connection, rollbackTxID, rollbackInsertSQL); err != nil {
		fatalf("queue rollback demo insert: %v", err)
	}

	if err := rollbackTx(ctx, connection, rollbackTxID); err != nil {
		fatalf("rollback demo tx: %v", err)
	}
	fmt.Println("rollback demo completed")

	crossTxID, err := beginTx(ctx, connection, "cross", []string{*domain, *secondaryDomain})
	if err != nil {
		fatalf("begin cross-domain tx: %v", err)
	}

	if err := rollbackTx(ctx, connection, crossTxID); err != nil {
		fatalf("rollback cross-domain tx: %v", err)
	}
	fmt.Printf("cross-domain begin/rollback demo completed (%s, %s)\n", *domain, *secondaryDomain)

	if !*verifyAdmin {
		fmt.Println("admin checks skipped (enable with -verify-admin)")
		return
	}

	rows, err := timeTravelQuery(ctx, connection, *domain, fmt.Sprintf("SELECT id, email FROM %s", *table), math.MaxUint64)
	if err != nil {
		handleAdminVerificationError("time-travel query", err, *verifyAdminStrict)
		return
	}

	printJSON("rows", rows)

	rowsByTimestamp, err := timeTravelQueryByTimestamp(ctx, connection, *domain, fmt.Sprintf("SELECT id, email FROM %s", *table), math.MaxUint64)
	if err != nil {
		handleAdminVerificationError("time-travel query by timestamp", err, *verifyAdminStrict)
		return
	}

	printJSON("rows_by_timestamp", rowsByTimestamp)

	if err := replayToLSN(ctx, connection, math.MaxUint64); err != nil {
		handleAdminVerificationError("replay to latest", err, *verifyAdminStrict)
		return
	}
	fmt.Println("replay applied to latest available LSN")
}

func beginTx(ctx context.Context, connection *grpcgo.ClientConn, mode string, domains []string) (string, error) {
	cleanDomains := make([]string, 0, len(domains))
	for _, domain := range domains {
		trimmed := strings.TrimSpace(domain)
		if trimmed == "" {
			continue
		}
		cleanDomains = append(cleanDomains, trimmed)
	}

	response := new(api.BeginTxResponse)
	err := connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &api.BeginTxRequest{
		Mode:    strings.TrimSpace(mode),
		Domains: cleanDomains,
	}, response)
	if err != nil {
		return "", err
	}

	return response.TxID, nil
}

func rollbackTx(ctx context.Context, connection *grpcgo.ClientConn, txID string) error {
	response := new(api.RollbackTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/RollbackTx", &api.RollbackTxRequest{TxID: txID}, response); err != nil {
		return err
	}

	if strings.TrimSpace(response.Status) != "ROLLBACK" {
		return fmt.Errorf("unexpected rollback status: %s", response.Status)
	}

	return nil
}

func execute(ctx context.Context, connection *grpcgo.ClientConn, txID, sql string) error {
	response := new(api.ExecuteResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/Execute", &api.ExecuteRequest{TxID: txID, SQL: sql}, response); err != nil {
		return err
	}

	return nil
}

func commitTx(ctx context.Context, connection *grpcgo.ClientConn, txID string) error {
	response := new(api.CommitTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/CommitTx", &api.CommitTxRequest{TxID: txID}, response); err != nil {
		return err
	}

	if strings.TrimSpace(response.Status) != "COMMIT" {
		return fmt.Errorf("unexpected commit status: %s", response.Status)
	}

	return nil
}

func timeTravelQuery(ctx context.Context, connection *grpcgo.ClientConn, domain, sql string, lsn uint64) ([]map[string]interface{}, error) {
	response := new(api.TimeTravelQueryResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/TimeTravelQuery", &api.TimeTravelQueryRequest{
		SQL:     sql,
		Domains: []string{domain},
		LSN:     lsn,
	}, response); err != nil {
		return nil, err
	}

	return response.Rows, nil
}

func timeTravelQueryByTimestamp(ctx context.Context, connection *grpcgo.ClientConn, domain, sql string, logicalTimestamp uint64) ([]map[string]interface{}, error) {
	response := new(api.TimeTravelQueryResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/TimeTravelQuery", &api.TimeTravelQueryRequest{
		SQL:              sql,
		Domains:          []string{domain},
		LogicalTimestamp: logicalTimestamp,
	}, response); err != nil {
		return nil, err
	}

	return response.Rows, nil
}

func replayToLSN(ctx context.Context, connection *grpcgo.ClientConn, lsn uint64) error {
	response := new(api.ReplayToLSNResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ReplayToLSN", &api.ReplayToLSNRequest{LSN: lsn}, response); err != nil {
		return err
	}

	return nil
}

func initializeSchema(ctx context.Context, connection *grpcgo.ClientConn, domain, table string) error {
	txID, err := beginTx(ctx, connection, "domain", []string{domain})
	if err != nil {
		return fmt.Errorf("begin schema tx: %w", err)
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (id INT, email TEXT)", table)
	if err := execute(ctx, connection, txID, createSQL); err != nil {
		return fmt.Errorf("queue create table: %w", err)
	}

	if err := commitTx(ctx, connection, txID); err != nil {
		return fmt.Errorf("commit schema tx: %w", err)
	}

	return nil
}


func withAuth(ctx context.Context, authToken, dbPrincipal, dbPassword string) context.Context {
	token := strings.TrimSpace(authToken)
	if token != "" {
		if strings.HasPrefix(strings.ToLower(token), "bearer ") {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", token)
		} else {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		}
	}

	principal := strings.TrimSpace(dbPrincipal)
	password := strings.TrimSpace(dbPassword)
	if principal != "" || password != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "asql-principal", principal, "asql-password", password)
	}

	return ctx
}

func printJSON(label string, value interface{}) {
	bytes, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fatalf("marshal %s: %v", label, err)
	}

	fmt.Printf("%s:\n%s\n", label, string(bytes))
}

func escapeString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func fatalf(format string, args ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func handleAdminVerificationError(step string, err error, strict bool) {
	if strict {
		fatalf("%s: %v", step, err)
	}

	fmt.Printf("warning: %s failed (best-effort mode): %v\n", step, err)

	message := strings.ToLower(err.Error())
	if strings.Contains(message, "apply replay mutation") {
		fmt.Println("hint: admin replay/time-travel is scanning historical WAL; this usually means older WAL entries are inconsistent.")
		fmt.Println("hint: restart asqld with a fresh data directory (for example: -data-dir ./fresh-data) and try -verify-admin again.")
	}
}
