package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"asql/internal/engine/executor"
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
	pgwireAddr := flag.String("pgwire", "127.0.0.1:5433", "ASQL pgwire endpoint (used by shell)")
	authToken := flag.String("auth-token", "", "optional bearer token for authenticated APIs")
	demo := flag.Bool("demo", false, "run end-to-end gRPC demo flow")
	command := flag.String("command", "", "operation: shell|begin|execute|commit|rollback|time-travel|replay|backup-create|backup-manifest|backup-verify|restore-lsn|restore-timestamp|snapshot-catalog|wal-retention")
	mode := flag.String("mode", "domain", "tx mode for begin: domain|cross")
	domains := flag.String("domains", "", "comma-separated domains (required for begin and usually for time-travel)")
	txID := flag.String("tx-id", "", "transaction id for execute/commit/rollback")
	sql := flag.String("sql", "", "sql for execute or time-travel")
	lsn := flag.Uint64("lsn", 0, "lsn for replay or time-travel")
	logicalTS := flag.Uint64("logical-ts", 0, "logical timestamp for time-travel when lsn is not provided")
	dataDir := flag.String("data-dir", "", "local ASQL data directory for backup/restore commands")
	backupDir := flag.String("backup-dir", "", "backup directory for backup/restore commands")
	flag.Parse()

	// Support positional  "asqlctl shell"  syntax in addition to -command.
	if args := flag.Args(); len(args) > 0 && strings.EqualFold(args[0], "shell") && *command == "" {
		*command = "shell"
	}

	if !*demo {
		if strings.TrimSpace(*command) == "" {
			fmt.Fprintf(os.Stdout, "asqlctl ready (endpoint=%s).\n", *endpoint)
			fmt.Fprintln(os.Stdout, "Use -demo, 'shell', or -command shell|begin|execute|commit|rollback|time-travel|replay|...")
			return
		}

		// Interactive shell — connects via pgwire, not gRPC.
		if strings.EqualFold(*command, "shell") {
			if err := runShell(shellConfig{
				PgwireAddr: *pgwireAddr,
				AuthToken:  *authToken,
			}); err != nil {
				fmt.Fprintf(os.Stderr, "shell error: %v\n", err)
				os.Exit(1)
			}
			return
		}

		if err := runCommand(os.Stdout, *endpoint, *authToken, *command, *mode, *domains, *txID, *sql, *dataDir, *backupDir, *lsn, *logicalTS); err != nil {
			fmt.Fprintf(os.Stderr, "command failed: %v\n", err)
			os.Exit(1)
		}

		return
	}

	if err := runDemo(*endpoint, *authToken); err != nil {
		fmt.Fprintf(os.Stderr, "demo failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stdout, "demo completed successfully")
}

func runDemo(endpoint, authToken string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tableName := fmt.Sprintf("users_demo_%d", time.Now().UnixNano())

	connection, err := dialConnection(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("dial server: %w", err)
	}
	defer connection.Close()

	ctx = withAuth(ctx, authToken)

	begin := new(api.BeginTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &api.BeginTxRequest{
		Mode:    "domain",
		Domains: []string{"accounts"},
	}, begin); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err := execute(ctx, connection, begin.TxID, fmt.Sprintf("CREATE TABLE %s (id INT, email TEXT)", tableName)); err != nil {
		return err
	}
	if err := execute(ctx, connection, begin.TxID, fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'demo@asql.dev')", tableName)); err != nil {
		return err
	}

	commit := new(api.CommitTxResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/CommitTx", &api.CommitTxRequest{TxID: begin.TxID}, commit); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	timeTravel := new(api.TimeTravelQueryResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/TimeTravelQuery", &api.TimeTravelQueryRequest{
		SQL:     fmt.Sprintf("SELECT id, email FROM %s", tableName),
		Domains: []string{"accounts"},
		LSN:     4,
	}, timeTravel); err != nil {
		return fmt.Errorf("time travel query: %w", err)
	}

	fmt.Fprintf(os.Stdout, "time-travel rows: %d\n", len(timeTravel.Rows))

	replay := new(api.ReplayToLSNResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ReplayToLSN", &api.ReplayToLSNRequest{LSN: 4}, replay); err != nil {
		return fmt.Errorf("replay to lsn: %w", err)
	}

	fmt.Fprintf(os.Stdout, "replay applied lsn: %d\n", replay.AppliedLSN)
	return nil
}

func runCommand(out io.Writer, endpoint, authToken, command, mode, domainsCSV, txID, sql, dataDir, backupDir string, lsn, logicalTS uint64) error {
	if isLocalRecoveryCommand(command) {
		return runLocalRecoveryCommand(context.Background(), out, command, dataDir, backupDir, lsn, logicalTS)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	connection, err := dialConnection(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("dial server: %w", err)
	}
	defer connection.Close()

	ctx = withAuth(ctx, authToken)
	domains := parseDomains(domainsCSV)

	switch strings.ToLower(strings.TrimSpace(command)) {
	case "begin":
		if len(domains) == 0 {
			return errors.New("begin requires -domains")
		}
		response := new(api.BeginTxResponse)
		if err := connection.Invoke(ctx, "/asql.v1.ASQLService/BeginTx", &api.BeginTxRequest{Mode: mode, Domains: domains}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "execute":
		if strings.TrimSpace(txID) == "" {
			return errors.New("execute requires -tx-id")
		}
		if strings.TrimSpace(sql) == "" {
			return errors.New("execute requires -sql")
		}
		response := new(api.ExecuteResponse)
		if err := connection.Invoke(ctx, "/asql.v1.ASQLService/Execute", &api.ExecuteRequest{TxID: txID, SQL: sql}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "commit":
		if strings.TrimSpace(txID) == "" {
			return errors.New("commit requires -tx-id")
		}
		response := new(api.CommitTxResponse)
		if err := connection.Invoke(ctx, "/asql.v1.ASQLService/CommitTx", &api.CommitTxRequest{TxID: txID}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "rollback":
		if strings.TrimSpace(txID) == "" {
			return errors.New("rollback requires -tx-id")
		}
		response := new(api.RollbackTxResponse)
		if err := connection.Invoke(ctx, "/asql.v1.ASQLService/RollbackTx", &api.RollbackTxRequest{TxID: txID}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "time-travel":
		if strings.TrimSpace(sql) == "" {
			return errors.New("time-travel requires -sql")
		}
		if lsn == 0 && logicalTS == 0 {
			return errors.New("time-travel requires -lsn or -logical-ts")
		}
		response := new(api.TimeTravelQueryResponse)
		if err := connection.Invoke(ctx, "/asql.v1.ASQLService/TimeTravelQuery", &api.TimeTravelQueryRequest{SQL: sql, Domains: domains, LSN: lsn, LogicalTimestamp: logicalTS}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "replay":
		if lsn == 0 {
			return errors.New("replay requires -lsn > 0")
		}
		response := new(api.ReplayToLSNResponse)
		if err := connection.Invoke(ctx, "/asql.v1.ASQLService/ReplayToLSN", &api.ReplayToLSNRequest{LSN: lsn}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	default:
		return fmt.Errorf("unsupported -command %q", command)
	}
}

func isLocalRecoveryCommand(command string) bool {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "backup-create", "backup-manifest", "backup-verify", "restore-lsn", "restore-timestamp", "restore-ts", "snapshot-catalog", "wal-retention":
		return true
	default:
		return false
	}
}

func runLocalRecoveryCommand(ctx context.Context, out io.Writer, command, dataDir, backupDir string, lsn, logicalTS uint64) error {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "backup-create":
		if strings.TrimSpace(dataDir) == "" || strings.TrimSpace(backupDir) == "" {
			return errors.New("backup-create requires -data-dir and -backup-dir")
		}
		manifest, err := executor.CreateBaseBackup(dataDir, backupDir)
		if err != nil {
			return err
		}
		return printJSONTo(out, manifest)
	case "backup-manifest":
		if strings.TrimSpace(backupDir) == "" {
			return errors.New("backup-manifest requires -backup-dir")
		}
		manifest, err := executor.LoadBaseBackupManifest(backupDir)
		if err != nil {
			return err
		}
		return printJSONTo(out, manifest)
	case "backup-verify":
		if strings.TrimSpace(backupDir) == "" {
			return errors.New("backup-verify requires -backup-dir")
		}
		manifest, err := executor.VerifyBaseBackup(backupDir)
		if err != nil {
			return err
		}
		return printJSONTo(out, struct {
			Status   string                      `json:"status"`
			Manifest executor.BaseBackupManifest `json:"manifest"`
		}{Status: "OK", Manifest: manifest})
	case "restore-lsn":
		if strings.TrimSpace(dataDir) == "" || strings.TrimSpace(backupDir) == "" {
			return errors.New("restore-lsn requires -data-dir and -backup-dir")
		}
		if lsn == 0 {
			return errors.New("restore-lsn requires -lsn > 0")
		}
		result, err := executor.RestoreBaseBackupToLSN(ctx, backupDir, dataDir, lsn)
		if err != nil {
			return err
		}
		return printJSONTo(out, result)
	case "restore-timestamp", "restore-ts":
		if strings.TrimSpace(dataDir) == "" || strings.TrimSpace(backupDir) == "" {
			return errors.New("restore-timestamp requires -data-dir and -backup-dir")
		}
		if logicalTS == 0 {
			return errors.New("restore-timestamp requires -logical-ts > 0")
		}
		result, err := executor.RestoreBaseBackupToTimestamp(ctx, backupDir, dataDir, logicalTS)
		if err != nil {
			return err
		}
		return printJSONTo(out, result)
	case "snapshot-catalog":
		if strings.TrimSpace(dataDir) == "" {
			return errors.New("snapshot-catalog requires -data-dir")
		}
		catalog, err := executor.InspectDataDirSnapshotCatalog(strings.TrimSpace(dataDir))
		if err != nil {
			return err
		}
		return printJSONTo(out, catalog)
	case "wal-retention":
		if strings.TrimSpace(dataDir) == "" {
			return errors.New("wal-retention requires -data-dir")
		}
		state, err := executor.InspectDataDirWALRetention(strings.TrimSpace(dataDir))
		if err != nil {
			return err
		}
		return printJSONTo(out, state)
	default:
		return fmt.Errorf("unsupported local recovery command %q", command)
	}
}

func dialConnection(ctx context.Context, endpoint string) (*grpcgo.ClientConn, error) {
	return grpcgo.DialContext(
		ctx,
		endpoint,
		grpcgo.WithTransportCredentials(insecure.NewCredentials()),
		grpcgo.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: 3 * time.Second}
			return dialer.DialContext(ctx, "tcp", address)
		}),
		grpcgo.WithDefaultCallOptions(grpcgo.ForceCodec(jsonCodec{})),
	)
}

func withAuth(ctx context.Context, authToken string) context.Context {
	token := strings.TrimSpace(authToken)
	if token == "" {
		return ctx
	}

	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		return metadata.AppendToOutgoingContext(ctx, "authorization", token)
	}

	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func parseDomains(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		result = append(result, trimmed)
	}

	return result
}

func printJSON(value any) error {
	return printJSONTo(os.Stdout, value)
}

func printJSONTo(out io.Writer, value any) error {
	bytes, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(out, string(bytes))
	return err
}

func execute(ctx context.Context, connection *grpcgo.ClientConn, txID, sql string) error {
	response := new(api.ExecuteResponse)
	if err := connection.Invoke(ctx, "/asql.v1.ASQLService/Execute", &api.ExecuteRequest{TxID: txID, SQL: sql}, response); err != nil {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "CREATE TABLE ") && strings.Contains(strings.ToLower(err.Error()), "table already exists") {
			return nil
		}
		return fmt.Errorf("execute %q: %w", sql, err)
	}

	return nil
}
