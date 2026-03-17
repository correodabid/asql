package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"asql/internal/engine/executor"
	api "asql/internal/server/grpc"
	adminapi "asql/pkg/adminapi"
	"asql/pkg/fixtures"

	"github.com/jackc/pgx/v5"
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
	adminHTTPAddr := flag.String("admin-http", "", "ASQL admin HTTP endpoint for operational/security commands (for example 127.0.0.1:9090)")
	authToken := flag.String("auth-token", "", "optional bearer token for authenticated APIs")
	demo := flag.Bool("demo", false, "run end-to-end gRPC demo flow")
	command := flag.String("command", "", "operation: shell|begin|execute|commit|rollback|time-travel|replay|migration-preflight|backup-create|backup-manifest|backup-verify|restore-lsn|restore-timestamp|snapshot-catalog|wal-retention|audit-report|audit-export|fixture-validate|fixture-load|fixture-export|principal-list|principal-bootstrap-admin|principal-create-user|principal-create-role|principal-grant-privilege|principal-revoke-privilege|principal-grant-role|principal-revoke-role|principal-set-password|principal-disable|principal-enable|principal-delete")
	mode := flag.String("mode", "domain", "tx mode for begin: domain|cross")
	domains := flag.String("domains", "", "comma-separated domains (required for begin and usually for time-travel)")
	tableName := flag.String("table", "", "table filter for audit commands")
	operation := flag.String("operation", "", "operation filter for audit commands (INSERT|UPDATE|DELETE)")
	txID := flag.String("tx-id", "", "transaction id for execute/commit/rollback")
	sql := flag.String("sql", "", "sql for execute or time-travel")
	principal := flag.String("principal", "", "principal name for security management commands")
	password := flag.String("password", "", "principal password for bootstrap/create-user/principal-set-password commands")
	role := flag.String("role", "", "role principal for principal-grant-role/principal-revoke-role")
	privilege := flag.String("privilege", "", "privilege name for principal-grant-privilege (ADMIN|SELECT_HISTORY)")
	rollbackSQL := flag.String("rollback-sql", "", "semicolon-separated rollback SQL for migration-preflight")
	lsn := flag.Uint64("lsn", 0, "lsn for replay or time-travel")
	limit := flag.Int("limit", 0, "row limit for audit commands (0 = unlimited)")
	logicalTS := flag.Uint64("logical-ts", 0, "logical timestamp for time-travel when lsn is not provided")
	dataDir := flag.String("data-dir", "", "local ASQL data directory for backup/restore commands")
	backupDir := flag.String("backup-dir", "", "backup directory for backup/restore commands")
	outputPath := flag.String("output", "", "output file for export commands")
	outputFormat := flag.String("format", "json", "output format for export commands: json|jsonl")
	fixtureFile := flag.String("fixture-file", "", "path to a deterministic fixture JSON file")
	flag.Parse()

	// Support positional  "asqlctl shell"  syntax in addition to -command.
	if args := flag.Args(); len(args) > 0 && strings.EqualFold(args[0], "shell") && *command == "" {
		*command = "shell"
	}

	if !*demo {
		if strings.TrimSpace(*command) == "" {
			fmt.Fprintf(os.Stdout, "asqlctl ready (endpoint=%s).\n", *endpoint)
			fmt.Fprintln(os.Stdout, "Use -demo, 'shell', or -command shell|begin|execute|commit|rollback|time-travel|replay|migration-preflight|audit-report|audit-export|...")
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

		if isFixtureCommand(*command) {
			if err := runFixtureCommand(context.Background(), os.Stdout, *command, *fixtureFile, *domains, *pgwireAddr, *authToken); err != nil {
				fmt.Fprintf(os.Stderr, "command failed: %v\n", err)
				os.Exit(1)
			}
			return
		}

		if isAdminSecurityCommand(*command) {
			if err := runAdminSecurityCommand(context.Background(), os.Stdout, *adminHTTPAddr, *authToken, *command, *principal, *password, *role, *privilege); err != nil {
				fmt.Fprintf(os.Stderr, "command failed: %v\n", err)
				os.Exit(1)
			}
			return
		}

		if isLocalAuditCommand(*command) {
			if err := runLocalAuditCommand(context.Background(), os.Stdout, *command, auditCommandOptions{
				DataDir:   *dataDir,
				Domains:   parseDomains(*domains),
				Table:     *tableName,
				Operation: *operation,
				FromLSN:   *lsn,
				Limit:     *limit,
				Output:    *outputPath,
				Format:    *outputFormat,
			}); err != nil {
				fmt.Fprintf(os.Stderr, "command failed: %v\n", err)
				os.Exit(1)
			}
			return
		}

		if err := runCommand(os.Stdout, *endpoint, *authToken, *command, *mode, *domains, *txID, *sql, *rollbackSQL, *dataDir, *backupDir, *lsn, *logicalTS); err != nil {
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

func runCommand(out io.Writer, endpoint, authToken, command, mode, domainsCSV, txID, sql, rollbackSQL, dataDir, backupDir string, lsn, logicalTS uint64) error {
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
	case "migration-preflight":
		if len(domains) != 1 {
			return errors.New("migration-preflight requires exactly one domain in -domains")
		}
		forward := splitSQLStatements(sql)
		if len(forward) == 0 {
			return errors.New("migration-preflight requires -sql with at least one statement")
		}
		rollback := splitSQLStatements(rollbackSQL)
		response := new(api.MigrationPreflightResponse)
		if err := connection.Invoke(ctx, "/asql.v1.ASQLService/MigrationPreflight", &api.MigrationPreflightRequest{
			Domain:      domains[0],
			ForwardSQL:  forward,
			RollbackSQL: rollback,
		}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	default:
		return fmt.Errorf("unsupported -command %q", command)
	}
}

func splitSQLStatements(sql string) []string {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return nil
	}

	segments := make([]string, 0)
	var current strings.Builder
	inString := false
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch == '\'' {
			if inString && i+1 < len(trimmed) && trimmed[i+1] == '\'' {
				current.WriteByte(ch)
				current.WriteByte(ch)
				i++
				continue
			}
			inString = !inString
		}
		if ch == ';' && !inString {
			segment := strings.TrimSpace(current.String())
			if segment != "" {
				segments = append(segments, segment)
			}
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}
	if segment := strings.TrimSpace(current.String()); segment != "" {
		segments = append(segments, segment)
	}
	return segments
}

func isLocalRecoveryCommand(command string) bool {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "backup-create", "backup-manifest", "backup-verify", "restore-lsn", "restore-timestamp", "restore-ts", "snapshot-catalog", "wal-retention":
		return true
	default:
		return false
	}
}

func isFixtureCommand(command string) bool {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "fixture-validate", "fixture-load", "fixture-export":
		return true
	default:
		return false
	}
}

func isLocalAuditCommand(command string) bool {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "audit-report", "audit-export":
		return true
	default:
		return false
	}
}

func isAdminSecurityCommand(command string) bool {
	switch strings.ToLower(strings.TrimSpace(command)) {
	case "principal-list", "principal-show", "principal-who-can-history", "principal-bootstrap-admin", "principal-create-user", "principal-create-role", "principal-grant-privilege", "principal-revoke-privilege", "principal-grant-role", "principal-revoke-role", "principal-set-password", "principal-disable", "principal-enable", "principal-delete":
		return true
	default:
		return false
	}
}

func runAdminSecurityCommand(ctx context.Context, out io.Writer, adminHTTPAddr, authToken, command, principal, password, role, privilege string) error {
	adminHTTPAddr = strings.TrimSpace(adminHTTPAddr)
	if adminHTTPAddr == "" {
		return errors.New("security commands require -admin-http")
	}
	client := &http.Client{Timeout: 10 * time.Second}

	switch strings.ToLower(strings.TrimSpace(command)) {
	case "principal-list":
		response := new(adminapi.ListPrincipalsResponse)
		if err := doAdminJSON(ctx, client, http.MethodGet, adminHTTPAddr, "/api/v1/security/principals", authToken, nil, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-show":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-show requires -principal")
		}
		response := new(adminapi.ListPrincipalsResponse)
		if err := doAdminJSON(ctx, client, http.MethodGet, adminHTTPAddr, "/api/v1/security/principals", authToken, nil, response); err != nil {
			return err
		}
		target := strings.ToLower(strings.TrimSpace(principal))
		for _, record := range response.Principals {
			if strings.ToLower(strings.TrimSpace(record.Name)) != target {
				continue
			}
			return printJSONTo(out, record)
		}
		return fmt.Errorf("principal %q not found", principal)
	case "principal-who-can-history":
		response := new(adminapi.ListPrincipalsResponse)
		if err := doAdminJSON(ctx, client, http.MethodGet, adminHTTPAddr, "/api/v1/security/principals", authToken, nil, response); err != nil {
			return err
		}
		filtered := make([]adminapi.PrincipalRecord, 0)
		for _, record := range response.Principals {
			for _, granted := range record.EffectivePrivileges {
				if granted == executor.PrincipalPrivilegeSelectHistory {
					filtered = append(filtered, record)
					break
				}
			}
		}
		return printJSONTo(out, struct {
			Principals []adminapi.PrincipalRecord `json:"principals"`
		}{Principals: filtered})
	case "principal-bootstrap-admin":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-bootstrap-admin requires -principal")
		}
		if strings.TrimSpace(password) == "" {
			return errors.New("principal-bootstrap-admin requires -password")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/bootstrap-admin", authToken, adminapi.BootstrapAdminPrincipalRequest{Principal: principal, Password: password}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-create-user":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-create-user requires -principal")
		}
		if strings.TrimSpace(password) == "" {
			return errors.New("principal-create-user requires -password")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/users", authToken, adminapi.CreateUserRequest{Principal: principal, Password: password}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-create-role":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-create-role requires -principal")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/roles", authToken, adminapi.CreateRoleRequest{Principal: principal}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-grant-privilege":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-grant-privilege requires -principal")
		}
		if strings.TrimSpace(privilege) == "" {
			return errors.New("principal-grant-privilege requires -privilege")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/privileges/grant", authToken, adminapi.GrantPrivilegeRequest{Principal: principal, Privilege: privilege}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-revoke-privilege":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-revoke-privilege requires -principal")
		}
		if strings.TrimSpace(privilege) == "" {
			return errors.New("principal-revoke-privilege requires -privilege")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/privileges/revoke", authToken, adminapi.RevokePrivilegeRequest{Principal: principal, Privilege: privilege}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-grant-role":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-grant-role requires -principal")
		}
		if strings.TrimSpace(role) == "" {
			return errors.New("principal-grant-role requires -role")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/roles/grant", authToken, adminapi.GrantRoleRequest{Principal: principal, Role: role}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-revoke-role":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-revoke-role requires -principal")
		}
		if strings.TrimSpace(role) == "" {
			return errors.New("principal-revoke-role requires -role")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/roles/revoke", authToken, adminapi.RevokeRoleRequest{Principal: principal, Role: role}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-set-password":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-set-password requires -principal")
		}
		if strings.TrimSpace(password) == "" {
			return errors.New("principal-set-password requires -password")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/passwords/set", authToken, adminapi.SetPasswordRequest{Principal: principal, Password: password}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-disable":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-disable requires -principal")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/principals/disable", authToken, adminapi.DisablePrincipalRequest{Principal: principal}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-enable":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-enable requires -principal")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/principals/enable", authToken, adminapi.EnablePrincipalRequest{Principal: principal}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	case "principal-delete":
		if strings.TrimSpace(principal) == "" {
			return errors.New("principal-delete requires -principal")
		}
		response := new(adminapi.SecurityMutationResponse)
		if err := doAdminJSON(ctx, client, http.MethodPost, adminHTTPAddr, "/api/v1/security/principals/delete", authToken, adminapi.DeletePrincipalRequest{Principal: principal}, response); err != nil {
			return err
		}
		return printJSONTo(out, response)
	default:
		return fmt.Errorf("unsupported security command %q", command)
	}
}

func doAdminJSON(ctx context.Context, client *http.Client, method, adminHTTPAddr, path, authToken string, requestBody, responseBody any) error {
	var bodyReader io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+adminHTTPAddr+path, bodyReader)
	if err != nil {
		return err
	}
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token := strings.TrimSpace(authToken); token != "" {
		if strings.HasPrefix(strings.ToLower(token), "bearer ") {
			req.Header.Set("Authorization", token)
		} else {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error string `json:"error"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&apiErr); err == nil && strings.TrimSpace(apiErr.Error) != "" {
			return errors.New(apiErr.Error)
		}
		return fmt.Errorf("admin api request failed with status %d", resp.StatusCode)
	}
	if responseBody == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(responseBody)
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

func runFixtureCommand(ctx context.Context, out io.Writer, command, fixturePath, domainsCSV, pgwireAddr, authToken string) error {
	fixturePath = strings.TrimSpace(fixturePath)
	if fixturePath == "" {
		return errors.New("fixture command requires -fixture-file")
	}
	domains := parseDomains(domainsCSV)
	result := struct {
		Status      string   `json:"status"`
		Fixture     string   `json:"fixture"`
		FixtureName string   `json:"fixture_name"`
		Steps       int      `json:"steps"`
		Domains     []string `json:"domains,omitempty"`
	}{
		Fixture: fixturePath,
	}

	switch strings.ToLower(strings.TrimSpace(command)) {
	case "fixture-validate":
		fixture, err := fixtures.LoadFile(fixturePath)
		if err != nil {
			return err
		}
		if err := fixtures.ValidateDryRun(ctx, fixture); err != nil {
			return err
		}
		result.FixtureName = fixture.Name
		result.Steps = len(fixture.Steps)
		result.Status = "validated"
		return printJSONTo(out, result)
	case "fixture-load":
		fixture, err := fixtures.LoadFile(fixturePath)
		if err != nil {
			return err
		}
		if err := fixtures.ValidateDryRun(ctx, fixture); err != nil {
			return err
		}
		if strings.TrimSpace(pgwireAddr) == "" {
			return errors.New("fixture-load requires -pgwire")
		}
		conn, err := pgx.Connect(ctx, buildConnString(shellConfig{PgwireAddr: pgwireAddr, AuthToken: authToken}))
		if err != nil {
			return fmt.Errorf("connect pgwire: %w", err)
		}
		defer conn.Close(ctx)

		if err := fixtures.Apply(ctx, fixture, pgwireFixtureExecutor{conn: conn}); err != nil {
			return err
		}
		result.FixtureName = fixture.Name
		result.Steps = len(fixture.Steps)
		result.Status = "loaded"
		return printJSONTo(out, result)
	case "fixture-export":
		if strings.TrimSpace(pgwireAddr) == "" {
			return errors.New("fixture-export requires -pgwire")
		}
		if len(domains) == 0 {
			return errors.New("fixture-export requires -domains")
		}
		conn, err := pgx.Connect(ctx, buildConnString(shellConfig{PgwireAddr: pgwireAddr, AuthToken: authToken}))
		if err != nil {
			return fmt.Errorf("connect pgwire: %w", err)
		}
		defer conn.Close(ctx)

		exported, err := fixtures.ExportFromPGWire(ctx, conn, fixtures.ExportOptions{
			Domains: domains,
			Name:    strings.TrimSuffix(filepath.Base(fixturePath), filepath.Ext(fixturePath)),
		})
		if err != nil {
			return err
		}
		if err := fixtures.SaveFile(fixturePath, exported); err != nil {
			return err
		}
		result.Status = "exported"
		result.FixtureName = exported.Name
		result.Steps = len(exported.Steps)
		result.Domains = domains
		return printJSONTo(out, result)
	default:
		return fmt.Errorf("unsupported fixture command %q", command)
	}
}

type pgwireFixtureExecutor struct {
	conn *pgx.Conn
}

func (e pgwireFixtureExecutor) Exec(ctx context.Context, sql string) error {
	_, err := e.conn.Exec(ctx, sql)
	return err
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
