package pgwire

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/sqlerr"
	"github.com/correodabid/asql/internal/platform/buildinfo"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
)

// Result attestations (ADR 0006): SELECT * FROM asql_admin.attest_query
// WHERE sql = '...' [AND lsn = N] runs the SELECT at a committed LSN and
// returns its rows with a signed attestation that binds them, the SQL and
// the chain value at that LSN. Like digests, attestations are made here,
// outside the engine: nothing reaches the WAL or engine state.

var reAttestLSN = regexp.MustCompile(`(?i)\blsn\s*=\s*(\d+)`)

var attestQueryColumns = []string{
	"version", "lsn", "chain", "term", "timestamp", "domains", "sql", "result_hash",
	"row_count", "columns", "engine_version", "node_id", "key_id", "signed_at", "sig", "rows",
}

// errAttestation is returned for a query that cannot be attested, and
// errAttestationOff when the node does not sign.
var (
	errAttestation    = sqlerr.New("22023", "attest_query")
	errAttestationOff = sqlerr.New("55000", "attest_query: result attestations need ledger mode signed")
)

func (server *Server) adminAttestQuery(ctx context.Context, sql string, activeDomains []string, principal string) (interceptResult, bool) {
	row, err := server.attestQuery(ctx, sql, activeDomains, principal)
	if err != nil {
		return interceptResult{err: err}, true
	}
	return interceptResult{result: executor.Result{Status: "OK", Rows: []map[string]ast.Literal{row}}, columns: attestQueryColumns}, true
}

func (server *Server) attestQuery(ctx context.Context, sql string, activeDomains []string, principal string) (map[string]ast.Literal, error) {
	if server.ledger == nil {
		return nil, errAttestationOff
	}
	where := sql
	m := reRowHistorySQL.FindStringSubmatchIndex(sql)
	if m == nil {
		return nil, fmt.Errorf("%w: WHERE sql = '<SELECT ...>' is required", errAttestation)
	}
	inner := strings.ReplaceAll(sql[m[2]:m[3]], "''", "'")
	// The LSN is read from the rest of the WHERE, not from the quoted SQL.
	where = sql[:m[0]] + sql[m[1]:]
	statement, err := attestableSelect(inner)
	if err != nil {
		return nil, err
	}

	head := server.engine.HeadLSN()
	lsn := head
	if lm := reAttestLSN.FindStringSubmatch(where); lm != nil {
		if lsn, err = strconv.ParseUint(lm[1], 10, 64); err != nil {
			return nil, fmt.Errorf("%w: lsn: %v", errAttestation, err)
		}
	}
	if lsn == 0 || lsn > head {
		return nil, fmt.Errorf("%w: LSN %d is not committed (head is %d)", errAttestation, lsn, head)
	}

	domains := append([]string(nil), activeDomains...)
	sort.Strings(domains)
	if _, err := server.engine.AuthorizeSQL(principal, inner, domains); err != nil {
		return nil, err
	}
	if lsn != head {
		if err := server.authorizeHistoricalReadPrincipal(principal, historicalReadAuditDetail{queryKind: "attest_query", targetKind: "lsn", targetLSN: lsn}, true); err != nil {
			return nil, err
		}
	}
	result, err := server.engine.TimeTravelQueryAsOfLSN(ctx, inner, domains, lsn)
	if err != nil {
		return nil, err
	}
	columns, rows := attestedRows(statement, result.Rows)

	a, err := server.ledger.attest(ctx, ledgerdigest.Attestation{
		LSN:           lsn,
		Domains:       domains,
		SQL:           inner,
		Columns:       columns,
		RowCount:      uint64(len(rows)),
		EngineVersion: buildinfo.EngineVersion(),
	}, rows)
	if err != nil {
		return nil, err
	}
	return attestationRow(a, rows)
}

// attestableSelect parses the SQL to attest: a SELECT that is a function of
// the state at an LSN, with no placeholders, so its text is the whole
// question.
func attestableSelect(sql string) (ast.SelectStatement, error) {
	lower := strings.ToLower(sql)
	switch {
	case strings.TrimSpace(sql) == "":
		return ast.SelectStatement{}, fmt.Errorf("%w: the SQL is empty", errAttestation)
	case reAnyPlaceholder.MatchString(sql):
		return ast.SelectStatement{}, fmt.Errorf("%w: placeholders are not supported; write the values into the SQL", errAttestation)
	case strings.Contains(lower, "asql_admin.") || strings.Contains(lower, "pg_catalog.") || strings.Contains(lower, "information_schema."):
		return ast.SelectStatement{}, fmt.Errorf("%w: virtual tables are not a function of the committed state", errAttestation)
	case strings.Contains(lower, "/* as-of"):
		return ast.SelectStatement{}, fmt.Errorf("%w: give the LSN as lsn = N, not in the SQL", errAttestation)
	}
	statement, err := parser.Parse(sql)
	if err != nil {
		return ast.SelectStatement{}, err
	}
	sel, ok := statement.(ast.SelectStatement)
	if !ok {
		return ast.SelectStatement{}, fmt.Errorf("%w: only a SELECT can be attested", errAttestation)
	}
	switch {
	case sel.ForHistory:
		return ast.SelectStatement{}, fmt.Errorf("%w: FOR HISTORY reads the history store, not the state at an LSN", errAttestation)
	case sel.AsOfLSN != nil || sel.AsOfTimestampMicros != nil:
		return ast.SelectStatement{}, fmt.Errorf("%w: give the LSN as lsn = N, not AS OF in the SQL", errAttestation)
	}
	return sel, nil
}

var reAnyPlaceholder = regexp.MustCompile(`\$\d+`)

// attestedRows orders a result as a client sees it: the select list's
// columns, named by their alias, or the sorted keys for SELECT *.
func attestedRows(sel ast.SelectStatement, result []map[string]ast.Literal) ([]string, [][]ast.Literal) {
	keys := deriveColumns(sel, result)
	columns := make([]string, len(keys))
	lookup := make([][]string, len(keys))
	for i, key := range keys {
		columns[i] = key
		lookup[i] = []string{key}
		if expr, alias, ok := splitAlias(key); ok {
			columns[i] = alias
			lookup[i] = []string{alias, expr}
		}
	}
	rows := make([][]ast.Literal, len(result))
	for i, row := range result {
		values := make([]ast.Literal, len(keys))
		for j := range keys {
			values[j] = ast.Literal{Kind: ast.LiteralNull}
			for _, key := range lookup[j] {
				if v, ok := row[key]; ok {
					values[j] = v
					break
				}
			}
		}
		rows[i] = values
	}
	return columns, rows
}

// splitAlias splits "expr as alias" at its last top-level " as ".
func splitAlias(column string) (string, string, bool) {
	expr, alias := splitDescribedColumnAlias(column)
	expr, alias = strings.TrimSpace(expr), strings.TrimSpace(alias)
	if alias == "" || alias == strings.TrimSpace(column) {
		return "", "", false
	}
	return expr, alias, true
}

func attestationRow(a ledgerdigest.Attestation, rows [][]ast.Literal) (map[string]ast.Literal, error) {
	domains, err := json.Marshal(a.Domains)
	if err != nil {
		return nil, err
	}
	columns, err := json.Marshal(a.Columns)
	if err != nil {
		return nil, err
	}
	encodedRows, err := json.Marshal(ledgerdigest.Rows(rows))
	if err != nil {
		return nil, err
	}
	return map[string]ast.Literal{
		"version":        litU(uint64(a.Version)),
		"lsn":            litU(a.LSN),
		"chain":          litS(hex.EncodeToString(a.Chain[:])),
		"term":           litU(a.Term),
		"timestamp":      litU(a.Timestamp),
		"domains":        litS(string(domains)),
		"sql":            litS(a.SQL),
		"result_hash":    litS(hex.EncodeToString(a.ResultHash[:])),
		"row_count":      litU(a.RowCount),
		"columns":        litS(string(columns)),
		"engine_version": litS(a.EngineVersion),
		"node_id":        litS(a.NodeID),
		"key_id":         litS(a.KeyID),
		"signed_at":      litS(a.SignedAt.UTC().Format(time.RFC3339Nano)),
		"sig":            litS(base64.StdEncoding.EncodeToString(a.Sig)),
		"rows":           litS(string(encodedRows)),
	}, nil
}

// attest fills in the state at a.LSN and the signer's fields, hashes rows
// and signs.
func (ls *ledgerSigning) attest(ctx context.Context, a ledgerdigest.Attestation, rows [][]ast.Literal) (ledgerdigest.Attestation, error) {
	chain, err := ls.store.ChainAt(a.LSN)
	if err != nil {
		return ledgerdigest.Attestation{}, err
	}
	term, timestamp, err := ls.recordAt(ctx, a.LSN)
	if err != nil {
		return ledgerdigest.Attestation{}, err
	}
	sum, err := ledgerdigest.HashResult(a.Columns, rows)
	if err != nil {
		return ledgerdigest.Attestation{}, err
	}
	a.Chain, a.Term, a.Timestamp, a.ResultHash = chain, term, timestamp, sum
	a.NodeID = ls.nodeID
	a.SignedAt = ls.now()
	return ledgerdigest.SignAttestation(ls.signer, a)
}
