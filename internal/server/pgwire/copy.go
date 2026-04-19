package pgwire

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/executor"

	"github.com/jackc/pgx/v5/pgproto3"
)

type copyDirection string

var errNotCopyStatement = errors.New("not a COPY statement")

const (
	copyDirectionFrom copyDirection = "from"
	copyDirectionTo   copyDirection = "to"
)

type copyStatement struct {
	Direction copyDirection
	Domain    string
	Table     string
	Columns   []string
	Format    string
	SQL       string
}

type copyInState struct {
	statement    copyStatement
	savepoint    string
	startedTx    bool
	insertedRows int
	pendingLine  string
	ctx          context.Context
	finish       func()
}

func parseCopyStatement(sql string, session *executor.Session) (copyStatement, error) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "COPY ") {
		return copyStatement{}, errNotCopyStatement
	}
	remainder := strings.TrimSpace(trimmed[len("COPY "):])
	upperRemainder := strings.ToUpper(remainder)
	fromIdx := strings.Index(upperRemainder, " FROM STDIN")
	toIdx := strings.Index(upperRemainder, " TO STDOUT")
	var directionIdx int
	var direction copyDirection
	if fromIdx >= 0 {
		directionIdx = fromIdx
		direction = copyDirectionFrom
	} else if toIdx >= 0 {
		directionIdx = toIdx
		direction = copyDirectionTo
	} else {
		return copyStatement{}, fmt.Errorf("COPY only supports FROM STDIN or TO STDOUT")
	}

	targetPart := strings.TrimSpace(remainder[:directionIdx])
	rest := strings.TrimSpace(remainder[directionIdx:])
	if targetPart == "" {
		return copyStatement{}, fmt.Errorf("COPY target is required")
	}

	resolvedTarget := targetPart
	if idx := strings.Index(targetPart, "("); idx >= 0 {
		closeIdx := strings.LastIndex(targetPart, ")")
		if closeIdx <= idx {
			return copyStatement{}, fmt.Errorf("invalid COPY column list")
		}
		resolvedTarget = strings.TrimSpace(targetPart[:idx])
	}

	domain, table, err := resolveCopyTarget(resolvedTarget, session)
	if err != nil {
		return copyStatement{}, err
	}

	stmt := copyStatement{Direction: direction, Domain: domain, Table: table, SQL: trimmed}
	if cols := parseCopyColumns(targetPart); len(cols) > 0 {
		stmt.Columns = cols
	}

	upperRest := strings.ToUpper(rest)
	if strings.HasPrefix(upperRest, "FROM STDIN") {
		rest = strings.TrimSpace(rest[len("FROM STDIN"):])
	} else if strings.HasPrefix(upperRest, "TO STDOUT") {
		rest = strings.TrimSpace(rest[len("TO STDOUT"):])
	}

	stmt.Format = "text"
	if rest != "" {
		restUpper := strings.ToUpper(rest)
		if !strings.HasPrefix(restUpper, "WITH") {
			return copyStatement{}, fmt.Errorf("COPY options must use WITH")
		}
		if strings.Contains(restUpper, "CSV") {
			stmt.Format = "csv"
		}
		if strings.Contains(restUpper, "BINARY") {
			return copyStatement{}, fmt.Errorf("COPY BINARY is not supported")
		}
	}

	return stmt, nil
}

func resolveCopyTarget(target string, session *executor.Session) (string, string, error) {
	trimmed := strings.TrimSpace(target)
	if trimmed == "" {
		return "", "", fmt.Errorf("COPY target is required")
	}
	parts := strings.Split(trimmed, ".")
	if len(parts) == 2 {
		return strings.ToLower(strings.TrimSpace(parts[0])), strings.ToLower(strings.TrimSpace(parts[1])), nil
	}
	if len(parts) != 1 {
		return "", "", fmt.Errorf("COPY target must be table or domain.table")
	}
	domains := session.ActiveDomains()
	if len(domains) != 1 {
		return "", "", fmt.Errorf("COPY target without domain requires exactly one active domain")
	}
	return strings.ToLower(domains[0]), strings.ToLower(strings.TrimSpace(parts[0])), nil
}

func parseCopyColumns(target string) []string {
	openIdx := strings.Index(target, "(")
	closeIdx := strings.LastIndex(target, ")")
	if openIdx < 0 || closeIdx <= openIdx {
		return nil
	}
	raw := target[openIdx+1 : closeIdx]
	parts := strings.Split(raw, ",")
	cols := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.ToLower(strings.TrimSpace(part))
		if trimmed != "" {
			cols = append(cols, trimmed)
		}
	}
	return cols
}

func (server *Server) handleCopyQuery(backend *pgproto3.Backend, state *connState, sql string) (bool, error) {
	stmt, err := parseCopyStatement(sql, state.session)
	if err != nil {
		if errors.Is(err, errNotCopyStatement) {
			return false, nil
		}
		return true, sendErrorAndReadyCode(backend, err.Error(), "0A000", state.session)
	}
	if server.isFollower() && stmt.Direction == copyDirectionFrom {
		return true, sendFollowerRedirectError(backend, server.leaderPgwireAddr(), state.session)
	}
	if stmt.Direction == copyDirectionTo {
		return true, server.handleCopyTo(backend, state, stmt)
	}
	return true, server.startCopyIn(backend, state, stmt)
}

func (server *Server) startCopyIn(backend *pgproto3.Backend, state *connState, stmt copyStatement) error {
	if state.copyIn != nil {
		return sendErrorAndReadyCode(backend, "COPY already in progress", "55000", state.session)
	}
	ctx, finish := state.beginQuery()
	state.copySeq++
	savepoint := fmt.Sprintf("asql_copy_%d", state.copySeq)
	startedTx := false
	if !state.session.InTransaction() {
		if _, err := server.engine.Execute(ctx, state.session, fmt.Sprintf("BEGIN DOMAIN %s", stmt.Domain)); err != nil {
			finish()
			return sendErrorAndReady(backend, err, state.session)
		}
		startedTx = true
	}
	if _, err := server.engine.Execute(ctx, state.session, "SAVEPOINT "+savepoint); err != nil {
		if startedTx {
			_, _ = server.engine.Execute(ctx, state.session, "ROLLBACK")
		}
		finish()
		return sendErrorAndReady(backend, err, state.session)
	}
	state.copyIn = &copyInState{
		statement: stmt,
		savepoint: savepoint,
		startedTx: startedTx,
		ctx: ctx,
		finish: finish,
	}
	backend.Send(&pgproto3.CopyInResponse{OverallFormat: 0, ColumnFormatCodes: []uint16{0}})
	return backend.Flush()
}

func (server *Server) handleCopyTo(backend *pgproto3.Backend, state *connState, stmt copyStatement) error {
	ctx, finish := state.beginQuery()
	defer finish()

	selectSQL := buildCopySelectSQL(stmt)
	result, columns, err := server.executeSQL(ctx, state.session, selectSQL)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return sendErrorAndReadyCode(backend, "query canceled", "57014", state.session)
		}
		return sendErrorAndReady(backend, err, state.session)
	}
	if len(stmt.Columns) > 0 {
		columns = stmt.Columns
	}
	backend.Send(&pgproto3.CopyOutResponse{OverallFormat: 0, ColumnFormatCodes: []uint16{0}})
	for _, row := range result.Rows {
		runPgwireStreamHook()
		if err := ctx.Err(); err != nil {
			return sendErrorAndReadyCode(backend, "query canceled", "57014", state.session)
		}
		line, err := encodeCopyOutRow(columns, row, stmt.Format)
		if err != nil {
			return sendErrorAndReady(backend, err, state.session)
		}
		backend.Send(&pgproto3.CopyData{Data: line})
	}
	backend.Send(&pgproto3.CopyDone{})
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("COPY %d", len(result.Rows)))})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: txStatus(state.session)})
	return backend.Flush()
}

func (server *Server) handleCopyData(backend *pgproto3.Backend, state *connState, msg *pgproto3.CopyData) error {
	if state.copyIn == nil {
		return sendErrorAndReadyCode(backend, "COPY data received without COPY state", "58000", state.session)
	}
	lines, pending := splitCopyDataLines(state.copyIn.pendingLine + string(msg.Data))
	state.copyIn.pendingLine = pending
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		insertSQL, err := buildCopyInsertSQL(state.copyIn.statement, line)
		if err != nil {
			return server.failCopyIn(backend, state, err.Error(), "22P02")
		}
		if _, err := server.engine.Execute(state.copyIn.ctx, state.session, insertSQL); err != nil {
			if errors.Is(err, context.Canceled) {
				return server.failCopyIn(backend, state, "query canceled", "57014")
			}
			return server.failCopyIn(backend, state, err.Error(), mapErrorToSQLState(err))
		}
		state.copyIn.insertedRows++
	}
	return nil
}

func (server *Server) handleCopyDone(backend *pgproto3.Backend, state *connState) error {
	if state.copyIn == nil {
		return sendErrorAndReadyCode(backend, "COPY done without COPY state", "58000", state.session)
	}
	if strings.TrimSpace(state.copyIn.pendingLine) != "" {
		insertSQL, err := buildCopyInsertSQL(state.copyIn.statement, state.copyIn.pendingLine)
		if err != nil {
			return server.failCopyIn(backend, state, err.Error(), "22P02")
		}
		if _, err := server.engine.Execute(state.copyIn.ctx, state.session, insertSQL); err != nil {
			if errors.Is(err, context.Canceled) {
				return server.failCopyIn(backend, state, "query canceled", "57014")
			}
			return server.failCopyIn(backend, state, err.Error(), mapErrorToSQLState(err))
		}
		state.copyIn.insertedRows++
	}
	copyState := state.copyIn
	state.copyIn = nil
	if copyState.startedTx {
		if _, err := server.engine.Execute(copyState.ctx, state.session, "COMMIT"); err != nil {
			copyState.finish()
			return sendErrorAndReady(backend, err, state.session)
		}
	}
	copyState.finish()
	return sendMessages(
		backend,
		&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("COPY %d", copyState.insertedRows))},
		&pgproto3.ReadyForQuery{TxStatus: txStatus(state.session)},
	)
}

func (server *Server) handleCopyFail(backend *pgproto3.Backend, state *connState, msg *pgproto3.CopyFail) error {
	if state.copyIn == nil {
		return sendErrorAndReadyCode(backend, "COPY fail without COPY state", "58000", state.session)
	}
	message := strings.TrimSpace(msg.Message)
	if message == "" {
		message = "COPY failed"
	}
	return server.failCopyIn(backend, state, message, "57014")
}

func (server *Server) failCopyIn(backend *pgproto3.Backend, state *connState, message, code string) error {
	copyState := state.copyIn
	state.copyIn = nil
	if copyState == nil {
		return sendErrorAndReadyCode(backend, message, code, state.session)
	}
	if copyState.savepoint != "" {
		_, _ = server.engine.Execute(copyState.ctx, state.session, "ROLLBACK TO SAVEPOINT "+copyState.savepoint)
	}
	if copyState.startedTx {
		_, _ = server.engine.Execute(copyState.ctx, state.session, "ROLLBACK")
	}
	copyState.finish()
	return sendErrorAndReadyCode(backend, message, code, state.session)
}

func buildCopySelectSQL(stmt copyStatement) string {
	if len(stmt.Columns) == 0 {
		return fmt.Sprintf("SELECT * FROM %s.%s", stmt.Domain, stmt.Table)
	}
	return fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(stmt.Columns, ", "), stmt.Domain, stmt.Table)
}

func splitCopyDataLines(data string) ([]string, string) {
	raw := strings.ReplaceAll(data, "\r\n", "\n")
	raw = strings.ReplaceAll(raw, "\r", "\n")
	parts := strings.Split(raw, "\n")
	if len(parts) == 0 {
		return nil, ""
	}
	if strings.HasSuffix(raw, "\n") {
		return parts[:len(parts)-1], ""
	}
	return parts[:len(parts)-1], parts[len(parts)-1]
}

func buildCopyInsertSQL(stmt copyStatement, line string) (string, error) {
	values, err := parseCopyLine(line, stmt.Format)
	if err != nil {
		return "", err
	}
	columns := stmt.Columns
	if len(columns) == 0 {
		return "", fmt.Errorf("COPY without explicit column list is not supported yet")
	}
	if len(values) != len(columns) {
		return "", fmt.Errorf("COPY column count mismatch: got %d values want %d", len(values), len(columns))
	}
	quotedCols := make([]string, 0, len(columns))
	quotedVals := make([]string, 0, len(values))
	for i, column := range columns {
		quotedCols = append(quotedCols, column)
		quotedVals = append(quotedVals, copyValueToSQL(values[i]))
	}
	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", stmt.Domain, stmt.Table, strings.Join(quotedCols, ", "), strings.Join(quotedVals, ", ")), nil
}

func parseCopyLine(line, format string) ([]string, error) {
	if format == "csv" {
		return parseCopyCSVLine(line)
	}
	parts := strings.Split(line, "\t")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, decodeCopyTextValue(part))
	}
	return values, nil
}

func parseCopyCSVLine(line string) ([]string, error) {
	var (
		values   []string
		current  strings.Builder
		inQuotes bool
	)
	for i := 0; i < len(line); i++ {
		ch := line[i]
		switch ch {
		case '"':
			if inQuotes && i+1 < len(line) && line[i+1] == '"' {
				current.WriteByte('"')
				i++
				continue
			}
			inQuotes = !inQuotes
		case ',':
			if inQuotes {
				current.WriteByte(ch)
				continue
			}
			values = append(values, current.String())
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if inQuotes {
		return nil, fmt.Errorf("unterminated csv field")
	}
	values = append(values, current.String())
	return values, nil
}

func decodeCopyTextValue(raw string) string {
	if raw == "\\N" {
		return ""
	}
	replacer := strings.NewReplacer(`\\`, `\`, `\t`, "\t", `\n`, "\n", `\r`, "\r")
	return replacer.Replace(raw)
}

func copyValueToSQL(value string) string {
	if value == "" {
		return "NULL"
	}
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return value
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil && strings.Contains(value, ".") {
		return value
	}
	lower := strings.ToLower(value)
	if lower == "true" || lower == "false" || lower == "t" || lower == "f" {
		return lower
	}
	return quoteStringLiteral(value)
}

func quoteStringLiteral(value string) string {
	escaped := strings.ReplaceAll(value, `'`, `''`)
	return `'` + escaped + `'`
}

func encodeCopyOutRow(columns []string, row map[string]ast.Literal, format string) ([]byte, error) {
	values := make([]string, 0, len(columns))
	for _, column := range columns {
		lit, exists := row[column]
		if !exists || lit.Kind == ast.LiteralNull || lit.Kind == "" {
			if format == "csv" {
				values = append(values, "")
			} else {
				values = append(values, `\N`)
			}
			continue
		}
		text := string(literalToText(lit))
		if format == "csv" {
			values = append(values, encodeCopyCSVValue(text))
		} else {
			values = append(values, encodeCopyTextValue(text))
		}
	}
	sep := "\t"
	if format == "csv" {
		sep = ","
	}
	return []byte(strings.Join(values, sep) + "\n"), nil
}

func encodeCopyTextValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`)
	return replacer.Replace(value)
}

func encodeCopyCSVValue(value string) string {
	if strings.ContainsAny(value, ",\n\r\"") {
		return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
	}
	return value
}