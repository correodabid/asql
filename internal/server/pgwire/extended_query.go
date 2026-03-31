package pgwire

// extended_query.go — PostgreSQL Extended Query Protocol support.
//
// This implements the Parse → Bind → Describe → Execute → Sync pipeline
// that all modern postgres drivers (pgx, psycopg3, jdbc-postgres, etc.) use
// by default.
//
// Parameter substitution strategy: since ASQL's SQL execution layer does not
// natively support placeholders ($1, $2, …), parameters are interpolated as
// text literals at Bind time before the SQL is handed to the engine.  This is
// safe for the MVP while avoiding a full parameterised-query rewrite path.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"asql/internal/engine/executor"
	"asql/internal/engine/parser"
	"asql/internal/engine/parser/ast"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ── Connection state ─────────────────────────────────────────────────────────

// connState holds per-connection mutable state for one pgwire client.
type connState struct {
	session   *executor.Session
	prepared  map[string]preparedStmt // statement name → parsed SQL
	portals   map[string]portal       // portal name → bound, ready-to-execute SQL
	processID uint32
	secretKey uint32
	copyIn    *copyInState
	copySeq   uint64

	// errorPending is true when an extended-protocol error has been sent but
	// the client has not yet responded with a Sync message.  In this state the
	// server silently discards all messages until Sync.
	errorPending bool

	mu            sync.Mutex
	currentCancel context.CancelFunc
	logger        *slog.Logger
}

func (state *connState) beginQuery() (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	state.mu.Lock()
	state.currentCancel = cancel
	state.mu.Unlock()
	return ctx, func() {
		state.mu.Lock()
		if state.currentCancel != nil {
			state.currentCancel = nil
		}
		state.mu.Unlock()
		cancel()
	}
}

func (state *connState) cancelCurrentQuery() {
	state.mu.Lock()
	cancel := state.currentCancel
	state.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// preparedStmt is a named server-side prepared statement.
type preparedStmt struct {
	sql       string
	paramOIDs []uint32 // client-supplied OID hints (may be zero = unspecified)
}

// portal is a bound prepared statement with parameters already inlined.
type portal struct {
	sql      string // SQL ready for execution (params substituted)
	isSelect bool   // true if the SQL is a SELECT-like query
	columns  []string
	nextRow  int
	// describedColumns is the ordered column list sent in the RowDescription
	// during the Describe phase.  Execute must use the same list so that DataRow
	// values are in the exact positions pgx is expecting based on the OIDs it
	// cached from RowDescription.  If it is nil the Execute phase falls back to
	// deriving columns from actual result rows (legacy behaviour).
	describedColumns  []string
	tailEntityChanges *tailEntityChangesPortalState
}

// ── Message handlers ─────────────────────────────────────────────────────────

// handleParse stores a prepared statement and responds with ParseComplete.
func (server *Server) handleParse(backend *pgproto3.Backend, state *connState, msg *pgproto3.Parse) {
	if state.errorPending {
		return
	}
	// Infer number of $n parameters from the SQL so ParameterDescription
	// contains the right count even when the client sends no OID hints. For
	// common schema-aligned shapes we also infer concrete OIDs.
	paramOIDs := msg.ParameterOIDs
	if len(paramOIDs) == 0 {
		paramOIDs = server.inferParamOIDs(msg.Query)
	}
	state.prepared[msg.Name] = preparedStmt{
		sql:       msg.Query,
		paramOIDs: paramOIDs,
	}
	backend.Send(&pgproto3.ParseComplete{})
	_ = backend.Flush()
}

// inferParamOIDs returns ParameterDescription OIDs for common shapes and falls
// back to a count-only unspecified-OID slice when type inference is not yet
// available.
func (server *Server) inferParamOIDs(sql string) []uint32 {
	merged := mergeParamOIDHints(
		server.inferInsertParamOIDs(sql),
		server.inferUpdateParamOIDs(sql),
		server.inferPredicateParamOIDs(sql),
	)
	if len(merged) > 0 {
		return merged
	}
	return inferParamOIDCount(sql)
}

func mergeParamOIDHints(hints ...[]uint32) []uint32 {
	max := 0
	for _, hint := range hints {
		if len(hint) > max {
			max = len(hint)
		}
	}
	if max == 0 {
		return nil
	}
	merged := make([]uint32, max)
	for _, hint := range hints {
		for i, oid := range hint {
			if oid != 0 {
				merged[i] = oid
			}
		}
	}
	return merged
}

// inferParamOIDCount scans sql for the highest $n placeholder and returns a
// slice of that length filled with 0 (unspecified OID).
func inferParamOIDCount(sql string) []uint32 {
	max := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] != '$' {
			continue
		}
		j := i + 1
		for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
			j++
		}
		if j == i+1 {
			continue // bare '$', not a placeholder
		}
		n, _ := strconv.Atoi(sql[i+1 : j])
		if n > max {
			max = n
		}
	}
	if max == 0 {
		return nil
	}
	return make([]uint32, max) // all zeros = unspecified type
}

func (server *Server) inferInsertParamOIDs(sql string) []uint32 {
	trimmed := stripAsOfComment(strings.TrimSpace(sql))
	normalized := normalizePlaceholders(trimmed)
	stmt, err := parser.Parse(normalized)
	if err != nil {
		return nil
	}
	ins, ok := stmt.(ast.InsertStatement)
	if !ok || len(ins.Columns) == 0 {
		return nil
	}

	paramOrder := insertParameterOrder(sql)
	if len(paramOrder) == 0 {
		return nil
	}

	max := 0
	for _, n := range paramOrder {
		if n > max {
			max = n
		}
	}
	if max == 0 {
		return nil
	}

	result := make([]uint32, max)
	cols := make([]string, 0, len(ins.Columns))
	for _, c := range ins.Columns {
		cols = append(cols, strings.ToLower(strings.TrimSpace(c)))
	}
	oids := server.resolveColumnOIDs(ins.TableName, cols)
	for i, paramIndex := range paramOrder {
		if i >= len(cols) {
			break
		}
		if paramIndex <= 0 || paramIndex > len(result) {
			continue
		}
		if oid := oids[cols[i]]; oid != 0 {
			result[paramIndex-1] = oid
		}
	}
	return result
}

func (server *Server) inferUpdateParamOIDs(sql string) []uint32 {
	trimmed := stripAsOfComment(strings.TrimSpace(sql))
	normalized := normalizePlaceholders(trimmed)
	stmt, err := parser.Parse(normalized)
	if err != nil {
		return nil
	}
	upd, ok := stmt.(ast.UpdateStatement)
	if !ok || len(upd.Columns) == 0 {
		return nil
	}

	assignmentClause := extractUpdateSetClause(trimmed)
	if assignmentClause == "" {
		return nil
	}
	assignments := splitTopLevelCSV(assignmentClause)
	if len(assignments) == 0 {
		return nil
	}

	max := 0
	cols := make([]string, 0, len(assignments))
	paramIndexes := make([]int, 0, len(assignments))
	for _, assignment := range assignments {
		col, paramIndex, ok := parseUpdateAssignmentParam(assignment)
		if !ok {
			continue
		}
		cols = append(cols, col)
		paramIndexes = append(paramIndexes, paramIndex)
		if paramIndex > max {
			max = paramIndex
		}
	}
	if max == 0 {
		return nil
	}

	oids := server.resolveColumnOIDs(upd.TableName, cols)
	result := make([]uint32, max)
	for i, col := range cols {
		idx := paramIndexes[i] - 1
		if idx < 0 || idx >= len(result) {
			continue
		}
		if oid := oids[col]; oid != 0 {
			result[idx] = oid
		}
	}
	return result
}

func parseUpdateAssignmentParam(assignment string) (string, int, bool) {
	parts := strings.SplitN(assignment, "=", 2)
	if len(parts) != 2 {
		return "", 0, false
	}
	col := canonicalParamIdentifier(parts[0])
	rhs := strings.TrimSpace(parts[1])
	if col == "" {
		return "", 0, false
	}
	if strings.HasPrefix(rhs, "$") {
		paramIndex, err := strconv.Atoi(strings.TrimPrefix(rhs, "$"))
		if err != nil || paramIndex <= 0 {
			return "", 0, false
		}
		return col, paramIndex, true
	}
	if match := updateArithmeticParamPattern.FindStringSubmatch(rhs); len(match) == 4 {
		leftCol := canonicalParamIdentifier(match[1])
		paramIndex, err := strconv.Atoi(match[3])
		if err != nil || paramIndex <= 0 {
			return "", 0, false
		}
		// Only infer when the arithmetic source column matches the target column.
		// That keeps the OID hint conservative and schema-aligned.
		if leftCol == col {
			return col, paramIndex, true
		}
	}
	return "", 0, false
}

func (server *Server) inferPredicateParamOIDs(sql string) []uint32 {
	trimmed := stripAsOfComment(strings.TrimSpace(sql))
	normalized := normalizePlaceholders(trimmed)
	stmt, err := parser.Parse(normalized)
	if err != nil {
		return nil
	}

	var tableName string
	switch typed := stmt.(type) {
	case ast.SelectStatement:
		tableName = typed.TableName
	case ast.UpdateStatement:
		tableName = typed.TableName
	case ast.DeleteStatement:
		tableName = typed.TableName
	default:
		return nil
	}
	if tableName == "" {
		return nil
	}

	whereClause := extractWhereClause(trimmed)
	if whereClause == "" {
		return nil
	}

	matches := whereParamPattern.FindAllStringSubmatch(whereClause, -1)
	if len(matches) == 0 {
		return nil
	}

	max := 0
	cols := make([]string, 0, len(matches))
	paramIndexes := make([]int, 0, len(matches))
	for _, match := range matches {
		if len(match) < 4 {
			continue
		}
		col := canonicalParamIdentifier(match[1])
		if col == "" {
			continue
		}
		n, err := strconv.Atoi(match[3])
		if err != nil || n <= 0 {
			continue
		}
		cols = append(cols, col)
		paramIndexes = append(paramIndexes, n)
		if n > max {
			max = n
		}
	}
	if max == 0 || len(cols) == 0 {
		return nil
	}

	oids := server.resolveColumnOIDs(tableName, cols)
	result := make([]uint32, max)
	for i, col := range cols {
		idx := paramIndexes[i] - 1
		if idx < 0 || idx >= len(result) {
			continue
		}
		if oid := oids[col]; oid != 0 {
			result[idx] = oid
		}
	}
	return result
}

func insertParameterOrder(sql string) []int {
	upper := strings.ToUpper(sql)
	valuesIdx := strings.Index(upper, "VALUES")
	if valuesIdx == -1 {
		return nil
	}
	rest := sql[valuesIdx+len("VALUES"):]
	var order []int
	depth := 0
	inString := false
	stringQuote := byte(0)
	groupIndex := -1
	exprIndex := 0
	var token strings.Builder
	flush := func() {
		if groupIndex != 0 {
			token.Reset()
			return
		}
		expr := strings.TrimSpace(token.String())
		token.Reset()
		if exprIndex >= 0 {
			if len(expr) > 1 && expr[0] == '$' {
				if n, err := strconv.Atoi(expr[1:]); err == nil {
					order = append(order, n)
				}
			}
			exprIndex++
		}
	}

	for i := 0; i < len(rest); i++ {
		ch := rest[i]
		if inString {
			token.WriteByte(ch)
			if ch == stringQuote {
				if i+1 < len(rest) && rest[i+1] == stringQuote {
					token.WriteByte(rest[i+1])
					i++
					continue
				}
				inString = false
			}
			continue
		}
		switch ch {
		case '\'', '"':
			inString = true
			stringQuote = ch
			token.WriteByte(ch)
		case '(':
			depth++
			if depth == 1 {
				groupIndex++
				exprIndex = 0
				token.Reset()
				continue
			}
			token.WriteByte(ch)
		case ')':
			if depth == 1 {
				flush()
				depth--
				if groupIndex == 0 {
					return order
				}
				continue
			}
			if depth > 1 {
				depth--
				token.WriteByte(ch)
			}
		case ',':
			if depth == 1 {
				flush()
				continue
			}
			if depth > 1 {
				token.WriteByte(ch)
			}
		default:
			if depth >= 1 {
				token.WriteByte(ch)
			}
		}
	}
	return order
}

func extractUpdateSetClause(sql string) string {
	upper := strings.ToUpper(sql)
	setIdx := strings.Index(upper, " SET ")
	if setIdx == -1 {
		return ""
	}
	rest := sql[setIdx+len(" SET "):]
	upperRest := strings.ToUpper(rest)
	if whereIdx := strings.Index(upperRest, " WHERE "); whereIdx != -1 {
		return strings.TrimSpace(rest[:whereIdx])
	}
	return strings.TrimSpace(rest)
}

func splitTopLevelCSV(clause string) []string {
	if strings.TrimSpace(clause) == "" {
		return nil
	}
	parts := make([]string, 0, 4)
	start := 0
	depth := 0
	inString := false
	quote := byte(0)
	for i := 0; i < len(clause); i++ {
		ch := clause[i]
		if inString {
			if ch == quote {
				if i+1 < len(clause) && clause[i+1] == quote {
					i++
					continue
				}
				inString = false
			}
			continue
		}
		switch ch {
		case '\'', '"':
			inString = true
			quote = ch
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(clause[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(clause[start:]))
	return parts
}

var (
	whereParamPattern            = regexp.MustCompile(`(?i)([a-zA-Z_][a-zA-Z0-9_\."\>]*)\s*(=|<>|!=|>=|<=|>|<|like|ilike|not\s+like|not\s+ilike)\s*\$(\d+)`)
	updateArithmeticParamPattern = regexp.MustCompile(`(?i)^([a-zA-Z_][a-zA-Z0-9_\."\>]*)\s*([+\-*/])\s*\$(\d+)$`)
)

func extractWhereClause(sql string) string {
	upper := strings.ToUpper(sql)
	whereIdx := strings.Index(upper, " WHERE ")
	if whereIdx == -1 {
		return ""
	}
	clause := sql[whereIdx+len(" WHERE "):]
	upperClause := strings.ToUpper(clause)
	end := len(clause)
	for _, kw := range []string{" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT ", " OFFSET ", " RETURNING "} {
		if idx := strings.Index(upperClause, kw); idx != -1 && idx < end {
			end = idx
		}
	}
	return strings.TrimSpace(clause[:end])
}

func canonicalParamIdentifier(raw string) string {
	name := strings.TrimSpace(raw)
	if dot := strings.LastIndex(name, "."); dot != -1 {
		name = name[dot+1:]
	}
	name = strings.Trim(name, `"`)
	return strings.ToLower(name)
}

// handleBind substitutes parameters into the prepared statement, stores the
// resulting portal, and responds with BindComplete.
func (server *Server) handleBind(backend *pgproto3.Backend, state *connState, msg *pgproto3.Bind) {
	if state.errorPending {
		return
	}
	stmt, ok := state.prepared[msg.PreparedStatement]
	if !ok {
		server.extendedError(backend, state, fmt.Sprintf("prepared statement %q does not exist", msg.PreparedStatement), "26000")
		return
	}

	sql, err := substituteParams(stmt.sql, msg.Parameters, msg.ParameterFormatCodes, stmt.paramOIDs)
	if err != nil {
		server.extendedError(backend, state, "bind error: "+err.Error(), "22P02")
		return
	}

	// Strip AS-OF comments before syntactic isSelect detection so that queries
	// appended with /* as-of-lsn: N */ or /* as-of-ts: N */ are still
	// recognised as SELECT statements (the comment trips parser.Parse).
	// The full sql (with the comment) is kept in the portal so executeSQL can
	// extract the LSN/TS at execution time.
	sqlForParse := stripAsOfComment(sql)
	// Also strip leading IMPORT directives: they prefix the SQL with "IMPORT"
	// instead of "SELECT", causing isSelect to be set to false and the Describe
	// phase to send NoData (→ empty rows).
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sqlForParse)), "IMPORT ") {
		if _, selectPart, err2 := parser.ExtractImports(strings.TrimSpace(sqlForParse)); err2 == nil && strings.TrimSpace(selectPart) != "" {
			sqlForParse = strings.TrimSpace(selectPart)
		}
	}

	parsedStmt, parseErr := parser.Parse(sqlForParse)
	isSelect := false
	var columns []string
	if _, isExplain := stripExplainSQLPrefix(sqlForParse); isExplain {
		isSelect = true
		columns = append([]string(nil), explainResultColumns...)
	}
	if parseErr == nil {
		if _, ok := parsedStmt.(ast.SelectStatement); ok {
			isSelect = true
			columns = deriveColumns(parsedStmt, nil) // get column names from AST (no rows yet)
		}
	}
	// If parse failed for reasons other than the comment (e.g. WITH clause),
	// fall back to a simple prefix check for SELECT / WITH.
	if !isSelect {
		upper := strings.ToUpper(strings.TrimSpace(sqlForParse))
		if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "WITH") {
			isSelect = true
		}
	}
	// The currently supported INSERT ... RETURNING path also produces rows.
	if !isSelect {
		if cols := server.returningResultColumns(sql); len(cols) > 0 {
			isSelect = true
			columns = cols
		}
	}
	// SHOW <param> produces a single-column, single-row result.
	if !isSelect {
		if paramName, ok := parseShowStatement(sql); ok {
			isSelect = true
			columns = []string{paramName}
		}
	}
	if !isSelect {
		if _, ok, err := parseTailEntityChangesStatement(sql); ok && err == nil {
			isSelect = true
			columns = append([]string(nil), tailEntityChangesColumns...)
		}
	}

	// Eagerly compute the described column list at Bind time using the same
	// logic that Describe Statement ('S') and Describe Portal ('P') use.
	// This is the authoritative contract that Execute must honour:
	//   DataRow[i] must correspond to RowDescription[i].
	//
	// The canonical problem: resolveStarColumns (Describe) returns ALL schema
	// columns, but columnsFromRows (Execute) only returns columns that exist
	// in the stored row map.  For rows inserted without a TIMESTAMP/etc column
	// (no DEFAULT, no explicit value), that column is simply absent → Execute
	// produces fewer values than RowDescription has fields → pgx applies the
	// wrong OID to the shifted positions → "parsing time \"\" as \"2006...\"".
	//
	// By computing describedColumns here (at Bind, before any Describe message
	// has been exchanged), the Execute path always has the correct list whether
	// or not the client sent Describe Statement, Describe Portal, or neither.
	var describedColumns []string
	if isSelect {
		if fields := server.describeFields(sql, state.session.ActiveDomains(), state.session.Principal()); fields != nil {
			describedColumns = make([]string, len(fields))
			for i, f := range fields {
				describedColumns[i] = strings.ToLower(string(f.Name))
			}
		}
	}

	var tailState *tailEntityChangesPortalState
	if isSelect {
		if tailStmt, ok, err := parseTailEntityChangesStatement(sql); ok && err == nil && tailStmt.Follow {
			tailState = newTailEntityChangesPortalState(tailStmt)
		}
	}

	state.portals[msg.DestinationPortal] = portal{
		sql:               sql,
		isSelect:          isSelect,
		columns:           columns,
		nextRow:           0,
		describedColumns:  describedColumns,
		tailEntityChanges: tailState,
	}
	backend.Send(&pgproto3.BindComplete{})
	_ = backend.Flush()
}

// handleDescribe returns RowDescription for SELECT portals/statements, or
// NoData for commands.  For statements it also sends ParameterDescription.
func (server *Server) handleDescribe(backend *pgproto3.Backend, state *connState, msg *pgproto3.Describe) {
	if state.errorPending {
		return
	}

	switch msg.ObjectType {
	case 'S': // Describe Statement
		stmt, ok := state.prepared[msg.Name]
		if !ok {
			server.extendedError(backend, state, fmt.Sprintf("prepared statement %q does not exist", msg.Name), "26000")
			return
		}
		backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: stmt.paramOIDs})
		fields := server.describeFields(stmt.sql, state.session.ActiveDomains(), state.session.Principal())
		// INSERT ... RETURNING produces rows that describeFields
		// cannot detect (it only handles SELECT/WITH).  Fall back to the
		// RETURNING column parser so pgx receives a RowDescription instead of
		// NoData — without this, pgx treats the statement as non-row-returning
		// and QueryRow().Scan() fails with "no rows in result set".
		if fields == nil {
			if returningFields := server.describeReturningFields(stmt.sql); len(returningFields) > 0 {
				fields = returningFields
			}
		}
		if fields != nil {
			backend.Send(&pgproto3.RowDescription{Fields: fields})
		} else {
			backend.Send(&pgproto3.NoData{})
		}

	case 'P': // Describe Portal
		p, ok := state.portals[msg.Name]
		if !ok {
			server.extendedError(backend, state, fmt.Sprintf("portal %q does not exist", msg.Name), "34000")
			return
		}
		if !p.isSelect {
			backend.Send(&pgproto3.NoData{})
		} else {
			fields := server.describeFields(p.sql, state.session.ActiveDomains(), state.session.Principal())
			if fields == nil && len(p.columns) > 0 {
				if returningFields := server.describeReturningFields(p.sql); len(returningFields) > 0 {
					fields = returningFields
				} else {
					fields = make([]pgproto3.FieldDescription, len(p.columns))
					for i, c := range p.columns {
						fields[i] = pgproto3.FieldDescription{
							Name:                 []byte(c),
							TableAttributeNumber: 0,
							DataTypeOID:          25,
							DataTypeSize:         -1,
							TypeModifier:         -1,
						}
					}
				}
			}
			if fields != nil {
				// Save the column order that we advertised in RowDescription so
				// Execute can send DataRow values in exactly the same positions.
				// Without this, schema columns that are absent from a stored row
				// (inserted before the column existed / no DEFAULT) cause a
				// positional shift: pgx decodes a text value with a TIMESTAMP OID
				// and fails with "parsing time \"\" as \"2006...\""
				descCols := make([]string, len(fields))
				for i, f := range fields {
					descCols[i] = strings.ToLower(string(f.Name))
				}
				p.describedColumns = descCols
				state.portals[msg.Name] = p
				backend.Send(&pgproto3.RowDescription{Fields: fields})
			} else {
				backend.Send(&pgproto3.NoData{})
			}
		}
	}
	_ = backend.Flush()
}

// handleExtendedExecute executes a portal and streams results.
// It does NOT send ReadyForQuery — that comes from the Sync handler.
func (server *Server) handleExtendedExecute(backend *pgproto3.Backend, state *connState, msg *pgproto3.Execute) error {
	if state.errorPending {
		return nil
	}
	ctx, finish := state.beginQuery()
	defer finish()
	p, ok := state.portals[msg.Portal]
	if !ok {
		server.extendedError(backend, state, fmt.Sprintf("portal %q does not exist", msg.Portal), "34000")
		return nil
	}

	// SHOW interception: runtime parameters are handled at the pgwire layer.
	// This MUST run before catalog interception because interceptCatalog has
	// a catch-all "show " prefix that would shadow asql_* runtime params.
	if paramName, ok := parseShowStatement(p.sql); ok {
		result, columns := server.showParamResult(state.session, paramName)
		if result.Status == "error" {
			server.extendedError(backend, state, result.Status, "42704")
			return nil
		}
		nextRow, _, err := server.streamExtendedResult(ctx, backend, state, p.sql, result, columns, msg.MaxRows, p.nextRow)
		p.nextRow = nextRow
		state.portals[msg.Portal] = p
		return err
	}

	// Catalog interception.
	if err := server.authorizeCatalogQuery(state.session.Principal(), p.sql, true); err != nil {
		server.extendedError(backend, state, err.Error(), mapErrorToSQLState(err))
		return nil
	}
	if intercepted, ok := server.interceptCatalog(ctx, p.sql, state.session.ActiveDomains(), state.session.Principal()); ok {
		nextRow, _, err := server.streamExtendedResult(ctx, backend, state, p.sql, intercepted.result, intercepted.columns, msg.MaxRows, p.nextRow)
		p.nextRow = nextRow
		state.portals[msg.Portal] = p
		return err
	}

	// Follower redirect: reject write statements on non-leader nodes.
	if server.isFollower() && isWriteStatement(p.sql) {
		server.extendedFollowerRedirect(backend, state, server.leaderPgwireAddr())
		return nil
	}

	if p.tailEntityChanges != nil {
		_, err := server.streamExtendedTailEntityChanges(ctx, backend, state, state.session, p.tailEntityChanges, msg.MaxRows)
		state.portals[msg.Portal] = p
		return err
	}

	result, columns, err := server.executeSQL(ctx, state.session, p.sql)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			server.extendedError(backend, state, "query canceled", "57014")
			return nil
		}
		state.logger.Info("pgwire extended execute error",
			slog.String("sql", p.sql[:min(len(p.sql), 120)]),
			slog.String("error", err.Error()),
		)
		server.extendedError(backend, state, err.Error(), mapErrorToSQLState(err))
		return nil
	}

	// If the Describe phase already told pgx which columns to expect (and in
	// which order), honour that contract exactly.  Using the Describe column
	// list instead of columns derived from actual row keys guarantees that:
	//  1. The DataRow value count always equals the RowDescription field count.
	//  2. Each positional value is decoded with the correct OID (e.g. a
	//     TIMESTAMP column that is absent from an old row is sent as NULL rather
	//     than being silently dropped, which would shift subsequent values and
	//     cause pgx to decode a text value with a timestamp OID →
	//     "parsing time \"\" as \"2006...\"").
	if len(p.describedColumns) > 0 {
		columns = p.describedColumns
	}
	nextRow, _, err := server.streamExtendedResult(ctx, backend, state, p.sql, result, columns, msg.MaxRows, p.nextRow)
	p.nextRow = nextRow
	state.portals[msg.Portal] = p
	return err
}

// streamExtendedResult sends DataRows + CommandComplete for the extended protocol.
// RowDescription must have been sent by Describe; we do NOT send it here.
var pgwireStreamHook func()

func runPgwireStreamHook() {
	if pgwireStreamHook != nil {
		pgwireStreamHook()
	}
}

func (server *Server) streamExtendedResult(ctx context.Context, backend *pgproto3.Backend, state *connState, _ string, result executor.Result, columns []string, maxRows uint32, startRow int) (int, bool, error) {
	if startRow < 0 {
		startRow = 0
	}
	if startRow > len(result.Rows) {
		startRow = len(result.Rows)
	}

	sent := uint32(0)
	if len(columns) > 0 {
		for rowIndex := startRow; rowIndex < len(result.Rows); rowIndex++ {
			runPgwireStreamHook()
			if err := ctx.Err(); err != nil {
				server.extendedError(backend, state, "query canceled", "57014")
				return rowIndex, false, nil
			}
			if maxRows > 0 && sent >= maxRows {
				backend.Send(&pgproto3.PortalSuspended{})
				return rowIndex, true, backend.Flush()
			}
			row := result.Rows[rowIndex]
			values := make([][]byte, 0, len(columns))
			for _, col := range columns {
				lit, exists := row[col]
				if !exists || lit.Kind == ast.LiteralNull || lit.Kind == "" {
					values = append(values, nil)
					continue
				}
				values = append(values, literalToText(lit))
			}
			backend.Send(&pgproto3.DataRow{Values: values})
			sent++
		}
	}
	tag := commandTag(result)
	if err := ctx.Err(); err != nil {
		server.extendedError(backend, state, "query canceled", "57014")
		return startRow, false, nil
	}
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	return len(result.Rows), false, backend.Flush()
}

func (server *Server) streamExtendedTailEntityChanges(ctx context.Context, backend *pgproto3.Backend, state *connState, session *executor.Session, portalState *tailEntityChangesPortalState, maxRows uint32) (bool, error) {
	if portalState == nil {
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
		return false, backend.Flush()
	}
	if portalState.completed {
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", portalState.totalRows))})
		return false, backend.Flush()
	}
	if err := server.authorizeHistoricalRead(session, historicalReadAuditDetail{queryKind: "entity_changes_follow", targetKind: "history_stream"}); err != nil {
		server.extendedError(backend, state, err.Error(), mapErrorToSQLState(err))
		return false, nil
	}

	sent := uint32(0)
	for {
		batchLimit := 0
		if portalState.remaining > 0 {
			batchLimit = portalState.remaining
		}
		if maxRows > 0 {
			allowed := int(maxRows - sent)
			if allowed <= 0 {
				backend.Send(&pgproto3.PortalSuspended{})
				return true, backend.Flush()
			}
			if batchLimit == 0 || allowed < batchLimit {
				batchLimit = allowed
			}
		}

		ch := server.walStore.Subscribe()
		events, err := server.engine.EntityChanges(ctx, executor.EntityChangesRequest{
			Domain:  portalState.statement.Domain,
			Entity:  portalState.statement.Entity,
			RootPK:  portalState.statement.RootPK,
			FromLSN: portalState.nextFrom,
			ToLSN:   portalState.statement.ToLSN,
			Limit:   batchLimit,
		})
		if err != nil {
			if errors.Is(err, context.Canceled) {
				server.extendedError(backend, state, "query canceled", "57014")
				return false, nil
			}
			server.extendedError(backend, state, err.Error(), mapErrorToSQLState(err))
			return false, nil
		}
		if len(events) > 0 {
			for _, event := range events {
				runPgwireStreamHook()
				if err := ctx.Err(); err != nil {
					server.extendedError(backend, state, "query canceled", "57014")
					return false, nil
				}
				values, valueErr := entityChangeEventValues(event)
				if valueErr != nil {
					server.extendedError(backend, state, valueErr.Error(), "XX000")
					return false, nil
				}
				backend.Send(&pgproto3.DataRow{Values: values})
				sent++
				portalState.totalRows++
			}
			if err := backend.Flush(); err != nil {
				return false, err
			}
			portalState.nextFrom = events[len(events)-1].CommitLSN + 1
			if portalState.remaining > 0 {
				portalState.remaining -= len(events)
				if portalState.remaining <= 0 {
					portalState.completed = true
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", portalState.totalRows))})
					return false, backend.Flush()
				}
			}
			if portalState.statement.ToLSN > 0 && portalState.nextFrom > portalState.statement.ToLSN {
				portalState.completed = true
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", portalState.totalRows))})
				return false, backend.Flush()
			}
			if maxRows > 0 && sent >= maxRows {
				backend.Send(&pgproto3.PortalSuspended{})
				return true, backend.Flush()
			}
			continue
		}
		if !portalState.statement.Follow || (portalState.statement.ToLSN > 0 && portalState.nextFrom > portalState.statement.ToLSN) {
			portalState.completed = true
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", portalState.totalRows))})
			return false, backend.Flush()
		}
		if err := server.waitForTailEntityChangesWake(ctx, ch); err != nil {
			if errors.Is(err, context.Canceled) {
				server.extendedError(backend, state, "query canceled", "57014")
				return false, nil
			}
			server.extendedError(backend, state, err.Error(), "57P01")
			return false, nil
		}
	}
}

// handleCloseMessage closes a named statement or portal.
func (server *Server) handleCloseMessage(backend *pgproto3.Backend, state *connState, msg *pgproto3.Close) {
	if state.errorPending {
		return
	}
	switch msg.ObjectType {
	case 'S':
		delete(state.prepared, msg.Name)
	case 'P':
		delete(state.portals, msg.Name)
	}
	backend.Send(&pgproto3.CloseComplete{})
	_ = backend.Flush()
}

// extendedError sends an ErrorResponse and marks the connection as errorPending.
// The caller must not send further result messages until Sync clears the flag.
func (server *Server) extendedError(backend *pgproto3.Backend, state *connState, msg, sqlstate string) {
	state.errorPending = true
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     sqlstate,
		Message:  msg,
	})
	_ = backend.Flush()
}

// extendedFollowerRedirect sends SQLSTATE 25006 with the leader hint for the
// extended query protocol path. Sets errorPending so subsequent messages in
// the same pipeline are silently discarded until a Sync is received.
func (server *Server) extendedFollowerRedirect(backend *pgproto3.Backend, state *connState, leaderAddr string) {
	state.errorPending = true
	msg := "not the leader: write must be directed to the current leader"
	hint := ""
	if leaderAddr != "" {
		msg = fmt.Sprintf("not the leader: redirect writes to %s", leaderAddr)
		hint = fmt.Sprintf("asql_leader=%s", leaderAddr)
	}
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "25006",
		Message:  msg,
		Hint:     hint,
	})
	_ = backend.Flush()
}

// ── Column description helper ────────────────────────────────────────────────

// returningColumns parses the column names after a RETURNING keyword in an
// insert-focused statement. Returns nil if no RETURNING clause is found.
func returningColumns(sql string) []string {
	upper := strings.ToUpper(sql)
	idx := strings.LastIndex(upper, " RETURNING ")
	if idx == -1 {
		// Also handle trailing RETURNING without trailing space
		if strings.HasSuffix(strings.TrimRight(upper, " \t\n\r"), " RETURNING") {
			return []string{"*"}
		}
		return nil
	}
	clause := strings.TrimSpace(sql[idx+len(" RETURNING "):])
	if clause == "" {
		return nil
	}
	// Split by comma, trim each name
	parts := strings.Split(clause, ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		name := strings.TrimSpace(p)
		// Strip any table qualifier (tbl.col → col)
		if dot := strings.LastIndex(name, "."); dot != -1 {
			name = name[dot+1:]
		}
		// Strip alias (col AS alias → col)
		if i := strings.Index(strings.ToUpper(name), " AS "); i != -1 {
			name = strings.TrimSpace(name[i+4:])
		}
		name = strings.ToLower(strings.Trim(name, `"`))
		if name != "" {
			cols = append(cols, name)
		}
	}
	if len(cols) == 0 {
		return nil
	}
	return cols
}

func (server *Server) returningResultColumns(sql string) []string {
	trimmed := stripAsOfComment(strings.TrimSpace(sql))
	normalized := normalizePlaceholders(trimmed)
	stmt, err := parser.Parse(normalized)
	if err != nil {
		return returningColumns(sql)
	}
	ins, ok := stmt.(ast.InsertStatement)
	if !ok || len(ins.ReturningColumns) == 0 {
		return returningColumns(sql)
	}
	if len(ins.ReturningColumns) == 1 && ins.ReturningColumns[0] == "*" {
		return server.resolveTableColumns(ins.TableName)
	}
	cols := make([]string, 0, len(ins.ReturningColumns))
	for _, c := range ins.ReturningColumns {
		col := strings.ToLower(strings.TrimSpace(c))
		if col != "" {
			cols = append(cols, col)
		}
	}
	if len(cols) == 0 {
		return returningColumns(sql)
	}
	return cols
}

func (server *Server) describeReturningFields(sql string) []pgproto3.FieldDescription {
	trimmed := stripAsOfComment(strings.TrimSpace(sql))
	normalized := normalizePlaceholders(trimmed)
	stmt, err := parser.Parse(normalized)
	if err != nil {
		return nil
	}
	ins, ok := stmt.(ast.InsertStatement)
	if !ok || len(ins.ReturningColumns) == 0 {
		return nil
	}
	cols := server.returningResultColumns(sql)
	if len(cols) == 0 {
		return nil
	}
	oids := server.resolveColumnOIDs(ins.TableName, cols)
	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, c := range cols {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(c),
			TableAttributeNumber: 0,
			DataTypeOID:          oids[c],
			DataTypeSize:         -1,
			TypeModifier:         -1,
		}
	}
	return fields
}

// describeFields returns the FieldDescriptions for a SELECT query (all columns
// typed as text for now; OIDs are refined later by the type-inference logic in
// inferColumnTypeOIDs when rows are actually returned).
func (server *Server) describeFields(sql string, activeDomains []string, principal string) []pgproto3.FieldDescription {
	// Strip AS-OF comments (/* as-of-lsn: N */ / /* as-of-ts: N */) before
	// parsing: the SQL stored in prepared statements carries the appended
	// comment token, which our parser does not handle and would return an
	// error, causing describeFields to return nil and the Describe phase to
	// send NoData — making pgx treat every DataRow as column-less.
	trimmed := stripAsOfComment(strings.TrimSpace(sql))

	// Strip IMPORT directives (e.g. "IMPORT domain.table AS alias;").
	// Without this, IMPORT+SELECT queries begin with "IMPORT" rather than
	// "SELECT"/"WITH", bypass the early guard below, and return nil.  The
	// Describe phase then sends NoData, pgx sees 0 columns in RowDescription,
	// and every DataRow is decoded as an empty map — correct row count but
	// all values missing ("100 resultados pero vacíos").
	var importedTables map[string]string // lowercase alias → "domain.table"
	if strings.HasPrefix(strings.ToUpper(trimmed), "IMPORT ") {
		imports, selectPart, err := parser.ExtractImports(trimmed)
		if err != nil || strings.TrimSpace(selectPart) == "" {
			return nil
		}
		importedTables = make(map[string]string, len(imports))
		for _, imp := range imports {
			alias := imp.Alias
			if alias == "" {
				alias = imp.SourceTable
			}
			importedTables[strings.ToLower(alias)] = strings.ToLower(imp.SourceDomain) + "." + strings.ToLower(imp.SourceTable)
		}
		trimmed = strings.TrimSpace(selectPart)
	}

	// SHOW <param> produces a single-column result set.
	if paramName, ok := parseShowStatement(trimmed); ok {
		return []pgproto3.FieldDescription{{
			Name:         []byte(paramName),
			DataTypeOID:  25, // text
			DataTypeSize: -1,
			TypeModifier: -1,
		}}
	}
	if _, ok, err := parseTailEntityChangesStatement(trimmed); ok && err == nil {
		return tailEntityChangesFields()
	}

	if _, isExplain := stripExplainSQLPrefix(trimmed); isExplain {
		fields := make([]pgproto3.FieldDescription, len(explainResultColumns))
		for i, column := range explainResultColumns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(column),
				TableAttributeNumber: 0,
				DataTypeOID:          25,
				DataTypeSize:         -1,
				TypeModifier:         -1,
			}
		}
		return fields
	}

	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") {
		return nil
	}

	// For catalog / virtual-table queries, intercept now so we can derive
	// accurate OIDs from the real row data rather than defaulting everything
	// to text (OID 25).
	if err := server.authorizeCatalogQuery(principal, trimmed, false); err != nil {
		return nil
	}
	if intercepted, ok := server.interceptCatalog(context.Background(), trimmed, activeDomains, principal); ok {
		if len(intercepted.columns) == 0 {
			return nil
		}
		oids := inferColumnTypeOIDs(intercepted.columns, intercepted.result.Rows)
		fields := make([]pgproto3.FieldDescription, len(intercepted.columns))
		for i, c := range intercepted.columns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(c),
				TableAttributeNumber: 0,
				DataTypeOID:          oids[c],
				DataTypeSize:         -1,
				TypeModifier:         -1,
			}
		}
		return fields
	}

	// Replace $n placeholders with literal 0 so the parser can process the
	// query structure without failing on unknown placeholder tokens.
	normalized := normalizePlaceholders(trimmed)

	stmt, err := parser.Parse(normalized)
	if err != nil {
		return nil
	}
	sel, ok := stmt.(ast.SelectStatement)
	if !ok {
		return nil
	}

	described := server.describeSelectColumns(sel, importedTables)
	cols := make([]string, len(described))
	colOIDs := make(map[string]uint32, len(described))
	for i, desc := range described {
		cols[i] = desc.Name
		colOIDs[desc.Name] = desc.OID
	}

	if len(cols) == 0 {
		return []pgproto3.FieldDescription{
			{Name: []byte("?column?"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1},
		}
	}

	// For FOR HISTORY queries: the extended-query Describe and Execute phases
	// are independent code paths.  Describe uses schema-derived OIDs while
	// Execute uses actual row data from RowHistory.  If a history row is
	// missing columns that exist in the schema (e.g. columns added after the
	// original INSERT), the column sets differ and positional OID assignment
	// breaks — a TEXT value ends up decoded with an INT OID.  Avoid the
	// mismatch by using only text (25) for user columns and int8 (20) for
	// __commit_lsn, which is always present and always numeric.
	if sel.ForHistory {
		fields := make([]pgproto3.FieldDescription, len(cols))
		for i, c := range cols {
			oid := uint32(25) // text
			if c == executor.HistoryCommitLSNColumnName {
				oid = 20 // int8
			}
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(c),
				TableAttributeNumber: 0,
				DataTypeOID:          oid,
				DataTypeSize:         -1,
				TypeModifier:         -1,
			}
		}
		return fields
	}

	fields := make([]pgproto3.FieldDescription, len(cols))
	for i, c := range cols {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(c),
			TableAttributeNumber: 0,
			DataTypeOID:          colOIDs[c],
			DataTypeSize:         -1,
			TypeModifier:         -1,
		}
	}
	return fields
}

type describedSelectColumn struct {
	Name string
	OID  uint32
}

type describedSource struct {
	Columns []string
	OIDs    map[string]uint32
}

func (server *Server) describeSelectColumns(sel ast.SelectStatement, importedTables map[string]string) []describedSelectColumn {
	cteMap := make(map[string]ast.SelectStatement, len(sel.CTEs))
	for _, cte := range sel.CTEs {
		cteMap[strings.ToLower(cte.Name)] = cte.Statement
	}

	visiting := make(map[string]bool)
	sources, baseKey := server.describeSelectSources(sel, cteMap, importedTables, visiting)
	windowAliases := make(map[string]ast.WindowFunction, len(sel.WindowFunctions))
	for _, wf := range sel.WindowFunctions {
		windowAliases[strings.ToLower(wf.Alias)] = wf
	}
	jsonAliases := make(map[string]ast.JsonAccess, len(sel.JsonAccessColumns))
	for _, ja := range sel.JsonAccessColumns {
		jsonAliases[strings.ToLower(ja.Alias)] = ja
	}
	caseAliases := make(map[string]struct{}, len(sel.CaseWhenColumns))
	for _, cw := range sel.CaseWhenColumns {
		caseAliases[strings.ToLower(cw.Alias)] = struct{}{}
	}

	result := make([]describedSelectColumn, 0, len(sel.Columns)+2)
	for _, raw := range sel.Columns {
		expr, displayName := splitDescribedColumnAlias(raw)
		column := strings.TrimSpace(strings.ToLower(expr))
		name := strings.TrimSpace(strings.ToLower(displayName))
		if column == "" || name == "" {
			continue
		}
		if column == "*" {
			if source, ok := sources[baseKey]; ok {
				for _, name := range source.Columns {
					result = append(result, describedSelectColumn{Name: name, OID: source.OIDs[name]})
				}
			}
			continue
		}
		if prefix, ok := parseQualifiedStarName(column); ok {
			if source, found := sources[prefix]; found {
				for _, name := range source.Columns {
					result = append(result, describedSelectColumn{Name: name, OID: source.OIDs[name]})
				}
			}
			continue
		}

		oid := uint32(25)
		if wf, ok := windowAliases[column]; ok {
			oid = inferWindowFunctionOID(wf, sources, baseKey)
		} else if ja, ok := jsonAliases[column]; ok {
			if ja.TextMode {
				oid = 25
			} else {
				oid = 114
			}
		} else if _, ok := caseAliases[column]; ok {
			oid = 25
		} else if resolvedOID, ok := resolveDescribedColumnOID(column, sources, baseKey); ok {
			oid = resolvedOID
		}
		result = append(result, describedSelectColumn{Name: name, OID: oid})
	}

	if sel.ForHistory && len(result) > 0 {
		result = append(result,
			describedSelectColumn{Name: executor.HistoryOperationColumnName, OID: 25},
			describedSelectColumn{Name: executor.HistoryCommitLSNColumnName, OID: 20},
		)
	}

	return result
}

func splitDescribedColumnAlias(column string) (expr string, alias string) {
	trimmed := strings.TrimSpace(column)
	if trimmed == "" {
		return "", ""
	}
	lower := strings.ToLower(trimmed)
	depth := 0
	inString := false
	lastAs := -1
	for i := 0; i < len(lower); i++ {
		ch := lower[i]
		if ch == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && i+4 <= len(lower) && lower[i:i+4] == " as " {
			lastAs = i
		}
	}
	if lastAs == -1 {
		return trimmed, trimmed
	}
	expr = strings.TrimSpace(trimmed[:lastAs])
	alias = strings.TrimSpace(trimmed[lastAs+4:])
	if expr == "" || alias == "" {
		return trimmed, trimmed
	}
	return expr, alias
}

func (server *Server) describeSelectSources(sel ast.SelectStatement, cteMap map[string]ast.SelectStatement, importedTables map[string]string, visiting map[string]bool) (map[string]describedSource, string) {
	sources := make(map[string]describedSource)
	baseKey := strings.ToLower(displaySourceKey(sel.TableName, sel.TableAlias))
	server.addDescribedSource(sources, sel.TableName, sel.TableAlias, cteMap, importedTables, visiting)
	for _, join := range sel.Joins {
		server.addDescribedSource(sources, join.TableName, join.Alias, cteMap, importedTables, visiting)
	}
	return sources, baseKey
}

func displaySourceKey(tableName, alias string) string {
	if strings.TrimSpace(alias) != "" {
		return strings.ToLower(strings.TrimSpace(alias))
	}
	return strings.ToLower(strings.TrimSpace(tableName))
}

func (server *Server) addDescribedSource(target map[string]describedSource, tableName, alias string, cteMap map[string]ast.SelectStatement, importedTables map[string]string, visiting map[string]bool) {
	source := server.resolveDescribedSource(tableName, cteMap, importedTables, visiting)
	if len(source.Columns) == 0 {
		return
	}
	key := displaySourceKey(tableName, alias)
	if key != "" {
		target[key] = source
	}
	tableKey := strings.ToLower(strings.TrimSpace(tableName))
	if tableKey != "" {
		target[tableKey] = source
	}
}

func (server *Server) resolveDescribedSource(tableName string, cteMap map[string]ast.SelectStatement, importedTables map[string]string, visiting map[string]bool) describedSource {
	canonical := strings.ToLower(strings.TrimSpace(tableName))
	if canonical == "" {
		return describedSource{}
	}
	if stmt, ok := cteMap[canonical]; ok {
		if visiting[canonical] {
			return describedSource{}
		}
		visiting[canonical] = true
		defer delete(visiting, canonical)
		cols := server.describeSelectColumns(stmt, importedTables)
		columnNames := make([]string, 0, len(cols))
		oids := make(map[string]uint32, len(cols))
		for _, col := range cols {
			columnNames = append(columnNames, col.Name)
			oids[col.Name] = col.OID
		}
		sortColumns(columnNames)
		return describedSource{Columns: columnNames, OIDs: oids}
	}

	resolved := canonical
	if importedTables != nil {
		if physical, ok := importedTables[canonical]; ok {
			resolved = physical
		}
	}
	cols := server.resolveTableColumns(resolved)
	if len(cols) == 0 {
		return describedSource{}
	}
	sortColumns(cols)
	oids := server.resolveColumnOIDs(resolved, cols)
	return describedSource{Columns: cols, OIDs: oids}
}

func parseQualifiedStarName(column string) (string, bool) {
	if len(column) <= 2 || !strings.HasSuffix(column, ".*") {
		return "", false
	}
	prefix := strings.TrimSpace(strings.TrimSuffix(column, ".*"))
	if prefix == "" {
		return "", false
	}
	return strings.ToLower(prefix), true
}

func resolveDescribedColumnOID(column string, sources map[string]describedSource, baseKey string) (uint32, bool) {
	if parts := strings.SplitN(column, ".", 2); len(parts) == 2 {
		if source, ok := sources[parts[0]]; ok {
			oid, found := source.OIDs[parts[1]]
			return oid, found
		}
		return 25, false
	}
	if source, ok := sources[baseKey]; ok {
		if oid, found := source.OIDs[column]; found {
			return oid, true
		}
	}
	var resolved uint32
	var found bool
	for _, source := range sources {
		if oid, ok := source.OIDs[column]; ok {
			if found {
				return 25, false
			}
			resolved = oid
			found = true
		}
	}
	return resolved, found
}

func inferWindowFunctionOID(wf ast.WindowFunction, sources map[string]describedSource, baseKey string) uint32 {
	switch strings.ToUpper(strings.TrimSpace(wf.Function)) {
	case "ROW_NUMBER", "RANK":
		return 20
	case "LAG", "LEAD":
		if len(wf.Args) > 0 {
			if oid, ok := resolveDescribedColumnOID(strings.ToLower(strings.TrimSpace(wf.Args[0])), sources, baseKey); ok {
				return oid
			}
		}
	}
	return 25
}

// resolveColumnOIDs returns a map of lowercase column name → postgres OID by
// looking up the table schema.  Unknown columns default to OID 25 (text).
// Well-known system columns (_lsn, _operation) get their correct OIDs.
func (server *Server) resolveColumnOIDs(tableName string, cols []string) map[string]uint32 {
	result := make(map[string]uint32, len(cols))
	for _, c := range cols {
		switch c {
		case "_lsn":
			result[c] = 20 // int8
		default:
			result[c] = 25 // text default
		}
	}

	from := strings.ToLower(strings.TrimSpace(tableName))
	if from == "" {
		return result
	}
	parts := strings.SplitN(from, ".", 2)
	var domainName, tblName string
	if len(parts) == 2 {
		domainName = parts[0]
		tblName = parts[1]
	} else {
		tblName = parts[0]
	}

	snap := server.engine.SchemaSnapshot(nil)
	for _, d := range snap.Domains {
		if domainName != "" && strings.ToLower(d.Name) != domainName {
			continue
		}
		for _, t := range d.Tables {
			if strings.ToLower(t.Name) != tblName {
				continue
			}
			for _, col := range t.Columns {
				colLower := strings.ToLower(col.Name)
				if colLower == "_lsn" {
					result[colLower] = 20
					continue
				}
				if _, wanted := result[colLower]; wanted {
					result[colLower] = uint32(schemaTypeToOID(col.Type)) //nolint:gosec
				}
			}
			return result
		}
	}
	return result
}

// normalizePlaceholders replaces $1…$n with the literal value 0 so the SQL
// parser can process query structure without choking on placeholder tokens.
func normalizePlaceholders(sql string) string {
	result := sql
	for i := 20; i >= 1; i-- {
		result = strings.ReplaceAll(result, fmt.Sprintf("$%d", i), "0")
	}
	return result
}

// resolveStarColumns attempts to expand SELECT * by looking up the target
// table's columns in the engine's schema snapshot.  Columns are returned in
// sorted (alphabetical) order so that the RowDescription produced during the
// Describe phase matches the DataRow column order produced by columnsFromRows
// during Execute (which also sorts alphabetically).
func (server *Server) resolveStarColumns(sel ast.SelectStatement) []string {
	// SelectStatement.TableName is the table reference, e.g. "accounts.users"
	from := strings.ToLower(strings.TrimSpace(sel.TableName))
	if from == "" {
		return nil
	}

	parts := strings.SplitN(from, ".", 2)
	var domainName, tableName string
	if len(parts) == 2 {
		domainName = parts[0]
		tableName = parts[1]
	} else {
		tableName = parts[0]
	}

	snap := server.engine.SchemaSnapshot(nil)
	for _, d := range snap.Domains {
		if domainName != "" && strings.ToLower(d.Name) != domainName {
			continue
		}
		for _, t := range d.Tables {
			if strings.ToLower(t.Name) == tableName {
				cols := make([]string, 0, len(t.Columns))
				for _, c := range t.Columns {
					if isHiddenProjectionColumn(c.Name) {
						continue
					}
					cols = append(cols, strings.ToLower(c.Name))
				}
				sortColumns(cols)
				return cols
			}
		}
	}
	return nil
}

func (server *Server) resolveTableColumns(tableName string) []string {
	from := strings.ToLower(strings.TrimSpace(tableName))
	if from == "" {
		return nil
	}

	parts := strings.SplitN(from, ".", 2)
	var domainName, tblName string
	if len(parts) == 2 {
		domainName = parts[0]
		tblName = parts[1]
	} else {
		tblName = parts[0]
	}

	snap := server.engine.SchemaSnapshot(nil)
	for _, d := range snap.Domains {
		if domainName != "" && strings.ToLower(d.Name) != domainName {
			continue
		}
		for _, t := range d.Tables {
			if strings.ToLower(t.Name) != tblName {
				continue
			}
			cols := make([]string, 0, len(t.Columns))
			for _, c := range t.Columns {
				if isHiddenProjectionColumn(c.Name) {
					continue
				}
				cols = append(cols, strings.ToLower(c.Name))
			}
			return cols
		}
	}
	return nil
}

func isHiddenProjectionColumn(columnName string) bool {
	return strings.HasPrefix(strings.TrimSpace(columnName), "_")
}

// ── Parameter substitution ────────────────────────────────────────────────────

// substituteParams replaces $1…$n placeholders in sql with the values provided
// by the Bind message. Text-format parameters are supported broadly; binary
// format is intentionally narrow and currently only decodes int4/int8/bool.
func substituteParams(sql string, params [][]byte, formats []int16, paramOIDs []uint32) (string, error) {
	if len(params) == 0 {
		return sql, nil
	}

	result := sql
	// Work backwards so that $10 doesn't match $1.
	for i := len(params) - 1; i >= 0; i-- {
		placeholder := fmt.Sprintf("$%d", i+1)
		if !strings.Contains(result, placeholder) {
			continue
		}

		// Determine wire format for this parameter.
		var format int16
		if len(formats) == 1 {
			format = formats[0] // single format code applies to all
		} else if i < len(formats) {
			format = formats[i]
		}

		if format != 0 {
			// Binary format: we only decode numeric types; for others we fail.
			decoded, err := decodeBinaryParam(params[i], paramOIDFor(i, paramOIDs))
			if err != nil {
				return "", fmt.Errorf("parameter $%d: binary format not supported for OID %d", i+1, paramOIDFor(i, paramOIDs))
			}
			result = strings.ReplaceAll(result, placeholder, decoded)
			continue
		}

		val := string(params[i])
		result = strings.ReplaceAll(result, placeholder, quoteParam(val, paramOIDFor(i, paramOIDs)))
	}
	return result, nil
}

// paramOIDFor returns the OID hint for the i-th parameter (or 0 if not given).
func paramOIDFor(i int, oids []uint32) uint32 {
	if i < len(oids) {
		return oids[i]
	}
	return 0
}

// decodeBinaryParam attempts to decode a binary-format parameter for numeric types.
func decodeBinaryParam(data []byte, oid uint32) (string, error) {
	switch oid {
	case 23: // int4
		if len(data) != 4 {
			return "", fmt.Errorf("invalid int4 binary length %d", len(data))
		}
		n := int32(binary.BigEndian.Uint32(data))
		return strconv.FormatInt(int64(n), 10), nil
	case 20: // int8
		if len(data) != 8 {
			return "", fmt.Errorf("invalid int8 binary length %d", len(data))
		}
		n := int64(binary.BigEndian.Uint64(data))
		return strconv.FormatInt(n, 10), nil
	case 16: // bool
		if len(data) != 1 {
			return "", fmt.Errorf("invalid bool binary length %d", len(data))
		}
		if data[0] != 0 {
			return "true", nil
		}
		return "false", nil
	default:
		return "", fmt.Errorf("unsupported binary OID %d", oid)
	}
}

// quoteParam returns the appropriate SQL representation of val given the
// parameter OID hint.  When the OID suggests a numeric/boolean type the value
// is passed through; otherwise it is single-quoted.
func quoteParam(val string, oid uint32) string {
	switch oid {
	case 16: // bool
		return val
	case 20, 21, 23, 26, 700, 701, 1700: // int8, int2, int4, oid, float4, float8, numeric
		return val
	case 0: // unspecified — heuristic
		if looksLikeNumber(val) {
			return val
		}
		return quoteSQLString(val)
	default:
		return quoteSQLString(val)
	}
}

// looksLikeNumber returns true when val parses as an integer or float.
func looksLikeNumber(val string) bool {
	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(val, 64); err == nil {
		return true
	}
	return false
}

// quoteSQLString wraps val in single quotes, escaping internal single quotes.
func quoteSQLString(val string) string {
	return "'" + strings.ReplaceAll(val, "'", "''") + "'"
}
