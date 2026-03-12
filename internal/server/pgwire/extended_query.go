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
	"errors"
	"fmt"
	"log/slog"
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
	describedColumns []string
}

// ── Message handlers ─────────────────────────────────────────────────────────

// handleParse stores a prepared statement and responds with ParseComplete.
func (server *Server) handleParse(backend *pgproto3.Backend, state *connState, msg *pgproto3.Parse) {
	if state.errorPending {
		return
	}
	// Infer number of $n parameters from the SQL so ParameterDescription
	// contains the right count even when the client sends no OID hints.
	paramOIDs := msg.ParameterOIDs
	if len(paramOIDs) == 0 {
		paramOIDs = inferParamOIDs(msg.Query)
	}
	state.prepared[msg.Name] = preparedStmt{
		sql:       msg.Query,
		paramOIDs: paramOIDs,
	}
	backend.Send(&pgproto3.ParseComplete{})
	_ = backend.Flush()
}

// inferParamOIDs scans sql for the highest $n placeholder and returns a
// slice of that length filled with 0 (unspecified OID).
func inferParamOIDs(sql string) []uint32 {
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
	// INSERT/UPDATE/DELETE ... RETURNING also produces rows.
	if !isSelect {
		if cols := returningColumns(sql); len(cols) > 0 {
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
		if fields := server.describeFields(sql); fields != nil {
			describedColumns = make([]string, len(fields))
			for i, f := range fields {
				describedColumns[i] = strings.ToLower(string(f.Name))
			}
		}
	}

	state.portals[msg.DestinationPortal] = portal{
		sql:              sql,
		isSelect:         isSelect,
		columns:          columns,
		nextRow:          0,
		describedColumns: describedColumns,
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
		fields := server.describeFields(stmt.sql)
		// INSERT/UPDATE/DELETE ... RETURNING produces rows that describeFields
		// cannot detect (it only handles SELECT/WITH).  Fall back to the
		// RETURNING column parser so pgx receives a RowDescription instead of
		// NoData — without this, pgx treats the statement as non-row-returning
		// and QueryRow().Scan() fails with "no rows in result set".
		if fields == nil {
			if cols := returningColumns(stmt.sql); len(cols) > 0 {
				fields = make([]pgproto3.FieldDescription, len(cols))
				for i, c := range cols {
					fields[i] = pgproto3.FieldDescription{
						Name:                 []byte(c),
						TableAttributeNumber: uint16(i + 1),
						DataTypeOID:          25, // text
						DataTypeSize:         -1,
						TypeModifier:         -1,
					}
				}
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
			fields := server.describeFields(p.sql)
			if fields == nil && len(p.columns) > 0 {
				// INSERT...RETURNING or similar: build fields from the known column list.
				fields = make([]pgproto3.FieldDescription, len(p.columns))
				for i, c := range p.columns {
					fields[i] = pgproto3.FieldDescription{
						Name:                 []byte(c),
						TableAttributeNumber: uint16(i + 1),
						DataTypeOID:          25, // text; refined when rows arrive
						DataTypeSize:         -1,
						TypeModifier:         -1,
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
	if intercepted, ok := server.interceptCatalog(ctx, p.sql); ok {
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
// INSERT/UPDATE/DELETE statement.  Returns nil if no RETURNING clause found.
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

// describeFields returns the FieldDescriptions for a SELECT query (all columns
// typed as text for now; OIDs are refined later by the type-inference logic in
// inferColumnTypeOIDs when rows are actually returned).
func (server *Server) describeFields(sql string) []pgproto3.FieldDescription {
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

	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") {
		return nil
	}

	// For catalog / virtual-table queries, intercept now so we can derive
	// accurate OIDs from the real row data rather than defaulting everything
	// to text (OID 25).
	if intercepted, ok := server.interceptCatalog(context.Background(), trimmed); ok {
		if len(intercepted.columns) == 0 {
			return nil
		}
		oids := inferColumnTypeOIDs(intercepted.columns, intercepted.result.Rows)
		fields := make([]pgproto3.FieldDescription, len(intercepted.columns))
		for i, c := range intercepted.columns {
			fields[i] = pgproto3.FieldDescription{
				Name:                 []byte(c),
				TableAttributeNumber: uint16(i + 1),
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

	// Explicit column list from AST.
	cols := make([]string, 0, len(sel.Columns))
	for _, c := range sel.Columns {
		canonical := strings.TrimSpace(strings.ToLower(c))
		if canonical == "" || canonical == "*" {
			continue
		}
		cols = append(cols, canonical)
	}

	// If SELECT * — try schema lookup to enumerate the table's actual columns.
	// When the table name is an IMPORT alias, resolve it to the real
	// domain.table so the schema snapshot lookup succeeds.
	if len(cols) == 0 && len(sel.Columns) > 0 {
		lookupSel := sel
		if importedTables != nil {
			if resolved, ok := importedTables[strings.ToLower(sel.TableName)]; ok {
				lookupSel.TableName = resolved
			}
		}
		cols = server.resolveStarColumns(lookupSel)
		// FOR HISTORY adds canonical metadata columns to every result row; include
		// them in the RowDescription so the column count matches the DataRows.
		if sel.ForHistory && len(cols) > 0 {
			cols = append(cols, executor.HistoryOperationColumnName, executor.HistoryCommitLSNColumnName)
			sortColumns(cols)
		}
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
				TableAttributeNumber: uint16(i + 1),
				DataTypeOID:          oid,
				DataTypeSize:         -1,
				TypeModifier:         -1,
			}
		}
		return fields
	}

	fields := make([]pgproto3.FieldDescription, len(cols))
	// Try schema-aware type OIDs; fall back to text (25) for unknowns.
	// Resolve import aliases to the real domain.table for OID lookup.
	resolvedTableName := sel.TableName
	if importedTables != nil {
		if resolved, ok := importedTables[strings.ToLower(sel.TableName)]; ok {
			resolvedTableName = resolved
		}
	}
	colOIDs := server.resolveColumnOIDs(resolvedTableName, cols)
	for i, c := range cols {
		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(c),
			TableAttributeNumber: uint16(i + 1),
			DataTypeOID:          colOIDs[c],
			DataTypeSize:         -1,
			TypeModifier:         -1,
		}
	}
	return fields
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
					cols = append(cols, strings.ToLower(c.Name))
				}
				sortColumns(cols)
				return cols
			}
		}
	}
	return nil
}

// ── Parameter substitution ────────────────────────────────────────────────────

// substituteParams replaces $1…$n placeholders in sql with the values provided
// by the Bind message.  Only text-format parameters are supported; binary
// format returns an error.
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
	case 23, 20: // int4, int8
		var n int64
		for _, b := range data {
			n = n<<8 | int64(b)
		}
		return strconv.FormatInt(n, 10), nil
	case 16: // bool
		if len(data) == 1 && data[0] != 0 {
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
