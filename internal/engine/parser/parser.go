package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"asql/internal/engine/parser/ast"
)

var (
	errUnsupportedSQL = errors.New("unsupported sql statement")
	errInvalidSQL     = errors.New("invalid sql statement")
)

// hasPrefixFold checks if s starts with prefix, case-insensitively,
// without allocating a new uppercase copy of the entire string.
func hasPrefixFold(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return strings.EqualFold(s[:len(prefix)], prefix)
}

// indexFold returns the index of the first case-insensitive occurrence of
// substr in s, or -1 if not present. substr MUST be all-uppercase ASCII.
// This avoids allocating an uppercase copy of s.
func indexFold(s, substr string) int {
	n := len(substr)
	if n == 0 {
		return 0
	}
	if n > len(s) {
		return -1
	}
	for i := 0; i <= len(s)-n; i++ {
		if strings.EqualFold(s[i:i+n], substr) {
			return i
		}
	}
	return -1
}

// containsFold returns true if s contains substr case-insensitively.
// substr MUST be all-uppercase ASCII.
func containsFold(s, substr string) bool {
	return indexFold(s, substr) >= 0
}

// toUpperSmall uppercases a short token (≤ ~64 bytes). For small strings
// the allocation cost is negligible and this is simpler than case-folding.
func toUpperSmall(s string) string {
	return strings.ToUpper(s)
}

// Parse parses a minimal SQL subset into an AST statement.
func Parse(sql string) (ast.Statement, error) {
	trimmed := normalizeSQL(sql)
	if err := unsupportedSQLGuidance(trimmed); err != nil {
		return nil, err
	}

	// Use case-insensitive prefix matching instead of uppercasing the
	// entire SQL string. INSERT statements can be very long and the old
	// strings.ToUpper(trimmed) was spending >4s just uppercasing values.
	switch {
	case hasPrefixFold(trimmed, "WITH "):
		return parseWithCTE(trimmed)
	case hasPrefixFold(trimmed, "BEGIN CROSS DOMAIN "):
		return parseBeginCrossDomain(trimmed)
	case hasPrefixFold(trimmed, "CREATE TABLE IF NOT EXISTS "):
		return parseCreateTable(trimmed, true)
	case hasPrefixFold(trimmed, "CREATE TABLE "):
		return parseCreateTable(trimmed, false)
	case hasPrefixFold(trimmed, "ALTER TABLE "):
		return parseAlterTable(trimmed)
	case hasPrefixFold(trimmed, "CREATE INDEX IF NOT EXISTS "):
		return parseCreateIndex(trimmed, true)
	case hasPrefixFold(trimmed, "CREATE INDEX "):
		return parseCreateIndex(trimmed, false)
	case hasPrefixFold(trimmed, "CREATE ENTITY IF NOT EXISTS "):
		return parseCreateEntity(trimmed, true)
	case hasPrefixFold(trimmed, "CREATE ENTITY "):
		return parseCreateEntity(trimmed, false)
	case hasPrefixFold(trimmed, "DROP TABLE IF EXISTS "):
		return parseDropTable(trimmed, true)
	case hasPrefixFold(trimmed, "DROP TABLE "):
		return parseDropTable(trimmed, false)
	case hasPrefixFold(trimmed, "DROP INDEX IF EXISTS "):
		return parseDropIndex(trimmed, true)
	case hasPrefixFold(trimmed, "DROP INDEX "):
		return parseDropIndex(trimmed, false)
	case hasPrefixFold(trimmed, "TRUNCATE TABLE "):
		return parseTruncateTable(trimmed)
	case hasPrefixFold(trimmed, "TRUNCATE "):
		return parseTruncateTable(trimmed)
	case hasPrefixFold(trimmed, "INSERT INTO "):
		return parseInsert(trimmed)
	case hasPrefixFold(trimmed, "UPDATE "):
		return parseUpdate(trimmed)
	case hasPrefixFold(trimmed, "DELETE FROM "):
		return parseDelete(trimmed)
	case hasPrefixFold(trimmed, "SELECT "):
		// Check for top-level set operations (UNION, INTERSECT, EXCEPT).
		if op, pos := findTopLevelSetOp(trimmed); pos >= 0 {
			return parseSetOperation(trimmed, op, pos)
		}
		return parseSelect(trimmed)
	default:
		return nil, errUnsupportedSQL
	}
}

func unsupportedSQLGuidance(sql string) error {
	trimmed := strings.TrimSpace(sql)
	switch {
	case strings.EqualFold(trimmed, "BEGIN") || strings.EqualFold(trimmed, "BEGIN;"):
		return fmt.Errorf("%w: use BEGIN DOMAIN <name> or BEGIN CROSS DOMAIN <a>, <b>", errUnsupportedSQL)
	case hasPrefixFold(trimmed, "START TRANSACTION"):
		return fmt.Errorf("%w: use BEGIN DOMAIN <name> or BEGIN CROSS DOMAIN <a>, <b> instead of START TRANSACTION", errUnsupportedSQL)
	case containsFold(trimmed, "ANY(") || containsFold(trimmed, " ANY ("):
		return fmt.Errorf("%w: ANY(...) predicates are not supported; use IN (...) for literal lists or IN (SELECT ...) for subqueries", errUnsupportedSQL)
	case containsFold(trimmed, "ARRAY[") || containsFold(trimmed, "ARRAY ["):
		return fmt.Errorf("%w: PostgreSQL array literals are not supported; use IN (...) / IN (SELECT ...) or model collections with JSON/rows", errUnsupportedSQL)
	default:
		return nil
	}
}

// ParsePredicate parses a predicate expression string into an AST Predicate.
// This is used by the executor to evaluate CASE WHEN branch conditions.
func ParsePredicate(expression string) (*ast.Predicate, error) {
	return parseWhere(expression)
}

func parseWithCTE(sql string) (ast.Statement, error) {
	rest := strings.TrimSpace(sql[len("WITH "):])
	ctes := make([]ast.CTE, 0)

	for {
		// Parse: name AS (SELECT ...)

		// Find "AS" keyword
		asIndex := indexFold(rest, " AS ")
		if asIndex <= 0 {
			return nil, fmt.Errorf("%w: CTE requires name AS (SELECT ...)", errInvalidSQL)
		}

		cteName := canonicalIdentifier(strings.TrimSpace(rest[:asIndex]))
		if cteName == "" {
			return nil, fmt.Errorf("%w: CTE name required", errInvalidSQL)
		}

		afterAS := strings.TrimSpace(rest[asIndex+len(" AS "):])
		if len(afterAS) == 0 || afterAS[0] != '(' {
			return nil, fmt.Errorf("%w: CTE body must be parenthesized", errInvalidSQL)
		}

		// Find matching closing paren
		closeIndex := findMatchingParen(afterAS, 0)
		if closeIndex == -1 {
			return nil, fmt.Errorf("%w: unmatched parenthesis in CTE", errInvalidSQL)
		}

		innerSQL := strings.TrimSpace(afterAS[1:closeIndex])
		innerStmt, err := Parse(innerSQL)
		if err != nil {
			return nil, fmt.Errorf("CTE %q parse error: %w", cteName, err)
		}

		innerSelect, ok := innerStmt.(ast.SelectStatement)
		if !ok {
			return nil, fmt.Errorf("%w: CTE body must be a SELECT statement", errInvalidSQL)
		}

		ctes = append(ctes, ast.CTE{Name: cteName, Statement: innerSelect})

		// After the closing paren, check for comma (more CTEs) or SELECT
		rest = strings.TrimSpace(afterAS[closeIndex+1:])
		if len(rest) == 0 {
			return nil, fmt.Errorf("%w: CTE requires a trailing SELECT statement", errInvalidSQL)
		}

		if rest[0] == ',' {
			rest = strings.TrimSpace(rest[1:])
			continue
		}

		break
	}

	// The rest must be a SELECT statement
	if !hasPrefixFold(rest, "SELECT ") {
		return nil, fmt.Errorf("%w: CTE must be followed by a SELECT statement", errInvalidSQL)
	}

	selectStmt, err := parseSelect(rest)
	if err != nil {
		return nil, err
	}

	sel := selectStmt.(ast.SelectStatement)
	sel.CTEs = ctes
	return sel, nil
}

// findMatchingParen finds the index of the closing paren matching the open paren at position start.
func findMatchingParen(s string, start int) int {
	if start >= len(s) || s[start] != '(' {
		return -1
	}
	depth := 0
	inString := false
	for i := start; i < len(s); i++ {
		ch := s[i]
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
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func parseAlterTable(sql string) (ast.Statement, error) {
	rest := strings.TrimSpace(sql[len("ALTER TABLE "):])

	// DROP COLUMN
	dropColumnIndex := indexFold(rest, " DROP COLUMN ")
	if dropColumnIndex > 0 {
		table := canonicalIdentifier(strings.TrimSpace(rest[:dropColumnIndex]))
		if table == "" {
			return nil, fmt.Errorf("%w: table name required", errInvalidSQL)
		}
		columnName := canonicalIdentifier(strings.TrimSpace(rest[dropColumnIndex+len(" DROP COLUMN "):]))
		if columnName == "" {
			return nil, fmt.Errorf("%w: column name required for DROP COLUMN", errInvalidSQL)
		}
		return ast.AlterTableDropColumnStatement{
			TableName:  table,
			ColumnName: columnName,
		}, nil
	}

	// RENAME COLUMN old TO new
	renameColumnIndex := indexFold(rest, " RENAME COLUMN ")
	if renameColumnIndex > 0 {
		table := canonicalIdentifier(strings.TrimSpace(rest[:renameColumnIndex]))
		if table == "" {
			return nil, fmt.Errorf("%w: table name required", errInvalidSQL)
		}
		renameSpec := strings.TrimSpace(rest[renameColumnIndex+len(" RENAME COLUMN "):])
		toIndex := indexFold(renameSpec, " TO ")
		if toIndex <= 0 {
			return nil, fmt.Errorf("%w: RENAME COLUMN requires old TO new syntax", errInvalidSQL)
		}
		oldName := canonicalIdentifier(strings.TrimSpace(renameSpec[:toIndex]))
		newName := canonicalIdentifier(strings.TrimSpace(renameSpec[toIndex+len(" TO "):]))
		if oldName == "" || newName == "" {
			return nil, fmt.Errorf("%w: old and new column names required for RENAME COLUMN", errInvalidSQL)
		}
		return ast.AlterTableRenameColumnStatement{
			TableName: table,
			OldName:   oldName,
			NewName:   newName,
		}, nil
	}

	// ADD COLUMN (original)
	addColumnIndex := indexFold(rest, " ADD COLUMN ")
	if addColumnIndex <= 0 {
		return nil, fmt.Errorf("%w: ALTER TABLE requires ADD COLUMN, DROP COLUMN, or RENAME COLUMN clause", errInvalidSQL)
	}

	table := canonicalIdentifier(strings.TrimSpace(rest[:addColumnIndex]))
	if table == "" {
		return nil, fmt.Errorf("%w: table name required", errInvalidSQL)
	}

	columnSpec := strings.TrimSpace(rest[addColumnIndex+len(" ADD COLUMN "):])
	parts := strings.Fields(columnSpec)
	if len(parts) < 2 {
		return nil, fmt.Errorf("%w: ADD COLUMN requires name and type", errInvalidSQL)
	}

	columnName := canonicalIdentifier(parts[0])
	if columnName == "" {
		return nil, fmt.Errorf("%w: column name required", errInvalidSQL)
	}

	dataType, err := parseDataType(parts[1])
	if err != nil {
		return nil, err
	}

	column := ast.ColumnDefinition{
		Name: columnName,
		Type: dataType,
	}

	if err := parseOnlineSafeAddColumnSuffix(parts[2:], &column); err != nil {
		return nil, err
	}

	return ast.AlterTableAddColumnStatement{
		TableName: table,
		Column:    column,
	}, nil
}

func parseOnlineSafeAddColumnSuffix(suffix []string, column *ast.ColumnDefinition) error {
	if column == nil || len(suffix) == 0 {
		return nil
	}

	for index := 0; index < len(suffix); {
		token := strings.ToUpper(suffix[index])
		switch token {
		case "DEFAULT":
			if index+1 >= len(suffix) {
				return fmt.Errorf("%w: DEFAULT requires a value", errInvalidSQL)
			}
			defaultToken := strings.ToUpper(suffix[index+1])
			switch defaultToken {
			case "AUTOINCREMENT", "UUID_V7":
				return fmt.Errorf("%w: online-safe ADD COLUMN supports only literal DEFAULT values", errInvalidSQL)
			default:
				lit, err := parseLiteral(suffix[index+1])
				if err != nil {
					return fmt.Errorf("%w: invalid DEFAULT value %q", errInvalidSQL, suffix[index+1])
				}
				column.DefaultValue = &ast.DefaultExpr{Kind: ast.DefaultLiteral, Value: lit}
			}
			index += 2
		case "NOT":
			if index+1 >= len(suffix) || strings.ToUpper(suffix[index+1]) != "NULL" {
				return fmt.Errorf("%w: expected NOT NULL", errInvalidSQL)
			}
			column.NotNull = true
			index += 2
		default:
			return fmt.Errorf("%w: unsupported ADD COLUMN constraint %q in online-safe mode", errInvalidSQL, suffix[index])
		}
	}

	return nil
}

func parseDropTable(sql string, ifExists bool) (ast.Statement, error) {
	prefix := "DROP TABLE "
	if ifExists {
		prefix = "DROP TABLE IF EXISTS "
	}
	rest := strings.TrimSpace(sql[len(prefix):])

	cascade := false
	if len(rest) >= len(" CASCADE") && strings.EqualFold(rest[len(rest)-len(" CASCADE"):], " CASCADE") {
		cascade = true
		rest = strings.TrimSpace(rest[:len(rest)-len(" CASCADE")])
	}

	table := canonicalIdentifier(rest)
	if table == "" {
		return nil, fmt.Errorf("%w: DROP TABLE requires table name", errInvalidSQL)
	}

	return ast.DropTableStatement{
		TableName: table,
		IfExists:  ifExists,
		Cascade:   cascade,
	}, nil
}

func parseDropIndex(sql string, ifExists bool) (ast.Statement, error) {
	prefix := "DROP INDEX "
	if ifExists {
		prefix = "DROP INDEX IF EXISTS "
	}
	rest := strings.TrimSpace(sql[len(prefix):])

	// Support optional ON table syntax: DROP INDEX idx_name ON table_name
	upper := strings.ToUpper(rest)
	tableName := ""
	onIdx := strings.Index(upper, " ON ")
	if onIdx > 0 {
		tableName = canonicalIdentifier(strings.TrimSpace(rest[onIdx+len(" ON "):]))
		rest = strings.TrimSpace(rest[:onIdx])
	}

	indexName := canonicalIdentifier(rest)
	if indexName == "" {
		return nil, fmt.Errorf("%w: DROP INDEX requires index name", errInvalidSQL)
	}

	return ast.DropIndexStatement{
		IndexName: indexName,
		TableName: tableName,
		IfExists:  ifExists,
	}, nil
}

func parseTruncateTable(sql string) (ast.Statement, error) {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	var rest string
	if strings.HasPrefix(upper, "TRUNCATE TABLE ") {
		rest = strings.TrimSpace(sql[len("TRUNCATE TABLE "):])
	} else {
		rest = strings.TrimSpace(sql[len("TRUNCATE "):])
	}

	table := canonicalIdentifier(rest)
	if table == "" {
		return nil, fmt.Errorf("%w: TRUNCATE requires table name", errInvalidSQL)
	}

	return ast.TruncateTableStatement{
		TableName: table,
	}, nil
}

func parseBeginCrossDomain(sql string) (ast.Statement, error) {
	rest := strings.TrimSpace(sql[len("BEGIN CROSS DOMAIN "):])
	domainSpecs := splitCSV(rest)
	if len(domainSpecs) < 2 {
		return nil, fmt.Errorf("%w: at least two domains are required", errInvalidSQL)
	}

	domains := make([]string, 0, len(domainSpecs))
	for _, domain := range domainSpecs {
		canonical := canonicalIdentifier(domain)
		if canonical == "" {
			return nil, fmt.Errorf("%w: invalid domain name", errInvalidSQL)
		}
		domains = append(domains, canonical)
	}

	begin := ast.NewBeginCrossDomainStatement(domains)
	if len(begin.Domains) < 2 {
		return nil, fmt.Errorf("%w: at least two unique domains are required", errInvalidSQL)
	}

	return begin, nil
}

func parseCreateTable(sql string, ifNotExists bool) (ast.Statement, error) {
	prefix := "CREATE TABLE "
	if ifNotExists {
		prefix = "CREATE TABLE IF NOT EXISTS "
	}
	rest := strings.TrimSpace(sql[len(prefix):])
	open := strings.Index(rest, "(")
	close := strings.LastIndex(rest, ")")
	if open <= 0 || close <= open {
		return nil, fmt.Errorf("%w: create table columns clause", errInvalidSQL)
	}

	table := canonicalIdentifier(strings.TrimSpace(rest[:open]))
	if table == "" {
		return nil, fmt.Errorf("%w: table name required", errInvalidSQL)
	}

	columnClause := rest[open+1 : close]
	columnSpecs := splitCSVDepthAware(columnClause)
	if len(columnSpecs) == 0 {
		return nil, fmt.Errorf("%w: at least one column required", errInvalidSQL)
	}

	columns := make([]ast.ColumnDefinition, 0, len(columnSpecs))
	var versionedFKs []ast.VersionedForeignKey
	primaryKeys := 0
	for _, spec := range columnSpecs {
		parts := strings.Fields(spec)
		if len(parts) < 2 {
			return nil, fmt.Errorf("%w: invalid column definition %q", errInvalidSQL, spec)
		}

		// Detect table-level VERSIONED FOREIGN KEY constraint.
		if strings.ToUpper(parts[0]) == "VERSIONED" {
			vfk, err := parseVersionedForeignKey(parts)
			if err != nil {
				return nil, err
			}
			versionedFKs = append(versionedFKs, vfk)
			continue
		}

		dataType, err := parseDataType(parts[1])
		if err != nil {
			return nil, err
		}

		column := ast.ColumnDefinition{
			Name: canonicalIdentifier(parts[0]),
			Type: dataType,
		}

		suffix := parts[2:]
		for index := 0; index < len(suffix); {
			token := strings.ToUpper(suffix[index])
			switch token {
			case "UNIQUE":
				column.Unique = true
				index++
			case "NOT":
				if index+1 >= len(suffix) || strings.ToUpper(suffix[index+1]) != "NULL" {
					return nil, fmt.Errorf("%w: expected NOT NULL", errInvalidSQL)
				}
				column.NotNull = true
				index += 2
			case "PRIMARY":
				if index+1 >= len(suffix) || strings.ToUpper(suffix[index+1]) != "KEY" {
					return nil, fmt.Errorf("%w: invalid PRIMARY KEY constraint", errInvalidSQL)
				}
				column.PrimaryKey = true
				column.Unique = true
				column.NotNull = true
				primaryKeys++
				index += 2
			case "REFERENCES":
				if index+1 >= len(suffix) {
					return nil, fmt.Errorf("%w: REFERENCES target required", errInvalidSQL)
				}

				referencesTable, referencesColumn, err := parseReferencesTarget(suffix[index+1])
				if err != nil {
					return nil, err
				}
				column.ReferencesTable = referencesTable
				column.ReferencesColumn = referencesColumn
				index += 2
			case "DEFAULT":
				if index+1 >= len(suffix) {
					return nil, fmt.Errorf("%w: DEFAULT requires a value", errInvalidSQL)
				}
				defaultToken := strings.ToUpper(suffix[index+1])
				switch defaultToken {
				case "AUTOINCREMENT":
					column.DefaultValue = &ast.DefaultExpr{Kind: ast.DefaultAutoIncrement}
				case "UUID_V7":
					column.DefaultValue = &ast.DefaultExpr{Kind: ast.DefaultUUIDv7}
				default:
					lit, litErr := parseLiteral(suffix[index+1])
					if litErr != nil {
						return nil, fmt.Errorf("%w: invalid DEFAULT value %q", errInvalidSQL, suffix[index+1])
					}
					column.DefaultValue = &ast.DefaultExpr{Kind: ast.DefaultLiteral, Value: lit}
				}
				index += 2
			case "CHECK":
				if index+1 >= len(suffix) {
					return nil, fmt.Errorf("%w: CHECK predicate required", errInvalidSQL)
				}

				rawPredicate := strings.Join(suffix[index+1:], " ")
				trimmedPredicate := strings.TrimSpace(rawPredicate)
				if !strings.HasPrefix(trimmedPredicate, "(") || !strings.HasSuffix(trimmedPredicate, ")") {
					return nil, fmt.Errorf("%w: CHECK requires parenthesized predicate", errInvalidSQL)
				}

				inner := strings.TrimSpace(trimmedPredicate[1 : len(trimmedPredicate)-1])
				if inner == "" {
					return nil, fmt.Errorf("%w: CHECK predicate required", errInvalidSQL)
				}

				parsedCheck, err := parseWhere(inner)
				if err != nil {
					return nil, err
				}
				column.Check = parsedCheck
				index = len(suffix)
			default:
				return nil, fmt.Errorf("%w: unsupported column constraint %q", errInvalidSQL, suffix[index])
			}
		}

		if column.Name == "" {
			return nil, fmt.Errorf("%w: column name required", errInvalidSQL)
		}

		columns = append(columns, column)
	}

	if primaryKeys > 1 {
		return nil, fmt.Errorf("%w: only one PRIMARY KEY column is supported", errInvalidSQL)
	}

	stmt := ast.CreateTableStatement{
		TableName:            table,
		Columns:              columns,
		VersionedForeignKeys: versionedFKs,
		IfNotExists:          ifNotExists,
	}
	return stmt, nil
}

// parseVersionedForeignKey parses a VERSIONED FOREIGN KEY table-level constraint.
// Expected tokens: VERSIONED FOREIGN KEY (col) REFERENCES [domain.]table(col) AS OF lsn_col
func parseVersionedForeignKey(parts []string) (ast.VersionedForeignKey, error) {
	// Minimum: VERSIONED FOREIGN KEY (col) REFERENCES target(col) AS OF lsn_col = 9 tokens
	// But (col) may be attached or separate — normalise via rejoin.
	raw := strings.Join(parts, " ")

	// Validate prefix: VERSIONED FOREIGN KEY
	if !hasPrefixFold(raw, "VERSIONED FOREIGN KEY ") {
		return ast.VersionedForeignKey{}, fmt.Errorf("%w: expected VERSIONED FOREIGN KEY", errInvalidSQL)
	}
	after := strings.TrimSpace(raw[len("VERSIONED FOREIGN KEY "):])

	// Extract FK column name from (col)
	if len(after) == 0 || after[0] != '(' {
		return ast.VersionedForeignKey{}, fmt.Errorf("%w: VERSIONED FOREIGN KEY requires (column)", errInvalidSQL)
	}
	closeParen := strings.Index(after, ")")
	if closeParen <= 1 {
		return ast.VersionedForeignKey{}, fmt.Errorf("%w: VERSIONED FOREIGN KEY requires (column)", errInvalidSQL)
	}
	fkColumn := canonicalIdentifier(after[1:closeParen])
	if fkColumn == "" {
		return ast.VersionedForeignKey{}, fmt.Errorf("%w: VERSIONED FOREIGN KEY column name required", errInvalidSQL)
	}
	after = strings.TrimSpace(after[closeParen+1:])

	// Expect REFERENCES
	if !hasPrefixFold(after, "REFERENCES ") {
		return ast.VersionedForeignKey{}, fmt.Errorf("%w: VERSIONED FOREIGN KEY requires REFERENCES clause", errInvalidSQL)
	}
	after = strings.TrimSpace(after[len("REFERENCES "):])

	// Find AS OF to split the references target from the lsn column
	asOfIdx := indexFold(after, " AS OF ")
	if asOfIdx <= 0 {
		return ast.VersionedForeignKey{}, fmt.Errorf("%w: VERSIONED FOREIGN KEY requires AS OF clause", errInvalidSQL)
	}

	refTarget := strings.TrimSpace(after[:asOfIdx])
	lsnColumn := canonicalIdentifier(strings.TrimSpace(after[asOfIdx+len(" AS OF "):]))
	if lsnColumn == "" {
		return ast.VersionedForeignKey{}, fmt.Errorf("%w: AS OF requires a column name", errInvalidSQL)
	}

	// Parse references target: [domain.]table(column)
	refDomain, refTable, refColumn, err := parseDomainReferencesTarget(refTarget)
	if err != nil {
		return ast.VersionedForeignKey{}, err
	}

	return ast.VersionedForeignKey{
		Column:           fkColumn,
		LSNColumn:        lsnColumn,
		ReferencesDomain: refDomain,
		ReferencesTable:  refTable,
		ReferencesColumn: refColumn,
	}, nil
}

// parseDomainReferencesTarget parses a references target that may include a domain prefix.
// Supports: "table(column)" → ("", table, column)
//
//	"domain.table(column)" → (domain, table, column)
func parseDomainReferencesTarget(target string) (string, string, string, error) {
	trimmed := strings.TrimSpace(target)
	open := strings.Index(trimmed, "(")
	close := strings.LastIndex(trimmed, ")")
	if open <= 0 || close <= open || close != len(trimmed)-1 {
		return "", "", "", fmt.Errorf("%w: invalid REFERENCES target %q", errInvalidSQL, target)
	}

	tableSpec := strings.TrimSpace(trimmed[:open])
	column := canonicalIdentifier(strings.TrimSpace(trimmed[open+1 : close]))
	if column == "" {
		return "", "", "", fmt.Errorf("%w: REFERENCES requires column", errInvalidSQL)
	}

	// Check for domain.table
	dotIdx := strings.Index(tableSpec, ".")
	if dotIdx > 0 {
		domain := canonicalIdentifier(tableSpec[:dotIdx])
		table := canonicalIdentifier(tableSpec[dotIdx+1:])
		if domain == "" || table == "" {
			return "", "", "", fmt.Errorf("%w: REFERENCES requires domain and table", errInvalidSQL)
		}
		return domain, table, column, nil
	}

	table := canonicalIdentifier(tableSpec)
	if table == "" {
		return "", "", "", fmt.Errorf("%w: REFERENCES requires table", errInvalidSQL)
	}
	return "", table, column, nil
}

func parseReferencesTarget(target string) (string, string, error) {
	trimmed := strings.TrimSpace(target)
	open := strings.Index(trimmed, "(")
	close := strings.LastIndex(trimmed, ")")
	if open <= 0 || close <= open || close != len(trimmed)-1 {
		return "", "", fmt.Errorf("%w: invalid REFERENCES target %q", errInvalidSQL, target)
	}

	table := canonicalIdentifier(strings.TrimSpace(trimmed[:open]))
	column := canonicalIdentifier(strings.TrimSpace(trimmed[open+1 : close]))
	if table == "" || column == "" {
		return "", "", fmt.Errorf("%w: REFERENCES requires table and column", errInvalidSQL)
	}

	return table, column, nil
}

func parseInsert(sql string) (ast.Statement, error) {
	rest := strings.TrimSpace(sql[len("INSERT INTO "):])
	firstOpen := strings.Index(rest, "(")
	firstClose := strings.Index(rest, ")")
	if firstOpen <= 0 || firstClose <= firstOpen {
		return nil, fmt.Errorf("%w: insert columns clause", errInvalidSQL)
	}

	table := canonicalIdentifier(strings.TrimSpace(rest[:firstOpen]))
	if table == "" {
		return nil, fmt.Errorf("%w: table name required", errInvalidSQL)
	}

	columnsClause := rest[firstOpen+1 : firstClose]
	columnsRaw := splitCSV(columnsClause)
	if len(columnsRaw) == 0 {
		return nil, fmt.Errorf("%w: insert requires columns", errInvalidSQL)
	}

	columns := make([]string, 0, len(columnsRaw))
	for _, column := range columnsRaw {
		columns = append(columns, canonicalIdentifier(column))
	}

	afterColumns := strings.TrimSpace(rest[firstClose+1:])
	if !hasPrefixFold(afterColumns, "VALUES") {
		return nil, fmt.Errorf("%w: VALUES clause required", errInvalidSQL)
	}

	valuesText := strings.TrimSpace(afterColumns[len("VALUES"):])
	open := strings.Index(valuesText, "(")
	if open != 0 {
		return nil, fmt.Errorf("%w: invalid values clause", errInvalidSQL)
	}

	// Parse one or more value groups: (v1,v2), (v3,v4), ...
	allValueGroups := make([][]ast.Literal, 0, 1)
	remaining := valuesText
	for {
		remaining = strings.TrimSpace(remaining)
		if len(remaining) == 0 || remaining[0] != '(' {
			break
		}
		closeIdx := findMatchingParen(remaining, 0)
		if closeIdx <= 0 {
			return nil, fmt.Errorf("%w: unmatched parenthesis in VALUES", errInvalidSQL)
		}

		valueSpecs := splitCSV(remaining[1:closeIdx])
		values := make([]ast.Literal, 0, len(valueSpecs))
		for _, valueSpec := range valueSpecs {
			literal, err := parseLiteral(valueSpec)
			if err != nil {
				return nil, err
			}
			values = append(values, literal)
		}
		if len(columns) != len(values) {
			return nil, fmt.Errorf("%w: columns and values length mismatch", errInvalidSQL)
		}
		allValueGroups = append(allValueGroups, values)

		remaining = strings.TrimSpace(remaining[closeIdx+1:])
		if len(remaining) > 0 && remaining[0] == ',' {
			remaining = strings.TrimSpace(remaining[1:])
			continue
		}
		break
	}

	if len(allValueGroups) == 0 {
		return nil, fmt.Errorf("%w: VALUES requires at least one value group", errInvalidSQL)
	}

	// Parse optional ON CONFLICT and RETURNING clauses after the VALUES.
	var returningColumns []string
	var onConflict *ast.OnConflictClause
	afterValues := strings.TrimRight(strings.TrimSpace(remaining), ";")
	afterValues = strings.TrimSpace(afterValues)
	if len(afterValues) > 0 {
		// ON CONFLICT must come before RETURNING
		if hasPrefixFold(afterValues, "ON CONFLICT") {
			var err error
			onConflict, afterValues, err = parseOnConflict(afterValues)
			if err != nil {
				return nil, err
			}
			afterValues = strings.TrimSpace(afterValues)
		}
		if hasPrefixFold(afterValues, "RETURNING ") {
			retClause := strings.TrimSpace(afterValues[len("RETURNING "):])
			if retClause == "*" {
				returningColumns = []string{"*"}
			} else {
				parts := splitCSV(retClause)
				returningColumns = make([]string, 0, len(parts))
				for _, p := range parts {
					returningColumns = append(returningColumns, canonicalIdentifier(p))
				}
			}
		}
	}

	if len(allValueGroups) == 1 {
		return ast.InsertStatement{
			TableName:        table,
			Columns:          columns,
			Values:           allValueGroups[0],
			ReturningColumns: returningColumns,
			OnConflict:       onConflict,
		}, nil
	}

	return ast.InsertStatement{
		TableName:        table,
		Columns:          columns,
		Values:           allValueGroups[0],
		MultiValues:      allValueGroups[1:],
		ReturningColumns: returningColumns,
		OnConflict:       onConflict,
	}, nil
}

func parseCreateIndex(sql string, ifNotExists bool) (ast.Statement, error) {
	prefix := "CREATE INDEX "
	if ifNotExists {
		prefix = "CREATE INDEX IF NOT EXISTS "
	}
	rest := strings.TrimSpace(sql[len(prefix):])
	onIndex := indexFold(rest, " ON ")
	if onIndex <= 0 {
		return nil, fmt.Errorf("%w: CREATE INDEX requires ON clause", errInvalidSQL)
	}

	indexName := canonicalIdentifier(strings.TrimSpace(rest[:onIndex]))
	if indexName == "" {
		return nil, fmt.Errorf("%w: index name required", errInvalidSQL)
	}

	afterOn := strings.TrimSpace(rest[onIndex+len(" ON "):])
	usingIndex := indexFold(afterOn, " USING ")
	targetClause := afterOn
	method := ""
	if usingIndex != -1 {
		targetClause = strings.TrimSpace(afterOn[:usingIndex])
		methodClause := strings.TrimSpace(afterOn[usingIndex+len(" USING "):])
		if methodClause == "" {
			return nil, fmt.Errorf("%w: index method required after USING", errInvalidSQL)
		}
		if strings.Contains(methodClause, " ") {
			return nil, fmt.Errorf("%w: invalid index method clause", errInvalidSQL)
		}
		method = strings.ToLower(methodClause)
		if method != "hash" && method != "btree" {
			return nil, fmt.Errorf("%w: unsupported index method %q", errInvalidSQL, method)
		}
	}

	open := strings.Index(targetClause, "(")
	close := strings.LastIndex(targetClause, ")")
	if open <= 0 || close <= open {
		return nil, fmt.Errorf("%w: invalid index target clause", errInvalidSQL)
	}

	table := canonicalIdentifier(strings.TrimSpace(targetClause[:open]))
	if table == "" {
		return nil, fmt.Errorf("%w: table name required", errInvalidSQL)
	}

	columnSpecs := splitCSV(targetClause[open+1 : close])
	if len(columnSpecs) == 0 {
		return nil, fmt.Errorf("%w: indexed column required", errInvalidSQL)
	}

	columns := make([]string, 0, len(columnSpecs))
	for _, spec := range columnSpecs {
		column := canonicalIdentifier(strings.TrimSpace(spec))
		if column == "" {
			return nil, fmt.Errorf("%w: indexed column required", errInvalidSQL)
		}
		columns = append(columns, column)
	}

	if strings.TrimSpace(targetClause[close+1:]) != "" {
		return nil, fmt.Errorf("%w: invalid trailing index clause", errInvalidSQL)
	}

	statement := ast.CreateIndexStatement{IndexName: indexName, TableName: table, Column: columns[0], Method: method, IfNotExists: ifNotExists}
	if len(columns) > 1 {
		statement.Columns = columns
	}

	return statement, nil
}

// parseCreateEntity parses:
//
//	CREATE ENTITY [IF NOT EXISTS] <name> ( ROOT <table>, INCLUDES <table1>, <table2>, ... )
func parseCreateEntity(sql string, ifNotExists bool) (ast.Statement, error) {
	prefix := "CREATE ENTITY "
	if ifNotExists {
		prefix = "CREATE ENTITY IF NOT EXISTS "
	}
	rest := strings.TrimSpace(sql[len(prefix):])

	open := strings.Index(rest, "(")
	if open <= 0 {
		return nil, fmt.Errorf("%w: CREATE ENTITY requires parenthesized body", errInvalidSQL)
	}
	close := strings.LastIndex(rest, ")")
	if close <= open {
		return nil, fmt.Errorf("%w: unmatched parenthesis in CREATE ENTITY", errInvalidSQL)
	}

	entityName := canonicalIdentifier(strings.TrimSpace(rest[:open]))
	if entityName == "" {
		return nil, fmt.Errorf("%w: entity name required", errInvalidSQL)
	}

	body := strings.TrimSpace(rest[open+1 : close])

	if !hasPrefixFold(body, "ROOT ") {
		return nil, fmt.Errorf("%w: CREATE ENTITY body must start with ROOT <table>", errInvalidSQL)
	}

	// Split body by comma to get ROOT and INCLUDES parts.
	parts := splitCSV(body)
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: CREATE ENTITY requires ROOT declaration", errInvalidSQL)
	}

	// Parse ROOT <table>
	rootPart := strings.TrimSpace(parts[0])
	if !hasPrefixFold(rootPart, "ROOT ") {
		return nil, fmt.Errorf("%w: first clause must be ROOT <table>", errInvalidSQL)
	}
	rootTable := canonicalIdentifier(strings.TrimSpace(rootPart[len("ROOT "):]))
	if rootTable == "" {
		return nil, fmt.Errorf("%w: ROOT table name required", errInvalidSQL)
	}

	tables := []string{rootTable}

	// Parse remaining parts — each should start with INCLUDES or be a plain table name after INCLUDES.
	for i := 1; i < len(parts); i++ {
		part := strings.TrimSpace(parts[i])
		if hasPrefixFold(part, "INCLUDES ") {
			// "INCLUDES table1" — extract table name
			tableName := canonicalIdentifier(strings.TrimSpace(part[len("INCLUDES "):]))
			if tableName == "" {
				return nil, fmt.Errorf("%w: table name required after INCLUDES", errInvalidSQL)
			}
			tables = append(tables, tableName)
		} else {
			// Plain table name (continuation after INCLUDES keyword)
			tableName := canonicalIdentifier(part)
			if tableName == "" {
				return nil, fmt.Errorf("%w: invalid table in CREATE ENTITY", errInvalidSQL)
			}
			tables = append(tables, tableName)
		}
	}

	return ast.CreateEntityStatement{
		EntityName:  entityName,
		RootTable:   rootTable,
		Tables:      tables,
		IfNotExists: ifNotExists,
	}, nil
}

func parseUpdate(sql string) (ast.Statement, error) {
	rest := strings.TrimSpace(sql[len("UPDATE "):])
	setIndex := indexFold(rest, " SET ")
	if setIndex <= 0 {
		return nil, fmt.Errorf("%w: UPDATE requires SET clause", errInvalidSQL)
	}

	table := canonicalIdentifier(strings.TrimSpace(rest[:setIndex]))
	if table == "" {
		return nil, fmt.Errorf("%w: table name required", errInvalidSQL)
	}

	assignmentAndWhere := strings.TrimSpace(rest[setIndex+len(" SET "):])
	whereIndex := indexFold(assignmentAndWhere, " WHERE ")

	assignmentsClause := assignmentAndWhere
	whereClause := ""
	if whereIndex != -1 {
		assignmentsClause = strings.TrimSpace(assignmentAndWhere[:whereIndex])
		whereClause = strings.TrimSpace(assignmentAndWhere[whereIndex+len(" WHERE "):])
	}

	if assignmentsClause == "" {
		return nil, fmt.Errorf("%w: UPDATE requires at least one assignment", errInvalidSQL)
	}

	assignmentSpecs := splitCSV(assignmentsClause)
	if len(assignmentSpecs) == 0 {
		return nil, fmt.Errorf("%w: UPDATE requires at least one assignment", errInvalidSQL)
	}

	columns := make([]string, 0, len(assignmentSpecs))
	values := make([]ast.Literal, 0, len(assignmentSpecs))
	var expressions []ast.UpdateExpr
	hasArithmetic := false
	for _, spec := range assignmentSpecs {
		parts := strings.SplitN(spec, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%w: invalid UPDATE assignment %q", errInvalidSQL, spec)
		}

		column := canonicalIdentifier(parts[0])
		if column == "" {
			return nil, fmt.Errorf("%w: assignment column required", errInvalidSQL)
		}

		rhsTrimmed := strings.TrimSpace(parts[1])

		// Try parsing as arithmetic expression: identifier OP literal
		if expr := tryParseArithmeticExpr(rhsTrimmed); expr != nil {
			columns = append(columns, column)
			values = append(values, ast.Literal{}) // placeholder
			expressions = append(expressions, *expr)
			hasArithmetic = true
			continue
		}

		value, err := parseLiteral(parts[1])
		if err != nil {
			return nil, err
		}

		columns = append(columns, column)
		values = append(values, value)
		expressions = append(expressions, ast.UpdateExpr{Kind: ast.UpdateExprLiteral, Value: value})
	}

	// Only attach Expressions if there is at least one arithmetic expression.
	if !hasArithmetic {
		expressions = nil
	}

	var predicate *ast.Predicate
	if whereIndex != -1 {
		if whereClause == "" {
			return nil, fmt.Errorf("%w: WHERE predicate required", errInvalidSQL)
		}
		parsedWhere, err := parseWhere(whereClause)
		if err != nil {
			return nil, err
		}
		predicate = parsedWhere
	}

	return ast.UpdateStatement{
		TableName:   table,
		Columns:     columns,
		Values:      values,
		Expressions: expressions,
		Where:       predicate,
	}, nil
}

func parseDelete(sql string) (ast.Statement, error) {
	rest := strings.TrimSpace(sql[len("DELETE FROM "):])
	whereIndex := indexFold(rest, " WHERE ")

	tableClause := rest
	whereClause := ""
	if whereIndex != -1 {
		tableClause = strings.TrimSpace(rest[:whereIndex])
		whereClause = strings.TrimSpace(rest[whereIndex+len(" WHERE "):])
	}

	table := canonicalIdentifier(tableClause)
	if table == "" {
		return nil, fmt.Errorf("%w: table name required", errInvalidSQL)
	}

	var predicate *ast.Predicate
	if whereIndex != -1 {
		if whereClause == "" {
			return nil, fmt.Errorf("%w: WHERE predicate required", errInvalidSQL)
		}
		parsedWhere, err := parseWhere(whereClause)
		if err != nil {
			return nil, err
		}
		predicate = parsedWhere
	}

	return ast.DeleteStatement{TableName: table, Where: predicate}, nil
}

func parseSelect(sql string) (ast.Statement, error) {
	rest := strings.TrimSpace(sql[len("SELECT "):])

	distinct := false
	if hasPrefixFold(rest, "DISTINCT ") {
		distinct = true
		rest = strings.TrimSpace(rest[len("DISTINCT "):])
	}

	fromIndex := findTopLevelKeyword(rest, "FROM")
	if fromIndex <= 0 {
		return nil, fmt.Errorf("%w: SELECT requires FROM", errInvalidSQL)
	}

	columnsClause := strings.TrimSpace(rest[:fromIndex])
	columnsRaw := splitCSVDepthAware(columnsClause)
	if len(columnsRaw) == 0 {
		return nil, fmt.Errorf("%w: select requires columns", errInvalidSQL)
	}

	columns := make([]string, 0, len(columnsRaw))
	windowFunctions := make([]ast.WindowFunction, 0)
	jsonAccessColumns := make([]ast.JsonAccess, 0)
	caseWhenColumns := make([]ast.CaseWhenExpression, 0)
	for _, column := range columnsRaw {
		trimmed := strings.TrimSpace(column)
		if trimmed == "*" {
			columns = append(columns, "*")
			continue
		}

		// Detect window function: FUNC(...) OVER (...) AS alias
		if wf, ok := parseWindowFunctionColumn(trimmed); ok {
			windowFunctions = append(windowFunctions, wf)
			columns = append(columns, wf.Alias)
			continue
		}

		// Detect CASE WHEN expression: CASE WHEN ... END [AS alias]
		if cw, ok := parseCaseWhenColumn(trimmed); ok {
			caseWhenColumns = append(caseWhenColumns, cw)
			columns = append(columns, cw.Alias)
			continue
		}

		// Detect JSON access: col->>'key' or col->'key'->>'nested'
		if ja, ok := parseJsonAccessColumn(trimmed); ok {
			jsonAccessColumns = append(jsonAccessColumns, ja)
			columns = append(columns, ja.Alias)
			continue
		}

		columns = append(columns, canonicalIdentifier(trimmed))
	}

	afterFrom := strings.TrimSpace(rest[fromIndex+len("FROM "):])

	clauseIndex := firstClauseIndex(afterFrom)
	fromSpec := ""
	remaining := ""
	if clauseIndex == -1 {
		fromSpec = afterFrom
	} else {
		fromSpec = strings.TrimSpace(afterFrom[:clauseIndex])
		remaining = strings.TrimSpace(afterFrom[clauseIndex:])
	}

	forHistory := false
	if len(fromSpec) >= len(" FOR HISTORY") && strings.EqualFold(fromSpec[len(fromSpec)-len(" FOR HISTORY"):], " FOR HISTORY") {
		forHistory = true
		fromSpec = strings.TrimSpace(fromSpec[:len(fromSpec)-len(" FOR HISTORY")])
	}

	table, tableAlias, joins, err := parseFromAndJoin(fromSpec)
	if err != nil {
		return nil, err
	}
	var predicate *ast.Predicate
	groupBy := make([]string, 0)
	var having *ast.Predicate
	orderBy := make([]ast.OrderByClause, 0)
	var limit *int
	var offset *int

	if hasPrefixFold(remaining, "WHERE ") {
		whereBody := strings.TrimSpace(remaining[len("WHERE "):])
		nextIndex := firstClauseIndex(whereBody)
		whereClause := whereBody
		remaining = ""
		if nextIndex != -1 {
			whereClause = strings.TrimSpace(whereBody[:nextIndex])
			remaining = strings.TrimSpace(whereBody[nextIndex:])
		}

		parsed, err := parseWhere(whereClause)
		if err != nil {
			return nil, err
		}
		predicate = parsed
	}

	if hasPrefixFold(remaining, "GROUP BY ") {
		groupBody := strings.TrimSpace(remaining[len("GROUP BY "):])
		nextIndex := firstClauseIndex(groupBody)
		groupClause := groupBody
		remaining = ""
		if nextIndex != -1 {
			groupClause = strings.TrimSpace(groupBody[:nextIndex])
			remaining = strings.TrimSpace(groupBody[nextIndex:])
		}

		parsed, err := parseGroupBy(groupClause)
		if err != nil {
			return nil, err
		}
		groupBy = parsed
	}

	if hasPrefixFold(remaining, "HAVING ") {
		havingBody := strings.TrimSpace(remaining[len("HAVING "):])
		nextIndex := firstClauseIndex(havingBody)
		havingClause := havingBody
		remaining = ""
		if nextIndex != -1 {
			havingClause = strings.TrimSpace(havingBody[:nextIndex])
			remaining = strings.TrimSpace(havingBody[nextIndex:])
		}

		parsed, err := parseWhere(havingClause)
		if err != nil {
			return nil, err
		}
		having = parsed
	}

	if hasPrefixFold(remaining, "ORDER BY ") {
		orderBody := strings.TrimSpace(remaining[len("ORDER BY "):])
		nextIndex := firstClauseIndex(orderBody)
		orderClause := orderBody
		remaining = ""
		if nextIndex != -1 {
			orderClause = strings.TrimSpace(orderBody[:nextIndex])
			remaining = strings.TrimSpace(orderBody[nextIndex:])
		}

		parsed, err := parseOrderBy(orderClause)
		if err != nil {
			return nil, err
		}
		orderBy = parsed
	}

	if hasPrefixFold(remaining, "LIMIT ") {
		limitBody := strings.TrimSpace(remaining[len("LIMIT "):])
		parsedLimit, parsedOffset, err := parseLimit(limitBody)
		if err != nil {
			return nil, err
		}
		limit = parsedLimit
		offset = parsedOffset
		remaining = ""
	}

	if hasPrefixFold(remaining, "OFFSET ") {
		offsetBody := strings.TrimSpace(remaining[len("OFFSET "):])
		parsed, err := parseSingleInt(offsetBody, "OFFSET")
		if err != nil {
			return nil, err
		}
		offset = parsed
		remaining = ""
	}

	if strings.TrimSpace(remaining) != "" {
		return nil, fmt.Errorf("%w: unsupported select clause %q", errInvalidSQL, remaining)
	}

	if table == "" {
		return nil, fmt.Errorf("%w: table name required", errInvalidSQL)
	}

	return ast.SelectStatement{
		Distinct:          distinct,
		Columns:           columns,
		JsonAccessColumns: jsonAccessColumns,
		CaseWhenColumns:   caseWhenColumns,
		TableName:         table,
		TableAlias:        tableAlias,
		ForHistory:        forHistory,
		Joins:             joins,
		Where:             predicate,
		GroupBy:           groupBy,
		Having:            having,
		OrderBy:           orderBy,
		Limit:             limit,
		Offset:            offset,
		WindowFunctions:   windowFunctions,
	}, nil
}

// parseTableRef extracts a table name and optional alias from "tablename" or "tablename alias".
func parseTableRef(raw string) (table, alias string) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 {
		return "", ""
	}
	table = canonicalIdentifier(parts[0])
	if len(parts) >= 2 {
		candidate := parts[1]
		// Don't treat SQL keywords as aliases.
		switch {
		case strings.EqualFold(candidate, "ON"),
			strings.EqualFold(candidate, "LEFT"),
			strings.EqualFold(candidate, "RIGHT"),
			strings.EqualFold(candidate, "INNER"),
			strings.EqualFold(candidate, "CROSS"),
			strings.EqualFold(candidate, "JOIN"),
			strings.EqualFold(candidate, "WHERE"),
			strings.EqualFold(candidate, "GROUP"),
			strings.EqualFold(candidate, "ORDER"),
			strings.EqualFold(candidate, "LIMIT"),
			strings.EqualFold(candidate, "OFFSET"),
			strings.EqualFold(candidate, "HAVING"),
			strings.EqualFold(candidate, "FOR"):
			return table, ""
		}
		alias = canonicalIdentifier(parts[1])
	}
	return table, alias
}

// joinKeywords defines the recognised JOIN prefixes in specificity order.
var joinKeywords = []struct {
	keyword  string
	joinType ast.JoinType
}{
	{" LEFT JOIN ", ast.JoinLeft},
	{" RIGHT JOIN ", ast.JoinRight},
	{" CROSS JOIN ", ast.JoinCross},
	{" INNER JOIN ", ast.JoinInner},
	{" JOIN ", ast.JoinInner},
}

// findFirstJoin finds the earliest case-insensitive JOIN keyword in s.
// Returns the index, matched keyword string, join type, and whether a match was found.
func findFirstJoin(s string) (int, string, ast.JoinType, bool) {
	bestIdx := -1
	bestKeyword := ""
	bestType := ast.JoinInner
	for _, jt := range joinKeywords {
		idx := indexFold(s, jt.keyword)
		if idx != -1 && (bestIdx == -1 || idx < bestIdx) {
			bestIdx = idx
			bestKeyword = jt.keyword
			bestType = jt.joinType
		}
	}
	return bestIdx, bestKeyword, bestType, bestIdx != -1
}

func parseFromAndJoin(spec string) (string, string, []ast.JoinClause, error) {
	joinIndex, matchedKeyword, matchedType, found := findFirstJoin(spec)

	if !found {
		table, alias := parseTableRef(spec)
		if table == "" {
			return "", "", nil, fmt.Errorf("%w: table name required", errInvalidSQL)
		}
		return table, alias, nil, nil
	}

	leftRaw := strings.TrimSpace(spec[:joinIndex])
	leftTable, leftAlias := parseTableRef(leftRaw)
	if leftTable == "" {
		return "", "", nil, fmt.Errorf("%w: left table name required", errInvalidSQL)
	}

	// Iteratively parse each JOIN clause from the remainder.
	var joins []ast.JoinClause
	rest := strings.TrimSpace(spec[joinIndex+len(matchedKeyword):])
	currentType := matchedType

	for {
		if currentType == ast.JoinCross {
			// CROSS JOIN has no ON clause — find the next JOIN or end of string.
			nextIdx, nextKW, nextType, nextFound := findFirstJoin(rest)
			var tableRaw string
			if !nextFound {
				tableRaw = rest
				rest = ""
			} else {
				tableRaw = strings.TrimSpace(rest[:nextIdx])
				rest = strings.TrimSpace(rest[nextIdx+len(nextKW):])
			}
			rightTable, rightAlias := parseTableRef(tableRaw)
			if rightTable == "" {
				return "", "", nil, fmt.Errorf("%w: right table name required", errInvalidSQL)
			}
			joins = append(joins, ast.JoinClause{
				JoinType:  ast.JoinCross,
				TableName: rightTable,
				Alias:     rightAlias,
			})
			if !nextFound {
				break
			}
			currentType = nextType
			continue
		}

		// Non-CROSS JOIN: find ON keyword.
		onIndex := indexFold(rest, " ON ")
		if onIndex <= 0 {
			return "", "", nil, fmt.Errorf("%w: %s JOIN requires ON clause", errInvalidSQL, string(currentType))
		}

		tableRaw := strings.TrimSpace(rest[:onIndex])
		rightTable, rightAlias := parseTableRef(tableRaw)
		if rightTable == "" {
			return "", "", nil, fmt.Errorf("%w: right table name required", errInvalidSQL)
		}

		afterON := strings.TrimSpace(rest[onIndex+len(" ON "):])

		// The ON clause runs until the next JOIN keyword or end of string.
		nextIdx, nextKW, nextType, nextFound := findFirstJoin(afterON)
		var onClause string
		if !nextFound {
			onClause = afterON
			rest = ""
		} else {
			onClause = strings.TrimSpace(afterON[:nextIdx])
			rest = strings.TrimSpace(afterON[nextIdx+len(nextKW):])
		}

		leftColumn, rightColumn, err := parseJoinPredicate(onClause)
		if err != nil {
			return "", "", nil, err
		}

		joins = append(joins, ast.JoinClause{
			JoinType:    currentType,
			TableName:   rightTable,
			Alias:       rightAlias,
			LeftColumn:  leftColumn,
			RightColumn: rightColumn,
		})

		if !nextFound {
			break
		}
		currentType = nextType
	}

	return leftTable, leftAlias, joins, nil
}

func parseJoinPredicate(clause string) (string, string, error) {
	parts := strings.SplitN(clause, "=", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("%w: JOIN ON requires '=' predicate", errInvalidSQL)
	}

	leftColumn := canonicalIdentifier(strings.TrimSpace(parts[0]))
	rightColumn := canonicalIdentifier(strings.TrimSpace(parts[1]))
	if leftColumn == "" || rightColumn == "" {
		return "", "", fmt.Errorf("%w: JOIN ON columns required", errInvalidSQL)
	}

	if !strings.Contains(leftColumn, ".") || !strings.Contains(rightColumn, ".") {
		return "", "", fmt.Errorf("%w: JOIN ON columns must be table-qualified", errInvalidSQL)
	}

	return leftColumn, rightColumn, nil
}

// firstClauseIndex scans s for the first top-level SQL clause keyword
// (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET) respecting
// parenthesis depth and string literals. Returns the position of the
// keyword (without leading space) or -1.
// The comparison is case-insensitive so callers do NOT need to uppercase s.
func firstClauseIndex(s string) int {
	keywords := []string{" WHERE ", " GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT ", " OFFSET "}
	prefixes := []string{"WHERE ", "GROUP BY ", "HAVING ", "ORDER BY ", "LIMIT ", "OFFSET "}

	depth := 0
	inString := false
	first := -1

	for i := 0; i < len(s); i++ {
		ch := s[i]
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
		if depth != 0 {
			continue
		}

		for _, kw := range keywords {
			if i+len(kw) <= len(s) && strings.EqualFold(s[i:i+len(kw)], kw) {
				pos := i + 1 // skip the leading space
				if first == -1 || pos < first {
					first = pos
				}
			}
		}
		if first != -1 && first <= i+1 {
			return first
		}
	}

	if first != -1 {
		return first
	}

	for _, p := range prefixes {
		if len(s) >= len(p) && strings.EqualFold(s[:len(p)], p) {
			return 0
		}
	}

	return -1
}

func parseGroupBy(clause string) ([]string, error) {
	entries := splitCSV(clause)
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: GROUP BY requires at least one column", errInvalidSQL)
	}

	columns := make([]string, 0, len(entries))
	for _, entry := range entries {
		column := canonicalIdentifier(entry)
		if column == "" {
			return nil, fmt.Errorf("%w: GROUP BY column required", errInvalidSQL)
		}
		columns = append(columns, column)
	}

	return columns, nil
}

func parseOrderBy(clause string) ([]ast.OrderByClause, error) {
	entries := splitCSV(clause)
	if len(entries) == 0 {
		return nil, fmt.Errorf("%w: ORDER BY requires at least one column", errInvalidSQL)
	}

	clauses := make([]ast.OrderByClause, 0, len(entries))
	for _, entry := range entries {
		parts := strings.Fields(entry)
		if len(parts) == 0 || len(parts) > 2 {
			return nil, fmt.Errorf("%w: invalid ORDER BY clause", errInvalidSQL)
		}

		direction := ast.SortAsc
		if len(parts) == 2 {
			switch strings.ToUpper(parts[1]) {
			case string(ast.SortAsc):
				direction = ast.SortAsc
			case string(ast.SortDesc):
				direction = ast.SortDesc
			default:
				return nil, fmt.Errorf("%w: ORDER BY direction must be ASC or DESC", errInvalidSQL)
			}
		}

		column := canonicalIdentifier(parts[0])
		if column == "" {
			return nil, fmt.Errorf("%w: ORDER BY column required", errInvalidSQL)
		}

		clauses = append(clauses, ast.OrderByClause{Column: column, Direction: direction})
	}

	return clauses, nil
}

func parseLimit(clause string) (*int, *int, error) {
	upper := strings.ToUpper(clause)
	// Handle LIMIT X OFFSET Y
	if idx := strings.Index(upper, "OFFSET "); idx != -1 {
		limitStr := strings.TrimSpace(clause[:idx])
		offsetStr := strings.TrimSpace(clause[idx+len("OFFSET "):])
		limitVal, err := parseSingleInt(limitStr, "LIMIT")
		if err != nil {
			return nil, nil, err
		}
		offsetVal, err := parseSingleInt(offsetStr, "OFFSET")
		if err != nil {
			return nil, nil, err
		}
		return limitVal, offsetVal, nil
	}

	// LIMIT X only
	limitVal, err := parseSingleInt(clause, "LIMIT")
	if err != nil {
		return nil, nil, err
	}
	return limitVal, nil, nil
}

func parseSingleInt(clause string, label string) (*int, error) {
	parts := strings.Fields(clause)
	if len(parts) != 1 {
		return nil, fmt.Errorf("%w: %s requires a single numeric value", errInvalidSQL, label)
	}

	value, err := strconv.Atoi(parts[0])
	if err != nil || value < 0 {
		return nil, fmt.Errorf("%w: %s must be a non-negative integer", errInvalidSQL, label)
	}

	return &value, nil
}

// isSubqueryExpression checks if text is a parenthesized SELECT statement.
func isSubqueryExpression(text string) bool {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < 3 || trimmed[0] != '(' || trimmed[len(trimmed)-1] != ')' {
		return false
	}
	inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	return hasPrefixFold(inner, "SELECT ")
}

// parseSubqueryExpression strips outer parens and parses the inner SELECT.
func parseSubqueryExpression(text string) (ast.SelectStatement, error) {
	trimmed := strings.TrimSpace(text)
	inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	stmt, err := Parse(inner)
	if err != nil {
		return ast.SelectStatement{}, fmt.Errorf("subquery parse error: %w", err)
	}
	sel, ok := stmt.(ast.SelectStatement)
	if !ok {
		return ast.SelectStatement{}, fmt.Errorf("%w: subquery must be a SELECT statement", errInvalidSQL)
	}
	return sel, nil
}

// findTopLevelKeyword finds keyword at paren depth 0, returning the position
// of the keyword in the original string, or -1 if not found.
func findTopLevelKeyword(clause string, keyword string) int {
	upper := strings.ToUpper(clause)
	target := " " + keyword + " "
	depth := 0
	inStr := false

	for i := 0; i < len(clause); i++ {
		ch := clause[i]
		if ch == '\'' {
			inStr = !inStr
			continue
		}
		if inStr {
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
		if depth != 0 {
			continue
		}

		// Check " KEYWORD " at this position
		if i+len(target) <= len(upper) && upper[i:i+len(target)] == target {
			return i + 1 // skip leading space
		}
	}

	// Also check if clause starts with "KEYWORD "
	prefix := keyword + " "
	if strings.HasPrefix(upper, prefix) {
		return 0
	}

	return -1
}

func parseWhere(clause string) (*ast.Predicate, error) {
	trimmed := strings.TrimSpace(clause)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: predicate is required", errInvalidSQL)
	}

	for hasOuterParentheses(trimmed) {
		// Don't strip parens if this is a subquery expression like (SELECT ...)
		if isSubqueryExpression(trimmed) {
			break
		}
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	}

	// EXISTS (SELECT ...) — operator-only predicate
	upperTr := strings.ToUpper(trimmed)
	if strings.HasPrefix(upperTr, "EXISTS ") || strings.HasPrefix(upperTr, "EXISTS(") {
		subExpr := strings.TrimSpace(trimmed[len("EXISTS"):])
		if !isSubqueryExpression(subExpr) {
			return nil, fmt.Errorf("%w: EXISTS requires a subquery", errInvalidSQL)
		}
		sel, err := parseSubqueryExpression(subExpr)
		if err != nil {
			return nil, err
		}
		return &ast.Predicate{
			Operator: "EXISTS",
			Subquery: &ast.Subquery{Statement: sel},
		}, nil
	}

	if branches, ok := splitTopLevelLogical(trimmed, "OR"); ok {
		current, err := parseWhere(branches[0])
		if err != nil {
			return nil, err
		}
		for _, branch := range branches[1:] {
			right, err := parseWhere(branch)
			if err != nil {
				return nil, err
			}
			current = &ast.Predicate{Operator: "OR", Left: current, Right: right}
		}
		return current, nil
	}

	if branches, ok := splitTopLevelLogical(trimmed, "AND"); ok {
		current, err := parseWhere(branches[0])
		if err != nil {
			return nil, err
		}
		for _, branch := range branches[1:] {
			right, err := parseWhere(branch)
			if err != nil {
				return nil, err
			}
			current = &ast.Predicate{Operator: "AND", Left: current, Right: right}
		}
		return current, nil
	}

	if strings.HasPrefix(strings.ToUpper(trimmed), "NOT ") {
		inner, err := parseWhere(strings.TrimSpace(trimmed[len("NOT "):]))
		if err != nil {
			return nil, err
		}
		return &ast.Predicate{Operator: "NOT", Left: inner}, nil
	}

	return parseSimplePredicate(trimmed)
}

func parseSimplePredicate(clause string) (*ast.Predicate, error) {
	trimmed := strings.TrimSpace(clause)
	upperTrimmed := strings.ToUpper(trimmed)
	if strings.HasSuffix(upperTrimmed, " IS NULL") {
		column := canonicalIdentifier(strings.TrimSpace(trimmed[:len(trimmed)-len(" IS NULL")]))
		if column == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		return &ast.Predicate{Column: column, Operator: "IS NULL", Value: ast.Literal{Kind: ast.LiteralNull}}, nil
	}
	if strings.HasSuffix(upperTrimmed, " IS NOT NULL") {
		column := canonicalIdentifier(strings.TrimSpace(trimmed[:len(trimmed)-len(" IS NOT NULL")]))
		if column == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		return &ast.Predicate{Column: column, Operator: "IS NOT NULL", Value: ast.Literal{Kind: ast.LiteralNull}}, nil
	}

	// col NOT IN (SELECT ...) or col NOT IN (val1, val2, ...)
	if idx := findTopLevelKeyword(trimmed, "NOT IN"); idx != -1 {
		col := canonicalIdentifier(strings.TrimSpace(trimmed[:idx]))
		if col == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		rhs := strings.TrimSpace(trimmed[idx+len("NOT IN"):])
		if isSubqueryExpression(rhs) {
			sel, err := parseSubqueryExpression(rhs)
			if err != nil {
				return nil, err
			}
			return &ast.Predicate{
				Column:   col,
				Operator: "NOT IN",
				Subquery: &ast.Subquery{Statement: sel},
			}, nil
		}
		// Parenthesized literal list
		if strings.HasPrefix(rhs, "(") && strings.HasSuffix(rhs, ")") {
			inner := rhs[1 : len(rhs)-1]
			items := splitCSV(inner)
			if len(items) == 0 {
				return nil, fmt.Errorf("%w: NOT IN list requires at least one value", errInvalidSQL)
			}
			inValues := make([]ast.Literal, 0, len(items))
			for _, item := range items {
				val, err := parseLiteral(item)
				if err != nil {
					return nil, fmt.Errorf("%w: invalid NOT IN list value %q", errInvalidSQL, item)
				}
				inValues = append(inValues, val)
			}
			return &ast.Predicate{
				Column:   col,
				Operator: "NOT IN",
				InValues: inValues,
			}, nil
		}
		return nil, fmt.Errorf("%w: NOT IN requires a subquery or parenthesized value list", errInvalidSQL)
	}

	// col IN (SELECT ...) or col IN (val1, val2, ...)
	if idx := findTopLevelKeyword(trimmed, "IN"); idx != -1 {
		rhs := strings.TrimSpace(trimmed[idx+len("IN"):])
		col := canonicalIdentifier(strings.TrimSpace(trimmed[:idx]))
		if col == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		if isSubqueryExpression(rhs) {
			sel, err := parseSubqueryExpression(rhs)
			if err != nil {
				return nil, err
			}
			return &ast.Predicate{
				Column:   col,
				Operator: "IN",
				Subquery: &ast.Subquery{Statement: sel},
			}, nil
		}
		// Must be a parenthesized literal list: (val1, val2, ...)
		if strings.HasPrefix(rhs, "(") && strings.HasSuffix(rhs, ")") {
			inner := rhs[1 : len(rhs)-1]
			items := splitCSV(inner)
			if len(items) == 0 {
				return nil, fmt.Errorf("%w: IN list requires at least one value", errInvalidSQL)
			}
			inValues := make([]ast.Literal, 0, len(items))
			for _, item := range items {
				val, err := parseLiteral(item)
				if err != nil {
					return nil, fmt.Errorf("%w: invalid IN list value %q", errInvalidSQL, item)
				}
				inValues = append(inValues, val)
			}
			return &ast.Predicate{
				Column:   col,
				Operator: "IN",
				InValues: inValues,
			}, nil
		}
		return nil, fmt.Errorf("%w: IN requires a subquery or parenthesized value list", errInvalidSQL)
	}

	// col NOT LIKE 'pattern'
	if idx := findTopLevelKeyword(trimmed, "NOT LIKE"); idx != -1 {
		col := canonicalIdentifier(strings.TrimSpace(trimmed[:idx]))
		if col == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		rhs := strings.TrimSpace(trimmed[idx+len("NOT LIKE"):])
		val, err := parseLiteral(rhs)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid LIKE pattern", errInvalidSQL)
		}
		return &ast.Predicate{Column: col, Operator: "NOT LIKE", Value: val}, nil
	}

	// col NOT ILIKE 'pattern'
	if idx := findTopLevelKeyword(trimmed, "NOT ILIKE"); idx != -1 {
		col := canonicalIdentifier(strings.TrimSpace(trimmed[:idx]))
		if col == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		rhs := strings.TrimSpace(trimmed[idx+len("NOT ILIKE"):])
		val, err := parseLiteral(rhs)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid ILIKE pattern", errInvalidSQL)
		}
		return &ast.Predicate{Column: col, Operator: "NOT ILIKE", Value: val}, nil
	}

	// col LIKE 'pattern'
	if idx := findTopLevelKeyword(trimmed, "LIKE"); idx != -1 {
		col := canonicalIdentifier(strings.TrimSpace(trimmed[:idx]))
		if col == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		rhs := strings.TrimSpace(trimmed[idx+len("LIKE"):])
		val, err := parseLiteral(rhs)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid LIKE pattern", errInvalidSQL)
		}
		return &ast.Predicate{Column: col, Operator: "LIKE", Value: val}, nil
	}

	// col ILIKE 'pattern'
	if idx := findTopLevelKeyword(trimmed, "ILIKE"); idx != -1 {
		col := canonicalIdentifier(strings.TrimSpace(trimmed[:idx]))
		if col == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		rhs := strings.TrimSpace(trimmed[idx+len("ILIKE"):])
		val, err := parseLiteral(rhs)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid ILIKE pattern", errInvalidSQL)
		}
		return &ast.Predicate{Column: col, Operator: "ILIKE", Value: val}, nil
	}

	// col NOT BETWEEN val1 AND val2
	if idx := findTopLevelKeyword(trimmed, "NOT BETWEEN"); idx != -1 {
		col := canonicalIdentifier(strings.TrimSpace(trimmed[:idx]))
		if col == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		rhs := strings.TrimSpace(trimmed[idx+len("NOT BETWEEN"):])
		lo, hi, err := parseBetweenBounds(rhs)
		if err != nil {
			return nil, err
		}
		return &ast.Predicate{Column: col, Operator: "NOT BETWEEN", Value: lo, Value2: &hi}, nil
	}

	// col BETWEEN val1 AND val2
	if idx := findTopLevelKeyword(trimmed, "BETWEEN"); idx != -1 {
		col := canonicalIdentifier(strings.TrimSpace(trimmed[:idx]))
		if col == "" {
			return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
		}
		rhs := strings.TrimSpace(trimmed[idx+len("BETWEEN"):])
		lo, hi, err := parseBetweenBounds(rhs)
		if err != nil {
			return nil, err
		}
		return &ast.Predicate{Column: col, Operator: "BETWEEN", Value: lo, Value2: &hi}, nil
	}

	operator := ""
	left := ""
	right := ""
	// Find comparison operator while skipping > inside ->> and -> JSON operators.
	// Scan from the right to find the last comparison operator not inside JSON arrows.
	splitIdx := findComparisonOperator(clause)
	if splitIdx >= 0 {
		left = clause[:splitIdx]
		rest := clause[splitIdx:]
		if strings.HasPrefix(rest, ">=") {
			operator = ">="
			right = rest[2:]
		} else if strings.HasPrefix(rest, "<=") {
			operator = "<="
			right = rest[2:]
		} else if rest[0] == '=' {
			operator = "="
			right = rest[1:]
		} else if rest[0] == '>' {
			operator = ">"
			right = rest[1:]
		} else if rest[0] == '<' {
			operator = "<"
			right = rest[1:]
		}
	}

	if operator == "" {
		return nil, fmt.Errorf("%w: unsupported where predicate operator", errInvalidSQL)
	}

	leftTrimmed := strings.TrimSpace(left)
	rightTrimmed := strings.TrimSpace(right)

	// Check if the LHS is a JSON access expression.
	if ja, ok := parseJsonAccessExpr(leftTrimmed); ok {
		// Scalar subquery: col->>'key' = (SELECT ... LIMIT 1)
		if isSubqueryExpression(rightTrimmed) {
			sel, err := parseSubqueryExpression(rightTrimmed)
			if err != nil {
				return nil, err
			}
			return &ast.Predicate{
				JsonAccess: &ja,
				Operator:   operator,
				Subquery:   &ast.Subquery{Statement: sel},
			}, nil
		}

		value, err := parseLiteral(right)
		if err != nil {
			return nil, err
		}
		return &ast.Predicate{
			JsonAccess: &ja,
			Operator:   operator,
			Value:      value,
		}, nil
	}

	column := canonicalIdentifier(leftTrimmed)
	if column == "" {
		return nil, fmt.Errorf("%w: predicate column is required", errInvalidSQL)
	}

	// Scalar subquery: col = (SELECT ... LIMIT 1)
	if isSubqueryExpression(rightTrimmed) {
		sel, err := parseSubqueryExpression(rightTrimmed)
		if err != nil {
			return nil, err
		}
		return &ast.Predicate{
			Column:   column,
			Operator: operator,
			Subquery: &ast.Subquery{Statement: sel},
		}, nil
	}

	value, err := parseLiteral(right)
	if err != nil {
		return nil, err
	}

	return &ast.Predicate{
		Column:   column,
		Operator: operator,
		Value:    value,
	}, nil
}

// findComparisonOperator finds the position of the comparison operator (=, >, <, >=, <=)
// in a predicate clause, skipping > characters that are part of -> or ->> JSON operators.
// Returns -1 if no comparison operator is found.
func findComparisonOperator(clause string) int {
	inString := false
	for i := 0; i < len(clause); i++ {
		ch := clause[i]
		if ch == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		// Skip ->> and -> sequences.
		if ch == '-' && i+1 < len(clause) && clause[i+1] == '>' {
			if i+2 < len(clause) && clause[i+2] == '>' {
				i += 2 // skip ->>
			} else {
				i += 1 // skip ->
			}
			continue
		}

		switch ch {
		case '=':
			return i
		case '>':
			return i
		case '<':
			return i
		}
	}
	return -1
}

func splitTopLevelLogical(clause string, keyword string) ([]string, bool) {
	if clause == "" {
		return nil, false
	}

	upper := strings.ToUpper(clause)
	target := " " + keyword + " "
	depth := 0
	inString := false
	inBetween := false // skip the AND inside BETWEEN ... AND ...
	start := 0
	parts := make([]string, 0)

	for index := 0; index < len(clause); index++ {
		char := clause[index]
		if char == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		switch char {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}

		if depth != 0 {
			continue
		}

		// Detect BETWEEN keyword to mark that the next AND belongs to it.
		if keyword == "AND" && !inBetween {
			betweenTarget := " BETWEEN "
			if strings.HasPrefix(upper[index:], betweenTarget) {
				inBetween = true
				index += len(betweenTarget) - 1
				continue
			}
			// Also handle "NOT BETWEEN" variant.
			notBetweenTarget := " NOT BETWEEN "
			if strings.HasPrefix(upper[index:], notBetweenTarget) {
				inBetween = true
				index += len(notBetweenTarget) - 1
				continue
			}
		}

		if strings.HasPrefix(upper[index:], target) {
			if keyword == "AND" && inBetween {
				// This AND belongs to BETWEEN, skip it.
				inBetween = false
				index += len(target) - 1
				continue
			}
			part := strings.TrimSpace(clause[start:index])
			if part == "" {
				return nil, false
			}
			parts = append(parts, part)
			start = index + len(target)
			index = start - 1
		}
	}

	if len(parts) == 0 {
		return nil, false
	}

	last := strings.TrimSpace(clause[start:])
	if last == "" {
		return nil, false
	}
	parts = append(parts, last)
	return parts, true
}

func hasOuterParentheses(clause string) bool {
	if len(clause) < 2 || clause[0] != '(' || clause[len(clause)-1] != ')' {
		return false
	}

	depth := 0
	inString := false
	for index := 0; index < len(clause); index++ {
		char := clause[index]
		if char == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}

		if char == '(' {
			depth++
		} else if char == ')' {
			depth--
			if depth == 0 && index != len(clause)-1 {
				return false
			}
		}
		if depth < 0 {
			return false
		}
	}

	return depth == 0
}

func parseDataType(raw string) (ast.DataType, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case string(ast.DataTypeInt):
		return ast.DataTypeInt, nil
	case string(ast.DataTypeText):
		return ast.DataTypeText, nil
	case string(ast.DataTypeJSON):
		return ast.DataTypeJSON, nil
	case string(ast.DataTypeBool):
		return ast.DataTypeBool, nil
	case string(ast.DataTypeFloat):
		return ast.DataTypeFloat, nil
	case string(ast.DataTypeTimestamp):
		return ast.DataTypeTimestamp, nil
	default:
		return "", fmt.Errorf("%w: unsupported data type %q", errInvalidSQL, raw)
	}
}

// parseOnConflict parses ON CONFLICT (cols) DO NOTHING | DO UPDATE SET ...
// Returns the clause, the remaining text after it, and any error.
func parseOnConflict(clause string) (*ast.OnConflictClause, string, error) {
	trimmed := strings.TrimSpace(clause)
	upper := strings.ToUpper(trimmed)

	if !strings.HasPrefix(upper, "ON CONFLICT") {
		return nil, trimmed, nil
	}

	afterOC := strings.TrimSpace(trimmed[len("ON CONFLICT"):])
	// Parse conflict target: (col1, col2, ...)
	if len(afterOC) == 0 || afterOC[0] != '(' {
		return nil, "", fmt.Errorf("%w: ON CONFLICT requires (column_list)", errInvalidSQL)
	}
	closeIdx := findMatchingParen(afterOC, 0)
	if closeIdx <= 0 {
		return nil, "", fmt.Errorf("%w: unmatched parenthesis in ON CONFLICT", errInvalidSQL)
	}
	colSpecs := splitCSV(afterOC[1:closeIdx])
	if len(colSpecs) == 0 {
		return nil, "", fmt.Errorf("%w: ON CONFLICT requires at least one column", errInvalidSQL)
	}
	conflictCols := make([]string, 0, len(colSpecs))
	for _, cs := range colSpecs {
		conflictCols = append(conflictCols, canonicalIdentifier(cs))
	}

	afterCols := strings.TrimSpace(afterOC[closeIdx+1:])
	upperAfter := strings.ToUpper(afterCols)

	if strings.HasPrefix(upperAfter, "DO NOTHING") {
		rest := strings.TrimSpace(afterCols[len("DO NOTHING"):])
		return &ast.OnConflictClause{
			ConflictColumns: conflictCols,
			Action:          ast.OnConflictDoNothing,
		}, rest, nil
	}

	if !strings.HasPrefix(upperAfter, "DO UPDATE SET ") {
		return nil, "", fmt.Errorf("%w: ON CONFLICT requires DO NOTHING or DO UPDATE SET", errInvalidSQL)
	}

	afterSet := strings.TrimSpace(afterCols[len("DO UPDATE SET "):])

	// Find where RETURNING starts (if any) to bound the SET clause.
	upperAfterSet := strings.ToUpper(afterSet)
	retIdx := strings.Index(upperAfterSet, " RETURNING ")
	setClause := afterSet
	rest := ""
	if retIdx != -1 {
		setClause = strings.TrimSpace(afterSet[:retIdx])
		rest = strings.TrimSpace(afterSet[retIdx+1:]) // keep "RETURNING ..."
	}

	assignments := splitCSV(setClause)
	if len(assignments) == 0 {
		return nil, "", fmt.Errorf("%w: DO UPDATE SET requires at least one assignment", errInvalidSQL)
	}

	updateColumns := make([]string, 0, len(assignments))
	updateValues := make([]ast.Literal, 0, len(assignments))
	updateExcluded := make([]string, 0)

	for _, a := range assignments {
		parts := strings.SplitN(a, "=", 2)
		if len(parts) != 2 {
			return nil, "", fmt.Errorf("%w: invalid ON CONFLICT assignment %q", errInvalidSQL, a)
		}
		col := canonicalIdentifier(parts[0])
		rhs := strings.TrimSpace(parts[1])

		// Check for EXCLUDED.col pattern
		upperRHS := strings.ToUpper(rhs)
		if strings.HasPrefix(upperRHS, "EXCLUDED.") {
			exCol := canonicalIdentifier(rhs[len("EXCLUDED."):])
			updateColumns = append(updateColumns, col)
			updateValues = append(updateValues, ast.Literal{}) // placeholder
			updateExcluded = append(updateExcluded, exCol)
			continue
		}

		val, err := parseLiteral(rhs)
		if err != nil {
			return nil, "", fmt.Errorf("%w: invalid ON CONFLICT SET value", errInvalidSQL)
		}
		updateColumns = append(updateColumns, col)
		updateValues = append(updateValues, val)
		updateExcluded = append(updateExcluded, "") // empty = use literal
	}

	return &ast.OnConflictClause{
		ConflictColumns: conflictCols,
		Action:          ast.OnConflictDoUpdate,
		UpdateColumns:   updateColumns,
		UpdateValues:    updateValues,
		UpdateExcluded:  updateExcluded,
	}, rest, nil
}

// parseBetweenBounds parses "val1 AND val2" from a BETWEEN clause.
func parseBetweenBounds(s string) (ast.Literal, ast.Literal, error) {
	upper := strings.ToUpper(s)
	idx := strings.Index(upper, " AND ")
	if idx <= 0 {
		return ast.Literal{}, ast.Literal{}, fmt.Errorf("%w: BETWEEN requires AND between bounds", errInvalidSQL)
	}
	loRaw := strings.TrimSpace(s[:idx])
	hiRaw := strings.TrimSpace(s[idx+len(" AND "):])
	lo, err := parseLiteral(loRaw)
	if err != nil {
		return ast.Literal{}, ast.Literal{}, fmt.Errorf("%w: invalid BETWEEN lower bound %q", errInvalidSQL, loRaw)
	}
	hi, err := parseLiteral(hiRaw)
	if err != nil {
		return ast.Literal{}, ast.Literal{}, fmt.Errorf("%w: invalid BETWEEN upper bound %q", errInvalidSQL, hiRaw)
	}
	return lo, hi, nil
}

// tryParseArithmeticExpr tries to parse a string like "column + 1" or "column * 2.5"
// as an arithmetic SET expression. Returns nil if it doesn't match.
func tryParseArithmeticExpr(raw string) *ast.UpdateExpr {
	trimmed := strings.TrimSpace(raw)
	// Try operators with spaces around them: "col + 1", "col - 2", etc.
	// Order: *, / first (less ambiguous), then +, - (- could be negative literal prefix).
	for _, op := range []string{" * ", " / ", " + ", " - "} {
		idx := strings.Index(trimmed, op)
		if idx <= 0 {
			continue
		}
		left := strings.TrimSpace(trimmed[:idx])
		right := strings.TrimSpace(trimmed[idx+len(op):])
		// Left must be a valid identifier (no spaces, not a number, not empty).
		col := canonicalIdentifier(left)
		if col == "" || strings.ContainsAny(col, " \t'\"()") {
			continue
		}
		// Right must be a valid literal (number, float, etc.).
		operand, err := parseLiteral(right)
		if err != nil {
			continue
		}
		return &ast.UpdateExpr{
			Kind:     ast.UpdateExprArithmetic,
			Column:   col,
			Operator: strings.TrimSpace(op),
			Operand:  operand,
		}
	}
	return nil
}

func parseLiteral(raw string) (ast.Literal, error) {
	trimmed := strings.TrimSpace(raw)
	if len(trimmed) >= 2 && strings.HasPrefix(trimmed, "'") && strings.HasSuffix(trimmed, "'") {
		content := strings.TrimSuffix(strings.TrimPrefix(trimmed, "'"), "'")
		content = strings.ReplaceAll(content, "''", "'")
		// Quick pre-check for RFC3339 format (2006-01-02T15:04:05...) to avoid
		// expensive time.Parse + error allocation on every non-timestamp string.
		if len(content) >= 20 && content[4] == '-' && (content[10] == 'T' || content[10] == 't') {
			if t, err := time.Parse(time.RFC3339, content); err == nil {
				return ast.Literal{Kind: ast.LiteralTimestamp, NumberValue: t.UnixMicro()}, nil
			}
		}
		if len(content) >= 2 && (content[0] == '{' || content[0] == '[') && json.Valid([]byte(content)) {
			return ast.Literal{Kind: ast.LiteralJSON, StringValue: content}, nil
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: content}, nil
	}

	if strings.EqualFold(trimmed, "NULL") {
		return ast.Literal{Kind: ast.LiteralNull}, nil
	}

	if strings.EqualFold(trimmed, "TRUE") {
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: true}, nil
	}
	if strings.EqualFold(trimmed, "FALSE") {
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: false}, nil
	}

	number, err := strconv.ParseInt(trimmed, 10, 64)
	if err == nil {
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: number}, nil
	}

	floatVal, err := strconv.ParseFloat(trimmed, 64)
	if err == nil {
		return ast.Literal{Kind: ast.LiteralFloat, FloatValue: floatVal}, nil
	}

	return ast.Literal{}, fmt.Errorf("%w: unsupported literal %q", errInvalidSQL, trimmed)
}

func normalizeSQL(sql string) string {
	trimmed := strings.TrimSpace(sql)
	trimmed = strings.TrimSuffix(trimmed, ";")
	trimmed = strings.TrimSpace(trimmed)

	// Fast path: if no tabs, newlines, or consecutive spaces, skip Builder.
	needsNormalize := false
	prevWasSpace := false
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch == '\t' || ch == '\n' || ch == '\r' {
			needsNormalize = true
			break
		}
		if ch == ' ' {
			if prevWasSpace {
				needsNormalize = true
				break
			}
			prevWasSpace = true
		} else {
			prevWasSpace = false
		}
	}
	if !needsNormalize {
		return trimmed
	}

	// Collapse internal whitespace to single spaces (preserve string literals).
	var b strings.Builder
	b.Grow(len(trimmed))
	inString := false
	prevSpace := false
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch == '\'' {
			if inString && i+1 < len(trimmed) && trimmed[i+1] == '\'' {
				b.WriteByte(ch)
				b.WriteByte(ch)
				i++
				continue
			}
			inString = !inString
			b.WriteByte(ch)
			prevSpace = false
			continue
		}
		if inString {
			b.WriteByte(ch)
			continue
		}
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
			continue
		}
		b.WriteByte(ch)
		prevSpace = false
	}
	return b.String()
}

// asciiLower lowercases an ASCII byte without allocation.
func asciiLower(ch byte) byte {
	if ch >= 'A' && ch <= 'Z' {
		return ch + ('a' - 'A')
	}
	return ch
}

func canonicalIdentifier(identifier string) string {
	trimmed := strings.TrimSpace(identifier)
	if trimmed == "" {
		return ""
	}

	// Fast path: if identifier has no quotes and is already lowercase, return as-is.
	needsWork := false
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch == '\'' || (ch >= 'A' && ch <= 'Z') {
			needsWork = true
			break
		}
	}
	if !needsWork {
		return trimmed
	}

	// Preserve case inside string literals while lowercasing identifiers/keywords.
	var result strings.Builder
	result.Grow(len(trimmed))
	inQuote := false
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch == '\'' {
			inQuote = !inQuote
			result.WriteByte(ch)
			continue
		}
		if inQuote {
			result.WriteByte(ch)
		} else {
			result.WriteByte(asciiLower(ch))
		}
	}
	return result.String()
}

func splitCSV(input string) []string {
	// Fast path: no commas means single element.
	if !strings.Contains(input, ",") {
		part := strings.TrimSpace(input)
		if part == "" {
			return nil
		}
		return []string{part}
	}

	parts := make([]string, 0, 8)
	inString := false
	start := 0

	for i := 0; i < len(input); i++ {
		ch := input[i]

		if ch == '\'' {
			inString = !inString
			continue
		}

		if ch == ',' && !inString {
			part := strings.TrimSpace(input[start:i])
			if part != "" {
				parts = append(parts, part)
			}
			start = i + 1
			continue
		}
	}

	part := strings.TrimSpace(input[start:])
	if part != "" {
		parts = append(parts, part)
	}

	return parts
}

// splitCSVDepthAware splits on commas at paren depth 0, respecting strings
// and CASE...END blocks. Unlike splitCSV, it tracks parenthesis depth and
// keyword block depth so that commas inside function calls, subqueries, or
// CASE WHEN expressions are not treated as separators.
func splitCSVDepthAware(input string) []string {
	parts := make([]string, 0)
	var current strings.Builder
	inString := false
	depth := 0
	caseDepth := 0
	upper := strings.ToUpper(input)

	for i := 0; i < len(input); i++ {
		ch := input[i]

		if ch == '\'' {
			inString = !inString
			current.WriteByte(ch)
			continue
		}
		if inString {
			current.WriteByte(ch)
			continue
		}

		// Track CASE...END keyword blocks.
		if depth == 0 {
			if i+5 <= len(upper) && upper[i:i+5] == "CASE " {
				caseDepth++
			}
			if i+3 <= len(upper) && upper[i:i+3] == "END" {
				// Only count END as closing if followed by boundary (space, comma, EOF).
				endBoundary := i+3 == len(upper)
				if !endBoundary && i+3 < len(upper) {
					next := upper[i+3]
					endBoundary = next == ' ' || next == ',' || next == ')' || next == '\t' || next == '\n'
				}
				if endBoundary && caseDepth > 0 {
					caseDepth--
				}
			}
		}

		switch ch {
		case '(':
			depth++
			current.WriteByte(ch)
		case ')':
			if depth > 0 {
				depth--
			}
			current.WriteByte(ch)
		case ',':
			if depth == 0 && caseDepth == 0 {
				part := strings.TrimSpace(current.String())
				if part != "" {
					parts = append(parts, part)
				}
				current.Reset()
			} else {
				current.WriteByte(ch)
			}
		default:
			current.WriteByte(ch)
		}
	}

	part := strings.TrimSpace(current.String())
	if part != "" {
		parts = append(parts, part)
	}
	return parts
}

// parseCaseWhenColumn parses a CASE WHEN ... END [AS alias] expression into a
// CaseWhenExpression struct. Returns the struct and true on success.
// Supported form: CASE WHEN condition THEN result [WHEN ...] [ELSE result] END [AS alias]
func parseCaseWhenColumn(expr string) (ast.CaseWhenExpression, bool) {
	upper := strings.ToUpper(strings.TrimSpace(expr))
	if !strings.HasPrefix(upper, "CASE ") {
		return ast.CaseWhenExpression{}, false
	}

	// Find the top-level END keyword.
	trimmed := strings.TrimSpace(expr)
	endIndex := findTopLevelCaseEnd(trimmed)
	if endIndex < 0 {
		return ast.CaseWhenExpression{}, false
	}

	body := strings.TrimSpace(trimmed[len("CASE"):endIndex])
	after := strings.TrimSpace(trimmed[endIndex+3:]) // after "END"

	alias := ""
	if len(after) > 0 {
		upperAfter := strings.ToUpper(after)
		if strings.HasPrefix(upperAfter, "AS ") {
			alias = strings.TrimSpace(after[3:])
		}
	}

	// Parse WHEN ... THEN ... branches and optional ELSE.
	branches := make([]ast.CaseWhenBranch, 0)
	elseResult := ""

	upperBody := strings.ToUpper(body)
	remaining := body
	remainingUpper := upperBody

	for {
		remainingUpper = strings.ToUpper(remaining)
		if !strings.HasPrefix(strings.TrimSpace(remainingUpper), "WHEN ") {
			break
		}
		remaining = strings.TrimSpace(remaining)
		remaining = remaining[len("WHEN"):]
		remainingUpper = strings.ToUpper(remaining)

		thenIdx := findTopLevelKeyword(remaining, "THEN")
		if thenIdx < 0 {
			return ast.CaseWhenExpression{}, false
		}

		condition := strings.TrimSpace(remaining[:thenIdx])
		remaining = strings.TrimSpace(remaining[thenIdx+len("THEN"):])
		remainingUpper = strings.ToUpper(remaining)

		// Result extends until next top-level WHEN or ELSE or end of body.
		nextWhen := findTopLevelKeyword(remaining, "WHEN")
		nextElse := findTopLevelKeyword(remaining, "ELSE")

		endOfResult := len(remaining)
		if nextWhen >= 0 && nextWhen < endOfResult {
			endOfResult = nextWhen
		}
		if nextElse >= 0 && nextElse < endOfResult {
			endOfResult = nextElse
		}

		result := strings.TrimSpace(remaining[:endOfResult])
		remaining = strings.TrimSpace(remaining[endOfResult:])

		branches = append(branches, ast.CaseWhenBranch{
			Condition: condition,
			Result:    result,
		})
	}

	remaining = strings.TrimSpace(remaining)
	remainingUpper = strings.ToUpper(remaining)
	if strings.HasPrefix(remainingUpper, "ELSE ") {
		elseResult = strings.TrimSpace(remaining[len("ELSE"):])
	}

	if len(branches) == 0 {
		return ast.CaseWhenExpression{}, false
	}

	if alias == "" {
		alias = canonicalIdentifier(trimmed)
	} else {
		alias = canonicalIdentifier(alias)
	}

	return ast.CaseWhenExpression{
		Branches:   branches,
		ElseResult: elseResult,
		Alias:      alias,
	}, true
}

// findTopLevelCaseEnd finds the index of the top-level END keyword in a CASE expression.
// It correctly handles nested CASE blocks by tracking CASE/END depth.
func findTopLevelCaseEnd(expr string) int {
	upper := strings.ToUpper(expr)
	depth := 0
	inString := false
	parenDepth := 0

	for i := 0; i < len(upper); i++ {
		ch := upper[i]
		if ch == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		if ch == '(' {
			parenDepth++
			continue
		}
		if ch == ')' {
			if parenDepth > 0 {
				parenDepth--
			}
			continue
		}
		if parenDepth > 0 {
			continue
		}

		if i+5 <= len(upper) && upper[i:i+5] == "CASE " {
			depth++
			continue
		}
		if i+3 <= len(upper) && upper[i:i+3] == "END" {
			endBoundary := i+3 == len(upper)
			if !endBoundary {
				next := upper[i+3]
				endBoundary = next == ' ' || next == ',' || next == ')' || next == '\t' || next == '\n'
			}
			if endBoundary {
				if depth <= 1 {
					return i
				}
				depth--
			}
		}
	}
	return -1
}

// parseWindowFunctionColumn detects and parses a window function column expression:
// FUNC(args) OVER (PARTITION BY ... ORDER BY ...) AS alias
func parseWindowFunctionColumn(expr string) (ast.WindowFunction, bool) {
	upper := strings.ToUpper(expr)

	// Find " OVER " or " OVER(" at top level
	overIndex := -1
	depth := 0
	inStr := false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		if ch == '\'' {
			inStr = !inStr
			continue
		}
		if inStr {
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
		if depth == 0 && i+len(" OVER ") <= len(upper) && upper[i:i+len(" OVER ")] == " OVER " {
			overIndex = i
			break
		}
		if depth == 0 && i+len(" OVER(") <= len(upper) && upper[i:i+len(" OVER(")] == " OVER(" {
			overIndex = i
			break
		}
	}

	if overIndex == -1 {
		return ast.WindowFunction{}, false
	}

	funcPart := strings.TrimSpace(expr[:overIndex])
	afterOver := strings.TrimSpace(expr[overIndex+len(" OVER"):])

	// Parse function name and arguments: FUNC(args)
	funcOpen := strings.Index(funcPart, "(")
	funcClose := strings.LastIndex(funcPart, ")")
	if funcOpen <= 0 || funcClose <= funcOpen {
		return ast.WindowFunction{}, false
	}

	funcName := strings.ToUpper(strings.TrimSpace(funcPart[:funcOpen]))
	switch funcName {
	case "ROW_NUMBER", "RANK", "LAG", "LEAD":
		// supported
	default:
		return ast.WindowFunction{}, false
	}

	argsStr := strings.TrimSpace(funcPart[funcOpen+1 : funcClose])
	var args []string
	if argsStr != "" {
		for _, a := range strings.Split(argsStr, ",") {
			args = append(args, canonicalIdentifier(a))
		}
	}

	// Parse OVER clause: ( PARTITION BY ... ORDER BY ... )
	if len(afterOver) == 0 || afterOver[0] != '(' {
		return ast.WindowFunction{}, false
	}
	closeOver := findMatchingParen(afterOver, 0)
	if closeOver == -1 {
		return ast.WindowFunction{}, false
	}

	overBody := strings.TrimSpace(afterOver[1:closeOver])
	afterOverParen := strings.TrimSpace(afterOver[closeOver+1:])

	// Parse alias: AS alias
	alias := ""
	if strings.HasPrefix(strings.ToUpper(afterOverParen), "AS ") {
		alias = canonicalIdentifier(strings.TrimSpace(afterOverParen[len("AS "):]))
	}
	if alias == "" {
		return ast.WindowFunction{}, false
	}

	// Parse PARTITION BY and ORDER BY inside OVER(...)
	var partition []string
	var orderBy []ast.OrderByClause

	overUpper := strings.ToUpper(overBody)
	partitionIndex := strings.Index(overUpper, "PARTITION BY ")
	orderIndex := strings.Index(overUpper, "ORDER BY ")

	if partitionIndex != -1 {
		partEnd := len(overBody)
		if orderIndex != -1 && orderIndex > partitionIndex {
			partEnd = orderIndex
		}
		partClause := strings.TrimSpace(overBody[partitionIndex+len("PARTITION BY ") : partEnd])
		for _, p := range strings.Split(partClause, ",") {
			col := canonicalIdentifier(p)
			if col != "" {
				partition = append(partition, col)
			}
		}
	}

	if orderIndex != -1 {
		orderClause := strings.TrimSpace(overBody[orderIndex+len("ORDER BY "):])
		parsed, err := parseOrderBy(orderClause)
		if err == nil {
			orderBy = parsed
		}
	}

	return ast.WindowFunction{
		Function:  funcName,
		Args:      args,
		Partition: partition,
		OrderBy:   orderBy,
		Alias:     alias,
	}, true
}

// parseJsonAccessColumn parses a JSON access expression as a SELECT column:
// col->>'key' AS alias, col->'key'->>'nested' AS alias
func parseJsonAccessColumn(expr string) (ast.JsonAccess, bool) {
	if !strings.Contains(expr, "->") {
		return ast.JsonAccess{}, false
	}

	// Check for AS alias first.
	alias := ""
	body := expr
	upperExpr := strings.ToUpper(expr)
	if idx := strings.LastIndex(upperExpr, " AS "); idx != -1 {
		alias = canonicalIdentifier(strings.TrimSpace(expr[idx+4:]))
		body = strings.TrimSpace(expr[:idx])
	}

	ja, ok := parseJsonAccessExpr(body)
	if !ok {
		return ast.JsonAccess{}, false
	}

	if alias != "" {
		ja.Alias = alias
	}
	// Auto-generate alias from last path key if none provided.
	if ja.Alias == "" && len(ja.Path) > 0 {
		ja.Alias = ja.Path[len(ja.Path)-1]
	}

	return ja, true
}

// parseJsonAccessExpr parses a chain of -> and ->> operators:
// col->>'key', col->'key'->>'nested', col->'a'->'b'->>'c'
func parseJsonAccessExpr(expr string) (ast.JsonAccess, bool) {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" || !strings.Contains(trimmed, "->") {
		return ast.JsonAccess{}, false
	}

	// Tokenise into segments split on ->> and -> (greedy: ->> first).
	// Result: column name, then alternating operator/key pairs.
	var column string
	var path []string
	textMode := false

	remaining := trimmed
	// Find the first -> or ->> to extract the column name.
	idx := strings.Index(remaining, "->>")
	idxArrow := strings.Index(remaining, "->")
	if idxArrow < 0 {
		return ast.JsonAccess{}, false
	}

	column = canonicalIdentifier(strings.TrimSpace(remaining[:idxArrow]))
	if column == "" {
		return ast.JsonAccess{}, false
	}

	// Process the chain.
	remaining = remaining[idxArrow:]
	for remaining != "" {
		if strings.HasPrefix(remaining, "->>") {
			textMode = true
			remaining = remaining[3:]
		} else if strings.HasPrefix(remaining, "->") {
			textMode = false
			remaining = remaining[2:]
		} else {
			return ast.JsonAccess{}, false
		}

		remaining = strings.TrimSpace(remaining)

		// Extract key — must be a quoted string 'key'.
		if len(remaining) < 2 || remaining[0] != '\'' {
			return ast.JsonAccess{}, false
		}

		closeQuote := strings.Index(remaining[1:], "'")
		if closeQuote < 0 {
			return ast.JsonAccess{}, false
		}
		closeQuote++ // adjust for the offset

		key := remaining[1:closeQuote]
		path = append(path, key)
		remaining = strings.TrimSpace(remaining[closeQuote+1:])

		// Check if the previous arrow was ->>; if so and there are more
		// arrows, that means the textMode applies only to the last hop.
		if strings.HasPrefix(remaining, "->") {
			// More hops coming — this wasn't the final operator.
			// textMode will be reset by the next iteration.
			continue
		}

		// If idx is -1 the last operator's mode stands.
		_ = idx
	}

	if len(path) == 0 {
		return ast.JsonAccess{}, false
	}

	return ast.JsonAccess{
		Column:   column,
		Path:     path,
		TextMode: textMode,
	}, true
}

// ExtractImports splits a compound SQL string into IMPORT directives and
// the remaining SQL. IMPORT directives look like:
//
//	IMPORT domain.table [AS alias]; SELECT ...
//
// Multiple IMPORT directives can be chained before the final SELECT.
// If no IMPORT prefix is found, imports is nil and the SQL is unchanged.
func ExtractImports(sql string) ([]ast.ImportDirective, string, error) {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "IMPORT ") {
		return nil, sql, nil
	}

	// Split by semicolons (respecting string literals).
	segments := splitSemicolons(trimmed)

	var imports []ast.ImportDirective
	selectIdx := -1
	for i, seg := range segments {
		seg = strings.TrimSpace(seg)
		segUpper := strings.ToUpper(seg)
		if !strings.HasPrefix(segUpper, "IMPORT ") {
			selectIdx = i
			break
		}

		imp, err := parseImportDirective(seg)
		if err != nil {
			return nil, "", err
		}
		imports = append(imports, imp)
	}

	if selectIdx == -1 {
		return nil, "", fmt.Errorf("import requires a SELECT statement after the import directives")
	}

	remaining := strings.TrimSpace(strings.Join(segments[selectIdx:], ";"))
	return imports, remaining, nil
}

// parseImportDirective parses a single IMPORT directive:
//
//	IMPORT domain.table [AS alias]
func parseImportDirective(s string) (ast.ImportDirective, error) {
	trimmed := strings.TrimSpace(s)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "IMPORT ") {
		return ast.ImportDirective{}, fmt.Errorf("expected IMPORT directive, got %q", trimmed)
	}

	rest := strings.TrimSpace(trimmed[len("IMPORT "):])

	// Check for AS alias.
	alias := ""
	upperRest := strings.ToUpper(rest)
	if idx := strings.Index(upperRest, " AS "); idx >= 0 {
		alias = strings.TrimSpace(rest[idx+4:])
		rest = strings.TrimSpace(rest[:idx])
	}

	// Parse domain.table.
	parts := strings.SplitN(rest, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ast.ImportDirective{}, fmt.Errorf("import requires qualified name domain.table, got %q", rest)
	}

	return ast.ImportDirective{
		SourceDomain: strings.ToLower(parts[0]),
		SourceTable:  strings.ToLower(parts[1]),
		Alias:        strings.ToLower(alias),
	}, nil
}

// splitSemicolons splits SQL by semicolons, respecting string literals.
func splitSemicolons(sql string) []string {
	var segments []string
	var current strings.Builder
	inString := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			if inString && i+1 < len(sql) && sql[i+1] == '\'' {
				current.WriteByte(ch)
				current.WriteByte(ch)
				i++
				continue
			}
			inString = !inString
		}

		if ch == ';' && !inString {
			seg := strings.TrimSpace(current.String())
			if seg != "" {
				segments = append(segments, seg)
			}
			current.Reset()
			continue
		}

		current.WriteByte(ch)
	}

	if seg := strings.TrimSpace(current.String()); seg != "" {
		segments = append(segments, seg)
	}

	return segments
}

// findTopLevelSetOp scans sql for a top-level UNION [ALL], INTERSECT, or
// EXCEPT keyword that is not inside parentheses or string literals.
// Returns the operation kind and byte position of the keyword, or ("", -1).
func findTopLevelSetOp(sql string) (ast.SetOpKind, int) {
	upper := strings.ToUpper(sql)
	depth := 0
	inString := false
	n := len(upper)

	for i := 0; i < n; i++ {
		ch := upper[i]

		if ch == '\'' {
			if inString && i+1 < n && upper[i+1] == '\'' {
				i++ // skip escaped quote
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		if ch == '(' {
			depth++
			continue
		}
		if ch == ')' {
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth > 0 {
			continue
		}

		// Check for set operation keywords at depth 0.
		// Order matters: check UNION ALL before UNION.
		remaining := upper[i:]
		if strings.HasPrefix(remaining, " UNION ALL ") {
			return ast.SetOpUnionAll, i + 1 // +1 to skip leading space
		}
		if strings.HasPrefix(remaining, " UNION ") {
			return ast.SetOpUnion, i + 1
		}
		if strings.HasPrefix(remaining, " INTERSECT ") {
			return ast.SetOpIntersect, i + 1
		}
		if strings.HasPrefix(remaining, " EXCEPT ") {
			return ast.SetOpExcept, i + 1
		}
	}
	return "", -1
}

// parseSetOperation splits the SQL at the set operation keyword and parses
// the left and right SELECT statements independently.
func parseSetOperation(sql string, op ast.SetOpKind, pos int) (ast.SetOperationStatement, error) {
	opStr := string(op)

	// Split using the keyword position in the original (mixed-case) string.
	leftSQL := strings.TrimSpace(sql[:pos])
	rightSQL := strings.TrimSpace(sql[pos+len(opStr):])

	leftStmt, err := parseSelect(leftSQL)
	if err != nil {
		return ast.SetOperationStatement{}, fmt.Errorf("parse left side of %s: %w", opStr, err)
	}
	leftSel, ok := leftStmt.(ast.SelectStatement)
	if !ok {
		return ast.SetOperationStatement{}, fmt.Errorf("%w: left side of %s must be SELECT", errInvalidSQL, opStr)
	}

	rightStmt, err := parseSelect(rightSQL)
	if err != nil {
		return ast.SetOperationStatement{}, fmt.Errorf("parse right side of %s: %w", opStr, err)
	}
	rightSel, ok := rightStmt.(ast.SelectStatement)
	if !ok {
		return ast.SetOperationStatement{}, fmt.Errorf("%w: right side of %s must be SELECT", errInvalidSQL, opStr)
	}

	return ast.SetOperationStatement{
		Op:    op,
		Left:  leftSel,
		Right: rightSel,
	}, nil
}
