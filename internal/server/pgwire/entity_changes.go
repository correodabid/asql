package pgwire

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"

	"github.com/jackc/pgx/v5/pgproto3"
)

type tailEntityChangesStatement struct {
	Domain  string
	Entity  string
	RootPK  string
	FromLSN uint64
	ToLSN   uint64
	Limit   int
	Follow  bool
}

type tailEntityChangesPortalState struct {
	statement            tailEntityChangesStatement
	nextFrom             uint64
	remaining            int
	totalRows            int
	completed            bool
	rowDescriptionSent   bool // true once RowDescription has been sent to the client
}

func newTailEntityChangesPortalState(statement tailEntityChangesStatement) *tailEntityChangesPortalState {
	return &tailEntityChangesPortalState{
		statement: statement,
		nextFrom:  statement.FromLSN,
		remaining: statement.Limit,
	}
}

var tailEntityChangesColumns = []string{"commit_lsn", "commit_timestamp", "domain", "entity", "root_pk", "entity_version", "tables"}

func parseTailEntityChangesStatement(sql string) (tailEntityChangesStatement, bool, error) {
	s := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	if !strings.HasPrefix(strings.ToUpper(s), "TAIL ENTITY CHANGES ") {
		return tailEntityChangesStatement{}, false, nil
	}
	rest := strings.TrimSpace(s[len("TAIL ENTITY CHANGES "):])
	ref, remainder := readTailToken(rest)
	parts := strings.SplitN(strings.ToLower(ref), ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return tailEntityChangesStatement{}, true, fmt.Errorf("TAIL ENTITY CHANGES requires a domain.entity target")
	}
	stmt := tailEntityChangesStatement{Domain: parts[0], Entity: parts[1]}
	for remainder != "" {
		switch {
		case strings.HasPrefix(strings.ToUpper(remainder), "FOR "):
			if stmt.RootPK != "" {
				return tailEntityChangesStatement{}, true, fmt.Errorf("TAIL ENTITY CHANGES accepts FOR only once")
			}
			var token string
			token, remainder = readTailToken(strings.TrimSpace(remainder[4:]))
			if token == "" {
				return tailEntityChangesStatement{}, true, fmt.Errorf("TAIL ENTITY CHANGES FOR requires a root primary key")
			}
			stmt.RootPK = trimTailQuotedToken(token)
		case strings.HasPrefix(strings.ToUpper(remainder), "FROM LSN "):
			value, next, err := readTailUint(strings.TrimSpace(remainder[len("FROM LSN "):]))
			if err != nil {
				return tailEntityChangesStatement{}, true, fmt.Errorf("TAIL ENTITY CHANGES FROM LSN: %w", err)
			}
			stmt.FromLSN = value
			remainder = next
		case strings.HasPrefix(strings.ToUpper(remainder), "TO LSN "):
			value, next, err := readTailUint(strings.TrimSpace(remainder[len("TO LSN "):]))
			if err != nil {
				return tailEntityChangesStatement{}, true, fmt.Errorf("TAIL ENTITY CHANGES TO LSN: %w", err)
			}
			stmt.ToLSN = value
			remainder = next
		case strings.HasPrefix(strings.ToUpper(remainder), "LIMIT "):
			value, next, err := readTailUint(strings.TrimSpace(remainder[len("LIMIT "):]))
			if err != nil {
				return tailEntityChangesStatement{}, true, fmt.Errorf("TAIL ENTITY CHANGES LIMIT: %w", err)
			}
			stmt.Limit = int(value)
			remainder = next
		case strings.EqualFold(remainder, "FOLLOW"):
			stmt.Follow = true
			remainder = ""
		default:
			return tailEntityChangesStatement{}, true, fmt.Errorf("unsupported TAIL ENTITY CHANGES clause near %q", remainder)
		}
	}
	if stmt.ToLSN > 0 && stmt.FromLSN > stmt.ToLSN {
		return tailEntityChangesStatement{}, true, fmt.Errorf("TAIL ENTITY CHANGES requires FROM LSN <= TO LSN")
	}
	return stmt, true, nil
}

func tailEntityChangesFields() []pgproto3.FieldDescription {
	return []pgproto3.FieldDescription{
		{Name: []byte("commit_lsn"), DataTypeOID: 20, DataTypeSize: 8, TypeModifier: -1},
		{Name: []byte("commit_timestamp"), DataTypeOID: 1114, DataTypeSize: 8, TypeModifier: -1},
		{Name: []byte("domain"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1},
		{Name: []byte("entity"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1},
		{Name: []byte("root_pk"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1},
		{Name: []byte("entity_version"), DataTypeOID: 20, DataTypeSize: 8, TypeModifier: -1},
		{Name: []byte("tables"), DataTypeOID: 114, DataTypeSize: -1, TypeModifier: -1},
	}
}

func readTailToken(input string) (string, string) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", ""
	}
	if trimmed[0] == '\'' {
		for i := 1; i < len(trimmed); i++ {
			if trimmed[i] == '\'' {
				return trimmed[:i+1], strings.TrimSpace(trimmed[i+1:])
			}
		}
		return trimmed, ""
	}
	for i, r := range trimmed {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			return trimmed[:i], strings.TrimSpace(trimmed[i+1:])
		}
	}
	return trimmed, ""
}

func trimTailQuotedToken(token string) string {
	if len(token) >= 2 && token[0] == '\'' && token[len(token)-1] == '\'' {
		return token[1 : len(token)-1]
	}
	return token
}

func readTailUint(input string) (uint64, string, error) {
	token, remainder := readTailToken(input)
	if token == "" {
		return 0, "", fmt.Errorf("missing numeric value")
	}
	value, err := strconv.ParseUint(token, 10, 64)
	if err != nil {
		return 0, "", err
	}
	return value, remainder, nil
}

func entityChangeEventRow(event executor.EntityChangeEvent) (map[string]ast.Literal, error) {
	tablesJSON, err := json.Marshal(event.Tables)
	if err != nil {
		return nil, err
	}
	return map[string]ast.Literal{
		"commit_lsn":       {Kind: ast.LiteralNumber, NumberValue: int64(event.CommitLSN)},
		"commit_timestamp": {Kind: ast.LiteralTimestamp, NumberValue: int64(event.Timestamp)},
		"domain":           {Kind: ast.LiteralString, StringValue: event.Domain},
		"entity":           {Kind: ast.LiteralString, StringValue: event.Entity},
		"root_pk":          {Kind: ast.LiteralString, StringValue: event.RootPK},
		"entity_version":   {Kind: ast.LiteralNumber, NumberValue: int64(event.Version)},
		"tables":           {Kind: ast.LiteralJSON, StringValue: string(tablesJSON)},
	}, nil
}

func entityChangeEventValues(event executor.EntityChangeEvent) ([][]byte, error) {
	row, err := entityChangeEventRow(event)
	if err != nil {
		return nil, err
	}
	values := make([][]byte, 0, len(tailEntityChangesColumns))
	for _, column := range tailEntityChangesColumns {
		lit, ok := row[column]
		if !ok || lit.Kind == ast.LiteralNull || lit.Kind == "" {
			values = append(values, nil)
			continue
		}
		values = append(values, literalToText(lit))
	}
	return values, nil
}
