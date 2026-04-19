package executor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

// RowLSN returns the current visible row-head LSN for a row identified by its
// primary key in a domain-qualified table reference (`domain.table`).
// It returns ok=false when the row or table does not exist.
func (engine *Engine) RowLSN(tableRef, rawPrimaryKey string) (lsn uint64, ok bool, err error) {
	if engine == nil {
		return 0, false, nil
	}

	domainName, tableName, err := splitDomainTableRef(tableRef)
	if err != nil {
		return 0, false, err
	}

	state := engine.readState.Load()
	if state == nil {
		return 0, false, nil
	}
	domainState, ok := state.domains[domainName]
	if !ok {
		return 0, false, nil
	}
	table, ok := domainState.tables[tableName]
	if !ok || table == nil || table.primaryKey == "" {
		return 0, false, nil
	}

	row, ok := lookupRowByPrimaryKey(table, rawPrimaryKey)
	if !ok {
		return 0, false, nil
	}

	rowLSN, ok := row["_lsn"]
	if !ok || rowLSN.Kind != ast.LiteralNumber || rowLSN.NumberValue < 0 {
		return 0, false, nil
	}
	return uint64(rowLSN.NumberValue), true, nil
}

// EntityVersion returns the latest visible version number for an entity root PK.
// It returns ok=false when the entity instance does not exist.
func (engine *Engine) EntityVersion(domain, entityName, rawRootPK string) (version uint64, ok bool, err error) {
	entry, ok, err := engine.latestEntityVersionEntry(domain, entityName, rawRootPK)
	if !ok || err != nil {
		return 0, ok, err
	}
	return entry.version, true, nil
}

// EntityHeadLSN returns the commit LSN of the latest visible version for an
// entity root PK. It returns ok=false when the entity instance does not exist.
func (engine *Engine) EntityHeadLSN(domain, entityName, rawRootPK string) (lsn uint64, ok bool, err error) {
	entry, ok, err := engine.latestEntityVersionEntry(domain, entityName, rawRootPK)
	if !ok || err != nil {
		return 0, ok, err
	}
	return entry.commitLSN, true, nil
}

// EntityVersionLSN returns the commit LSN for a specific visible entity version.
// It returns ok=false when the entity instance or requested version does not exist.
func (engine *Engine) EntityVersionLSN(domain, entityName, rawRootPK string, version uint64) (lsn uint64, ok bool, err error) {
	if engine == nil {
		return 0, false, nil
	}

	state := engine.readState.Load()
	if state == nil {
		return 0, false, nil
	}

	domainState, ok := state.domains[strings.ToLower(strings.TrimSpace(domain))]
	if !ok || domainState == nil || domainState.entityVersions == nil {
		return 0, false, nil
	}

	idx, ok := domainState.entityVersions[strings.ToLower(strings.TrimSpace(entityName))]
	if !ok || idx == nil {
		return 0, false, nil
	}

	for _, candidate := range literalKeyCandidates(rawRootPK) {
		versions := idx.getVersions(candidate)
		for _, entry := range versions {
			if entry.version == version {
				return entry.commitLSN, true, nil
			}
		}
	}

	return 0, false, nil
}

// ResolveReference returns the token that a versioned foreign key would
// auto-capture for the current visible row identified by its primary key.
//
// For entity root tables, the resolved token is the latest visible entity
// version number. For non-entity tables, the resolved token is the row head
// LSN. It returns ok=false when the row or table does not exist.
func (engine *Engine) ResolveReference(tableRef, rawPrimaryKey string) (token uint64, ok bool, err error) {
	if engine == nil {
		return 0, false, nil
	}

	domainName, tableName, err := splitDomainTableRef(tableRef)
	if err != nil {
		return 0, false, err
	}

	state := engine.readState.Load()
	if state == nil {
		return 0, false, nil
	}
	domainState, ok := state.domains[domainName]
	if !ok || domainState == nil {
		return 0, false, nil
	}
	table, ok := domainState.tables[tableName]
	if !ok || table == nil || table.primaryKey == "" {
		return 0, false, nil
	}

	row, ok := lookupRowByPrimaryKey(table, rawPrimaryKey)
	if !ok {
		return 0, false, nil
	}

	entityName, entity := findEntityForTableInDomain(domainState, tableName)
	if entity == nil {
		return engine.RowLSN(tableRef, rawPrimaryKey)
	}
	if tableName != entity.rootTable {
		return 0, false, fmt.Errorf("table %s.%s belongs to entity %q but is not its root table %s", domainName, tableName, entityName, entity.rootTable)
	}

	rootPK, ok := row[table.primaryKey]
	if !ok {
		return 0, false, nil
	}
	version, ok := latestEntityVersion(domainState, entityName, literalKey(rootPK))
	if !ok {
		return 0, false, fmt.Errorf("entity %q has no committed version for root PK %s", entityName, literalKey(rootPK))
	}
	return version, true, nil
}

func (engine *Engine) latestEntityVersionEntry(domain, entityName, rawRootPK string) (entityVersion, bool, error) {
	if engine == nil {
		return entityVersion{}, false, nil
	}

	state := engine.readState.Load()
	if state == nil {
		return entityVersion{}, false, nil
	}

	domainState, ok := state.domains[strings.ToLower(strings.TrimSpace(domain))]
	if !ok || domainState == nil || domainState.entityVersions == nil {
		return entityVersion{}, false, nil
	}

	entityKey := strings.ToLower(strings.TrimSpace(entityName))
	idx, ok := domainState.entityVersions[entityKey]
	if !ok || idx == nil {
		return entityVersion{}, false, nil
	}

	for _, candidate := range literalKeyCandidates(rawRootPK) {
		versions := idx.getVersions(candidate)
		if len(versions) == 0 {
			continue
		}
		return versions[len(versions)-1], true, nil
	}

	return entityVersion{}, false, nil
}

func splitDomainTableRef(tableRef string) (string, string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(tableRef))
	parts := strings.SplitN(trimmed, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("expected domain-qualified table reference, got %q", tableRef)
	}
	return parts[0], parts[1], nil
}

func lookupRowByPrimaryKey(table *tableState, rawPrimaryKey string) (map[string]ast.Literal, bool) {
	if table == nil || table.primaryKey == "" {
		return nil, false
	}
	for _, candidate := range literalKeyCandidates(rawPrimaryKey) {
		if row, ok := indexLookupRow(table, table.primaryKey, candidate); ok {
			return row, true
		}
	}
	return nil, false
}

func literalKeyCandidates(raw string) []string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return []string{"s:"}
	}

	seen := make(map[string]struct{}, 8)
	add := func(value string) {
		if value == "" {
			return
		}
		if _, exists := seen[value]; exists {
			return
		}
		seen[value] = struct{}{}
	}

	if strings.Contains(trimmed, ":") {
		add(trimmed)
	}
	add("s:" + trimmed)

	if number, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		add(literalKey(ast.Literal{Kind: ast.LiteralNumber, NumberValue: number}))
	}
	if floatValue, err := strconv.ParseFloat(trimmed, 64); err == nil {
		add(literalKey(ast.Literal{Kind: ast.LiteralFloat, FloatValue: floatValue}))
	}
	if strings.EqualFold(trimmed, "true") {
		add(literalKey(ast.Literal{Kind: ast.LiteralBoolean, BoolValue: true}))
	}
	if strings.EqualFold(trimmed, "false") {
		add(literalKey(ast.Literal{Kind: ast.LiteralBoolean, BoolValue: false}))
	}
	if strings.EqualFold(trimmed, "null") {
		add(literalKey(ast.Literal{Kind: ast.LiteralNull}))
	}

	keys := make([]string, 0, len(seen))
	for _, candidate := range []string{
		trimmed,
		"s:" + trimmed,
		literalKey(ast.Literal{Kind: ast.LiteralNull}),
		literalKey(ast.Literal{Kind: ast.LiteralBoolean, BoolValue: true}),
		literalKey(ast.Literal{Kind: ast.LiteralBoolean, BoolValue: false}),
	} {
		if _, ok := seen[candidate]; ok {
			keys = append(keys, candidate)
			delete(seen, candidate)
		}
	}
	for candidate := range seen {
		keys = append(keys, candidate)
	}
	return keys
}
