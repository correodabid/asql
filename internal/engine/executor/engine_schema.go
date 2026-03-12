package executor

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"asql/internal/engine/parser/ast"
)

func (engine *Engine) SchemaSnapshot(domainsFilter []string) SchemaSnapshot {
	state := engine.readState.Load()

	allowed := make(map[string]struct{}, len(domainsFilter))
	for _, domain := range domainsFilter {
		normalized := strings.TrimSpace(strings.ToLower(domain))
		if normalized == "" {
			continue
		}
		allowed[normalized] = struct{}{}
	}

	domainNames := make([]string, 0, len(state.domains))
	for domainName := range state.domains {
		if len(allowed) > 0 {
			if _, ok := allowed[domainName]; !ok {
				continue
			}
		}
		domainNames = append(domainNames, domainName)
	}
	sort.Strings(domainNames)

	snapshot := SchemaSnapshot{Domains: make([]SchemaDomain, 0, len(domainNames))}
	for _, domainName := range domainNames {
		domainState := state.domains[domainName]
		tableNames := make([]string, 0, len(domainState.tables))
		for tableName := range domainState.tables {
			tableNames = append(tableNames, tableName)
		}
		sort.Strings(tableNames)

		domain := SchemaDomain{Name: domainName, Tables: make([]SchemaTable, 0, len(tableNames)), Entities: schemaEntities(domainState)}
		for _, tableName := range tableNames {
			table := domainState.tables[tableName]
			// Skip VFK projection shadow tables — they are internal and must not
			// appear in schema snapshots or be visible to external consumers.
			if table.isProjection {
				continue
			}
			columns := make([]SchemaColumn, 0, len(table.columns))
			for _, columnName := range table.columns {
				definition, hasDefinition := table.columnDefinitions[columnName]
				columnType := "TEXT"
				if hasDefinition && strings.TrimSpace(string(definition.Type)) != "" {
					columnType = strings.ToUpper(strings.TrimSpace(string(definition.Type)))
				}
				column := SchemaColumn{
					Name:             columnName,
					Type:             columnType,
					PrimaryKey:       table.primaryKey == columnName,
					Unique:           table.columnIsUnique(columnName),
					ReferencesTable:  strings.TrimSpace(definition.ReferencesTable),
					ReferencesColumn: strings.TrimSpace(definition.ReferencesColumn),
					DefaultValue:     definition.DefaultValue,
				}
				columns = append(columns, column)
			}
			domain.Tables = append(domain.Tables, SchemaTable{Name: tableName, Columns: columns, Indexes: schemaIndexes(table), VersionedForeignKeys: schemaVersionedFKs(table)})
		}

		snapshot.Domains = append(snapshot.Domains, domain)
	}

	return snapshot
}

func (table *tableState) columnIsUnique(column string) bool {
	if table == nil {
		return false
	}
	_, ok := table.uniqueColumns[column]
	return ok
}

func schemaIndexes(table *tableState) []SchemaIndex {
	if table == nil || len(table.indexes) == 0 {
		return nil
	}
	names := make([]string, 0, len(table.indexes))
	for name := range table.indexes {
		names = append(names, name)
	}
	sort.Strings(names)

	indexes := make([]SchemaIndex, 0, len(names))
	for _, name := range names {
		idx := table.indexes[name]
		cols := make([]string, 0)
		if len(idx.columns) > 0 {
			cols = append(cols, idx.columns...)
		} else if idx.column != "" {
			cols = append(cols, idx.column)
		}
		method := idx.kind
		if method == "" {
			method = "btree"
		}
		indexes = append(indexes, SchemaIndex{Name: name, Columns: cols, Method: method})
	}
	return indexes
}

func schemaVersionedFKs(table *tableState) []SchemaVersionedFK {
	if table == nil || len(table.versionedForeignKeys) == 0 {
		return nil
	}
	result := make([]SchemaVersionedFK, 0, len(table.versionedForeignKeys))
	for _, vfk := range table.versionedForeignKeys {
		result = append(result, SchemaVersionedFK{
			Column:           vfk.column,
			LSNColumn:        vfk.lsnColumn,
			ReferencesDomain: vfk.referencesDomain,
			ReferencesTable:  vfk.referencesTable,
			ReferencesColumn: vfk.referencesColumn,
		})
	}
	return result
}

func schemaEntities(domain *domainState) []SchemaEntity {
	if domain == nil || len(domain.entities) == 0 {
		return nil
	}
	names := make([]string, 0, len(domain.entities))
	for name := range domain.entities {
		names = append(names, name)
	}
	sort.Strings(names)

	entities := make([]SchemaEntity, 0, len(names))
	for _, name := range names {
		entity := domain.entities[name]
		tables := make([]string, len(entity.tables))
		copy(tables, entity.tables)
		entities = append(entities, SchemaEntity{
			Name:      entity.name,
			RootTable: entity.rootTable,
			Tables:    tables,
		})
	}
	return entities
}

func tableHasColumn(table *tableState, column string) bool {
	if table == nil {
		return false
	}

	for _, existing := range table.columns {
		if existing == column {
			return true
		}
	}

	return false
}

func tableContainsValue(table *tableState, column string, value ast.Literal) bool {
	if table == nil || value.Kind == ast.LiteralNull {
		return false
	}

	// Fast path: use hash index on the referenced column (PK / UNIQUE always indexed).
	// After WAL replay, indexes are flattened so the chain is always clean.
	if idxName, ok := table.indexedColumns[column]; ok {
		if idx, exists := table.indexes[idxName]; exists && idx.kind == "hash" {
			return idx.hasBucket(literalKey(value))
		}
	}

	// Fallback for columns without a hash index: linear scan.
	colPos, hasCol := table.columnIndex[column]
	if !hasCol {
		return false
	}
	for _, row := range table.rows {
		if colPos >= len(row) {
			continue
		}
		referenced := row[colPos]
		if referenced.Kind == ast.LiteralNull {
			continue
		}
		if compareLiterals(referenced, value) == 0 {
			return true
		}
	}

	return false
}

func getOrCreateDomain(state *readableState, name string) *domainState {
	if domain, exists := state.domains[name]; exists {
		return domain
	}

	domain := &domainState{tables: make(map[string]*tableState)}
	state.domains[name] = domain
	return domain
}

// allDomainKeys returns a set of all domain names in the map.
func allDomainKeys(domains map[string]*domainState) map[string]struct{} {
	keys := make(map[string]struct{}, len(domains))
	for k := range domains {
		keys[k] = struct{}{}
	}
	return keys
}

func decodeCanonical(payload []byte, target any) error {
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("unmarshal canonical payload: %w", err)
	}

	return nil
}
