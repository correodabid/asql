package executor

import (
	"fmt"
	"sort"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
)

// rebuildEntityTablesSet rebuilds the domain-level entityTables set
// from all entity definitions. Called when entities are created/modified.
func rebuildEntityTablesSet(domain *domainState) {
	if len(domain.entities) == 0 {
		domain.entityTables = nil
		return
	}
	set := make(map[string]struct{})
	for _, entity := range domain.entities {
		for _, t := range entity.tables {
			set[t] = struct{}{}
		}
	}
	domain.entityTables = set
}

// resolveEntityRootPK follows FK path hops from a child row to extract the root table's PK value as a string key.
// For root table rows, returns the PK directly. Returns ("", false) if resolution fails.
func resolveEntityRootPK(domain *domainState, entity *entityDefinition, tableName string, row map[string]ast.Literal) (string, bool) {
	if tableName == entity.rootTable {
		rootTable := domain.tables[entity.rootTable]
		if rootTable == nil || rootTable.primaryKey == "" {
			return "", false
		}
		pk, ok := row[rootTable.primaryKey]
		if !ok {
			return "", false
		}
		return literalKey(pk), true
	}

	hops, ok := entity.fkPaths[tableName]
	if !ok || len(hops) == 0 {
		return "", false
	}

	// Single-hop fast path: direct child → root (most common case).
	if len(hops) == 1 {
		hop := hops[0]
		parentTable := domain.tables[hop.toTable]
		if parentTable == nil {
			return "", false
		}
		targetKey := literalKey(row[hop.fromColumn])
		parentRow, found := indexLookupRow(parentTable, hop.toColumn, targetKey)
		if !found {
			return "", false
		}
		rootTable := domain.tables[entity.rootTable]
		if rootTable == nil || rootTable.primaryKey == "" {
			return "", false
		}
		return literalKey(parentRow[rootTable.primaryKey]), true
	}

	currentValue := row[hops[0].fromColumn]
	for i, hop := range hops {
		parentTable := domain.tables[hop.toTable]
		if parentTable == nil {
			return "", false
		}
		targetKey := literalKey(currentValue)

		parentRow, found := indexLookupRow(parentTable, hop.toColumn, targetKey)
		if !found {
			return "", false
		}

		if i == len(hops)-1 {
			// Last hop — extract root PK.
			rootTable := domain.tables[entity.rootTable]
			if rootTable == nil || rootTable.primaryKey == "" {
				return "", false
			}
			pk := parentRow[rootTable.primaryKey]
			return literalKey(pk), true
		}
		currentValue = parentRow[hops[i+1].fromColumn]
	}
	return "", false
}

// indexLookupRow finds a single row by column value using the table's hash index.
// Falls back to linear scan if no index exists on the column or if the index
// returns no match (safety net for overlay chain compaction edge cases).
func indexLookupRow(table *tableState, column, key string) (map[string]ast.Literal, bool) {
	if idxName, ok := table.indexedColumns[column]; ok {
		if idx, has := table.indexes[idxName]; has {
			rowIDs := idx.lookupBucket(key)
			if len(rowIDs) > 0 && rowIDs[0] < len(table.rows) {
				return rowToMap(table, table.rows[rowIDs[0]]), true
			}
			// Index miss — fall through to linear scan as safety net.
		}
	}
	// Fallback: linear scan (for tables without index on this column).
	colPos, hasCol := table.columnIndex[column]
	for _, rowSlice := range table.rows {
		if hasCol && colPos < len(rowSlice) && literalKey(rowSlice[colPos]) == key {
			return rowToMap(table, rowSlice), true
		}
	}
	return nil, false
}

// entityFKCacheKey is a batch-level cache key for resolved entity root PKs.
// Avoids redundant FK chain walks when multiple transactions in the same
// commit batch insert children referencing the same parent.
type entityFKCacheKey struct {
	entity  string
	table   string
	fkValue string
}

// collectEntityMutationsSingle is an optimised version of collectEntityMutations
// for the common case of a single affected row (single-row INSERT).
// Avoids allocating a []map[string]ast.Literal wrapper slice.
// fkCache is an optional batch-level cache (nil = no caching).
func collectEntityMutationsSingle(
	domain *domainState,
	plan planner.Plan,
	row map[string]ast.Literal,
	collector map[string]map[string][]string,
	fkCache map[entityFKCacheKey]string,
) {
	if domain.entities == nil {
		return
	}
	if domain.entityTables != nil {
		if _, ok := domain.entityTables[plan.TableName]; !ok {
			return
		}
	}

	for entityName, entity := range domain.entities {
		if _, isPartOfEntity := entity.tableSet[plan.TableName]; !isPartOfEntity {
			continue
		}

		// For root table inserts, resolve PK directly (skip FK chain walk).
		var rootPK string
		var ok bool
		if plan.TableName == entity.rootTable {
			rootTable := domain.tables[entity.rootTable]
			if rootTable != nil && rootTable.primaryKey != "" {
				if pk, has := row[rootTable.primaryKey]; has {
					rootPK = literalKey(pk)
					ok = true
				}
			}
		} else if fkCache != nil {
			// Try batch-level FK cache before doing full chain walk.
			if hops, hasHops := entity.fkPaths[plan.TableName]; hasHops && len(hops) > 0 {
				if fkVal, hasFK := row[hops[0].fromColumn]; hasFK {
					fkStr := literalKey(fkVal)
					ck := entityFKCacheKey{entityName, plan.TableName, fkStr}
					if cached, found := fkCache[ck]; found {
						rootPK = cached
						ok = cached != ""
					} else {
						rootPK, ok = resolveEntityRootPK(domain, entity, plan.TableName, row)
						if ok {
							fkCache[ck] = rootPK
						} else {
							fkCache[ck] = ""
						}
					}
				} else {
					rootPK, ok = resolveEntityRootPK(domain, entity, plan.TableName, row)
				}
			} else {
				rootPK, ok = resolveEntityRootPK(domain, entity, plan.TableName, row)
			}
		} else {
			rootPK, ok = resolveEntityRootPK(domain, entity, plan.TableName, row)
		}

		if !ok {
			continue
		}

		if collector[entityName] == nil {
			collector[entityName] = make(map[string][]string)
		}

		tables := collector[entityName][rootPK]
		alreadyHasTable := false
		for _, t := range tables {
			if t == plan.TableName {
				alreadyHasTable = true
				break
			}
		}
		if !alreadyHasTable {
			collector[entityName][rootPK] = append(tables, plan.TableName)
		}
	}
}

// collectEntityMutations identifies which entity root PKs are affected by a DML plan.
// For DELETE, this must be called BEFORE rows are removed from the table.
// For INSERT/UPDATE, this should be called AFTER the mutation is applied.
func collectEntityMutations(
	domain *domainState,
	plan planner.Plan,
	rows []map[string]ast.Literal,
	collector map[string]map[string][]string,
) {
	if domain.entities == nil {
		return
	}
	// Fast skip: if this table doesn't participate in any entity, return immediately.
	if domain.entityTables != nil {
		if _, ok := domain.entityTables[plan.TableName]; !ok {
			return
		}
	}

	// Single-row fast path: avoid cache setup overhead.
	if len(rows) == 1 {
		collectEntityMutationsSingle(domain, plan, rows[0], collector, nil)
		return
	}

	// Cache resolved root PKs per entity to avoid redundant FK chain lookups
	// when multiple rows share the same foreign key value.
	type cacheKey struct {
		entity string
		fk     string
	}
	var rootPKCache map[cacheKey]string

	for entityName, entity := range domain.entities {
		if _, isPartOfEntity := entity.tableSet[plan.TableName]; !isPartOfEntity {
			continue
		}

		if collector[entityName] == nil {
			collector[entityName] = make(map[string][]string)
		}

		// Determine FK column for cache key (first hop's fromColumn for child tables).
		var fkCol string
		isRootTable := plan.TableName == entity.rootTable
		if !isRootTable {
			if hops, ok := entity.fkPaths[plan.TableName]; ok && len(hops) > 0 {
				fkCol = hops[0].fromColumn
			}
		}

		for _, row := range rows {
			var rootPK string
			var ok bool

			if isRootTable {
				// Root table: resolve PK directly (no FK chain walk).
				rootTable := domain.tables[entity.rootTable]
				if rootTable != nil && rootTable.primaryKey != "" {
					if pk, has := row[rootTable.primaryKey]; has {
						rootPK = literalKey(pk)
						ok = true
					}
				}
			} else if fkCol != "" {
				// Child table with known FK column: use cache.
				fkVal, hasFK := row[fkCol]
				if hasFK {
					fkStr := literalKey(fkVal)
					ck := cacheKey{entityName, fkStr}
					if rootPKCache == nil {
						rootPKCache = make(map[cacheKey]string, len(rows))
					}
					if cached, found := rootPKCache[ck]; found {
						rootPK = cached
						ok = cached != ""
					} else {
						rootPK, ok = resolveEntityRootPK(domain, entity, plan.TableName, row)
						if ok {
							rootPKCache[ck] = rootPK
						} else {
							rootPKCache[ck] = ""
						}
					}
				} else {
					rootPK, ok = resolveEntityRootPK(domain, entity, plan.TableName, row)
				}
			} else {
				rootPK, ok = resolveEntityRootPK(domain, entity, plan.TableName, row)
			}

			if !ok {
				continue
			}
			tables := collector[entityName][rootPK]
			alreadyHasTable := false
			for _, t := range tables {
				if t == plan.TableName {
					alreadyHasTable = true
					break
				}
			}
			if !alreadyHasTable {
				collector[entityName][rootPK] = append(tables, plan.TableName)
			}
		}
	}
}

// recordEntityVersions bumps entity versions for all collected mutations.
// Called once per transaction after all plans are applied.
//
// Uses an overlay-based COW strategy: instead of cloning the entire versions
// map (O(all_entity_root_PKs)), it creates a lightweight overlay that only
// contains the modified PKs. This reduces per-commit COW cost from O(N) to
// O(affected_PKs). The overlay chain is flattened when it exceeds
// maxEntityVersionOverlayDepth.
func recordEntityVersions(
	state *readableState,
	collector map[string]map[string][]string,
	commitLSN uint64,
) {
	clonedDomainMaps := make(map[*domainState]bool)
	overlayIndexes := make(map[*entityVersionIndex]*entityVersionIndex)

	for entityName, rootPKs := range collector {
		for _, domain := range state.domains {
			if domain.entityVersions == nil {
				continue
			}
			origIdx, ok := domain.entityVersions[entityName]
			if !ok {
				continue
			}

			// COW: clone domain entityVersions map once per mutated domain.
			if !clonedDomainMaps[domain] {
				clonedMap := make(map[string]*entityVersionIndex, len(domain.entityVersions))
				for k, v := range domain.entityVersions {
					clonedMap[k] = v
				}
				domain.entityVersions = clonedMap
				clonedDomainMaps[domain] = true
			}

			// COW: create overlay with only affected PKs (O(affected) instead of O(all)).
			idx, alreadyOverlaid := overlayIndexes[origIdx]
			if !alreadyOverlaid {
				// Flatten parent chain if too deep.
				base := origIdx
				if origIdx.depth >= maxEntityVersionOverlayDepth {
					base = flattenEntityVersionIndex(origIdx)
				}
				idx = &entityVersionIndex{
					versions: make(map[string][]entityVersion, len(rootPKs)),
					parent:   base,
					depth:    base.depth + 1,
				}
				overlayIndexes[origIdx] = idx
			}
			domain.entityVersions[entityName] = idx

			for rootPK, tables := range rootPKs {
				existing := idx.getVersions(rootPK)
				nextVersion := uint64(1)
				if len(existing) > 0 {
					nextVersion = existing[len(existing)-1].version + 1
				}
				tablesCopy := make([]string, len(tables))
				copy(tablesCopy, tables)
				sort.Strings(tablesCopy)

				// COW: copy the rootPK version slice before append to avoid
				// mutating shared backing arrays from previous readable states.
				next := make([]entityVersion, len(existing), len(existing)+1)
				copy(next, existing)
				next = append(next, entityVersion{
					version:   nextVersion,
					commitLSN: commitLSN,
					tables:    tablesCopy,
				})
				idx.versions[rootPK] = next
			}
		}
	}
}

// findEntityForTableInDomain returns the entity name and definition that includes
// the given table, or ("", nil) if the table doesn't belong to any entity.
func findEntityForTableInDomain(ds *domainState, tableName string) (string, *entityDefinition) {
	if ds == nil || ds.entities == nil {
		return "", nil
	}
	for name, entity := range ds.entities {
		if _, ok := entity.tableSet[tableName]; ok {
			return name, entity
		}
	}
	return "", nil
}

// resolveEntityVersionCommitLSN looks up the commitLSN for a specific entity
// version number and root PK.
func resolveEntityVersionCommitLSN(ds *domainState, entityName, rootPK string, version uint64) (uint64, bool) {
	if ds.entityVersions == nil {
		return 0, false
	}
	idx, ok := ds.entityVersions[entityName]
	if !ok {
		return 0, false
	}
	versions := idx.getVersions(rootPK)
	for _, v := range versions {
		if v.version == version {
			return v.commitLSN, true
		}
	}
	return 0, false
}

// latestEntityVersion returns the latest version number for a root PK, or (0, false).
func latestEntityVersion(ds *domainState, entityName, rootPK string) (uint64, bool) {
	if ds.entityVersions == nil {
		return 0, false
	}
	idx, ok := ds.entityVersions[entityName]
	if !ok {
		return 0, false
	}
	versions := idx.getVersions(rootPK)
	if len(versions) == 0 {
		return 0, false
	}
	return versions[len(versions)-1].version, true
}

func rebuildTableIndexes(table *tableState) {
	if table == nil || len(table.indexes) == 0 {
		return
	}

	for _, index := range table.indexes {
		index.parent = nil
		index.cachedDepth = 0
		if index.kind == "hash" {
			index.buckets = make(map[string][]int)
			index.entries = nil
		} else {
			index.entries = make([]indexEntry, 0)
			index.buckets = nil
		}
	}

	for rowID, row := range table.rows {
		for _, index := range table.indexes {
			entry, exists := buildIndexEntryForRow(index, row, table.columnIndex, rowID)
			if !exists {
				continue
			}

			if index.kind == "hash" {
				key := literalKey(entry.value)
				index.addToBucket(key, rowID)
				continue
			}
			index.entries = append(index.entries, entry)
		}
	}

	for _, index := range table.indexes {
		if index.kind != "btree" || len(index.entries) <= 1 {
			continue
		}
		sort.Slice(index.entries, func(i, j int) bool {
			cmp := compareIndexEntries(index.entries[i], index.entries[j])
			if cmp != 0 {
				return cmp < 0
			}
			return index.entries[i].rowID < index.entries[j].rowID
		})
	}

	// Set baseSize now that all entries are populated.
	for _, index := range table.indexes {
		if index.kind == "hash" {
			index.baseSize = len(index.buckets)
		} else {
			index.baseSize = len(index.entries)
		}
	}
}

// resolveEntityFKPath uses BFS to find a FK chain from childTable back to rootTable.
// Each hop represents a foreign key relationship: child.fkColumn -> parent.pkColumn.
// Returns the path as a slice of fkHop from child toward root.
func resolveEntityFKPath(domain *domainState, childTable, rootTable string) ([]fkHop, error) {
	if childTable == rootTable {
		return nil, nil
	}

	type bfsNode struct {
		table string
		path  []fkHop
	}

	visited := map[string]bool{childTable: true}
	queue := []bfsNode{{table: childTable, path: nil}}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		table, exists := domain.tables[current.table]
		if !exists {
			continue
		}

		for _, fk := range table.foreignKeys {
			hop := fkHop{
				fromTable:  current.table,
				fromColumn: fk.column,
				toTable:    fk.referencesTable,
				toColumn:   fk.referencesColumn,
			}
			newPath := make([]fkHop, len(current.path)+1)
			copy(newPath, current.path)
			newPath[len(current.path)] = hop

			if fk.referencesTable == rootTable {
				return newPath, nil
			}

			if !visited[fk.referencesTable] {
				visited[fk.referencesTable] = true
				queue = append(queue, bfsNode{table: fk.referencesTable, path: newPath})
			}
		}
	}

	return nil, fmt.Errorf("%w: table %s has no FK path to root table %s", errEntityFKPath, childTable, rootTable)
}
