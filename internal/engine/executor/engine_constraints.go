package executor

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"asql/internal/engine/domains"
	"asql/internal/engine/parser/ast"
	"asql/internal/engine/planner"
)

// validateInsertRow performs O(1) constraint validation for a single new row
// by checking existing indexes instead of re-scanning the full table.
// When skipPKUnique is true, the PK uniqueness check is skipped
// (safe for auto-generated UUIDv7 PKs that are collision-free).
// fkValCache, when non-nil, caches positive FK-existence lookups so that
// repeated child inserts referencing the same parent value skip redundant
// hasBucket walks of the parent overlay chain.
// Keys are "referencesTable\x00literalKey(value)".
func validateInsertRow(domainState *domainState, table *tableState, row map[string]ast.Literal, skipPKUnique bool, fkValCache map[string]struct{}) error {
	if table == nil {
		return nil
	}

	// PK: null check is always required; uniqueness check skipped for auto-UUID.
	if table.primaryKey != "" {
		value, exists := row[table.primaryKey]
		if !exists || value.Kind == ast.LiteralNull {
			return fmt.Errorf("%w: primary key %s cannot be null", errConstraintPKNull, table.primaryKey)
		}
		if !skipPKUnique {
			if idxName, ok := table.indexedColumns[table.primaryKey]; ok {
				if idx, has := table.indexes[idxName]; has && idx.kind == "hash" {
					if idx.hasBucket(literalKey(value)) {
						return fmt.Errorf("%w: primary key %s duplicate value %s", errConstraintPKDup, table.primaryKey, literalKey(value))
					}
				}
			}
		}
	}

	// UNIQUE: check each unique column via hash index (pre-computed slice).
	for _, col := range table.uniqueColumnList {
		value, exists := row[col]
		if !exists || value.Kind == ast.LiteralNull {
			continue
		}
		if idxName, ok := table.indexedColumns[col]; ok {
			if idx, has := table.indexes[idxName]; has && idx.kind == "hash" {
				if idx.hasBucket(literalKey(value)) {
					return fmt.Errorf("%w: unique %s duplicate value %s", errConstraintUniqueDup, col, literalKey(value))
				}
			}
		}
	}

	// FK: check only the new row against parent tables.
	for _, fk := range table.foreignKeys {
		value, exists := row[fk.column]
		if !exists || value.Kind == ast.LiteralNull {
			continue
		}
		// Build cache key once; used for both lookup and store.
		// Key format: "referencesTable\x00literalKey(value)" — single string,
		// avoids multi-field struct overhead on the GC.
		var ck string
		if fkValCache != nil {
			ck = fk.referencesTable + "\x00" + literalKey(value)
			if _, hit := fkValCache[ck]; hit {
				continue // already validated in this batch
			}
		}
		parent, ok := domainState.tables[fk.referencesTable]
		if !ok {
			return fmt.Errorf("%w: referenced table %s not found", errConstraintFK, fk.referencesTable)
		}
		if !tableContainsValue(parent, fk.referencesColumn, value) {
			return fmt.Errorf("%w: foreign key %s references missing %s(%s)", errConstraintFK, fk.column, fk.referencesTable, fk.referencesColumn)
		}
		// Cache positive result for subsequent rows in the same batch.
		if fkValCache != nil {
			fkValCache[ck] = struct{}{}
		}
	}

	// Check constraints.
	for _, check := range table.checkConstraints {
		if check.predicate == nil {
			continue
		}
		if !checkConstraintSatisfied(row, check.predicate) {
			return fmt.Errorf("%w: check constraint on %s failed", errConstraintCheck, check.column)
		}
	}

	// NOT NULL constraints (pre-computed slice avoids map iteration).
	for _, colName := range table.notNullColumns {
		value, exists := row[colName]
		if !exists || value.Kind == ast.LiteralNull {
			return fmt.Errorf("%w: column %s cannot be null", errConstraintNotNull, colName)
		}
	}

	return nil
}

// expandDomainsForVFKJoins inspects a parsed SELECT statement and, if it
// contains JOINs, auto-expands txDomains to include domains referenced by
// versioned foreign keys defined on the base table. This allows cross-domain
// JOINs to VFK-referenced tables without explicitly listing those domains.
func (engine *Engine) expandDomainsForVFKJoins(statement ast.Statement, txDomains []string) []string {
	sel, ok := statement.(ast.SelectStatement)
	if !ok || len(sel.Joins) == 0 {
		return txDomains
	}

	// Resolve the base table's domain name.
	baseDomain := ""
	baseTableName := strings.ToLower(strings.TrimSpace(sel.TableName))
	if parts := strings.Split(baseTableName, "."); len(parts) == 2 {
		baseDomain = parts[0]
		baseTableName = parts[1]
	} else if len(txDomains) == 1 {
		baseDomain = txDomains[0]
	}
	if baseDomain == "" {
		return txDomains
	}

	// Read the committed engine state to access VFK metadata.
	state := engine.readState.Load()
	ds, ok := state.domains[baseDomain]
	if !ok {
		return txDomains
	}
	table, ok := ds.tables[baseTableName]
	if !ok || len(table.versionedForeignKeys) == 0 {
		return txDomains
	}

	// Build set of already-allowed domains.
	domainSet := make(map[string]struct{}, len(txDomains))
	for _, d := range txDomains {
		domainSet[d] = struct{}{}
	}

	// Add VFK-referenced domains.
	expanded := make([]string, len(txDomains))
	copy(expanded, txDomains)
	for _, vfk := range table.versionedForeignKeys {
		refDomain := strings.ToLower(vfk.referencesDomain)
		if _, exists := domainSet[refDomain]; !exists {
			expanded = append(expanded, refDomain)
			domainSet[refDomain] = struct{}{}
		}
	}

	return expanded
}

// validateVersionedForeignKeys validates versioned FK constraints for a row.
// If the LSN column is absent/NULL, auto-captures either the latest visible
// entity version number (when the referenced table belongs to an entity) or the
// referenced row's current head LSN.
func validateVersionedForeignKeys(state *readableState, engine *Engine, table *tableState, row map[string]ast.Literal, pendingEntityVersions map[string]map[string][]string) error {
	if table == nil || len(table.versionedForeignKeys) == 0 {
		return nil
	}

	for _, vfk := range table.versionedForeignKeys {
		// Get FK value; if NULL/absent, skip validation (like regular FKs).
		fkValue, fkExists := row[vfk.column]
		if !fkExists || fkValue.Kind == ast.LiteralNull {
			continue
		}

		// Get LSN value.
		lsnValue, lsnExists := row[vfk.lsnColumn]
		lsnProvided := lsnExists && lsnValue.Kind != ast.LiteralNull

		if !lsnProvided {
			// Auto-capture: validate against current state, fill version/LSN.
			refDomainState, domainExists := state.domains[vfk.referencesDomain]
			if !domainExists {
				return fmt.Errorf("%w: versioned FK domain %q not found", errConstraintGeneric, vfk.referencesDomain)
			}
			refTable, tableExists := refDomainState.tables[vfk.referencesTable]
			if !tableExists {
				return fmt.Errorf("%w: versioned FK table %s.%s not found", errConstraintGeneric, vfk.referencesDomain, vfk.referencesTable)
			}
			refRow, found := lookupUniqueRow(refTable, vfk.referencesColumn, fkValue)
			if !found {
				return fmt.Errorf("%w: versioned FK %s references missing %s.%s(%s) [lookup=%v rows=%d headLSN=%d]",
					errConstraintGeneric, vfk.column, vfk.referencesDomain, vfk.referencesTable, vfk.referencesColumn,
					fkValue.StringValue, len(refTable.rows), state.headLSN)
			}
			// Check if referenced table belongs to an entity.
			entityName, entity := findEntityForTableInDomain(refDomainState, vfk.referencesTable)
			if entity != nil {
				// Entity-aware auto-capture: store entity version number.
				rootPK := literalKey(fkValue)
				if vfk.referencesTable != entity.rootTable {
					// VPK references a child table — for now, error out.
					return fmt.Errorf("%w: versioned FK %s references child table %s of entity %q; must reference root table %s",
						errConstraintGeneric, vfk.column, vfk.referencesTable, entityName, entity.rootTable)
				}
				version, ok := visibleEntityVersion(refDomainState, entityName, rootPK, pendingEntityVersions)
				if !ok {
					return fmt.Errorf("%w: versioned FK %s: entity %q has no committed version for root PK %s; commit the entity first",
						errConstraintGeneric, vfk.column, entityName, rootPK)
				}
				row[vfk.lsnColumn] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(version)}
			} else {
				// No entity — capture the referenced row's current visible row-head LSN.
				refLSN, ok := refRow["_lsn"]
				if !ok || refLSN.Kind != ast.LiteralNumber {
					return fmt.Errorf("%w: versioned FK %s references %s.%s(%s) but row has no visible _lsn",
						errConstraintGeneric, vfk.column, vfk.referencesDomain, vfk.referencesTable, vfk.referencesColumn)
				}
				row[vfk.lsnColumn] = refLSN
			}
		} else {
			// Explicit value provided: validate accordingly.
			if lsnValue.Kind != ast.LiteralNumber {
				return fmt.Errorf("%w: versioned FK LSN column %s must be an integer", errConstraintGeneric, vfk.lsnColumn)
			}

			refDomainState := state.domains[vfk.referencesDomain]
			entityName, entity := findEntityForTableInDomain(refDomainState, vfk.referencesTable)
			if entity != nil {
				// Value is an entity version number — validate that the version exists.
				// If resolveEntityVersionCommitLSN succeeds, the reference is valid by definition:
				// the entity version was recorded when a mutation was committed for this root PK.
				rootPK := literalKey(fkValue)
				version := uint64(lsnValue.NumberValue)
				_, ok := resolveEntityVersionCommitLSN(refDomainState, entityName, rootPK, version)
				if !ok {
					ok = visiblePendingEntityVersion(pendingEntityVersions, entityName, rootPK, refDomainState, version)
				}
				if !ok {
					return fmt.Errorf("%w: versioned FK %s: entity %q version %d not found for root PK %s",
						errConstraintGeneric, vfk.column, entityName, version, rootPK)
				}
			} else {
				// No entity — raw LSN (current behavior).
				targetLSN := uint64(lsnValue.NumberValue)
				found, err := engine.tableContainsValueAtLSN(state, vfk.referencesDomain, vfk.referencesTable, vfk.referencesColumn, fkValue, targetLSN)
				if err != nil {
					return fmt.Errorf("%w: versioned FK validation error: %w", errConstraintGeneric, err)
				}
				if !found {
					return fmt.Errorf("%w: versioned FK %s references missing %s.%s(%s) at LSN %d",
						errConstraintGeneric, vfk.column, vfk.referencesDomain, vfk.referencesTable, vfk.referencesColumn, targetLSN)
				}
			}
		}
	}
	return nil
}

func lookupUniqueRow(table *tableState, column string, value ast.Literal) (map[string]ast.Literal, bool) {
	if table == nil {
		return nil, false
	}
	return indexLookupRow(table, column, literalKey(value))
}

func visibleEntityVersion(ds *domainState, entityName, rootPK string, pendingEntityVersions map[string]map[string][]string) (uint64, bool) {
	latest, ok := latestEntityVersion(ds, entityName, rootPK)
	if visiblePendingEntityVersion(pendingEntityVersions, entityName, rootPK, ds, latest+1) {
		return latest + 1, true
	}
	if ok {
		return latest, true
	}
	if visiblePendingEntityVersion(pendingEntityVersions, entityName, rootPK, ds, 1) {
		return 1, true
	}
	return 0, false
}

func visiblePendingEntityVersion(pendingEntityVersions map[string]map[string][]string, entityName, rootPK string, ds *domainState, version uint64) bool {
	if pendingEntityVersions == nil || ds == nil {
		return false
	}
	pendingRoots, ok := pendingEntityVersions[entityName]
	if !ok {
		return false
	}
	if _, ok := pendingRoots[rootPK]; !ok {
		return false
	}
	latest, hasLatest := latestEntityVersion(ds, entityName, rootPK)
	if !hasLatest {
		return version == 1
	}
	return version == latest+1
}

// tableContainsValueAtLSN checks whether a row with the given value existed in a table at a specific LSN.
func (engine *Engine) tableContainsValueAtLSN(state *readableState, domain, table, column string, value ast.Literal, targetLSN uint64) (bool, error) {
	// Fast path: target LSN >= current head → check current state.
	if targetLSN >= state.headLSN && state.headLSN > 0 {
		refDomainState, exists := state.domains[domain]
		if !exists {
			return false, nil
		}
		refTable, exists := refDomainState.tables[table]
		if !exists {
			return false, nil
		}
		return tableContainsValue(refTable, column, value), nil
	}

	// Slow path: reconstruct state at targetLSN via snapshots + WAL partial replay.
	if engine == nil || engine.logStore == nil {
		return false, fmt.Errorf("cannot perform historical lookup: engine not available")
	}

	if cachedState, ok := engine.cachedHistoricalState(targetLSN); ok {
		refDomainState, exists := cachedState.domains[domain]
		if !exists {
			return false, nil
		}
		refTable, exists := refDomainState.tables[table]
		if !exists {
			return false, nil
		}
		return tableContainsValue(refTable, column, value), nil
	}

	records, err := engine.readAllRecords(context.Background())
	if err != nil {
		return false, fmt.Errorf("versioned FK historical lookup: %w", err)
	}

	temp := &Engine{
		catalog:          domains.NewCatalog(),
		scanStats:        make(map[scanStrategy]uint64),
		vfkSubscriptions: make(map[string][]projectionSubscription),
		logStore:         engine.logStore,
		snapshots:        engine.snapshots,
	}
	tempInitial := &readableState{domains: make(map[string]*domainState)}
	temp.readState.Store(tempInitial)

	// Try to restore from the closest snapshot.
	snap := engine.closestSnapshot(targetLSN)

	startFromLSN := uint64(0)
	if snap != nil {
		temp.restoreSnapshot(snap)
		startFromLSN = snap.lsn
	}

	if err := temp.rebuildFromRecordsPartial(records, startFromLSN, targetLSN); err != nil {
		return false, fmt.Errorf("versioned FK historical replay: %w", err)
	}

	tempState := temp.readState.Load()
	engine.storeHistoricalState(targetLSN, tempState)
	refDomainState, exists := tempState.domains[domain]
	if !exists {
		return false, nil
	}
	refTable, exists := refDomainState.tables[table]
	if !exists {
		return false, nil
	}
	return tableContainsValue(refTable, column, value), nil
}

// resolveDefaults fills in default values for columns not present in the row.
func resolveDefaults(table *tableState, row map[string]ast.Literal) {
	if table == nil {
		return
	}
	for _, colName := range table.columns {
		if colName == "_lsn" {
			continue
		}
		if _, exists := row[colName]; exists {
			continue
		}
		def, hasDef := table.columnDefinitions[colName]
		if !hasDef || def.DefaultValue == nil {
			continue
		}
		switch def.DefaultValue.Kind {
		case ast.DefaultLiteral:
			row[colName] = def.DefaultValue.Value
		case ast.DefaultAutoIncrement:
			row[colName] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: nextAutoIncrement(table, colName)}
		case ast.DefaultUUIDv7:
			row[colName] = ast.Literal{Kind: ast.LiteralString, StringValue: generateUUIDv7()}
		case ast.DefaultTxTimestamp:
			row[colName] = ast.Literal{Kind: ast.LiteralString, StringValue: generateTxTimestamp()}
		}
	}
}

// resolveVolatileDefaults fills only non-deterministic defaults that must be
// materialized into the WAL exactly once so replay/time-travel reproduce the
// original row values.
func resolveVolatileDefaults(table *tableState, row map[string]ast.Literal) bool {
	if table == nil {
		return false
	}
	changed := false
	for _, colName := range table.columns {
		if colName == "_lsn" {
			continue
		}
		if _, exists := row[colName]; exists {
			continue
		}
		def, hasDef := table.columnDefinitions[colName]
		if !hasDef || def.DefaultValue == nil {
			continue
		}
		switch def.DefaultValue.Kind {
		case ast.DefaultUUIDv7:
			row[colName] = ast.Literal{Kind: ast.LiteralString, StringValue: generateUUIDv7()}
			changed = true
		case ast.DefaultTxTimestamp:
			row[colName] = ast.Literal{Kind: ast.LiteralString, StringValue: generateTxTimestamp()}
			changed = true
		}
	}
	return changed
}

// materializeVolatileInsertDefaultsInPlan resolves non-deterministic INSERT
// defaults into the plan before the mutation is encoded into the WAL. This
// keeps restart replay, time-travel replay, and entity history/version views
// stable without changing commit-time resolution of deterministic defaults like
// AUTO_INCREMENT.
func materializeVolatileInsertDefaultsInPlan(state *readableState, plan *planner.Plan) bool {
	if state == nil || plan == nil || plan.Operation != planner.OperationInsert {
		return false
	}
	domainState := state.domains[plan.DomainName]
	if domainState == nil {
		return false
	}
	table := domainState.tables[plan.TableName]
	if table == nil {
		return false
	}

	originalColumns := append([]string(nil), plan.Columns...)
	buildRow := func(values []ast.Literal) map[string]ast.Literal {
		row := make(map[string]ast.Literal, len(table.columns)+1)
		for i, col := range originalColumns {
			if i < len(values) {
				row[col] = values[i]
			}
		}
		return row
	}

	row := buildRow(plan.Values)
	if !resolveVolatileDefaults(table, row) {
		return false
	}
	plan.Columns, plan.Values = flattenRow(row, table.columns)
	for i, values := range plan.MultiValues {
		multiRow := buildRow(values)
		resolveVolatileDefaults(table, multiRow)
		_, plan.MultiValues[i] = flattenRow(multiRow, table.columns)
	}
	return true
}

// resolveVFKVersions pre-fills VFK lsn_column values at Execute time (before
// queuing) so that INSERT ... RETURNING can return them immediately. This
// mirrors the auto-capture logic in validateVersionedForeignKeys: if the
// referenced table belongs to an entity, store the latest entity version
// number; otherwise store headLSN. The resolved value is baked into the plan
// so that commit-time validation treats it as an explicit value (and
// re-validates it still exists).
func resolveVFKVersions(state *readableState, table *tableState, row map[string]ast.Literal) {
	if table == nil || len(table.versionedForeignKeys) == 0 {
		return
	}
	for _, vfk := range table.versionedForeignKeys {
		// Only auto-fill if lsn_column is absent/NULL.
		if v, exists := row[vfk.lsnColumn]; exists && v.Kind != ast.LiteralNull {
			continue
		}
		// FK value must be present.
		fkValue, fkExists := row[vfk.column]
		if !fkExists || fkValue.Kind == ast.LiteralNull {
			continue
		}
		refDomainState, ok := state.domains[vfk.referencesDomain]
		if !ok {
			continue
		}
		entityName, entity := findEntityForTableInDomain(refDomainState, vfk.referencesTable)
		if entity != nil {
			rootPK := literalKey(fkValue)
			version, ok := latestEntityVersion(refDomainState, entityName, rootPK)
			if ok {
				row[vfk.lsnColumn] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(version)}
			}
		} else {
			row[vfk.lsnColumn] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(state.headLSN)}
		}
	}
}

// nextAutoIncrement computes max(column) + 1 across all existing rows. Returns 1 if the table is empty.
func nextAutoIncrement(table *tableState, column string) int64 {
	colPos, ok := table.columnIndex[column]
	if !ok {
		return 1
	}
	var maxVal int64
	for _, row := range table.rows {
		if colPos < len(row) && row[colPos].Kind == ast.LiteralNumber {
			if row[colPos].NumberValue > maxVal {
				maxVal = row[colPos].NumberValue
			}
		}
	}
	return maxVal + 1
}

// generateUUIDv7 produces a UUID v7 string (timestamp-sortable, RFC 9562).
func generateUUIDv7() string {
	var uuid [16]byte
	ms := uint64(time.Now().UnixMilli())
	binary.BigEndian.PutUint32(uuid[0:4], uint32(ms>>16))
	binary.BigEndian.PutUint16(uuid[4:6], uint16(ms&0xFFFF))
	rand.Read(uuid[6:])
	uuid[6] = (uuid[6] & 0x0F) | 0x70 // version 7
	uuid[8] = (uuid[8] & 0x3F) | 0x80 // variant 10
	// Encode directly into stack buffer — avoids fmt.Sprintf overhead.
	var buf [36]byte
	hex.Encode(buf[0:8], uuid[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], uuid[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], uuid[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], uuid[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], uuid[10:16])
	return string(buf[:])
}

// generateTxTimestamp returns the current wall-clock UTC time as an RFC3339Nano
// string. The value must be materialized into the WAL payload exactly once so
// replay reproduces the same value deterministically.
func generateTxTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// flattenRow converts a row map back into ordered Columns and Values slices,
// following the table's canonical column order. Used by INSERT ... RETURNING
// to bake eagerly-resolved defaults back into the plan.
func flattenRow(row map[string]ast.Literal, tableColumns []string) ([]string, []ast.Literal) {
	cols := make([]string, 0, len(row))
	vals := make([]ast.Literal, 0, len(row))
	for _, col := range tableColumns {
		if col == "_lsn" {
			continue
		}
		if val, ok := row[col]; ok {
			cols = append(cols, col)
			vals = append(vals, val)
		}
	}
	return cols, vals
}

// buildReturningRow extracts the requested columns from a row map.
// If returningCols contains "*", all table columns are returned.
func buildReturningRow(row map[string]ast.Literal, returningCols []string, tableColumns []string) map[string]ast.Literal {
	result := make(map[string]ast.Literal, len(returningCols))
	if len(returningCols) == 1 && returningCols[0] == "*" {
		for _, col := range tableColumns {
			if col == "_lsn" {
				continue
			}
			if val, ok := row[col]; ok {
				result[col] = val
			}
		}
		return result
	}
	for _, col := range returningCols {
		if val, ok := row[col]; ok {
			result[col] = val
		}
	}
	return result
}

// rebuildInsertSQL reconstructs an INSERT statement from resolved columns and values.
// This is used to store the materialized SQL in the WAL after eager default resolution,
// ensuring replay produces identical rows (no regenerated UUIDs).
func rebuildInsertSQL(tableName string, columns []string, values []ast.Literal) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" (")
	for i, col := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
	}
	b.WriteString(") VALUES (")
	for i, val := range values {
		if i > 0 {
			b.WriteString(", ")
		}
		switch val.Kind {
		case ast.LiteralNull:
			b.WriteString("NULL")
		case ast.LiteralNumber:
			b.WriteString(strconv.FormatInt(val.NumberValue, 10))
		case ast.LiteralFloat:
			b.WriteString(strconv.FormatFloat(val.FloatValue, 'f', -1, 64))
		case ast.LiteralBoolean:
			if val.BoolValue {
				b.WriteString("true")
			} else {
				b.WriteString("false")
			}
		case ast.LiteralString, ast.LiteralTimestamp:
			b.WriteByte('\'')
			b.WriteString(strings.ReplaceAll(val.StringValue, "'", "''"))
			b.WriteByte('\'')
		case ast.LiteralJSON:
			b.WriteByte('\'')
			b.WriteString(strings.ReplaceAll(val.StringValue, "'", "''"))
			b.WriteByte('\'')
		default:
			b.WriteByte('\'')
			b.WriteString(strings.ReplaceAll(val.StringValue, "'", "''"))
			b.WriteByte('\'')
		}
	}
	b.WriteString(");")
	return b.String()
}

// rebuildMultiInsertSQL rebuilds a multi-row INSERT SQL statement with resolved defaults.
func rebuildMultiInsertSQL(tableName string, columns []string, firstValues []ast.Literal, extraRows [][]ast.Literal) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(tableName)
	b.WriteString(" (")
	for i, col := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(col)
	}
	b.WriteString(") VALUES ")

	writeRow := func(values []ast.Literal) {
		b.WriteByte('(')
		for i, val := range values {
			if i > 0 {
				b.WriteString(", ")
			}
			switch val.Kind {
			case ast.LiteralNull:
				b.WriteString("NULL")
			case ast.LiteralNumber:
				b.WriteString(strconv.FormatInt(val.NumberValue, 10))
			case ast.LiteralFloat:
				b.WriteString(strconv.FormatFloat(val.FloatValue, 'f', -1, 64))
			case ast.LiteralBoolean:
				if val.BoolValue {
					b.WriteString("true")
				} else {
					b.WriteString("false")
				}
			case ast.LiteralString, ast.LiteralTimestamp:
				b.WriteByte('\'')
				b.WriteString(strings.ReplaceAll(val.StringValue, "'", "''"))
				b.WriteByte('\'')
			case ast.LiteralJSON:
				b.WriteByte('\'')
				b.WriteString(strings.ReplaceAll(val.StringValue, "'", "''"))
				b.WriteByte('\'')
			default:
				b.WriteByte('\'')
				b.WriteString(strings.ReplaceAll(val.StringValue, "'", "''"))
				b.WriteByte('\'')
			}
		}
		b.WriteByte(')')
	}

	writeRow(firstValues)
	for _, row := range extraRows {
		b.WriteString(", ")
		writeRow(row)
	}
	b.WriteByte(';')
	return b.String()
}

func validateConstraints(domainState *domainState, table *tableState, rows []map[string]ast.Literal) error {
	if table == nil {
		return nil
	}

	if table.primaryKey != "" {
		seen := make(map[string]int)
		for rowID, row := range rows {
			value, exists := row[table.primaryKey]
			if !exists || value.Kind == ast.LiteralNull {
				return fmt.Errorf("%w: primary key %s cannot be null", errConstraintPKNull, table.primaryKey)
			}

			key := literalKey(value)
			if previousRowID, ok := seen[key]; ok {
				return fmt.Errorf("%w: primary key %s duplicate between rows %d and %d", errConstraintPKDup, table.primaryKey, previousRowID, rowID)
			}
			seen[key] = rowID
		}
	}

	if len(table.uniqueColumns) == 0 {
		return nil
	}

	columns := make([]string, 0, len(table.uniqueColumns))
	for column := range table.uniqueColumns {
		if column == table.primaryKey {
			continue
		}
		columns = append(columns, column)
	}
	sort.Strings(columns)

	for _, column := range columns {
		seen := make(map[string]int)
		for rowID, row := range rows {
			value, exists := row[column]
			if !exists || value.Kind == ast.LiteralNull {
				continue
			}

			key := literalKey(value)
			if previousRowID, ok := seen[key]; ok {
				return fmt.Errorf("%w: unique %s duplicate between rows %d and %d", errConstraintUniqueDup, column, previousRowID, rowID)
			}
			seen[key] = rowID
		}
	}

	for _, foreignKey := range table.foreignKeys {
		parent, exists := domainState.tables[foreignKey.referencesTable]
		if !exists {
			return fmt.Errorf("%w: referenced table %s not found", errConstraintFK, foreignKey.referencesTable)
		}

		for rowID, row := range rows {
			value, exists := row[foreignKey.column]
			if !exists || value.Kind == ast.LiteralNull {
				continue
			}

			if !tableContainsValue(parent, foreignKey.referencesColumn, value) {
				return fmt.Errorf("%w: foreign key %s references missing %s(%s) for row %d", errConstraintFK, foreignKey.column, foreignKey.referencesTable, foreignKey.referencesColumn, rowID)
			}
		}
	}

	for _, check := range table.checkConstraints {
		if check.predicate == nil {
			continue
		}
		for rowID, row := range rows {
			if !checkConstraintSatisfied(row, check.predicate) {
				return fmt.Errorf("%w: check constraint on %s failed at row %d", errConstraintCheck, check.column, rowID)
			}
		}
	}

	// NOT NULL constraints.
	for colName, colDef := range table.columnDefinitions {
		if !colDef.NotNull {
			continue
		}
		if colName == table.primaryKey {
			continue // PK already validates above.
		}
		for rowID, row := range rows {
			value, exists := row[colName]
			if !exists || value.Kind == ast.LiteralNull {
				return fmt.Errorf("%w: column %s cannot be null at row %d", errConstraintNotNull, colName, rowID)
			}
		}
	}

	return nil
}

func checkConstraintSatisfied(row map[string]ast.Literal, predicate *ast.Predicate) bool {
	result := evaluatePredicate3VL(row, predicate, nil, nil)
	return result == ternaryTrue || result == ternaryUnknown
}

// coerceJSONValues validates and coerces values for JSON-typed columns.
// String values that contain valid JSON are promoted to LiteralJSON.
// Invalid JSON strings and non-null/non-string/non-JSON types are rejected.
func coerceJSONValues(table *tableState, row map[string]ast.Literal) error {
	if table == nil {
		return nil
	}
	for colName, colDef := range table.columnDefinitions {
		if colDef.Type != ast.DataTypeJSON {
			continue
		}
		val, exists := row[colName]
		if !exists || val.Kind == ast.LiteralNull {
			continue
		}
		if val.Kind == ast.LiteralJSON {
			// Already validated by parser.
			continue
		}
		if val.Kind == ast.LiteralString {
			if !json.Valid([]byte(val.StringValue)) {
				return fmt.Errorf("invalid JSON value for column %s", colName)
			}
			row[colName] = ast.Literal{Kind: ast.LiteralJSON, StringValue: val.StringValue}
			continue
		}
		return fmt.Errorf("column %s requires JSON value", colName)
	}
	return nil
}

func validateForeignKeyDefinitions(domainState *domainState, tableName string, uniqueColumns map[string]struct{}, foreignKeys []foreignKeyConstraint) error {
	if domainState == nil || len(foreignKeys) == 0 {
		return nil
	}

	for _, foreignKey := range foreignKeys {
		if foreignKey.referencesTable == tableName {
			if _, unique := uniqueColumns[foreignKey.referencesColumn]; !unique {
				return fmt.Errorf("%w: self-referenced column %s(%s) must be PRIMARY KEY or UNIQUE", errConstraintGeneric, foreignKey.referencesTable, foreignKey.referencesColumn)
			}
			continue
		}

		referencedTable, exists := domainState.tables[foreignKey.referencesTable]
		if !exists {
			return fmt.Errorf("%w: referenced table %s not found", errConstraintFK, foreignKey.referencesTable)
		}

		if !tableHasColumn(referencedTable, foreignKey.referencesColumn) {
			return fmt.Errorf("%w: referenced column %s.%s not found", errConstraintGeneric, foreignKey.referencesTable, foreignKey.referencesColumn)
		}

		if referencedTable.primaryKey != foreignKey.referencesColumn {
			if _, unique := referencedTable.uniqueColumns[foreignKey.referencesColumn]; !unique {
				return fmt.Errorf("%w: referenced column %s.%s must be PRIMARY KEY or UNIQUE", errConstraintGeneric, foreignKey.referencesTable, foreignKey.referencesColumn)
			}
		}
	}

	return nil
}
