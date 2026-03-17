package executor

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"asql/internal/engine/domains"
	"asql/internal/engine/parser/ast"
	"asql/internal/engine/ports"
	"asql/internal/storage/wal"

	"github.com/klauspost/compress/zstd"
)

// snapshotFileName is kept for backward compatibility with callers
// that still derive the snapshot path from a WAL path.
// Prefer using DataDir.SnapDir() with numbered snapshot files.
func snapshotFileName(walPath string) string {
	return walPath + ".snap"
}

// ---------- on-disk structures (exported fields for JSON) ----------

type persistedSnapshot struct {
	LSN        uint64
	LogicalTS  uint64
	Catalog    persistedCatalog
	Principals []persistedPrincipal
	Domains    map[string]*persistedDomain
}

type persistedCatalog struct {
	// map[domainName] → set of table names
	Domains map[string][]string
}

type persistedPrincipal struct {
	Name         string
	Kind         string
	PasswordHash string
	Enabled      bool
	Roles        []string
	Privileges   []string
}

func principalsStateFromPersisted(principals []persistedPrincipal) map[string]*principalState {
	if len(principals) == 0 {
		return nil
	}
	result := make(map[string]*principalState, len(principals))
	for _, principal := range principals {
		name := normalizePrincipalName(principal.Name)
		roles := make(map[string]struct{}, len(principal.Roles))
		for _, role := range principal.Roles {
			roles[normalizePrincipalName(role)] = struct{}{}
		}
		privileges := make(map[PrincipalPrivilege]struct{}, len(principal.Privileges))
		for _, privilege := range principal.Privileges {
			privileges[PrincipalPrivilege(privilege)] = struct{}{}
		}
		result[name] = &principalState{
			name:         name,
			kind:         PrincipalKind(principal.Kind),
			passwordHash: principal.PasswordHash,
			enabled:      principal.Enabled,
			roles:        roles,
			privileges:   privileges,
		}
	}
	return result
}

func persistedPrincipalsFromState(principals map[string]*principalState) []persistedPrincipal {
	if len(principals) == 0 {
		return nil
	}
	names := make([]string, 0, len(principals))
	for name := range principals {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]persistedPrincipal, 0, len(names))
	for _, name := range names {
		principal := principals[name]
		if principal == nil {
			continue
		}
		roles := make([]string, 0, len(principal.roles))
		for role := range principal.roles {
			roles = append(roles, role)
		}
		sort.Strings(roles)
		privileges := make([]string, 0, len(principal.privileges))
		for privilege := range principal.privileges {
			privileges = append(privileges, string(privilege))
		}
		sort.Strings(privileges)
		result = append(result, persistedPrincipal{
			Name:         principal.name,
			Kind:         string(principal.kind),
			PasswordHash: principal.passwordHash,
			Enabled:      principal.enabled,
			Roles:        roles,
			Privileges:   privileges,
		})
	}
	return result
}

type persistedDomain struct {
	Tables         map[string]*persistedTable
	Entities       map[string]*persistedEntity
	EntityVersions map[string]*persistedEntityVersionIndex
}

type persistedEntity struct {
	Name      string
	RootTable string
	Tables    []string
	FKPaths   map[string][]persistedFKHop
}

type persistedFKHop struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string
}

type persistedEntityVersionIndex struct {
	Versions map[string][]persistedEntityVersion
}

type persistedEntityVersion struct {
	Version   uint64
	CommitLSN uint64
	Tables    []string
}

type persistedTable struct {
	Columns              []string
	ColumnDefinitions    map[string]ast.ColumnDefinition
	Rows                 []map[string]ast.Literal
	decodedRowColumns    []string
	decodedRows          [][]ast.Literal
	Indexes              map[string]*persistedIndex
	IndexedColumns       map[string]string
	IndexedColumnSets    map[string]string
	PrimaryKey           string
	UniqueColumns        []string
	ForeignKeys          []persistedFK
	CheckConstraints     []persistedCheck
	VersionedForeignKeys []persistedVersionedFK
	LastMutationTS       uint64
	ChangeLog            []persistedChangeLogEntry `json:",omitempty"`
}

type persistedChangeLogEntry struct {
	CommitLSN uint64                 `json:"lsn"`
	Operation string                 `json:"op"`
	OldRow    map[string]ast.Literal `json:"old,omitempty"`
	NewRow    map[string]ast.Literal `json:"new,omitempty"`
}

type persistedIndex struct {
	Name           string
	Column         string
	Columns        []string
	Kind           string
	decodedEntries []indexEntry
	// v11+: set to true when bucket/entry data is present so the reader
	// can skip the O(N×indexes) rebuild on startup.
	DataLoaded bool                  `json:",omitempty"`
	Buckets    map[string][]int      `json:",omitempty"` // hash indexes
	Entries    []persistedIndexEntry `json:",omitempty"` // btree indexes
}

// persistedIndexEntry is a single btree entry (value + row position).
type persistedIndexEntry struct {
	Value  ast.Literal   `json:"v"`
	Values []ast.Literal `json:"vs,omitempty"`
	RowID  int           `json:"r"`
}

type persistedFK struct {
	Column           string
	ReferencesTable  string
	ReferencesColumn string
}

type persistedCheck struct {
	Column    string
	Predicate *ast.Predicate
}

type persistedVersionedFK struct {
	Column           string
	LSNColumn        string
	ReferencesDomain string
	ReferencesTable  string
	ReferencesColumn string
}

// ---------- marshal (engine state → persisted) ----------

func tableStateToMarshalable(table *tableState) *persistedTable {
	// Convert positional row slices to maps for persistence.
	persistedRows := make([]map[string]ast.Literal, len(table.rows))
	for i, rowSlice := range table.rows {
		persistedRows[i] = rowToMap(table, rowSlice)
	}

	pt := &persistedTable{
		Columns:           table.columns,
		ColumnDefinitions: table.columnDefinitions,
		Rows:              persistedRows,
		PrimaryKey:        table.primaryKey,
		LastMutationTS:    table.lastMutationTS,
		// changeLog is intentionally omitted from snapshots/checkpoints.
		// It is rebuilt from the WAL during replay, keeping the WAL as
		// the single source of truth for row-level history (RowHistory,
		// compliance audit). This eliminates the biggest source of
		// snapshot disk bloat.
	}

	// Indexes – persist full materialized data (buckets + entries).
	// Flatten the COW overlay chain first so we capture the complete view.
	// This eliminates the O(N×indexes) rebuild on startup (snapshot v11+).
	if len(table.indexes) > 0 {
		pt.Indexes = make(map[string]*persistedIndex, len(table.indexes))
		for name, idx := range table.indexes {
			flat := flattenIndex(idx)
			if flat == nil {
				flat = idx
			}
			pi := &persistedIndex{
				Name:       flat.name,
				Column:     flat.column,
				Columns:    flat.columns,
				Kind:       flat.kind,
				DataLoaded: true,
			}
			switch flat.kind {
			case "hash":
				if flat.buckets != nil {
					pi.Buckets = flat.buckets
				} else {
					pi.Buckets = make(map[string][]int)
				}
			case "btree":
				entries := flat.allEntries()
				pi.Entries = make([]persistedIndexEntry, len(entries))
				for i, e := range entries {
					pi.Entries[i] = persistedIndexEntry{
						Value:  e.value,
						Values: e.values,
						RowID:  e.rowID,
					}
				}
			}
			pt.Indexes[name] = pi
		}
	}

	if len(table.indexedColumns) > 0 {
		pt.IndexedColumns = table.indexedColumns
	}
	if len(table.indexedColumnSets) > 0 {
		pt.IndexedColumnSets = table.indexedColumnSets
	}

	// Unique columns
	if len(table.uniqueColumns) > 0 {
		pt.UniqueColumns = make([]string, 0, len(table.uniqueColumns))
		for col := range table.uniqueColumns {
			pt.UniqueColumns = append(pt.UniqueColumns, col)
		}
	}

	// Foreign keys
	if len(table.foreignKeys) > 0 {
		pt.ForeignKeys = make([]persistedFK, len(table.foreignKeys))
		for i, fk := range table.foreignKeys {
			pt.ForeignKeys[i] = persistedFK{
				Column:           fk.column,
				ReferencesTable:  fk.referencesTable,
				ReferencesColumn: fk.referencesColumn,
			}
		}
	}

	// Check constraints
	if len(table.checkConstraints) > 0 {
		pt.CheckConstraints = make([]persistedCheck, len(table.checkConstraints))
		for i, cc := range table.checkConstraints {
			pt.CheckConstraints[i] = persistedCheck{
				Column:    cc.column,
				Predicate: cc.predicate,
			}
		}
	}

	// Versioned foreign keys
	if len(table.versionedForeignKeys) > 0 {
		pt.VersionedForeignKeys = make([]persistedVersionedFK, len(table.versionedForeignKeys))
		for i, vfk := range table.versionedForeignKeys {
			pt.VersionedForeignKeys[i] = persistedVersionedFK{
				Column:           vfk.column,
				LSNColumn:        vfk.lsnColumn,
				ReferencesDomain: vfk.referencesDomain,
				ReferencesTable:  vfk.referencesTable,
				ReferencesColumn: vfk.referencesColumn,
			}
		}
	}

	// changeLog is NOT serialized into snapshots/checkpoints.
	// See comment in tableStateToMarshalable.

	return pt
}

func domainEntitiesToMarshalable(entities map[string]*entityDefinition) map[string]*persistedEntity {
	if len(entities) == 0 {
		return nil
	}
	result := make(map[string]*persistedEntity, len(entities))
	for name, e := range entities {
		pe := &persistedEntity{
			Name:      e.name,
			RootTable: e.rootTable,
		}
		if len(e.tables) > 0 {
			pe.Tables = make([]string, len(e.tables))
			copy(pe.Tables, e.tables)
		}
		if len(e.fkPaths) > 0 {
			pe.FKPaths = make(map[string][]persistedFKHop, len(e.fkPaths))
			for t, hops := range e.fkPaths {
				pHops := make([]persistedFKHop, len(hops))
				for i, h := range hops {
					pHops[i] = persistedFKHop{
						FromTable:  h.fromTable,
						FromColumn: h.fromColumn,
						ToTable:    h.toTable,
						ToColumn:   h.toColumn,
					}
				}
				pe.FKPaths[t] = pHops
			}
		}
		result[name] = pe
	}
	return result
}

func domainEntityVersionsToMarshalable(versions map[string]*entityVersionIndex) map[string]*persistedEntityVersionIndex {
	if len(versions) == 0 {
		return nil
	}
	result := make(map[string]*persistedEntityVersionIndex, len(versions))
	for name, idx := range versions {
		pIdx := &persistedEntityVersionIndex{}
		// Flatten overlay chain before serialization to get complete view.
		flat := flattenEntityVersionIndex(idx)
		if flat != nil && len(flat.versions) > 0 {
			pIdx.Versions = make(map[string][]persistedEntityVersion, len(flat.versions))
			for pk, vers := range flat.versions {
				pVers := make([]persistedEntityVersion, len(vers))
				for i, v := range vers {
					tables := make([]string, len(v.tables))
					copy(tables, v.tables)
					pVers[i] = persistedEntityVersion{
						Version:   v.version,
						CommitLSN: v.commitLSN,
						Tables:    tables,
					}
				}
				pIdx.Versions[pk] = pVers
			}
		}
		result[name] = pIdx
	}
	return result
}

// ---------- unmarshal (persisted → engine state) ----------

func marshalableToSnapshot(ps persistedSnapshot) engineSnapshot {
	snap := engineSnapshot{
		lsn:       ps.LSN,
		logicalTS: ps.LogicalTS,
		state:     engineState{domains: make(map[string]*domainState, len(ps.Domains)), principals: make(map[string]*principalState, len(ps.Principals))},
	}

	// Catalog
	cat := domains.NewCatalog()
	for domain, tables := range ps.Catalog.Domains {
		cat.EnsureDomain(domain)
		for _, table := range tables {
			cat.RegisterTable(domain, table)
		}
	}
	snap.catalog = cat

	snap.state.principals = principalsStateFromPersisted(ps.Principals)

	// State
	for domainName, pd := range ps.Domains {
		ds := &domainState{tables: make(map[string]*tableState, len(pd.Tables))}
		for tableName, pt := range pd.Tables {
			ds.tables[tableName] = marshalableToTableState(pt)
		}
		// Entities
		if len(pd.Entities) > 0 {
			ds.entities = make(map[string]*entityDefinition, len(pd.Entities))
			for name, pe := range pd.Entities {
				tables := make([]string, len(pe.Tables))
				copy(tables, pe.Tables)
				fkPaths := make(map[string][]fkHop, len(pe.FKPaths))
				for t, hops := range pe.FKPaths {
					copied := make([]fkHop, len(hops))
					for i, h := range hops {
						copied[i] = fkHop{
							fromTable:  h.FromTable,
							fromColumn: h.FromColumn,
							toTable:    h.ToTable,
							toColumn:   h.ToColumn,
						}
					}
					fkPaths[t] = copied
				}
				tableSet := make(map[string]struct{}, len(tables))
				for _, t := range tables {
					tableSet[t] = struct{}{}
				}
				ds.entities[name] = &entityDefinition{
					name:      pe.Name,
					rootTable: pe.RootTable,
					tables:    tables,
					tableSet:  tableSet,
					fkPaths:   fkPaths,
				}
			}
			rebuildEntityTablesSet(ds)
		}
		// Entity versions
		if len(pd.EntityVersions) > 0 {
			ds.entityVersions = make(map[string]*entityVersionIndex, len(pd.EntityVersions))
			for name, pev := range pd.EntityVersions {
				versions := make(map[string][]entityVersion, len(pev.Versions))
				for pk, vers := range pev.Versions {
					copied := make([]entityVersion, len(vers))
					for i, ev := range vers {
						tables := make([]string, len(ev.Tables))
						copy(tables, ev.Tables)
						copied[i] = entityVersion{version: ev.Version, commitLSN: ev.CommitLSN, tables: tables}
					}
					versions[pk] = copied
				}
				ds.entityVersions[name] = &entityVersionIndex{versions: versions}
			}
		}
		snap.state.domains[domainName] = ds
	}

	return snap
}

func marshalableToTableState(pt *persistedTable) *tableState {
	var rows [][]ast.Literal
	if len(pt.decodedRows) > 0 {
		if sameStringSlice(pt.Columns, pt.decodedRowColumns) {
			rows = pt.decodedRows
		} else {
			rows = make([][]ast.Literal, len(pt.decodedRows))
			colIndex := make(map[string]int, len(pt.decodedRowColumns))
			for i, col := range pt.decodedRowColumns {
				colIndex[col] = i
			}
			for i, decodedRow := range pt.decodedRows {
				row := make([]ast.Literal, len(pt.Columns))
				for colIdx, col := range pt.Columns {
					if srcIdx, ok := colIndex[col]; ok && srcIdx < len(decodedRow) {
						row[colIdx] = decodedRow[srcIdx]
					} else {
						row[colIdx] = ast.Literal{Kind: ast.LiteralNull}
					}
				}
				rows[i] = row
			}
		}
	} else {
		// Convert persisted map rows to positional slices.
		rows = make([][]ast.Literal, len(pt.Rows))
		for i, rowMap := range pt.Rows {
			rows[i] = rowFromMap(pt.Columns, rowMap)
		}
	}

	columnDefinitions := pt.ColumnDefinitions
	if columnDefinitions == nil {
		columnDefinitions = make(map[string]ast.ColumnDefinition)
	}

	columnIndex := make(map[string]int, len(pt.Columns))
	for i, col := range pt.Columns {
		columnIndex[col] = i
	}

	uniqueColumns := make(map[string]struct{}, len(pt.UniqueColumns))
	uniqueColumnList := make([]string, 0, len(pt.UniqueColumns))
	for _, col := range pt.UniqueColumns {
		uniqueColumns[col] = struct{}{}
		if col != pt.PrimaryKey {
			uniqueColumnList = append(uniqueColumnList, col)
		}
	}

	notNullColumns := make([]string, 0, len(columnDefinitions))
	pkAutoUUID := false
	if pt.PrimaryKey != "" {
		if def, ok := columnDefinitions[pt.PrimaryKey]; ok && def.DefaultValue != nil {
			pkAutoUUID = def.DefaultValue.Kind == ast.DefaultUUIDv7
		}
	}
	for colName, colDef := range columnDefinitions {
		if colDef.NotNull && colName != pt.PrimaryKey {
			notNullColumns = append(notNullColumns, colName)
		}
	}

	indexes := make(map[string]*indexState, len(pt.Indexes))
	indexedColumns := pt.IndexedColumns
	if indexedColumns == nil {
		indexedColumns = make(map[string]string)
	}
	indexedColumnSets := pt.IndexedColumnSets
	if indexedColumnSets == nil {
		indexedColumnSets = make(map[string]string)
	}

	ts := &tableState{
		columns:           pt.Columns,
		columnDefinitions: columnDefinitions,
		columnIndex:       columnIndex,
		rows:              rows,
		primaryKey:        pt.PrimaryKey,
		lastMutationTS:    pt.LastMutationTS,
		indexes:           indexes,
		indexedColumns:    indexedColumns,
		indexedColumnSets: indexedColumnSets,
		uniqueColumns:     uniqueColumns,
		uniqueColumnList:  uniqueColumnList,
		notNullColumns:    notNullColumns,
		pkAutoUUID:        pkAutoUUID,
	}

	// Ensure non-nil slices/maps
	if ts.columns == nil {
		ts.columns = []string{}
	}
	if ts.rows == nil {
		ts.rows = [][]ast.Literal{}
	}

	// Indexes — restore from persisted data when available (v11+), else mark
	// for rebuild from row data.
	for name, pi := range pt.Indexes {
		idx := &indexState{
			name:    pi.Name,
			column:  pi.Column,
			columns: pi.Columns,
			kind:    pi.Kind,
		}
		if pi.DataLoaded {
			switch pi.Kind {
			case "hash":
				if pi.Buckets != nil {
					idx.buckets = pi.Buckets
				} else {
					idx.buckets = make(map[string][]int)
				}
			case "btree":
				if len(pi.decodedEntries) > 0 {
					idx.entries = pi.decodedEntries
				} else {
					idx.entries = make([]indexEntry, len(pi.Entries))
					for i, e := range pi.Entries {
						idx.entries[i] = indexEntry{
							value:  e.Value,
							values: e.Values,
							rowID:  e.RowID,
						}
					}
				}
			default:
				idx.buckets = make(map[string][]int)
			}
			ts.indexesLoaded = true
		} else {
			// Legacy snapshot (v10 or earlier): buckets will be rebuilt below.
			idx.buckets = make(map[string][]int)
		}
		ts.indexes[name] = idx
	}

	// Foreign keys
	if len(pt.ForeignKeys) > 0 {
		ts.foreignKeys = make([]foreignKeyConstraint, len(pt.ForeignKeys))
		for i, fk := range pt.ForeignKeys {
			ts.foreignKeys[i] = foreignKeyConstraint{
				column:           fk.Column,
				referencesTable:  fk.ReferencesTable,
				referencesColumn: fk.ReferencesColumn,
			}
		}
	}

	// Check constraints
	if len(pt.CheckConstraints) > 0 {
		ts.checkConstraints = make([]checkConstraint, len(pt.CheckConstraints))
		for i, cc := range pt.CheckConstraints {
			ts.checkConstraints[i] = checkConstraint{
				column:    cc.Column,
				predicate: cc.Predicate,
			}
		}
	}

	// Versioned foreign keys
	if len(pt.VersionedForeignKeys) > 0 {
		ts.versionedForeignKeys = make([]versionedForeignKeyConstraint, len(pt.VersionedForeignKeys))
		for i, vfk := range pt.VersionedForeignKeys {
			ts.versionedForeignKeys[i] = versionedForeignKeyConstraint{
				column:           vfk.Column,
				lsnColumn:        vfk.LSNColumn,
				referencesDomain: vfk.ReferencesDomain,
				referencesTable:  vfk.ReferencesTable,
				referencesColumn: vfk.ReferencesColumn,
			}
		}
	}

	// Change log
	if len(pt.ChangeLog) > 0 {
		ts.changeLog = make([]changeLogEntry, len(pt.ChangeLog))
		for i, entry := range pt.ChangeLog {
			ts.changeLog[i] = changeLogEntry{
				commitLSN: entry.CommitLSN,
				operation: entry.Operation,
				oldRow:    entry.OldRow,
				newRow:    entry.NewRow,
			}
		}
	}

	return ts
}

func sameStringSlice(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------- disk I/O ----------

// rebuildAllIndexes reconstructs index data for tables that don't have
// pre-loaded index data (i.e., snapshots older than v11).
// For v11+ snapshots all indexes are already populated; this is a no-op.
func rebuildAllIndexes(snap *engineSnapshot) {
	for _, domain := range snap.state.domains {
		for _, table := range domain.tables {
			if table.indexesLoaded {
				// Index data loaded from snapshot — skip O(N) rebuild.
				continue
			}
			rebuildTableIndexes(table)
		}
	}
}

// persistAllSnapshots writes a single checkpoint to disk: the latest
// (highest-LSN) in-memory snapshot. This is the only disk-persisted state;
// intermediate snapshots are kept in memory for time-travel during the session
// and regenerated from the WAL on restart.
// Called during shutdown via WaitPendingSnapshots.
func (engine *Engine) persistAllSnapshots() {
	if engine.snapDir == "" || engine.snapshots == nil {
		return
	}
	started := time.Now()

	engine.snapshots.mu.Lock()
	if len(engine.snapshots.snapshots) == 0 {
		engine.snapshots.mu.Unlock()
		return
	}
	latest := engine.snapshots.snapshots[len(engine.snapshots.snapshots)-1]
	engine.snapshots.mu.Unlock()

	if latest.lsn == engine.lastDiskSnapshotLSN && latest.logicalTS == engine.lastDiskSnapshotLogicalTS {
		return
	}

	// Write the latest snapshot as the single checkpoint file.
	// Every fullSnapshotFrequency-th file (seq-1 divisible) is a full snapshot;
	// the rest are delta snapshots (only tables mutated since the last checkpoint).
	engine.snapSeq++
	isFull := (engine.snapSeq-1)%fullSnapshotFrequency == 0
	prevLogicalTS := engine.lastDiskSnapshotLogicalTS
	if err := writeSnapshotToDir(engine.snapDir, engine.snapSeq, latest, isFull, prevLogicalTS); err != nil {
		slog.Error("snapshot: failed to write disk checkpoint", "lsn", latest.lsn, "seq", engine.snapSeq, "error", err.Error())
		engine.snapSeq-- // revert so next attempt retries the same seq
		return
	}
	engine.lastDiskSnapshotLSN = latest.lsn
	engine.lastDiskSnapshotLogicalTS = latest.logicalTS
	engine.lastDiskSnapshotMutationCount = engine.mutationCount
	slog.Info("snapshot: disk checkpoint written", "lsn", latest.lsn, "seq", engine.snapSeq, "full", isFull)
	if sizer, ok := engine.logStore.(ports.Sizer); ok {
		if sz, err := sizer.TotalSize(); err == nil && sz > 0 {
			engine.lastCheckpointWALSize = uint64(sz)
		}
	}
	if engine.perf != nil {
		engine.perf.recordSnapshotPersist(time.Since(started))
	}
	engine.maybeGCWAL(latest.lsn)

	// Apply retention — keep only the most recent maxDiskSnapshots files.
	_ = cleanupOldSnapshotFiles(engine.snapDir, maxDiskSnapshots)
}

// writeLatestSnapshotToDisk atomically writes only the latest snapshot from
// the in-memory store to disk. The single snapshot is always encoded as full
// (no deltas needed). Intermediate snapshots remain in memory for time-travel
// and get regenerated during WAL replay on restart.
// Uses write-to-temp + rename for crash safety.
func writeLatestSnapshotToDisk(snapshotPath string, store *snapshotStore) error {
	if store == nil || len(store.snapshots) == 0 {
		return nil
	}

	latest := store.snapshots[len(store.snapshots)-1]
	return writeSnapshotToDisk(snapshotPath, latest)
}

// writeSnapshotToDisk atomically writes a single snapshot to disk.
// This function does not hold any locks — the caller provides an immutable
// snapshot value captured at commit time (COW semantics guarantee safety).
// This avoids blocking concurrent commits that need snapStore.mu.
func writeSnapshotToDisk(snapshotPath string, snap engineSnapshot) error {
	tmpStore := &snapshotStore{snapshots: []engineSnapshot{snap}}
	data, err := encodeSnapshotsBinary(tmpStore)
	if err != nil {
		return fmt.Errorf("encode latest snapshot binary: %w", err)
	}
	if data == nil {
		return nil
	}

	compressed, err := compressZstd(data)
	if err != nil {
		return fmt.Errorf("compress snapshot: %w", err)
	}

	target := snapshotPath
	dir := filepath.Dir(target)
	tmp, err := os.CreateTemp(dir, ".asql-snap-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp snapshot file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(compressed); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write snapshot data: %w", err)
	}
	if err := wal.PlatformSync(tmp); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("sync snapshot file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close snapshot file: %w", err)
	}

	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename snapshot file: %w", err)
	}

	return nil
}

// buildChangedDomains serializes only domains/tables that changed since prevLogicalTS.
// Entities and entity versions are always included for domains that have table changes.
func buildChangedDomains(snap *engineSnapshot, prevLogicalTS uint64) map[string]*persistedDomain {
	result := make(map[string]*persistedDomain)
	for domainName, domain := range snap.state.domains {
		pd := &persistedDomain{Tables: make(map[string]*persistedTable)}
		hasChanges := false
		for tableName, table := range domain.tables {
			if table.lastMutationTS > prevLogicalTS {
				pd.Tables[tableName] = tableStateToMarshalable(table)
				hasChanges = true
			}
		}
		if hasChanges {
			pd.Entities = domainEntitiesToMarshalable(domain.entities)
			pd.EntityVersions = domainEntityVersionsToMarshalable(domain.entityVersions)
			result[domainName] = pd
		}
	}
	return result
}

// readAllSnapshotsFromDisk loads every persisted snapshot from the binary file
// at snapshotPath. Returns nil, nil if the file does not exist.
func readAllSnapshotsFromDisk(snapshotPath string) ([]engineSnapshot, error) {
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read snapshot file: %w", err)
	}

	// Decompress if zstd-compressed.
	if isZstd(data) {
		data, err = decompressZstd(data)
		if err != nil {
			return nil, fmt.Errorf("decompress snapshot file: %w", err)
		}
	}

	return decodeSnapshotsBinary(data)
}

func loadRawSnapshotFile(filePath string) (rawSnapshotFileEntry, error) {
	entries, err := loadRawSnapshotEntries(filePath)
	if err != nil {
		return rawSnapshotFileEntry{}, err
	}
	if len(entries) != 1 {
		return rawSnapshotFileEntry{}, fmt.Errorf("decode snapshot file: expected 1 snapshot entry, got %d", len(entries))
	}
	return entries[0], nil
}

func loadRawSnapshotEntries(filePath string) ([]rawSnapshotFileEntry, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read snapshot file: %w", err)
	}

	if isZstd(data) {
		data, err = decompressZstd(data)
		if err != nil {
			return nil, fmt.Errorf("decompress snapshot file: %w", err)
		}
	}

	entries, err := decodeSnapshotFileBinaryRaw(data)
	if err != nil {
		return nil, fmt.Errorf("decode snapshot file: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("decode snapshot file: empty snapshot file")
	}
	return entries, nil
}

// ---------- multi-file snapshot directory I/O ----------
//
// Snapshot files are stored as snap.NNNNNN inside the snapDir directory.
// Each file contains exactly one full zstd-compressed binary snapshot.
// The sequence number is monotonically increasing and allocated by the engine
// under writeMu when a snapshot persist is triggered.

const snapFilePrefix = "snap."

type snapshotDirFile struct {
	seq  uint64
	name string
}

// snapFilePath returns the path for a snapshot file with the given sequence number.
func snapFilePath(snapDir string, seq uint64) string {
	return filepath.Join(snapDir, fmt.Sprintf("%s%06d", snapFilePrefix, seq))
}

// writeSnapshotToDir atomically writes a single snapshot as a new numbered
// file in the snapDir directory.
// isFull=true writes all domains (full checkpoint); isFull=false writes only
// tables mutated after prevLogicalTS (delta checkpoint).
// Callers should pass isFull=(seq%fullSnapshotFrequency==0) and
// prevLogicalTS=lastDiskSnapshotLogicalTS for correct delta chain semantics.
func writeSnapshotToDir(snapDir string, seq uint64, snap engineSnapshot, isFull bool, prevLogicalTS uint64) error {
	data, err := encodeSnapshotFileBinary(&snap, isFull, prevLogicalTS)
	if err != nil {
		return fmt.Errorf("encode snapshot binary: %w", err)
	}
	if data == nil {
		return nil
	}

	compressed, err := compressZstd(data)
	if err != nil {
		return fmt.Errorf("compress snapshot: %w", err)
	}

	target := snapFilePath(snapDir, seq)
	tmp, err := os.CreateTemp(snapDir, ".asql-snap-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp snapshot file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(compressed); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write snapshot data: %w", err)
	}
	if err := wal.PlatformSync(tmp); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("sync snapshot file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close snapshot file: %w", err)
	}

	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename snapshot file: %w", err)
	}

	return nil
}

// readAllSnapshotsFromDir reads all snapshot files from the snapDir directory.
// Files are decoded in seq order so that delta files can be merged with their
// preceding full snapshot. Returns snapshots sorted by LSN (ascending) and
// the highest sequence number found.
// Returns (nil, 0, nil) if the directory is empty or contains no snapshot files.
func readAllSnapshotsFromDir(snapDir string) ([]engineSnapshot, uint64, error) {
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("read snapshot dir: %w", err)
	}

	var files []snapshotDirFile
	var maxSeq uint64

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, snapFilePrefix) {
			continue
		}
		seqStr := strings.TrimPrefix(name, snapFilePrefix)
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
		files = append(files, snapshotDirFile{seq: seq, name: name})
	}

	if len(files) == 0 {
		return nil, 0, nil
	}
	if len(files) == 1 {
		snapshots, err := loadSnapshotsFile(filepath.Join(snapDir, files[0].name))
		if err != nil {
			return nil, 0, fmt.Errorf("%s: %w", files[0].name, err)
		}
		return snapshots, maxSeq, nil
	}

	// Process files in seq order to maintain delta chain integrity.
	sort.Slice(files, func(i, j int) bool {
		return files[i].seq < files[j].seq
	})

	decodedFiles, err := loadRawSnapshotEntriesFromDir(snapDir, files)
	if err != nil {
		return nil, 0, err
	}

	var accumulated map[string]*persistedDomain
	result := make([]engineSnapshot, 0, len(files))

	for fileIdx := range files {
		fileEntries := decodedFiles[fileIdx]
		for _, entry := range fileEntries {
			if entry.isFull || accumulated == nil {
				// Full snapshot (or first file seen — treat orphan delta as full).
				if !entry.isFull {
					slog.Warn("snapshot: delta file has no preceding full snapshot, treating as full",
						"lsn", entry.lsn)
				}
				accumulated = entry.domains
			} else {
				// Delta: overlay changed tables onto accumulated state.
				accumulated = applyDeltaBinary(accumulated, entry.domains, entry.catalog)
			}

			full := persistedSnapshot{
				LSN:        entry.lsn,
				LogicalTS:  entry.logicalTS,
				Catalog:    entry.catalog,
				Principals: entry.principals,
				// `marshalableToSnapshot` below materializes a fresh engine state,
				// so duplicating the accumulated persisted-domain tree first only
				// adds restart-time copy cost without providing extra isolation.
				Domains: accumulated,
			}
			snap := marshalableToSnapshot(full)
			rebuildAllIndexes(&snap)
			result = append(result, snap)
		}
	}

	if len(result) == 0 {
		return nil, 0, nil
	}

	return result, maxSeq, nil

}

func loadSnapshotsFile(path string) ([]engineSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read snapshot file: %w", err)
	}
	if isZstd(data) {
		data, err = decompressZstd(data)
		if err != nil {
			return nil, fmt.Errorf("decompress snapshot file: %w", err)
		}
	}
	if snapshots, ok, err := decodeSingleFullSnapshotBinary(data); err != nil {
		return nil, err
	} else if ok {
		return snapshots, nil
	}
	return decodeSnapshotsBinary(data)
}

func loadRawSnapshotEntriesFromDir(snapDir string, files []snapshotDirFile) ([][]rawSnapshotFileEntry, error) {
	if len(files) == 0 {
		return nil, nil
	}
	if len(files) == 1 {
		entries, err := loadRawSnapshotEntries(filepath.Join(snapDir, files[0].name))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", files[0].name, err)
		}
		return [][]rawSnapshotFileEntry{entries}, nil
	}

	workerCount := runtime.GOMAXPROCS(0)
	if workerCount > len(files) {
		workerCount = len(files)
	}
	if workerCount > 4 {
		workerCount = 4
	}
	if workerCount < 1 {
		workerCount = 1
	}

	results := make([][]rawSnapshotFileEntry, len(files))
	jobs := make(chan int, len(files))
	var wg sync.WaitGroup
	var errOnce sync.Once
	var firstErr error

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				entries, err := loadRawSnapshotEntries(filepath.Join(snapDir, files[idx].name))
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("%s: %w", files[idx].name, err)
					})
					continue
				}
				results[idx] = entries
			}
		}()
	}

	for idx := range files {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

// cleanupOldSnapshotFiles removes the oldest snapshot files in the directory,
// keeping at most 'keep' files. Files are sorted by sequence number and the
// lowest-numbered files are removed first.
func cleanupOldSnapshotFiles(snapDir string, keep int) error {
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return nil // best effort
	}

	type seqFile struct {
		seq  uint64
		name string
	}
	var files []seqFile

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, snapFilePrefix) {
			continue
		}
		seqStr := strings.TrimPrefix(name, snapFilePrefix)
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			continue
		}
		files = append(files, seqFile{seq: seq, name: name})
	}

	if len(files) <= keep {
		return nil
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].seq < files[j].seq
	})

	removeCount := len(files) - keep
	for i := 0; i < removeCount; i++ {
		os.Remove(filepath.Join(snapDir, files[i].name))
	}

	return nil
}

// totalSnapshotDirSize returns the total size in bytes of all snapshot files
// in the directory. Used for PerfStats reporting.
func totalSnapshotDirSize(snapDir string) int64 {
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return 0
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

// migrateOldSnapshot checks for a legacy single-file snapshot (state.snap)
// in the snapDir directory. If found and no numbered snapshot files exist,
// it loads the snapshot and writes it as snap.000001, then removes the old file.
func migrateOldSnapshot(snapDir string) ([]engineSnapshot, uint64, error) {
	legacy := filepath.Join(snapDir, "state.snap")
	data, err := os.ReadFile(legacy)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("read legacy snapshot: %w", err)
	}

	if isZstd(data) {
		data, err = decompressZstd(data)
		if err != nil {
			return nil, 0, fmt.Errorf("decompress legacy snapshot: %w", err)
		}
	}

	snapshots, err := decodeSnapshotsBinary(data)
	if err != nil || len(snapshots) == 0 {
		return nil, 0, err
	}

	// Write as numbered file (always full — no delta base for migration).
	latest := snapshots[len(snapshots)-1]
	if err := writeSnapshotToDir(snapDir, 1, latest, true, 0); err != nil {
		return nil, 0, fmt.Errorf("migrate legacy snapshot: %w", err)
	}

	// Remove old file (best-effort).
	os.Remove(legacy)

	return []engineSnapshot{latest}, 1, nil
}

// ---------- deep-copy helpers for persisted types ----------

func deepCopyPersistedDomains(src map[string]*persistedDomain) map[string]*persistedDomain {
	if src == nil {
		return make(map[string]*persistedDomain)
	}
	dst := make(map[string]*persistedDomain, len(src))
	for name, domain := range src {
		dst[name] = deepCopyPersistedDomain(domain)
	}
	return dst
}

func deepCopyPersistedDomain(src *persistedDomain) *persistedDomain {
	if src == nil {
		return &persistedDomain{Tables: make(map[string]*persistedTable)}
	}
	dst := &persistedDomain{Tables: make(map[string]*persistedTable, len(src.Tables))}
	for name, table := range src.Tables {
		dst.Tables[name] = deepCopyPersistedTable(table)
	}

	// Entities
	if len(src.Entities) > 0 {
		dst.Entities = make(map[string]*persistedEntity, len(src.Entities))
		for name, e := range src.Entities {
			pe := &persistedEntity{
				Name:      e.Name,
				RootTable: e.RootTable,
			}
			if len(e.Tables) > 0 {
				pe.Tables = make([]string, len(e.Tables))
				copy(pe.Tables, e.Tables)
			}
			if len(e.FKPaths) > 0 {
				pe.FKPaths = make(map[string][]persistedFKHop, len(e.FKPaths))
				for t, hops := range e.FKPaths {
					copied := make([]persistedFKHop, len(hops))
					copy(copied, hops)
					pe.FKPaths[t] = copied
				}
			}
			dst.Entities[name] = pe
		}
	}

	// Entity versions
	if len(src.EntityVersions) > 0 {
		dst.EntityVersions = make(map[string]*persistedEntityVersionIndex, len(src.EntityVersions))
		for name, idx := range src.EntityVersions {
			pIdx := &persistedEntityVersionIndex{}
			if len(idx.Versions) > 0 {
				pIdx.Versions = make(map[string][]persistedEntityVersion, len(idx.Versions))
				for pk, vers := range idx.Versions {
					copied := make([]persistedEntityVersion, len(vers))
					for i, v := range vers {
						copied[i] = persistedEntityVersion{
							Version:   v.Version,
							CommitLSN: v.CommitLSN,
						}
						if len(v.Tables) > 0 {
							copied[i].Tables = make([]string, len(v.Tables))
							copy(copied[i].Tables, v.Tables)
						}
					}
					pIdx.Versions[pk] = copied
				}
			}
			dst.EntityVersions[name] = pIdx
		}
	}

	return dst
}

func deepCopyPersistedTable(src *persistedTable) *persistedTable {
	if src == nil {
		return &persistedTable{}
	}
	dst := &persistedTable{
		PrimaryKey:     src.PrimaryKey,
		LastMutationTS: src.LastMutationTS,
	}

	// Columns
	if len(src.Columns) > 0 {
		dst.Columns = make([]string, len(src.Columns))
		copy(dst.Columns, src.Columns)
	}

	// ColumnDefinitions
	if len(src.ColumnDefinitions) > 0 {
		dst.ColumnDefinitions = make(map[string]ast.ColumnDefinition, len(src.ColumnDefinitions))
		for k, v := range src.ColumnDefinitions {
			dst.ColumnDefinitions[k] = v
		}
	}

	// Rows — deep copy each row map
	if len(src.Rows) > 0 {
		dst.Rows = make([]map[string]ast.Literal, len(src.Rows))
		for i, row := range src.Rows {
			nr := make(map[string]ast.Literal, len(row))
			for k, v := range row {
				nr[k] = v
			}
			dst.Rows[i] = nr
		}
	}

	// Indexes
	if len(src.Indexes) > 0 {
		dst.Indexes = make(map[string]*persistedIndex, len(src.Indexes))
		for k, v := range src.Indexes {
			pi := &persistedIndex{
				Name:   v.Name,
				Column: v.Column,
				Kind:   v.Kind,
			}
			if len(v.Columns) > 0 {
				pi.Columns = make([]string, len(v.Columns))
				copy(pi.Columns, v.Columns)
			}
			dst.Indexes[k] = pi
		}
	}

	// IndexedColumns
	if len(src.IndexedColumns) > 0 {
		dst.IndexedColumns = make(map[string]string, len(src.IndexedColumns))
		for k, v := range src.IndexedColumns {
			dst.IndexedColumns[k] = v
		}
	}

	// IndexedColumnSets
	if len(src.IndexedColumnSets) > 0 {
		dst.IndexedColumnSets = make(map[string]string, len(src.IndexedColumnSets))
		for k, v := range src.IndexedColumnSets {
			dst.IndexedColumnSets[k] = v
		}
	}

	// UniqueColumns
	if len(src.UniqueColumns) > 0 {
		dst.UniqueColumns = make([]string, len(src.UniqueColumns))
		copy(dst.UniqueColumns, src.UniqueColumns)
	}

	// Foreign keys
	if len(src.ForeignKeys) > 0 {
		dst.ForeignKeys = make([]persistedFK, len(src.ForeignKeys))
		copy(dst.ForeignKeys, src.ForeignKeys)
	}

	// Check constraints
	if len(src.CheckConstraints) > 0 {
		dst.CheckConstraints = make([]persistedCheck, len(src.CheckConstraints))
		copy(dst.CheckConstraints, src.CheckConstraints)
	}

	// Versioned foreign keys
	if len(src.VersionedForeignKeys) > 0 {
		dst.VersionedForeignKeys = make([]persistedVersionedFK, len(src.VersionedForeignKeys))
		copy(dst.VersionedForeignKeys, src.VersionedForeignKeys)
	}

	// Change log
	if len(src.ChangeLog) > 0 {
		dst.ChangeLog = make([]persistedChangeLogEntry, len(src.ChangeLog))
		for i, entry := range src.ChangeLog {
			e := persistedChangeLogEntry{
				CommitLSN: entry.CommitLSN,
				Operation: entry.Operation,
			}
			if entry.OldRow != nil {
				e.OldRow = make(map[string]ast.Literal, len(entry.OldRow))
				for k, v := range entry.OldRow {
					e.OldRow[k] = v
				}
			}
			if entry.NewRow != nil {
				e.NewRow = make(map[string]ast.Literal, len(entry.NewRow))
				for k, v := range entry.NewRow {
					e.NewRow[k] = v
				}
			}
			dst.ChangeLog[i] = e
		}
	}

	return dst
}

// ---------- zstd compression ----------

// Pooled zstd encoder/decoder to avoid per-call allocation overhead.
var (
	zstdEncoderPool = sync.Pool{
		New: func() any {
			enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
			return enc
		},
	}
	zstdDecoderPool = sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil)
			return dec
		},
	}
)

// zstd magic: 0x28 0xB5 0x2F 0xFD
func isZstd(data []byte) bool {
	return len(data) >= 4 && data[0] == 0x28 && data[1] == 0xB5 && data[2] == 0x2F && data[3] == 0xFD
}

func compressZstd(data []byte) ([]byte, error) {
	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(enc)
	return enc.EncodeAll(data, nil), nil
}

func decompressZstd(data []byte) ([]byte, error) {
	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(dec)
	return dec.DecodeAll(data, nil)
}
