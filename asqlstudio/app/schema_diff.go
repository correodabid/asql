package studioapp

import (
	"fmt"
	"sort"
	"strings"
)

type schemaDiffRequest struct {
	Base   schemaDDLRequest `json:"base"`
	Target schemaDDLRequest `json:"target"`
}

type schemaDiffResponse struct {
	Domain     string                `json:"domain"`
	Safe       bool                  `json:"safe"`
	Operations []schemaDiffOperation `json:"operations"`
	Statements []string              `json:"statements"`
	Warnings   []string              `json:"warnings,omitempty"`
}

type schemaDiffOperation struct {
	Type      string `json:"type"`
	Table     string `json:"table"`
	Column    string `json:"column,omitempty"`
	Statement string `json:"statement,omitempty"`
	Safe      bool   `json:"safe"`
	Breaking  bool   `json:"breaking,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

func BuildSchemaDiff(base schemaDDLRequest, target schemaDDLRequest) (schemaDiffResponse, error) {
	domain, err := normalizeSchemaDomain(target.Domain)
	if err != nil {
		return schemaDiffResponse{}, err
	}

	if _, err := BuildSchemaDDLScript(target); err != nil {
		return schemaDiffResponse{}, err
	}

	baseDomain := strings.TrimSpace(base.Domain)
	if baseDomain == "" {
		baseDomain = domain
	}
	if baseDomain != domain {
		return schemaDiffResponse{}, fmt.Errorf("base domain %q does not match target domain %q", baseDomain, domain)
	}

	baseTables := map[string]schemaDDLTable{}
	targetTables := map[string]schemaDDLTable{}
	baseEntities := map[string]schemaDDLEntity{}
	targetEntities := map[string]schemaDDLEntity{}

	for _, table := range base.Tables {
		baseTables[strings.TrimSpace(table.Name)] = table
	}
	for _, table := range target.Tables {
		targetTables[strings.TrimSpace(table.Name)] = table
	}
	for _, entity := range base.Entities {
		baseEntities[strings.TrimSpace(entity.Name)] = entity
	}
	for _, entity := range target.Entities {
		targetEntities[strings.TrimSpace(entity.Name)] = entity
	}

	operations := make([]schemaDiffOperation, 0)
	statements := make([]string, 0)
	warnings := make([]string, 0)
	safe := true

	baseNames := sortedTableNames(baseTables)
	targetNames := sortedTableNames(targetTables)

	for _, tableName := range targetNames {
		targetTable := targetTables[tableName]
		baseTable, exists := baseTables[tableName]
		if !exists {
			statement, err := buildCreateTableStatement(targetTable)
			if err != nil {
				return schemaDiffResponse{}, err
			}
			operations = append(operations, schemaDiffOperation{
				Type:      "add_table",
				Table:     tableName,
				Statement: statement,
				Safe:      true,
			})
			statements = append(statements, statement)
			continue
		}

		baseColumns := columnMap(baseTable.Columns)
		targetColumns := columnMap(targetTable.Columns)
		baseColumnNames := sortedColumnNames(baseColumns)
		targetColumnNames := sortedColumnNames(targetColumns)

		for _, columnName := range targetColumnNames {
			targetColumn := targetColumns[columnName]
			baseColumn, columnExists := baseColumns[columnName]
			if !columnExists {
				// FK REFERENCES is folded inline into the ADD COLUMN statement.
				// The parser supports: ALTER TABLE t ADD COLUMN col TYPE REFERENCES ref(col)
				statement := buildAddColumnStatement(tableName, targetColumn)
				operations = append(operations, schemaDiffOperation{
					Type:      "add_column",
					Table:     tableName,
					Column:    columnName,
					Statement: statement,
					Safe:      true,
				})
				statements = append(statements, statement)
				continue
			}

			if !columnsEquivalent(baseColumn, targetColumn) {
				// FK-only changes on existing columns cannot be applied automatically —
				// the engine has no ALTER COLUMN or ADD/DROP CONSTRAINT SQL.
				// Classify by impact and show a clear reason; no Statement emitted.
				if columnCoreEquivalent(baseColumn, targetColumn) {
					switch {
					case baseColumn.References == nil && targetColumn.References != nil:
						safe = false
						operations = append(operations, schemaDiffOperation{
							Type:   "add_foreign_key",
							Table:  tableName,
							Column: columnName,
							Safe:   false,
							Reason: fmt.Sprintf("FK REFERENCES %s(%s) added to existing column — cannot apply automatically; drop and re-add the column with the REFERENCES clause", strings.TrimSpace(targetColumn.References.Table), strings.TrimSpace(targetColumn.References.Column)),
						})
						warnings = append(warnings, fmt.Sprintf("%s.%s: FK added to existing column — requires drop + re-add", tableName, columnName))
					case baseColumn.References != nil && targetColumn.References == nil:
						safe = false
						operations = append(operations, schemaDiffOperation{
							Type:     "drop_foreign_key",
							Table:    tableName,
							Column:   columnName,
							Safe:     false,
							Breaking: true,
							Reason:   "FK constraint removed from existing column — cannot apply automatically; drop and re-add the column without the REFERENCES clause",
						})
						warnings = append(warnings, fmt.Sprintf("%s.%s: FK removed — requires drop + re-add", tableName, columnName))
					default:
						safe = false
						operations = append(operations, schemaDiffOperation{
							Type:   "modify_foreign_key",
							Table:  tableName,
							Column: columnName,
							Safe:   false,
							Reason: "FK target changed on existing column — cannot apply automatically; drop and re-add the column with the new REFERENCES clause",
						})
						warnings = append(warnings, fmt.Sprintf("%s.%s: FK target changed — requires drop + re-add", tableName, columnName))
					}
				} else {
					safe = false
					operations = append(operations, schemaDiffOperation{
						Type:   "modify_column",
						Table:  tableName,
						Column: columnName,
						Safe:   false,
						Reason: "column definition changed; type or constraint changes cannot be applied automatically — manual migration (add new column, backfill, drop old) required to avoid data integrity issues",
					})
					warnings = append(warnings, fmt.Sprintf("%s.%s definition changed — requires manual migration", tableName, columnName))
				}
			}
		}

		for _, columnName := range baseColumnNames {
			if _, stillExists := targetColumns[columnName]; !stillExists {
				safe = false
				operations = append(operations, schemaDiffOperation{
					Type:     "drop_column",
					Table:    tableName,
					Column:   columnName,
					Safe:     false,
					Breaking: true,
					Reason:   "live queries referencing this column will fail immediately; WAL retains all historical values so time-travel still works and data is recoverable via replay — no WAL-level data loss",
				})
				warnings = append(warnings, fmt.Sprintf("%s.%s dropped — current data lost, WAL history preserved; rollback requires manual re-add with backfill", tableName, columnName))
			}
		}

		baseVFKs := versionedFKMap(baseTable.VersionedForeignKeys)
		targetVFKs := versionedFKMap(targetTable.VersionedForeignKeys)

		for _, vfkColumn := range sortedVersionedFKColumns(targetVFKs) {
			targetVFK := targetVFKs[vfkColumn]
			baseVFK, exists := baseVFKs[vfkColumn]
			if !exists {
				safe = false
				operations = append(operations, schemaDiffOperation{
					Type:   "add_versioned_foreign_key",
					Table:  tableName,
					Column: vfkColumn,
					Safe:   false,
					Reason: "versioned foreign key addition changes temporal capture semantics and requires explicit preflight review",
				})
				warnings = append(warnings, fmt.Sprintf("%s.%s added as VERSIONED FOREIGN KEY: review temporal capture and historical semantics", tableName, vfkColumn))
				continue
			}
			if !versionedFKsEquivalent(baseVFK, targetVFK) {
				safe = false
				operations = append(operations, schemaDiffOperation{
					Type:   "modify_versioned_foreign_key",
					Table:  tableName,
					Column: vfkColumn,
					Safe:   false,
					Reason: "versioned foreign key definition changed (likely historical-semantics change)",
				})
				warnings = append(warnings, fmt.Sprintf("%s.%s VERSIONED FOREIGN KEY changed: review captured token semantics, replay impact, and rollback path", tableName, vfkColumn))
			}
		}

		for _, vfkColumn := range sortedVersionedFKColumns(baseVFKs) {
			if _, stillExists := targetVFKs[vfkColumn]; stillExists {
				continue
			}
			safe = false
			operations = append(operations, schemaDiffOperation{
				Type:   "drop_versioned_foreign_key",
				Table:  tableName,
				Column: vfkColumn,
				Safe:   false,
				Reason: "removing a versioned foreign key changes historical reference semantics",
			})
			warnings = append(warnings, fmt.Sprintf("%s.%s VERSIONED FOREIGN KEY removed: historical-reference behavior changes", tableName, vfkColumn))
		}

		// Index diff
		baseIndexes := indexMap(baseTable.Indexes)
		targetIndexes := indexMap(targetTable.Indexes)

		for _, idxName := range sortedIndexNames(targetIndexes) {
			targetIdx := targetIndexes[idxName]
			baseIdx, exists := baseIndexes[idxName]
			if !exists {
				colList := strings.Join(targetIdx.Columns, ", ")
				method := strings.ToLower(strings.TrimSpace(targetIdx.Method))
				if method == "" {
					method = "btree"
				}
				statement := fmt.Sprintf("CREATE INDEX %s ON %s (%s) USING %s;", idxName, tableName, colList, method)
				operations = append(operations, schemaDiffOperation{
					Type:      "add_index",
					Table:     tableName,
					Column:    idxName,
					Statement: statement,
					Safe:      true,
				})
				statements = append(statements, statement)
				continue
			}
			if !indexesEquivalent(baseIdx, targetIdx) {
				colList := strings.Join(targetIdx.Columns, ", ")
				method := strings.ToLower(strings.TrimSpace(targetIdx.Method))
				if method == "" {
					method = "btree"
				}
				modifyStmt := fmt.Sprintf("DROP INDEX %s;\nCREATE INDEX %s ON %s (%s) USING %s;", idxName, idxName, tableName, colList, method)
				operations = append(operations, schemaDiffOperation{
					Type:      "modify_index",
					Table:     tableName,
					Column:    idxName,
					Statement: modifyStmt,
					Safe:      false,
					Breaking:  true,
					Reason:    "index must be dropped and recreated — indexes are derived projections with no stored data, so no WAL-level data loss; queries fall back to full scan during the window",
				})
				warnings = append(warnings, fmt.Sprintf("%s: index %s changed — drop and recreate required", tableName, idxName))
				safe = false
			}
		}

		for _, idxName := range sortedIndexNames(baseIndexes) {
			if _, stillExists := targetIndexes[idxName]; !stillExists {
				operations = append(operations, schemaDiffOperation{
					Type:      "drop_index",
					Table:     tableName,
					Column:    idxName,
					Statement: fmt.Sprintf("DROP INDEX %s;", idxName),
					Safe:      false,
					Breaking:  true,
					Reason:    "indexes are derived projections with no stored data — no WAL-level data loss; live queries fall back to full scans until the index is recreated",
				})
				warnings = append(warnings, fmt.Sprintf("%s: index %s removed — no data loss, but queries using this index will fall back to full scan", tableName, idxName))
				safe = false
			}
		}
	}

	for _, tableName := range baseNames {
		if _, exists := targetTables[tableName]; exists {
			continue
		}
		safe = false
		operations = append(operations, schemaDiffOperation{
			Type:      "drop_table",
			Table:     tableName,
			Statement: fmt.Sprintf("DROP TABLE %s;", tableName),
			Safe:      false,
			Breaking:  true,
			Reason:    "live queries against this table will fail immediately; WAL retains all historical rows so time-travel still works and data is recoverable via replay — no WAL-level data loss",
		})
		warnings = append(warnings, fmt.Sprintf("%s dropped — current data lost, WAL history preserved", tableName))
	}

	for _, entityName := range sortedEntityNames(targetEntities) {
		targetEntity := targetEntities[entityName]
		baseEntity, exists := baseEntities[entityName]
		if !exists {
			safe = false
			operations = append(operations, schemaDiffOperation{
				Type:   "add_entity",
				Table:  targetEntity.RootTable,
				Column: entityName,
				Safe:   false,
				Reason: "no data loss, but entity version tracking begins from this LSN forward; rows written before this point will not have captured entity versions and time-travel explanation for those rows will be incomplete — requires explicit migration review",
			})
			warnings = append(warnings, fmt.Sprintf("entity %s added — version tracking starts from this LSN; historical rows pre-date entity capture", entityName))
			continue
		}
		if !entitiesEquivalent(baseEntity, targetEntity) {
			safe = false
			operations = append(operations, schemaDiffOperation{
				Type:   "modify_entity",
				Table:  targetEntity.RootTable,
				Column: entityName,
				Safe:   false,
				Reason: "entity definition changed (root/includes affect historical semantics)",
			})
			warnings = append(warnings, fmt.Sprintf("entity %s changed: review aggregate boundary, captured versions, and replay-visible history", entityName))
		}
	}

	for _, entityName := range sortedEntityNames(baseEntities) {
		if _, exists := targetEntities[entityName]; exists {
			continue
		}
		safe = false
		operations = append(operations, schemaDiffOperation{
			Type:   "drop_entity",
			Column: entityName,
			Safe:   false,
			Reason: "removing an entity changes aggregate-version semantics",
		})
		warnings = append(warnings, fmt.Sprintf("entity %s removed: versioned-reference behavior and historical explanation may change", entityName))
	}

	return schemaDiffResponse{
		Domain:     domain,
		Safe:       safe,
		Operations: operations,
		Statements: statements,
		Warnings:   warnings,
	}, nil
}

func sortedTableNames(tables map[string]schemaDDLTable) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func columnMap(columns []schemaDDLColumn) map[string]schemaDDLColumn {
	mapped := make(map[string]schemaDDLColumn, len(columns))
	for _, column := range columns {
		mapped[strings.TrimSpace(column.Name)] = column
	}
	return mapped
}

func sortedColumnNames(columns map[string]schemaDDLColumn) []string {
	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func versionedFKMap(vfks []schemaDDLVersionedFK) map[string]schemaDDLVersionedFK {
	mapped := make(map[string]schemaDDLVersionedFK, len(vfks))
	for _, vfk := range vfks {
		mapped[strings.TrimSpace(vfk.Column)] = vfk
	}
	return mapped
}

func sortedVersionedFKColumns(vfks map[string]schemaDDLVersionedFK) []string {
	names := make([]string, 0, len(vfks))
	for name := range vfks {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedEntityNames(entities map[string]schemaDDLEntity) []string {
	names := make([]string, 0, len(entities))
	for name := range entities {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func buildCreateTableStatement(table schemaDDLTable) (string, error) {
	tableName := strings.TrimSpace(table.Name)
	if !isValidIdentifier(tableName) {
		return "", fmt.Errorf("invalid table name %q", tableName)
	}
	if len(table.Columns) == 0 {
		return "", fmt.Errorf("table %q must contain at least one column", tableName)
	}

	defs := make([]string, 0, len(table.Columns))
	for _, column := range table.Columns {
		columnDef, err := columnDefinition(column)
		if err != nil {
			return "", fmt.Errorf("table %q: %w", tableName, err)
		}
		defs = append(defs, columnDef)
	}
	return fmt.Sprintf("CREATE TABLE %s (\n  %s\n);", tableName, strings.Join(defs, ",\n  ")), nil
}

func buildAddColumnStatement(tableName string, column schemaDDLColumn) string {
	def, _ := columnDefinition(column)
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", tableName, def)
}

// columnCoreEquivalent checks column equivalence ignoring the FK References field.
// Use this to detect whether a diff is purely a FK change vs. an actual type/constraint change.
func columnCoreEquivalent(left, right schemaDDLColumn) bool {
	if strings.TrimSpace(left.Name) != strings.TrimSpace(right.Name) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(left.Type), strings.TrimSpace(right.Type)) {
		return false
	}
	if left.Nullable != right.Nullable || left.PrimaryKey != right.PrimaryKey || left.Unique != right.Unique {
		return false
	}
	if strings.TrimSpace(left.DefaultValue) != strings.TrimSpace(right.DefaultValue) {
		return false
	}
	return true
}

func columnDefinition(column schemaDDLColumn) (string, error) {
	name := strings.TrimSpace(column.Name)
	if !isValidIdentifier(name) {
		return "", fmt.Errorf("invalid column name %q", name)
	}
	columnType := strings.ToUpper(strings.TrimSpace(column.Type))
	if columnType == "" || !typePattern.MatchString(columnType) {
		return "", fmt.Errorf("invalid type %q for column %s", column.Type, name)
	}

	def := fmt.Sprintf("%s %s", name, columnType)
	if column.PrimaryKey || !column.Nullable {
		def += " NOT NULL"
	}
	if column.PrimaryKey {
		def += " PRIMARY KEY"
	}
	if column.Unique && !column.PrimaryKey {
		def += " UNIQUE"
	}
	if value := strings.TrimSpace(column.DefaultValue); value != "" {
		def += " DEFAULT " + value
	}
	if column.References != nil {
		refTable := strings.TrimSpace(column.References.Table)
		refColumn := strings.TrimSpace(column.References.Column)
		if refTable != "" && refColumn != "" {
			def += fmt.Sprintf(" REFERENCES %s(%s)", refTable, refColumn)
		}
	}
	return def, nil
}

func columnsEquivalent(left schemaDDLColumn, right schemaDDLColumn) bool {
	if strings.TrimSpace(left.Name) != strings.TrimSpace(right.Name) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(left.Type), strings.TrimSpace(right.Type)) {
		return false
	}
	if left.Nullable != right.Nullable || left.PrimaryKey != right.PrimaryKey || left.Unique != right.Unique {
		return false
	}
	if strings.TrimSpace(left.DefaultValue) != strings.TrimSpace(right.DefaultValue) {
		return false
	}
	leftHasRef := left.References != nil
	rightHasRef := right.References != nil
	if leftHasRef != rightHasRef {
		return false
	}
	if !leftHasRef {
		return true
	}
	return strings.TrimSpace(left.References.Table) == strings.TrimSpace(right.References.Table) && strings.TrimSpace(left.References.Column) == strings.TrimSpace(right.References.Column)
}

func versionedFKsEquivalent(left schemaDDLVersionedFK, right schemaDDLVersionedFK) bool {
	return strings.EqualFold(strings.TrimSpace(left.Column), strings.TrimSpace(right.Column)) &&
		strings.EqualFold(strings.TrimSpace(left.LSNColumn), strings.TrimSpace(right.LSNColumn)) &&
		strings.EqualFold(strings.TrimSpace(left.ReferencesDomain), strings.TrimSpace(right.ReferencesDomain)) &&
		strings.EqualFold(strings.TrimSpace(left.ReferencesTable), strings.TrimSpace(right.ReferencesTable)) &&
		strings.EqualFold(strings.TrimSpace(left.ReferencesColumn), strings.TrimSpace(right.ReferencesColumn))
}

func entitiesEquivalent(left schemaDDLEntity, right schemaDDLEntity) bool {
	if !strings.EqualFold(strings.TrimSpace(left.Name), strings.TrimSpace(right.Name)) ||
		!strings.EqualFold(strings.TrimSpace(left.RootTable), strings.TrimSpace(right.RootTable)) {
		return false
	}
	leftTables := append([]string(nil), left.Tables...)
	rightTables := append([]string(nil), right.Tables...)
	for i := range leftTables {
		leftTables[i] = strings.TrimSpace(leftTables[i])
	}
	for i := range rightTables {
		rightTables[i] = strings.TrimSpace(rightTables[i])
	}
	sort.Strings(leftTables)
	sort.Strings(rightTables)
	if len(leftTables) != len(rightTables) {
		return false
	}
	for i := range leftTables {
		if !strings.EqualFold(leftTables[i], rightTables[i]) {
			return false
		}
	}
	return true
}

func indexMap(indexes []schemaDDLIndex) map[string]schemaDDLIndex {
	mapped := make(map[string]schemaDDLIndex, len(indexes))
	for _, idx := range indexes {
		mapped[strings.TrimSpace(idx.Name)] = idx
	}
	return mapped
}

func sortedIndexNames(indexes map[string]schemaDDLIndex) []string {
	names := make([]string, 0, len(indexes))
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func indexesEquivalent(left schemaDDLIndex, right schemaDDLIndex) bool {
	if strings.TrimSpace(left.Name) != strings.TrimSpace(right.Name) {
		return false
	}
	leftMethod := strings.ToLower(strings.TrimSpace(left.Method))
	rightMethod := strings.ToLower(strings.TrimSpace(right.Method))
	if leftMethod == "" {
		leftMethod = "btree"
	}
	if rightMethod == "" {
		rightMethod = "btree"
	}
	if leftMethod != rightMethod {
		return false
	}
	if len(left.Columns) != len(right.Columns) {
		return false
	}
	for i := range left.Columns {
		if strings.TrimSpace(left.Columns[i]) != strings.TrimSpace(right.Columns[i]) {
			return false
		}
	}
	return true
}
