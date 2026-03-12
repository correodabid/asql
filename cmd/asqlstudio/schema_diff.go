package main

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

	for _, table := range base.Tables {
		baseTables[strings.TrimSpace(table.Name)] = table
	}
	for _, table := range target.Tables {
		targetTables[strings.TrimSpace(table.Name)] = table
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
				safe = false
				operations = append(operations, schemaDiffOperation{
					Type:   "modify_column",
					Table:  tableName,
					Column: columnName,
					Safe:   false,
					Reason: "column definition changed (requires manual migration)",
				})
				warnings = append(warnings, fmt.Sprintf("%s.%s changed: manual migration required", tableName, columnName))
			}
		}

		for _, columnName := range baseColumnNames {
			if _, stillExists := targetColumns[columnName]; !stillExists {
				safe = false
				operations = append(operations, schemaDiffOperation{
					Type:   "drop_column",
					Table:  tableName,
					Column: columnName,
					Safe:   false,
					Reason: "column drop is potentially destructive",
				})
				warnings = append(warnings, fmt.Sprintf("%s.%s removed: destructive change", tableName, columnName))
			}
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
				operations = append(operations, schemaDiffOperation{
					Type:   "modify_index",
					Table:  tableName,
					Column: idxName,
					Safe:   false,
					Reason: "index definition changed (drop and recreate manually)",
				})
				warnings = append(warnings, fmt.Sprintf("%s.%s index changed: manual migration required", tableName, idxName))
				safe = false
			}
		}

		for _, idxName := range sortedIndexNames(baseIndexes) {
			if _, stillExists := targetIndexes[idxName]; !stillExists {
				operations = append(operations, schemaDiffOperation{
					Type:   "drop_index",
					Table:  tableName,
					Column: idxName,
					Safe:   false,
					Reason: "index drop is potentially destructive",
				})
				warnings = append(warnings, fmt.Sprintf("%s.%s index removed: destructive change", tableName, idxName))
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
			Type:   "drop_table",
			Table:  tableName,
			Safe:   false,
			Reason: "table drop is potentially destructive",
		})
		warnings = append(warnings, fmt.Sprintf("%s removed: destructive change", tableName))
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
