package studioapp

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var typePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_(), ]*$`)

type schemaDDLEntity struct {
	Name      string   `json:"name"`
	RootTable string   `json:"root_table"`
	Tables    []string `json:"tables"`
}

type schemaDDLRequest struct {
	Domain   string            `json:"domain"`
	Tables   []schemaDDLTable  `json:"tables"`
	Entities []schemaDDLEntity `json:"entities,omitempty"`
}

type schemaDDLTable struct {
	Name                 string                 `json:"name"`
	Columns              []schemaDDLColumn      `json:"columns"`
	Indexes              []schemaDDLIndex       `json:"indexes,omitempty"`
	VersionedForeignKeys []schemaDDLVersionedFK `json:"versioned_foreign_keys,omitempty"`
}

type schemaDDLIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Method  string   `json:"method"` // "hash" or "btree"
}

type schemaDDLColumn struct {
	Name         string              `json:"name"`
	Type         string              `json:"type"`
	Nullable     bool                `json:"nullable"`
	PrimaryKey   bool                `json:"primary_key"`
	Unique       bool                `json:"unique"`
	DefaultValue string              `json:"default_value,omitempty"`
	References   *schemaDDLReference `json:"references,omitempty"`
}

type schemaDDLReference struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

type schemaDDLVersionedFK struct {
	Column           string `json:"column"`
	LSNColumn        string `json:"lsn_column"`
	ReferencesDomain string `json:"references_domain"`
	ReferencesTable  string `json:"references_table"`
	ReferencesColumn string `json:"references_column"`
}

type schemaDDLResponse struct {
	DDL        string   `json:"ddl"`
	Statements []string `json:"statements"`
}

type fkStatement struct {
	Table      string
	Column     string
	RefTable   string
	RefColumn  string
	Constraint string
}

func BuildSchemaDDLScript(request schemaDDLRequest) (schemaDDLResponse, error) {
	domain, err := normalizeSchemaDomain(request.Domain)
	if err != nil {
		return schemaDDLResponse{}, err
	}
	if len(request.Tables) == 0 {
		return schemaDDLResponse{}, fmt.Errorf("at least one table is required")
	}

	tables := make([]schemaDDLTable, len(request.Tables))
	copy(tables, request.Tables)
	sort.SliceStable(tables, func(left, right int) bool {
		return strings.TrimSpace(tables[left].Name) < strings.TrimSpace(tables[right].Name)
	})

	tableNames := map[string]struct{}{}
	createStatements := make([]string, 0, len(tables))
	indexStatements := make([]string, 0)
	foreignKeys := make([]fkStatement, 0)

	for _, table := range tables {
		tableName := strings.TrimSpace(table.Name)
		if !isValidIdentifier(tableName) {
			return schemaDDLResponse{}, fmt.Errorf("invalid table name %q", tableName)
		}
		if _, exists := tableNames[tableName]; exists {
			return schemaDDLResponse{}, fmt.Errorf("duplicate table name %q", tableName)
		}
		tableNames[tableName] = struct{}{}

		if len(table.Columns) == 0 {
			return schemaDDLResponse{}, fmt.Errorf("table %q must contain at least one column", tableName)
		}

		columnNames := map[string]struct{}{}
		columnDefs := make([]string, 0, len(table.Columns))
		for _, column := range table.Columns {
			columnName := strings.TrimSpace(column.Name)
			if !isValidIdentifier(columnName) {
				return schemaDDLResponse{}, fmt.Errorf("invalid column name %q in table %q", columnName, tableName)
			}
			if _, exists := columnNames[columnName]; exists {
				return schemaDDLResponse{}, fmt.Errorf("duplicate column name %q in table %q", columnName, tableName)
			}
			columnNames[columnName] = struct{}{}

			columnType := strings.ToUpper(strings.TrimSpace(column.Type))
			if columnType == "" || !typePattern.MatchString(columnType) {
				return schemaDDLResponse{}, fmt.Errorf("invalid type %q for %s.%s", column.Type, tableName, columnName)
			}

			definition := fmt.Sprintf("%s %s", columnName, columnType)
			if column.PrimaryKey || !column.Nullable {
				definition += " NOT NULL"
			}
			if column.PrimaryKey {
				definition += " PRIMARY KEY"
			}
			if column.Unique && !column.PrimaryKey {
				definition += " UNIQUE"
			}
			if value := strings.TrimSpace(column.DefaultValue); value != "" {
				definition += " DEFAULT " + value
			}
			columnDefs = append(columnDefs, definition)

			if column.References != nil {
				refTable := strings.TrimSpace(column.References.Table)
				refColumn := strings.TrimSpace(column.References.Column)
				if !isValidIdentifier(refTable) || !isValidIdentifier(refColumn) {
					return schemaDDLResponse{}, fmt.Errorf("invalid foreign key reference in %s.%s", tableName, columnName)
				}
				foreignKeys = append(foreignKeys, fkStatement{
					Table:      tableName,
					Column:     columnName,
					RefTable:   refTable,
					RefColumn:  refColumn,
					Constraint: fmt.Sprintf("fk_%s_%s__%s_%s", tableName, columnName, refTable, refColumn),
				})
			}
		}

		// Versioned FK table-level constraints
		vfkDefs := make([]string, 0, len(table.VersionedForeignKeys))
		for _, vfk := range table.VersionedForeignKeys {
			col := strings.TrimSpace(vfk.Column)
			lsnCol := strings.TrimSpace(vfk.LSNColumn)
			refDomain := strings.TrimSpace(vfk.ReferencesDomain)
			refTable := strings.TrimSpace(vfk.ReferencesTable)
			refColumn := strings.TrimSpace(vfk.ReferencesColumn)
			if col == "" || lsnCol == "" || refTable == "" || refColumn == "" {
				return schemaDDLResponse{}, fmt.Errorf("versioned foreign key in table %q has missing fields", tableName)
			}
			var refTarget string
			if refDomain != "" {
				refTarget = fmt.Sprintf("%s.%s(%s)", refDomain, refTable, refColumn)
			} else {
				refTarget = fmt.Sprintf("%s(%s)", refTable, refColumn)
			}
			vfkDefs = append(vfkDefs, fmt.Sprintf("VERSIONED FOREIGN KEY (%s) REFERENCES %s AS OF %s", col, refTarget, lsnCol))
		}

		allDefs := make([]string, 0, len(columnDefs)+len(vfkDefs))
		allDefs = append(allDefs, columnDefs...)
		allDefs = append(allDefs, vfkDefs...)
		createStatements = append(createStatements, fmt.Sprintf("CREATE TABLE %s (\n  %s\n);", tableName, strings.Join(allDefs, ",\n  ")))

		// Validate and collect index definitions
		indexNames := map[string]struct{}{}
		for _, idx := range table.Indexes {
			idxName := strings.TrimSpace(idx.Name)
			if !isValidIdentifier(idxName) {
				return schemaDDLResponse{}, fmt.Errorf("invalid index name %q on table %q", idxName, tableName)
			}
			if _, exists := indexNames[idxName]; exists {
				return schemaDDLResponse{}, fmt.Errorf("duplicate index name %q on table %q", idxName, tableName)
			}
			indexNames[idxName] = struct{}{}

			if len(idx.Columns) == 0 {
				return schemaDDLResponse{}, fmt.Errorf("index %q on table %q must specify at least one column", idxName, tableName)
			}
			for _, col := range idx.Columns {
				colName := strings.TrimSpace(col)
				if _, exists := columnNames[colName]; !exists {
					return schemaDDLResponse{}, fmt.Errorf("index %q references unknown column %q in table %q", idxName, colName, tableName)
				}
			}

			method := strings.ToLower(strings.TrimSpace(idx.Method))
			if method == "" {
				method = "btree"
			}
			if method != "hash" && method != "btree" {
				return schemaDDLResponse{}, fmt.Errorf("invalid index method %q for index %q on table %q", idx.Method, idxName, tableName)
			}

			colList := strings.Join(idx.Columns, ", ")
			indexStatements = append(indexStatements, fmt.Sprintf("CREATE INDEX %s ON %s (%s) USING %s;", idxName, tableName, colList, method))
		}
	}

	for _, fk := range foreignKeys {
		if _, ok := tableNames[fk.RefTable]; !ok {
			return schemaDDLResponse{}, fmt.Errorf("foreign key reference table %q does not exist", fk.RefTable)
		}
	}

	sort.SliceStable(foreignKeys, func(left, right int) bool {
		leftKey := fmt.Sprintf("%s.%s", foreignKeys[left].Table, foreignKeys[left].Column)
		rightKey := fmt.Sprintf("%s.%s", foreignKeys[right].Table, foreignKeys[right].Column)
		return leftKey < rightKey
	})

	statements := make([]string, 0, 2+len(createStatements)+len(foreignKeys)+len(indexStatements))
	statements = append(statements, fmt.Sprintf("BEGIN DOMAIN %s;", domain))
	statements = append(statements, createStatements...)
	for _, fk := range foreignKeys {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s);", fk.Table, fk.Constraint, fk.Column, fk.RefTable, fk.RefColumn))
	}
	statements = append(statements, indexStatements...)
	statements = append(statements, "COMMIT;")

	return schemaDDLResponse{DDL: strings.Join(statements, "\n\n"), Statements: statements}, nil
}

func normalizeSchemaDomain(value string) (string, error) {
	domain := strings.TrimSpace(value)
	if domain == "" {
		domain = "default"
	}
	if !isValidIdentifier(domain) {
		return "", fmt.Errorf("invalid domain name %q", domain)
	}
	return domain, nil
}

func isValidIdentifier(value string) bool {
	return identifierPattern.MatchString(strings.TrimSpace(value))
}
