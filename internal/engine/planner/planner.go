package planner

import (
	"errors"
	"fmt"
	"strings"

	"asql/internal/engine/parser/ast"
)

var (
	errUnsupportedStatement = errors.New("unsupported statement for planner")
	errDomainAccessDenied   = errors.New("domain access denied")
	errInvalidTableRef      = errors.New("invalid table reference")
)

type Operation string

const (
	OperationCreateTable            Operation = "create_table"
	OperationAlterTableAddColumn    Operation = "alter_table_add_column"
	OperationAlterTableDropColumn   Operation = "alter_table_drop_column"
	OperationAlterTableRenameColumn Operation = "alter_table_rename_column"
	OperationCreateIndex            Operation = "create_index"
	OperationInsert                 Operation = "insert"
	OperationUpdate                 Operation = "update"
	OperationDelete                 Operation = "delete"
	OperationSelect                 Operation = "select"
	OperationCreateEntity           Operation = "create_entity"
	OperationDropTable              Operation = "drop_table"
	OperationDropIndex              Operation = "drop_index"
	OperationTruncateTable          Operation = "truncate_table"
	OperationSetOp                  Operation = "set_operation"
)

type Plan struct {
	Operation            Operation                 `json:"operation"`
	DomainName           string                    `json:"domain_name,omitempty"`
	TableName            string                    `json:"table_name"`
	AlterColumn          *ast.ColumnDefinition     `json:"alter_column,omitempty"`
	DropColumnName       string                    `json:"drop_column_name,omitempty"`
	RenameOldColumn      string                    `json:"rename_old_column,omitempty"`
	RenameNewColumn      string                    `json:"rename_new_column,omitempty"`
	IndexName            string                    `json:"index_name,omitempty"`
	IndexColumn          string                    `json:"index_column,omitempty"`
	IndexColumns         []string                  `json:"index_columns,omitempty"`
	IndexMethod          string                    `json:"index_method,omitempty"`
	Columns              []string                  `json:"columns,omitempty"`
	Distinct             bool                      `json:"distinct,omitempty"`
	Schema               []ast.ColumnDefinition    `json:"schema,omitempty"`
	Values               []ast.Literal             `json:"values,omitempty"`
	MultiValues          [][]ast.Literal           `json:"multi_values,omitempty"`
	Filter               *ast.Predicate            `json:"filter,omitempty"`
	Joins                []ast.JoinClause          `json:"joins,omitempty"`
	TableAlias           string                    `json:"table_alias,omitempty"`
	GroupBy              []string                  `json:"group_by,omitempty"`
	Having               *ast.Predicate            `json:"having,omitempty"`
	OrderBy              []ast.OrderByClause       `json:"order_by,omitempty"`
	Limit                *int                      `json:"limit,omitempty"`
	Offset               *int                      `json:"offset,omitempty"`
	CTEs                 []ast.CTE                 `json:"ctes,omitempty"`
	WindowFunctions      []ast.WindowFunction      `json:"window_functions,omitempty"`
	JsonAccessColumns    []ast.JsonAccess          `json:"json_access_columns,omitempty"`
	CaseWhenColumns      []ast.CaseWhenExpression  `json:"case_when_columns,omitempty"`
	ForHistory           bool                      `json:"for_history,omitempty"`
	IfNotExists          bool                      `json:"if_not_exists,omitempty"`
	ReturningColumns     []string                  `json:"returning_columns,omitempty"`
	VersionedForeignKeys []ast.VersionedForeignKey `json:"versioned_foreign_keys,omitempty"`
	EntityName           string                    `json:"entity_name,omitempty"`
	EntityRootTable      string                    `json:"entity_root_table,omitempty"`
	EntityTables         []string                  `json:"entity_tables,omitempty"`
	IfExists             bool                      `json:"if_exists,omitempty"`
	Cascade              bool                      `json:"cascade,omitempty"`
	UpdateExprs          []ast.UpdateExpr          `json:"update_exprs,omitempty"`
	OnConflict           *ast.OnConflictClause     `json:"on_conflict,omitempty"`
	SetOp                ast.SetOpKind             `json:"set_op,omitempty"`
	LeftPlan             *Plan                     `json:"left_plan,omitempty"`
	RightPlan            *Plan                     `json:"right_plan,omitempty"`
}

// Build converts an AST statement into an executable logical plan.
func Build(statement ast.Statement) (Plan, error) {
	return BuildForDomains(statement, nil)
}

// BuildForDomain converts an AST statement into a plan and validates domain scope.
func BuildForDomain(statement ast.Statement, txDomain string) (Plan, error) {
	if strings.TrimSpace(txDomain) == "" {
		return BuildForDomains(statement, nil)
	}

	return BuildForDomains(statement, []string{txDomain})
}

// BuildForDomains converts an AST statement into a plan and validates domain scope.
func BuildForDomains(statement ast.Statement, txDomains []string) (Plan, error) {
	allowedDomains := normalizeDomains(txDomains)

	switch typed := statement.(type) {
	case ast.CreateTableStatement:
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		return Plan{
			Operation:            OperationCreateTable,
			DomainName:           domain,
			TableName:            table,
			Schema:               typed.Columns,
			VersionedForeignKeys: typed.VersionedForeignKeys,
			IfNotExists:          typed.IfNotExists,
		}, nil
	case ast.AlterTableAddColumnStatement:
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		column := typed.Column
		return Plan{
			Operation:   OperationAlterTableAddColumn,
			DomainName:  domain,
			TableName:   table,
			AlterColumn: &column,
		}, nil
	case ast.AlterTableDropColumnStatement:
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		return Plan{
			Operation:      OperationAlterTableDropColumn,
			DomainName:     domain,
			TableName:      table,
			DropColumnName: typed.ColumnName,
		}, nil
	case ast.AlterTableRenameColumnStatement:
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		return Plan{
			Operation:       OperationAlterTableRenameColumn,
			DomainName:      domain,
			TableName:       table,
			RenameOldColumn: typed.OldName,
			RenameNewColumn: typed.NewName,
		}, nil
	case ast.CreateIndexStatement:
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		var indexColumns []string
		if len(typed.Columns) > 1 {
			indexColumns = typed.Columns
		}
		return Plan{
			Operation:    OperationCreateIndex,
			DomainName:   domain,
			TableName:    table,
			IndexName:    typed.IndexName,
			IndexColumn:  typed.Column,
			IndexColumns: indexColumns,
			IndexMethod:  typed.Method,
			IfNotExists:  typed.IfNotExists,
		}, nil
	case ast.CreateEntityStatement:
		domain := ""
		if len(allowedDomains) == 1 {
			domain = allowedDomains[0]
		} else if len(allowedDomains) > 1 {
			domain = allowedDomains[0]
		}
		return Plan{
			Operation:       OperationCreateEntity,
			DomainName:      domain,
			EntityName:      typed.EntityName,
			EntityRootTable: typed.RootTable,
			EntityTables:    typed.Tables,
			IfNotExists:     typed.IfNotExists,
		}, nil
	case ast.DropTableStatement:
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		return Plan{
			Operation:  OperationDropTable,
			DomainName: domain,
			TableName:  table,
			IfExists:   typed.IfExists,
			Cascade:    typed.Cascade,
		}, nil
	case ast.DropIndexStatement:
		domain := ""
		table := typed.TableName
		if typed.TableName != "" {
			var err error
			domain, table, err = resolveTableDomain(typed.TableName, allowedDomains)
			if err != nil {
				return Plan{}, err
			}
		} else if len(allowedDomains) > 0 {
			domain = allowedDomains[0]
		}
		return Plan{
			Operation:  OperationDropIndex,
			DomainName: domain,
			TableName:  table,
			IndexName:  typed.IndexName,
			IfExists:   typed.IfExists,
		}, nil
	case ast.TruncateTableStatement:
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		return Plan{
			Operation:  OperationTruncateTable,
			DomainName: domain,
			TableName:  table,
		}, nil
	case ast.InsertStatement:
		if len(typed.Columns) != len(typed.Values) {
			return Plan{}, fmt.Errorf("insert plan mismatch: columns=%d values=%d", len(typed.Columns), len(typed.Values))
		}
		for i, row := range typed.MultiValues {
			if len(typed.Columns) != len(row) {
				return Plan{}, fmt.Errorf("insert plan mismatch row %d: columns=%d values=%d", i+2, len(typed.Columns), len(row))
			}
		}
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		return Plan{
			Operation:        OperationInsert,
			DomainName:       domain,
			TableName:        table,
			Columns:          typed.Columns,
			Values:           typed.Values,
			MultiValues:      typed.MultiValues,
			ReturningColumns: typed.ReturningColumns,
			OnConflict:       typed.OnConflict,
		}, nil
	case ast.UpdateStatement:
		if len(typed.Columns) == 0 {
			return Plan{}, fmt.Errorf("update plan requires at least one column")
		}
		if len(typed.Columns) != len(typed.Values) {
			return Plan{}, fmt.Errorf("update plan mismatch: columns=%d values=%d", len(typed.Columns), len(typed.Values))
		}
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		return Plan{
			Operation:   OperationUpdate,
			DomainName:  domain,
			TableName:   table,
			Columns:     typed.Columns,
			Values:      typed.Values,
			UpdateExprs: typed.Expressions,
			Filter:      typed.Where,
		}, nil
	case ast.DeleteStatement:
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}
		return Plan{
			Operation:  OperationDelete,
			DomainName: domain,
			TableName:  table,
			Filter:     typed.Where,
		}, nil
	case ast.SelectStatement:
		domain, table, err := resolveTableDomain(typed.TableName, allowedDomains)
		if err != nil {
			return Plan{}, err
		}

		var joins []ast.JoinClause
		for _, j := range typed.Joins {
			joinDomain, joinTable, err := resolveTableDomain(j.TableName, allowedDomains)
			if err != nil {
				return Plan{}, err
			}
			joins = append(joins, ast.JoinClause{
				JoinType:    j.JoinType,
				DomainName:  joinDomain,
				TableName:   joinTable,
				Alias:       j.Alias,
				LeftColumn:  j.LeftColumn,
				RightColumn: j.RightColumn,
			})
		}

		return Plan{
			Operation:         OperationSelect,
			DomainName:        domain,
			TableName:         table,
			TableAlias:        typed.TableAlias,
			Columns:           typed.Columns,
			Distinct:          typed.Distinct,
			JsonAccessColumns: typed.JsonAccessColumns,
			CaseWhenColumns:   typed.CaseWhenColumns,
			Filter:            typed.Where,
			Joins:             joins,
			GroupBy:           typed.GroupBy,
			Having:            typed.Having,
			OrderBy:           typed.OrderBy,
			Limit:             typed.Limit,
			Offset:            typed.Offset,
			CTEs:              typed.CTEs,
			WindowFunctions:   typed.WindowFunctions,
			ForHistory:        typed.ForHistory,
		}, nil
	case ast.SetOperationStatement:
		leftPlan, err := BuildForDomains(typed.Left, txDomains)
		if err != nil {
			return Plan{}, fmt.Errorf("plan left side of %s: %w", typed.Op, err)
		}
		rightPlan, err := BuildForDomains(typed.Right, txDomains)
		if err != nil {
			return Plan{}, fmt.Errorf("plan right side of %s: %w", typed.Op, err)
		}
		return Plan{
			Operation: OperationSetOp,
			SetOp:     typed.Op,
			LeftPlan:  &leftPlan,
			RightPlan: &rightPlan,
		}, nil
	default:
		return Plan{}, errUnsupportedStatement
	}
}

func resolveTableDomain(tableReference string, txDomains []string) (string, string, error) {
	trimmedRef := strings.TrimSpace(strings.ToLower(tableReference))
	domainSet := make(map[string]struct{}, len(txDomains))
	for _, domain := range txDomains {
		domainSet[domain] = struct{}{}
	}

	parts := strings.Split(trimmedRef, ".")
	if len(parts) == 1 {
		if parts[0] == "" {
			return "", "", errInvalidTableRef
		}

		if len(txDomains) == 0 {
			return "", parts[0], nil
		}

		// Use the first domain as the default for unqualified table references.
		// When domains are auto-expanded (e.g., via VFK), the primary domain is first.
		return txDomains[0], parts[0], nil
	}

	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", errInvalidTableRef
	}

	if len(txDomains) > 0 {
		if _, exists := domainSet[parts[0]]; !exists {
			return "", "", fmt.Errorf("%w: tx=%v table=%s", errDomainAccessDenied, txDomains, trimmedRef)
		}
	}

	return parts[0], parts[1], nil
}

func normalizeDomains(domains []string) []string {
	if len(domains) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(domains))
	normalized := make([]string, 0, len(domains))
	for _, domain := range domains {
		canonical := strings.ToLower(strings.TrimSpace(domain))
		if canonical == "" {
			continue
		}
		if _, exists := seen[canonical]; exists {
			continue
		}
		seen[canonical] = struct{}{}
		normalized = append(normalized, canonical)
	}

	return normalized
}
