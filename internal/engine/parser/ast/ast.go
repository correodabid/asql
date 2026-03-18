package ast

import "sort"

// Statement represents a SQL statement node.
type Statement interface {
	statementNode()
	Kind() string
}

type DataType string

const (
	DataTypeInt       DataType = "INT"
	DataTypeText      DataType = "TEXT"
	DataTypeJSON      DataType = "JSON"
	DataTypeBool      DataType = "BOOL"
	DataTypeFloat     DataType = "FLOAT"
	DataTypeTimestamp DataType = "TIMESTAMP"
)

type LiteralKind string

const (
	LiteralString    LiteralKind = "string"
	LiteralNumber    LiteralKind = "number"
	LiteralNull      LiteralKind = "null"
	LiteralBoolean   LiteralKind = "boolean"
	LiteralFloat     LiteralKind = "float"
	LiteralTimestamp LiteralKind = "timestamp"
	LiteralJSON      LiteralKind = "json"
)

type Literal struct {
	Kind        LiteralKind `json:"kind"`
	StringValue string      `json:"string_value,omitempty"`
	NumberValue int64       `json:"number_value,omitempty"`
	BoolValue   bool        `json:"bool_value,omitempty"`
	FloatValue  float64     `json:"float_value,omitempty"`
}

type ColumnDefinition struct {
	Name             string       `json:"name"`
	Type             DataType     `json:"type"`
	PrimaryKey       bool         `json:"primary_key,omitempty"`
	Unique           bool         `json:"unique,omitempty"`
	NotNull          bool         `json:"not_null,omitempty"`
	ReferencesTable  string       `json:"references_table,omitempty"`
	ReferencesColumn string       `json:"references_column,omitempty"`
	Check            *Predicate   `json:"check,omitempty"`
	DefaultValue     *DefaultExpr `json:"default_value,omitempty"`
}

type DefaultKind string

const (
	DefaultLiteral       DefaultKind = "literal"
	DefaultAutoIncrement DefaultKind = "autoincrement"
	DefaultUUIDv7        DefaultKind = "uuid_v7"
	DefaultTxTimestamp   DefaultKind = "tx_timestamp"
)

type DefaultExpr struct {
	Kind  DefaultKind `json:"kind"`
	Value Literal     `json:"value,omitempty"`
}

type Subquery struct {
	Statement SelectStatement `json:"statement"`
}

type Predicate struct {
	Column      string      `json:"column,omitempty"`
	RightColumn string      `json:"right_column,omitempty"` // column-to-column comparison (correlated subqueries, join predicates)
	JsonAccess  *JsonAccess `json:"json_access,omitempty"`
	Operator    string      `json:"operator"`
	Value       Literal     `json:"value,omitempty"`
	Value2      *Literal    `json:"value2,omitempty"`    // for BETWEEN: upper bound
	InValues    []Literal   `json:"in_values,omitempty"` // for IN (list): literal values
	Left        *Predicate  `json:"left,omitempty"`
	Right       *Predicate  `json:"right,omitempty"`
	Subquery    *Subquery   `json:"subquery,omitempty"`
}

// JsonAccess represents a PostgreSQL-style JSON path expression: col->>'key' or col->'key'.
type JsonAccess struct {
	Column   string   `json:"column"`              // base column name
	Path     []string `json:"path"`                // sequence of keys
	TextMode bool     `json:"text_mode,omitempty"` // true when last operator is ->> (returns text)
	Alias    string   `json:"alias,omitempty"`     // AS alias for SELECT
}

type SortDirection string

const (
	SortAsc  SortDirection = "ASC"
	SortDesc SortDirection = "DESC"
)

type OrderByClause struct {
	Column    string        `json:"column"`
	Direction SortDirection `json:"direction"`
}

type JoinType string

const (
	JoinInner JoinType = "INNER"
	JoinLeft  JoinType = "LEFT"
	JoinRight JoinType = "RIGHT"
	JoinCross JoinType = "CROSS"
)

type JoinClause struct {
	JoinType    JoinType `json:"join_type"`
	DomainName  string   `json:"domain_name,omitempty"`
	TableName   string   `json:"table_name"`
	Alias       string   `json:"alias,omitempty"`
	LeftColumn  string   `json:"left_column,omitempty"`
	RightColumn string   `json:"right_column,omitempty"`
}

type VersionedForeignKey struct {
	Column           string `json:"column"`
	LSNColumn        string `json:"lsn_column"`
	ReferencesDomain string `json:"references_domain"`
	ReferencesTable  string `json:"references_table"`
	ReferencesColumn string `json:"references_column"`
}

type CreateTableStatement struct {
	TableName            string                `json:"table_name"`
	Columns              []ColumnDefinition    `json:"columns"`
	VersionedForeignKeys []VersionedForeignKey `json:"versioned_foreign_keys,omitempty"`
	IfNotExists          bool                  `json:"if_not_exists,omitempty"`
}

func (CreateTableStatement) statementNode() {}

func (CreateTableStatement) Kind() string {
	return "create_table"
}

type AlterTableAddColumnStatement struct {
	TableName string           `json:"table_name"`
	Column    ColumnDefinition `json:"column"`
}

func (AlterTableAddColumnStatement) statementNode() {}

func (AlterTableAddColumnStatement) Kind() string {
	return "alter_table_add_column"
}

// AlterTableDropColumnStatement represents ALTER TABLE ... DROP COLUMN ...
type AlterTableDropColumnStatement struct {
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
}

func (AlterTableDropColumnStatement) statementNode() {}

func (AlterTableDropColumnStatement) Kind() string {
	return "alter_table_drop_column"
}

// AlterTableRenameColumnStatement represents ALTER TABLE ... RENAME COLUMN old TO new
type AlterTableRenameColumnStatement struct {
	TableName string `json:"table_name"`
	OldName   string `json:"old_name"`
	NewName   string `json:"new_name"`
}

func (AlterTableRenameColumnStatement) statementNode() {}

func (AlterTableRenameColumnStatement) Kind() string {
	return "alter_table_rename_column"
}

// OnConflictAction describes what to do when INSERT hits a conflict.
type OnConflictAction string

const (
	OnConflictDoNothing OnConflictAction = "DO NOTHING"
	OnConflictDoUpdate  OnConflictAction = "DO UPDATE"
)

// OnConflictClause represents ON CONFLICT (...) DO NOTHING | DO UPDATE SET ...
type OnConflictClause struct {
	ConflictColumns []string         `json:"conflict_columns"`          // columns in the conflict target
	Action          OnConflictAction `json:"action"`                    // DO NOTHING or DO UPDATE
	UpdateColumns   []string         `json:"update_columns,omitempty"`  // SET columns (for DO UPDATE)
	UpdateValues    []Literal        `json:"update_values,omitempty"`   // SET literal values
	UpdateExcluded  []string         `json:"update_excluded,omitempty"` // SET col = EXCLUDED.col (column names)
}

type InsertStatement struct {
	TableName        string            `json:"table_name"`
	Columns          []string          `json:"columns"`
	Values           []Literal         `json:"values"`
	MultiValues      [][]Literal       `json:"multi_values,omitempty"`
	ReturningColumns []string          `json:"returning_columns,omitempty"`
	OnConflict       *OnConflictClause `json:"on_conflict,omitempty"`
}

func (InsertStatement) statementNode() {}

func (InsertStatement) Kind() string {
	return "insert"
}

// UpdateExprKind classifies an update assignment value.
type UpdateExprKind string

const (
	// UpdateExprLiteral means the assignment is a plain literal value.
	UpdateExprLiteral UpdateExprKind = "literal"
	// UpdateExprArithmetic means the assignment is column OP literal.
	UpdateExprArithmetic UpdateExprKind = "arithmetic"
)

// UpdateExpr represents the right-hand side of a SET assignment.
type UpdateExpr struct {
	Kind     UpdateExprKind `json:"kind"`
	Value    Literal        `json:"value,omitempty"`    // for literal kind
	Column   string         `json:"column,omitempty"`   // for arithmetic: source column reference
	Operator string         `json:"operator,omitempty"` // +, -, *, /
	Operand  Literal        `json:"operand,omitempty"`  // for arithmetic: right-hand literal
}

type UpdateStatement struct {
	TableName   string       `json:"table_name"`
	Columns     []string     `json:"columns"`
	Values      []Literal    `json:"values"`
	Expressions []UpdateExpr `json:"expressions,omitempty"` // when set, parallel to Values; holds arithmetic SET exprs
	Where       *Predicate   `json:"where,omitempty"`
}

func (UpdateStatement) statementNode() {}

func (UpdateStatement) Kind() string {
	return "update"
}

type DeleteStatement struct {
	TableName string     `json:"table_name"`
	Where     *Predicate `json:"where,omitempty"`
}

func (DeleteStatement) statementNode() {}

func (DeleteStatement) Kind() string {
	return "delete"
}

type CreateIndexStatement struct {
	IndexName   string   `json:"index_name"`
	TableName   string   `json:"table_name"`
	Column      string   `json:"column"`
	Columns     []string `json:"columns,omitempty"`
	Method      string   `json:"method,omitempty"`
	IfNotExists bool     `json:"if_not_exists,omitempty"`
}

func (CreateIndexStatement) statementNode() {}

func (CreateIndexStatement) Kind() string {
	return "create_index"
}

type CreateEntityStatement struct {
	EntityName  string   `json:"entity_name"`
	RootTable   string   `json:"root_table"`
	Tables      []string `json:"tables"`
	IfNotExists bool     `json:"if_not_exists,omitempty"`
}

func (CreateEntityStatement) statementNode() {}

func (CreateEntityStatement) Kind() string {
	return "create_entity"
}

// DropTableStatement represents a DROP TABLE statement.
type DropTableStatement struct {
	TableName string `json:"table_name"`
	IfExists  bool   `json:"if_exists,omitempty"`
	Cascade   bool   `json:"cascade,omitempty"`
}

func (DropTableStatement) statementNode() {}

func (DropTableStatement) Kind() string {
	return "drop_table"
}

// DropIndexStatement represents a DROP INDEX statement.
type DropIndexStatement struct {
	IndexName string `json:"index_name"`
	TableName string `json:"table_name,omitempty"`
	IfExists  bool   `json:"if_exists,omitempty"`
}

func (DropIndexStatement) statementNode() {}

func (DropIndexStatement) Kind() string {
	return "drop_index"
}

// TruncateTableStatement represents a TRUNCATE TABLE statement.
type TruncateTableStatement struct {
	TableName string `json:"table_name"`
}

func (TruncateTableStatement) statementNode() {}

func (TruncateTableStatement) Kind() string {
	return "truncate_table"
}

type CTE struct {
	Name      string          `json:"name"`
	Statement SelectStatement `json:"statement"`
}

type CaseWhenBranch struct {
	Condition string `json:"condition"`
	Result    string `json:"result"`
}

type CaseWhenExpression struct {
	Branches   []CaseWhenBranch `json:"branches"`
	ElseResult string           `json:"else_result,omitempty"`
	Alias      string           `json:"alias"`
}

type WindowFunction struct {
	Function  string          `json:"function"`
	Args      []string        `json:"args,omitempty"`
	Partition []string        `json:"partition,omitempty"`
	OrderBy   []OrderByClause `json:"order_by,omitempty"`
	Alias     string          `json:"alias"`
}

type SelectStatement struct {
	CTEs              []CTE                `json:"ctes,omitempty"`
	Distinct          bool                 `json:"distinct,omitempty"`
	Columns           []string             `json:"columns"`
	JsonAccessColumns []JsonAccess         `json:"json_access_columns,omitempty"`
	CaseWhenColumns   []CaseWhenExpression `json:"case_when_columns,omitempty"`
	TableName         string               `json:"table_name"`
	TableAlias        string               `json:"table_alias,omitempty"`
	ForHistory        bool                 `json:"for_history,omitempty"`
	Joins             []JoinClause         `json:"joins,omitempty"`
	Where             *Predicate           `json:"where,omitempty"`
	GroupBy           []string             `json:"group_by,omitempty"`
	Having            *Predicate           `json:"having,omitempty"`
	OrderBy           []OrderByClause      `json:"order_by,omitempty"`
	Limit             *int                 `json:"limit,omitempty"`
	Offset            *int                 `json:"offset,omitempty"`
	WindowFunctions   []WindowFunction     `json:"window_functions,omitempty"`
}

func (SelectStatement) statementNode() {}

func (SelectStatement) Kind() string {
	return "select"
}

// SetOpKind identifies the type of set operation.
type SetOpKind string

const (
	SetOpUnion     SetOpKind = "UNION"
	SetOpUnionAll  SetOpKind = "UNION ALL"
	SetOpIntersect SetOpKind = "INTERSECT"
	SetOpExcept    SetOpKind = "EXCEPT"
)

// SetOperationStatement combines two SELECT statements with a set operator.
type SetOperationStatement struct {
	Op    SetOpKind       `json:"op"`
	Left  SelectStatement `json:"left"`
	Right SelectStatement `json:"right"`
}

func (SetOperationStatement) statementNode() {}
func (SetOperationStatement) Kind() string   { return "set_operation" }

type ImportDirective struct {
	SourceDomain string `json:"source_domain"`
	SourceTable  string `json:"source_table"`
	Alias        string `json:"alias,omitempty"`
}

type BeginCrossDomainStatement struct {
	Domains []string `json:"domains"`
}

func (BeginCrossDomainStatement) statementNode() {}

func (BeginCrossDomainStatement) Kind() string {
	return "begin_cross_domain"
}

func NewBeginCrossDomainStatement(domains []string) BeginCrossDomainStatement {
	canonical := make([]string, 0, len(domains))
	seen := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		if _, exists := seen[domain]; exists {
			continue
		}
		seen[domain] = struct{}{}
		canonical = append(canonical, domain)
	}

	sort.Strings(canonical)
	return BeginCrossDomainStatement{Domains: canonical}
}
