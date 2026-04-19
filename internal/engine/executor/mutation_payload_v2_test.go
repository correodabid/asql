package executor

import (
	"reflect"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
)

// roundTrip encodes then decodes a plan and returns the decoded domain+plan.
func roundTrip(t *testing.T, domain string, plan planner.Plan) (string, planner.Plan) {
	t.Helper()
	data := encodeMutationPayloadV2(domain, plan, "-- unused for DDL")
	gotDomain, gotPlan, err := decodeMutationPayloadV2(data)
	if err != nil {
		t.Fatalf("decodeMutationPayloadV2: %v", err)
	}
	return gotDomain, gotPlan
}

func TestDDLStructRoundTrip_CreateTable(t *testing.T) {
	plan := planner.Plan{
		Operation:   planner.OperationCreateTable,
		DomainName:  "billing",
		TableName:   "invoices",
		IfNotExists: true,
		Schema: []ast.ColumnDefinition{
			{
				Name:       "id",
				Type:       ast.DataTypeText,
				PrimaryKey: true,
				NotNull:    true,
			},
			{
				Name:    "amount",
				Type:    ast.DataTypeFloat,
				NotNull: false,
				DefaultValue: &ast.DefaultExpr{
					Kind:  ast.DefaultLiteral,
					Value: ast.Literal{Kind: ast.LiteralFloat, FloatValue: 0.0},
				},
			},
			{
				Name:   "status",
				Type:   ast.DataTypeText,
				Unique: false,
				DefaultValue: &ast.DefaultExpr{
					Kind: ast.DefaultAutoIncrement,
				},
			},
		},
		VersionedForeignKeys: []ast.VersionedForeignKey{
			{
				Column:           "user_lsn",
				LSNColumn:        "user_lsn",
				ReferencesDomain: "identity",
				ReferencesTable:  "users",
				ReferencesColumn: "id",
			},
		},
	}

	gotDomain, gotPlan := roundTrip(t, "billing", plan)
	if gotDomain != "billing" {
		t.Errorf("domain: got %q, want %q", gotDomain, "billing")
	}
	if gotPlan.Operation != planner.OperationCreateTable {
		t.Errorf("operation: got %q", gotPlan.Operation)
	}
	if gotPlan.TableName != "invoices" {
		t.Errorf("tableName: got %q", gotPlan.TableName)
	}
	if !gotPlan.IfNotExists {
		t.Error("IfNotExists should be true")
	}
	if len(gotPlan.Schema) != 3 {
		t.Fatalf("schema len: got %d, want 3", len(gotPlan.Schema))
	}
	col0 := gotPlan.Schema[0]
	if col0.Name != "id" || col0.Type != ast.DataTypeText || !col0.PrimaryKey || !col0.NotNull {
		t.Errorf("col0 mismatch: %+v", col0)
	}
	col1 := gotPlan.Schema[1]
	if col1.Name != "amount" || col1.Type != ast.DataTypeFloat {
		t.Errorf("col1 mismatch: %+v", col1)
	}
	if col1.DefaultValue == nil || col1.DefaultValue.Kind != ast.DefaultLiteral {
		t.Errorf("col1 default mismatch: %+v", col1.DefaultValue)
	}
	col2 := gotPlan.Schema[2]
	if col2.DefaultValue == nil || col2.DefaultValue.Kind != ast.DefaultAutoIncrement {
		t.Errorf("col2 default mismatch: %+v", col2.DefaultValue)
	}
	if len(gotPlan.VersionedForeignKeys) != 1 {
		t.Fatalf("vfk len: got %d", len(gotPlan.VersionedForeignKeys))
	}
	vfk := gotPlan.VersionedForeignKeys[0]
	if vfk.Column != "user_lsn" || vfk.ReferencesDomain != "identity" || vfk.ReferencesTable != "users" {
		t.Errorf("vfk mismatch: %+v", vfk)
	}
}

func TestDDLStructRoundTrip_AlterTableAddColumn(t *testing.T) {
	col := ast.ColumnDefinition{
		Name:    "status",
		Type:    ast.DataTypeText,
		NotNull: true,
		DefaultValue: &ast.DefaultExpr{
			Kind:  ast.DefaultLiteral,
			Value: ast.Literal{Kind: ast.LiteralString, StringValue: "active"},
		},
	}
	plan := planner.Plan{
		Operation:   planner.OperationAlterTableAddColumn,
		DomainName:  "accounts",
		TableName:   "users",
		AlterColumn: &col,
	}

	gotDomain, gotPlan := roundTrip(t, "accounts", plan)
	if gotDomain != "accounts" {
		t.Errorf("domain: got %q", gotDomain)
	}
	if gotPlan.Operation != planner.OperationAlterTableAddColumn {
		t.Errorf("operation: got %q", gotPlan.Operation)
	}
	if gotPlan.TableName != "users" {
		t.Errorf("tableName: got %q", gotPlan.TableName)
	}
	if gotPlan.AlterColumn == nil {
		t.Fatal("AlterColumn is nil")
	}
	if gotPlan.AlterColumn.Name != "status" || gotPlan.AlterColumn.Type != ast.DataTypeText || !gotPlan.AlterColumn.NotNull {
		t.Errorf("AlterColumn mismatch: %+v", gotPlan.AlterColumn)
	}
	if gotPlan.AlterColumn.DefaultValue == nil || gotPlan.AlterColumn.DefaultValue.Kind != ast.DefaultLiteral {
		t.Errorf("AlterColumn default mismatch: %+v", gotPlan.AlterColumn.DefaultValue)
	}
	if gotPlan.AlterColumn.DefaultValue.Value.StringValue != "active" {
		t.Errorf("AlterColumn default value: got %q", gotPlan.AlterColumn.DefaultValue.Value.StringValue)
	}
}

func TestDDLStructRoundTrip_AlterTableDropColumn(t *testing.T) {
	plan := planner.Plan{
		Operation:      planner.OperationAlterTableDropColumn,
		DomainName:     "accounts",
		TableName:      "users",
		DropColumnName: "legacy_col",
	}

	_, gotPlan := roundTrip(t, "accounts", plan)
	if gotPlan.Operation != planner.OperationAlterTableDropColumn {
		t.Errorf("operation: got %q", gotPlan.Operation)
	}
	if gotPlan.TableName != "users" {
		t.Errorf("tableName: got %q", gotPlan.TableName)
	}
	if gotPlan.DropColumnName != "legacy_col" {
		t.Errorf("DropColumnName: got %q", gotPlan.DropColumnName)
	}
}

func TestDDLStructRoundTrip_AlterTableRenameColumn(t *testing.T) {
	plan := planner.Plan{
		Operation:       planner.OperationAlterTableRenameColumn,
		DomainName:      "accounts",
		TableName:       "users",
		RenameOldColumn: "email_addr",
		RenameNewColumn: "email",
	}

	_, gotPlan := roundTrip(t, "accounts", plan)
	if gotPlan.Operation != planner.OperationAlterTableRenameColumn {
		t.Errorf("operation: got %q", gotPlan.Operation)
	}
	if gotPlan.RenameOldColumn != "email_addr" || gotPlan.RenameNewColumn != "email" {
		t.Errorf("rename cols: old=%q new=%q", gotPlan.RenameOldColumn, gotPlan.RenameNewColumn)
	}
}

func TestDDLStructRoundTrip_CreateIndex(t *testing.T) {
	tests := []struct {
		name string
		plan planner.Plan
	}{
		{
			name: "single column hash",
			plan: planner.Plan{
				Operation:   planner.OperationCreateIndex,
				DomainName:  "billing",
				TableName:   "invoices",
				IndexName:   "idx_invoices_status",
				IndexColumn: "status",
				IndexMethod: "hash",
				IfNotExists: true,
			},
		},
		{
			name: "multi column btree",
			plan: planner.Plan{
				Operation:    planner.OperationCreateIndex,
				DomainName:   "billing",
				TableName:    "invoices",
				IndexName:    "idx_invoices_multi",
				IndexColumn:  "user_id",
				IndexColumns: []string{"user_id", "created_at"},
				IndexMethod:  "btree",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, gotPlan := roundTrip(t, tc.plan.DomainName, tc.plan)
			if gotPlan.Operation != planner.OperationCreateIndex {
				t.Errorf("operation: got %q", gotPlan.Operation)
			}
			if gotPlan.IndexName != tc.plan.IndexName {
				t.Errorf("IndexName: got %q", gotPlan.IndexName)
			}
			if gotPlan.IndexColumn != tc.plan.IndexColumn {
				t.Errorf("IndexColumn: got %q", gotPlan.IndexColumn)
			}
			if gotPlan.IndexMethod != tc.plan.IndexMethod {
				t.Errorf("IndexMethod: got %q", gotPlan.IndexMethod)
			}
			if gotPlan.IfNotExists != tc.plan.IfNotExists {
				t.Errorf("IfNotExists: got %v", gotPlan.IfNotExists)
			}
			if !reflect.DeepEqual(gotPlan.IndexColumns, tc.plan.IndexColumns) {
				t.Errorf("IndexColumns: got %v, want %v", gotPlan.IndexColumns, tc.plan.IndexColumns)
			}
		})
	}
}

func TestDDLStructRoundTrip_CreateEntity(t *testing.T) {
	plan := planner.Plan{
		Operation:       planner.OperationCreateEntity,
		DomainName:      "execution",
		EntityName:      "process_order",
		EntityRootTable: "process_orders",
		EntityTables:    []string{"process_orders", "process_steps"},
		IfNotExists:     true,
	}

	_, gotPlan := roundTrip(t, "execution", plan)
	if gotPlan.Operation != planner.OperationCreateEntity {
		t.Errorf("operation: got %q", gotPlan.Operation)
	}
	if gotPlan.EntityName != "process_order" {
		t.Errorf("EntityName: got %q", gotPlan.EntityName)
	}
	if gotPlan.EntityRootTable != "process_orders" {
		t.Errorf("EntityRootTable: got %q", gotPlan.EntityRootTable)
	}
	if !reflect.DeepEqual(gotPlan.EntityTables, []string{"process_orders", "process_steps"}) {
		t.Errorf("EntityTables: got %v", gotPlan.EntityTables)
	}
	if !gotPlan.IfNotExists {
		t.Error("IfNotExists should be true")
	}
}

func TestDDLStructRoundTrip_DropTable(t *testing.T) {
	plan := planner.Plan{
		Operation:  planner.OperationDropTable,
		DomainName: "billing",
		TableName:  "old_invoices",
		IfExists:   true,
		Cascade:    true,
	}

	_, gotPlan := roundTrip(t, "billing", plan)
	if gotPlan.Operation != planner.OperationDropTable {
		t.Errorf("operation: got %q", gotPlan.Operation)
	}
	if gotPlan.TableName != "old_invoices" {
		t.Errorf("tableName: got %q", gotPlan.TableName)
	}
	if !gotPlan.IfExists || !gotPlan.Cascade {
		t.Errorf("flags: IfExists=%v Cascade=%v", gotPlan.IfExists, gotPlan.Cascade)
	}
}

func TestDDLStructRoundTrip_DropIndex(t *testing.T) {
	plan := planner.Plan{
		Operation:  planner.OperationDropIndex,
		DomainName: "billing",
		TableName:  "invoices",
		IndexName:  "idx_invoices_status",
		IfExists:   true,
	}

	_, gotPlan := roundTrip(t, "billing", plan)
	if gotPlan.Operation != planner.OperationDropIndex {
		t.Errorf("operation: got %q", gotPlan.Operation)
	}
	if gotPlan.IndexName != "idx_invoices_status" {
		t.Errorf("IndexName: got %q", gotPlan.IndexName)
	}
	if !gotPlan.IfExists {
		t.Error("IfExists should be true")
	}
}

func TestDDLStructRoundTrip_TruncateTable(t *testing.T) {
	plan := planner.Plan{
		Operation:  planner.OperationTruncateTable,
		DomainName: "billing",
		TableName:  "events",
	}

	_, gotPlan := roundTrip(t, "billing", plan)
	if gotPlan.Operation != planner.OperationTruncateTable {
		t.Errorf("operation: got %q", gotPlan.Operation)
	}
	if gotPlan.TableName != "events" {
		t.Errorf("tableName: got %q", gotPlan.TableName)
	}
}

func TestDDLStructRoundTrip_ColumnDefFull(t *testing.T) {
	// Column with all flags set: refs, check, default.
	plan := planner.Plan{
		Operation:  planner.OperationCreateTable,
		DomainName: "orders",
		TableName:  "line_items",
		Schema: []ast.ColumnDefinition{
			{
				Name:             "product_id",
				Type:             ast.DataTypeText,
				NotNull:          true,
				Unique:           true,
				ReferencesTable:  "products",
				ReferencesColumn: "id",
				Check: &ast.Predicate{
					Column:   "product_id",
					Operator: "IS NOT NULL",
				},
				DefaultValue: &ast.DefaultExpr{
					Kind: ast.DefaultUUIDv7,
				},
			},
		},
	}

	_, gotPlan := roundTrip(t, "orders", plan)
	if len(gotPlan.Schema) != 1 {
		t.Fatalf("schema len: got %d", len(gotPlan.Schema))
	}
	col := gotPlan.Schema[0]
	if col.Name != "product_id" {
		t.Errorf("col Name: got %q", col.Name)
	}
	if !col.NotNull || !col.Unique {
		t.Errorf("col flags: NotNull=%v Unique=%v", col.NotNull, col.Unique)
	}
	if col.ReferencesTable != "products" || col.ReferencesColumn != "id" {
		t.Errorf("col refs: table=%q col=%q", col.ReferencesTable, col.ReferencesColumn)
	}
	if col.Check == nil || col.Check.Operator != "IS NOT NULL" {
		t.Errorf("col check: %+v", col.Check)
	}
	if col.DefaultValue == nil || col.DefaultValue.Kind != ast.DefaultUUIDv7 {
		t.Errorf("col default: %+v", col.DefaultValue)
	}
}

// TestDDLStructOpCode verifies new DDL writes v2OpDDLStruct (0x05), not v2OpDDL (0x04).
func TestDDLStructOpCode(t *testing.T) {
	plan := planner.Plan{
		Operation:  planner.OperationCreateTable,
		DomainName: "test",
		TableName:  "t",
		Schema:     []ast.ColumnDefinition{{Name: "id", Type: ast.DataTypeText}},
	}
	data := encodeMutationPayloadV2("test", plan, "-- unused")
	// data[0] = version byte (0x02)
	// data[1..] = domain uvarint + domain bytes
	// after domain = op byte
	// domain = "test" → uvarint(4) + "test" = 5 bytes → op at offset 6
	domainLen := len("test")
	opOffset := 1 + 1 + domainLen // version + uvarint(4) + "test"
	if data[opOffset] != v2OpDDLStruct {
		t.Errorf("expected op 0x%02x (DDLStruct), got 0x%02x", v2OpDDLStruct, data[opOffset])
	}
}
