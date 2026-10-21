package executor

import (
	"fmt"

	"github.com/correodabid/asql/internal/engine/planner"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// checkVerifiable rejects a VERIFIABLE table without a primary key: the
// ledger's state tree (ADR 0008) finds and proves rows by primary key. Like
// checkAppendOnly it runs when a statement is queued, against the committed
// table, and again when the commit applies it. table is the table SET
// VERIFIABLE names, or nil when it does not exist yet.
func checkVerifiable(table *tableState, plan planner.Plan) error {
	switch {
	case plan.Operation == planner.OperationCreateTable && plan.Verifiable:
		if len(plan.PrimaryKeyColumns) > 0 {
			return nil
		}
		for _, column := range plan.Schema {
			if column.PrimaryKey {
				return nil
			}
		}
	case plan.Operation == planner.OperationAlterTableSetVerifiable && table != nil:
		if tableHasPrimaryKey(table) {
			return nil
		}
	default:
		return nil
	}
	return sqlerr.New("42P16", fmt.Sprintf("table %s.%s needs a primary key to be VERIFIABLE: its rows are proven by primary key", plan.DomainName, plan.TableName))
}

// tableHasPrimaryKey reports whether table declares a primary key.
func tableHasPrimaryKey(table *tableState) bool {
	return table.primaryKey != "" || len(table.primaryKeyColumns) > 0
}
