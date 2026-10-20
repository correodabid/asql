package executor

import (
	"fmt"
	"sort"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// checkAppendOnly rejects a plan that would change or remove existing rows
// of an append-only table. It runs when a statement is queued, for an early
// error, and again when the commit applies it, against the state of that
// moment. It applies to every principal, ADMIN included: the guarantee is
// the table's, not a privilege.
func checkAppendOnly(table *tableState, plan planner.Plan) error {
	if table == nil || !table.appendOnly {
		return nil
	}
	var what string
	switch plan.Operation {
	case planner.OperationUpdate:
		what = "UPDATE"
	case planner.OperationDelete:
		what = "DELETE"
	case planner.OperationTruncateTable:
		what = "TRUNCATE TABLE"
	case planner.OperationDropTable:
		what = "DROP TABLE"
	case planner.OperationAlterTableDropColumn:
		what = "DROP COLUMN"
	case planner.OperationInsert:
		if plan.OnConflict == nil || plan.OnConflict.Action != ast.OnConflictDoUpdate {
			return nil
		}
		what = "INSERT ... ON CONFLICT DO UPDATE"
	default:
		return nil
	}
	return sqlerr.New("42501", fmt.Sprintf("permission denied: table %s.%s is append-only and does not allow %s", plan.DomainName, plan.TableName, what))
}

// committedTable returns a table of the committed state, or nil.
func (engine *Engine) committedTable(domain, table string) *tableState {
	state := engine.readState.Load()
	if state == nil {
		return nil
	}
	ds, ok := state.domains[domain]
	if !ok {
		return nil
	}
	return ds.tables[table]
}

// appendOnlyTableRefs lists the append-only tables as (domain, table),
// sorted, for snapshots.
func appendOnlyTableRefs(domains map[string]*domainState) [][2]string {
	var refs [][2]string
	for domainName, domain := range domains {
		for tableName, table := range domain.tables {
			if table.appendOnly {
				refs = append(refs, [2]string{domainName, tableName})
			}
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i][0] != refs[j][0] {
			return refs[i][0] < refs[j][0]
		}
		return refs[i][1] < refs[j][1]
	})
	return refs
}

// markAppendOnlyTables restores the append-only flags a snapshot recorded.
func markAppendOnlyTables(domains map[string]*domainState, refs [][2]string) {
	for _, ref := range refs {
		if domain, ok := domains[ref[0]]; ok {
			if table, ok := domain.tables[ref[1]]; ok {
				table.appendOnly = true
			}
		}
	}
}
