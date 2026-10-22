package executor

import (
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// rowVersionColumn is the column the engine adds to every table: the LSN
// of the record that wrote each row version.
const rowVersionColumn = "_lsn"

var errReservedColumn = sqlerr.New("42701", "column _lsn is the engine's row-version column: every table has it, and it cannot be declared, renamed or dropped")

// rejectReservedColumns refuses a client statement that declares, renames
// or drops _lsn: declaring it gave a table two _lsn columns. It runs where a
// client's statement enters the engine, never on replay, so WALs written
// before it, which may declare _lsn, still replay as they did.
func rejectReservedColumns(statement ast.Statement) error {
	switch s := statement.(type) {
	case ast.CreateTableStatement:
		for _, column := range s.Columns {
			if column.Name == rowVersionColumn {
				return errReservedColumn
			}
		}
	case ast.AlterTableAddColumnStatement:
		if s.Column.Name == rowVersionColumn {
			return errReservedColumn
		}
	case ast.AlterTableDropColumnStatement:
		if s.ColumnName == rowVersionColumn {
			return errReservedColumn
		}
	case ast.AlterTableRenameColumnStatement:
		if s.OldName == rowVersionColumn || s.NewName == rowVersionColumn {
			return errReservedColumn
		}
	}
	return nil
}
