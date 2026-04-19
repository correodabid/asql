package executor

import "github.com/correodabid/asql/internal/engine/parser/ast"

const (
	// HistoryOperationColumnName is the canonical metadata column exposed by
	// SELECT ... FOR HISTORY for the row operation at a commit.
	HistoryOperationColumnName = "__operation"
	// HistoryCommitLSNColumnName is the canonical metadata column exposed by
	// SELECT ... FOR HISTORY for the commit LSN of the history entry.
	HistoryCommitLSNColumnName = "__commit_lsn"
)

func buildHistoryResultRow(row map[string]ast.Literal, commitLSN uint64, operation string) map[string]ast.Literal {
	resultRow := cloneRow(row)
	// FOR HISTORY exposes its own stable metadata contract. The base row carries
	// the mutable row-head `_lsn`, which is useful for normal table reads but too
	// ambiguous for historical output.
	delete(resultRow, "_lsn")
	delete(resultRow, "_operation")
	resultRow[HistoryOperationColumnName] = ast.Literal{Kind: ast.LiteralString, StringValue: operation}
	resultRow[HistoryCommitLSNColumnName] = ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(commitLSN)}
	return resultRow
}
