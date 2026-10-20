package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// TestQuotedLiteralsTakeTheColumnType: quoted literals are stored and
// compared as the type of their column, live and after replaying the WAL,
// including the statements the WAL keeps as SQL text.
func TestQuotedLiteralsTakeTheColumnType(t *testing.T) {
	ctx := context.Background()
	n := openAuditedNode(t, t.TempDir())
	n.mustTx(
		"CREATE TABLE jobs (id INT PRIMARY KEY, attempts INT, ready BOOL, weight FLOAT, note TEXT)",
		// The table is not committed yet: its queued schema types the row.
		"INSERT INTO jobs (id, attempts, ready, weight, note) VALUES ('1', '0', 'true', '0.5', '1')",
	)
	n.mustTx("INSERT INTO jobs (id, attempts, ready, weight, note) VALUES ('2', ' 3 ', 'off', '2', 'two'), ('3', '0', 't', '-1.25', 'three')")
	n.mustTx(
		"UPDATE jobs SET attempts = attempts + '1' WHERE id = '1'",
		// BETWEEN and LIKE keep these as SQL text in the WAL.
		"UPDATE jobs SET weight = '9.5' WHERE id BETWEEN '2' AND '3' AND note LIKE 't%'",
		"DELETE FROM jobs WHERE ready = 'no' AND note LIKE 'tw%'",
	)

	want := map[int64]map[string]ast.Literal{
		1: {"attempts": {Kind: ast.LiteralNumber, NumberValue: 1}, "ready": {Kind: ast.LiteralBoolean, BoolValue: true}, "weight": {Kind: ast.LiteralFloat, FloatValue: 0.5}, "note": {Kind: ast.LiteralString, StringValue: "1"}},
		3: {"attempts": {Kind: ast.LiteralNumber, NumberValue: 0}, "ready": {Kind: ast.LiteralBoolean, BoolValue: true}, "weight": {Kind: ast.LiteralFloat, FloatValue: 9.5}, "note": {Kind: ast.LiteralString, StringValue: "three"}},
	}
	check := func(label string) {
		t.Helper()
		res, err := n.engine.Query(ctx, "SELECT * FROM jobs WHERE id IN ('1', '2', '3')", []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(res.Rows) != len(want) {
			t.Fatalf("%s: rows %v, want ids 1 and 3", label, res.Rows)
		}
		for _, row := range res.Rows {
			id := row["id"]
			if id.Kind != ast.LiteralNumber {
				t.Fatalf("%s: id %+v is not a number", label, id)
			}
			for column, value := range want[id.NumberValue] {
				if row[column] != value {
					t.Fatalf("%s: row %d %s = %+v, want %+v", label, id.NumberValue, column, row[column], value)
				}
			}
		}
		history, err := n.engine.RowHistory(ctx, "SELECT * FROM jobs FOR HISTORY WHERE id = '3'", []string{"app"})
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		if len(history.Rows) != 2 {
			t.Fatalf("%s: history of row 3 is %v", label, history.Rows)
		}
		since := history.Rows[1][HistoryCommitLSNColumnName].NumberValue
		later, err := n.engine.RowHistory(ctx, fmt.Sprintf("SELECT * FROM jobs FOR HISTORY WHERE id = '3' AND %s >= '%d'", HistoryCommitLSNColumnName, since), []string{"app"})
		if err != nil || len(later.Rows) != 1 {
			t.Fatalf("%s: history of row 3 since LSN %d is %v, err %v", label, since, later.Rows, err)
		}
	}

	check("live")
	n.close()
	for _, dir := range []string{n.snapDir, filepath.Dir(n.auditPath)} {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	n.open()
	defer n.close()
	check("replay of the WAL alone")

	for _, sql := range []string{
		"INSERT INTO jobs (id, attempts) VALUES (4, 'many')",
		"UPDATE jobs SET ready = 'maybe' WHERE id = 1",
		"DELETE FROM jobs WHERE weight > 'heavy'",
	} {
		var sqlErr *sqlerr.SQLError
		if err := n.tx(sql); !errors.As(err, &sqlErr) || sqlErr.Code != "22P02" {
			t.Fatalf("%s: %v, want 22P02", sql, err)
		}
	}
	if _, err := n.engine.Query(ctx, "SELECT id FROM jobs WHERE id = 'one'", []string{"app"}); err == nil {
		t.Fatal("text compared with an INT column: no error")
	}
}

// TestLegacySQLTextDMLIsNotCoerced: DML that older versions stored as SQL
// text replays as it did then, so state rebuilt from an old WAL matches the
// snapshots taken from it.
func TestLegacySQLTextDMLIsNotCoerced(t *testing.T) {
	legacy := encodeDDLPayload("app", "UPDATE jobs SET attempts = '7' WHERE note LIKE 'a%'")
	_, plan, err := decodeMutationPayloadV2(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CoerceLiterals {
		t.Fatal("legacy SQL text payload is coerced on replay")
	}
	current := encodeSQLPayload("app", v2OpDMLSQL, "UPDATE jobs SET attempts = '7' WHERE note LIKE 'a%'")
	if _, plan, err = decodeMutationPayloadV2(current); err != nil || !plan.CoerceLiterals {
		t.Fatalf("SQL text DML payload: coerce %v, err %v; want coerced", plan.CoerceLiterals, err)
	}
}
