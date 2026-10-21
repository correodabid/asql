package parser

import (
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

func TestParseCompositePrimaryKey(t *testing.T) {
	stmt, err := Parse("CREATE TABLE artifact_parents (child_id TEXT, parent_id TEXT, ordinal INT, PRIMARY KEY (child_id, parent_id)) APPEND ONLY")
	if err != nil {
		t.Fatal(err)
	}
	create := stmt.(ast.CreateTableStatement)
	if strings.Join(create.PrimaryKeyColumns, ",") != "child_id,parent_id" || !create.AppendOnly {
		t.Fatalf("composite key %v, append only %v", create.PrimaryKeyColumns, create.AppendOnly)
	}
	for _, c := range create.Columns {
		if c.PrimaryKey || (c.Name != "ordinal") != c.NotNull {
			t.Fatalf("column %s: primary key %v, not null %v", c.Name, c.PrimaryKey, c.NotNull)
		}
	}

	// A one-column table constraint is the column form.
	stmt, err = Parse("CREATE TABLE t (id TEXT, v INT, PRIMARY KEY (id))")
	if err != nil {
		t.Fatal(err)
	}
	create = stmt.(ast.CreateTableStatement)
	if len(create.PrimaryKeyColumns) != 0 || !create.Columns[0].PrimaryKey {
		t.Fatalf("single-column constraint: %+v", create)
	}

	stmt, err = Parse("ALTER TABLE artifact_parents ADD PRIMARY KEY (child_id, parent_id)")
	if err != nil {
		t.Fatal(err)
	}
	if add := stmt.(ast.AlterTableAddPrimaryKeyStatement); add.TableName != "artifact_parents" || strings.Join(add.Columns, ",") != "child_id,parent_id" {
		t.Fatalf("ADD PRIMARY KEY: %+v", add)
	}

	for _, sql := range []string{
		"CREATE TABLE t (a TEXT PRIMARY KEY, b TEXT, PRIMARY KEY (a, b))",
		"CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY (a, b), PRIMARY KEY (b))",
		"CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY (a, c))",
		"CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY (a, a))",
		"CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY a, b)",
		"CREATE TABLE t (a TEXT, b TEXT, PRIMARY KEY ())",
		"ALTER TABLE t ADD PRIMARY KEY a",
		"ALTER TABLE t ADD PRIMARY KEY ()",
	} {
		if _, err := Parse(sql); err == nil {
			t.Errorf("%s: accepted", sql)
		}
	}
}
