package pgwire

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestAsOfAppliesToTheWholeQuery (ASQL-13): AS OF applies to the whole
// query, CTEs included; inside a CTE, a subquery or a side of a set
// operation it is refused instead of reading the present. AS OF may come
// before a JOIN, set operations work outside a transaction and line up by
// position, and a statement that does not parse is a syntax error.
func TestAsOfAppliesToTheWholeQuery(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	exec := func(sqls ...string) {
		t.Helper()
		for _, sql := range sqls {
			if _, err := conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		}
	}
	exec("BEGIN DOMAIN lab", "CREATE TABLE artifacts (id TEXT PRIMARY KEY)", "CREATE TABLE artifact_parents (id INT PRIMARY KEY, child_id TEXT, parent_id TEXT)", "COMMIT",
		"BEGIN DOMAIN lab", "INSERT INTO artifacts (id) VALUES ('aa'), ('bb')", "INSERT INTO artifact_parents (id, child_id, parent_id) VALUES (1, 'bb', 'aa')", "COMMIT")
	var lsn int64
	if err := conn.QueryRow(ctx, "SELECT current_lsn()").Scan(&lsn); err != nil {
		t.Fatal(err)
	}
	at := strconv.FormatInt(lsn, 10)
	// After the LSN: two artifacts more, and dd -> cc -> bb.
	exec("BEGIN DOMAIN lab", "INSERT INTO artifacts (id) VALUES ('cc'), ('dd')",
		"INSERT INTO artifact_parents (id, child_id, parent_id) VALUES (2, 'dd', 'cc'), (3, 'cc', 'bb')", "COMMIT")

	ids := func(sql string) ([]string, error) {
		rows, err := conn.Query(ctx, sql)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			out = append(out, id)
		}
		sort.Strings(out)
		return out, rows.Err()
	}
	const ancestorsOfDD = "WITH RECURSIVE anc(id) AS (SELECT parent_id FROM lab.artifact_parents WHERE child_id = 'dd' UNION SELECT p.parent_id FROM lab.artifact_parents p JOIN anc ON p.child_id = anc.id) "
	for sql, want := range map[string]string{
		"SELECT id FROM lab.artifacts AS OF LSN " + at: "aa,bb",
		// The CTE reads the state at the LSN too, where dd had no parents.
		ancestorsOfDD + "SELECT id FROM lab.artifacts AS OF LSN " + at + " WHERE id IN (SELECT id FROM anc)": "",
		ancestorsOfDD + "SELECT a.id FROM lab.artifacts a AS OF LSN " + at + " JOIN anc ON a.id = anc.id":    "",
		ancestorsOfDD + "SELECT a.id FROM lab.artifacts a JOIN anc ON a.id = anc.id":                         "aa,bb,cc",
		"WITH x AS (SELECT id FROM lab.artifacts) SELECT id FROM x AS OF LSN " + at:                          "aa,bb",
		// Outside a transaction, and by position.
		"SELECT id FROM lab.artifacts UNION SELECT parent_id FROM lab.artifact_parents": "aa,bb,cc,dd",
		"SELECT id FROM lab.artifacts EXCEPT SELECT child_id FROM lab.artifact_parents": "aa",
	} {
		got, err := ids(sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if strings.Join(got, ",") != want {
			t.Fatalf("%s: %v, want %q", sql, got, want)
		}
	}

	for sql, code := range map[string]string{
		"WITH old(id) AS (SELECT id FROM lab.artifacts AS OF LSN " + at + ") SELECT id FROM old":               "0A000",
		"SELECT id FROM lab.artifacts WHERE id IN (SELECT id FROM lab.artifacts AS OF LSN " + at + ")":         "0A000",
		"SELECT id FROM lab.artifacts WHERE id IN (SELECT id FROM lab.artifacts AS OF TIMESTAMP '2000-01-01')": "0A000",
		"SELECT id FROM lab.artifacts AS OF LSN " + at + " UNION SELECT id FROM lab.artifacts":                 "0A000",
		"SELECT id FROM lab.artifacts UNION SELECT id, child_id FROM lab.artifact_parents":                     "42601",
		"SELEC id FROM lab.artifacts": "0A000",
	} {
		_, err := ids(sql)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != code {
			t.Fatalf("%s: %v, want %s", sql, err, code)
		}
	}
}
