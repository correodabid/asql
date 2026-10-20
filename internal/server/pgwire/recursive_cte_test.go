package pgwire

import (
	"context"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestRecursiveCTEOverPgx: the lineage of an artifact, walked up a DAG of
// provenance edges in one query, over both protocols.
func TestRecursiveCTEOverPgx(t *testing.T) {
	addr := startProjectionTestServer(t)
	ctx := context.Background()
	setup, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = setup.Close(context.Background()) })
	for _, sql := range []string{
		"BEGIN DOMAIN lab",
		"CREATE TABLE artifacts (id INT PRIMARY KEY, kind TEXT)",
		"CREATE TABLE artifact_parents (id INT PRIMARY KEY, child_id INT, parent_id INT)",
		"CREATE INDEX parents_by_child ON artifact_parents (child_id) USING HASH",
		"COMMIT",
		"BEGIN DOMAIN lab",
		// 5 <- 4 <- 2 <- 1 and 5 <- 3 <- 1: a diamond.
		"INSERT INTO artifacts (id, kind) VALUES (1, 'source'), (2, 'step'), (3, 'step'), (4, 'step'), (5, 'result'), (6, 'other')",
		"INSERT INTO artifact_parents (id, child_id, parent_id) VALUES (1, 5, 4), (2, 4, 2), (3, 2, 1), (4, 5, 3), (5, 3, 1)",
		"COMMIT",
	} {
		if _, err := setup.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	const lineage = `WITH RECURSIVE lineage(id) AS (
		SELECT parent_id FROM lab.artifact_parents WHERE child_id = $1
		UNION
		SELECT p.parent_id FROM lab.artifact_parents p JOIN lineage l ON p.child_id = l.id
	) SELECT a.id, a.kind FROM lineage l JOIN lab.artifacts a ON a.id = l.id`
	for _, mode := range []string{"simple_protocol", "cache_statement"} {
		conn, err := pgx.Connect(ctx, "postgres://asql@"+addr+"/asql?sslmode=disable&default_query_exec_mode="+mode)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close(context.Background()) })
		rows, err := conn.Query(ctx, lineage, 5)
		if err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		var ids []int64
		kinds := map[int64]string{}
		for rows.Next() {
			var id int64
			var kind string
			if err := rows.Scan(&id, &kind); err != nil {
				rows.Close()
				t.Fatalf("%s: %v", mode, err)
			}
			ids = append(ids, id)
			kinds[id] = kind
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if len(ids) != 4 || ids[0] != 1 || ids[3] != 4 || kinds[1] != "source" {
			t.Fatalf("%s: lineage of 5 is %v %v, want 1 to 4", mode, ids, kinds)
		}

		// The CTE's own columns, named by its column list.
		var maxID int64
		if err := conn.QueryRow(ctx, `WITH RECURSIVE lineage(id) AS (
			SELECT parent_id FROM lab.artifact_parents WHERE child_id = $1
			UNION
			SELECT p.parent_id FROM lineage l JOIN lab.artifact_parents p ON p.child_id = l.id
		) SELECT id FROM lineage ORDER BY id DESC LIMIT 1`, 5).Scan(&maxID); err != nil || maxID != 4 {
			t.Fatalf("%s: highest ancestor id %d, err %v; want 4", mode, maxID, err)
		}

		var count int64
		if err := conn.QueryRow(ctx, `WITH RECURSIVE lineage(id) AS (
			SELECT parent_id FROM lab.artifact_parents WHERE child_id = $1
			UNION ALL
			SELECT p.parent_id FROM lineage l JOIN lab.artifact_parents p ON p.child_id = l.id
		) SELECT COUNT(*) FROM lineage`, 5).Scan(&count); err != nil || count != 5 {
			t.Fatalf("%s: UNION ALL lineage has %d rows, err %v; want 5 (1 is reached twice)", mode, count, err)
		}
	}
}
