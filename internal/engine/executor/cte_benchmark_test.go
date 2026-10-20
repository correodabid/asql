package executor

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// seedLineageBenchmark loads 100,000 artifacts in a DAG of 6 levels: each
// artifact below the first level has two parents in the level above.
func seedLineageBenchmark(b *testing.B) (*Engine, func()) {
	b.Helper()
	engine, closeAll := newAuditedBenchmarkEngine(b)
	ctx := context.Background()
	session := engine.NewSession()
	mustExecBenchmark(b, ctx, engine, session, "BEGIN DOMAIN bench")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE artifacts (id INT PRIMARY KEY, kind TEXT)")
	mustExecBenchmark(b, ctx, engine, session, "CREATE TABLE artifact_parents (id INT PRIMARY KEY, child_id INT, parent_id INT)")
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX parents_by_child ON artifact_parents (child_id) USING HASH")
	mustExecBenchmark(b, ctx, engine, session, "CREATE INDEX parents_by_parent ON artifact_parents (parent_id) USING HASH")
	mustExecBenchmark(b, ctx, engine, session, "COMMIT")

	const artifacts, levels = 100_000, 6
	perLevel := artifacts / levels
	edge := 0
	for start := 0; start < artifacts; start += 1000 {
		tx := engine.NewSession()
		mustExecBenchmark(b, ctx, engine, tx, "BEGIN DOMAIN bench")
		var rows, edges []string
		for id := start; id < start+1000 && id < artifacts; id++ {
			rows = append(rows, fmt.Sprintf("(%d, 'k')", id))
			if level := id / perLevel; level > 0 && level < levels {
				above := (level - 1) * perLevel
				for _, parent := range []int{above + id%perLevel, above + (id*7+3)%perLevel} {
					edges = append(edges, fmt.Sprintf("(%d, %d, %d)", edge, id, parent))
					edge++
				}
			}
		}
		mustExecBenchmark(b, ctx, engine, tx, "INSERT INTO artifacts (id, kind) VALUES "+strings.Join(rows, ", "))
		if len(edges) > 0 {
			mustExecBenchmark(b, ctx, engine, tx, "INSERT INTO artifact_parents (id, child_id, parent_id) VALUES "+strings.Join(edges, ", "))
		}
		mustExecBenchmark(b, ctx, engine, tx, "COMMIT")
	}
	return engine, closeAll
}

// BenchmarkRecursiveCTELineage: the ancestors of an artifact on the last
// level of a 100,000-artifact DAG, in one query.
func BenchmarkRecursiveCTELineage(b *testing.B) {
	engine, closeAll := seedLineageBenchmark(b)
	defer closeAll()
	ctx := context.Background()
	const term = "WITH RECURSIVE lineage(id) AS (SELECT parent_id FROM artifact_parents WHERE child_id = 83340 UNION SELECT p.parent_id %s) %s"
	for _, shape := range []struct{ name, join, query string }{
		{"ids/edges-first", "FROM artifact_parents p JOIN lineage l ON p.child_id = l.id", "SELECT id FROM lineage"},
		{"ids/cte-first", "FROM lineage l JOIN artifact_parents p ON p.child_id = l.id", "SELECT id FROM lineage"},
		{"rows/cte-first", "FROM lineage l JOIN artifact_parents p ON p.child_id = l.id", "SELECT a.id, a.kind FROM lineage l JOIN artifacts a ON a.id = l.id"},
	} {
		sql := fmt.Sprintf(term, shape.join, shape.query)
		b.Run(shape.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				res, err := engine.Query(ctx, sql, []string{"bench"})
				if err != nil {
					b.Fatal(err)
				}
				if len(res.Rows) == 0 {
					b.Fatal("no ancestors")
				}
			}
		})
	}
}
