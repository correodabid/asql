package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type snapshotMap map[string]int64

func main() {
	var (
		pgwireAddr = flag.String("pgwire", "127.0.0.1:5433", "ASQL pgwire endpoint")
		mode       = flag.String("mode", "all", "schema|scenario|inspect|all|print-sql")
		timeout    = flag.Duration("timeout", 30*time.Second, "execution timeout")
	)
	flag.Parse()

	switch *mode {
	case "print-sql":
		printSQL()
		return
	case "schema", "scenario", "inspect", "all":
	default:
		log.Fatalf("unsupported mode %q", *mode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := pgx.Connect(ctx, connectionString(*pgwireAddr))
	if err != nil {
		log.Fatalf("connect to ASQL: %v", err)
	}
	defer func() {
		_ = conn.Close(context.Background())
	}()

	snapshots := snapshotMap{}

	switch *mode {
	case "schema":
		if _, err := executeSteps(ctx, conn, schemaSteps(), true); err != nil {
			log.Fatalf("apply schema: %v", err)
		}
		fmt.Println("schema applied")
	case "scenario":
		if _, err := executeSteps(ctx, conn, workflowSteps(), true); err != nil {
			log.Fatalf("run scenario: %v", err)
		}
		fmt.Println("scenario executed")
	case "inspect":
		if err := inspectScenario(ctx, conn, snapshots); err != nil {
			log.Fatalf("inspect scenario: %v", err)
		}
	case "all":
		schemaSnapshots, err := executeSteps(ctx, conn, schemaSteps(), true)
		if err != nil {
			log.Fatalf("apply schema: %v", err)
		}
		for k, v := range schemaSnapshots {
			snapshots[k] = v
		}
		workflowSnapshots, err := executeSteps(ctx, conn, workflowSteps(), true)
		if err != nil {
			log.Fatalf("run scenario: %v", err)
		}
		for k, v := range workflowSnapshots {
			snapshots[k] = v
		}
		if err := inspectScenario(ctx, conn, snapshots); err != nil {
			log.Fatalf("inspect scenario: %v", err)
		}
	}
}

func connectionString(pgwireAddr string) string {
	return fmt.Sprintf("postgres://asql@%s/asql?sslmode=disable&default_query_exec_mode=simple_protocol", pgwireAddr)
}

func executeSteps(ctx context.Context, conn *pgx.Conn, steps []scenarioStep, recordLSN bool) (snapshotMap, error) {
	snapshots := snapshotMap{}

	for _, step := range steps {
		if err := executeStep(ctx, conn, step); err != nil {
			return nil, err
		}
		if !recordLSN {
			continue
		}
		lsn, err := currentLSN(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf("current_lsn after %q: %w", step.Name, err)
		}
		snapshots[step.Name] = lsn
		fmt.Printf("step complete: %s (lsn=%d)\n", step.Name, lsn)
	}

	return snapshots, nil
}

func executeStep(ctx context.Context, conn *pgx.Conn, step scenarioStep) error {
	return runScopedTx(ctx, conn, step.Mode, step.Domains, func(conn *pgx.Conn) error {
		for _, statement := range step.Statements {
			if _, err := conn.Exec(ctx, statement); err != nil {
				return fmt.Errorf("step %q failed on %q: %w", step.Name, statement, err)
			}
		}
		return nil
	})
}

func rollback(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, "ROLLBACK")
	if err == nil || strings.Contains(strings.ToLower(err.Error()), "no active transaction") {
		return nil
	}
	return err
}

func currentLSN(ctx context.Context, conn *pgx.Conn) (int64, error) {
	var lsn int64
	if err := conn.QueryRow(ctx, "SELECT current_lsn()").Scan(&lsn); err != nil {
		return 0, err
	}
	return lsn, nil
}

func inspectScenario(ctx context.Context, conn *pgx.Conn, snapshots snapshotMap) error {
	fmt.Println()
	fmt.Println("== snapshot points ==")
	printSnapshots(snapshots)

	fmt.Println()
	fmt.Println("== current recipe state ==")
	if err := printQuery(ctx, conn, "SELECT id, recipe_code, title, status, target_batch_size_kg FROM recipe.master_recipes ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== current batch state ==")
	if err := printQuery(ctx, conn, "SELECT id, batch_number, recipe_id, recipe_version, planned_quantity_kg, status FROM execution.batch_orders ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== current batch materials ==")
	if err := printQuery(ctx, conn, "SELECT id, batch_id, lot_id, lot_version, material_role, consumed_grams FROM execution.batch_material_links ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== current inventory lots ==")
	if err := printQuery(ctx, conn, "SELECT id, material_code, supplier_lot_code, status, quantity_grams, expiry_day FROM inventory.material_lots ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== current deviations ==")
	if err := printQuery(ctx, conn, "SELECT id, batch_id, batch_version, lot_id, lot_version, status, severity FROM quality.deviations ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== compliance signatures ==")
	if err := printQuery(ctx, conn, "SELECT id, batch_id, batch_version, step_code, meaning, signer_id, signer_role FROM compliance.batch_signatures ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== helper surface ==")
	if err := printHelpers(ctx, conn); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== batch history ==")
	if err := printQuery(ctx, conn, "SELECT * FROM execution.batch_orders FOR HISTORY WHERE id = 'batch-001'"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== recipe history ==")
	if err := printQuery(ctx, conn, "SELECT * FROM recipe.master_recipes FOR HISTORY WHERE id = 'recipe-001'"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== deviation history ==")
	if err := printQuery(ctx, conn, "SELECT * FROM quality.deviations FOR HISTORY WHERE id = 'dev-001'"); err != nil {
		return err
	}

	batchCreatedLSN, hasBatchCreated := snapshots[stepCreateBatch]
	deviationLSN, hasDeviation := snapshots[stepRaiseDeviation]
	recipeRevisedLSN, hasRecipeRevised := snapshots[stepReviseMasterRecipe]
	releasedLSN, hasReleased := snapshots[stepReleaseBatch]
	if !hasBatchCreated || !hasDeviation || !hasRecipeRevised || !hasReleased {
		fmt.Println()
		fmt.Println("inspection note: LSN checkpoints not available, skipping AS OF LSN comparisons")
		return nil
	}

	fmt.Println()
	fmt.Printf("== batch snapshot AS OF LSN %d (created) ==\n", batchCreatedLSN)
	if err := printQuery(ctx, conn, "SELECT id, batch_number, recipe_id, recipe_version, status FROM execution.batch_orders AS OF LSN $1 WHERE id = 'batch-001'", batchCreatedLSN); err != nil {
		return err
	}

	fmt.Println()
	fmt.Printf("== batch snapshot AS OF LSN %d (on hold) ==\n", deviationLSN)
	if err := printQuery(ctx, conn, "SELECT id, batch_number, recipe_id, recipe_version, status FROM execution.batch_orders AS OF LSN $1 WHERE id = 'batch-001'", deviationLSN); err != nil {
		return err
	}

	fmt.Println()
	fmt.Printf("== recipe snapshot AS OF LSN %d (captured by batch) ==\n", batchCreatedLSN)
	if err := printQuery(ctx, conn, "SELECT id, recipe_code, title, status FROM recipe.master_recipes AS OF LSN $1 WHERE id = 'recipe-001'", batchCreatedLSN); err != nil {
		return err
	}

	fmt.Println()
	fmt.Printf("== recipe snapshot AS OF LSN %d (after recipe revision) ==\n", recipeRevisedLSN)
	if err := printQuery(ctx, conn, "SELECT id, recipe_code, title, status FROM recipe.master_recipes AS OF LSN $1 WHERE id = 'recipe-001'", recipeRevisedLSN); err != nil {
		return err
	}

	fmt.Println()
	fmt.Printf("== batch snapshot AS OF LSN %d (released) ==\n", releasedLSN)
	if err := printQuery(ctx, conn, "SELECT id, batch_number, recipe_id, recipe_version, status FROM execution.batch_orders AS OF LSN $1 WHERE id = 'batch-001'", releasedLSN); err != nil {
		return err
	}

	return nil
}

func printHelpers(ctx context.Context, conn *pgx.Conn) error {
	queries := []string{
		"SELECT current_lsn()",
		"SELECT row_lsn('execution.batch_orders', 'batch-001')",
		"SELECT entity_version('execution', 'batch_record_entity', 'batch-001')",
		"SELECT entity_head_lsn('execution', 'batch_record_entity', 'batch-001')",
		"SELECT entity_version_lsn('execution', 'batch_record_entity', 'batch-001', 1)",
		"SELECT entity_version_lsn('execution', 'batch_record_entity', 'batch-001', 2)",
		"SELECT entity_version_lsn('execution', 'batch_record_entity', 'batch-001', 3)",
		"SELECT entity_version_lsn('execution', 'batch_record_entity', 'batch-001', 4)",
		"SELECT entity_version('recipe', 'master_recipe_entity', 'recipe-001')",
		"SELECT entity_version_lsn('recipe', 'master_recipe_entity', 'recipe-001', 1)",
		"SELECT entity_version_lsn('recipe', 'master_recipe_entity', 'recipe-001', 2)",
		"SELECT resolve_reference('recipe.master_recipes', 'recipe-001')",
		"SELECT resolve_reference('inventory.material_lots', 'lot-api-001')",
	}
	for _, query := range queries {
		if err := printQuery(ctx, conn, query); err != nil {
			return err
		}
	}
	return nil
}

func printQuery(ctx context.Context, conn *pgx.Conn, sql string, args ...any) error {
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("query %q: %w", sql, err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	columns := make([]string, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, string(field.Name))
	}
	fmt.Println(strings.Join(columns, " | "))

	rowCount := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("values for %q: %w", sql, err)
		}
		parts := make([]string, 0, len(values))
		for _, value := range values {
			parts = append(parts, formatValue(value))
		}
		fmt.Println(strings.Join(parts, " | "))
		rowCount++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate %q: %w", sql, err)
	}
	if rowCount == 0 {
		fmt.Println("<no rows>")
	}
	return nil
}

func printSnapshots(snapshots snapshotMap) {
	if len(snapshots) == 0 {
		fmt.Println("<not recorded>")
		return
	}

	names := make([]string, 0, len(snapshots))
	for name := range snapshots {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		fmt.Printf("- %s => %d\n", name, snapshots[name])
	}
}

func printSQL() {
	for _, step := range allSteps() {
		beginSQL, err := step.BeginSQL()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("-- %s\n", step.Name)
		fmt.Println(beginSQL)
		for _, statement := range step.Statements {
			fmt.Println(statement)
		}
		fmt.Println("COMMIT")
		fmt.Println()
	}
}

func formatValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(typed)
	default:
		return fmt.Sprintf("%v", typed)
	}
}
