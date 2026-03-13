package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type snapshotMap map[string]int64

// Step name constants used by inspectScenario for LSN lookups.
const (
	stepCreateBatch        = "create batch and reserve lots"
	stepStartBatch         = "start batch with signature"
	stepRaiseDeviation     = "raise deviation and hold batch"
	stepReviseMasterRecipe = "revise master recipe after batch start"
	stepReleaseBatch       = "close deviation and release batch"
)

func main() {
	var (
		pgwireAddr  = flag.String("pgwire", "127.0.0.1:5433", "ASQL pgwire endpoint")
		mode        = flag.String("mode", "all", "schema|scenario|inspect|all|print-sql")
		timeout     = flag.Duration("timeout", 30*time.Second, "execution timeout")
		fixtureFile = flag.String("fixture", "", "path to fixture JSON (default: embedded pharma-manufacturing-demo-v1.json)")
	)
	flag.Parse()

	// Load fixture steps.
	var fixtureData []byte
	if *fixtureFile != "" {
		var err error
		fixtureData, err = os.ReadFile(*fixtureFile)
		if err != nil {
			log.Fatalf("read fixture %q: %v", *fixtureFile, err)
		}
	} else {
		fixtureData = defaultFixtureJSON
	}

	allSteps, err := loadFixtureSteps(fixtureData)
	if err != nil {
		log.Fatalf("load fixture: %v", err)
	}
	schemaSteps, workflowSteps := splitSteps(allSteps)

	switch *mode {
	case "print-sql":
		printSQL(allSteps)
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
		if _, err := executeSteps(ctx, conn, schemaSteps, true); err != nil {
			log.Fatalf("apply schema: %v", err)
		}
		fmt.Println("schema applied")
	case "scenario":
		if _, err := executeSteps(ctx, conn, workflowSteps, true); err != nil {
			log.Fatalf("run scenario: %v", err)
		}
		fmt.Println("scenario executed")
	case "inspect":
		if err := inspectScenario(ctx, conn, snapshots); err != nil {
			log.Fatalf("inspect scenario: %v", err)
		}
	case "all":
		schemaSnapshots, err := executeSteps(ctx, conn, schemaSteps, true)
		if err != nil {
			log.Fatalf("apply schema: %v", err)
		}
		for k, v := range schemaSnapshots {
			snapshots[k] = v
		}
		workflowSnapshots, err := executeSteps(ctx, conn, workflowSteps, true)
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
	fmt.Println("== ISA-95 physical model ==")
	if err := printQuery(ctx, conn, "SELECT id, site_code, title, gxp_status FROM operations.sites ORDER BY id"); err != nil {
		return err
	}
	if err := printQuery(ctx, conn, "SELECT id, unit_code, title, unit_class FROM operations.units ORDER BY id"); err != nil {
		return err
	}
	if err := printQuery(ctx, conn, "SELECT id, equipment_code, title, asset_role, calibration_state FROM operations.equipment_assets ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== ISA-88 recipe hierarchy ==")
	if err := printQuery(ctx, conn, "SELECT id, unit_procedure_code, title, sequence_no FROM recipe.recipe_unit_procedures ORDER BY sequence_no, id"); err != nil {
		return err
	}
	if err := printQuery(ctx, conn, "SELECT id, unit_procedure_code, operation_code, instruction_text, sequence_no FROM recipe.recipe_operations ORDER BY sequence_no, id"); err != nil {
		return err
	}
	if err := printQuery(ctx, conn, "SELECT id, unit_procedure_code, operation_code, phase_code, instruction_text, sequence_no FROM recipe.recipe_phases ORDER BY sequence_no, id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== current recipe state ==")
	if err := printQuery(ctx, conn, "SELECT id, recipe_code, title, status, target_batch_size_kg FROM recipe.master_recipes ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== current batch state ==")
	if err := printQuery(ctx, conn, "SELECT id, batch_number, recipe_id, recipe_version, unit_id, unit_lsn, planned_quantity_kg, status, ebr_status FROM execution.batch_orders ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== current batch operations ==")
	if err := printQuery(ctx, conn, "SELECT id, batch_id, unit_procedure_code, operation_code, status, recorded_by, note FROM execution.batch_steps ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== current batch phases ==")
	if err := printQuery(ctx, conn, "SELECT id, batch_id, unit_procedure_code, operation_code, phase_code, phase_state, equipment_id, equipment_lsn, recorded_by FROM execution.batch_phase_records ORDER BY id"); err != nil {
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
	if err := printQuery(ctx, conn, "SELECT id, batch_id, batch_version, phase_record_id, lot_id, lot_version, equipment_id, equipment_lsn, status, severity FROM quality.deviations ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== compliance signatures ==")
	if err := printQuery(ctx, conn, "SELECT id, batch_id, batch_version, signature_scope, step_code, meaning, signer_id, signer_role FROM compliance.batch_signatures ORDER BY id"); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("== eBR reviews ==")
	if err := printQuery(ctx, conn, "SELECT id, batch_id, batch_version, review_stage, decision, reviewer_id, reviewer_role FROM compliance.ebr_reviews ORDER BY id"); err != nil {
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

	fmt.Println()
	fmt.Println("== phase history ==")
	if err := printQuery(ctx, conn, "SELECT * FROM execution.batch_phase_records FOR HISTORY WHERE id = 'phase-rec-002'"); err != nil {
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
		"SELECT entity_version('operations', 'manufacturing_model_entity', 'site-001')",
		"SELECT resolve_reference('recipe.master_recipes', 'recipe-001')",
		"SELECT resolve_reference('operations.units', 'unit-001')",
		"SELECT resolve_reference('operations.equipment_assets', 'equip-002')",
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

func printSQL(steps []scenarioStep) {
	for _, step := range steps {
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
