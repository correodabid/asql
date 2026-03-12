package main

import (
	"fmt"
	"strings"
)

type txMode string

const (
	txModeDomain txMode = "domain"
	txModeCross  txMode = "cross"
)

const (
	stepRecipeSchema       = "recipe schema"
	stepInventorySchema    = "inventory schema"
	stepExecutionSchema    = "execution schema"
	stepQualitySchema      = "quality schema"
	stepComplianceSchema   = "compliance schema"
	stepSeedMasterRecipe   = "seed master recipe"
	stepSeedMaterialLots   = "seed material lots"
	stepCreateBatch        = "create batch and reserve lots"
	stepStartBatch         = "start batch with signature"
	stepRaiseDeviation     = "raise deviation and hold batch"
	stepReviseMasterRecipe = "revise master recipe after batch start"
	stepReleaseBatch       = "close deviation and release batch"
)

type scenarioStep struct {
	Name       string
	Mode       txMode
	Domains    []string
	Statements []string
}

func (s scenarioStep) BeginSQL() (string, error) {
	if len(s.Domains) == 0 {
		return "", fmt.Errorf("step %q has no domains", s.Name)
	}

	switch s.Mode {
	case txModeDomain:
		if len(s.Domains) != 1 {
			return "", fmt.Errorf("domain step %q must have exactly one domain", s.Name)
		}
		return "BEGIN DOMAIN " + s.Domains[0], nil
	case txModeCross:
		return "BEGIN CROSS DOMAIN " + strings.Join(s.Domains, ", "), nil
	default:
		return "", fmt.Errorf("step %q has unsupported mode %q", s.Name, s.Mode)
	}
}

func schemaSteps() []scenarioStep {
	return []scenarioStep{
		{
			Name:    stepRecipeSchema,
			Mode:    txModeDomain,
			Domains: []string{"recipe"},
			Statements: []string{
				"CREATE TABLE recipe.master_recipes (id TEXT PRIMARY KEY, recipe_code TEXT UNIQUE, title TEXT NOT NULL, status TEXT NOT NULL, target_batch_size_kg INT NOT NULL DEFAULT 0)",
				"CREATE TABLE recipe.recipe_operations (id TEXT PRIMARY KEY, recipe_id TEXT NOT NULL REFERENCES master_recipes(id), operation_code TEXT NOT NULL, instruction_text TEXT NOT NULL, sequence_no INT NOT NULL DEFAULT 0)",
				"CREATE TABLE recipe.recipe_parameters (id TEXT PRIMARY KEY, recipe_id TEXT NOT NULL REFERENCES master_recipes(id), parameter_name TEXT NOT NULL, target_value TEXT NOT NULL)",
				"CREATE ENTITY master_recipe_entity (ROOT master_recipes, INCLUDES recipe_operations, recipe_parameters)",
			},
		},
		{
			Name:    stepInventorySchema,
			Mode:    txModeDomain,
			Domains: []string{"inventory"},
			Statements: []string{
				"CREATE TABLE inventory.material_lots (id TEXT PRIMARY KEY, material_code TEXT NOT NULL, supplier_lot_code TEXT NOT NULL, status TEXT NOT NULL, quantity_grams INT NOT NULL DEFAULT 0, expiry_day TEXT NOT NULL)",
				"CREATE TABLE inventory.lot_reservations (id TEXT PRIMARY KEY, lot_id TEXT NOT NULL REFERENCES material_lots(id), reserved_for_kind TEXT NOT NULL, reserved_for_id TEXT NOT NULL, reserved_grams INT NOT NULL DEFAULT 0, status TEXT NOT NULL)",
				"CREATE ENTITY material_lot_entity (ROOT material_lots, INCLUDES lot_reservations)",
			},
		},
		{
			Name:    stepExecutionSchema,
			Mode:    txModeCross,
			Domains: []string{"execution", "recipe", "inventory"},
			Statements: []string{
				"CREATE TABLE execution.batch_orders (id TEXT PRIMARY KEY, batch_number TEXT UNIQUE, recipe_id TEXT NOT NULL, recipe_version INT, planned_quantity_kg INT NOT NULL DEFAULT 0, status TEXT NOT NULL, opened_by TEXT NOT NULL, VERSIONED FOREIGN KEY (recipe_id) REFERENCES recipe.master_recipes(id) AS OF recipe_version)",
				"CREATE TABLE execution.batch_steps (id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch_orders(id), operation_code TEXT NOT NULL, status TEXT NOT NULL, recorded_by TEXT NOT NULL, note TEXT NOT NULL)",
				"CREATE TABLE execution.batch_material_links (id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES batch_orders(id), lot_id TEXT NOT NULL, lot_version INT, material_role TEXT NOT NULL, consumed_grams INT NOT NULL DEFAULT 0, VERSIONED FOREIGN KEY (lot_id) REFERENCES inventory.material_lots(id) AS OF lot_version)",
				"CREATE ENTITY batch_record_entity (ROOT batch_orders, INCLUDES batch_steps, batch_material_links)",
			},
		},
		{
			Name:    stepQualitySchema,
			Mode:    txModeCross,
			Domains: []string{"quality", "execution", "inventory"},
			Statements: []string{
				"CREATE TABLE quality.deviations (id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, batch_version INT, lot_id TEXT NOT NULL, lot_version INT, status TEXT NOT NULL, severity TEXT NOT NULL, summary TEXT NOT NULL, VERSIONED FOREIGN KEY (batch_id) REFERENCES execution.batch_orders(id) AS OF batch_version, VERSIONED FOREIGN KEY (lot_id) REFERENCES inventory.material_lots(id) AS OF lot_version)",
				"CREATE TABLE quality.quality_reviews (id TEXT PRIMARY KEY, deviation_id TEXT NOT NULL REFERENCES deviations(id), decision TEXT NOT NULL, rationale TEXT NOT NULL)",
				"CREATE ENTITY deviation_entity (ROOT deviations, INCLUDES quality_reviews)",
			},
		},
		{
			Name:    stepComplianceSchema,
			Mode:    txModeCross,
			Domains: []string{"compliance", "execution", "quality"},
			Statements: []string{
				"CREATE TABLE compliance.batch_signatures (id TEXT PRIMARY KEY, batch_id TEXT NOT NULL, batch_version INT, step_code TEXT NOT NULL, meaning TEXT NOT NULL, signer_id TEXT NOT NULL, signer_role TEXT NOT NULL, reason TEXT NOT NULL, VERSIONED FOREIGN KEY (batch_id) REFERENCES execution.batch_orders(id) AS OF batch_version)",
				"CREATE TABLE compliance.deviation_attestations (id TEXT PRIMARY KEY, deviation_id TEXT NOT NULL, deviation_version INT, meaning TEXT NOT NULL, signer_id TEXT NOT NULL, signer_role TEXT NOT NULL, reason TEXT NOT NULL, VERSIONED FOREIGN KEY (deviation_id) REFERENCES quality.deviations(id) AS OF deviation_version)",
			},
		},
	}
}

func workflowSteps() []scenarioStep {
	return []scenarioStep{
		{
			Name:    stepSeedMasterRecipe,
			Mode:    txModeDomain,
			Domains: []string{"recipe"},
			Statements: []string{
				"INSERT INTO recipe.master_recipes (id, recipe_code, title, status, target_batch_size_kg) VALUES ('recipe-001', 'MR-IBU-200', 'Ibuprofen Suspension 200 mg/5mL', 'EFFECTIVE', 100)",
				"INSERT INTO recipe.recipe_operations (id, recipe_id, operation_code, instruction_text, sequence_no) VALUES ('recipe-op-001', 'recipe-001', 'DISPENSE', 'Dispense verified materials to the pre-mix vessel', 10)",
				"INSERT INTO recipe.recipe_operations (id, recipe_id, operation_code, instruction_text, sequence_no) VALUES ('recipe-op-002', 'recipe-001', 'MIX', 'Mix suspension until torque and appearance are within target', 20)",
				"INSERT INTO recipe.recipe_operations (id, recipe_id, operation_code, instruction_text, sequence_no) VALUES ('recipe-op-003', 'recipe-001', 'FILL', 'Transfer bulk to filling line and reconcile yield', 30)",
				"INSERT INTO recipe.recipe_parameters (id, recipe_id, parameter_name, target_value) VALUES ('recipe-param-001', 'recipe-001', 'mix_speed_rpm', '450')",
				"INSERT INTO recipe.recipe_parameters (id, recipe_id, parameter_name, target_value) VALUES ('recipe-param-002', 'recipe-001', 'hold_time_min', '35')",
			},
		},
		{
			Name:    stepSeedMaterialLots,
			Mode:    txModeDomain,
			Domains: []string{"inventory"},
			Statements: []string{
				"INSERT INTO inventory.material_lots (id, material_code, supplier_lot_code, status, quantity_grams, expiry_day) VALUES ('lot-api-001', 'IBUPROFEN_API', 'SUP-API-7781', 'RELEASED', 80000, '2027-06-30')",
				"INSERT INTO inventory.material_lots (id, material_code, supplier_lot_code, status, quantity_grams, expiry_day) VALUES ('lot-veh-001', 'SUSPENSION_VEHICLE', 'SUP-VEH-1120', 'RELEASED', 36000, '2027-08-31')",
			},
		},
		{
			Name:    stepCreateBatch,
			Mode:    txModeCross,
			Domains: []string{"execution", "recipe", "inventory"},
			Statements: []string{
				"INSERT INTO execution.batch_orders (id, batch_number, recipe_id, planned_quantity_kg, status, opened_by) VALUES ('batch-001', 'PO-2026-0001', 'recipe-001', 100, 'PLANNED', 'planner-001')",
				"INSERT INTO execution.batch_steps (id, batch_id, operation_code, status, recorded_by, note) VALUES ('batch-step-001', 'batch-001', 'DISPENSE', 'PENDING', 'system', 'awaiting shop-floor execution')",
				"INSERT INTO execution.batch_steps (id, batch_id, operation_code, status, recorded_by, note) VALUES ('batch-step-002', 'batch-001', 'MIX', 'PENDING', 'system', 'awaiting shop-floor execution')",
				"INSERT INTO execution.batch_steps (id, batch_id, operation_code, status, recorded_by, note) VALUES ('batch-step-003', 'batch-001', 'FILL', 'PENDING', 'system', 'awaiting shop-floor execution')",
				"INSERT INTO execution.batch_material_links (id, batch_id, lot_id, material_role, consumed_grams) VALUES ('batch-mat-001', 'batch-001', 'lot-api-001', 'API', 8000)",
				"INSERT INTO execution.batch_material_links (id, batch_id, lot_id, material_role, consumed_grams) VALUES ('batch-mat-002', 'batch-001', 'lot-veh-001', 'VEHICLE', 5000)",
				"INSERT INTO inventory.lot_reservations (id, lot_id, reserved_for_kind, reserved_for_id, reserved_grams, status) VALUES ('reservation-001', 'lot-api-001', 'batch_order', 'batch-001', 8000, 'ACTIVE')",
				"INSERT INTO inventory.lot_reservations (id, lot_id, reserved_for_kind, reserved_for_id, reserved_grams, status) VALUES ('reservation-002', 'lot-veh-001', 'batch_order', 'batch-001', 5000, 'ACTIVE')",
			},
		},
		{
			Name:    stepStartBatch,
			Mode:    txModeCross,
			Domains: []string{"execution", "compliance"},
			Statements: []string{
				"UPDATE execution.batch_orders SET status = 'IN_EXECUTION' WHERE id = 'batch-001'",
				"UPDATE execution.batch_steps SET status = 'COMPLETE', recorded_by = 'op-001', note = 'dispense completed within tolerance' WHERE id = 'batch-step-001'",
				"INSERT INTO compliance.batch_signatures (id, batch_id, step_code, meaning, signer_id, signer_role, reason) VALUES ('sig-001', 'batch-001', 'DISPENSE', 'step-verification', 'op-001', 'operator', 'materials dispensed and verified')",
			},
		},
		{
			Name:    stepRaiseDeviation,
			Mode:    txModeCross,
			Domains: []string{"quality", "execution", "inventory", "compliance"},
			Statements: []string{
				"INSERT INTO quality.deviations (id, batch_id, lot_id, status, severity, summary) VALUES ('dev-001', 'batch-001', 'lot-api-001', 'OPEN', 'MAJOR', 'blend uniformity sample drifted outside pre-alert band')",
				"INSERT INTO quality.quality_reviews (id, deviation_id, decision, rationale) VALUES ('review-001', 'dev-001', 'OPEN', 'investigation started while batch is held')",
				"UPDATE execution.batch_orders SET status = 'ON_HOLD' WHERE id = 'batch-001'",
				"UPDATE execution.batch_steps SET status = 'ON_HOLD', recorded_by = 'op-002', note = 'mixing paused pending investigation' WHERE id = 'batch-step-002'",
				"INSERT INTO compliance.deviation_attestations (id, deviation_id, meaning, signer_id, signer_role, reason) VALUES ('attest-001', 'dev-001', 'deviation-opened', 'qa-001', 'quality-reviewer', 'formal investigation opened')",
			},
		},
		{
			Name:    stepReviseMasterRecipe,
			Mode:    txModeDomain,
			Domains: []string{"recipe"},
			Statements: []string{
				"UPDATE recipe.master_recipes SET title = 'Ibuprofen Suspension 200 mg/5mL - Rev B' WHERE id = 'recipe-001'",
				"UPDATE recipe.recipe_parameters SET target_value = '480' WHERE id = 'recipe-param-001'",
			},
		},
		{
			Name:    stepReleaseBatch,
			Mode:    txModeCross,
			Domains: []string{"quality", "execution", "compliance", "inventory"},
			Statements: []string{
				"UPDATE quality.deviations SET status = 'CLOSED' WHERE id = 'dev-001'",
				"UPDATE quality.quality_reviews SET decision = 'USE_AS_IS', rationale = 'investigation confirmed no impact to potency or traceability' WHERE id = 'review-001'",
				"UPDATE execution.batch_orders SET status = 'RELEASED' WHERE id = 'batch-001'",
				"UPDATE execution.batch_steps SET status = 'COMPLETE', recorded_by = 'op-003', note = 'mixing resumed and met revised instruction check' WHERE id = 'batch-step-002'",
				"UPDATE execution.batch_steps SET status = 'COMPLETE', recorded_by = 'op-004', note = 'fill and pack completed with reconciliation check' WHERE id = 'batch-step-003'",
				"INSERT INTO compliance.batch_signatures (id, batch_id, step_code, meaning, signer_id, signer_role, reason) VALUES ('sig-002', 'batch-001', 'RELEASE', 'batch-release', 'qa-002', 'qualified-person', 'batch released after deviation closure')",
				"INSERT INTO compliance.deviation_attestations (id, deviation_id, meaning, signer_id, signer_role, reason) VALUES ('attest-002', 'dev-001', 'deviation-closed', 'qa-002', 'qualified-person', 'final disposition recorded')",
				"UPDATE inventory.material_lots SET status = 'PARTIALLY_CONSUMED', quantity_grams = 72000 WHERE id = 'lot-api-001'",
				"UPDATE inventory.material_lots SET status = 'PARTIALLY_CONSUMED', quantity_grams = 31000 WHERE id = 'lot-veh-001'",
			},
		},
	}
}

func allSteps() []scenarioStep {
	steps := make([]scenarioStep, 0, len(schemaSteps())+len(workflowSteps()))
	steps = append(steps, schemaSteps()...)
	steps = append(steps, workflowSteps()...)
	return steps
}
