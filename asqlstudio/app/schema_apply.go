package studioapp

import (
	"context"
	"fmt"
	"strings"

	api "github.com/correodabid/asql/pkg/adminapi"
)

type schemaApplyResponse struct {
	Status             string   `json:"status"`
	TxID               string   `json:"tx_id"`
	Domain             string   `json:"domain"`
	StatementCount     int      `json:"statement_count"`
	ExecutedStatements []string `json:"executed_statements"`
}

type schemaApplySafeDiffResponse struct {
	Status             string   `json:"status"`
	TxID               string   `json:"tx_id"`
	Domain             string   `json:"domain"`
	DiffSafe           bool     `json:"diff_safe"`
	AppliedCount       int      `json:"applied_count"`
	UnsafeCount        int      `json:"unsafe_count"`
	ExecutedStatements []string `json:"executed_statements"`
	Warnings           []string `json:"warnings,omitempty"`
}

func executableSchemaStatements(statements []string) []string {
	executable := make([]string, 0, len(statements))
	for _, statement := range statements {
		normalized := strings.TrimSpace(statement)
		normalized = strings.TrimSuffix(normalized, ";")
		normalized = strings.TrimSpace(normalized)
		if normalized == "" {
			continue
		}
		upper := strings.ToUpper(normalized)
		if strings.HasPrefix(upper, "BEGIN DOMAIN ") || upper == "COMMIT" {
			continue
		}
		executable = append(executable, normalized)
	}
	return executable
}

func applySchemaDDLPlan(ctx context.Context, invoker engineInvoker, domain string, plan schemaDDLResponse) (schemaApplyResponse, error) {
	if invoker == nil {
		return schemaApplyResponse{}, fmt.Errorf("engine invoker is required")
	}

	executable := executableSchemaStatements(plan.Statements)
	if len(executable) == 0 {
		return schemaApplyResponse{}, fmt.Errorf("no executable schema statements found")
	}

	beginResponse, err := invoker.BeginTx(ctx, &api.BeginTxRequest{Mode: "domain", Domains: []string{domain}})
	if err != nil {
		return schemaApplyResponse{}, fmt.Errorf("begin schema transaction: %w", err)
	}
	if strings.TrimSpace(beginResponse.TxID) == "" {
		return schemaApplyResponse{}, fmt.Errorf("begin schema transaction: empty tx_id")
	}

	executed := make([]string, 0, len(executable))
	for _, statement := range executable {
		if _, execErr := invoker.Execute(ctx, &api.ExecuteRequest{TxID: beginResponse.TxID, SQL: statement}); execErr != nil {
			_, _ = invoker.RollbackTx(ctx, &api.RollbackTxRequest{TxID: beginResponse.TxID})
			return schemaApplyResponse{}, fmt.Errorf("apply statement %q: %w", statement, execErr)
		}
		executed = append(executed, statement)
	}

	commitResponse, err := invoker.CommitTx(ctx, &api.CommitTxRequest{TxID: beginResponse.TxID})
	if err != nil {
		_, _ = invoker.RollbackTx(ctx, &api.RollbackTxRequest{TxID: beginResponse.TxID})
		return schemaApplyResponse{}, fmt.Errorf("commit schema transaction: %w", err)
	}

	status := strings.TrimSpace(commitResponse.Status)
	if status == "" {
		status = "COMMITTED"
	}

	return schemaApplyResponse{
		Status:             status,
		TxID:               beginResponse.TxID,
		Domain:             domain,
		StatementCount:     len(executed),
		ExecutedStatements: executed,
	}, nil
}

func applySafeSchemaDiff(ctx context.Context, invoker engineInvoker, diff schemaDiffResponse) (schemaApplySafeDiffResponse, error) {
	plan := schemaDDLResponse{Statements: diff.Statements}
	applied, err := applySchemaDDLPlan(ctx, invoker, diff.Domain, plan)
	if err != nil {
		return schemaApplySafeDiffResponse{}, err
	}

	unsafeCount := 0
	for _, operation := range diff.Operations {
		if !operation.Safe {
			unsafeCount++
		}
	}

	return schemaApplySafeDiffResponse{
		Status:             applied.Status,
		TxID:               applied.TxID,
		Domain:             diff.Domain,
		DiffSafe:           diff.Safe,
		AppliedCount:       applied.StatementCount,
		UnsafeCount:        unsafeCount,
		ExecutedStatements: applied.ExecutedStatements,
		Warnings:           diff.Warnings,
	}, nil
}
