package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	api "asql/internal/server/httpapi"
)

type fakeSchemaInvoker struct {
	calls   []string
	txID    string
	failSQL string
	schema  *api.SchemaSnapshotResponse
}

func (fake *fakeSchemaInvoker) BeginTx(_ context.Context, req *api.BeginTxRequest) (*api.BeginTxResponse, error) {
	fake.calls = append(fake.calls, "BeginTx")
	if fake.txID == "" {
		fake.txID = "tx-schema-1"
	}
	return &api.BeginTxResponse{TxID: fake.txID}, nil
}

func (fake *fakeSchemaInvoker) Execute(_ context.Context, req *api.ExecuteRequest) (*api.ExecuteResponse, error) {
	fake.calls = append(fake.calls, "Execute")
	if fake.failSQL != "" && strings.EqualFold(strings.TrimSpace(req.SQL), strings.TrimSpace(fake.failSQL)) {
		return nil, errors.New("execute failed")
	}
	return &api.ExecuteResponse{Status: "QUEUED", TxID: req.TxID}, nil
}

func (fake *fakeSchemaInvoker) ExecuteBatch(_ context.Context, req *api.ExecuteBatchRequest) (*api.ExecuteBatchResponse, error) {
	fake.calls = append(fake.calls, "ExecuteBatch")
	return &api.ExecuteBatchResponse{Status: "OK", Executed: len(req.Statements)}, nil
}

func (fake *fakeSchemaInvoker) CommitTx(_ context.Context, _ *api.CommitTxRequest) (*api.CommitTxResponse, error) {
	fake.calls = append(fake.calls, "CommitTx")
	return &api.CommitTxResponse{Status: "COMMITTED"}, nil
}

func (fake *fakeSchemaInvoker) RollbackTx(_ context.Context, _ *api.RollbackTxRequest) (*api.RollbackTxResponse, error) {
	fake.calls = append(fake.calls, "RollbackTx")
	return &api.RollbackTxResponse{Status: "OK"}, nil
}

func (fake *fakeSchemaInvoker) SchemaSnapshot(_ context.Context, _ *api.SchemaSnapshotRequest) (*api.SchemaSnapshotResponse, error) {
	fake.calls = append(fake.calls, "SchemaSnapshot")
	if fake.schema != nil {
		return fake.schema, nil
	}
	return &api.SchemaSnapshotResponse{Status: "SNAPSHOT", Domains: []api.SchemaSnapshotDomain{}}, nil
}

func TestExecutableSchemaStatementsFiltersTransactionWrappers(t *testing.T) {
	statements := []string{"BEGIN DOMAIN accounts;", "CREATE TABLE users (id INT);", "ALTER TABLE users ADD COLUMN email TEXT;", "COMMIT;"}
	executable := executableSchemaStatements(statements)

	if len(executable) != 2 {
		t.Fatalf("expected 2 executable statements, got %d", len(executable))
	}
	if executable[0] != "CREATE TABLE users (id INT)" {
		t.Fatalf("unexpected first statement: %q", executable[0])
	}
	if executable[1] != "ALTER TABLE users ADD COLUMN email TEXT" {
		t.Fatalf("unexpected second statement: %q", executable[1])
	}
}

func TestApplySchemaDDLPlanSuccess(t *testing.T) {
	invoker := &fakeSchemaInvoker{}
	plan := schemaDDLResponse{Statements: []string{"BEGIN DOMAIN accounts;", "CREATE TABLE users (id INT);", "COMMIT;"}}

	response, err := applySchemaDDLPlan(context.Background(), invoker, "accounts", plan)
	if err != nil {
		t.Fatalf("apply schema should succeed: %v", err)
	}
	if response.Status != "COMMITTED" {
		t.Fatalf("expected COMMITTED status, got %q", response.Status)
	}
	if response.StatementCount != 1 {
		t.Fatalf("expected 1 statement, got %d", response.StatementCount)
	}
	if response.TxID == "" {
		t.Fatalf("expected tx id in response")
	}

	expectedCalls := []string{
		"BeginTx",
		"Execute",
		"CommitTx",
	}
	if strings.Join(invoker.calls, ",") != strings.Join(expectedCalls, ",") {
		t.Fatalf("unexpected call flow: %v", invoker.calls)
	}
}

func TestApplySchemaDDLPlanFailureRollsBack(t *testing.T) {
	invoker := &fakeSchemaInvoker{failSQL: "CREATE TABLE users (id INT)"}
	plan := schemaDDLResponse{Statements: []string{"BEGIN DOMAIN accounts;", "CREATE TABLE users (id INT);", "COMMIT;"}}

	_, err := applySchemaDDLPlan(context.Background(), invoker, "accounts", plan)
	if err == nil {
		t.Fatalf("expected apply schema to fail")
	}

	hasRollback := false
	for _, method := range invoker.calls {
		if method == "RollbackTx" {
			hasRollback = true
			break
		}
	}
	if !hasRollback {
		t.Fatalf("expected rollback call on failure, got call flow %v", invoker.calls)
	}
}

func TestApplySafeSchemaDiffAppliesOnlySafeStatements(t *testing.T) {
	invoker := &fakeSchemaInvoker{}
	diff := schemaDiffResponse{
		Domain:   "accounts",
		Safe:     false,
		Warnings: []string{"users.email removed: destructive change"},
		Operations: []schemaDiffOperation{
			{Type: "add_column", Table: "users", Column: "nickname", Safe: true, Statement: "ALTER TABLE users ADD COLUMN nickname TEXT;"},
			{Type: "drop_column", Table: "users", Column: "email", Safe: false, Reason: "column drop is potentially destructive"},
		},
		Statements: []string{"ALTER TABLE users ADD COLUMN nickname TEXT;"},
	}

	response, err := applySafeSchemaDiff(context.Background(), invoker, diff)
	if err != nil {
		t.Fatalf("apply safe diff should succeed: %v", err)
	}
	if response.AppliedCount != 1 {
		t.Fatalf("expected applied count 1, got %d", response.AppliedCount)
	}
	if response.UnsafeCount != 1 {
		t.Fatalf("expected unsafe count 1, got %d", response.UnsafeCount)
	}
	if response.DiffSafe {
		t.Fatalf("expected diff_safe=false")
	}
}

func TestSchemaSnapshotToDDLMapping(t *testing.T) {
	snapshot := &api.SchemaSnapshotResponse{
		Status: "SNAPSHOT",
		Domains: []api.SchemaSnapshotDomain{{
			Name: "accounts",
			Tables: []api.SchemaSnapshotTable{{
				Name: "users",
				Columns: []api.SchemaSnapshotColumn{{
					Name:       "id",
					Type:       "INT",
					PrimaryKey: true,
				}, {
					Name:   "email",
					Type:   "TEXT",
					Unique: true,
				}},
			}},
		}},
	}

	ddl, err := schemaSnapshotToDDL(snapshot, "accounts")
	if err != nil {
		t.Fatalf("schemaSnapshotToDDL failed: %v", err)
	}
	if ddl.Domain != "accounts" {
		t.Fatalf("expected domain accounts, got %q", ddl.Domain)
	}
	if len(ddl.Tables) != 1 || len(ddl.Tables[0].Columns) != 2 {
		t.Fatalf("unexpected tables/columns mapping: %+v", ddl)
	}
	if !ddl.Tables[0].Columns[0].PrimaryKey {
		t.Fatalf("expected first column to be primary key")
	}
}
