package studioapp

import (
	"context"
	"fmt"
	"strings"
	"testing"

	api "asql/pkg/adminapi"
)

type fakeAssistantLLMClient struct {
	plan *assistantLLMPlanEnvelope
	err  error
	seen []assistantLLMPlanRequest
}

func (f *fakeAssistantLLMClient) Plan(_ context.Context, req assistantLLMPlanRequest) (*assistantLLMPlanEnvelope, error) {
	f.seen = append(f.seen, req)
	if f.err != nil {
		return nil, f.err
	}
	if f.plan == nil {
		return nil, fmt.Errorf("no plan configured")
	}
	return f.plan, nil
}

func TestBuildAssistantQueryPlanCount(t *testing.T) {
	snapshot := &api.SchemaSnapshotResponse{
		Status: "SNAPSHOT",
		Domains: []api.SchemaSnapshotDomain{{
			Name: "default",
			Tables: []api.SchemaSnapshotTable{
				{Name: "users", Columns: []api.SchemaSnapshotColumn{{Name: "id", Type: "INT", PrimaryKey: true}, {Name: "email", Type: "TEXT"}}},
				{Name: "orders", Columns: []api.SchemaSnapshotColumn{{Name: "id", Type: "INT", PrimaryKey: true}, {Name: "amount", Type: "DECIMAL"}}},
			},
		}},
	}

	plan, err := buildAssistantQueryPlan("¿Cuántos users hay?", []string{"default"}, snapshot)
	if err != nil {
		t.Fatalf("buildAssistantQueryPlan: %v", err)
	}
	if plan.Mode != "count" {
		t.Fatalf("unexpected mode: got %q", plan.Mode)
	}
	if plan.PrimaryTable != "users" {
		t.Fatalf("unexpected primary table: got %q", plan.PrimaryTable)
	}
	if got, want := plan.SQL, "SELECT COUNT(*) AS total FROM users;"; got != want {
		t.Fatalf("unexpected sql:\n got: %s\nwant: %s", got, want)
	}
}

func TestBuildAssistantQueryPlanLatest(t *testing.T) {
	snapshot := &api.SchemaSnapshotResponse{
		Status: "SNAPSHOT",
		Domains: []api.SchemaSnapshotDomain{{
			Name: "default",
			Tables: []api.SchemaSnapshotTable{{
				Name: "orders",
				Columns: []api.SchemaSnapshotColumn{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "amount", Type: "DECIMAL"},
					{Name: "created_at", Type: "TIMESTAMP"},
				},
			}},
		}},
	}

	plan, err := buildAssistantQueryPlan("Muéstrame los últimos 5 orders", []string{"default"}, snapshot)
	if err != nil {
		t.Fatalf("buildAssistantQueryPlan: %v", err)
	}
	if plan.Mode != "latest" {
		t.Fatalf("unexpected mode: got %q", plan.Mode)
	}
	if got, want := plan.SQL, "SELECT * FROM orders ORDER BY created_at DESC LIMIT 5;"; got != want {
		t.Fatalf("unexpected sql:\n got: %s\nwant: %s", got, want)
	}
}

func TestAppAssistQueryUsesSchemaSnapshot(t *testing.T) {
	fake := &fakeSchemaInvoker{schema: &api.SchemaSnapshotResponse{
		Status: "SNAPSHOT",
		Domains: []api.SchemaSnapshotDomain{{
			Name: "default",
			Tables: []api.SchemaSnapshotTable{{
				Name: "users",
				Columns: []api.SchemaSnapshotColumn{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "email", Type: "TEXT"},
				},
			}},
		}},
	}}
	app := &App{schemaInvoker: fake}

	resp, err := app.AssistQuery(assistantQueryRequest{
		Question: `find users with email "alice@example.com"`,
		Domains:  []string{"default"},
	})
	if err != nil {
		t.Fatalf("AssistQuery: %v", err)
	}
	if len(fake.calls) == 0 || fake.calls[0] != "SchemaSnapshot" {
		t.Fatalf("expected SchemaSnapshot call, got %+v", fake.calls)
	}
	if got, want := resp.SQL, "SELECT email FROM users WHERE email = 'alice@example.com' LIMIT 100;"; got != want {
		t.Fatalf("unexpected sql:\n got: %s\nwant: %s", got, want)
	}
	if resp.Confidence == "" {
		t.Fatal("expected confidence to be set")
	}
}

func TestAppAssistQueryUsesLLMWhenEnabled(t *testing.T) {
	fakeSchema := &fakeSchemaInvoker{schema: &api.SchemaSnapshotResponse{
		Status: "SNAPSHOT",
		Domains: []api.SchemaSnapshotDomain{{
			Name: "default",
			Tables: []api.SchemaSnapshotTable{{
				Name: "orders",
				Columns: []api.SchemaSnapshotColumn{
					{Name: "id", Type: "INT", PrimaryKey: true},
					{Name: "amount", Type: "DECIMAL"},
					{Name: "created_at", Type: "TIMESTAMP"},
				},
			}},
		}},
	}}
	fakeLLM := &fakeAssistantLLMClient{plan: &assistantLLMPlanEnvelope{
		SQL:         "SELECT id, amount FROM orders ORDER BY created_at DESC LIMIT 3;",
		Summary:     "Trae los últimos pedidos.",
		Assumptions: []string{"Interpreto latest como orden descendente por created_at."},
		Mode:        "latest",
	}}
	app := &App{schemaInvoker: fakeSchema, assistantLLM: fakeLLM}

	resp, err := app.AssistQuery(assistantQueryRequest{
		Question: "show me the latest 3 orders",
		Domains:  []string{"default"},
		LLM: &assistantLLMSettings{
			Enabled:  true,
			Provider: assistantLLMProviderOllama,
			BaseURL:  "http://127.0.0.1:11434",
			Model:    "llama3.2",
		},
	})
	if err != nil {
		t.Fatalf("AssistQuery: %v", err)
	}
	if resp.Planner != "llm" {
		t.Fatalf("unexpected planner: %q", resp.Planner)
	}
	if resp.Provider != assistantLLMProviderOllama {
		t.Fatalf("unexpected provider: %q", resp.Provider)
	}
	if resp.Model != "llama3.2" {
		t.Fatalf("unexpected model: %q", resp.Model)
	}
	if got, want := resp.SQL, "SELECT id, amount FROM orders ORDER BY created_at DESC LIMIT 3;"; got != want {
		t.Fatalf("unexpected sql:\n got: %s\nwant: %s", got, want)
	}
	if len(fakeLLM.seen) != 1 {
		t.Fatalf("expected a single llm call, got %d", len(fakeLLM.seen))
	}
	if !strings.Contains(fakeLLM.seen[0].SchemaOverview, "orders") {
		t.Fatalf("expected schema overview to mention orders, got %q", fakeLLM.seen[0].SchemaOverview)
	}
}

func TestAppAssistQueryFallsBackWhenLLMUnavailable(t *testing.T) {
	fakeSchema := &fakeSchemaInvoker{schema: &api.SchemaSnapshotResponse{
		Status: "SNAPSHOT",
		Domains: []api.SchemaSnapshotDomain{{
			Name: "default",
			Tables: []api.SchemaSnapshotTable{{
				Name:    "users",
				Columns: []api.SchemaSnapshotColumn{{Name: "id", Type: "INT", PrimaryKey: true}, {Name: "email", Type: "TEXT"}},
			}},
		}},
	}}
	fakeLLM := &fakeAssistantLLMClient{err: fmt.Errorf("connection refused")}
	app := &App{schemaInvoker: fakeSchema, assistantLLM: fakeLLM}

	resp, err := app.AssistQuery(assistantQueryRequest{
		Question: "find users with email \"alice@example.com\"",
		Domains:  []string{"default"},
		LLM: &assistantLLMSettings{
			Enabled:       true,
			Provider:      assistantLLMProviderOllama,
			BaseURL:       "http://127.0.0.1:11434",
			Model:         "llama3.2",
			AllowFallback: true,
		},
	})
	if err != nil {
		t.Fatalf("AssistQuery: %v", err)
	}
	if resp.Planner != "deterministic" {
		t.Fatalf("expected deterministic fallback, got %q", resp.Planner)
	}
	if len(resp.Warnings) == 0 {
		t.Fatal("expected fallback warning to be present")
	}
	if got, want := resp.SQL, "SELECT email FROM users WHERE email = 'alice@example.com' LIMIT 100;"; got != want {
		t.Fatalf("unexpected fallback sql:\n got: %s\nwant: %s", got, want)
	}
}

func TestValidateAssistantGeneratedSQLRejectsWrites(t *testing.T) {
	if _, err := (&App{}).validateAssistantGeneratedSQL(context.Background(), "DELETE FROM users;", []string{"default"}); err == nil {
		t.Fatal("expected DELETE to be rejected")
	}
}
