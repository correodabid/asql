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

func mustAssistantCatalog(t *testing.T) *assistantLLMCatalog {
	t.Helper()
	catalog, err := loadAssistantLLMCatalog()
	if err != nil {
		t.Fatalf("loadAssistantLLMCatalog: %v", err)
	}
	return catalog
}

func mustAssistantProvider(t *testing.T, description string, match func(assistantLLMProviderCatalog) bool) assistantLLMProviderCatalog {
	t.Helper()
	catalog := mustAssistantCatalog(t)
	for _, provider := range catalog.Providers {
		if match(provider) {
			return provider
		}
	}
	t.Fatalf("assistant provider not found for %s", description)
	return assistantLLMProviderCatalog{}
}

func mustAssistantProviderModel(t *testing.T, provider assistantLLMProviderCatalog) string {
	t.Helper()
	if len(provider.Models) > 0 {
		return provider.Models[0].ID
	}
	if provider.ModelPlaceholder != "" {
		return provider.ModelPlaceholder
	}
	t.Fatalf("assistant provider %q has no model or placeholder", provider.ID)
	return ""
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
	provider := mustAssistantProvider(t, "llm request without mandatory api key", func(provider assistantLLMProviderCatalog) bool {
		return provider.APIKeyMode != assistantLLMAPIKeyModeRequired && provider.DefaultBaseURL != ""
	})
	model := mustAssistantProviderModel(t, provider)

	resp, err := app.AssistQuery(assistantQueryRequest{
		Question: "show me the latest 3 orders",
		Domains:  []string{"default"},
		LLM: &assistantLLMSettings{
			Enabled:  true,
			Provider: provider.ID,
			BaseURL:  provider.DefaultBaseURL,
			Model:    model,
		},
	})
	if err != nil {
		t.Fatalf("AssistQuery: %v", err)
	}
	if resp.Planner != "llm" {
		t.Fatalf("unexpected planner: %q", resp.Planner)
	}
	if resp.Provider != provider.ID {
		t.Fatalf("unexpected provider: %q", resp.Provider)
	}
	if resp.Model != model {
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
	provider := mustAssistantProvider(t, "fallback request without mandatory api key", func(provider assistantLLMProviderCatalog) bool {
		return provider.APIKeyMode != assistantLLMAPIKeyModeRequired && provider.DefaultBaseURL != ""
	})

	resp, err := app.AssistQuery(assistantQueryRequest{
		Question: "find users with email \"alice@example.com\"",
		Domains:  []string{"default"},
		LLM: &assistantLLMSettings{
			Enabled:       true,
			Provider:      provider.ID,
			BaseURL:       provider.DefaultBaseURL,
			Model:         mustAssistantProviderModel(t, provider),
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

func TestAssistantLLMCatalogLoadsProvidersFromJSON(t *testing.T) {
	catalog := mustAssistantCatalog(t)
	if catalog.DefaultProvider == "" {
		t.Fatalf("unexpected default provider: %q", catalog.DefaultProvider)
	}
	defaultProvider, ok := catalog.providerByID(catalog.DefaultProvider)
	if !ok {
		t.Fatalf("expected default provider %q to exist", catalog.DefaultProvider)
	}
	if defaultProvider.Transport.Type != assistantLLMTransportHTTPJSON {
		t.Fatalf("unexpected default transport type: %q", defaultProvider.Transport.Type)
	}
	hasRequiredAPIKeyProvider := false
	hasOptionalAPIKeyProvider := false
	for _, provider := range catalog.Providers {
		if provider.Transport.Type != assistantLLMTransportHTTPJSON {
			t.Fatalf("unexpected transport type for provider %q: %q", provider.ID, provider.Transport.Type)
		}
		if len(provider.Models) == 0 {
			t.Fatalf("expected provider %q to expose catalog models", provider.ID)
		}
		switch provider.APIKeyMode {
		case assistantLLMAPIKeyModeRequired:
			hasRequiredAPIKeyProvider = true
		case assistantLLMAPIKeyModeOptional, assistantLLMAPIKeyModeNone:
			hasOptionalAPIKeyProvider = true
		}
	}
	if !hasRequiredAPIKeyProvider {
		t.Fatal("expected at least one provider requiring an api key")
	}
	if !hasOptionalAPIKeyProvider {
		t.Fatal("expected at least one provider without mandatory api key")
	}
}

func TestNormalizeAssistantLLMSettingsUsesCatalogDefaults(t *testing.T) {
	provider := mustAssistantProvider(t, "provider with default base url", func(provider assistantLLMProviderCatalog) bool {
		return provider.DefaultBaseURL != "" && (len(provider.Models) > 0 || provider.ModelPlaceholder != "")
	})
	settings, err := normalizeAssistantLLMSettings(assistantLLMSettings{
		Enabled:  true,
		Provider: provider.ID,
		Model:    mustAssistantProviderModel(t, provider),
	})
	if err != nil {
		t.Fatalf("normalizeAssistantLLMSettings: %v", err)
	}
	if settings.BaseURL != provider.DefaultBaseURL {
		t.Fatalf("unexpected default base url: %q", settings.BaseURL)
	}
}

func TestNormalizeAssistantLLMSettingsRequiresCatalogConfiguredAPIKey(t *testing.T) {
	provider := mustAssistantProvider(t, "provider requiring api key", func(provider assistantLLMProviderCatalog) bool {
		return provider.APIKeyMode == assistantLLMAPIKeyModeRequired && (len(provider.Models) > 0 || provider.ModelPlaceholder != "")
	})
	_, err := normalizeAssistantLLMSettings(assistantLLMSettings{
		Enabled:  true,
		Provider: provider.ID,
		Model:    mustAssistantProviderModel(t, provider),
	})
	if err == nil {
		t.Fatal("expected missing api key to be rejected")
	}
	if !strings.Contains(err.Error(), "api key is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNormalizeAssistantLLMSettingsSupportsRequiredAPIKeyProvider(t *testing.T) {
	provider := mustAssistantProvider(t, "configured provider requiring api key", func(provider assistantLLMProviderCatalog) bool {
		return provider.APIKeyMode == assistantLLMAPIKeyModeRequired && provider.DefaultBaseURL != "" && (len(provider.Models) > 0 || provider.ModelPlaceholder != "")
	})
	settings, err := normalizeAssistantLLMSettings(assistantLLMSettings{
		Enabled:  true,
		Provider: provider.ID,
		Model:    mustAssistantProviderModel(t, provider),
		APIKey:   "test-api-key",
	})
	if err != nil {
		t.Fatalf("normalizeAssistantLLMSettings: %v", err)
	}
	if settings.BaseURL != provider.DefaultBaseURL {
		t.Fatalf("unexpected provider base url: %q", settings.BaseURL)
	}
}

func TestValidateAssistantGeneratedSQLRejectsWrites(t *testing.T) {
	if _, err := (&App{}).validateAssistantGeneratedSQL(context.Background(), "DELETE FROM users;", []string{"default"}); err == nil {
		t.Fatal("expected DELETE to be rejected")
	}
}
