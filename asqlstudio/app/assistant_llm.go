package studioapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	api "asql/pkg/adminapi"
)

const (
	assistantLLMProviderOllama = "ollama"
	assistantLLMProviderOpenAI = "openai"
)

var (
	assistantReadOnlyPrefixPattern = regexp.MustCompile(`(?is)^\s*(select|with)\b`)
	assistantWriteKeywordPattern   = regexp.MustCompile(`(?i)\b(insert|update|delete|create|alter|drop|truncate|begin|commit|rollback)\b`)
	assistantFromJoinPattern       = regexp.MustCompile(`(?i)\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)`)
)

type assistantLLMClient interface {
	Plan(ctx context.Context, req assistantLLMPlanRequest) (*assistantLLMPlanEnvelope, error)
}

type assistantLLMPlanRequest struct {
	Settings       assistantLLMSettings
	Question       string
	Domains        []string
	SchemaOverview string
	FallbackSQL    string
	FallbackMode   string
	FallbackNotes  []string
}

type assistantLLMPlanEnvelope struct {
	SQL         string   `json:"sql"`
	Summary     string   `json:"summary"`
	Assumptions []string `json:"assumptions,omitempty"`
	Warnings    []string `json:"warnings,omitempty"`
	Mode        string   `json:"mode,omitempty"`
}

type httpAssistantLLMClient struct {
	httpClient *http.Client
}

func (a *App) assistQueryWithLLM(ctx context.Context, question string, domains []string, snapshot *api.SchemaSnapshotResponse, raw assistantLLMSettings) (*assistantQueryResponse, error) {
	fallbackPlan, fallbackErr := buildAssistantQueryPlan(question, domains, snapshot)
	settings, err := normalizeAssistantLLMSettings(raw)
	if err != nil {
		if raw.AllowFallback && fallbackPlan != nil && fallbackErr == nil {
			fallbackPlan.Warnings = uniqueStrings(append(fallbackPlan.Warnings, fmt.Sprintf("No pude usar el modelo configurado; devolví el plan determinista: %v", err)))
			return fallbackPlan, nil
		}
		return nil, err
	}

	client := a.assistantLLM
	if client == nil {
		client = &httpAssistantLLMClient{httpClient: &http.Client{Timeout: 45 * time.Second}}
	}

	fallbackSQL := ""
	fallbackMode := ""
	fallbackNotes := []string(nil)
	if fallbackPlan != nil && fallbackErr == nil {
		fallbackSQL = fallbackPlan.SQL
		fallbackMode = fallbackPlan.Mode
		fallbackNotes = append(fallbackNotes, fallbackPlan.Assumptions...)
		fallbackNotes = append(fallbackNotes, fallbackPlan.Warnings...)
	}

	envelope, err := client.Plan(ctx, assistantLLMPlanRequest{
		Settings:       settings,
		Question:       question,
		Domains:        domains,
		SchemaOverview: buildAssistantSchemaOverview(question, domains, snapshot),
		FallbackSQL:    fallbackSQL,
		FallbackMode:   fallbackMode,
		FallbackNotes:  uniqueStrings(fallbackNotes),
	})
	if err != nil {
		if settings.AllowFallback && fallbackPlan != nil && fallbackErr == nil {
			fallbackPlan.Warnings = uniqueStrings(append(fallbackPlan.Warnings, fmt.Sprintf("El modelo %s no respondió como esperaba; devolví el plan determinista validado. Detalle: %v", settings.Model, err)))
			return fallbackPlan, nil
		}
		return nil, err
	}

	validatedSQL, err := a.validateAssistantGeneratedSQL(ctx, envelope.SQL, domains)
	if err != nil {
		if settings.AllowFallback && fallbackPlan != nil && fallbackErr == nil {
			fallbackPlan.Warnings = uniqueStrings(append(fallbackPlan.Warnings, fmt.Sprintf("El SQL del modelo fue rechazado por las guardas de lectura; devolví el plan determinista. Detalle: %v", err)))
			return fallbackPlan, nil
		}
		return nil, err
	}

	meta := summarizeAssistantSQL(validatedSQL)
	mode := strings.TrimSpace(envelope.Mode)
	if mode == "" {
		mode = meta.Mode
	}
	if mode == "" {
		mode = "read"
	}
	summary := strings.TrimSpace(envelope.Summary)
	if summary == "" {
		summary = fmt.Sprintf("Consulta generada por el modelo y validada como solo lectura sobre %s.", meta.DomainTableLabel())
	}
	warnings := uniqueStrings(envelope.Warnings)
	assumptions := uniqueStrings(envelope.Assumptions)
	if fallbackSQL != "" {
		assumptions = uniqueStrings(append(assumptions, "La salida del modelo se validó con el parser de ASQL antes de mostrarse."))
	}

	confidence := "medium"
	if len(warnings) == 0 {
		confidence = "high"
	}
	if len(warnings) > 1 {
		confidence = "low"
	}

	return &assistantQueryResponse{
		Status:         "OK",
		Question:       question,
		Domain:         firstAssistantDomain(meta.PrimaryDomain, domains),
		Mode:           mode,
		Planner:        "llm",
		Provider:       settings.Provider,
		Model:          settings.Model,
		Summary:        summary,
		SQL:            validatedSQL,
		PrimaryTable:   meta.PrimaryTable,
		MatchedTables:  meta.Tables,
		MatchedColumns: meta.Columns,
		Assumptions:    assumptions,
		Warnings:       warnings,
		Confidence:     confidence,
	}, nil
}

func normalizeAssistantLLMSettings(raw assistantLLMSettings) (assistantLLMSettings, error) {
	settings := raw
	settings.Provider = strings.ToLower(strings.TrimSpace(settings.Provider))
	settings.BaseURL = strings.TrimSpace(settings.BaseURL)
	settings.Model = strings.TrimSpace(settings.Model)
	settings.APIKey = strings.TrimSpace(settings.APIKey)

	if settings.Provider == "" {
		settings.Provider = strings.ToLower(strings.TrimSpace(os.Getenv("ASQL_STUDIO_LLM_PROVIDER")))
	}
	if settings.Provider == "" {
		settings.Provider = assistantLLMProviderOllama
	}
	if settings.BaseURL == "" {
		settings.BaseURL = strings.TrimSpace(os.Getenv("ASQL_STUDIO_LLM_BASE_URL"))
	}
	if settings.Model == "" {
		settings.Model = strings.TrimSpace(os.Getenv("ASQL_STUDIO_LLM_MODEL"))
	}
	if settings.APIKey == "" {
		settings.APIKey = strings.TrimSpace(os.Getenv("ASQL_STUDIO_LLM_API_KEY"))
	}
	if settings.Temperature == 0 {
		settings.Temperature = 0.1
	}

	switch settings.Provider {
	case assistantLLMProviderOllama:
		if settings.BaseURL == "" {
			settings.BaseURL = "http://127.0.0.1:11434"
		}
	case assistantLLMProviderOpenAI:
		if settings.BaseURL == "" {
			settings.BaseURL = "https://api.openai.com/v1"
		}
		if settings.APIKey == "" {
			return assistantLLMSettings{}, fmt.Errorf("api key is required for provider %q", settings.Provider)
		}
	default:
		return assistantLLMSettings{}, fmt.Errorf("unsupported LLM provider %q", settings.Provider)
	}

	if settings.Model == "" {
		return assistantLLMSettings{}, fmt.Errorf("model is required when LLM planning is enabled")
	}

	settings.BaseURL = strings.TrimRight(settings.BaseURL, "/")
	return settings, nil
}

func (c *httpAssistantLLMClient) Plan(ctx context.Context, req assistantLLMPlanRequest) (*assistantLLMPlanEnvelope, error) {
	if c == nil || c.httpClient == nil {
		c = &httpAssistantLLMClient{httpClient: &http.Client{Timeout: 45 * time.Second}}
	}

	systemPrompt, userPrompt := buildAssistantLLMPrompts(req)
	var raw string
	var err error
	switch req.Settings.Provider {
	case assistantLLMProviderOllama:
		raw, err = c.planWithOllama(ctx, req.Settings, systemPrompt, userPrompt)
	case assistantLLMProviderOpenAI:
		raw, err = c.planWithOpenAI(ctx, req.Settings, systemPrompt, userPrompt)
	default:
		return nil, fmt.Errorf("unsupported LLM provider %q", req.Settings.Provider)
	}
	if err != nil {
		return nil, err
	}
	return decodeAssistantLLMEnvelope(raw)
}

func buildAssistantLLMPrompts(req assistantLLMPlanRequest) (string, string) {
	system := strings.Join([]string{
		"You are the SQL planner inside ASQL Studio.",
		"Return exactly one read-only ASQL query using only the provided schema.",
		"Allowed shape: SELECT or WITH ... SELECT. Never emit INSERT, UPDATE, DELETE, DDL, comments, or multiple statements.",
		"Prefer COUNT(*) AS total for counts and LIMIT 100 for open-ended row listings.",
		"Return JSON only with keys: sql, summary, assumptions, warnings, mode.",
		"The sql value must end with a semicolon.",
	}, "\n")

	var user strings.Builder
	user.WriteString("Question:\n")
	user.WriteString(req.Question)
	user.WriteString("\n\nSelected domains:\n")
	user.WriteString(strings.Join(req.Domains, ", "))
	user.WriteString("\n\nSchema:\n")
	user.WriteString(req.SchemaOverview)
	if strings.TrimSpace(req.FallbackSQL) != "" {
		user.WriteString("\n\nDeterministic fallback candidate (use only if it helps, you may improve it):\n")
		user.WriteString(req.FallbackSQL)
		if strings.TrimSpace(req.FallbackMode) != "" {
			user.WriteString("\nFallback mode: ")
			user.WriteString(req.FallbackMode)
		}
	}
	if len(req.FallbackNotes) > 0 {
		user.WriteString("\n\nFallback notes:\n- ")
		user.WriteString(strings.Join(req.FallbackNotes, "\n- "))
	}
	return system, user.String()
}

func (c *httpAssistantLLMClient) planWithOllama(ctx context.Context, settings assistantLLMSettings, systemPrompt, userPrompt string) (string, error) {
	payload := map[string]interface{}{
		"model":  settings.Model,
		"stream": false,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": userPrompt},
		},
		"options": map[string]float64{"temperature": settings.Temperature},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, settings.BaseURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	rawBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("ollama returned %s: %s", resp.Status, strings.TrimSpace(string(rawBody)))
	}
	var decoded struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(rawBody, &decoded); err != nil {
		return "", err
	}
	return decoded.Message.Content, nil
}

func (c *httpAssistantLLMClient) planWithOpenAI(ctx context.Context, settings assistantLLMSettings, systemPrompt, userPrompt string) (string, error) {
	payload := map[string]interface{}{
		"model":       settings.Model,
		"temperature": settings.Temperature,
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": userPrompt},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, settings.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+settings.APIKey)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	rawBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("openai-compatible endpoint returned %s: %s", resp.Status, strings.TrimSpace(string(rawBody)))
	}
	var decoded struct {
		Choices []struct {
			Message struct {
				Content interface{} `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rawBody, &decoded); err != nil {
		return "", err
	}
	if len(decoded.Choices) == 0 {
		return "", fmt.Errorf("model response did not include any choices")
	}
	return openAIMessageText(decoded.Choices[0].Message.Content), nil
}

func openAIMessageText(content interface{}) string {
	switch typed := content.(type) {
	case string:
		return typed
	case []interface{}:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			obj, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			if text, ok := obj["text"].(string); ok {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func decodeAssistantLLMEnvelope(raw string) (*assistantLLMPlanEnvelope, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("model response was empty")
	}
	if jsonBlock := extractAssistantJSONObject(trimmed); jsonBlock != "" {
		var envelope assistantLLMPlanEnvelope
		if err := json.Unmarshal([]byte(jsonBlock), &envelope); err == nil {
			return &envelope, nil
		}
	}
	sql := extractAssistantSQL(trimmed)
	if sql == "" {
		return nil, fmt.Errorf("model response did not include a usable JSON envelope or SQL statement")
	}
	return &assistantLLMPlanEnvelope{
		SQL:         sql,
		Summary:     "Consulta generada por el modelo y recuperada sin el sobre JSON esperado.",
		Warnings:    []string{"El modelo no devolvió el JSON solicitado; solo se recuperó el SQL."},
		Assumptions: []string{"La respuesta del modelo se redujo a un único SQL antes de validarla."},
	}, nil
}

func extractAssistantJSONObject(raw string) string {
	if strings.HasPrefix(raw, "```") {
		parts := strings.Split(raw, "```")
		for _, part := range parts {
			candidate := strings.TrimSpace(strings.TrimPrefix(part, "json"))
			if strings.HasPrefix(candidate, "{") && strings.HasSuffix(candidate, "}") {
				return candidate
			}
		}
	}
	start := strings.Index(raw, "{")
	if start < 0 {
		return ""
	}
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == '"' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return raw[start : i+1]
			}
		}
	}
	return ""
}

func extractAssistantSQL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, "```") {
		parts := strings.Split(trimmed, "```")
		for _, part := range parts {
			candidate := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(part, "sql"), "SQL"))
			if startsWithAssistantSQL(candidate) {
				return candidate
			}
		}
	}
	if startsWithAssistantSQL(trimmed) {
		return trimmed
	}
	upper := strings.ToUpper(trimmed)
	idx := strings.Index(upper, "SELECT ")
	if idx < 0 {
		idx = strings.Index(upper, "WITH ")
	}
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(trimmed[idx:])
}

func startsWithAssistantSQL(value string) bool {
	upper := strings.ToUpper(strings.TrimSpace(value))
	return strings.HasPrefix(upper, "SELECT ") || strings.HasPrefix(upper, "WITH ")
}

func (a *App) validateAssistantGeneratedSQL(ctx context.Context, sql string, domains []string) (string, error) {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return "", fmt.Errorf("model returned an empty SQL string")
	}
	if hasAssistantInternalSemicolon(trimmed) {
		return "", fmt.Errorf("multiple SQL statements are not allowed")
	}
	if !assistantReadOnlyPrefixPattern.MatchString(trimmed) {
		return "", fmt.Errorf("generated SQL must start with SELECT or WITH")
	}
	if assistantWriteKeywordPattern.MatchString(trimmed) && !assistantReadOnlyPrefixPattern.MatchString(trimmed) {
		return "", fmt.Errorf("generated SQL must be read-only")
	}
	if !strings.HasSuffix(trimmed, ";") {
		trimmed += ";"
	}
	client := a.getLeaderClient()
	if client == nil {
		client = a.engine
	}
	if client != nil {
		if _, err := client.ExplainQuery(ctx, &api.ExplainQueryRequest{SQL: trimmed, Domains: domains}); err != nil {
			return "", fmt.Errorf("generated SQL did not validate in ASQL: %w", err)
		}
	}
	return trimmed, nil
}

func hasAssistantInternalSemicolon(sql string) bool {
	inString := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			if inString && i+1 < len(sql) && sql[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		if ch == ';' && i != len(sql)-1 {
			for j := i + 1; j < len(sql); j++ {
				if sql[j] != ' ' && sql[j] != '\n' && sql[j] != '\r' && sql[j] != '\t' {
					return true
				}
			}
		}
	}
	return false
}

type assistantStatementSummary struct {
	PrimaryDomain string
	PrimaryTable  string
	Tables        []string
	Columns       []string
	Mode          string
}

func (s assistantStatementSummary) DomainTableLabel() string {
	if s.PrimaryTable == "" {
		return "los dominios seleccionados"
	}
	if s.PrimaryDomain == "" {
		return s.PrimaryTable
	}
	return s.PrimaryDomain + "." + s.PrimaryTable
}

func summarizeAssistantSQL(sql string) assistantStatementSummary {
	summary := assistantStatementSummary{}
	matches := assistantFromJoinPattern.FindAllStringSubmatch(sql, -1)
	for _, match := range matches {
		if len(match) == 2 {
			summary.Tables = append(summary.Tables, strings.TrimSpace(match[1]))
		}
	}
	summary.Tables = uniqueStrings(filterAssistantValues(summary.Tables))
	if len(summary.Tables) > 0 {
		summary.PrimaryTable = summary.Tables[0]
	}
	summary.Columns = extractAssistantSelectColumns(sql)
	summary.Mode = inferAssistantModeFromSQL(sql)
	return summary
}

func inferAssistantModeFromSQL(sql string) string {
	joinedColumns := strings.ToLower(sql)
	switch {
	case strings.Contains(joinedColumns, "count("):
		return "count"
	case strings.Contains(joinedColumns, "sum("):
		return "sum"
	case strings.Contains(joinedColumns, "avg("):
		return "avg"
	case strings.Contains(joinedColumns, "max("):
		return "max"
	case strings.Contains(joinedColumns, "min("):
		return "min"
	case strings.Contains(joinedColumns, " order by ") && strings.Contains(joinedColumns, " limit "):
		return "latest"
	default:
		return "read"
	}
}

func extractAssistantSelectColumns(sql string) []string {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	selectIdx := strings.Index(upper, "SELECT ")
	fromIdx := strings.Index(upper, " FROM ")
	if selectIdx < 0 || fromIdx <= selectIdx+len("SELECT ") {
		return nil
	}
	columnsPart := trimmed[selectIdx+len("SELECT ") : fromIdx]
	parts := strings.Split(columnsPart, ",")
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmedPart := strings.TrimSpace(part)
		if trimmedPart == "" {
			continue
		}
		columns = append(columns, trimmedPart)
	}
	return uniqueStrings(columns)
}

func filterAssistantValues(values []string) []string {
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		filtered = append(filtered, trimmed)
	}
	return filtered
}

func firstAssistantDomain(primary string, domains []string) string {
	if strings.TrimSpace(primary) != "" {
		return primary
	}
	if len(domains) == 0 {
		return ""
	}
	return domains[0]
}

func buildAssistantSchemaOverview(question string, preferredDomains []string, snapshot *api.SchemaSnapshotResponse) string {
	orderedDomains := orderedAssistantDomains(preferredDomains, snapshot)
	if len(orderedDomains) == 0 {
		return "(no schema available)"
	}
	normalizedQuestion := normalizeAssistantText(question)
	questionTokens := uniqueStrings(assistantTokenPattern.FindAllString(normalizedQuestion, -1))
	candidates := make([]assistantTableCandidate, 0)
	for _, domain := range orderedDomains {
		for _, table := range domain.Tables {
			candidate := scoreAssistantTable(normalizedQuestion, questionTokens, domain.Name, table)
			if candidate.score == 0 {
				candidate = assistantTableCandidate{domain: domain.Name, table: table, score: 0}
			}
			candidates = append(candidates, candidate)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		if candidates[i].domain != candidates[j].domain {
			return candidates[i].domain < candidates[j].domain
		}
		return candidates[i].table.Name < candidates[j].table.Name
	})
	if len(candidates) > 8 {
		candidates = candidates[:8]
	}
	var b strings.Builder
	currentDomain := ""
	for _, candidate := range candidates {
		if candidate.domain != currentDomain {
			if b.Len() > 0 {
				b.WriteString("\n")
			}
			currentDomain = candidate.domain
			b.WriteString("Domain ")
			b.WriteString(candidate.domain)
			b.WriteString(":\n")
		}
		b.WriteString("- ")
		b.WriteString(candidate.table.Name)
		b.WriteString("(")
		columnParts := make([]string, 0, len(candidate.table.Columns))
		for _, column := range candidate.table.Columns {
			part := column.Name + " " + column.Type
			if column.PrimaryKey {
				part += " PRIMARY KEY"
			}
			columnParts = append(columnParts, part)
		}
		b.WriteString(strings.Join(columnParts, ", "))
		b.WriteString(")\n")
	}
	return strings.TrimSpace(b.String())
}
