package studioapp

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	api "asql/pkg/adminapi"
)

var (
	assistantTokenPattern       = regexp.MustCompile(`[a-z0-9]+`)
	assistantQuotedValuePattern = regexp.MustCompile(`["']([^"']+)["']`)
	assistantTopNPattern        = regexp.MustCompile(`\b(top|last|latest|first)\s+(\d+)\b`)
	assistantReverseTopNPattern = regexp.MustCompile(`\b(\d+)\s+(latest|last)\b`)
	assistantIDPattern          = regexp.MustCompile(`\bid\s+(\d+)\b`)
)

var assistantStopWords = map[string]struct{}{
	"a": {}, "all": {}, "by": {}, "for": {}, "from": {}, "list": {},
	"show": {}, "the": {}, "top": {}, "where": {}, "with": {},
}

var assistantGenericColumnTokens = map[string]struct{}{
	"created": {}, "date": {}, "id": {}, "name": {}, "status": {}, "time": {}, "updated": {},
}

var assistantStatusKeywords = []string{"active", "inactive", "pending", "paid", "open", "closed", "enabled", "disabled"}

type assistantTableCandidate struct {
	domain         string
	table          api.SchemaSnapshotTable
	score          int
	exactNameMatch bool
	matchedColumns []string
}

func (a *App) AssistQuery(req assistantQueryRequest) (*assistantQueryResponse, error) {
	question := strings.TrimSpace(req.Question)
	if question == "" {
		return nil, fmt.Errorf("question is required")
	}
	if len(req.Domains) == 0 {
		return nil, fmt.Errorf("domains are required")
	}

	domains := make([]string, 0, len(req.Domains))
	for _, raw := range req.Domains {
		domain, err := normalizeSchemaDomain(raw)
		if err != nil {
			return nil, err
		}
		domains = append(domains, domain)
	}

	ctx, cancel := a.reqCtx()
	defer cancel()

	invoker := a.schemaInvoker
	if invoker == nil {
		invoker = a.getLeaderClient()
	}

	snapshot, err := invoker.SchemaSnapshot(ctx, &api.SchemaSnapshotRequest{Domains: domains})
	if err != nil {
		return nil, err
	}

	if req.LLM != nil && req.LLM.Enabled {
		return a.assistQueryWithLLM(ctx, question, domains, snapshot, *req.LLM)
	}

	return buildAssistantQueryPlan(question, domains, snapshot)
}

func buildAssistantQueryPlan(question string, preferredDomains []string, snapshot *api.SchemaSnapshotResponse) (*assistantQueryResponse, error) {
	normalizedQuestion := normalizeAssistantText(question)
	questionTokens := uniqueStrings(assistantTokenPattern.FindAllString(normalizedQuestion, -1))
	if len(questionTokens) == 0 {
		return nil, fmt.Errorf("question did not contain any usable terms")
	}

	orderedDomains := orderedAssistantDomains(preferredDomains, snapshot)
	if len(orderedDomains) == 0 {
		return nil, fmt.Errorf("no schema snapshot available for the selected domains")
	}

	candidates := make([]assistantTableCandidate, 0)
	totalTables := 0
	for _, domain := range orderedDomains {
		for _, table := range domain.Tables {
			totalTables++
			candidate := scoreAssistantTable(normalizedQuestion, questionTokens, domain.Name, table)
			if candidate.score > 0 {
				candidates = append(candidates, candidate)
			}
		}
	}

	warnings := make([]string, 0)
	assumptions := make([]string, 0)

	if len(candidates) == 0 {
		if totalTables == 1 {
			domain := orderedDomains[0]
			chosen := assistantTableCandidate{domain: domain.Name, table: domain.Tables[0], score: 1}
			warnings = append(warnings, "Did not find a clear match for the question; used the only table available in the domain.")
			return finalizeAssistantPlan(question, normalizedQuestion, questionTokens, chosen, warnings, assumptions)
		}
		return nil, fmt.Errorf("could not match the question to a table in the selected domains")
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

	chosen := candidates[0]
	if len(candidates) > 1 && candidates[1].score == chosen.score && candidates[1].table.Name != chosen.table.Name {
		warnings = append(warnings, fmt.Sprintf("The question could also refer to %s.%s; review the SQL before running it.", candidates[1].domain, candidates[1].table.Name))
	}
	if !chosen.exactNameMatch {
		assumptions = append(assumptions, fmt.Sprintf("Inferred %s.%s as the primary table based on table and column name similarity.", chosen.domain, chosen.table.Name))
	}

	return finalizeAssistantPlan(question, normalizedQuestion, questionTokens, chosen, warnings, assumptions)
}

func finalizeAssistantPlan(question, normalizedQuestion string, questionTokens []string, chosen assistantTableCandidate, warnings, assumptions []string) (*assistantQueryResponse, error) {
	mode := detectAssistantMode(questionTokens)
	limit := detectAssistantLimit(normalizedQuestion, mode)
	matchedColumns := matchedAssistantColumns(questionTokens, chosen.table)
	filters, filterAssumptions := buildAssistantFilters(question, questionTokens, chosen.table, matchedColumns)
	assumptions = append(assumptions, filterAssumptions...)

	timeColumn := chooseAssistantTimeColumn(chosen.table)
	metricColumn := chooseAssistantMetricColumn(mode, chosen.table, matchedColumns)
	selectColumns := chooseAssistantSelectColumns(mode, chosen.table, matchedColumns)

	sql, modeAssumptions, err := buildAssistantSQL(mode, chosen.table, selectColumns, filters, metricColumn, timeColumn, limit)
	if err != nil {
		return nil, err
	}
	assumptions = append(assumptions, modeAssumptions...)

	confidence := "high"
	if len(warnings) > 0 {
		confidence = "medium"
	}
	if !chosen.exactNameMatch || len(assumptions) > 2 {
		confidence = "medium"
	}
	if len(warnings) > 1 {
		confidence = "low"
	}

	matchedTables := []string{chosen.table.Name}
	summary := buildAssistantSummary(mode, chosen.domain, chosen.table.Name, metricColumn, timeColumn, filters)

	return &assistantQueryResponse{
		Status:         "OK",
		Question:       question,
		Domain:         chosen.domain,
		Mode:           mode,
		Planner:        "deterministic",
		Summary:        summary,
		SQL:            sql,
		PrimaryTable:   chosen.table.Name,
		MatchedTables:  matchedTables,
		MatchedColumns: summarizeAssistantColumns(selectColumns, filters, metricColumn, timeColumn),
		Assumptions:    uniqueStrings(assumptions),
		Warnings:       uniqueStrings(warnings),
		Confidence:     confidence,
	}, nil
}

func orderedAssistantDomains(preferredDomains []string, snapshot *api.SchemaSnapshotResponse) []api.SchemaSnapshotDomain {
	if snapshot == nil {
		return nil
	}
	byName := make(map[string]api.SchemaSnapshotDomain, len(snapshot.Domains))
	for _, domain := range snapshot.Domains {
		byName[domain.Name] = domain
	}
	ordered := make([]api.SchemaSnapshotDomain, 0, len(snapshot.Domains))
	seen := make(map[string]struct{}, len(snapshot.Domains))
	for _, name := range preferredDomains {
		if domain, ok := byName[name]; ok {
			ordered = append(ordered, domain)
			seen[name] = struct{}{}
		}
	}
	for _, domain := range snapshot.Domains {
		if _, ok := seen[domain.Name]; ok {
			continue
		}
		ordered = append(ordered, domain)
	}
	return ordered
}

func scoreAssistantTable(question string, questionTokens []string, domain string, table api.SchemaSnapshotTable) assistantTableCandidate {
	candidate := assistantTableCandidate{domain: domain, table: table}
	tableTokens := identifierAssistantTokens(table.Name)
	tablePhrase := strings.Join(tableTokens, " ")
	if tablePhrase != "" && strings.Contains(" "+question+" ", " "+tablePhrase+" ") {
		candidate.score += 12
		candidate.exactNameMatch = true
	}
	for _, token := range questionTokens {
		if _, skip := assistantStopWords[token]; skip {
			continue
		}
		for _, tableToken := range tableTokens {
			if assistantTokensMatch(token, tableToken) {
				candidate.score += 4
				break
			}
		}
		for _, column := range table.Columns {
			for _, columnToken := range identifierAssistantTokens(column.Name) {
				if !assistantTokensMatch(token, columnToken) {
					continue
				}
				candidate.score += 2
				candidate.matchedColumns = append(candidate.matchedColumns, column.Name)
				break
			}
		}
	}
	candidate.matchedColumns = uniqueStrings(candidate.matchedColumns)
	return candidate
}

func detectAssistantMode(tokens []string) string {
	if hasAnyAssistantToken(tokens, "count", "total", "number") {
		return "count"
	}
	if hasAnyAssistantToken(tokens, "sum") {
		return "sum"
	}
	if hasAnyAssistantToken(tokens, "avg", "average") {
		return "avg"
	}
	if hasAnyAssistantToken(tokens, "max", "highest") {
		return "max"
	}
	if hasAnyAssistantToken(tokens, "min", "lowest") {
		return "min"
	}
	if hasAnyAssistantToken(tokens, "latest", "last", "recent") {
		return "latest"
	}
	if hasAnyAssistantToken(tokens, "top") {
		return "top"
	}
	return "list"
}

func detectAssistantLimit(question, mode string) int {
	if mode == "count" || mode == "sum" || mode == "avg" || mode == "max" || mode == "min" {
		return 0
	}
	if matches := assistantTopNPattern.FindStringSubmatch(question); len(matches) == 3 {
		if limit, err := strconv.Atoi(matches[2]); err == nil && limit > 0 {
			return limit
		}
	}
	if matches := assistantReverseTopNPattern.FindStringSubmatch(question); len(matches) == 3 {
		if limit, err := strconv.Atoi(matches[1]); err == nil && limit > 0 {
			return limit
		}
	}
	if mode == "latest" || mode == "top" {
		return 25
	}
	return 100
}

func matchedAssistantColumns(questionTokens []string, table api.SchemaSnapshotTable) []string {
	matched := make([]string, 0)
	for _, column := range table.Columns {
		for _, colToken := range identifierAssistantTokens(column.Name) {
			if _, generic := assistantGenericColumnTokens[colToken]; generic {
				continue
			}
			for _, questionToken := range questionTokens {
				if assistantTokensMatch(questionToken, colToken) {
					matched = append(matched, column.Name)
					goto nextColumn
				}
			}
		}
	nextColumn:
	}
	return uniqueStrings(matched)
}

type assistantFilter struct {
	column string
	value  string
	isText bool
}

func buildAssistantFilters(question string, questionTokens []string, table api.SchemaSnapshotTable, matchedColumns []string) ([]assistantFilter, []string) {
	filters := make([]assistantFilter, 0)
	assumptions := make([]string, 0)
	quoted := extractAssistantQuotedValues(question)
	if len(quoted) > 0 {
		if column := chooseAssistantSearchColumn(table, matchedColumns); column != nil {
			filters = append(filters, assistantFilter{column: column.Name, value: quoted[0], isText: !assistantColumnIsNumeric(*column)})
			assumptions = append(assumptions, fmt.Sprintf("Used quoted value %q as a filter on %s.", quoted[0], column.Name))
		}
	}
	if matches := assistantIDPattern.FindStringSubmatch(question); len(matches) == 2 {
		if column := findAssistantColumn(table, "id"); column != nil {
			filters = append(filters, assistantFilter{column: column.Name, value: matches[1], isText: !assistantColumnIsNumeric(*column)})
			assumptions = append(assumptions, fmt.Sprintf("Interpreted %s as a filter on %s.", matches[1], column.Name))
		}
	}
	if len(filters) == 0 {
		for _, keyword := range assistantStatusKeywords {
			if !hasAnyAssistantToken(questionTokens, keyword) {
				continue
			}
			column := findAssistantColumn(table, "status")
			if column == nil {
				column = findAssistantColumn(table, "state")
			}
			if column != nil {
				filters = append(filters, assistantFilter{column: column.Name, value: keyword, isText: true})
				assumptions = append(assumptions, fmt.Sprintf("Applied filter %s = %q because the question mentions that status.", column.Name, keyword))
			}
			break
		}
	}
	return uniqueAssistantFilters(filters), assumptions
}

func buildAssistantSQL(mode string, table api.SchemaSnapshotTable, selectColumns []string, filters []assistantFilter, metricColumn, timeColumn string, limit int) (string, []string, error) {
	whereClause := buildAssistantWhereClause(filters)
	assumptions := make([]string, 0)
	switch mode {
	case "count":
		return fmt.Sprintf("SELECT COUNT(*) AS total FROM %s%s;", table.Name, whereClause), assumptions, nil
	case "sum", "avg", "max", "min":
		if metricColumn == "" {
			return "", nil, fmt.Errorf("could not infer a numeric column for %s", mode)
		}
		fn := map[string]string{"sum": "SUM", "avg": "AVG", "max": "MAX", "min": "MIN"}[mode]
		aliasPrefix := map[string]string{"sum": "total", "avg": "avg", "max": "max", "min": "min"}[mode]
		return fmt.Sprintf("SELECT %s(%s) AS %s_%s FROM %s%s;", fn, metricColumn, aliasPrefix, metricColumn, table.Name, whereClause), assumptions, nil
	case "latest":
		if timeColumn == "" {
			assumptions = append(assumptions, "Did not find a clear time column; returned a limited list without temporal ordering.")
			return fmt.Sprintf("SELECT %s FROM %s%s LIMIT %d;", buildAssistantSelectClause(selectColumns), table.Name, whereClause, limit), assumptions, nil
		}
		assumptions = append(assumptions, fmt.Sprintf("Ordered by %s DESC to satisfy the latest/recent intent.", timeColumn))
		return fmt.Sprintf("SELECT %s FROM %s%s ORDER BY %s DESC LIMIT %d;", buildAssistantSelectClause(selectColumns), table.Name, whereClause, timeColumn, limit), assumptions, nil
	case "top":
		if metricColumn != "" {
			assumptions = append(assumptions, fmt.Sprintf("Ordered by %s DESC to satisfy the ranking/top intent.", metricColumn))
			return fmt.Sprintf("SELECT %s FROM %s%s ORDER BY %s DESC LIMIT %d;", buildAssistantSelectClause(selectColumns), table.Name, whereClause, metricColumn, limit), assumptions, nil
		}
		assumptions = append(assumptions, "Did not find a clear numeric metric; returned a limited list.")
		return fmt.Sprintf("SELECT %s FROM %s%s LIMIT %d;", buildAssistantSelectClause(selectColumns), table.Name, whereClause, limit), assumptions, nil
	default:
		return fmt.Sprintf("SELECT %s FROM %s%s LIMIT %d;", buildAssistantSelectClause(selectColumns), table.Name, whereClause, limit), assumptions, nil
	}
}

func chooseAssistantMetricColumn(mode string, table api.SchemaSnapshotTable, matchedColumns []string) string {
	if mode == "count" || mode == "list" || mode == "latest" {
		return ""
	}
	for _, name := range matchedColumns {
		column := findAssistantColumn(table, name)
		if column != nil && assistantColumnIsNumeric(*column) {
			return column.Name
		}
	}
	for _, preferred := range []string{"amount", "total", "price", "cost", "qty", "quantity", "balance", "value", "score"} {
		column := findAssistantColumn(table, preferred)
		if column != nil && assistantColumnIsNumeric(*column) {
			return column.Name
		}
	}
	for _, column := range table.Columns {
		if column.PrimaryKey || !assistantColumnIsNumeric(column) {
			continue
		}
		return column.Name
	}
	return ""
}

func chooseAssistantTimeColumn(table api.SchemaSnapshotTable) string {
	for _, preferred := range []string{"created_at", "updated_at", "timestamp", "committed_at", "event_time", "occurred_at", "logical_timestamp", "ts", "date"} {
		column := findAssistantColumn(table, preferred)
		if column != nil {
			return column.Name
		}
	}
	for _, column := range table.Columns {
		for _, token := range identifierAssistantTokens(column.Name) {
			if token == "created" || token == "updated" || token == "time" || token == "date" || token == "timestamp" || token == "ts" {
				return column.Name
			}
		}
	}
	return ""
}

func chooseAssistantSelectColumns(mode string, table api.SchemaSnapshotTable, matchedColumns []string) []string {
	if mode == "count" || mode == "sum" || mode == "avg" || mode == "max" || mode == "min" {
		return nil
	}
	if len(matchedColumns) == 0 {
		return nil
	}
	selected := make([]string, 0, len(matchedColumns))
	for _, column := range table.Columns {
		for _, matched := range matchedColumns {
			if column.Name == matched {
				selected = append(selected, column.Name)
				break
			}
		}
		if len(selected) >= 4 {
			break
		}
	}
	return uniqueStrings(selected)
}

func chooseAssistantSearchColumn(table api.SchemaSnapshotTable, matchedColumns []string) *api.SchemaSnapshotColumn {
	for _, name := range matchedColumns {
		column := findAssistantColumn(table, name)
		if column == nil {
			continue
		}
		if !assistantColumnIsNumeric(*column) {
			return column
		}
	}
	for _, preferred := range []string{"email", "name", "title", "code", "status", "state", "slug"} {
		if column := findAssistantColumn(table, preferred); column != nil {
			return column
		}
	}
	for _, column := range table.Columns {
		if !assistantColumnIsNumeric(column) {
			return &column
		}
	}
	return nil
}

func buildAssistantWhereClause(filters []assistantFilter) string {
	if len(filters) == 0 {
		return ""
	}
	parts := make([]string, 0, len(filters))
	for _, filter := range filters {
		if filter.isText {
			parts = append(parts, fmt.Sprintf("%s = '%s'", filter.column, escapeAssistantString(filter.value)))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s = %s", filter.column, filter.value))
	}
	return " WHERE " + strings.Join(parts, " AND ")
}

func buildAssistantSelectClause(columns []string) string {
	if len(columns) == 0 {
		return "*"
	}
	return strings.Join(columns, ", ")
}

func buildAssistantSummary(mode, domain, table, metricColumn, timeColumn string, filters []assistantFilter) string {
	target := fmt.Sprintf("%s.%s", domain, table)
	switch mode {
	case "count":
		if len(filters) > 0 {
			return fmt.Sprintf("Read-only count over %s with filters inferred from the question.", target)
		}
		return fmt.Sprintf("Read-only count over %s.", target)
	case "sum":
		return fmt.Sprintf("Read-only SUM aggregate over %s using %s as the primary metric.", target, metricColumn)
	case "avg":
		return fmt.Sprintf("Read-only AVG aggregate over %s using %s as the primary metric.", target, metricColumn)
	case "max":
		return fmt.Sprintf("Read-only MAX aggregate over %s using %s as the primary metric.", target, metricColumn)
	case "min":
		return fmt.Sprintf("Read-only MIN aggregate over %s using %s as the primary metric.", target, metricColumn)
	case "latest":
		if timeColumn != "" {
			return fmt.Sprintf("Read-only list of recent rows from %s ordered by %s DESC.", target, timeColumn)
		}
		return fmt.Sprintf("Read-only list of %s with the default limit because no clear time column was found.", target)
	case "top":
		if metricColumn != "" {
			return fmt.Sprintf("Read-only ranking over %s ordered by %s DESC.", target, metricColumn)
		}
		return fmt.Sprintf("Limited read-only list over %s.", target)
	default:
		if len(filters) > 0 {
			return fmt.Sprintf("Read-only list over %s with filters inferred from the question.", target)
		}
		return fmt.Sprintf("Read-only list over %s.", target)
	}
}

func summarizeAssistantColumns(selectColumns []string, filters []assistantFilter, metricColumn, timeColumn string) []string {
	columns := append([]string(nil), selectColumns...)
	for _, filter := range filters {
		columns = append(columns, filter.column)
	}
	if metricColumn != "" {
		columns = append(columns, metricColumn)
	}
	if timeColumn != "" {
		columns = append(columns, timeColumn)
	}
	return uniqueStrings(columns)
}

func normalizeAssistantText(value string) string {
	replacer := strings.NewReplacer(
		"á", "a", "é", "e", "í", "i", "ó", "o", "ú", "u", "ü", "u", "ñ", "n",
		"Á", "a", "É", "e", "Í", "i", "Ó", "o", "Ú", "u", "Ü", "u", "Ñ", "n",
	)
	normalized := strings.ToLower(strings.TrimSpace(replacer.Replace(value)))
	var builder strings.Builder
	lastSpace := false
	for _, r := range normalized {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			builder.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace {
			builder.WriteByte(' ')
			lastSpace = true
		}
	}
	return strings.TrimSpace(builder.String())
}

func identifierAssistantTokens(value string) []string {
	return assistantTokenPattern.FindAllString(normalizeAssistantText(value), -1)
}

func assistantTokensMatch(a, b string) bool {
	return singularizeAssistantToken(a) == singularizeAssistantToken(b)
}

func hasAnyAssistantToken(tokens []string, wanted ...string) bool {
	for _, token := range tokens {
		for _, candidate := range wanted {
			if assistantTokensMatch(token, candidate) {
				return true
			}
		}
	}
	return false
}

func extractAssistantQuotedValues(question string) []string {
	matches := assistantQuotedValuePattern.FindAllStringSubmatch(question, -1)
	values := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) == 2 && strings.TrimSpace(match[1]) != "" {
			values = append(values, strings.TrimSpace(match[1]))
		}
	}
	return values
}

func findAssistantColumn(table api.SchemaSnapshotTable, name string) *api.SchemaSnapshotColumn {
	normalizedName := normalizeAssistantText(name)
	for _, column := range table.Columns {
		if normalizeAssistantText(column.Name) == normalizedName {
			return &column
		}
	}
	return nil
}

func assistantColumnIsNumeric(column api.SchemaSnapshotColumn) bool {
	typ := normalizeAssistantText(column.Type)
	for _, candidate := range []string{"int", "integer", "bigint", "smallint", "serial", "decimal", "numeric", "float", "double", "real"} {
		if strings.Contains(typ, candidate) {
			return true
		}
	}
	return false
}

func uniqueAssistantFilters(filters []assistantFilter) []assistantFilter {
	seen := make(map[string]struct{}, len(filters))
	unique := make([]assistantFilter, 0, len(filters))
	for _, filter := range filters {
		key := filter.column + "\x00" + filter.value
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, filter)
	}
	return unique
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		unique = append(unique, trimmed)
	}
	return unique
}

func singularizeAssistantToken(value string) string {
	trimmed := strings.TrimSpace(value)
	if strings.HasSuffix(trimmed, "ies") && len(trimmed) > 3 {
		return strings.TrimSuffix(trimmed, "ies") + "y"
	}
	if strings.HasSuffix(trimmed, "es") && len(trimmed) > 2 {
		return strings.TrimSuffix(trimmed, "es")
	}
	if strings.HasSuffix(trimmed, "s") && len(trimmed) > 1 {
		return strings.TrimSuffix(trimmed, "s")
	}
	return trimmed
}

func escapeAssistantString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
