package fixtures

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"asql/internal/engine/executor"
	"asql/internal/storage/wal"

	"github.com/jackc/pgx/v5"
)

const CurrentVersion = "v1"

type File struct {
	Version     string `json:"version"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Steps       []Step `json:"steps"`
}

type Step struct {
	Name       string   `json:"name"`
	Mode       string   `json:"mode"`
	Domains    []string `json:"domains"`
	Statements []string `json:"statements"`
}

type ExportOptions struct {
	Domains     []string
	Name        string
	Description string
}

type SQLExecutor interface {
	Exec(ctx context.Context, sql string) error
}

type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	if e == nil || len(e.Problems) == 0 {
		return "fixture validation failed"
	}
	return strings.Join(e.Problems, "; ")
}

func (e *ValidationError) addf(format string, args ...any) {
	e.Problems = append(e.Problems, fmt.Sprintf(format, args...))
}

func (e *ValidationError) orNil() error {
	if e == nil || len(e.Problems) == 0 {
		return nil
	}
	return e
}

func LoadFile(path string) (*File, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read fixture: %w", err)
	}

	dec := json.NewDecoder(bytes.NewReader(content))
	dec.DisallowUnknownFields()

	var fixture File
	if err := dec.Decode(&fixture); err != nil {
		return nil, fmt.Errorf("decode fixture: %w", err)
	}

	if err := ValidateSpec(&fixture); err != nil {
		return nil, err
	}

	return &fixture, nil
}

func SaveFile(path string, fixture *File) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("fixture path is required")
	}
	if err := ValidateSpec(fixture); err != nil {
		return err
	}
	content, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		return fmt.Errorf("encode fixture: %w", err)
	}
	content = append(content, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create fixture directory: %w", err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return fmt.Errorf("write fixture: %w", err)
	}
	return nil
}

func ExportFromPGWire(ctx context.Context, conn *pgx.Conn, options ExportOptions) (*File, error) {
	if conn == nil {
		return nil, fmt.Errorf("pgwire connection is required")
	}
	domains := normalizeDomains(options.Domains)
	if len(domains) == 0 {
		return nil, fmt.Errorf("fixture export requires at least one domain")
	}

	snapshot, err := readSchemaSnapshot(ctx, conn, domains)
	if err != nil {
		return nil, err
	}
	tables, entities, err := buildExportCatalog(snapshot, domains)
	if err != nil {
		return nil, err
	}
	orderedTables, err := topologicalTableOrder(tables)
	if err != nil {
		return nil, err
	}

	steps := make([]Step, 0, len(orderedTables)*2+len(entities))
	for _, table := range orderedTables {
		statements := buildCreateTableStatements(table)
		steps = append(steps, Step{
			Name:       "schema " + table.FullName(),
			Mode:       stepMode(table.RequiredDomains()),
			Domains:    table.RequiredDomains(),
			Statements: statements,
		})
	}
	for _, entity := range entities {
		steps = append(steps, Step{
			Name:       "entity " + entity.Domain + "." + entity.Name,
			Mode:       "domain",
			Domains:    []string{entity.Domain},
			Statements: []string{entity.CreateStatement()},
		})
	}

	for _, table := range orderedTables {
		inserts, err := exportTableRows(ctx, conn, table)
		if err != nil {
			return nil, err
		}
		if len(inserts) == 0 {
			continue
		}
		steps = append(steps, Step{
			Name:       "data " + table.FullName(),
			Mode:       stepMode(table.RequiredDomains()),
			Domains:    table.RequiredDomains(),
			Statements: inserts,
		})
	}

	name := strings.TrimSpace(options.Name)
	if name == "" {
		name = strings.Join(domains, "-") + "-export"
	}
	fixture := &File{
		Version:     CurrentVersion,
		Name:        name,
		Description: strings.TrimSpace(options.Description),
		Steps:       steps,
	}
	if err := ValidateSpec(fixture); err != nil {
		return nil, err
	}
	return fixture, nil
}

func ValidateSpec(fixture *File) error {
	problems := &ValidationError{}
	if fixture == nil {
		problems.addf("fixture is required")
		return problems
	}

	if strings.TrimSpace(fixture.Version) != CurrentVersion {
		problems.addf("fixture version must be %q", CurrentVersion)
	}
	if strings.TrimSpace(fixture.Name) == "" {
		problems.addf("fixture name is required")
	}
	if len(fixture.Steps) == 0 {
		problems.addf("fixture must define at least one step")
		return problems.orNil()
	}

	for idx := range fixture.Steps {
		step := &fixture.Steps[idx]
		stepLabel := fmt.Sprintf("step %d", idx+1)
		if name := strings.TrimSpace(step.Name); name != "" {
			stepLabel = fmt.Sprintf("step %d (%s)", idx+1, name)
		}

		mode := strings.ToLower(strings.TrimSpace(step.Mode))
		switch mode {
		case "domain":
			if len(step.Domains) != 1 {
				problems.addf("%s must declare exactly one domain when mode=domain", stepLabel)
			}
		case "cross":
			if len(step.Domains) < 2 {
				problems.addf("%s must declare at least two domains when mode=cross", stepLabel)
			}
		default:
			problems.addf("%s has unsupported mode %q", stepLabel, step.Mode)
		}

		seenDomains := make(map[string]struct{}, len(step.Domains))
		for _, domain := range step.Domains {
			normalized := strings.TrimSpace(domain)
			if normalized == "" {
				problems.addf("%s contains an empty domain name", stepLabel)
				continue
			}
			key := strings.ToLower(normalized)
			if _, exists := seenDomains[key]; exists {
				problems.addf("%s repeats domain %q", stepLabel, normalized)
				continue
			}
			seenDomains[key] = struct{}{}
		}

		if len(step.Statements) == 0 {
			problems.addf("%s must contain at least one statement", stepLabel)
			continue
		}

		for statementIdx, statement := range step.Statements {
			trimmed := normalizeStatement(statement)
			if trimmed == "" {
				problems.addf("%s statement %d is empty", stepLabel, statementIdx+1)
				continue
			}
			if err := validateStatement(trimmed); err != nil {
				problems.addf("%s statement %d: %v", stepLabel, statementIdx+1, err)
			}
			step.Statements[statementIdx] = trimmed
		}

		step.Mode = mode
	}

	return problems.orNil()
}

func ValidateDryRun(ctx context.Context, fixture *File) error {
	if err := ValidateSpec(fixture); err != nil {
		return err
	}

	walPath := filepath.Join(os.TempDir(), fmt.Sprintf("asql-fixture-%s.wal", sanitizeForPath(fixture.Name)))
	walPath = uniqueTempPath(walPath)
	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		return fmt.Errorf("create validation wal: %w", err)
	}
	defer func() {
		_ = store.Close()
		_ = os.RemoveAll(filepath.Dir(walPath))
	}()

	engine, err := executor.New(ctx, store, "")
	if err != nil {
		return fmt.Errorf("create validation engine: %w", err)
	}

	return Apply(ctx, fixture, newEngineExecutor(engine))
}

func Apply(ctx context.Context, fixture *File, exec SQLExecutor) error {
	if exec == nil {
		return fmt.Errorf("fixture executor is required")
	}
	if err := ValidateSpec(fixture); err != nil {
		return err
	}

	for stepIdx, step := range fixture.Steps {
		begin := beginStatement(step)
		if err := exec.Exec(ctx, begin); err != nil {
			return fmt.Errorf("fixture step %d (%s) begin failed: %w", stepIdx+1, step.Name, err)
		}

		for statementIdx, statement := range step.Statements {
			if err := exec.Exec(ctx, statement); err != nil {
				_ = exec.Exec(ctx, "ROLLBACK")
				return fmt.Errorf("fixture step %d (%s) statement %d failed: %w", stepIdx+1, step.Name, statementIdx+1, err)
			}
		}

		if err := exec.Exec(ctx, "COMMIT"); err != nil {
			_ = exec.Exec(ctx, "ROLLBACK")
			return fmt.Errorf("fixture step %d (%s) commit failed: %w", stepIdx+1, step.Name, err)
		}
	}

	return nil
}

func validateStatement(statement string) error {
	upper := strings.ToUpper(statement)
	switch {
	case upper == "COMMIT" || upper == "ROLLBACK":
		return fmt.Errorf("transaction control is managed by the fixture loader")
	case strings.HasPrefix(upper, "BEGIN ") || strings.HasPrefix(upper, "START TRANSACTION"):
		return fmt.Errorf("step transactions must be declared with mode/domains, not explicit BEGIN/START TRANSACTION statements")
	}

	nondeterministicTokens := []string{
		"NOW()",
		"CURRENT_TIMESTAMP",
		"RANDOM()",
		"UUID()",
		"UUID_V7()",
	}
	for _, token := range nondeterministicTokens {
		if strings.Contains(upper, token) {
			return fmt.Errorf("statement uses non-deterministic token %q; fixtures must use explicit IDs and timestamps", token)
		}
	}

	return nil
}

func normalizeStatement(statement string) string {
	trimmed := strings.TrimSpace(statement)
	trimmed = strings.TrimSuffix(trimmed, ";")
	return strings.TrimSpace(trimmed)
}

func beginStatement(step Step) string {
	domains := make([]string, 0, len(step.Domains))
	for _, domain := range step.Domains {
		domains = append(domains, strings.TrimSpace(domain))
	}
	if step.Mode == "cross" {
		return "BEGIN CROSS DOMAIN " + strings.Join(domains, ", ")
	}
	return "BEGIN DOMAIN " + domains[0]
}

type engineExecutor struct {
	engine  *executor.Engine
	session *executor.Session
}

func newEngineExecutor(engine *executor.Engine) *engineExecutor {
	return &engineExecutor{engine: engine, session: engine.NewSession()}
}

func (e *engineExecutor) Exec(ctx context.Context, sql string) error {
	_, err := e.engine.Execute(ctx, e.session, sql)
	return err
}

func sanitizeForPath(value string) string {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	trimmed = strings.ReplaceAll(trimmed, " ", "-")
	trimmed = strings.ReplaceAll(trimmed, "/", "-")
	trimmed = strings.ReplaceAll(trimmed, "\\", "-")
	if trimmed == "" {
		return "fixture"
	}
	return trimmed
}

func uniqueTempPath(base string) string {
	dir, err := os.MkdirTemp("", "asql-fixture-")
	if err != nil {
		return base
	}
	return filepath.Join(dir, filepath.Base(base))
}

type schemaSnapshotResponse struct {
	Status  string                 `json:"status"`
	Domains []schemaSnapshotDomain `json:"domains"`
}

type schemaSnapshotDomain struct {
	Name     string                 `json:"name"`
	Tables   []schemaSnapshotTable  `json:"tables"`
	Entities []schemaSnapshotEntity `json:"entities,omitempty"`
}

type schemaSnapshotTable struct {
	Name                 string                 `json:"name"`
	Columns              []schemaSnapshotColumn `json:"columns"`
	Indexes              []schemaSnapshotIndex  `json:"indexes,omitempty"`
	VersionedForeignKeys []schemaSnapshotVFK    `json:"versioned_foreign_keys,omitempty"`
}

type schemaSnapshotColumn struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	PrimaryKey       bool   `json:"primary_key,omitempty"`
	Unique           bool   `json:"unique,omitempty"`
	ReferencesTable  string `json:"references_table,omitempty"`
	ReferencesColumn string `json:"references_column,omitempty"`
	DefaultValue     string `json:"default_value,omitempty"`
}

type schemaSnapshotIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Method  string   `json:"method"`
}

type schemaSnapshotVFK struct {
	Column           string `json:"column"`
	LSNColumn        string `json:"lsn_column"`
	ReferencesDomain string `json:"references_domain"`
	ReferencesTable  string `json:"references_table"`
	ReferencesColumn string `json:"references_column"`
}

type schemaSnapshotEntity struct {
	Name      string   `json:"name"`
	RootTable string   `json:"root_table"`
	Tables    []string `json:"tables"`
}

type exportTable struct {
	Domain    string
	Snapshot  schemaSnapshotTable
	DependsOn []string
	PKColumn  string
}

func (t exportTable) FullName() string {
	return t.Domain + "." + t.Snapshot.Name
}

func (t exportTable) RequiredDomains() []string {
	required := map[string]struct{}{t.Domain: {}}
	for _, dep := range t.DependsOn {
		domain, _, ok := strings.Cut(dep, ".")
		if ok && domain != "" {
			required[domain] = struct{}{}
		}
	}
	for _, vfk := range t.Snapshot.VersionedForeignKeys {
		if strings.TrimSpace(vfk.ReferencesDomain) != "" {
			required[strings.ToLower(strings.TrimSpace(vfk.ReferencesDomain))] = struct{}{}
		}
	}
	result := make([]string, 0, len(required))
	for domain := range required {
		result = append(result, domain)
	}
	sort.Strings(result)
	return result
}

type exportEntity struct {
	Domain string
	Name   string
	Root   string
	Tables []string
}

func (e exportEntity) CreateStatement() string {
	parts := []string{"CREATE ENTITY " + e.Name + " (ROOT " + e.Root}
	includes := make([]string, 0, len(e.Tables))
	for _, table := range e.Tables {
		if table == e.Root {
			continue
		}
		includes = append(includes, table)
	}
	if len(includes) > 0 {
		parts[0] += ", INCLUDES " + strings.Join(includes, ", ")
	}
	parts[0] += ")"
	return parts[0]
}

func normalizeDomains(domains []string) []string {
	seen := make(map[string]struct{}, len(domains))
	result := make([]string, 0, len(domains))
	for _, domain := range domains {
		normalized := strings.ToLower(strings.TrimSpace(domain))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	sort.Strings(result)
	return result
}

func readSchemaSnapshot(ctx context.Context, conn *pgx.Conn, domains []string) (*schemaSnapshotResponse, error) {
	query := "SELECT snapshot FROM asql_admin.schema_snapshot"
	if len(domains) > 0 {
		parts := make([]string, 0, len(domains))
		for _, domain := range domains {
			parts = append(parts, quoteSQLString(domain))
		}
		query += " WHERE domain IN (" + strings.Join(parts, ", ") + ")"
	}
	var raw string
	if err := conn.QueryRow(ctx, query).Scan(&raw); err != nil {
		return nil, fmt.Errorf("read schema snapshot: %w", err)
	}
	var snapshot schemaSnapshotResponse
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return nil, fmt.Errorf("decode schema snapshot: %w", err)
	}
	return &snapshot, nil
}

func buildExportCatalog(snapshot *schemaSnapshotResponse, domains []string) (map[string]exportTable, []exportEntity, error) {
	allowed := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		allowed[domain] = struct{}{}
	}
	tables := make(map[string]exportTable)
	entities := make([]exportEntity, 0)
	for _, domain := range snapshot.Domains {
		domainName := strings.ToLower(strings.TrimSpace(domain.Name))
		if _, ok := allowed[domainName]; !ok {
			continue
		}
		for _, table := range domain.Tables {
			pk := ""
			deps := make([]string, 0)
			for _, column := range table.Columns {
				if column.PrimaryKey {
					pk = column.Name
				}
				dep, hasDep := resolveReferenceTable(domainName, column.ReferencesTable)
				if hasDep && dep != domainName+"."+table.Name {
					deps = append(deps, dep)
				}
			}
			for _, vfk := range table.VersionedForeignKeys {
				depDomain := domainName
				if strings.TrimSpace(vfk.ReferencesDomain) != "" {
					depDomain = strings.ToLower(strings.TrimSpace(vfk.ReferencesDomain))
				}
				dep := depDomain + "." + strings.ToLower(strings.TrimSpace(vfk.ReferencesTable))
				if dep != domainName+"."+table.Name {
					deps = append(deps, dep)
				}
			}
			key := domainName + "." + strings.ToLower(strings.TrimSpace(table.Name))
			tables[key] = exportTable{
				Domain:    domainName,
				Snapshot:  table,
				DependsOn: uniqueSorted(deps),
				PKColumn:  pk,
			}
		}
		for _, entity := range domain.Entities {
			tablesCopy := append([]string(nil), entity.Tables...)
			sort.Strings(tablesCopy)
			entities = append(entities, exportEntity{
				Domain: domainName,
				Name:   entity.Name,
				Root:   entity.RootTable,
				Tables: tablesCopy,
			})
		}
	}
	sort.Slice(entities, func(i, j int) bool {
		if entities[i].Domain != entities[j].Domain {
			return entities[i].Domain < entities[j].Domain
		}
		return entities[i].Name < entities[j].Name
	})
	for name, table := range tables {
		for _, dep := range table.DependsOn {
			if _, ok := tables[dep]; !ok {
				return nil, nil, fmt.Errorf("cannot export %s: referenced table %s is outside the selected domains", name, dep)
			}
		}
		if table.PKColumn == "" {
			return nil, nil, fmt.Errorf("cannot export %s: table has no primary key for deterministic ordering", name)
		}
	}
	return tables, entities, nil
}

func topologicalTableOrder(tables map[string]exportTable) ([]exportTable, error) {
	inDegree := make(map[string]int, len(tables))
	adj := make(map[string][]string, len(tables))
	for name := range tables {
		inDegree[name] = 0
	}
	for name, table := range tables {
		for _, dep := range table.DependsOn {
			adj[dep] = append(adj[dep], name)
			inDegree[name]++
		}
	}
	ready := make([]string, 0)
	for name, degree := range inDegree {
		if degree == 0 {
			ready = append(ready, name)
		}
	}
	sort.Strings(ready)
	ordered := make([]exportTable, 0, len(tables))
	for len(ready) > 0 {
		name := ready[0]
		ready = ready[1:]
		ordered = append(ordered, tables[name])
		children := append([]string(nil), adj[name]...)
		sort.Strings(children)
		for _, child := range children {
			inDegree[child]--
			if inDegree[child] == 0 {
				ready = append(ready, child)
				sort.Strings(ready)
			}
		}
	}
	if len(ordered) != len(tables) {
		remaining := make([]string, 0)
		for name, degree := range inDegree {
			if degree > 0 {
				remaining = append(remaining, name)
			}
		}
		sort.Strings(remaining)
		return nil, fmt.Errorf("cannot export fixture deterministically: dependency cycle detected among %s", strings.Join(remaining, ", "))
	}
	return ordered, nil
}

func buildCreateTableStatements(table exportTable) []string {
	columnDefs := make([]string, 0, len(table.Snapshot.Columns)+len(table.Snapshot.VersionedForeignKeys))
	for _, column := range table.Snapshot.Columns {
		parts := []string{column.Name, normalizeColumnType(column.Type)}
		if column.PrimaryKey {
			parts = append(parts, "PRIMARY KEY")
		} else if column.Unique {
			parts = append(parts, "UNIQUE")
		}
		if strings.TrimSpace(column.DefaultValue) != "" {
			parts = append(parts, "DEFAULT "+column.DefaultValue)
		}
		if ref, ok := resolveReferenceTable(table.Domain, column.ReferencesTable); ok && strings.TrimSpace(column.ReferencesColumn) != "" {
			parts = append(parts, "REFERENCES "+ref+"("+column.ReferencesColumn+")")
		}
		columnDefs = append(columnDefs, strings.Join(parts, " "))
	}
	for _, vfk := range table.Snapshot.VersionedForeignKeys {
		refDomain := table.Domain
		if strings.TrimSpace(vfk.ReferencesDomain) != "" {
			refDomain = strings.ToLower(strings.TrimSpace(vfk.ReferencesDomain))
		}
		columnDefs = append(columnDefs,
			fmt.Sprintf("VERSIONED FOREIGN KEY (%s) REFERENCES %s.%s(%s) AS OF %s",
				vfk.Column,
				refDomain,
				vfk.ReferencesTable,
				vfk.ReferencesColumn,
				vfk.LSNColumn,
			),
		)
	}
	statements := []string{fmt.Sprintf("CREATE TABLE %s (%s)", table.FullName(), strings.Join(columnDefs, ", "))}
	indexes := append([]schemaSnapshotIndex(nil), table.Snapshot.Indexes...)
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Name < indexes[j].Name })
	for _, index := range indexes {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(index.Name)), "__auto_") {
			continue
		}
		statements = append(statements,
			fmt.Sprintf("CREATE INDEX %s ON %s (%s) USING %s", index.Name, table.FullName(), strings.Join(index.Columns, ", "), normalizeIndexMethod(index.Method)),
		)
	}
	return statements
}

func exportTableRows(ctx context.Context, conn *pgx.Conn, table exportTable) ([]string, error) {
	columns := make([]schemaSnapshotColumn, len(table.Snapshot.Columns))
	copy(columns, table.Snapshot.Columns)
	columnNames := make([]string, 0, len(columns))
	columnTypes := make(map[string]string, len(columns))
	for _, column := range columns {
		columnNames = append(columnNames, column.Name)
		columnTypes[column.Name] = column.Type
	}
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", strings.Join(columnNames, ", "), table.FullName(), table.PKColumn)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("export rows from %s: %w", table.FullName(), err)
	}
	defer rows.Close()

	statements := make([]string, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read row values from %s: %w", table.FullName(), err)
		}
		literals := make([]string, 0, len(values))
		for idx, value := range values {
			literals = append(literals, sqlLiteralForValue(value, columnTypes[columnNames[idx]]))
		}
		statements = append(statements,
			fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table.FullName(), strings.Join(columnNames, ", "), strings.Join(literals, ", ")),
		)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate exported rows from %s: %w", table.FullName(), err)
	}
	return statements, nil
}

func resolveReferenceTable(currentDomain, raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}
	if strings.Contains(trimmed, ".") {
		parts := strings.SplitN(trimmed, ".", 2)
		return strings.ToLower(strings.TrimSpace(parts[0])) + "." + strings.ToLower(strings.TrimSpace(parts[1])), true
	}
	return currentDomain + "." + strings.ToLower(trimmed), true
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	sort.Strings(result)
	return result
}

func stepMode(domains []string) string {
	if len(domains) > 1 {
		return "cross"
	}
	return "domain"
}

func normalizeColumnType(raw string) string {
	typeName := strings.ToUpper(strings.TrimSpace(raw))
	if typeName == "" {
		return "TEXT"
	}
	return typeName
}

func normalizeIndexMethod(raw string) string {
	method := strings.ToLower(strings.TrimSpace(raw))
	if method == "" {
		return "btree"
	}
	return method
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func quoteQualifiedIdentifier(domain, table string) string {
	return quoteIdentifier(domain) + "." + quoteIdentifier(table)
}

func joinIdentifiers(columns []string) string {
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		quoted = append(quoted, quoteIdentifier(column))
	}
	return strings.Join(quoted, ", ")
}

func quoteSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func sqlLiteralForValue(value any, columnType string) string {
	if value == nil {
		return "NULL"
	}
	switch typed := value.(type) {
	case string:
		return quoteSQLString(typed)
	case []byte:
		return quoteSQLString(string(typed))
	case int:
		return strconv.Itoa(typed)
	case int8:
		return strconv.FormatInt(int64(typed), 10)
	case int16:
		return strconv.FormatInt(int64(typed), 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case uint8:
		return strconv.FormatUint(uint64(typed), 10)
	case uint16:
		return strconv.FormatUint(uint64(typed), 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case float32:
		return strconv.FormatFloat(float64(typed), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64)
	case bool:
		if typed {
			return "TRUE"
		}
		return "FALSE"
	case time.Time:
		if strings.EqualFold(strings.TrimSpace(columnType), "TIMESTAMP") {
			return quoteSQLString(typed.UTC().Format(time.RFC3339Nano))
		}
		return quoteSQLString(typed.UTC().Format(time.RFC3339Nano))
	default:
		return quoteSQLString(fmt.Sprint(value))
	}
}
