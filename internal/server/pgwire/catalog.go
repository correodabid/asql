package pgwire

// catalog.go — Synthetic pg_catalog / information_schema responses.
//
// Tools like psql, DBeaver, and various BI connectors query system catalog
// tables before doing any user-facing work.  ASQL does not implement a full
// pg_catalog but provides enough of the surface to make common tooling work.
//
// Strategy: intercept specific well-known query patterns before handing them
// to the SQL engine, and return synthetic results built from the engine's own
// SchemaSnapshot.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"asql/internal/engine/executor"
	"asql/internal/engine/parser/ast"
)

// interceptResult bundles the synthetic result set and its ordered column list.
type interceptResult struct {
	result  executor.Result
	columns []string
}

// ── Regex matchers for function-call interception ─────────────────────────────

// reCurrentSetting matches  SELECT current_setting('param')  with optional
// alias and surrounding whitespace.
var reCurrentSetting = regexp.MustCompile(`(?i)current_setting\s*\(\s*'([^']+)'\s*\)`)

// reSetConfig matches  SELECT set_config('param', 'value', false/true)
var reSetConfig = regexp.MustCompile(`(?i)set_config\s*\(\s*'([^']+)'\s*,\s*'([^']*)'\s*,\s*(?:false|true)\s*\)`)

var reRowLSN = regexp.MustCompile(`(?is)^select\s+row_lsn\s*\(\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')*)'\s*\)\s*(?:as\s+row_lsn)?\s*$`)
var reEntityVersion = regexp.MustCompile(`(?is)^select\s+entity_version\s*\(\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*\)\s*(?:as\s+entity_version)?\s*$`)
var reEntityHeadLSN = regexp.MustCompile(`(?is)^select\s+entity_head_lsn\s*\(\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*\)\s*(?:as\s+entity_head_lsn)?\s*$`)
var reEntityVersionLSN = regexp.MustCompile(`(?is)^select\s+entity_version_lsn\s*\(\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')*)'\s*,\s*([0-9]+)\s*\)\s*(?:as\s+entity_version_lsn)?\s*$`)
var reResolveReference = regexp.MustCompile(`(?is)^select\s+resolve_reference\s*\(\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')*)'\s*\)\s*(?:as\s+resolve_reference)?\s*$`)

// currentSettingDefaults maps GUC parameter names to the default values
// ASQL should report.  The set is deliberately small — only the parameters
// that commonly appear in startup introspection from psql, JDBC, DBeaver,
// DataGrip, pgx, and similar clients.
var currentSettingDefaults = map[string]string{
	"server_version":                "16.0",
	"server_version_num":            "160000",
	"server_encoding":               "UTF8",
	"client_encoding":               "UTF8",
	"datestyle":                     "ISO, MDY",
	"intervalstyle":                 "postgres",
	"timezone":                      "UTC",
	"integer_datetimes":             "on",
	"standard_conforming_strings":   "on",
	"search_path":                   "\"$user\", public",
	"max_identifier_length":         "63",
	"transaction_isolation":         "read committed",
	"is_superuser":                  "on",
	"session_authorization":         "asql",
	"lc_collate":                    "en_US.UTF-8",
	"lc_ctype":                      "en_US.UTF-8",
	"lc_messages":                   "en_US.UTF-8",
	"lc_monetary":                   "en_US.UTF-8",
	"lc_numeric":                    "en_US.UTF-8",
	"lc_time":                       "en_US.UTF-8",
	"default_transaction_isolation": "read committed",
	"bytea_output":                  "hex",
}

// interceptCatalog returns a synthetic result when sql is a recognised catalog
// query.  The second return value is true when the query was intercepted.
func (server *Server) interceptCatalog(ctx context.Context, sql string) (interceptResult, bool) {
	trimmed := strings.TrimSpace(sql)
	trimmed = strings.TrimRight(trimmed, "; \t\n\r")
	lower := strings.ToLower(trimmed)

	switch {
	// ── Scalar utility queries ────────────────────────────────────────────

	case reRowLSN.MatchString(trimmed):
		return server.handleRowLSN(trimmed)

	case reEntityVersion.MatchString(trimmed):
		return server.handleEntityVersion(trimmed)

	case reEntityHeadLSN.MatchString(trimmed):
		return server.handleEntityHeadLSN(trimmed)

	case reEntityVersionLSN.MatchString(trimmed):
		return server.handleEntityVersionLSN(trimmed)

	case reResolveReference.MatchString(trimmed):
		return server.handleResolveReference(trimmed)

	case lower == "select current_lsn()",
		lower == "select current_lsn() as current_lsn":
		return literalScalarResult("current_lsn", litU(server.engine.CurrentLSN()))

	case lower == "select current_database()",
		lower == "select current_database() as current_database":
		return scalarResult("current_database", "asql")

	case lower == "select version()",
		lower == "select version() as version":
		return scalarResult("version", "PostgreSQL 16.0 (asql)")

	case lower == "select current_schema()",
		lower == "select current_schema() as current_schema":
		return scalarResult("current_schema", "public")

	case lower == "select current_user",
		lower == "select current_user as current_user",
		lower == "select user":
		return scalarResult("current_user", "asql")

	case strings.HasPrefix(lower, "show server_version"):
		return scalarResult("server_version", "16.0-asql")

	case strings.HasPrefix(lower, "show server_version_num"):
		return scalarResult("server_version_num", "160000")

	case strings.HasPrefix(lower, "show search_path"):
		return scalarResult("search_path", "\"$user\", public")

	case strings.HasPrefix(lower, "show "):
		// Unknown SHOW — return empty string rather than an engine error.
		// Skip asql_* params: they are handled by resolveShowParam which
		// returns live runtime state (node role, cluster leader, etc.).
		param := strings.TrimSpace(lower[5:])
		if strings.HasPrefix(param, "asql_") {
			return interceptResult{}, false
		}
		return scalarResult(param, "")

	// ── current_setting('param') function calls ──────────────────────────
	// psql, JDBC, DBeaver, and many pgx-based drivers call current_setting()
	// during connection setup and introspection to discover server capabilities.
	case reCurrentSetting.MatchString(lower):
		return server.handleCurrentSetting(lower)

	// ── pg_catalog.set_config(name, value, is_local) ────────────────────
	// DBeaver and DataGrip call set_config on connect.  Treat as a no-op
	// that echoes the value back in the result.
	case reSetConfig.MatchString(lower):
		return handleSetConfig(trimmed)

	// ── pg_is_in_recovery() ──────────────────────────────────────────────
	// Replication-aware clients and connection pools call this to determine
	// whether they are connected to a primary or a standby.
	case strings.Contains(lower, "pg_is_in_recovery()"):
		inRecovery := "f"
		if server.isFollower() {
			inRecovery = "t"
		}
		return scalarResult("pg_is_in_recovery", inRecovery)

	// ── pg_backend_pid() ─────────────────────────────────────────────────
	case strings.Contains(lower, "pg_backend_pid()"):
		return scalarResult("pg_backend_pid", "1")

	// ── inet_server_addr() / inet_server_port() ──────────────────────────
	case strings.Contains(lower, "inet_server_addr()"):
		return scalarResult("inet_server_addr", "127.0.0.1")
	case strings.Contains(lower, "inet_server_port()"):
		addr := server.config.Address
		port := "5433"
		if _, p, err := net.SplitHostPort(addr); err == nil {
			port = p
		}
		return scalarResult("inet_server_port", port)

	// ── obj_description / col_description / shobj_description ────────────
	// psql tab-completion and \d commands call these; return NULLs.
	case strings.Contains(lower, "obj_description"),
		strings.Contains(lower, "col_description"),
		strings.Contains(lower, "shobj_description"):
		return scalarResult("description", "")

	// ── has_schema_privilege / has_table_privilege / has_database_privilege ─
	// Authorization-probe functions used by psql \d and GUI tools.
	case strings.Contains(lower, "has_schema_privilege"),
		strings.Contains(lower, "has_table_privilege"),
		strings.Contains(lower, "has_database_privilege"):
		return scalarResult("has_privilege", "t")

	// ── pg_encoding_to_char ──────────────────────────────────────────────
	case strings.Contains(lower, "pg_encoding_to_char"):
		return scalarResult("pg_encoding_to_char", "UTF8")

	// ── SET / RESET / DEALLOCATE — treat as no-ops ───────────────────────
	// JDBC drivers and GUI tools (DBeaver, DataGrip, pgAdmin, psql) send
	// SET/RESET statements during connection setup and session management.
	// ASQL accepts them deterministically as no-ops.
	case strings.HasPrefix(lower, "set "),
		strings.HasPrefix(lower, "reset "),
		lower == "reset all",
		strings.HasPrefix(lower, "deallocate "),
		lower == "deallocate all":
		return emptyResult()

	// ── pg_catalog.pg_tables / information_schema.tables ────────────────
	case strings.Contains(lower, "pg_catalog.pg_tables"),
		strings.Contains(lower, "information_schema.tables"),
		strings.Contains(lower, "pg_tables"):
		return server.catalogTables()

	// ── pg_catalog.pg_namespace ──────────────────────────────────────────
	case strings.Contains(lower, "pg_catalog.pg_namespace"),
		strings.Contains(lower, "pg_namespace"):
		return server.catalogNamespaces()

	// ── pg_catalog.pg_class ──────────────────────────────────────────────
	case strings.Contains(lower, "pg_catalog.pg_class"),
		strings.Contains(lower, "pg_class"):
		return server.catalogClasses()

	// ── pg_catalog.pg_attribute ──────────────────────────────────────────
	case strings.Contains(lower, "pg_catalog.pg_attribute"),
		strings.Contains(lower, "pg_attribute"):
		return server.catalogAttributes()

	// ── pg_catalog.pg_type ───────────────────────────────────────────────
	case strings.Contains(lower, "pg_catalog.pg_type"),
		strings.Contains(lower, "pg_type"):
		return server.catalogTypes()

	// ── information_schema.columns ───────────────────────────────────────
	case strings.Contains(lower, "information_schema.columns"):
		return server.catalogColumns()

	// ── information_schema.schemata ──────────────────────────────────────
	case strings.Contains(lower, "information_schema.schemata"):
		return server.catalogSchemata()

	// ── pg_catalog.pg_settings ───────────────────────────────────────────
	// Some GUI tools (DBeaver, DataGrip) query pg_settings to discover GUCs.
	case strings.Contains(lower, "pg_catalog.pg_settings"),
		strings.Contains(lower, "pg_settings"):
		return catalogPgSettings()

	// ── pg_catalog.pg_database ───────────────────────────────────────────
	// psql \l and GUI tools query pg_database to list available databases.
	case strings.Contains(lower, "pg_catalog.pg_database"),
		strings.Contains(lower, "pg_database"):
		return catalogPgDatabase()

	// ── pg_catalog.pg_index / pg_constraint / pg_proc ────────────────────
	// Return schema-stable empty result sets for tables we haven't fully
	// implemented so clients still receive RowDescription metadata.
	case strings.Contains(lower, "pg_index"):
		return emptyResultWithColumns(
			"indexrelid", "indrelid", "indnatts", "indnkeyatts", "indisunique",
			"indnullsnotdistinct", "indisprimary", "indisexclusion", "indimmediate",
			"indisclustered", "indisvalid", "indcheckxmin", "indisready",
			"indislive", "indisreplident", "indkey", "indcollation", "indclass",
			"indoption", "indexprs", "indpred",
		)
	case strings.Contains(lower, "pg_constraint"):
		return emptyResultWithColumns(
			"oid", "conname", "connamespace", "contype", "condeferrable",
			"condeferred", "convalidated", "conrelid", "contypid", "conindid",
			"conparentid", "confrelid", "confupdtype", "confdeltype",
			"confmatchtype", "conislocal", "coninhcount", "connoinherit",
			"conkey", "confkey", "conpfeqop", "conppeqop", "conffeqop",
			"confdelsetcols", "conexclop", "conbin",
		)
	case strings.Contains(lower, "pg_proc"):
		return emptyResultWithColumns(
			"oid", "proname", "pronamespace", "proowner", "prolang", "procost",
			"prorows", "provariadic", "prosupport", "prokind", "prosecdef",
			"proleakproof", "proisstrict", "proretset", "provolatile",
			"proparallel", "pronargs", "pronargdefaults", "prorettype",
			"proargtypes", "proallargtypes", "proargmodes", "proargnames",
			"proargdefaults", "protrftypes", "prosrc", "probin", "prosqlbody",
			"proconfig", "proacl",
		)
	case strings.Contains(lower, "pg_am"):
		return emptyResultWithColumns("oid", "amname", "amhandler", "amtype")
	case strings.Contains(lower, "pg_extension"):
		return emptyResultWithColumns(
			"oid", "extname", "extowner", "extnamespace", "extrelocatable",
			"extversion", "extconfig", "extcondition",
		)
	case strings.Contains(lower, "pg_roles"):
		return emptyResultWithColumns(
			"oid", "rolname", "rolsuper", "rolinherit", "rolcreaterole",
			"rolcreatedb", "rolcanlogin", "rolreplication", "rolbypassrls",
			"rolconnlimit", "rolpassword", "rolvaliduntil", "rolconfig",
		)
	case strings.Contains(lower, "pg_authid"):
		return emptyResultWithColumns(
			"oid", "rolname", "rolsuper", "rolinherit", "rolcreaterole",
			"rolcreatedb", "rolcanlogin", "rolreplication", "rolbypassrls",
			"rolconnlimit", "rolpassword", "rolvaliduntil",
		)
	case strings.Contains(lower, "pg_user"):
		return emptyResultWithColumns(
			"usename", "usesysid", "usecreatedb", "usesuper", "userepl",
			"usebypassrls", "passwd", "valuntil", "useconfig",
		)

	// ── asql_admin virtual schema ─────────────────────────────────────────
	// Each sub-table exposes engine internals as SQL rows, allowing admin
	// operations without a separate HTTP layer.

	case strings.Contains(lower, "asql_admin.engine_stats"):
		return server.adminEngineStats()

	case strings.Contains(lower, "asql_admin.schema_snapshot"):
		return server.adminSchemaSnapshot(sql)

	case strings.Contains(lower, "asql_admin.timeline_commits"):
		return server.adminTimelineCommits(ctx, sql)

	case strings.Contains(lower, "asql_admin.scan_strategy_stats"):
		return server.adminScanStrategyStats()

	case strings.Contains(lower, "asql_admin.replication_status"):
		return server.adminReplicationStatus()

	case strings.Contains(lower, "asql_admin.leadership_state"):
		return server.adminLeadershipState(sql)

	case strings.Contains(lower, "asql_admin.row_history"):
		return server.adminRowHistory(ctx, sql)

	case strings.Contains(lower, "asql_admin.entity_version_history"):
		return server.adminEntityVersionHistory(ctx, sql)

	case strings.Contains(lower, "asql_admin.cluster_members"):
		return server.adminClusterMembers()

	case strings.HasPrefix(lower, "select asql_admin.replay_to_lsn("):
		return server.adminReplayToLSN(ctx, sql)
	}

	return interceptResult{}, false
}

// ── current_setting / set_config handlers ─────────────────────────────────────

// handleCurrentSetting extracts the parameter name from a
// SELECT current_setting('param') query and returns its default value.
func (server *Server) handleCurrentSetting(lower string) (interceptResult, bool) {
	m := reCurrentSetting.FindStringSubmatch(lower)
	if m == nil {
		return interceptResult{}, false
	}
	param := strings.ToLower(m[1])

	// Special case: pg_is_in_recovery is sometimes queried via current_setting.
	if param == "is_in_recovery" {
		v := "off"
		if server.isFollower() {
			v = "on"
		}
		return scalarResult("current_setting", v)
	}

	if val, ok := currentSettingDefaults[param]; ok {
		return scalarResult("current_setting", val)
	}
	// Unknown parameter — return empty string rather than erroring out.
	return scalarResult("current_setting", "")
}

func (server *Server) handleRowLSN(sql string) (interceptResult, bool) {
	m := reRowLSN.FindStringSubmatch(sql)
	if m == nil {
		return interceptResult{}, false
	}
	tableRef := strings.ReplaceAll(m[1], "''", "'")
	primaryKey := strings.ReplaceAll(m[2], "''", "'")
	rowLSN, ok, err := server.engine.RowLSN(tableRef, primaryKey)
	if err != nil {
		return interceptResult{result: executor.Result{Status: fmt.Sprintf("ERROR: %v", err)}}, true
	}
	if !ok {
		return literalScalarResult("row_lsn", ast.Literal{Kind: ast.LiteralNull})
	}
	return literalScalarResult("row_lsn", litU(rowLSN))
}

func (server *Server) handleEntityVersion(sql string) (interceptResult, bool) {
	m := reEntityVersion.FindStringSubmatch(sql)
	if m == nil {
		return interceptResult{}, false
	}
	domain := strings.ReplaceAll(m[1], "''", "'")
	entityName := strings.ReplaceAll(m[2], "''", "'")
	rootPK := strings.ReplaceAll(m[3], "''", "'")
	version, ok, err := server.engine.EntityVersion(domain, entityName, rootPK)
	if err != nil {
		return interceptResult{result: executor.Result{Status: fmt.Sprintf("ERROR: %v", err)}}, true
	}
	if !ok {
		return literalScalarResult("entity_version", ast.Literal{Kind: ast.LiteralNull})
	}
	return literalScalarResult("entity_version", litU(version))
}

func (server *Server) handleEntityHeadLSN(sql string) (interceptResult, bool) {
	m := reEntityHeadLSN.FindStringSubmatch(sql)
	if m == nil {
		return interceptResult{}, false
	}
	domain := strings.ReplaceAll(m[1], "''", "'")
	entityName := strings.ReplaceAll(m[2], "''", "'")
	rootPK := strings.ReplaceAll(m[3], "''", "'")
	headLSN, ok, err := server.engine.EntityHeadLSN(domain, entityName, rootPK)
	if err != nil {
		return interceptResult{result: executor.Result{Status: fmt.Sprintf("ERROR: %v", err)}}, true
	}
	if !ok {
		return literalScalarResult("entity_head_lsn", ast.Literal{Kind: ast.LiteralNull})
	}
	return literalScalarResult("entity_head_lsn", litU(headLSN))
}

func (server *Server) handleEntityVersionLSN(sql string) (interceptResult, bool) {
	m := reEntityVersionLSN.FindStringSubmatch(sql)
	if m == nil {
		return interceptResult{}, false
	}
	domain := strings.ReplaceAll(m[1], "''", "'")
	entityName := strings.ReplaceAll(m[2], "''", "'")
	rootPK := strings.ReplaceAll(m[3], "''", "'")
	version, err := strconv.ParseUint(m[4], 10, 64)
	if err != nil {
		return interceptResult{result: executor.Result{Status: fmt.Sprintf("ERROR: invalid entity version %q", m[4])}}, true
	}
	lsn, ok, err := server.engine.EntityVersionLSN(domain, entityName, rootPK, version)
	if err != nil {
		return interceptResult{result: executor.Result{Status: fmt.Sprintf("ERROR: %v", err)}}, true
	}
	if !ok {
		return literalScalarResult("entity_version_lsn", ast.Literal{Kind: ast.LiteralNull})
	}
	return literalScalarResult("entity_version_lsn", litU(lsn))
}

func (server *Server) handleResolveReference(sql string) (interceptResult, bool) {
	m := reResolveReference.FindStringSubmatch(sql)
	if m == nil {
		return interceptResult{}, false
	}
	tableRef := strings.ReplaceAll(m[1], "''", "'")
	primaryKey := strings.ReplaceAll(m[2], "''", "'")
	resolved, ok, err := server.engine.ResolveReference(tableRef, primaryKey)
	if err != nil {
		return interceptResult{result: executor.Result{Status: fmt.Sprintf("ERROR: %v", err)}}, true
	}
	if !ok {
		return literalScalarResult("resolve_reference", ast.Literal{Kind: ast.LiteralNull})
	}
	return literalScalarResult("resolve_reference", litU(resolved))
}

// handleSetConfig is a no-op handler for set_config(name, value, is_local).
// It extracts the value argument and echoes it back as the result.
func handleSetConfig(sql string) (interceptResult, bool) {
	m := reSetConfig.FindStringSubmatch(sql)
	if m == nil {
		return interceptResult{}, false
	}
	// m[2] is the value argument — use original-cased sql to preserve case.
	return scalarResult("set_config", m[2])
}

// ── pg_settings synthetic table ───────────────────────────────────────────────

// catalogPgSettings returns a minimal pg_settings result containing the
// GUC parameters from currentSettingDefaults.  The real pg_settings has
// ~17 columns, but most tools only need name + setting.
func catalogPgSettings() (interceptResult, bool) {
	cols := []string{"name", "setting", "unit", "category", "short_desc", "context", "vartype", "source"}
	rows := make([]map[string]ast.Literal, 0, len(currentSettingDefaults))
	for name, val := range currentSettingDefaults {
		rows = append(rows, map[string]ast.Literal{
			"name":       litS(name),
			"setting":    litS(val),
			"unit":       litS(""),
			"category":   litS("Preset Options"),
			"short_desc": litS(""),
			"context":    litS("internal"),
			"vartype":    litS("string"),
			"source":     litS("default"),
		})
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: cols,
	}, true
}

// ── pg_database synthetic table ───────────────────────────────────────────────

// catalogPgDatabase returns a single-row pg_database with the "asql" database.
func catalogPgDatabase() (interceptResult, bool) {
	cols := []string{
		"oid", "datname", "datdba", "encoding", "datcollate",
		"datctype", "datistemplate", "datallowconn", "datconnlimit",
		"datlastsysoid", "datfrozenxid", "datminmxid", "dattablespace",
	}
	row := map[string]ast.Literal{
		"oid":           lit(16384),
		"datname":       litS("asql"),
		"datdba":        lit(10),
		"encoding":      lit(6),
		"datcollate":    litS("en_US.UTF-8"),
		"datctype":      litS("en_US.UTF-8"),
		"datistemplate": litB(false),
		"datallowconn":  litB(true),
		"datconnlimit":  lit(-1),
		"datlastsysoid": lit(12000),
		"datfrozenxid":  litS("726"),
		"datminmxid":    litS("1"),
		"dattablespace": lit(1663),
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: []map[string]ast.Literal{row}},
		columns: cols,
	}, true
}

// ── asql_admin virtual table builders ─────────────────────────────────────────

// ── JSON types for schema_snapshot — mirrors httpapi.SchemaSnapshotResponse ──
// Defined locally to avoid a cross-package dependency.

type snapColumn struct {
	Name             string `json:"name"`
	Type             string `json:"type"`
	PrimaryKey       bool   `json:"primary_key,omitempty"`
	Unique           bool   `json:"unique,omitempty"`
	ReferencesTable  string `json:"references_table,omitempty"`
	ReferencesColumn string `json:"references_column,omitempty"`
	DefaultValue     string `json:"default_value,omitempty"`
}

type snapIndex struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Method  string   `json:"method"`
}

type snapVersionedFK struct {
	Column           string `json:"column"`
	LSNColumn        string `json:"lsn_column"`
	ReferencesDomain string `json:"references_domain"`
	ReferencesTable  string `json:"references_table"`
	ReferencesColumn string `json:"references_column"`
}

type snapTable struct {
	Name                 string            `json:"name"`
	Columns              []snapColumn      `json:"columns"`
	Indexes              []snapIndex       `json:"indexes,omitempty"`
	VersionedForeignKeys []snapVersionedFK `json:"versioned_foreign_keys,omitempty"`
}

type snapEntity struct {
	Name      string   `json:"name"`
	RootTable string   `json:"root_table"`
	Tables    []string `json:"tables"`
}

type snapDomain struct {
	Name     string       `json:"name"`
	Tables   []snapTable  `json:"tables"`
	Entities []snapEntity `json:"entities,omitempty"`
}

type snapResponse struct {
	Status  string       `json:"status"`
	Domains []snapDomain `json:"domains"`
}

// reSnapshotDomains extracts optional domain names from a WHERE clause:
// WHERE domain IN ('a','b') or WHERE domain = 'a'
var reSnapshotDomains = regexp.MustCompile(`(?i)domain\s+in\s*\(([^)]+)\)|domain\s*=\s*'([^']*)'`)

func extractSnapshotDomains(sql string) []string {
	m := reSnapshotDomains.FindStringSubmatch(sql)
	if m == nil {
		return nil
	}
	if m[2] != "" {
		return []string{m[2]}
	}
	var out []string
	for _, part := range strings.Split(m[1], ",") {
		s := strings.Trim(strings.TrimSpace(part), "'\"")
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// snapFormatDefault formats an ast.DefaultExpr the same way as httpapi.formatDefaultValue.
func snapFormatDefault(def *ast.DefaultExpr) string {
	if def == nil {
		return ""
	}
	switch def.Kind {
	case ast.DefaultAutoIncrement:
		return "AUTOINCREMENT"
	case ast.DefaultUUIDv7:
		return "UUID_V7"
	case ast.DefaultLiteral:
		switch def.Value.Kind {
		case ast.LiteralNull:
			return "NULL"
		case ast.LiteralString:
			return "'" + def.Value.StringValue + "'"
		case ast.LiteralNumber:
			return fmt.Sprintf("%d", def.Value.NumberValue)
		case ast.LiteralBoolean:
			if def.Value.BoolValue {
				return "TRUE"
			}
			return "FALSE"
		case ast.LiteralFloat:
			return fmt.Sprintf("%g", def.Value.FloatValue)
		}
	}
	return ""
}

// adminSchemaSnapshot serves SELECT snapshot FROM asql_admin.schema_snapshot.
// Returns a single row with a JSON column containing the full schema.
func (server *Server) adminSchemaSnapshot(sql string) (interceptResult, bool) {
	domains := extractSnapshotDomains(sql)
	snap := server.engine.SchemaSnapshot(domains)

	resp := snapResponse{Status: "SNAPSHOT", Domains: make([]snapDomain, 0, len(snap.Domains))}
	for _, d := range snap.Domains {
		tables := make([]snapTable, 0, len(d.Tables))
		for _, t := range d.Tables {
			cols := make([]snapColumn, 0, len(t.Columns))
			for _, c := range t.Columns {
				cols = append(cols, snapColumn{
					Name:             c.Name,
					Type:             c.Type,
					PrimaryKey:       c.PrimaryKey,
					Unique:           c.Unique,
					ReferencesTable:  c.ReferencesTable,
					ReferencesColumn: c.ReferencesColumn,
					DefaultValue:     snapFormatDefault(c.DefaultValue),
				})
			}
			idxs := make([]snapIndex, 0, len(t.Indexes))
			for _, idx := range t.Indexes {
				cp := make([]string, len(idx.Columns))
				copy(cp, idx.Columns)
				idxs = append(idxs, snapIndex{Name: idx.Name, Columns: cp, Method: idx.Method})
			}
			vfks := make([]snapVersionedFK, 0, len(t.VersionedForeignKeys))
			for _, vfk := range t.VersionedForeignKeys {
				vfks = append(vfks, snapVersionedFK{
					Column: vfk.Column, LSNColumn: vfk.LSNColumn,
					ReferencesDomain: vfk.ReferencesDomain, ReferencesTable: vfk.ReferencesTable,
					ReferencesColumn: vfk.ReferencesColumn,
				})
			}
			tables = append(tables, snapTable{Name: t.Name, Columns: cols, Indexes: idxs, VersionedForeignKeys: vfks})
		}
		entities := make([]snapEntity, 0, len(d.Entities))
		for _, e := range d.Entities {
			cp := make([]string, len(e.Tables))
			copy(cp, e.Tables)
			entities = append(entities, snapEntity{Name: e.Name, RootTable: e.RootTable, Tables: cp})
		}
		resp.Domains = append(resp.Domains, snapDomain{Name: d.Name, Tables: tables, Entities: entities})
	}

	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		return interceptResult{result: executor.Result{Status: fmt.Sprintf("ERROR: %v", err)}}, true
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: []map[string]ast.Literal{{"snapshot": litS(string(jsonBytes))}}},
		columns: []string{"snapshot"},
	}, true
}

// adminEngineStats returns a single-row result with live engine performance
// metrics for SELECT * FROM asql_admin.engine_stats.
func (server *Server) adminEngineStats() (interceptResult, bool) {
	s := server.engine.PerfStats()
	columns := []string{
		"total_commits", "total_reads", "total_rollbacks", "total_begins",
		"total_cross_domain_begins",
		"total_time_travel_queries", "total_snapshots", "total_replays", "total_fsync_errors", "total_audit_errors", "active_transactions", "commit_batch_count",
		"cross_domain_begin_avg_domains", "cross_domain_begin_max_domains",
		"commit_batch_avg_jobs", "commit_batch_max_jobs", "commit_batch_avg_wal_records", "commit_batch_max_wal_records",
		"commit_latency_p50_ms", "commit_latency_p95_ms", "commit_latency_p99_ms",
		"fsync_latency_p50_ms", "fsync_latency_p95_ms", "fsync_latency_p99_ms",
		"commit_queue_wait_p50_ms", "commit_queue_wait_p95_ms", "commit_queue_wait_p99_ms",
		"commit_write_hold_p50_ms", "commit_write_hold_p95_ms", "commit_write_hold_p99_ms",
		"commit_apply_p50_ms", "commit_apply_p95_ms", "commit_apply_p99_ms",
		"read_latency_p50_ms", "read_latency_p95_ms", "read_latency_p99_ms",
		"time_travel_latency_p50_ms", "time_travel_latency_p95_ms", "time_travel_latency_p99_ms",
		"replay_duration_ms", "snapshot_duration_ms",
		"commit_throughput_per_sec", "read_throughput_per_sec",
		"wal_file_size_bytes", "snapshot_file_size_bytes", "audit_file_size_bytes",
	}
	row := map[string]ast.Literal{
		"total_commits":                  litU(s.TotalCommits),
		"total_reads":                    litU(s.TotalReads),
		"total_rollbacks":                litU(s.TotalRollbacks),
		"total_begins":                   litU(s.TotalBegins),
		"total_cross_domain_begins":      litU(s.TotalCrossDomainBegins),
		"total_time_travel_queries":      litU(s.TotalTimeTravelQueries),
		"total_snapshots":                litU(s.TotalSnapshots),
		"total_replays":                  litU(s.TotalReplays),
		"total_fsync_errors":             litU(s.TotalFsyncErrors),
		"total_audit_errors":             litU(s.TotalAuditErrors),
		"active_transactions":            lit(s.ActiveTransactions),
		"commit_batch_count":             litU(s.CommitBatchCount),
		"cross_domain_begin_avg_domains": litF(s.CrossDomainBeginAvgDomains),
		"cross_domain_begin_max_domains": litU(s.CrossDomainBeginMaxDomains),
		"commit_batch_avg_jobs":          litF(s.CommitBatchAvgJobs),
		"commit_batch_max_jobs":          litU(s.CommitBatchMaxJobs),
		"commit_batch_avg_wal_records":   litF(s.CommitBatchAvgWalRecords),
		"commit_batch_max_wal_records":   litU(s.CommitBatchMaxWalRecords),
		"commit_latency_p50_ms":          litF(s.CommitLatencyP50),
		"commit_latency_p95_ms":          litF(s.CommitLatencyP95),
		"commit_latency_p99_ms":          litF(s.CommitLatencyP99),
		"fsync_latency_p50_ms":           litF(s.FsyncLatencyP50),
		"fsync_latency_p95_ms":           litF(s.FsyncLatencyP95),
		"fsync_latency_p99_ms":           litF(s.FsyncLatencyP99),
		"commit_queue_wait_p50_ms":       litF(s.CommitQueueWaitP50),
		"commit_queue_wait_p95_ms":       litF(s.CommitQueueWaitP95),
		"commit_queue_wait_p99_ms":       litF(s.CommitQueueWaitP99),
		"commit_write_hold_p50_ms":       litF(s.CommitWriteHoldP50),
		"commit_write_hold_p95_ms":       litF(s.CommitWriteHoldP95),
		"commit_write_hold_p99_ms":       litF(s.CommitWriteHoldP99),
		"commit_apply_p50_ms":            litF(s.CommitApplyP50),
		"commit_apply_p95_ms":            litF(s.CommitApplyP95),
		"commit_apply_p99_ms":            litF(s.CommitApplyP99),
		"read_latency_p50_ms":            litF(s.ReadLatencyP50),
		"read_latency_p95_ms":            litF(s.ReadLatencyP95),
		"read_latency_p99_ms":            litF(s.ReadLatencyP99),
		"time_travel_latency_p50_ms":     litF(s.TimeTravelLatencyP50),
		"time_travel_latency_p95_ms":     litF(s.TimeTravelLatencyP95),
		"time_travel_latency_p99_ms":     litF(s.TimeTravelLatencyP99),
		"replay_duration_ms":             litF(s.ReplayDurationMs),
		"snapshot_duration_ms":           litF(s.SnapshotDurationMs),
		"commit_throughput_per_sec":      litF(s.CommitThroughput),
		"read_throughput_per_sec":        litF(s.ReadThroughput),
		"wal_file_size_bytes":            lit(s.WALFileSize),
		"snapshot_file_size_bytes":       lit(s.SnapshotFileSize), "audit_file_size_bytes": lit(s.AuditFileSize)}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: []map[string]ast.Literal{row}},
		columns: columns,
	}, true
}

// reFromLSN / reTOLSN / reDomain / reLimit extract simple filter literals from
// a timeline_commits query of the form:
//
//	SELECT * FROM asql_admin.timeline_commits
//	  WHERE from_lsn >= 10 AND to_lsn <= 200 AND domain = 'orders' LIMIT 50
var (
	reFromLSN     = regexp.MustCompile(`(?i)from_lsn\s*[>=]+\s*(\d+)`)
	reToLSN       = regexp.MustCompile(`(?i)to_lsn\s*[<=]+\s*(\d+)`)
	reAdminDomain = regexp.MustCompile(`(?i)domain\s*=\s*'([^']*)'`)
	reLimit       = regexp.MustCompile(`(?i)\blimit\s+(\d+)`)
)

// adminTimelineCommits serves SELECT * FROM asql_admin.timeline_commits.
// Supports optional WHERE from_lsn, to_lsn, domain and LIMIT filters.
func (server *Server) adminTimelineCommits(ctx context.Context, sql string) (interceptResult, bool) {
	fromLSN := uint64(0)
	toLSN := uint64(0)
	domain := ""
	limit := 0

	if m := reFromLSN.FindStringSubmatch(sql); m != nil {
		fromLSN, _ = strconv.ParseUint(m[1], 10, 64)
	}
	if m := reToLSN.FindStringSubmatch(sql); m != nil {
		toLSN, _ = strconv.ParseUint(m[1], 10, 64)
	}
	if m := reAdminDomain.FindStringSubmatch(sql); m != nil {
		domain = m[1]
	}
	if m := reLimit.FindStringSubmatch(sql); m != nil {
		limit, _ = strconv.Atoi(m[1])
	}

	commits, err := server.engine.TimelineCommits(ctx, fromLSN, toLSN, domain, limit)
	if err != nil {
		return interceptResult{
			result: executor.Result{Status: "ERROR", Rows: nil},
		}, true
	}

	columns := []string{"lsn", "tx_id", "timestamp", "mutation_count"}
	rows := make([]map[string]ast.Literal, 0, len(commits))
	for _, c := range commits {
		rows = append(rows, map[string]ast.Literal{
			"lsn":            litU(c.LSN),
			"tx_id":          litS(c.TxID),
			"timestamp":      litU(c.Timestamp),
			"mutation_count": lit(int64(len(c.Tables))),
		})
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

// adminScanStrategyStats serves SELECT * FROM asql_admin.scan_strategy_stats.
func (server *Server) adminScanStrategyStats() (interceptResult, bool) {
	counts := server.engine.ScanStrategyCounts()
	columns := []string{"strategy", "count"}
	rows := make([]map[string]ast.Literal, 0, len(counts))
	for strategy, count := range counts {
		rows = append(rows, map[string]ast.Literal{
			"strategy": litS(strategy),
			"count":    litU(count),
		})
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

// adminReplicationStatus serves SELECT * FROM asql_admin.replication_status.
func (server *Server) adminReplicationStatus() (interceptResult, bool) {
	var lastLSN uint64
	if server.walStore != nil {
		lastLSN = server.walStore.LastLSN()
	}
	columns := []string{"last_lsn"}
	row := map[string]ast.Literal{"last_lsn": litU(lastLSN)}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: []map[string]ast.Literal{row}},
		columns: columns,
	}, true
}

// reLeaderGroup extracts the group filter from a leadership_state query.
var reLeaderGroup = regexp.MustCompile(`(?i)"?group"?\s*=\s*'([^']*)'`)

// adminLeadershipState serves SELECT * FROM asql_admin.leadership_state.
func (server *Server) adminLeadershipState(sql string) (interceptResult, bool) {
	columns := []string{"group_name", "term", "leader_id", "fencing_token", "lease_expires_at", "last_leader_lsn", "lease_active"}
	if server.leadership == nil {
		return interceptResult{
			result:  executor.Result{Status: "OK", Rows: nil},
			columns: columns,
		}, true
	}

	var groups []string
	if m := reLeaderGroup.FindStringSubmatch(sql); m != nil {
		groups = []string{m[1]}
	} else {
		groups = server.leadership.Groups()
	}

	rows := make([]map[string]ast.Literal, 0, len(groups))
	for _, g := range groups {
		state, exists, leaseActive := server.leadership.SnapshotWithLeaseStatus(g)
		if !exists {
			continue
		}
		rows = append(rows, map[string]ast.Literal{
			"group_name":       litS(state.Group),
			"term":             litU(state.Term),
			"leader_id":        litS(state.LeaderID),
			"fencing_token":    litS(state.FencingToken),
			"lease_expires_at": litS(state.LeaseExpiresAt.UTC().Format(time.RFC3339)),
			"last_leader_lsn":  litU(state.LastLeaderLSN),
			"lease_active":     litB(leaseActive),
		})
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

// adminRowHistory serves SELECT * FROM asql_admin.row_history.
// The original query is rewritten into a SELECT ... FOR HISTORY query.
// Example: SELECT * FROM asql_admin.row_history WHERE sql = 'SELECT id FROM orders.orders'
//
// The regex uses the SQL-style ” escape sequence for single quotes inside the
// delimited string (not backslash, which is what the old pattern handled).
// Pattern: match any char that is not a single-quote, OR match exactly ” (two
// consecutive single quotes = one escaped quote in SQL).
var reRowHistorySQL = regexp.MustCompile(`(?i)sql\s*=\s*'((?:[^']|'')*)'`)

func (server *Server) adminRowHistory(ctx context.Context, sql string) (interceptResult, bool) {
	m := reRowHistorySQL.FindStringSubmatch(sql)
	if m == nil {
		return interceptResult{
			result: executor.Result{Status: "ERROR"},
		}, true
	}
	innerSQL := strings.ReplaceAll(m[1], "''", "'") // unescape SQL-escaped single quotes
	result, err := server.engine.RowHistory(ctx, innerSQL, nil)
	if err != nil {
		return interceptResult{
			result: executor.Result{Status: fmt.Sprintf("ERROR: %v", err)},
		}, true
	}
	// allColumnsFromRows returns a SORTED column list which is critical for
	// the extended query protocol: describeFields and handleExtendedExecute
	// each call interceptCatalog independently.  Both must produce columns in
	// the same deterministic order so that RowDescription OIDs align with
	// DataRow values.  Go map iteration is non-deterministic, so a plain
	// `for k := range row` here would produce different orderings on each
	// call, causing pgx to decode values with the wrong OID.
	columns := allColumnsFromRows(result.Rows)
	if len(result.Rows) == 0 {
		columns = []string{}
	}
	return interceptResult{result: result, columns: columns}, true
}

// adminEntityVersionHistory serves SELECT * FROM asql_admin.entity_version_history.
// Example: SELECT * FROM asql_admin.entity_version_history
//
//	WHERE domain = 'orders' AND entity = 'Order' AND root_pk = '42'
var (
	reEntityDomain = regexp.MustCompile(`(?i)domain\s*=\s*'([^']*)'`)
	reEntity       = regexp.MustCompile(`(?i)entity\s*=\s*'([^']*)'`)
	reRootPK       = regexp.MustCompile(`(?i)root_pk\s*=\s*'([^']*)'`)
)

func (server *Server) adminEntityVersionHistory(ctx context.Context, sql string) (interceptResult, bool) {
	domain := ""
	entity := ""
	rootPK := ""
	if m := reEntityDomain.FindStringSubmatch(sql); m != nil {
		domain = m[1]
	}
	if m := reEntity.FindStringSubmatch(sql); m != nil {
		entity = m[1]
	}
	if m := reRootPK.FindStringSubmatch(sql); m != nil {
		rootPK = m[1]
	}

	entries, err := server.engine.EntityVersionHistory(ctx, domain, entity, rootPK)
	if err != nil {
		return interceptResult{
			result: executor.Result{Status: fmt.Sprintf("ERROR: %v", err)},
		}, true
	}

	columns := []string{"version", "commit_lsn", "tables"}
	rows := make([]map[string]ast.Literal, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, map[string]ast.Literal{
			"version":    litU(e.Version),
			"commit_lsn": litU(e.CommitLSN),
			"tables":     litS(strings.Join(e.Tables, ",")),
		})
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

// reReplayLSN extracts the LSN argument from SELECT asql_admin.replay_to_lsn(N).
var reReplayLSN = regexp.MustCompile(`(?i)asql_admin\.replay_to_lsn\((\d+)\)`)

// adminReplayToLSN serves SELECT asql_admin.replay_to_lsn(N).
func (server *Server) adminReplayToLSN(ctx context.Context, sql string) (interceptResult, bool) {
	m := reReplayLSN.FindStringSubmatch(sql)
	if m == nil {
		return scalarResult("replay_to_lsn", "ERROR: missing LSN argument")
	}
	targetLSN, _ := strconv.ParseUint(m[1], 10, 64)
	if err := server.engine.ReplayToLSN(ctx, targetLSN); err != nil {
		return scalarResult("replay_to_lsn", fmt.Sprintf("ERROR: %v", err))
	}
	return scalarResult("replay_to_lsn", fmt.Sprintf("OK: replayed to LSN %d", targetLSN))
}

// ── Catalog result builders ───────────────────────────────────────────────────

func (server *Server) catalogTables() (interceptResult, bool) {
	snap := server.engine.SchemaSnapshot(nil)
	columns := []string{"table_schema", "table_name", "table_type"}
	rows := make([]map[string]ast.Literal, 0)
	for _, d := range snap.Domains {
		for _, t := range d.Tables {
			rows = append(rows, map[string]ast.Literal{
				"table_schema": {Kind: ast.LiteralString, StringValue: d.Name},
				"table_name":   {Kind: ast.LiteralString, StringValue: t.Name},
				"table_type":   {Kind: ast.LiteralString, StringValue: "BASE TABLE"},
			})
		}
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

func (server *Server) catalogNamespaces() (interceptResult, bool) {
	snap := server.engine.SchemaSnapshot(nil)
	columns := []string{"nspname", "nspowner"}
	rows := []map[string]ast.Literal{
		{
			"nspname":  {Kind: ast.LiteralString, StringValue: "public"},
			"nspowner": {Kind: ast.LiteralNumber, NumberValue: 10},
		},
	}
	for i, d := range snap.Domains {
		rows = append(rows, map[string]ast.Literal{
			"nspname":  {Kind: ast.LiteralString, StringValue: d.Name},
			"nspowner": {Kind: ast.LiteralNumber, NumberValue: int64(100 + i)},
		})
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

func (server *Server) catalogClasses() (interceptResult, bool) {
	snap := server.engine.SchemaSnapshot(nil)
	columns := []string{"relname", "relnamespace", "relkind", "reltuples"}
	rows := make([]map[string]ast.Literal, 0)
	for i, d := range snap.Domains {
		for _, t := range d.Tables {
			rows = append(rows, map[string]ast.Literal{
				"relname":      {Kind: ast.LiteralString, StringValue: t.Name},
				"relnamespace": {Kind: ast.LiteralNumber, NumberValue: int64(100 + i)},
				"relkind":      {Kind: ast.LiteralString, StringValue: "r"}, // ordinary table
				"reltuples":    {Kind: ast.LiteralFloat, FloatValue: 0},
			})
		}
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

func (server *Server) catalogAttributes() (interceptResult, bool) {
	snap := server.engine.SchemaSnapshot(nil)
	columns := []string{"attrelid", "attname", "atttypid", "attnum", "attnotnull", "atthasdef"}
	rows := make([]map[string]ast.Literal, 0)
	relID := int64(1000)
	for _, d := range snap.Domains {
		for _, t := range d.Tables {
			for i, c := range t.Columns {
				rows = append(rows, map[string]ast.Literal{
					"attrelid":   {Kind: ast.LiteralNumber, NumberValue: relID},
					"attname":    {Kind: ast.LiteralString, StringValue: c.Name},
					"atttypid":   {Kind: ast.LiteralNumber, NumberValue: schemaTypeToOID(c.Type)},
					"attnum":     {Kind: ast.LiteralNumber, NumberValue: int64(i + 1)},
					"attnotnull": {Kind: ast.LiteralBoolean, BoolValue: c.PrimaryKey},
					"atthasdef":  {Kind: ast.LiteralBoolean, BoolValue: false},
				})
			}
			relID++
		}
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

func (server *Server) catalogTypes() (interceptResult, bool) {
	// Return the basic built-in types that tools care about.
	columns := []string{"oid", "typname", "typnamespace", "typlen"}
	rows := []map[string]ast.Literal{
		{"oid": lit(16), "typname": litS("bool"), "typnamespace": lit(11), "typlen": lit(1)},
		{"oid": lit(20), "typname": litS("int8"), "typnamespace": lit(11), "typlen": lit(8)},
		{"oid": lit(23), "typname": litS("int4"), "typnamespace": lit(11), "typlen": lit(4)},
		{"oid": lit(25), "typname": litS("text"), "typnamespace": lit(11), "typlen": lit(-1)},
		{"oid": lit(114), "typname": litS("json"), "typnamespace": lit(11), "typlen": lit(-1)},
		{"oid": lit(701), "typname": litS("float8"), "typnamespace": lit(11), "typlen": lit(8)},
		{"oid": lit(1043), "typname": litS("varchar"), "typnamespace": lit(11), "typlen": lit(-1)},
		{"oid": lit(1114), "typname": litS("timestamp"), "typnamespace": lit(11), "typlen": lit(8)},
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

func (server *Server) catalogColumns() (interceptResult, bool) {
	snap := server.engine.SchemaSnapshot(nil)
	columns := []string{"table_schema", "table_name", "column_name", "ordinal_position", "data_type", "is_nullable"}
	rows := make([]map[string]ast.Literal, 0)
	for _, d := range snap.Domains {
		for _, t := range d.Tables {
			for i, c := range t.Columns {
				nullable := "YES"
				if c.PrimaryKey {
					nullable = "NO"
				}
				rows = append(rows, map[string]ast.Literal{
					"table_schema":     litS(d.Name),
					"table_name":       litS(t.Name),
					"column_name":      litS(c.Name),
					"ordinal_position": lit(int64(i + 1)),
					"data_type":        litS(strings.ToLower(c.Type)),
					"is_nullable":      litS(nullable),
				})
			}
		}
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

func (server *Server) catalogSchemata() (interceptResult, bool) {
	snap := server.engine.SchemaSnapshot(nil)
	columns := []string{"catalog_name", "schema_name", "schema_owner"}
	rows := []map[string]ast.Literal{
		{"catalog_name": litS("asql"), "schema_name": litS("public"), "schema_owner": litS("asql")},
	}
	for _, d := range snap.Domains {
		rows = append(rows, map[string]ast.Literal{
			"catalog_name": litS("asql"),
			"schema_name":  litS(d.Name),
			"schema_owner": litS("asql"),
		})
	}
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func scalarResult(column, value string) (interceptResult, bool) {
	return interceptResult{
		result: executor.Result{
			Status: "OK",
			Rows:   []map[string]ast.Literal{{column: litS(value)}},
		},
		columns: []string{column},
	}, true
}

func literalScalarResult(column string, value ast.Literal) (interceptResult, bool) {
	return interceptResult{
		result: executor.Result{
			Status: "OK",
			Rows:   []map[string]ast.Literal{{column: value}},
		},
		columns: []string{column},
	}, true
}

func emptyResult() (interceptResult, bool) {
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: nil},
		columns: nil,
	}, true
}

func emptyResultWithColumns(columns ...string) (interceptResult, bool) {
	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: nil},
		columns: append([]string(nil), columns...),
	}, true
}

func lit(n int64) ast.Literal    { return ast.Literal{Kind: ast.LiteralNumber, NumberValue: n} }
func litS(s string) ast.Literal  { return ast.Literal{Kind: ast.LiteralString, StringValue: s} }
func litU(n uint64) ast.Literal  { return ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(n)} }
func litF(f float64) ast.Literal { return ast.Literal{Kind: ast.LiteralFloat, FloatValue: f} }
func litB(b bool) ast.Literal    { return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: b} }

// adminClusterMembers serves SELECT * FROM asql_admin.cluster_members.
// It returns one row per known cluster node: self + all peers in the
// heartbeat loop. Studio uses this to dynamically discover new nodes that
// hot-joined after Studio started.
func (server *Server) adminClusterMembers() (interceptResult, bool) {
	columns := []string{"node_id", "grpc_address", "pgwire_address", "is_self"}

	normalizeAddr := func(addr string) string {
		if addr == "" {
			return ""
		}
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return addr
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			return "127.0.0.1:" + port
		}
		return addr
	}

	rows := []map[string]ast.Literal{}

	// Self node.
	selfNodeID := server.config.NodeID
	if selfNodeID != "" {
		rows = append(rows, map[string]ast.Literal{
			"node_id":        litS(selfNodeID),
			"grpc_address":   litS(normalizeAddr(server.config.ClusterGRPCAddr)),
			"pgwire_address": litS(normalizeAddr(server.config.Address)),
			"is_self":        litB(true),
		})
	}

	// Peers from the heartbeat loop.
	if server.heartbeatLoop != nil {
		for _, p := range server.heartbeatLoop.Peers() {
			rows = append(rows, map[string]ast.Literal{
				"node_id":        litS(p.NodeID),
				"grpc_address":   litS(p.Address),
				"pgwire_address": litS(p.PgwireAddress),
				"is_self":        litB(false),
			})
		}
	}

	return interceptResult{
		result:  executor.Result{Status: "OK", Rows: rows},
		columns: columns,
	}, true
}

// schemaTypeToOID maps an ASQL column type string to a postgres type OID.
func schemaTypeToOID(t string) int64 {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	// ASQL stores all integers as int64 internally; advertise int8 (OID 20)
	// for every integer type so pgx never tries to decode via ParseInt(..., 32)
	// and overflows for large values (e.g. Unix-ms timestamps in INT columns).
	case "INT", "INTEGER", "INT4", "BIGINT", "INT8":
		return 20
	case "SMALLINT", "INT2":
		return 21
	case "BOOL", "BOOLEAN":
		return 16
	case "FLOAT", "FLOAT4", "REAL":
		return 700
	case "FLOAT8", "DOUBLE PRECISION":
		return 701
	case "JSON", "JSONB":
		return 114
	case "TIMESTAMP", "TIMESTAMPTZ":
		return 1114
	case "VARCHAR", "CHARACTER VARYING":
		return 1043
	default:
		return 25 // text
	}
}
