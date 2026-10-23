package pgwire

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"
)

var (
	reCatalogIndexRelIDFilter    = regexp.MustCompile(`(?i)\b(?:\w+\.)?indrelid\s*=\s*([0-9]+)`)
	reCatalogIndexesTableFilter  = regexp.MustCompile(`(?i)\b(?:\w+\.)?tablename\s*=\s*'([^']+)'`)
	reCatalogIndexesSchemaFilter = regexp.MustCompile(`(?i)\b(?:\w+\.)?schemaname\s*=\s*'([^']+)'`)
	catalogPgIndexColumns        = []string{"indexrelid", "indrelid", "indnatts", "indnkeyatts", "indisunique", "indnullsnotdistinct", "indisprimary", "indisexclusion", "indimmediate", "indisclustered", "indisvalid", "indcheckxmin", "indisready", "indislive", "indisreplident", "indkey", "indcollation", "indclass", "indoption", "indexprs", "indpred"}
	catalogPgIndexesColumns      = []string{"schemaname", "tablename", "indexname", "tablespace", "indexdef"}
	catalogImplicitIndexPrefixes = []string{"__auto_", "__pk_"}
)

// catalogIndex is one index as pg_index and pg_indexes show it.
type catalogIndex struct {
	oid      int64
	relOID   int64
	schema   string
	table    string
	name     string
	method   string
	columns  []string
	ordinals []int64
	unique   bool
	primary  bool
}

// catalogIndexes lists the indexes of every table: the primary key as
// <table>_pkey, each UNIQUE column as <table>_<column>_key (the names
// pg_constraint gives their constraints), then the indexes made with
// CREATE INDEX, by name. The engine's own hash indexes behind keys and
// UNIQUE columns are not listed again.
func catalogIndexes(snap executor.SchemaSnapshot) []catalogIndex {
	indexes := make([]catalogIndex, 0)
	for domainIndex, d := range snap.Domains {
		for tableIndex, t := range d.Tables {
			relOID := syntheticRelationOID(domainIndex, tableIndex)
			ordinals := catalogColumnOrdinals(t)
			ordinal := 0
			add := func(name, method string, columns []string, unique, primary bool) {
				ordinal++
				keyOrdinals := make([]int64, len(columns))
				for i, column := range columns {
					keyOrdinals[i] = ordinals[column]
				}
				indexes = append(indexes, catalogIndex{
					oid:      syntheticIndexOID(domainIndex, tableIndex, ordinal),
					relOID:   relOID,
					schema:   d.Name,
					table:    t.Name,
					name:     name,
					method:   method,
					columns:  columns,
					ordinals: keyOrdinals,
					unique:   unique,
					primary:  primary,
				})
			}
			// Postgres backs key and UNIQUE constraints with btree indexes;
			// the engine's hash index is how it checks them, not part of
			// the schema.
			if len(t.PrimaryKey) > 0 {
				add(t.Name+"_pkey", "btree", t.PrimaryKey, true, true)
			}
			for _, c := range t.Columns {
				if strings.HasPrefix(c.Name, "_") || !c.Unique || c.PrimaryKey {
					continue
				}
				add(fmt.Sprintf("%s_%s_key", t.Name, c.Name), "btree", []string{c.Name}, true, false)
			}
			for _, idx := range t.Indexes {
				if isImplicitIndexName(idx.Name) {
					continue
				}
				add(idx.Name, idx.Method, idx.Columns, false, false)
			}
		}
	}
	return indexes
}

func isImplicitIndexName(name string) bool {
	lower := strings.ToLower(name)
	for _, prefix := range catalogImplicitIndexPrefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

// catalogColumnOrdinals numbers a table's columns from 1 as pg_attribute
// does, leaving out the engine's own columns (those starting with _).
func catalogColumnOrdinals(t executor.SchemaTable) map[string]int64 {
	ordinals := make(map[string]int64, len(t.Columns))
	ordinal := int64(1)
	for _, c := range t.Columns {
		if strings.HasPrefix(c.Name, "_") {
			continue
		}
		ordinals[c.Name] = ordinal
		ordinal++
	}
	return ordinals
}

func syntheticIndexOID(domainIndex, tableIndex, indexOrdinal int) int64 {
	return 200000 + int64(domainIndex*1000) + int64(tableIndex*50) + int64(indexOrdinal)
}

// catalogPgIndex answers pg_catalog.pg_index, optionally filtered by
// indrelid.
func (server *Server) catalogPgIndex(sql string) (interceptResult, bool) {
	indexes := catalogIndexes(server.engine.SchemaSnapshot(nil))
	relID, filtered := parseCatalogInt64Filter(sql, reCatalogIndexRelIDFilter)
	rows := make([]map[string]ast.Literal, 0, len(indexes))
	for _, idx := range indexes {
		if filtered && idx.relOID != relID {
			continue
		}
		zeros := strings.TrimSpace(strings.Repeat("0 ", len(idx.ordinals)))
		rows = append(rows, map[string]ast.Literal{
			"indexrelid":          lit(idx.oid),
			"indrelid":            lit(idx.relOID),
			"indnatts":            lit(int64(len(idx.ordinals))),
			"indnkeyatts":         lit(int64(len(idx.ordinals))),
			"indisunique":         litB(idx.unique),
			"indnullsnotdistinct": litB(false),
			"indisprimary":        litB(idx.primary),
			"indisexclusion":      litB(false),
			"indimmediate":        litB(idx.unique),
			"indisclustered":      litB(false),
			"indisvalid":          litB(true),
			"indcheckxmin":        litB(false),
			"indisready":          litB(true),
			"indislive":           litB(true),
			"indisreplident":      litB(false),
			"indkey":              litS(int2Vector(idx.ordinals)),
			"indcollation":        litS(zeros),
			"indclass":            litS(zeros),
			"indoption":           litS(zeros),
			"indexprs":            {Kind: ast.LiteralNull},
			"indpred":             {Kind: ast.LiteralNull},
		})
	}
	return interceptResult{result: executor.Result{Status: "OK", Rows: rows}, columns: catalogPgIndexColumns}, true
}

// catalogPgIndexes answers the pg_catalog.pg_indexes view, optionally
// filtered by tablename and schemaname.
func (server *Server) catalogPgIndexes(sql string) (interceptResult, bool) {
	indexes := catalogIndexes(server.engine.SchemaSnapshot(nil))
	table, byTable := parseCatalogStringFilter(sql, reCatalogIndexesTableFilter)
	schema, bySchema := parseCatalogStringFilter(sql, reCatalogIndexesSchemaFilter)
	rows := make([]map[string]ast.Literal, 0, len(indexes))
	for _, idx := range indexes {
		if byTable && !strings.EqualFold(idx.table, table) {
			continue
		}
		if bySchema && !strings.EqualFold(idx.schema, schema) {
			continue
		}
		rows = append(rows, map[string]ast.Literal{
			"schemaname": litS(idx.schema),
			"tablename":  litS(idx.table),
			"indexname":  litS(idx.name),
			"tablespace": {Kind: ast.LiteralNull},
			"indexdef":   litS(idx.definition()),
		})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		if a["schemaname"].StringValue != b["schemaname"].StringValue {
			return a["schemaname"].StringValue < b["schemaname"].StringValue
		}
		if a["tablename"].StringValue != b["tablename"].StringValue {
			return a["tablename"].StringValue < b["tablename"].StringValue
		}
		return a["indexname"].StringValue < b["indexname"].StringValue
	})
	return interceptResult{result: executor.Result{Status: "OK", Rows: rows}, columns: catalogPgIndexesColumns}, true
}

// definition is the index's CREATE INDEX statement, as pg_indexes.indexdef
// shows it.
func (idx catalogIndex) definition() string {
	unique := ""
	if idx.unique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON %s.%s USING %s (%s)", unique, idx.name, idx.schema, idx.table, idx.method, strings.Join(idx.columns, ", "))
}

// int2Vector renders column numbers as Postgres prints an int2vector:
// separated by spaces, without braces.
func int2Vector(ordinals []int64) string {
	parts := make([]string, len(ordinals))
	for i, ordinal := range ordinals {
		parts[i] = strconv.FormatInt(ordinal, 10)
	}
	return strings.Join(parts, " ")
}
