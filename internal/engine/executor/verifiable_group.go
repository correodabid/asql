package executor

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/correodabid/asql/internal/engine/planner"
	"github.com/correodabid/asql/internal/engine/sqlerr"
)

// Verifiable groups (ADR 0008, completeness upwards): ALTER TABLE t ADD
// VERIFIABLE GROUP (c1, ...) REFERENCES r (k1, ...) declares that, for every
// row of r, the rows of t whose columns equal that row's primary key form a
// group the ledger's state tree can prove complete, including when it is
// empty. Both tables must be VERIFIABLE and the referenced columns must be
// r's primary key. A group cannot be undone, and it keeps the columns it
// names: DROP COLUMN of a group column and DROP TABLE of a referenced table
// are refused, RENAME COLUMN follows.

// verifiableGroup is a group of the table that holds it.
type verifiableGroup struct {
	columns    []string // in this table, in the referenced key's order
	refTable   string   // same domain
	refColumns []string // the referenced table's primary key, in key order
}

func (g verifiableGroup) same(other verifiableGroup) bool {
	return g.refTable == other.refTable && slices.Equal(g.columns, other.columns)
}

func errInvalidGroup(format string, args ...any) error {
	return sqlerr.New("42P16", "verifiable group: "+fmt.Sprintf(format, args...))
}

// checkVerifiableGroup validates ADD VERIFIABLE GROUP against a domain's
// tables. When queued, tables the same transaction creates are not there
// yet, so a missing table is left to the check at commit (strict false).
func checkVerifiableGroup(domain *domainState, plan planner.Plan, strict bool) error {
	if plan.Operation != planner.OperationAlterTableAddVerifiableGroup {
		return nil
	}
	var table, ref *tableState
	if domain != nil {
		table, ref = domain.tables[plan.TableName], domain.tables[plan.GroupReferencesTable]
	}
	if table == nil || ref == nil {
		if strict {
			return errTableNotFound
		}
		return nil
	}
	group := verifiableGroup{columns: plan.GroupColumns, refTable: plan.GroupReferencesTable, refColumns: plan.GroupReferencesColumns}
	switch {
	case !table.verifiable:
		return errInvalidGroup("table %s is not VERIFIABLE", plan.TableName)
	case !ref.verifiable:
		return errInvalidGroup("referenced table %s is not VERIFIABLE", plan.GroupReferencesTable)
	case !slices.Equal(group.refColumns, primaryKeyColumnsOf(ref)):
		return errInvalidGroup("(%s) is not the primary key of %s, (%s)", strings.Join(group.refColumns, ", "), plan.GroupReferencesTable, strings.Join(primaryKeyColumnsOf(ref), ", "))
	case len(group.columns) != len(group.refColumns):
		return errInvalidGroup("%d columns for a %d-column key", len(group.columns), len(group.refColumns))
	}
	for i, column := range group.columns {
		def, ok := table.columnDefinitions[column]
		if !ok {
			return sqlerr.New("42703", fmt.Sprintf("verifiable group: column %s does not exist in table %s", column, plan.TableName))
		}
		if refDef := ref.columnDefinitions[group.refColumns[i]]; def.Type != refDef.Type {
			return errInvalidGroup("column %s is %s but %s.%s is %s", column, def.Type, plan.GroupReferencesTable, group.refColumns[i], refDef.Type)
		}
	}
	for _, existing := range table.verifiableGroups {
		if existing.same(group) {
			return errInvalidGroup("table %s already has the group (%s) REFERENCES %s", plan.TableName, strings.Join(group.columns, ", "), group.refTable)
		}
	}
	return nil
}

// applyAddVerifiableGroup runs ADD VERIFIABLE GROUP on a COW-cloned table.
func (engine *Engine) applyAddVerifiableGroup(domainState *domainState, plan planner.Plan, clonedTables map[string]struct{}) error {
	if !engine.catalog.HasTable(plan.DomainName, plan.TableName) {
		return errTableNotFound
	}
	if err := checkVerifiableGroup(domainState, plan, true); err != nil {
		return err
	}
	table := cowTable(domainState, plan.DomainName, plan.TableName, clonedTables)
	table.verifiableGroups = append(slices.Clone(table.verifiableGroups), verifiableGroup{
		columns:    slices.Clone(plan.GroupColumns),
		refTable:   plan.GroupReferencesTable,
		refColumns: slices.Clone(plan.GroupReferencesColumns),
	})
	sortVerifiableGroups(table.verifiableGroups)
	table.lastMutationTS = engine.logicalTS
	return nil
}

// cowTable returns the domain's table, cloned once per batch.
func cowTable(domainState *domainState, domain, name string, clonedTables map[string]struct{}) *tableState {
	table := domainState.tables[name]
	key := domain + "." + name
	if clonedTables == nil {
		table = cloneTableState(table)
		domainState.tables[name] = table
	} else if _, already := clonedTables[key]; !already {
		table = cloneTableState(table)
		domainState.tables[name] = table
		clonedTables[key] = struct{}{}
	}
	return table
}

func sortVerifiableGroups(groups []verifiableGroup) {
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].refTable != groups[j].refTable {
			return groups[i].refTable < groups[j].refTable
		}
		return strings.Join(groups[i].columns, "\x00") < strings.Join(groups[j].columns, "\x00")
	})
}

// checkDropGroupColumn refuses DROP COLUMN of a column a group of table
// names.
func checkDropGroupColumn(table *tableState, tableName, column string) error {
	for _, g := range table.verifiableGroups {
		if slices.Contains(g.columns, column) {
			return errInvalidGroup("column %s of %s is in the group (%s) REFERENCES %s", column, tableName, strings.Join(g.columns, ", "), g.refTable)
		}
	}
	return nil
}

// checkDropGroupReference refuses DROP TABLE of a table another table's
// group references, even with CASCADE: a group cannot be undone.
func checkDropGroupReference(domainState *domainState, tableName string) error {
	for otherName, other := range domainState.tables {
		if otherName == tableName || other == nil {
			continue
		}
		for _, g := range other.verifiableGroups {
			if g.refTable == tableName {
				return errInvalidGroup("table %s is referenced by the group (%s) of %s", tableName, strings.Join(g.columns, ", "), otherName)
			}
		}
	}
	return nil
}

// renameGroupColumns follows RENAME COLUMN oldCol TO newCol of tableName in
// the groups of tableName (already cloned) and in those of other tables
// that reference it.
func renameGroupColumns(domainState *domainState, domain, tableName, oldCol, newCol string, clonedTables map[string]struct{}) {
	if table := domainState.tables[tableName]; table != nil && len(table.verifiableGroups) > 0 {
		groups := slices.Clone(table.verifiableGroups)
		for i := range groups {
			groups[i].columns = renameInList(groups[i].columns, oldCol, newCol)
			if groups[i].refTable == tableName {
				groups[i].refColumns = renameInList(groups[i].refColumns, oldCol, newCol)
			}
		}
		sortVerifiableGroups(groups)
		table.verifiableGroups = groups
	}
	names := make([]string, 0, len(domainState.tables))
	for name := range domainState.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		other := domainState.tables[name]
		if name == tableName || other == nil {
			continue
		}
		touched := false
		for _, g := range other.verifiableGroups {
			if g.refTable == tableName && slices.Contains(g.refColumns, oldCol) {
				touched = true
			}
		}
		if !touched {
			continue
		}
		other = cowTable(domainState, domain, name, clonedTables)
		groups := slices.Clone(other.verifiableGroups)
		for i := range groups {
			if groups[i].refTable == tableName {
				groups[i].refColumns = renameInList(groups[i].refColumns, oldCol, newCol)
			}
		}
		sortVerifiableGroups(groups)
		other.verifiableGroups = groups
	}
}

// verifiableGroupRef names a table's group, for snapshots.
type verifiableGroupRef struct {
	domain, table string
	group         verifiableGroup
}

// verifiableGroupRefs lists every group, sorted by table and then as the
// table keeps them.
func verifiableGroupRefs(domains map[string]*domainState) []verifiableGroupRef {
	var refs []verifiableGroupRef
	for _, t := range tableRefs(domains, func(t *tableState) bool { return len(t.verifiableGroups) > 0 }) {
		for _, g := range domains[t[0]].tables[t[1]].verifiableGroups {
			refs = append(refs, verifiableGroupRef{domain: t[0], table: t[1], group: g})
		}
	}
	return refs
}

// markVerifiableGroups restores the groups a snapshot recorded.
func markVerifiableGroups(domains map[string]*domainState, refs []verifiableGroupRef) {
	for _, ref := range refs {
		if domain, ok := domains[ref.domain]; ok {
			if table, ok := domain.tables[ref.table]; ok {
				table.verifiableGroups = append(table.verifiableGroups, ref.group)
				sortVerifiableGroups(table.verifiableGroups)
			}
		}
	}
}

func schemaVerifiableGroups(table *tableState) []SchemaVerifiableGroup {
	var out []SchemaVerifiableGroup
	for _, g := range table.verifiableGroups {
		out = append(out, SchemaVerifiableGroup{Columns: slices.Clone(g.columns), References: g.refTable, ReferencedColumns: slices.Clone(g.refColumns)})
	}
	return out
}
