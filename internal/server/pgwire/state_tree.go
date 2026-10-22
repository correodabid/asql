package pgwire

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/statetree"
)

// stateTreeRecent is how many checkpoints' state trees are kept for proofs;
// older ones are rebuilt from the historical state.
const stateTreeRecent = 16

// stateTreeBuilder keeps the state tree of ADR 0008 for the engine's
// committed head, off the commit path. Its root is a function of the state
// alone: at each checkpoint it looks at every live row of the VERIFIABLE
// tables, so nothing it is told can make it miss a change.
//
// That look is cheap: rows are read in place, by primary key and version
// (_lsn), and only a row whose version changed is built and hashed again.
// A whole table is hashed again when its columns or primary key changed,
// since DROP, RENAME and ADD COLUMN change rows without new versions. Each
// row remembers the groups it belongs to, so only the group leaves whose
// members or referenced row changed are hashed again.
type stateTreeBuilder struct {
	engine *executor.Engine
	key    [32]byte

	mu     sync.Mutex
	lsn    uint64
	built  bool
	gen    uint64
	tree   statetree.Tree
	tables map[string]*stateTreeTable
	recent []stateTreeCheckpoint
	buf    []byte
}

type stateTreeTable struct {
	shape string // primary key and columns
	rows  map[string]*stateTreeRow
	// Verifiable groups (ADR 0008, completeness upwards).
	groupShape string
	groups     []executor.VerifiableGroup
	members    []map[string]map[string]bool // per group: group text -> member row texts
	groupKeys  []map[string]statetree.Hash  // per group: group text -> its key in the tree
	dirty      []map[string]bool            // per group: group texts to hash again
	// regrouped is set when the groups were reset: every referenced row
	// then needs its group leaf, empty ones included.
	regrouped bool
}

type stateTreeRow struct {
	key, leaf statetree.Hash
	lsn, gen  uint64
	pk        []ast.Literal
	groups    []string // the row's group text in each of its table's groups
}

type stateTreeCheckpoint struct {
	lsn  uint64
	tree statetree.Tree
}

func newStateTreeBuilder(engine *executor.Engine, key ledgerdigest.StateKey) *stateTreeBuilder {
	return &stateTreeBuilder{engine: engine, key: key, tables: map[string]*stateTreeTable{}}
}

// State brings the tree to the committed head and returns the head's LSN
// and root, for the checkpointer.
func (b *stateTreeBuilder) State(context.Context) (uint64, [32]byte, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	view := b.engine.VerifiableView()
	if !b.built || view.LSN != b.lsn {
		if err := b.advance(view); err != nil {
			return 0, [32]byte{}, false, err
		}
	}
	return b.lsn, b.tree.Root(), true, nil
}

type groupRef struct {
	table string
	group int
}

func (b *stateTreeBuilder) advance(view executor.VerifiableView) error {
	tree := b.tree
	b.gen++
	tables := view.Tables()

	// Which groups reference each table: its added and removed rows add and
	// remove their group leaves.
	referencedBy := map[string][]groupRef{}
	for _, table := range tables {
		for i, g := range table.Groups {
			ref := table.Domain + "." + g.RefTable
			referencedBy[ref] = append(referencedBy[ref], groupRef{table: table.Domain + "." + table.Name, group: i})
		}
	}
	type refChange struct {
		ref  groupRef
		text string
	}
	var refChanges []refChange
	markReferenced := func(id, text string) {
		for _, r := range referencedBy[id] {
			refChanges = append(refChanges, refChange{r, text})
		}
	}

	seenTables := map[string]bool{}
	for _, table := range tables {
		id := table.Domain + "." + table.Name
		seenTables[id] = true
		shape := strings.Join(table.PrimaryKey, ",") + "|" + strings.Join(table.Columns, ",")
		st := b.tables[id]
		if st != nil && st.shape != shape {
			tree = b.dropTable(tree, id, st, markReferenced)
			st = nil
		}
		if st == nil {
			st = &stateTreeTable{shape: shape, rows: map[string]*stateTreeRow{}}
			b.tables[id] = st
		}
		groupsChanged := st.members == nil || st.groupShape != groupShape(table.Groups)
		if groupsChanged {
			tree = st.resetGroups(tree, table.Groups)
		}

		err := view.Versions(table.Domain, table.Name, func(rv *executor.RowVersion) error {
			b.buf = appendKeyText(b.buf[:0], rv.Key)
			r := st.rows[string(b.buf)]
			if r != nil && r.lsn == rv.LSN && !groupsChanged {
				r.gen = b.gen
				return nil
			}
			row := rv.Row()
			text := string(b.buf)
			if r == nil {
				r = &stateTreeRow{pk: append([]ast.Literal(nil), rv.Key...)}
				st.rows[text] = r
				markReferenced(id, text)
			}
			if r.lsn != rv.LSN || r.leaf == (statetree.Hash{}) {
				key, leaf, err := stateTreeEntry(b.key, table.Domain, table.Name, row)
				if err != nil {
					return err
				}
				tree = tree.Set(key, leaf)
				r.key, r.leaf, r.lsn = key, leaf, rv.LSN
			}
			r.gen = b.gen
			st.regroup(text, r, row.Row)
			return nil
		})
		if err != nil {
			return fmt.Errorf("state tree: %s: %w", id, err)
		}
		for text, r := range st.rows {
			if r.gen != b.gen {
				tree = tree.Delete(r.key)
				st.ungroup(text, r)
				delete(st.rows, text)
				markReferenced(id, text)
			}
		}
	}
	for id, st := range b.tables {
		if !seenTables[id] {
			tree = b.dropTable(tree, id, st, markReferenced)
		}
	}

	for _, c := range refChanges {
		if st := b.tables[c.ref.table]; st != nil && c.ref.group < len(st.dirty) {
			st.dirty[c.ref.group][c.text] = true
		}
	}
	for _, table := range tables {
		tree = b.tables[table.Domain+"."+table.Name].rehashGroups(tree, table, b.tables)
	}
	b.tree, b.lsn, b.built = tree, view.LSN, true
	b.recent = append(b.recent, stateTreeCheckpoint{lsn: view.LSN, tree: tree})
	if len(b.recent) > stateTreeRecent {
		b.recent = b.recent[len(b.recent)-stateTreeRecent:]
	}
	return nil
}

// dropTable removes a table's rows and group leaves from the tree.
func (b *stateTreeBuilder) dropTable(tree statetree.Tree, id string, st *stateTreeTable, markReferenced func(string, string)) statetree.Tree {
	for text, r := range st.rows {
		tree = tree.Delete(r.key)
		markReferenced(id, text)
	}
	for _, keys := range st.groupKeys {
		for _, key := range keys {
			tree = tree.Delete(key)
		}
	}
	delete(b.tables, id)
	return tree
}

func groupShape(groups []executor.VerifiableGroup) string {
	var b strings.Builder
	for _, g := range groups {
		fmt.Fprintf(&b, "%s>%s(%s);", strings.Join(g.Columns, ","), g.RefTable, strings.Join(g.RefColumns, ","))
	}
	return b.String()
}

// resetGroups forgets the table's group leaves when its groups changed;
// every row then joins its groups again during the scan.
func (st *stateTreeTable) resetGroups(tree statetree.Tree, groups []executor.VerifiableGroup) statetree.Tree {
	for _, keys := range st.groupKeys {
		for _, key := range keys {
			tree = tree.Delete(key)
		}
	}
	st.groupShape, st.groups = groupShape(groups), groups
	st.members = make([]map[string]map[string]bool, len(groups))
	st.groupKeys = make([]map[string]statetree.Hash, len(groups))
	st.dirty = make([]map[string]bool, len(groups))
	for i := range groups {
		st.members[i], st.groupKeys[i], st.dirty[i] = map[string]map[string]bool{}, map[string]statetree.Hash{}, map[string]bool{}
	}
	for _, r := range st.rows {
		r.groups = nil
	}
	st.regrouped = true
	return tree
}

// regroup moves a row that was added, changed or regrouped into its
// groups, marking the groups it left and joined.
func (st *stateTreeTable) regroup(text string, r *stateTreeRow, row map[string]ast.Literal) {
	st.ungroup(text, r)
	if len(st.groups) == 0 {
		return
	}
	r.groups = make([]string, len(st.groups))
	for i, g := range st.groups {
		values := make([]ast.Literal, len(g.Columns))
		for j, column := range g.Columns {
			values[j] = row[column]
		}
		groupText := string(appendKeyText(nil, values))
		r.groups[i] = groupText
		if st.members[i][groupText] == nil {
			st.members[i][groupText] = map[string]bool{}
		}
		st.members[i][groupText][text] = true
		st.dirty[i][groupText] = true
	}
}

// ungroup takes a row out of its groups, marking them.
func (st *stateTreeTable) ungroup(text string, r *stateTreeRow) {
	for i, groupText := range r.groups {
		if i >= len(st.members) {
			break
		}
		delete(st.members[i][groupText], text)
		if len(st.members[i][groupText]) == 0 {
			delete(st.members[i], groupText)
		}
		st.dirty[i][groupText] = true
	}
	r.groups = nil
}

// rehashGroups sets the leaf of every marked group that has a referenced
// row, from its members' cached leaves, and removes the leaf of one whose
// referenced row is gone.
func (st *stateTreeTable) rehashGroups(tree statetree.Tree, table executor.VerifiableTable, tables map[string]*stateTreeTable) statetree.Tree {
	for i, g := range st.groups {
		ref := tables[table.Domain+"."+g.RefTable]
		if st.regrouped && ref != nil {
			for text := range ref.rows {
				st.dirty[i][text] = true
			}
		}
		for groupText := range st.dirty[i] {
			var refRow *stateTreeRow
			if ref != nil {
				refRow = ref.rows[groupText]
			}
			if refRow == nil {
				if key, ok := st.groupKeys[i][groupText]; ok {
					tree = tree.Delete(key)
					delete(st.groupKeys[i], groupText)
				}
				continue
			}
			key, err := statetree.GroupKey(table.Domain, table.Name, g.Columns, g.RefTable, refRow.pk)
			if err != nil {
				continue // primary key values always encode
			}
			var members []statetree.Entry
			for text := range st.members[i][groupText] {
				m := st.rows[text]
				members = append(members, statetree.Entry{Key: m.key, Leaf: m.leaf})
			}
			tree = tree.Set(key, statetree.GroupLeaf(members))
			st.groupKeys[i][groupText] = key
		}
		st.dirty[i] = map[string]bool{}
	}
	st.regrouped = false
	return tree
}

// TreeAt returns the state tree at a checkpoint LSN: a recent one as it
// was built, an older one rebuilt from the historical state.
func (b *stateTreeBuilder) TreeAt(ctx context.Context, lsn uint64) (statetree.Tree, executor.VerifiableView, error) {
	b.mu.Lock()
	var tree statetree.Tree
	found := false
	for _, c := range b.recent {
		if c.lsn == lsn {
			tree, found = c.tree, true
		}
	}
	b.mu.Unlock()
	view, err := b.engine.VerifiableViewAt(ctx, lsn)
	if err != nil {
		return statetree.Tree{}, executor.VerifiableView{}, err
	}
	if found {
		return tree, view, nil
	}
	tree, err = buildStateTree(b.key, view)
	return tree, view, err
}

// buildStateTree builds the state tree of a view from scratch, through the
// same code that keeps the head's.
func buildStateTree(key [32]byte, view executor.VerifiableView) (statetree.Tree, error) {
	b := &stateTreeBuilder{key: key, tables: map[string]*stateTreeTable{}}
	if err := b.advance(view); err != nil {
		return statetree.Tree{}, err
	}
	return b.tree, nil
}

// stateTreeEntry returns a row's key and salted leaf.
func stateTreeEntry(stateKey [32]byte, domain, table string, row executor.VerifiableRow) (statetree.Hash, statetree.Hash, error) {
	key, err := statetree.RowKey(domain, table, row.PrimaryKey)
	if err != nil {
		return statetree.Hash{}, statetree.Hash{}, err
	}
	leaf, err := statetree.RowLeaf(statetree.Salt(stateKey, key, row.LSN), row.Row)
	if err != nil {
		return statetree.Hash{}, statetree.Hash{}, err
	}
	return key, leaf, nil
}

// primaryKeyText identifies a row within its table without hashing.
func primaryKeyText(values []ast.Literal) string {
	return string(appendKeyText(nil, values))
}

func appendKeyText(b []byte, values []ast.Literal) []byte {
	for _, v := range values {
		b = append(b, string(v.Kind)...)
		b = append(b, 0)
		b = binary.BigEndian.AppendUint64(b, uint64(v.NumberValue))
		b = binary.BigEndian.AppendUint64(b, uint64(len(v.StringValue)))
		b = append(b, v.StringValue...)
		if v.BoolValue {
			b = append(b, 1)
		}
	}
	return b
}
