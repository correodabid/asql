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
// committed head, off the commit path. At each checkpoint it scans the live
// rows of the VERIFIABLE tables and hashes again only the rows whose
// version changed (their _lsn), and a whole table when its columns or
// primary key changed, since DROP, RENAME and ADD COLUMN change rows
// without new versions. Its root is a function of the state alone.
type stateTreeBuilder struct {
	engine *executor.Engine
	key    [32]byte

	mu     sync.Mutex
	lsn    uint64
	built  bool
	tree   statetree.Tree
	tables map[string]*stateTreeTable
	// groups holds the group keys of each table's verifiable groups, by
	// table id and the groups' shape.
	groups map[string]*stateTreeGroups
	recent []stateTreeCheckpoint
}

type stateTreeGroups struct {
	shape string
	keys  map[statetree.Hash]bool
}

type stateTreeTable struct {
	shape string
	rows  map[string]stateTreeRow // by primary key value text
}

type stateTreeRow struct {
	key, leaf statetree.Hash
	lsn       uint64
}

type stateTreeCheckpoint struct {
	lsn  uint64
	tree statetree.Tree
}

func newStateTreeBuilder(engine *executor.Engine, key ledgerdigest.StateKey) *stateTreeBuilder {
	return &stateTreeBuilder{engine: engine, key: key, tables: map[string]*stateTreeTable{}, groups: map[string]*stateTreeGroups{}}
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

func (b *stateTreeBuilder) advance(view executor.VerifiableView) error {
	tree := b.tree
	seenTables := map[string]bool{}
	changed := map[string]bool{} // tables with a row set or removed
	tables := view.Tables()
	for _, table := range tables {
		id := table.Domain + "." + table.Name
		seenTables[id] = true
		shape := strings.Join(table.PrimaryKey, ",") + "|" + strings.Join(table.Columns, ",")
		st := b.tables[id]
		if st == nil || st.shape != shape {
			if st != nil {
				for _, row := range st.rows {
					tree = tree.Delete(row.key)
				}
			}
			st = &stateTreeTable{shape: shape, rows: map[string]stateTreeRow{}}
			b.tables[id] = st
			changed[id] = true
		}
		seen := make(map[string]bool, len(st.rows))
		err := view.Rows(table.Domain, table.Name, func(row executor.VerifiableRow) error {
			id := primaryKeyText(row.PrimaryKey)
			seen[id] = true
			if prev, ok := st.rows[id]; ok && prev.lsn == row.LSN {
				return nil
			}
			key, leaf, err := stateTreeEntry(b.key, table.Domain, table.Name, row)
			if err != nil {
				return err
			}
			tree = tree.Set(key, leaf)
			st.rows[id] = stateTreeRow{key: key, leaf: leaf, lsn: row.LSN}
			changed[table.Domain+"."+table.Name] = true
			return nil
		})
		if err != nil {
			return fmt.Errorf("state tree: %s: %w", id, err)
		}
		for pk, row := range st.rows {
			if !seen[pk] {
				tree = tree.Delete(row.key)
				delete(st.rows, pk)
				changed[id] = true
			}
		}
	}
	for id, st := range b.tables {
		if !seenTables[id] {
			for _, row := range st.rows {
				tree = tree.Delete(row.key)
			}
			delete(b.tables, id)
		}
	}
	tree, err := b.advanceGroups(view, tables, changed, tree)
	if err != nil {
		return err
	}
	b.tree, b.lsn, b.built = tree, view.LSN, true
	b.recent = append(b.recent, stateTreeCheckpoint{lsn: view.LSN, tree: tree})
	if len(b.recent) > stateTreeRecent {
		b.recent = b.recent[len(b.recent)-stateTreeRecent:]
	}
	return nil
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
// same code that keeps the head's, so the two cannot disagree.
func buildStateTree(key [32]byte, view executor.VerifiableView) (statetree.Tree, error) {
	b := &stateTreeBuilder{key: key, tables: map[string]*stateTreeTable{}, groups: map[string]*stateTreeGroups{}}
	if err := b.advance(view); err != nil {
		return statetree.Tree{}, err
	}
	return b.tree, nil
}

// advanceGroups keeps one group leaf per row of each verifiable group's
// referenced table (ADR 0008, completeness upwards). A table's group
// leaves are recomputed when its rows, its referenced tables' rows or its
// groups changed.
func (b *stateTreeBuilder) advanceGroups(view executor.VerifiableView, tables []executor.VerifiableTable, changed map[string]bool, tree statetree.Tree) (statetree.Tree, error) {
	seen := map[string]bool{}
	for _, table := range tables {
		id := table.Domain + "." + table.Name
		if len(table.Groups) == 0 {
			continue
		}
		seen[id] = true
		var shape strings.Builder
		dirty := changed[id]
		for _, g := range table.Groups {
			fmt.Fprintf(&shape, "%s>%s(%s);", strings.Join(g.Columns, ","), g.RefTable, strings.Join(g.RefColumns, ","))
			dirty = dirty || changed[table.Domain+"."+g.RefTable]
		}
		gs := b.groups[id]
		if gs != nil && gs.shape == shape.String() && !dirty {
			continue
		}
		next := map[statetree.Hash]bool{}
		for _, g := range table.Groups {
			// Members by their group values.
			members := map[string][]statetree.Entry{}
			st := b.tables[id]
			err := view.Rows(table.Domain, table.Name, func(row executor.VerifiableRow) error {
				values := make([]ast.Literal, len(g.Columns))
				for i, column := range g.Columns {
					values[i] = row.Row[column]
				}
				cached := st.rows[primaryKeyText(row.PrimaryKey)]
				members[primaryKeyText(values)] = append(members[primaryKeyText(values)], statetree.Entry{Key: cached.key, Leaf: cached.leaf})
				return nil
			})
			if err != nil {
				return tree, fmt.Errorf("state tree: groups of %s: %w", id, err)
			}
			// One leaf per referenced row, empty when it has no members.
			err = view.Rows(table.Domain, g.RefTable, func(ref executor.VerifiableRow) error {
				key, err := statetree.GroupKey(table.Domain, table.Name, g.Columns, g.RefTable, ref.PrimaryKey)
				if err != nil {
					return err
				}
				tree = tree.Set(key, statetree.GroupLeaf(members[primaryKeyText(ref.PrimaryKey)]))
				next[key] = true
				return nil
			})
			if err != nil {
				return tree, fmt.Errorf("state tree: groups of %s: %w", id, err)
			}
		}
		if gs != nil {
			for key := range gs.keys {
				if !next[key] {
					tree = tree.Delete(key)
				}
			}
		}
		b.groups[id] = &stateTreeGroups{shape: shape.String(), keys: next}
	}
	for id, gs := range b.groups {
		if !seen[id] {
			for key := range gs.keys {
				tree = tree.Delete(key)
			}
			delete(b.groups, id)
		}
	}
	return tree, nil
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
	var b []byte
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
	return string(b)
}
