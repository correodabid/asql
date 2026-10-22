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
	recent []stateTreeCheckpoint
}

type stateTreeTable struct {
	shape string
	rows  map[string]stateTreeRow // by primary key value text
}

type stateTreeRow struct {
	key statetree.Hash
	lsn uint64
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

func (b *stateTreeBuilder) advance(view executor.VerifiableView) error {
	tree := b.tree
	seenTables := map[string]bool{}
	for _, table := range view.Tables() {
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
			st.rows[id] = stateTreeRow{key: key, lsn: row.LSN}
			return nil
		})
		if err != nil {
			return fmt.Errorf("state tree: %s: %w", id, err)
		}
		for id, row := range st.rows {
			if !seen[id] {
				tree = tree.Delete(row.key)
				delete(st.rows, id)
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

// buildStateTree builds the state tree of a view from scratch.
func buildStateTree(key [32]byte, view executor.VerifiableView) (statetree.Tree, error) {
	var entries []statetree.Entry
	for _, table := range view.Tables() {
		err := view.Rows(table.Domain, table.Name, func(row executor.VerifiableRow) error {
			k, leaf, err := stateTreeEntry(key, table.Domain, table.Name, row)
			if err != nil {
				return err
			}
			entries = append(entries, statetree.Entry{Key: k, Leaf: leaf})
			return nil
		})
		if err != nil {
			return statetree.Tree{}, fmt.Errorf("state tree: %s.%s: %w", table.Domain, table.Name, err)
		}
	}
	return statetree.Build(entries)
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
