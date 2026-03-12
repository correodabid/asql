import type { Dispatch, SetStateAction } from 'react'
import { clone, type SchemaIndex, type SchemaTable } from '../schema'
import { IconPlus, IconTrash } from './Icons'

type Props = {
  activeTable: SchemaTable | null
  updateTable: (updater: (table: SchemaTable) => SchemaTable) => void
  selectedIndex: number
  setSelectedIndex: Dispatch<SetStateAction<number>>
}

const INDEX_METHODS = ['btree', 'hash'] as const

export function IndexEditor({ activeTable, updateTable, selectedIndex, setSelectedIndex }: Props) {
  if (!activeTable) {
    return (
      <div className="index-editor">
        <div className="panel-empty">
          <span className="text-muted">Select a table to manage indexes</span>
        </div>
      </div>
    )
  }

  const indexes = activeTable.indexes || []
  const activeIndex = indexes[selectedIndex] || null

  const addIndex = () => {
    const name = `idx_${activeTable.name}_${indexes.length + 1}`
    const firstCol = activeTable.columns.length > 0 ? activeTable.columns[0].name : ''
    updateTable((table) => ({
      ...table,
      indexes: [
        ...(table.indexes || []),
        { name, columns: firstCol ? [firstCol] : [], method: 'btree' as const },
      ],
    }))
    setSelectedIndex(indexes.length)
  }

  const removeIndex = () => {
    if (indexes.length === 0) return
    updateTable((table) => {
      const next = clone(table)
      const nextIndexes = [...(next.indexes || [])]
      nextIndexes.splice(selectedIndex, 1)
      next.indexes = nextIndexes
      return next
    })
    setSelectedIndex(Math.max(0, selectedIndex - 1))
  }

  const updateIndex = (updater: (idx: SchemaIndex) => SchemaIndex) => {
    updateTable((table) => {
      const next = clone(table)
      const nextIndexes = [...(next.indexes || [])]
      if (nextIndexes[selectedIndex]) {
        nextIndexes[selectedIndex] = updater(nextIndexes[selectedIndex])
      }
      next.indexes = nextIndexes
      return next
    })
  }

  const addColumnToIndex = () => {
    if (!activeIndex) return
    const usedCols = new Set(activeIndex.columns)
    const available = activeTable.columns.filter((c) => !usedCols.has(c.name))
    if (available.length === 0) return
    updateIndex((idx) => ({ ...idx, columns: [...idx.columns, available[0].name] }))
  }

  const removeColumnFromIndex = (colIndex: number) => {
    if (!activeIndex || activeIndex.columns.length <= 1) return
    updateIndex((idx) => {
      const cols = [...idx.columns]
      cols.splice(colIndex, 1)
      return { ...idx, columns: cols }
    })
  }

  const changeIndexColumn = (colIndex: number, value: string) => {
    updateIndex((idx) => {
      const cols = [...idx.columns]
      cols[colIndex] = value
      return { ...idx, columns: cols }
    })
  }

  return (
    <div className="index-editor">
      <div className="editor-section">
        <div className="editor-section-header">
          <label className="editor-label">Indexes</label>
          <div className="editor-actions">
            <button className="icon-btn" onClick={addIndex} title="Add index"><IconPlus /></button>
            <button className="icon-btn danger" onClick={removeIndex} title="Remove index" disabled={indexes.length === 0}><IconTrash /></button>
          </div>
        </div>

        {indexes.length === 0 ? (
          <div className="panel-empty" style={{ padding: '12px 0' }}>
            <span className="text-muted">No indexes defined</span>
          </div>
        ) : (
          <div className="column-list">
            {indexes.map((idx, i) => (
              <button
                key={`${idx.name}-${i}`}
                className={`column-list-item ${i === selectedIndex ? 'active' : ''}`}
                onClick={() => setSelectedIndex(i)}
              >
                <span className="col-indicator">
                  <IconIndex />
                </span>
                <span className="col-list-name">{idx.name || '—'}</span>
                <span className="col-list-type">{idx.method}</span>
              </button>
            ))}
          </div>
        )}
      </div>

      {activeIndex && (
        <div className="editor-section column-detail">
          <label className="editor-label">Index Properties</label>

          <div className="editor-field">
            <label className="editor-field-label">Name</label>
            <input
              className="editor-input mono"
              value={activeIndex.name}
              onChange={(e) => updateIndex((idx) => ({ ...idx, name: e.target.value }))}
            />
          </div>

          <div className="editor-field">
            <label className="editor-field-label">Method</label>
            <select
              className="editor-input"
              value={activeIndex.method}
              onChange={(e) => updateIndex((idx) => ({ ...idx, method: e.target.value as 'hash' | 'btree' }))}
            >
              {INDEX_METHODS.map((m) => (
                <option key={m} value={m}>{m.toUpperCase()}</option>
              ))}
            </select>
          </div>

          <div className="editor-section">
            <div className="editor-section-header">
              <label className="editor-field-label">Indexed Columns</label>
              <div className="editor-actions">
                <button
                  className="icon-btn"
                  onClick={addColumnToIndex}
                  title="Add column to index"
                  disabled={activeIndex.columns.length >= activeTable.columns.length}
                >
                  <IconPlus />
                </button>
              </div>
            </div>

            <div className="index-columns-list">
              {activeIndex.columns.map((col, ci) => (
                <div key={ci} className="index-column-row">
                  <span className="index-column-order">{ci + 1}</span>
                  <select
                    className="editor-input mono"
                    value={col}
                    onChange={(e) => changeIndexColumn(ci, e.target.value)}
                  >
                    {activeTable.columns.map((c) => (
                      <option key={c.name} value={c.name}>{c.name}</option>
                    ))}
                  </select>
                  <button
                    className="icon-btn danger"
                    onClick={() => removeColumnFromIndex(ci)}
                    title="Remove column"
                    disabled={activeIndex.columns.length <= 1}
                  >
                    <IconTrash />
                  </button>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

/* Small index icon */
const IconIndex = () => (
  <svg width={16} height={16} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
    <path d="M4 6h16M4 12h10M4 18h6" />
  </svg>
)
