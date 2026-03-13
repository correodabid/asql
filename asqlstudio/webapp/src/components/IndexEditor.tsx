import { useEffect, useState } from 'react'
import type { Dispatch, ReactNode, SetStateAction } from 'react'
import { clone, type SchemaIndex, type SchemaTable } from '../schema'
import { IconChevronRight, IconPlus, IconTrash } from './Icons'

type Props = {
  activeTable: SchemaTable | null
  updateTable: (updater: (table: SchemaTable) => SchemaTable) => void
  selectedIndex: number
  setSelectedIndex: Dispatch<SetStateAction<number>>
}

const INDEX_METHODS = ['btree', 'hash'] as const

function SectionPanel({
  label,
  badge,
  open,
  onToggle,
  actions,
  children,
}: {
  label: string
  badge?: string | null
  open: boolean
  onToggle: () => void
  actions?: ReactNode
  children: ReactNode
}) {
  return (
    <div className={`ce-section${open ? ' open' : ''}`}>
      <button className="ce-section-hd" onClick={onToggle}>
        <span className={`ce-chevron${open ? ' open' : ''}`}><IconChevronRight /></span>
        <span className="ce-section-label">{label}</span>
        {badge != null && badge !== '' && <span className="ce-section-badge">{badge}</span>}
        {actions && (
          <span className="ce-section-acts" onClick={(e) => e.stopPropagation()}>
            {actions}
          </span>
        )}
      </button>
      {open && <div className="ce-section-body">{children}</div>}
    </div>
  )
}

export function IndexEditor({ activeTable, updateTable, selectedIndex, setSelectedIndex }: Props) {
  const [listOpen, setListOpen] = useState(true)
  const [propsOpen, setPropsOpen] = useState(true)

  const indexes = activeTable?.indexes || []
  const activeIndex = indexes[selectedIndex] || null

  // Auto-open properties when selection changes
  useEffect(() => {
    if (activeIndex) setPropsOpen(true)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedIndex])

  if (!activeTable) {
    return (
      <div className="column-editor">
        <div className="panel-empty">
          <span className="text-muted">Select a table to manage indexes</span>
        </div>
      </div>
    )
  }

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
    <div className="column-editor">

      {/* ── Index list ──────────────────────────────────────── */}
      <SectionPanel
        label="Indexes"
        badge={indexes.length > 0 ? String(indexes.length) : null}
        open={listOpen}
        onToggle={() => setListOpen((v) => !v)}
        actions={
          <>
            <button className="icon-btn" onClick={addIndex} title="Add index"><IconPlus /></button>
            <button className="icon-btn danger" onClick={removeIndex} title="Remove index" disabled={indexes.length === 0}><IconTrash /></button>
          </>
        }
      >
        {indexes.length === 0 ? (
          <div className="panel-empty" style={{ padding: '8px 0' }}>
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
                <span className="col-indicator"><IconIndex /></span>
                <span className="col-list-name">{idx.name || '—'}</span>
                <span className="col-list-type">{idx.method}</span>
              </button>
            ))}
          </div>
        )}
      </SectionPanel>

      {/* ── Index properties ────────────────────────────────── */}
      {activeIndex && (
        <SectionPanel
          label={activeIndex.name || 'index'}
          badge={activeIndex.method}
          open={propsOpen}
          onToggle={() => setPropsOpen((v) => !v)}
        >
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

          <div className="editor-field">
            <div className="editor-section-header" style={{ marginBottom: 6 }}>
              <label className="editor-field-label">Columns</label>
              <button
                className="icon-btn"
                onClick={addColumnToIndex}
                title="Add column to index"
                disabled={activeIndex.columns.length >= activeTable.columns.length}
              >
                <IconPlus />
              </button>
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
        </SectionPanel>
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

