import { useCallback, useMemo, useState } from 'react'
import type { SchemaColumn, SchemaTable } from '../schema'
import type { TableInfo } from '../types/workspace'
import { IconArrowRight, IconFilter, IconMinus, IconPlus, IconTrash } from './Icons'

type JoinClause = {
  id: string
  type: 'INNER JOIN' | 'LEFT JOIN' | 'RIGHT JOIN' | 'CROSS JOIN'
  table: string
  onLeft: string
  onRight: string
}

type WhereClause = {
  id: string
  column: string
  op: string
  value: string
}

type OrderClause = {
  id: string
  column: string
  dir: 'ASC' | 'DESC'
}

type BuilderState = {
  table: string
  columns: string[]
  joins: JoinClause[]
  wheres: WhereClause[]
  orders: OrderClause[]
  limit: string
  distinct: boolean
}

type Props = {
  tables: TableInfo[]
  getTableSchema: (name: string) => SchemaTable | undefined
  onGenerateSQL: (sql: string) => void
  onClose: () => void
}

let _id = 0
const nextId = () => `qb-${++_id}`

const OPS = ['=', '!=', '<', '>', '<=', '>=', 'LIKE', 'IN', 'IS NULL', 'IS NOT NULL'] as const

function initialState(): BuilderState {
  return { table: '', columns: [], joins: [], wheres: [], orders: [], limit: '100', distinct: false }
}

export function QueryBuilder({ tables, getTableSchema, onGenerateSQL, onClose }: Props) {
  const [state, setState] = useState<BuilderState>(initialState)

  const setField = useCallback(<K extends keyof BuilderState>(key: K, value: BuilderState[K]) => {
    setState((s) => ({ ...s, [key]: value }))
  }, [])

  // All available columns (main table + joined tables)
  const allColumns = useMemo(() => {
    const cols: { table: string; column: SchemaColumn; qualified: string }[] = []
    const addTableCols = (tName: string) => {
      const schema = getTableSchema(tName)
      if (!schema) return
      for (const col of schema.columns) {
        cols.push({ table: tName, column: col, qualified: `${tName}.${col.name}` })
      }
    }
    if (state.table) {
      addTableCols(state.table)
      for (const j of state.joins) {
        if (j.table) addTableCols(j.table)
      }
    }
    return cols
  }, [state.table, state.joins, getTableSchema])

  const mainTableCols = useMemo(() => {
    if (!state.table) return []
    const schema = getTableSchema(state.table)
    return schema?.columns ?? []
  }, [state.table, getTableSchema])

  // Build SQL from state
  const generatedSQL = useMemo(() => {
    if (!state.table) return ''
    const parts: string[] = []
    // SELECT
    const selCols = state.columns.length > 0 ? state.columns.join(', ') : '*'
    parts.push(`SELECT${state.distinct ? ' DISTINCT' : ''} ${selCols}`)
    // FROM
    parts.push(`FROM ${state.table}`)
    // JOINs
    for (const j of state.joins) {
      if (!j.table) continue
      const onClause = j.onLeft && j.onRight ? ` ON ${j.onLeft} = ${j.onRight}` : ''
      parts.push(`${j.type} ${j.table}${onClause}`)
    }
    // WHERE
    const validWheres = state.wheres.filter((w) => w.column && w.op)
    if (validWheres.length > 0) {
      const conds = validWheres.map((w) => {
        if (w.op === 'IS NULL' || w.op === 'IS NOT NULL') return `${w.column} ${w.op}`
        return `${w.column} ${w.op} ${w.value || "''"}`
      })
      parts.push(`WHERE ${conds.join('\n  AND ')}`)
    }
    // ORDER BY
    const validOrders = state.orders.filter((o) => o.column)
    if (validOrders.length > 0) {
      parts.push(`ORDER BY ${validOrders.map((o) => `${o.column} ${o.dir}`).join(', ')}`)
    }
    // LIMIT
    if (state.limit && state.limit !== '0') {
      parts.push(`LIMIT ${state.limit}`)
    }
    return parts.join('\n') + ';'
  }, [state])

  const handleSelectTable = (name: string) => {
    setState({ ...initialState(), table: name })
  }

  const toggleColumn = (qualified: string) => {
    setState((s) => {
      const cols = s.columns.includes(qualified)
        ? s.columns.filter((c) => c !== qualified)
        : [...s.columns, qualified]
      return { ...s, columns: cols }
    })
  }

  const addJoin = () => {
    setField('joins', [...state.joins, { id: nextId(), type: 'INNER JOIN', table: '', onLeft: '', onRight: '' }])
  }

  const updateJoin = (id: string, patch: Partial<JoinClause>) => {
    setField('joins', state.joins.map((j) => (j.id === id ? { ...j, ...patch } : j)))
  }

  const removeJoin = (id: string) => {
    setField('joins', state.joins.filter((j) => j.id !== id))
  }

  const addWhere = () => {
    setField('wheres', [...state.wheres, { id: nextId(), column: '', op: '=', value: '' }])
  }

  const updateWhere = (id: string, patch: Partial<WhereClause>) => {
    setField('wheres', state.wheres.map((w) => (w.id === id ? { ...w, ...patch } : w)))
  }

  const removeWhere = (id: string) => {
    setField('wheres', state.wheres.filter((w) => w.id !== id))
  }

  const addOrder = () => {
    setField('orders', [...state.orders, { id: nextId(), column: '', dir: 'ASC' }])
  }

  const updateOrder = (id: string, patch: Partial<OrderClause>) => {
    setField('orders', state.orders.map((o) => (o.id === id ? { ...o, ...patch } : o)))
  }

  const removeOrder = (id: string) => {
    setField('orders', state.orders.filter((o) => o.id !== id))
  }

  const handleEmit = () => {
    if (generatedSQL) onGenerateSQL(generatedSQL)
  }

  return (
    <div className="qb-overlay" onClick={(e) => { if (e.target === e.currentTarget) onClose() }}>
      <div className="qb-dialog">
        <div className="qb-header">
          <span className="qb-title">Query Builder</span>
          <div style={{ display: 'flex', gap: 6 }}>
            <button className="toolbar-btn primary" onClick={handleEmit} disabled={!state.table}>
              <IconArrowRight /> Use Query
            </button>
            <button className="icon-btn" onClick={onClose} title="Close">x</button>
          </div>
        </div>

        <div className="qb-body">
          {/* Left: Builder form */}
          <div className="qb-form">
            {/* FROM */}
            <div className="qb-section">
              <div className="qb-section-label">FROM Table</div>
              <select
                className="editor-input"
                value={state.table}
                onChange={(e) => handleSelectTable(e.target.value)}
              >
                <option value="">Select a table...</option>
                {tables.map((t) => (
                  <option key={t.name} value={t.name}>{t.name}</option>
                ))}
              </select>
            </div>

            {/* COLUMNS */}
            {state.table && (
              <div className="qb-section">
                <div className="qb-section-header">
                  <span className="qb-section-label">SELECT Columns</span>
                  <label className="qb-toggle-small">
                    <input type="checkbox" checked={state.distinct} onChange={(e) => setField('distinct', e.target.checked)} />
                    DISTINCT
                  </label>
                </div>
                <div className="qb-column-grid">
                  {allColumns.map((c) => (
                    <label key={c.qualified} className={`qb-column-chip ${state.columns.includes(c.qualified) ? 'selected' : ''}`}>
                      <input
                        type="checkbox"
                        checked={state.columns.includes(c.qualified)}
                        onChange={() => toggleColumn(c.qualified)}
                      />
                      <span className="qb-col-name">{c.column.name}</span>
                      <span className="qb-col-type">{c.column.type}</span>
                      {state.joins.length > 0 && (
                        <span className="qb-col-table">{c.table}</span>
                      )}
                    </label>
                  ))}
                  {state.columns.length > 0 && (
                    <button className="qb-clear-cols" onClick={() => setField('columns', [])}>
                      Clear (use *)
                    </button>
                  )}
                </div>
              </div>
            )}

            {/* JOINs */}
            {state.table && (
              <div className="qb-section">
                <div className="qb-section-header">
                  <span className="qb-section-label">JOIN</span>
                  <button className="icon-btn" onClick={addJoin} title="Add join"><IconPlus /></button>
                </div>
                {state.joins.map((j) => (
                  <div key={j.id} className="qb-join-row">
                    <select className="editor-input" value={j.type} onChange={(e) => updateJoin(j.id, { type: e.target.value as JoinClause['type'] })} style={{ width: 130 }}>
                      <option>INNER JOIN</option>
                      <option>LEFT JOIN</option>
                      <option>RIGHT JOIN</option>
                      <option>CROSS JOIN</option>
                    </select>
                    <select className="editor-input" value={j.table} onChange={(e) => updateJoin(j.id, { table: e.target.value })} style={{ flex: 1 }}>
                      <option value="">Table...</option>
                      {tables.filter((t) => t.name !== state.table).map((t) => (
                        <option key={t.name} value={t.name}>{t.name}</option>
                      ))}
                    </select>
                    <span className="qb-on-label">ON</span>
                    <select className="editor-input" value={j.onLeft} onChange={(e) => updateJoin(j.id, { onLeft: e.target.value })} style={{ flex: 1 }}>
                      <option value="">Column...</option>
                      {mainTableCols.map((c) => (
                        <option key={c.name} value={`${state.table}.${c.name}`}>{state.table}.{c.name}</option>
                      ))}
                    </select>
                    <span className="qb-eq">=</span>
                    <select className="editor-input" value={j.onRight} onChange={(e) => updateJoin(j.id, { onRight: e.target.value })} style={{ flex: 1 }}>
                      <option value="">Column...</option>
                      {j.table && (getTableSchema(j.table)?.columns ?? []).map((c) => (
                        <option key={c.name} value={`${j.table}.${c.name}`}>{j.table}.{c.name}</option>
                      ))}
                    </select>
                    <button className="icon-btn danger" onClick={() => removeJoin(j.id)}><IconMinus /></button>
                  </div>
                ))}
              </div>
            )}

            {/* WHERE */}
            {state.table && (
              <div className="qb-section">
                <div className="qb-section-header">
                  <span className="qb-section-label"><IconFilter /> WHERE</span>
                  <button className="icon-btn" onClick={addWhere} title="Add condition"><IconPlus /></button>
                </div>
                {state.wheres.map((w) => (
                  <div key={w.id} className="qb-where-row">
                    <select className="editor-input" value={w.column} onChange={(e) => updateWhere(w.id, { column: e.target.value })} style={{ flex: 1 }}>
                      <option value="">Column...</option>
                      {allColumns.map((c) => (
                        <option key={c.qualified} value={c.qualified}>{c.qualified}</option>
                      ))}
                    </select>
                    <select className="editor-input" value={w.op} onChange={(e) => updateWhere(w.id, { op: e.target.value })} style={{ width: 110 }}>
                      {OPS.map((op) => (
                        <option key={op} value={op}>{op}</option>
                      ))}
                    </select>
                    {w.op !== 'IS NULL' && w.op !== 'IS NOT NULL' && (
                      <input
                        className="editor-input mono"
                        value={w.value}
                        onChange={(e) => updateWhere(w.id, { value: e.target.value })}
                        placeholder="Value..."
                        style={{ flex: 1 }}
                      />
                    )}
                    <button className="icon-btn danger" onClick={() => removeWhere(w.id)}><IconMinus /></button>
                  </div>
                ))}
              </div>
            )}

            {/* ORDER BY */}
            {state.table && (
              <div className="qb-section">
                <div className="qb-section-header">
                  <span className="qb-section-label">ORDER BY</span>
                  <button className="icon-btn" onClick={addOrder} title="Add sort"><IconPlus /></button>
                </div>
                {state.orders.map((o) => (
                  <div key={o.id} className="qb-order-row">
                    <select className="editor-input" value={o.column} onChange={(e) => updateOrder(o.id, { column: e.target.value })} style={{ flex: 1 }}>
                      <option value="">Column...</option>
                      {allColumns.map((c) => (
                        <option key={c.qualified} value={c.qualified}>{c.qualified}</option>
                      ))}
                    </select>
                    <select className="editor-input" value={o.dir} onChange={(e) => updateOrder(o.id, { dir: e.target.value as 'ASC' | 'DESC' })} style={{ width: 80 }}>
                      <option value="ASC">ASC</option>
                      <option value="DESC">DESC</option>
                    </select>
                    <button className="icon-btn danger" onClick={() => removeOrder(o.id)}><IconMinus /></button>
                  </div>
                ))}
              </div>
            )}

            {/* LIMIT */}
            {state.table && (
              <div className="qb-section">
                <div className="qb-section-label">LIMIT</div>
                <input
                  className="editor-input mono"
                  type="number"
                  min="0"
                  value={state.limit}
                  onChange={(e) => setField('limit', e.target.value)}
                  style={{ width: 100 }}
                />
              </div>
            )}

            {/* Reset */}
            {state.table && (
              <button className="toolbar-btn" onClick={() => setState(initialState())} style={{ alignSelf: 'flex-start', marginTop: 8 }}>
                <IconTrash /> Reset
              </button>
            )}
          </div>

          {/* Right: SQL Preview */}
          <div className="qb-preview">
            <div className="qb-section-label">Generated SQL</div>
            <pre className="qb-sql-output">{generatedSQL || '-- Select a table to start building a query'}</pre>
          </div>
        </div>
      </div>
    </div>
  )
}
