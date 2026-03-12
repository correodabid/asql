import { useState, useMemo, useCallback } from 'react'
import type { DiffOperation } from '../schema'
import { IconCheck, IconShield, IconCopy, IconChevronDown, IconChevronUp, IconPlay, IconAlertTriangle, IconPlus, IconTrash, IconRefresh } from './Icons'

type Props = {
  diffSummary: string
  diffSafe: boolean | null
  diffOperations: DiffOperation[]
  diffWarnings: string[]
  onApplySelected?: (indices: number[]) => void
  onRefreshDiff?: () => void
}

/* ── Operation type helpers ──────────────────────────────── */

const OP_META: Record<string, { label: string; color: string; icon: React.ReactNode }> = {
  add_table:      { label: 'Add Table',      color: 'safe',    icon: <IconPlus /> },
  add_column:     { label: 'Add Column',     color: 'safe',    icon: <IconPlus /> },
  add_index:      { label: 'Add Index',      color: 'safe',    icon: <IconPlus /> },
  modify_column:  { label: 'Modify Column',  color: 'unsafe',  icon: <IconRefresh /> },
  modify_index:   { label: 'Modify Index',   color: 'unsafe',  icon: <IconRefresh /> },
  drop_column:    { label: 'Drop Column',    color: 'unsafe',  icon: <IconTrash /> },
  drop_table:     { label: 'Drop Table',     color: 'unsafe',  icon: <IconTrash /> },
  drop_index:     { label: 'Drop Index',     color: 'unsafe',  icon: <IconTrash /> },
}

function opMeta(type: string) {
  return OP_META[type] || { label: type, color: 'neutral', icon: null }
}

/* ── SQL keyword highlighting (shared) ───────────────────── */

const SQL_KW = new Set([
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
  'DELETE', 'CREATE', 'TABLE', 'ALTER', 'DROP', 'INDEX', 'ADD', 'COLUMN',
  'CONSTRAINT', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'NOT', 'NULL',
  'DEFAULT', 'UNIQUE', 'IF', 'EXISTS', 'BEGIN', 'COMMIT', 'DOMAIN', 'ON',
  'USING', 'INT', 'INTEGER', 'TEXT', 'BOOLEAN', 'FLOAT', 'TIMESTAMP',
  'VARCHAR', 'BIGINT', 'SERIAL', 'JSON', 'VERSIONED', 'CASCADE',
  'AUTO_INCREMENT', 'AUTOINCREMENT',
])

function highlightSQL(sql: string): React.ReactNode[] {
  return sql.split(/(\s+|[(),;]|'[^']*')/g).map((tok, i) => {
    if (!tok) return null
    const up = tok.toUpperCase()
    if (SQL_KW.has(up)) return <span key={i} className="sql-keyword">{tok}</span>
    if (tok.startsWith("'") && tok.endsWith("'")) return <span key={i} className="sql-string">{tok}</span>
    if (/^\d+$/.test(tok)) return <span key={i} className="sql-number">{tok}</span>
    return <span key={i}>{tok}</span>
  })
}

/* ── Group ops by table ──────────────────────────────────── */

type OpGroup = {
  table: string
  ops: (DiffOperation & { originalIndex: number })[]
  allSafe: boolean
}

function groupByTable(ops: DiffOperation[]): OpGroup[] {
  const map = new Map<string, OpGroup>()
  ops.forEach((op, i) => {
    const key = op.table || '(global)'
    if (!map.has(key)) {
      map.set(key, { table: key, ops: [], allSafe: true })
    }
    const g = map.get(key)!
    g.ops.push({ ...op, originalIndex: i })
    if (!op.safe) g.allSafe = false
  })
  return Array.from(map.values())
}

/* ── Main Component ──────────────────────────────────────── */

export function DiffPanel({ diffSummary, diffSafe, diffOperations, diffWarnings, onApplySelected, onRefreshDiff }: Props) {
  const [collapsedTables, setCollapsedTables] = useState<Set<string>>(new Set())
  const [selectedOps, setSelectedOps] = useState<Set<number>>(() => {
    // Auto-select all safe operations
    const safe = new Set<number>()
    diffOperations.forEach((op, i) => { if (op.safe) safe.add(i) })
    return safe
  })
  const [expandedSql, setExpandedSql] = useState<Set<number>>(new Set())
  const [copiedIdx, setCopiedIdx] = useState<number | null>(null)

  const groups = useMemo(() => groupByTable(diffOperations), [diffOperations])

  const stats = useMemo(() => {
    const safe = diffOperations.filter(op => op.safe).length
    const unsafe = diffOperations.filter(op => !op.safe).length
    const adds = diffOperations.filter(op => op.type.startsWith('add_')).length
    const modifies = diffOperations.filter(op => op.type.startsWith('modify_')).length
    const drops = diffOperations.filter(op => op.type.startsWith('drop_')).length
    return { safe, unsafe, adds, modifies, drops, total: diffOperations.length }
  }, [diffOperations])

  const toggleTable = (table: string) => {
    setCollapsedTables(prev => {
      const next = new Set(prev)
      if (next.has(table)) next.delete(table)
      else next.add(table)
      return next
    })
  }

  const toggleOp = useCallback((idx: number) => {
    setSelectedOps(prev => {
      const next = new Set(prev)
      if (next.has(idx)) next.delete(idx)
      else next.add(idx)
      return next
    })
  }, [])

  const toggleSql = useCallback((idx: number) => {
    setExpandedSql(prev => {
      const next = new Set(prev)
      if (next.has(idx)) next.delete(idx)
      else next.add(idx)
      return next
    })
  }, [])

  const copyStatement = useCallback(async (sql: string, idx: number) => {
    await navigator.clipboard.writeText(sql)
    setCopiedIdx(idx)
    setTimeout(() => setCopiedIdx(null), 1500)
  }, [])

  const handleApplySelected = useCallback(() => {
    if (onApplySelected) {
      const indices = Array.from(selectedOps).sort((a, b) => a - b)
      onApplySelected(indices)
    }
  }, [onApplySelected, selectedOps])

  // Empty state
  if (diffOperations.length === 0 && diffWarnings.length === 0) {
    return (
      <div className="diff-panel-v2">
        <div className="diff-empty-state">
          <div className="diff-empty-icon">
            <IconShield />
          </div>
          <h3>No Schema Differences</h3>
          <p>{diffSummary || 'Preview diff to compare baseline vs your current model.'}</p>
          {onRefreshDiff && (
            <button className="ddl-action-btn primary" onClick={onRefreshDiff} style={{ marginTop: 12 }}>
              <IconPlay /> Preview Diff
            </button>
          )}
        </div>
      </div>
    )
  }

  const safeSelected = Array.from(selectedOps).filter(i => diffOperations[i]?.safe).length

  return (
    <div className="diff-panel-v2">
      {/* ── Summary Header ───────────────────── */}
      <div className={`diff-summary-header ${diffSafe === true ? 'safe' : diffSafe === false ? 'unsafe' : ''}`}>
        <div className="diff-summary-left">
          {diffSafe === true ? <IconCheck /> : diffSafe === false ? <IconAlertTriangle /> : null}
          <span className="diff-summary-text">{diffSummary}</span>
        </div>
        {onRefreshDiff && (
          <button className="ddl-icon-btn" onClick={onRefreshDiff} title="Refresh diff">
            <IconRefresh />
          </button>
        )}
      </div>

      {/* ── Stats strip ──────────────────────── */}
      <div className="diff-stats-strip">
        <div className="diff-stat">
          <span className="diff-stat-value total">{stats.total}</span>
          <span className="diff-stat-label">changes</span>
        </div>
        <div className="diff-stat">
          <span className="diff-stat-value adds">{stats.adds}</span>
          <span className="diff-stat-label">additions</span>
        </div>
        <div className="diff-stat">
          <span className="diff-stat-value modifies">{stats.modifies}</span>
          <span className="diff-stat-label">modifications</span>
        </div>
        <div className="diff-stat">
          <span className="diff-stat-value drops">{stats.drops}</span>
          <span className="diff-stat-label">removals</span>
        </div>
        <div className="diff-stat-separator" />
        <div className="diff-stat">
          <span className="diff-stat-value safe-count">{stats.safe}</span>
          <span className="diff-stat-label">safe</span>
        </div>
        <div className="diff-stat">
          <span className="diff-stat-value unsafe-count">{stats.unsafe}</span>
          <span className="diff-stat-label">unsafe</span>
        </div>
      </div>

      {/* ── Apply controls ───────────────────── */}
      {onApplySelected && safeSelected > 0 && (
        <div className="diff-apply-bar">
          <span className="diff-apply-info">
            {safeSelected} safe operation{safeSelected !== 1 ? 's' : ''} selected
          </span>
          <button className="ddl-action-btn safe" onClick={handleApplySelected}>
            <IconPlay /> Apply Selected
          </button>
        </div>
      )}

      {/* ── Grouped operations by table ──────── */}
      <div className="diff-table-groups">
        {groups.map(group => {
          const isCollapsed = collapsedTables.has(group.table)
          return (
            <div key={group.table} className={`diff-table-group ${group.allSafe ? 'safe' : 'has-unsafe'}`}>
              <button className="diff-table-header" onClick={() => toggleTable(group.table)}>
                <span className={`diff-table-safety ${group.allSafe ? 'safe' : 'unsafe'}`} />
                <span className="diff-table-name">{group.table}</span>
                <span className="diff-table-count">{group.ops.length} change{group.ops.length !== 1 ? 's' : ''}</span>
                <span className="ddl-group-chevron">
                  {isCollapsed ? <IconChevronDown /> : <IconChevronUp />}
                </span>
              </button>
              {!isCollapsed && (
                <div className="diff-table-ops">
                  {group.ops.map(op => {
                    const meta = opMeta(op.type)
                    const idx = op.originalIndex
                    const isSelected = selectedOps.has(idx)
                    const isExpanded = expandedSql.has(idx)
                    return (
                      <div key={idx} className={`diff-op-card-v2 ${meta.color} ${isSelected ? 'selected' : ''}`}>
                        <div className="diff-op-top">
                          {/* Selection toggle for safe ops */}
                          {op.safe && onApplySelected && (
                            <label className="diff-op-checkbox">
                              <input
                                type="checkbox"
                                checked={isSelected}
                                onChange={() => toggleOp(idx)}
                              />
                              <span className="diff-op-checkmark" />
                            </label>
                          )}

                          <span className="diff-op-icon-wrap">{meta.icon}</span>

                          <div className="diff-op-info">
                            <span className="diff-op-label">{meta.label}</span>
                            {op.column && (
                              <code className="diff-op-col">.{op.column}</code>
                            )}
                          </div>

                          <span className={`diff-op-safety-badge ${op.safe ? 'safe' : 'unsafe'}`}>
                            {op.safe ? 'SAFE' : 'UNSAFE'}
                          </span>

                          {/* Actions */}
                          <div className="diff-op-actions">
                            {op.statement && (
                              <button
                                className="ddl-stmt-action-btn"
                                onClick={() => toggleSql(idx)}
                                title={isExpanded ? 'Hide SQL' : 'Show SQL'}
                              >
                                {isExpanded ? <IconChevronUp /> : <IconChevronDown />}
                              </button>
                            )}
                            {op.statement && (
                              <button
                                className="ddl-stmt-action-btn"
                                onClick={() => copyStatement(op.statement!, idx)}
                                title="Copy SQL"
                              >
                                {copiedIdx === idx ? <IconCheck /> : <IconCopy />}
                              </button>
                            )}
                          </div>
                        </div>

                        {/* Reason for unsafe ops */}
                        {op.reason && !op.statement && (
                          <div className="diff-op-reason">{op.reason}</div>
                        )}

                        {/* Expandable SQL */}
                        {isExpanded && op.statement && (
                          <pre className="diff-op-sql">{highlightSQL(op.statement)}</pre>
                        )}
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
          )
        })}
      </div>

      {/* ── Warnings ─────────────────────────── */}
      {diffWarnings.length > 0 && (
        <div className="diff-warnings-v2">
          <div className="diff-warnings-header">
            <IconAlertTriangle />
            <span>{diffWarnings.length} Warning{diffWarnings.length !== 1 ? 's' : ''}</span>
          </div>
          {diffWarnings.map((w, i) => (
            <div key={`w-${i}`} className="diff-warning-v2">
              <span className="diff-warning-bullet" />
              <span>{w}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
