/**
 * SchemaPanel — unified schema management panel.
 *
 * Layout:
 *   ┌─ Toolbar ─────────────────────────────────────────────┐
 *   │  Generate DDL · Load Baseline · Set Baseline          │
 *   │  Refresh Diff · Apply Safe · Auto-Apply               │
 *   ├─ Change Review (main, scrollable) ────────────────────┤
 *   │  Diff operations grouped by table                     │
 *   ├─ Full DDL (collapsible) ───────────────────────────────┤
 *   │  Grouped statements + execute buttons                 │
 *   └───────────────────────────────────────────────────────┘
 *
 * The diff is the operational view (incremental WAL apply).
 * The full DDL is secondary — bootstrapping or reference.
 */

import { useState, useMemo, useCallback } from 'react'
import type { DiffOperation } from '../schema'
import {
  IconPlay, IconRefresh, IconShield, IconDownload, IconCode,
  IconCopy, IconCheck, IconChevronDown, IconChevronUp,
  IconAlertTriangle, IconTrash, IconPlus,
} from './Icons'

/* ─────────────────── shared types ───────────────────────── */

type StatementStatus = 'pending' | 'running' | 'success' | 'error'

export type StatementState = {
  sql: string
  status: StatementStatus
  error?: string
}

/* ─────────────────── exported props ─────────────────────── */

type Props = {
  // DDL
  ddl: string
  ddlStatements: string[]
  statementStates: StatementState[]
  onGenerateDDL: () => void
  onLoadBaseline: () => void
  onSetBaseline: () => void
  onPreviewDiff: () => void
  onApplySafeDiff: () => void
  onRefreshAutoDiff: () => void
  onRefreshAutoDiffApplySafe: () => void
  onExecuteStatement: (index: number) => void
  onExecuteAll: () => void
  // Diff
  diffSummary: string
  diffSafe: boolean | null
  diffOperations: DiffOperation[]
  diffWarnings: string[]
  onApplySelected: (indices: number[]) => void
}

/* ─────────────────── SQL highlighter ────────────────────── */

const SQL_KW = new Set([
  'SELECT','FROM','WHERE','INSERT','INTO','VALUES','UPDATE','SET',
  'DELETE','CREATE','TABLE','ALTER','DROP','INDEX','ADD','COLUMN',
  'CONSTRAINT','PRIMARY','KEY','FOREIGN','REFERENCES','NOT','NULL',
  'DEFAULT','UNIQUE','IF','EXISTS','BEGIN','COMMIT','DOMAIN','ON',
  'USING','INT','INTEGER','TEXT','BOOLEAN','FLOAT','TIMESTAMP',
  'VARCHAR','BIGINT','SERIAL','JSON','VERSIONED','CASCADE',
  'AUTO_INCREMENT','AUTOINCREMENT',
])

function highlightSQL(sql: string): React.ReactNode[] {
  return sql.split(/(\s+|[(),;]|'[^']*')/g).map((tok, i) => {
    if (!tok) return null
    const up = tok.toUpperCase()
    if (SQL_KW.has(up))                           return <span key={i} className="sql-keyword">{tok}</span>
    if (tok.startsWith("'") && tok.endsWith("'")) return <span key={i} className="sql-string">{tok}</span>
    if (/^\d+$/.test(tok))                        return <span key={i} className="sql-number">{tok}</span>
    return <span key={i}>{tok}</span>
  })
}

/* ─────────────────── DDL statement grouping ─────────────── */

type StmtGroup = {
  label: string
  icon: string
  statements: { sql: string; originalIndex: number; state: StatementState }[]
}

function groupStatements(statements: string[], states: StatementState[]): StmtGroup[] {
  const groups = new Map<string, StmtGroup>()
  statements.forEach((sql, i) => {
    const up = sql.toUpperCase().trim()
    let label = 'Other'; let icon = '>'
    if (up.startsWith('CREATE TABLE'))   { label = 'Create Tables';  icon = '+' }
    else if (up.startsWith('ALTER TABLE'))   { label = 'Alter Tables';   icon = '~' }
    else if (up.startsWith('CREATE INDEX')) { label = 'Create Indexes'; icon = '#' }
    else if (up.startsWith('DROP'))      { label = 'Drop Operations'; icon = '-' }
    if (!groups.has(label)) groups.set(label, { label, icon, statements: [] })
    groups.get(label)!.statements.push({ sql, originalIndex: i, state: states[i] || { sql, status: 'pending' } })
  })
  return Array.from(groups.values())
}

function StatusDot({ status }: { status: StatementStatus }) {
  return <span className={`stmt-status-dot ${status}`} />
}

/* ─────────────────── Diff helpers ───────────────────────── */

const OP_META: Record<string, { label: string; color: string; icon: React.ReactNode }> = {
  add_table:                    { label: 'Add Table',           color: 'safe',     icon: <IconPlus /> },
  add_column:                   { label: 'Add Column',          color: 'safe',     icon: <IconPlus /> },
  add_index:                    { label: 'Add Index',           color: 'safe',     icon: <IconPlus /> },
  modify_column:                { label: 'Modify Column',       color: 'unsafe',   icon: <IconRefresh /> },
  modify_index:                 { label: 'Modify Index',        color: 'breaking', icon: <IconRefresh /> },
  drop_column:                  { label: 'Drop Column',         color: 'breaking', icon: <IconTrash /> },
  drop_table:                   { label: 'Drop Table',          color: 'breaking', icon: <IconTrash /> },
  drop_index:                   { label: 'Drop Index',          color: 'breaking', icon: <IconTrash /> },
  rename_column:                { label: 'Rename Column',       color: 'safe',     icon: <IconRefresh /> },
  add_foreign_key:              { label: 'Add FK',              color: 'safe',     icon: <IconPlus /> },
  drop_foreign_key:             { label: 'Drop FK',             color: 'breaking', icon: <IconTrash /> },
  modify_foreign_key:           { label: 'Modify FK',           color: 'breaking', icon: <IconRefresh /> },
  add_versioned_foreign_key:    { label: 'Add Versioned FK',    color: 'unsafe',   icon: <IconRefresh /> },
  modify_versioned_foreign_key: { label: 'Modify Versioned FK', color: 'unsafe',   icon: <IconRefresh /> },
  drop_versioned_foreign_key:   { label: 'Drop Versioned FK',   color: 'unsafe',   icon: <IconTrash /> },
  add_entity:                   { label: 'Add Entity',          color: 'unsafe',   icon: <IconPlus /> },
  modify_entity:                { label: 'Modify Entity',       color: 'unsafe',   icon: <IconRefresh /> },
  drop_entity:                  { label: 'Drop Entity',         color: 'unsafe',   icon: <IconTrash /> },
}

function opMeta(type: string) {
  return OP_META[type] || { label: type, color: 'neutral', icon: null }
}

function opLevel(op: DiffOperation): 'safe' | 'breaking' | 'unsafe' {
  if (op.safe)     return 'safe'
  if (op.breaking) return 'breaking'
  return 'unsafe'
}

type OpGroup = {
  table: string
  ops: (DiffOperation & { originalIndex: number })[]
  worstLevel: 'safe' | 'breaking' | 'unsafe'
}

function groupOpsByTable(ops: DiffOperation[]): OpGroup[] {
  const map = new Map<string, OpGroup>()
  ops.forEach((op, i) => {
    const key = op.table || '(global)'
    if (!map.has(key)) map.set(key, { table: key, ops: [], worstLevel: 'safe' })
    const g = map.get(key)!
    g.ops.push({ ...op, originalIndex: i })
    const lv = opLevel(op)
    if (lv === 'unsafe') g.worstLevel = 'unsafe'
    else if (lv === 'breaking' && g.worstLevel !== 'unsafe') g.worstLevel = 'breaking'
  })
  return Array.from(map.values())
}

/* ─────────────────── Main component ─────────────────────── */

export function SchemaPanel({
  ddl,
  ddlStatements,
  statementStates,
  onGenerateDDL,
  onLoadBaseline,
  onSetBaseline,
  onPreviewDiff,
  onApplySafeDiff,
  onRefreshAutoDiffApplySafe,
  onExecuteStatement,
  onExecuteAll,
  diffSummary,
  diffSafe,
  diffOperations,
  diffWarnings,
  onApplySelected,
}: Props) {
  /* DDL section */
  const [ddlOpen, setDdlOpen]         = useState(false)
  const [ddlViewMode, setDdlViewMode] = useState<'grouped' | 'raw'>('grouped')
  const [copiedDDL, setCopiedDDL]     = useState(false)
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set())
  const [copiedStmtIdx, setCopiedStmtIdx]     = useState<number | null>(null)

  /* Diff section */
  const [selectedOps, setSelectedOps] = useState<Set<number>>(() => {
    const safe = new Set<number>()
    diffOperations.forEach((op, i) => { if (op.safe) safe.add(i) })
    return safe
  })
  const [collapsedTables, setCollapsedTables] = useState<Set<string>>(new Set())
  const [expandedSql, setExpandedSql]         = useState<Set<number>>(new Set())
  const [copiedOpIdx, setCopiedOpIdx]         = useState<number | null>(null)

  /* ── Derived ──────────────────────────────────────────── */

  const opGroups   = useMemo(() => groupOpsByTable(diffOperations), [diffOperations])
  const stmtGroups = useMemo(() => groupStatements(ddlStatements, statementStates), [ddlStatements, statementStates])

  const diffStats = useMemo(() => ({
    safe:     diffOperations.filter(op => op.safe).length,
    breaking: diffOperations.filter(op => !op.safe && op.breaking).length,
    unsafe:   diffOperations.filter(op => !op.safe && !op.breaking).length,
    total:    diffOperations.length,
  }), [diffOperations])

  const stmtStats = useMemo(() => {
    const total   = statementStates.length
    const success = statementStates.filter(s => s.status === 'success').length
    const errors  = statementStates.filter(s => s.status === 'error').length
    const running = statementStates.filter(s => s.status === 'running').length
    return { total, success, errors, running, pending: total - success - errors - running }
  }, [statementStates])

  const safeSelected = Array.from(selectedOps).filter(i => diffOperations[i]?.safe).length
  const progressPct  = stmtStats.total > 0 ? Math.round((stmtStats.success / stmtStats.total) * 100) : 0

  /* ── Handlers ─────────────────────────────────────────── */

  const toggleOp = useCallback((idx: number) => {
    setSelectedOps(prev => { const n = new Set(prev); if (n.has(idx)) n.delete(idx); else n.add(idx); return n })
  }, [])
  const toggleTable = (t: string) => {
    setCollapsedTables(prev => { const n = new Set(prev); if (n.has(t)) n.delete(t); else n.add(t); return n })
  }
  const toggleSql = useCallback((idx: number) => {
    setExpandedSql(prev => { const n = new Set(prev); if (n.has(idx)) n.delete(idx); else n.add(idx); return n })
  }, [])
  const toggleGroup = (label: string) => {
    setCollapsedGroups(prev => { const n = new Set(prev); if (n.has(label)) n.delete(label); else n.add(label); return n })
  }
  const copyOp = useCallback(async (sql: string, idx: number) => {
    await navigator.clipboard.writeText(sql); setCopiedOpIdx(idx); setTimeout(() => setCopiedOpIdx(null), 1500)
  }, [])
  const copyStmt = useCallback(async (sql: string, idx: number) => {
    await navigator.clipboard.writeText(sql); setCopiedStmtIdx(idx); setTimeout(() => setCopiedStmtIdx(null), 1500)
  }, [])
  const copyAllDDL = async () => {
    await navigator.clipboard.writeText(ddl); setCopiedDDL(true); setTimeout(() => setCopiedDDL(false), 2000)
  }
  const handleApplySelected = useCallback(() => {
    onApplySelected(Array.from(selectedOps).sort((a, b) => a - b))
  }, [onApplySelected, selectedOps])

  /* ── Render ───────────────────────────────────────────── */

  return (
    <div className="schema-panel">

      {/* ══ Toolbar ═══════════════════════════════════════ */}
      <div className="ddl-toolbar schema-panel-toolbar">
        <div className="ddl-toolbar-left">
          <button className="ddl-action-btn primary" onClick={onGenerateDDL}><IconPlay /> Generate DDL</button>
          <span className="ddl-toolbar-divider" />
          <button className="ddl-action-btn" onClick={onLoadBaseline}><IconDownload /> Load Baseline</button>
          <button className="ddl-action-btn" onClick={onSetBaseline}><IconShield /> Set Baseline</button>
        </div>
        <div className="ddl-toolbar-right">
          <button className="ddl-action-btn" onClick={onPreviewDiff}><IconRefresh /> Refresh Diff</button>
          <button className="ddl-action-btn safe" onClick={onApplySafeDiff}><IconShield /> Apply Safe</button>
          <button className="ddl-action-btn accent" onClick={onRefreshAutoDiffApplySafe}><IconRefresh /> Auto-Apply</button>
        </div>
      </div>

      {/* ══ Body (scrollable) ═════════════════════════════ */}
      <div className="schema-panel-body">

        {/* ── Diff summary header ──────────────────── */}
        {(diffSummary || diffStats.total > 0) && (
          <div className={`diff-summary-header ${diffSafe === true ? 'safe' : diffSafe === false ? 'unsafe' : ''}`}>
            <div className="diff-summary-left">
              {diffSafe === true  && <IconCheck />}
              {diffSafe === false && <IconAlertTriangle />}
              <span className="diff-summary-text">{diffSummary}</span>
            </div>
          </div>
        )}

        {/* ── Stats strip ──────────────────────────── */}
        {diffStats.total > 0 && (
          <div className="diff-stats-strip">
            <div className="diff-stat"><span className="diff-stat-value total">{diffStats.total}</span><span className="diff-stat-label">changes</span></div>
            <div className="diff-stat-separator" />
            <div className="diff-stat"><span className="diff-stat-value safe-count">{diffStats.safe}</span><span className="diff-stat-label">safe</span></div>
            <div className="diff-stat"><span className="diff-stat-value breaking-count">{diffStats.breaking}</span><span className="diff-stat-label">breaking</span></div>
            <div className="diff-stat"><span className="diff-stat-value unsafe-count">{diffStats.unsafe}</span><span className="diff-stat-label">unsafe</span></div>
          </div>
        )}

        {/* ── Apply bar ────────────────────────────── */}
        {safeSelected > 0 && (
          <div className="diff-apply-bar">
            <span className="diff-apply-info">{safeSelected} safe op{safeSelected !== 1 ? 's' : ''} selected</span>
            <button className="ddl-action-btn safe" onClick={handleApplySelected}><IconPlay /> Apply Selected</button>
          </div>
        )}

        {/* ── Change ops grouped by table ──────────── */}
        {diffStats.total > 0 && (
          <div className="diff-table-groups">
            {opGroups.map(group => {
              const isCollapsed = collapsedTables.has(group.table)
              return (
                <div key={group.table} className={`diff-table-group ${group.worstLevel === 'safe' ? 'safe' : group.worstLevel === 'breaking' ? 'has-breaking' : 'has-unsafe'}`}>
                  <button className="diff-table-header" onClick={() => toggleTable(group.table)}>
                    <span className={`diff-table-safety ${group.worstLevel}`} />
                    <span className="diff-table-name">{group.table}</span>
                    <span className="diff-table-count">{group.ops.length} change{group.ops.length !== 1 ? 's' : ''}</span>
                    <span className="ddl-group-chevron">{isCollapsed ? <IconChevronDown /> : <IconChevronUp />}</span>
                  </button>
                  {!isCollapsed && (
                    <div className="diff-table-ops">
                      {group.ops.map(op => {
                        const meta = opMeta(op.type)
                        const idx  = op.originalIndex
                        const isExpanded = expandedSql.has(idx)
                        return (
                          <div key={idx} className={`diff-op-card-v2 ${meta.color} ${selectedOps.has(idx) ? 'selected' : ''}`}>
                            <div className="diff-op-top">
                              {op.safe && (
                                <label className="diff-op-checkbox">
                                  <input type="checkbox" checked={selectedOps.has(idx)} onChange={() => toggleOp(idx)} />
                                  <span className="diff-op-checkmark" />
                                </label>
                              )}
                              <span className="diff-op-icon-wrap">{meta.icon}</span>
                              <div className="diff-op-info">
                                <span className="diff-op-label">{meta.label}</span>
                                {op.column && <code className="diff-op-col">.{op.column}</code>}
                              </div>
                              <span className={`diff-op-safety-badge ${opLevel(op)}`}>
                                {op.safe ? 'SAFE' : op.breaking ? 'BREAKING' : 'UNSAFE'}
                              </span>
                              <div className="diff-op-actions">
                                {op.statement && (
                                  <button className="ddl-stmt-action-btn" onClick={() => toggleSql(idx)}>
                                    {isExpanded ? <IconChevronUp /> : <IconChevronDown />}
                                  </button>
                                )}
                                {op.statement && (
                                  <button className="ddl-stmt-action-btn" onClick={() => copyOp(op.statement!, idx)}>
                                    {copiedOpIdx === idx ? <IconCheck /> : <IconCopy />}
                                  </button>
                                )}
                              </div>
                            </div>
                            {op.reason && !op.statement && <div className="diff-op-reason">{op.reason}</div>}
                            {isExpanded && op.statement && <pre className="diff-op-sql">{highlightSQL(op.statement)}</pre>}
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}

        {/* ── Warnings ─────────────────────────────── */}
        {diffWarnings.length > 0 && (
          <div className="diff-warnings-v2">
            <div className="diff-warnings-header"><IconAlertTriangle /><span>{diffWarnings.length} Warning{diffWarnings.length !== 1 ? 's' : ''}</span></div>
            {diffWarnings.map((w, i) => (
              <div key={i} className="diff-warning-v2"><span className="diff-warning-bullet" /><span>{w}</span></div>
            ))}
          </div>
        )}

        {/* ── Empty diff state ─────────────────────── */}
        {diffStats.total === 0 && !diffSummary && (
          <div className="schema-panel-empty">
            <IconRefresh />
            <p>Click <strong>Refresh Diff</strong> to compare the current model against the saved baseline.</p>
          </div>
        )}

        {/* ══ Full DDL — collapsible ════════════════════════ */}
        <div className="schema-ddl-section">
          <button className="schema-ddl-toggle" onClick={() => setDdlOpen(v => !v)}>
            <IconCode />
            <span>Full DDL</span>
            {ddlStatements.length > 0 && (
              <span className="ddl-stmt-count">{ddlStatements.length} statements</span>
            )}
            {stmtStats.success > 0 && (
              <span className="schema-ddl-progress-badge">{stmtStats.success}/{stmtStats.total} executed</span>
            )}
            <span className="ddl-group-chevron">{ddlOpen ? <IconChevronUp /> : <IconChevronDown />}</span>
            {ddlOpen && (
              <div className="schema-ddl-toggle-actions" onClick={e => e.stopPropagation()}>
                <button className={`ddl-view-toggle ${ddlViewMode === 'grouped' ? 'active' : ''}`} onClick={() => setDdlViewMode('grouped')}>Grouped</button>
                <button className={`ddl-view-toggle ${ddlViewMode === 'raw' ? 'active' : ''}`} onClick={() => setDdlViewMode('raw')}>Raw</button>
                <span className="ddl-toolbar-divider" />
                <button className="ddl-icon-btn" onClick={copyAllDDL}><IconCopy />{copiedDDL ? ' Copied!' : ''}</button>
              </div>
            )}
          </button>

          {ddlOpen && (
            <div className="schema-ddl-body">
              {stmtStats.total > 0 && stmtStats.success > 0 && (
                <div className="ddl-progress-bar">
                  <div className="ddl-progress-track"><div className="ddl-progress-fill" style={{ width: `${progressPct}%` }} /></div>
                  <span className="ddl-progress-label">
                    {stmtStats.success}/{stmtStats.total} executed
                    {stmtStats.errors > 0 && <span className="ddl-progress-errors"> ({stmtStats.errors} failed)</span>}
                  </span>
                </div>
              )}

              {ddlViewMode === 'raw' && (
                <pre className="ddl-raw-code">{highlightSQL(ddl)}</pre>
              )}

              {ddlViewMode === 'grouped' && ddlStatements.length > 0 && (
                <div className="ddl-groups">
                  {stmtStats.pending > 0 && (
                    <button className="ddl-execute-all-btn" onClick={onExecuteAll}>
                      <IconPlay /> Execute All ({stmtStats.pending} pending)
                    </button>
                  )}
                  {stmtGroups.map(group => {
                    const isCollapsed  = collapsedGroups.has(group.label)
                    const groupSuccess = group.statements.filter(s => s.state.status === 'success').length
                    return (
                      <div key={group.label} className="ddl-group">
                        <button className="ddl-group-header" onClick={() => toggleGroup(group.label)}>
                          <span className="ddl-group-icon">{group.icon}</span>
                          <span className="ddl-group-label">{group.label}</span>
                          <span className="ddl-group-count">
                            {groupSuccess > 0 && <span className="ddl-group-progress">{groupSuccess}/</span>}
                            {group.statements.length}
                          </span>
                          <span className="ddl-group-chevron">{isCollapsed ? <IconChevronDown /> : <IconChevronUp />}</span>
                        </button>
                        {!isCollapsed && (
                          <div className="ddl-group-items">
                            {group.statements.map(({ sql, originalIndex, state }) => (
                              <div key={originalIndex} className={`ddl-stmt-card ${state.status}`}>
                                <div className="ddl-stmt-left">
                                  <StatusDot status={state.status} />
                                  <span className="ddl-stmt-num">{originalIndex + 1}</span>
                                </div>
                                <div className="ddl-stmt-body">
                                  <code className="ddl-stmt-sql">{highlightSQL(sql)}</code>
                                  {state.status === 'error' && state.error && (
                                    <div className="ddl-stmt-error">{state.error}</div>
                                  )}
                                </div>
                                <div className="ddl-stmt-actions">
                                  <button className="ddl-stmt-action-btn" onClick={() => copyStmt(sql, originalIndex)}>
                                    {copiedStmtIdx === originalIndex ? <IconCheck /> : <IconCopy />}
                                  </button>
                                  {state.status === 'pending' && (
                                    <button className="ddl-stmt-action-btn run" onClick={() => onExecuteStatement(originalIndex)}>
                                      <IconPlay />
                                    </button>
                                  )}
                                </div>
                              </div>
                            ))}
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              )}

              {ddlViewMode === 'grouped' && ddlStatements.length === 0 && (
                <div className="ddl-empty">
                  <IconCode />
                  <p>Click <strong>Generate DDL</strong> to build the full schema SQL.</p>
                </div>
              )}
            </div>
          )}
        </div>

      </div>{/* /schema-panel-body */}
    </div>
  )
}
