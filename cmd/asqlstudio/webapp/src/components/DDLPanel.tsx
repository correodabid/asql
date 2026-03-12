import { useState, useMemo, useCallback } from 'react'
import { IconCopy, IconRefresh, IconShield, IconDownload, IconPlay, IconCheck, IconChevronDown, IconChevronUp, IconCode } from './Icons'

type StatementStatus = 'pending' | 'running' | 'success' | 'error'

export type StatementState = {
  sql: string
  status: StatementStatus
  error?: string
}

type Props = {
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
}

/* ── SQL keyword-aware syntax highlighter ────────────────── */

const SQL_KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
  'DELETE', 'CREATE', 'TABLE', 'ALTER', 'DROP', 'INDEX', 'ADD', 'COLUMN',
  'CONSTRAINT', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'NOT', 'NULL',
  'DEFAULT', 'UNIQUE', 'IF', 'EXISTS', 'BEGIN', 'COMMIT', 'DOMAIN', 'ON',
  'USING', 'INT', 'INTEGER', 'TEXT', 'BOOLEAN', 'FLOAT', 'TIMESTAMP',
  'VARCHAR', 'BIGINT', 'SERIAL', 'JSON', 'VERSIONED', 'CASCADE',
  'AUTO_INCREMENT', 'AUTOINCREMENT',
])

function highlightSQL(sql: string): React.ReactNode[] {
  const tokens = sql.split(/(\s+|[(),;]|'[^']*')/g)
  return tokens.map((token, i) => {
    if (!token) return null
    const upper = token.toUpperCase()
    if (SQL_KEYWORDS.has(upper)) {
      return <span key={i} className="sql-keyword">{token}</span>
    }
    if (token.startsWith("'") && token.endsWith("'")) {
      return <span key={i} className="sql-string">{token}</span>
    }
    if (/^\d+$/.test(token)) {
      return <span key={i} className="sql-number">{token}</span>
    }
    if (token.startsWith('--')) {
      return <span key={i} className="sql-comment">{token}</span>
    }
    return <span key={i}>{token}</span>
  })
}

/* ── Statement grouping ──────────────────────────────────── */

type StatementGroup = {
  label: string
  icon: string
  statements: { sql: string; originalIndex: number; state: StatementState }[]
}

function groupStatements(statements: string[], states: StatementState[]): StatementGroup[] {
  const groups: Map<string, StatementGroup> = new Map()

  statements.forEach((sql, i) => {
    const upper = sql.toUpperCase().trim()
    let label = 'Other'
    let icon = '>'

    if (upper.startsWith('CREATE TABLE')) {
      label = 'Create Tables'
      icon = '+'
    } else if (upper.startsWith('ALTER TABLE')) {
      label = 'Alter Tables'
      icon = '~'
    } else if (upper.startsWith('CREATE INDEX')) {
      label = 'Create Indexes'
      icon = '#'
    } else if (upper.startsWith('DROP')) {
      label = 'Drop Operations'
      icon = '-'
    }

    if (!groups.has(label)) {
      groups.set(label, { label, icon, statements: [] })
    }
    groups.get(label)!.statements.push({
      sql,
      originalIndex: i,
      state: states[i] || { sql, status: 'pending' as const },
    })
  })

  return Array.from(groups.values())
}

/* ── Status badge ────────────────────────────────────────── */

function StatusDot({ status }: { status: StatementStatus }) {
  const cls = `stmt-status-dot ${status}`
  return <span className={cls} />
}

/* ── Main component ──────────────────────────────────────── */

export function DDLPanel({
  ddl,
  ddlStatements,
  statementStates,
  onGenerateDDL,
  onLoadBaseline,
  onSetBaseline,
  onPreviewDiff,
  onApplySafeDiff,
  onRefreshAutoDiff,
  onRefreshAutoDiffApplySafe,
  onExecuteStatement,
  onExecuteAll,
}: Props) {
  const [copied, setCopied] = useState(false)
  const [viewMode, setViewMode] = useState<'raw' | 'grouped'>('grouped')
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set())
  const [copiedIdx, setCopiedIdx] = useState<number | null>(null)

  const groups = useMemo(() => groupStatements(ddlStatements, statementStates), [ddlStatements, statementStates])

  const stats = useMemo(() => {
    const total = statementStates.length
    const success = statementStates.filter(s => s.status === 'success').length
    const errors = statementStates.filter(s => s.status === 'error').length
    const running = statementStates.filter(s => s.status === 'running').length
    return { total, success, errors, running, pending: total - success - errors - running }
  }, [statementStates])

  const copyDDL = async () => {
    await navigator.clipboard.writeText(ddl)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const copyStatement = useCallback(async (sql: string, idx: number) => {
    await navigator.clipboard.writeText(sql)
    setCopiedIdx(idx)
    setTimeout(() => setCopiedIdx(null), 1500)
  }, [])

  const toggleGroup = (label: string) => {
    setCollapsedGroups(prev => {
      const next = new Set(prev)
      if (next.has(label)) next.delete(label)
      else next.add(label)
      return next
    })
  }

  const hasStatements = ddlStatements.length > 0
  const progressPct = stats.total > 0 ? Math.round((stats.success / stats.total) * 100) : 0

  return (
    <div className="ddl-panel-v2">
      {/* ── Toolbar ──────────────────────────── */}
      <div className="ddl-toolbar">
        <div className="ddl-toolbar-left">
          <button className="ddl-action-btn primary" onClick={onGenerateDDL}>
            <IconPlay /> Generate DDL
          </button>
          <span className="ddl-toolbar-divider" />
          <button className="ddl-action-btn" onClick={onLoadBaseline}>
            <IconDownload /> Load Baseline
          </button>
          <button className="ddl-action-btn" onClick={onSetBaseline}>
            <IconShield /> Set as Baseline
          </button>
        </div>
        <div className="ddl-toolbar-right">
          <button className="ddl-action-btn" onClick={onPreviewDiff}>Preview Diff</button>
          <button className="ddl-action-btn safe" onClick={onApplySafeDiff}>
            <IconShield /> Apply Safe
          </button>
          <button className="ddl-action-btn" onClick={onRefreshAutoDiff}>
            <IconRefresh /> Auto-Diff
          </button>
          <button className="ddl-action-btn accent" onClick={onRefreshAutoDiffApplySafe}>
            <IconRefresh /> Auto-Apply
          </button>
        </div>
      </div>

      {/* ── DDL Preview ──────────────────────── */}
      <div className="ddl-preview-section">
        <div className="ddl-preview-header">
          <div className="ddl-preview-title">
            <IconCode />
            <span>Generated DDL</span>
            {hasStatements && (
              <span className="ddl-stmt-count">{ddlStatements.length} statement{ddlStatements.length !== 1 ? 's' : ''}</span>
            )}
          </div>
          <div className="ddl-preview-actions">
            <button
              className={`ddl-view-toggle ${viewMode === 'grouped' ? 'active' : ''}`}
              onClick={() => setViewMode('grouped')}
              title="Grouped view"
            >
              Grouped
            </button>
            <button
              className={`ddl-view-toggle ${viewMode === 'raw' ? 'active' : ''}`}
              onClick={() => setViewMode('raw')}
              title="Raw SQL view"
            >
              Raw
            </button>
            <span className="ddl-toolbar-divider" />
            <button className="ddl-icon-btn" onClick={copyDDL} title="Copy all DDL">
              <IconCopy /> {copied ? 'Copied!' : ''}
            </button>
          </div>
        </div>

        {/* ── Progress bar ──────────────────── */}
        {stats.total > 0 && stats.success > 0 && (
          <div className="ddl-progress-bar">
            <div className="ddl-progress-track">
              <div className="ddl-progress-fill" style={{ width: `${progressPct}%` }} />
            </div>
            <span className="ddl-progress-label">
              {stats.success}/{stats.total} executed
              {stats.errors > 0 && <span className="ddl-progress-errors"> ({stats.errors} failed)</span>}
            </span>
          </div>
        )}

        {/* ── Raw view ───────────────────────── */}
        {viewMode === 'raw' && (
          <pre className="ddl-raw-code">{highlightSQL(ddl)}</pre>
        )}

        {/* ── Grouped view ───────────────────── */}
        {viewMode === 'grouped' && hasStatements && (
          <div className="ddl-groups">
            {/* Execute All button */}
            {stats.pending > 0 && (
              <button className="ddl-execute-all-btn" onClick={onExecuteAll}>
                <IconPlay /> Execute All ({stats.pending} pending)
              </button>
            )}

            {groups.map(group => {
              const isCollapsed = collapsedGroups.has(group.label)
              const groupSuccess = group.statements.filter(s => s.state.status === 'success').length
              const groupTotal = group.statements.length
              return (
                <div key={group.label} className="ddl-group">
                  <button
                    className="ddl-group-header"
                    onClick={() => toggleGroup(group.label)}
                  >
                    <span className="ddl-group-icon">{group.icon}</span>
                    <span className="ddl-group-label">{group.label}</span>
                    <span className="ddl-group-count">
                      {groupSuccess > 0 && <span className="ddl-group-progress">{groupSuccess}/</span>}
                      {groupTotal}
                    </span>
                    <span className="ddl-group-chevron">
                      {isCollapsed ? <IconChevronDown /> : <IconChevronUp />}
                    </span>
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
                            <button
                              className="ddl-stmt-action-btn"
                              onClick={() => copyStatement(sql, originalIndex)}
                              title="Copy statement"
                            >
                              {copiedIdx === originalIndex ? <IconCheck /> : <IconCopy />}
                            </button>
                            {state.status === 'pending' && (
                              <button
                                className="ddl-stmt-action-btn run"
                                onClick={() => onExecuteStatement(originalIndex)}
                                title="Execute statement"
                              >
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

        {/* ── Empty state ────────────────────── */}
        {viewMode === 'grouped' && !hasStatements && (
          <div className="ddl-empty">
            <IconCode />
            <p>Build your model and click <strong>Generate DDL</strong> to see the SQL statements here.</p>
          </div>
        )}
      </div>
    </div>
  )
}
