import React, { useCallback, useEffect, useRef, useState } from 'react'
import { api } from '../lib/api'
import { formatCell } from '../lib/sql'
import { TimelineScrubber } from './TimelineScrubber'
import { TimeTravelDiff } from './TimeTravelDiff'
import { CellInspector } from './CellInspector'
import { IconClock, IconCopy, IconExpand, IconRefresh, IconTable, IconTimeline } from './Icons'

// ─── Types ──────────────────────────────────────────────

type View = 'snapshot' | 'diff' | 'history'
type RowStatus = 'added' | 'removed' | 'changed' | 'unchanged'
type TableInfo = { name: string }
type HistoryEntry = { __commit_lsn?: number; __operation?: string; _lsn?: number; _operation?: string; [k: string]: unknown }

// ─── Helpers ────────────────────────────────────────────

function formatBytes(b: number) {
  if (b <= 0) return '—'
  if (b < 1024) return `${b} B`
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
  return `${(b / (1024 * 1024)).toFixed(1)} MB`
}

function rowClass(s: RowStatus) {
  switch (s) {
    case 'added':   return 'te-row-added'
    case 'removed': return 'te-row-removed'
    case 'changed': return 'te-row-changed'
    default: return ''
  }
}

function makeRowKey(row: Record<string, unknown>, keyCol: string) {
  return String(row[keyCol] ?? JSON.stringify(row))
}

function renderCell(v: unknown): React.ReactNode {
  if (v == null) return <span className="cell-null">NULL</span>
  if (typeof v === 'boolean') return <span className="cell-bool">{v ? 'true' : 'false'}</span>
  const isObj = typeof v === 'object'
  const text = isObj ? JSON.stringify(v) : String(v)
  if (isObj) return <span className="cell-json">{text}</span>
  return <>{formatCell(v)}</>
}

// ─── Component ──────────────────────────────────────────

type Props = { domain: string }

export function TimeExplorer({ domain }: Props) {
  const [localDomain, setLocalDomain] = useState(domain)
  const [domains, setDomains]         = useState<string[]>([])
  const [tables, setTables]           = useState<TableInfo[]>([])
  const [selectedTable, setSelectedTable] = useState('')
  const [view, setView]               = useState<View>('snapshot')

  const [maxLSN, setMaxLSN]           = useState(0)
  const [currentLSN, setCurrentLSN]   = useState(0)

  // Snapshot view
  const [snapRows, setSnapRows]       = useState<Record<string, unknown>[]>([])
  const [headRows, setHeadRows]       = useState<Record<string, unknown>[]>([])
  const [columns, setColumns]         = useState<string[]>([])
  const [diffMap, setDiffMap]         = useState<Map<string, RowStatus>>(new Map())
  const [snapLoading, setSnapLoading] = useState(false)

  // Snapshot table UX
  const [sortCol, setSortCol]           = useState<string | null>(null)
  const [sortDir, setSortDir]           = useState<'asc' | 'desc'>('asc')
  const [tableSearch, setTableSearch]   = useState('')
  const [hiddenCols, setHiddenCols]     = useState<Set<string>>(new Set())
  const [detailRowIdx, setDetailRowIdx] = useState<number | null>(null)
  const [showColMenu, setShowColMenu]   = useState(false)
  const [copiedCell, setCopiedCell]     = useState<string | null>(null)
  const [inspectedCell, setInspectedCell] = useState<{ column: string; value: unknown } | null>(null)
  const copyTimeout = useRef<ReturnType<typeof setTimeout>>(undefined)

  // History view
  const [histFilter, setHistFilter]   = useState('')
  const [histRows, setHistRows]       = useState<HistoryEntry[]>([])
  const [histLoading, setHistLoading] = useState(false)

  // Stats
  const [walSize, setWalSize]         = useState(0)
  const [snapSize, setSnapSize]       = useState(0)

  const querySeq = useRef(0)

  // ─── Load helpers ──────────────────────────────────────

  const refreshMaxLSN = useCallback(async () => {
    try {
      const r = await api<{ lsn: number }>('/api/replication/last-lsn', 'GET')
      const lsn = r.lsn || 0
      setMaxLSN(lsn)
      setCurrentLSN(prev => prev === 0 ? lsn : prev)
    } catch { /* ignore */ }
  }, [])

  const loadDomains = useCallback(async () => {
    try {
      const r = await api<{ domains: string[] }>('/api/domains', 'GET')
      setDomains(r.domains || [])
    } catch { /* ignore */ }
  }, [])

  const loadTables = useCallback(async (d: string) => {
    try {
      const r = await api<{ tables: TableInfo[] }>(`/api/schema/tables?domain=${encodeURIComponent(d)}`, 'GET')
      const tbls = r.tables || []
      setTables(tbls)
      if (tbls.length > 0) setSelectedTable(t => t || tbls[0].name)
    } catch { /* ignore */ }
  }, [])

  const loadStats = useCallback(async () => {
    try {
      const r = await api<{ wal_file_size_bytes?: number; snapshot_file_size_bytes?: number }>('/api/engine-stats', 'GET')
      setWalSize(r.wal_file_size_bytes ?? 0)
      setSnapSize(r.snapshot_file_size_bytes ?? 0)
    } catch { /* ignore */ }
  }, [])

  useEffect(() => {
    loadDomains(); refreshMaxLSN(); loadStats()
  }, [loadDomains, refreshMaxLSN, loadStats])

  useEffect(() => {
    setSelectedTable('')
    loadTables(localDomain)
  }, [localDomain, loadTables])

  // ─── Snapshot query ────────────────────────────────────

  const runSnapshot = useCallback(async () => {
    if (!selectedTable || currentLSN === 0 || maxLSN === 0) return
    const seq = ++querySeq.current
    setSnapLoading(true)
    try {
      const sql = `SELECT * FROM ${selectedTable} LIMIT 500;`
      const [snapResp, headResp] = await Promise.all([
        api<{ rows?: Record<string, unknown>[]; columns?: string[] }>('/api/time-travel', 'POST', {
          sql, domains: [localDomain], lsn: currentLSN,
        }),
        api<{ rows?: Record<string, unknown>[]; columns?: string[] }>('/api/time-travel', 'POST', {
          sql, domains: [localDomain], lsn: maxLSN,
        }),
      ])
      if (seq !== querySeq.current) return

      const sr = snapResp.rows || []
      const hr = headResp.rows || []
      const cols: string[] = snapResp.columns || (sr.length > 0 ? Object.keys(sr[0]) : hr.length > 0 ? Object.keys(hr[0]) : [])
      setSnapRows(sr)
      setHeadRows(hr)
      setColumns(cols)

      // Build diff map (snapshot vs head)
      const keyCol = cols[0] ?? ''
      const headMap = new Map(hr.map(r => [makeRowKey(r, keyCol), r]))
      const snapSet = new Set(sr.map(r => makeRowKey(r, keyCol)))
      const dm = new Map<string, RowStatus>()

      for (const r of sr) {
        const k = makeRowKey(r, keyCol)
        const hRow = headMap.get(k)
        if (!hRow) {
          dm.set(k, 'removed') // existed at snapshot but deleted by head
        } else {
          const changed = cols.some(c => JSON.stringify(r[c]) !== JSON.stringify(hRow[c]))
          dm.set(k, changed ? 'changed' : 'unchanged')
        }
      }
      for (const r of hr) {
        const k = makeRowKey(r, keyCol)
        if (!snapSet.has(k)) dm.set(k, 'added') // not yet in snapshot, added after
      }
      setDiffMap(dm)
    } catch { /* ignore */ } finally {
      if (seq === querySeq.current) setSnapLoading(false)
    }
  }, [selectedTable, currentLSN, maxLSN, localDomain])

  useEffect(() => {
    if (view === 'snapshot') runSnapshot()
  }, [view, selectedTable, currentLSN, runSnapshot])

  // ─── History query ─────────────────────────────────────

  const runHistory = useCallback(async () => {
    if (!selectedTable) return
    setHistLoading(true)
    try {
      const where = histFilter.trim() ? ` WHERE ${histFilter.trim()}` : ''
      const sql = `SELECT * FROM ${selectedTable} FOR HISTORY${where} LIMIT 200;`
      const r = await api<{ rows?: Record<string, unknown>[] }>('/api/row-history', 'POST', {
        sql, domains: [localDomain],
      })
      setHistRows((r.rows ?? []) as HistoryEntry[])
    } catch { /* ignore */ } finally {
      setHistLoading(false)
    }
  }, [selectedTable, histFilter, localDomain])

  useEffect(() => {
    if (view === 'history' && selectedTable) runHistory()
  }, [view, selectedTable, runHistory])

  // ─── Diff stats ────────────────────────────────────────

  const addedCount   = [...diffMap.values()].filter(s => s === 'added').length
  const removedCount = [...diffMap.values()].filter(s => s === 'removed').length
  const changedCount = [...diffMap.values()].filter(s => s === 'changed').length
  const atHead       = currentLSN >= maxLSN

  // ─── Table helpers ────────────────────────────────────

  const toggleSort = (col: string) => {
    if (sortCol === col) {
      if (sortDir === 'asc') setSortDir('desc')
      else { setSortCol(null); setSortDir('asc') }
    } else {
      setSortCol(col); setSortDir('asc')
    }
  }

  const toggleHiddenCol = (col: string) => {
    setHiddenCols(prev => {
      const next = new Set(prev)
      if (next.has(col)) {
        next.delete(col)
      } else {
        next.add(col)
      }
      return next
    })
  }

  const copyVal = (val: unknown, key: string) => {
    const text = val == null ? '' : typeof val === 'object' ? JSON.stringify(val) : String(val)
    navigator.clipboard.writeText(text).then(() => {
      setCopiedCell(key)
      if (copyTimeout.current) clearTimeout(copyTimeout.current)
      copyTimeout.current = setTimeout(() => setCopiedCell(null), 600)
    })
  }

  // ─── Sidebar ──────────────────────────────────────────

  const sidebar = (
    <div className="te-sidebar">
      {/* Domain */}
      <div className="te-sidebar-section">
        <div className="te-sidebar-label">Domain</div>
        <select
          className="te-domain-select"
          value={localDomain}
          onChange={e => setLocalDomain(e.target.value)}
        >
          {domains.map(d => <option key={d} value={d}>{d}</option>)}
          {!domains.includes(localDomain) && <option value={localDomain}>{localDomain}</option>}
        </select>
      </div>

      {/* Tables */}
      <div className="te-sidebar-section te-sidebar-grow">
        <div className="te-sidebar-label">
          Tables
          <button className="te-icon-btn" onClick={() => loadTables(localDomain)} title="Refresh">
            <IconRefresh />
          </button>
        </div>
        <div className="te-table-list">
          {tables.map(t => (
            <button
              key={t.name}
              className={`te-table-item ${selectedTable === t.name ? 'active' : ''}`}
              onClick={() => setSelectedTable(t.name)}
            >
              <IconTable />
              <span>{t.name}</span>
            </button>
          ))}
          {tables.length === 0 && <div className="te-empty-small">No tables</div>}
        </div>
      </div>

      {/* Stats */}
      <div className="te-sidebar-section te-stats-section">
        <div className="te-sidebar-label">Stats</div>
        <div className="te-stat-row">
          <span className="te-stat-key">Head LSN</span>
          <span className="te-stat-val mono">{maxLSN.toLocaleString()}</span>
        </div>
        {!atHead && (
          <>
            <div className="te-stat-row">
              <span className="te-stat-key">Selected LSN</span>
              <span className="te-stat-val mono te-stat-past">{currentLSN.toLocaleString()}</span>
            </div>
            <div className="te-stat-row">
              <span className="te-stat-key">WAL delta</span>
              <span className="te-stat-val mono te-stat-past">
                {(maxLSN - currentLSN).toLocaleString()} records
              </span>
            </div>
          </>
        )}
        {walSize > 0 && (
          <div className="te-stat-row">
            <span className="te-stat-key">WAL size</span>
            <span className="te-stat-val">{formatBytes(walSize)}</span>
          </div>
        )}
        {snapSize > 0 && (
          <div className="te-stat-row">
            <span className="te-stat-key">Snap size</span>
            <span className="te-stat-val">{formatBytes(snapSize)}</span>
          </div>
        )}
        {!atHead && (addedCount + removedCount + changedCount > 0) && (
          <div className="te-diff-summary">
            {addedCount > 0 && <span className="te-diff-chip te-diff-chip-add">+{addedCount}</span>}
            {changedCount > 0 && <span className="te-diff-chip te-diff-chip-chg">~{changedCount}</span>}
            {removedCount > 0 && <span className="te-diff-chip te-diff-chip-del">−{removedCount}</span>}
            <span className="te-diff-chip-label">since this LSN</span>
          </div>
        )}
      </div>
    </div>
  )

  // ─── Snapshot view ─────────────────────────────────────

  const keyCol      = columns[0] ?? ''
  const headRowMap  = new Map(headRows.map(r => [makeRowKey(r, keyCol), r]))
  const visibleCols = columns.filter(c => !c.startsWith('_') && !hiddenCols.has(c))

  // Filter + sort pipeline
  const rawRows = atHead ? headRows : snapRows
  const filteredRows = tableSearch.trim()
    ? rawRows.filter(row =>
        visibleCols.some(c => String(row[c] ?? '').toLowerCase().includes(tableSearch.toLowerCase()))
      )
    : rawRows
  const sortedRows = sortCol
    ? [...filteredRows].sort((a, b) => {
        const av = a[sortCol], bv = b[sortCol]
        if (av == null && bv == null) return 0
        if (av == null) return sortDir === 'asc' ? -1 : 1
        if (bv == null) return sortDir === 'asc' ? 1 : -1
        if (typeof av === 'number' && typeof bv === 'number') return sortDir === 'asc' ? av - bv : bv - av
        return sortDir === 'asc' ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av))
      })
    : filteredRows

  const detailRowData  = detailRowIdx !== null ? sortedRows[detailRowIdx] : null
  const detailStatus   = detailRowData ? (diffMap.get(makeRowKey(detailRowData, keyCol)) ?? 'unchanged') : 'unchanged'
  const detailHeadData = detailRowData ? headRowMap.get(makeRowKey(detailRowData, keyCol)) : undefined
  const showHeadCol    = detailStatus === 'changed' && !!detailHeadData

  const snapshotView = !selectedTable ? (
    <div className="te-empty-state">
      <IconTable />
      <p>Select a table from the sidebar to explore</p>
    </div>
  ) : snapLoading ? (
    <div className="te-loading-rows">
      {[120, 100, 110, 95, 105].map((w, i) => (
        <div key={i} className="te-shimmer" style={{ width: `${w}%`, height: 28, marginBottom: 4 }} />
      ))}
    </div>
  ) : (
    <div className="te-snapshot-wrap">

      {/* ── Toolbar ── */}
      <div className="te-table-toolbar">
        <input
          className="te-table-search"
          placeholder="Filter rows…"
          value={tableSearch}
          onChange={e => { setTableSearch(e.target.value); setDetailRowIdx(null) }}
        />
        <div className="te-col-menu-wrap">
          <button className="te-col-menu-btn" onClick={() => setShowColMenu(v => !v)}>
            Columns ▾{hiddenCols.size > 0 && ` (${hiddenCols.size} hidden)`}
          </button>
          {showColMenu && (
            <div className="te-col-dropdown">
              {columns.filter(c => !c.startsWith('_')).map(c => (
                <label key={c} className="te-col-toggle">
                  <input type="checkbox" checked={!hiddenCols.has(c)} onChange={() => toggleHiddenCol(c)} />
                  {c}
                </label>
              ))}
            </div>
          )}
        </div>
        <button className="te-icon-btn" onClick={runSnapshot} title="Refresh" disabled={snapLoading}>
          <IconRefresh />
        </button>
        <div className="te-count-bar">
          <span className="te-count-num">{sortedRows.length.toLocaleString()}</span>
          <span>rows</span>
          {!atHead && (
            <>
              {addedCount  > 0 && <span className="te-diff-chip te-diff-chip-add">+{addedCount}</span>}
              {changedCount > 0 && <span className="te-diff-chip te-diff-chip-chg">~{changedCount}</span>}
              {removedCount > 0 && <span className="te-diff-chip te-diff-chip-del">−{removedCount}</span>}
            </>
          )}
          {tableSearch && filteredRows.length < rawRows.length && (
            <span className="te-count-filtered"> of {rawRows.length}</span>
          )}
        </div>
      </div>

      {/* ── Table ── */}
      <div className="te-table-scroll" onClick={() => setShowColMenu(false)}>
        <table className="te-result-table">
          <thead>
            <tr>
              <th style={{ width: 28 }} />
              {!atHead && <th style={{ width: 68 }}>Δ</th>}
              {visibleCols.map(c => (
                <th key={c} className="te-th-sort" onClick={() => toggleSort(c)}>
                  <span className="te-th-inner">
                    {c}
                    <span className={`te-sort-icon ${sortCol === c ? 'active' : ''}`}>
                      {sortCol === c ? (sortDir === 'asc' ? '↑' : '↓') : '↕'}
                    </span>
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row, i) => {
              const rk     = makeRowKey(row, keyCol)
              const status: RowStatus = diffMap.get(rk) ?? 'unchanged'
              const isOpen = detailRowIdx === i
              return (
                <React.Fragment key={i}>
                  <tr className={`${rowClass(status)} ${isOpen ? 'te-active-row' : ''}`}>
                    <td className="te-expand-cell">
                      <button
                        className={`te-expand-btn ${isOpen ? 'open' : ''}`}
                        onClick={() => setDetailRowIdx(isOpen ? null : i)}
                        title={isOpen ? 'Collapse' : 'Expand row'}
                      >
                        {isOpen ? '▾' : '▸'}
                      </button>
                    </td>
                    {!atHead && (
                      <td className="te-col-delta">
                        {status === 'removed' && <span className="te-delta-badge te-delta-del">−del</span>}
                        {status === 'changed' && <span className="te-delta-badge te-delta-chg">~mod</span>}
                        {status === 'added'   && <span className="te-delta-badge te-delta-add">+new</span>}
                      </td>
                    )}
                    {visibleCols.map(c => {
                      const isObj   = typeof row[c] === 'object' && row[c] !== null
                      const cellStr = isObj ? JSON.stringify(row[c]) : String(row[c] ?? '')
                      const isLong  = cellStr.length > 80
                      const cellKey = `${i}-${c}`
                      return (
                        <td
                          key={c}
                          className={copiedCell === cellKey ? 'cell-copied' : ''}
                        >
                          <div className="cell-content-wrap">
                            <span className="cell-value-wrap">{renderCell(row[c])}</span>
                            <span className="cell-actions">
                              <button
                                className="cell-action-btn"
                                onClick={e => { e.stopPropagation(); copyVal(row[c], cellKey) }}
                                title="Copy value"
                              >
                                <IconCopy />
                              </button>
                              {(isObj || isLong) && (
                                <button
                                  className="cell-action-btn"
                                  onClick={e => { e.stopPropagation(); setInspectedCell({ column: c, value: row[c] }) }}
                                  title="Inspect"
                                >
                                  <IconExpand />
                                </button>
                              )}
                            </span>
                          </div>
                        </td>
                      )
                    })}
                  </tr>

                  {isOpen && detailRowData && (
                    <tr className="te-detail-row">
                      <td colSpan={visibleCols.length + (atHead ? 1 : 2)}>
                        <div className="te-detail-panel">

                          <div className="te-detail-header">
                            <span className="te-delta-badge" style={{
                              background: detailStatus === 'changed' ? 'rgba(251,191,36,.15)' : 'rgba(99,102,241,.15)',
                              color: detailStatus === 'changed' ? '#fbbf24' : 'var(--text-muted)',
                              fontSize: 11, padding: '2px 8px',
                            }}>
                              {detailStatus === 'unchanged' ? 'ROW' : detailStatus.toUpperCase()}
                            </span>
                            <span className="mono" style={{ color: 'var(--text-muted)', fontSize: 11 }}>
                              {keyCol}: <strong>{String(detailRowData[keyCol] ?? '—')}</strong>
                            </span>
                            <button
                              className="te-detail-copy-btn"
                              onClick={e => {
                                e.stopPropagation()
                                navigator.clipboard.writeText(JSON.stringify(detailRowData, null, 2))
                                setCopiedCell('__row__')
                                if (copyTimeout.current) clearTimeout(copyTimeout.current)
                                copyTimeout.current = setTimeout(() => setCopiedCell(null), 600)
                              }}
                            >
                              {copiedCell === '__row__' ? '✓ copied!' : 'Copy JSON'}
                            </button>
                          </div>

                          {showHeadCol && (
                            <div className="te-detail-lsn-labels">
                              <span />
                              <span className="te-detail-lsn-tag">@ LSN {currentLSN.toLocaleString()}</span>
                              <span className="te-detail-lsn-tag te-detail-lsn-head">HEAD</span>
                            </div>
                          )}

                          <div className={`te-detail-grid ${showHeadCol ? 'te-detail-grid-3' : 'te-detail-grid-2'}`}>
                            {columns.filter(c => !c.startsWith('_')).map(c => {
                              const snapVal    = detailRowData[c]
                              const headVal    = detailHeadData?.[c]
                              const fieldDiff  = showHeadCol && JSON.stringify(snapVal) !== JSON.stringify(headVal)
                              return (
                                <div key={c} className={`te-detail-field ${fieldDiff ? 'te-detail-changed' : ''}`}>
                                  <div className="te-detail-key">{c}</div>
                                  <div className="te-detail-snap">
                                    {snapVal == null ? <em className="te-cell-null">null</em> : String(snapVal)}
                                  </div>
                                  {showHeadCol && (
                                    <div className={`te-detail-head ${fieldDiff ? 'te-detail-head-diff' : ''}`}>
                                      {headVal == null ? <em className="te-cell-null">null</em> : String(headVal)}
                                    </div>
                                  )}
                                </div>
                              )
                            })}
                          </div>

                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              )
            })}
            {sortedRows.length === 0 && (
              <tr>
                <td colSpan={visibleCols.length + (atHead ? 1 : 2)} className="te-no-rows">
                  {tableSearch ? 'No rows match the filter' : 'No rows at this LSN'}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )

  // ─── History view ──────────────────────────────────────

  const historyView = !selectedTable ? (
    <div className="te-empty-state">
      <IconClock />
      <p>Select a table to view row history</p>
    </div>
  ) : (
    <div className="te-history-wrap">
      <div className="te-history-filter-bar">
        <input
          className="te-filter-input"
          placeholder={`WHERE id = '...' (press Enter to run)`}
          value={histFilter}
          onChange={e => setHistFilter(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && runHistory()}
        />
        <button className="toolbar-btn" onClick={runHistory} disabled={histLoading}>
          {histLoading ? 'Loading…' : 'Run'}
        </button>
        <button className="te-icon-btn" onClick={() => { setHistFilter(''); setHistRows([]) }} title="Clear">
          ✕
        </button>
      </div>

      {histRows.length === 0 && !histLoading && (
        <div className="te-empty-state">
          <IconTimeline />
          <p>Enter a filter and press <kbd>Enter</kbd> to view the change history for matching rows.</p>
          <p className="te-empty-hint">INSERT → UPDATE → DELETE are shown chronologically in order.</p>
        </div>
      )}

      {histRows.length > 0 && (
        <div className="te-history-scroll">
          <div className="te-history-timeline">
            {histRows.map((entry, i) => {
                const op = String(entry.__operation || entry._operation || '').toLowerCase()
                const lsn = Number(entry.__commit_lsn || entry._lsn || 0)
                const fields = Object.entries(entry).filter(([k]) => k !== '__operation' && k !== '__commit_lsn' && !k.startsWith('_'))
              const isLast = i === histRows.length - 1
              return (
                <div key={i} className="te-history-entry">
                  <div className="te-history-spine">
                    <div className={`te-history-dot te-op-${op}`} />
                    {!isLast && <div className="te-history-line" />}
                  </div>
                  <div className={`te-history-card te-history-card-${op}`}>
                    <div className="te-history-card-header">
                      <span className={`te-op-badge te-op-${op}`}>{op.toUpperCase()}</span>
                      <span className="te-history-lsn mono">LSN {lsn.toLocaleString()}</span>
                    </div>
                    <div className="te-history-fields">
                      {fields.slice(0, 8).map(([k, v]) => (
                        <div key={k} className="te-history-field">
                          <span className="te-field-key">{k}</span>
                          <span className="te-field-val">
                            {v == null
                              ? <em className="te-null">null</em>
                              : String(v).length > 90
                                ? String(v).slice(0, 90) + '…'
                                : String(v)}
                          </span>
                        </div>
                      ))}
                      {fields.length > 8 && (
                        <div className="te-history-more">+{fields.length - 8} more fields</div>
                      )}
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )

  // ─── Diff view ─────────────────────────────────────────

  const diffView = !selectedTable ? (
    <div className="te-empty-state">
      <IconTable />
      <p>Select a table to compare two LSN snapshots</p>
    </div>
  ) : (
    <TimeTravelDiff
      domain={localDomain}
      tableName={selectedTable}
      maxLSN={maxLSN}
      onClose={() => setView('snapshot')}
    />
  )

  // ─── Root ──────────────────────────────────────────────

  return (
    <div className="te-layout">
      {sidebar}

      <div className="te-main">
        {/* Timeline scrubber — full width */}
        {maxLSN > 0
          ? (
            <TimelineScrubber
              maxLSN={maxLSN}
              currentLSN={currentLSN || maxLSN}
              domain={localDomain}
              onScrub={lsn => setCurrentLSN(lsn)}
              onRefresh={() => { refreshMaxLSN(); loadStats() }}
            />
          )
          : (
            <div className="te-connect-bar">
              <button className="toolbar-btn" onClick={refreshMaxLSN}>
                <IconTimeline /> Connect to engine
              </button>
            </div>
          )}

        {/* Sub-navigation */}
        <div className="te-sub-nav">
          <button className={`te-sub-btn ${view === 'snapshot' ? 'active' : ''}`} onClick={() => setView('snapshot')}>
            Snapshot
          </button>
          <button className={`te-sub-btn ${view === 'diff' ? 'active' : ''}`} onClick={() => setView('diff')}>
            Diff
          </button>
          <button className={`te-sub-btn ${view === 'history' ? 'active' : ''}`} onClick={() => setView('history')}>
            Row History
          </button>
        </div>

        {/* Content area */}
        <div className="te-content">
          {view === 'snapshot' && snapshotView}
          {view === 'diff'     && diffView}
          {view === 'history'  && historyView}
        </div>
      </div>

      {inspectedCell && (
        <CellInspector
          columnName={inspectedCell.column}
          value={inspectedCell.value}
          onClose={() => setInspectedCell(null)}
        />
      )}
    </div>
  )
}
