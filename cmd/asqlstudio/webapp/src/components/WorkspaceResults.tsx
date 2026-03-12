import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { formatCell } from '../lib/sql'
import { exportCSV, exportJSON, downloadFile } from '../lib/export'
import { useColumnResize } from '../hooks/useColumnResize'
import { useVirtualScroll } from '../hooks/useVirtualScroll'
import type { ExplainPlan, QueryResult } from '../types/workspace'
import type { CellEdit } from '../types/workspace'
import type { SchemaTable } from '../schema'
import { ExplainTree } from './ExplainTree'
import { CellInspector } from './CellInspector'
import { IconCompare, IconCopy, IconExpand, IconEye } from './Icons'
import { RowDiffModal } from './RowDiffModal'

type SortDir = 'asc' | 'desc' | null

type Props = {
  result: QueryResult | null
  results: QueryResult[]
  error: string | null
  loading: boolean
  explainPlan: ExplainPlan | null
  selectedRow: number | null
  onRowClick: (rowIndex: number) => void
  onRowView: (rowIndex: number) => void
  editingCell: CellEdit | null
  onCellDoubleClick: (rowIndex: number, columnName: string) => void
  onCellEditChange: (value: string) => void
  onCellEditCommit: () => void
  onCellEditCancel: () => void
  onContextMenu: (e: React.MouseEvent, rowIndex: number) => void
  onAddRow: () => void
  tableName: string | null
  tableSchema?: SchemaTable
}

const VIRTUAL_THRESHOLD = 200

export function WorkspaceResults({
  result: primaryResult,
  results,
  error,
  loading,
  explainPlan,
  selectedRow,
  onRowClick,
  onRowView,
  editingCell,
  onCellDoubleClick,
  onCellEditChange,
  onCellEditCommit,
  onCellEditCancel,
  onContextMenu,
  onAddRow,
  tableName,
  tableSchema,
}: Props) {
  const [sortCol, setSortCol] = useState<string | null>(null)
  const [sortDir, setSortDir] = useState<SortDir>(null)
  const [copiedCell, setCopiedCell] = useState<string | null>(null)
  const [inspectedCell, setInspectedCell] = useState<{ column: string; value: unknown } | null>(null)
  const [activeResultIdx, setActiveResultIdx] = useState(0)
  const [compareRows, setCompareRows] = useState<number[]>([])
  const [showRowDiff, setShowRowDiff] = useState(false)
  const copyTimeout = useRef<ReturnType<typeof setTimeout>>(undefined)

  const multiResult = results.length > 1
  const result = multiResult ? (results[activeResultIdx] ?? null) : primaryResult

  const columns = result?.columns ?? []
  const resize = useColumnResize(columns)

  // Build column type map from schema
  const columnTypes = useMemo(() => {
    if (!tableSchema) return {} as Record<string, string>
    const map: Record<string, string> = {}
    for (const col of tableSchema.columns) {
      map[col.name] = col.type
    }
    return map
  }, [tableSchema])

  // Sort rows
  const sortedRows = useMemo(() => {
    if (!result || !sortCol || !sortDir) return result?.rows ?? []
    const col = sortCol
    const dir = sortDir === 'asc' ? 1 : -1
    return [...result.rows].sort((a, b) => {
      const va = a[col]
      const vb = b[col]
      if (va === null || va === undefined) return dir
      if (vb === null || vb === undefined) return -dir
      if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir
      return String(va).localeCompare(String(vb)) * dir
    })
  }, [result, sortCol, sortDir])

  const rowCount = sortedRows.length
  const virtualEnabled = rowCount > VIRTUAL_THRESHOLD
  const vs = useVirtualScroll(rowCount, virtualEnabled)

  // Reset compare selection whenever the result set changes
  useEffect(() => {
    setCompareRows([])
    setShowRowDiff(false)
  }, [result])

  const handleCompareClick = useCallback((i: number, e: React.MouseEvent) => {
    e.stopPropagation()
    setCompareRows(prev => {
      if (prev.includes(i)) return prev.filter(r => r !== i)   // unpin
      if (prev.length === 0) return [i]                        // pin first
      if (prev.length >= 2)  return [i]                        // reset to new row
      // Second row picked → open diff immediately
      setShowRowDiff(true)
      return [prev[0], i]
    })
  }, [])

  const handleSort = useCallback((col: string) => {
    setSortCol((prev) => {
      if (prev !== col) {
        setSortDir('asc')
        return col
      }
      setSortDir((d) => {
        if (d === 'asc') return 'desc'
        if (d === 'desc') return null
        return 'asc'
      })
      return col
    })
  }, [])

  const copyCellToClipboard = useCallback((rowIndex: number, col: string, value: unknown) => {
    const text = value === null || value === undefined ? '' : typeof value === 'object' ? JSON.stringify(value) : String(value)
    navigator.clipboard.writeText(text).then(() => {
      const key = `${rowIndex}-${col}`
      setCopiedCell(key)
      if (copyTimeout.current) clearTimeout(copyTimeout.current)
      copyTimeout.current = setTimeout(() => setCopiedCell(null), 600)
    }).catch(() => { /* noop */ })
  }, [])

  const handleExportCSV = useCallback(() => {
    if (!result) return
    const csv = exportCSV(result)
    const name = tableName ? `${tableName}.csv` : 'query_results.csv'
    downloadFile(csv, name, 'text/csv')
  }, [result, tableName])

  const handleExportJSON = useCallback(() => {
    if (!result) return
    const json = exportJSON(result)
    const name = tableName ? `${tableName}.json` : 'query_results.json'
    downloadFile(json, name, 'application/json')
  }, [result, tableName])

  const renderCell = (rowIndex: number, value: unknown, col: string) => {
    const isObj = typeof value === 'object' && value !== null
    const text = isObj ? JSON.stringify(value) : String(value ?? '')
    const isLong = text.length > 80

    let content: React.ReactNode
    if (value === null || value === undefined) {
      content = <span className="cell-null">NULL</span>
    } else if (typeof value === 'boolean') {
      content = <span className="cell-bool">{value ? 'true' : 'false'}</span>
    } else if (isObj) {
      content = <span className="cell-json">{text}</span>
    } else {
      content = <>{formatCell(value)}</>
    }

    return (
      <div className="cell-content-wrap">
        <span className="cell-value-wrap">{content}</span>
        <span className="cell-actions">
          <button
            className="cell-action-btn"
            onClick={(e) => {
              e.stopPropagation()
              copyCellToClipboard(rowIndex, col, value)
            }}
            title="Copy value"
          >
            <IconCopy />
          </button>
          {(isObj || isLong) && (
            <button
              className="cell-action-btn"
              onClick={(e) => {
                e.stopPropagation()
                setInspectedCell({ column: col, value })
              }}
              title="Inspect"
            >
              <IconExpand />
            </button>
          )}
        </span>
      </div>
    )
  }

  const sortArrow = (col: string) => {
    if (sortCol !== col || !sortDir) return null
    return <span className="sort-arrow">{sortDir === 'asc' ? ' \u2191' : ' \u2193'}</span>
  }

  // Visible rows for rendering (virtual or all)
  const visibleRows = virtualEnabled ? sortedRows.slice(vs.startIndex, vs.endIndex) : sortedRows

  return (
    <div className="ws-results">
      {loading && (
        <div className="console-status-bar executing">
          <span className="pulse-dot" /> Executing...
        </div>
      )}

      {error && (
        <div className="console-status-bar error-bar">
          <span className="error-icon">!</span> {error}
        </div>
      )}

      {result && !loading && (
        <>
          {/* Multi-result sub-tabs */}
          {multiResult && (
            <div className="result-sub-tabs">
              {results.map((r, i) => (
                <button
                  key={i}
                  className={`result-sub-tab ${i === activeResultIdx ? 'active' : ''}`}
                  onClick={() => { setActiveResultIdx(i); setSortCol(null); setSortDir(null) }}
                >
                  <span className="result-sub-tab-label">Result {i + 1}</span>
                  <span className="result-sub-tab-meta">
                    {r.rowCount} row{r.rowCount !== 1 ? 's' : ''}
                  </span>
                </button>
              ))}
            </div>
          )}

          <div className="console-status-bar success-bar">
            <span className="result-meta">{result.status}</span>
            <span className="result-meta">
              {result.rowCount} row{result.rowCount !== 1 ? 's' : ''}
            </span>
            <span className="result-meta">{result.duration.toFixed(1)} ms</span>
            {result.route && <span className="result-meta">route: {result.route}</span>}
            {result.asOfLSN !== undefined && (
              <span className="result-meta">LSN: {result.asOfLSN}</span>
            )}
            <div className="result-actions">
              {result.rows.length > 0 && (
                <>
                  <button className="ws-export-btn" onClick={handleExportCSV} title="Export as CSV">
                    CSV
                  </button>
                  <button className="ws-export-btn" onClick={handleExportJSON} title="Export as JSON">
                    JSON
                  </button>
                </>
              )}
              {tableName && (
                <button className="ws-add-row-btn" onClick={onAddRow} title="Generate INSERT template">
                  + Row
                </button>
              )}
            </div>
          </div>

          {/* Compare hint bar – shown when 1 or 2 rows are selected for diff */}
          {compareRows.length === 1 && (
            <div className="compare-hint-bar">
              <span className="compare-hint-icon">⇄</span>
              <span className="compare-hint-text">
                Row <strong>{compareRows[0] + 1}</strong> selected —
                click <span className="compare-hint-icon-inline">⇄</span> on another row to compare
              </span>
              <button className="compare-hint-cancel" onClick={() => setCompareRows([])}>✕ cancel</button>
            </div>
          )}
          {compareRows.length === 2 && !showRowDiff && (
            <div className="compare-hint-bar compare-hint-bar-ready">
              <span className="compare-hint-icon">⇄</span>
              <span className="compare-hint-text">Rows <strong>{compareRows[0] + 1}</strong> &amp; <strong>{compareRows[1] + 1}</strong> selected</span>
              <button className="compare-hint-open" onClick={() => setShowRowDiff(true)}>Compare →</button>
              <button className="compare-hint-cancel" onClick={() => setCompareRows([])}>✕</button>
            </div>
          )}

          {explainPlan ? (
            <ExplainTree
              planShape={explainPlan.planShape}
              accessPlan={explainPlan.accessPlan}
              operation={explainPlan.operation}
              domain={explainPlan.domain}
              table={explainPlan.table}
            />
          ) : result.rows.length > 0 ? (
            <div
              className="console-table-wrap"
              ref={virtualEnabled ? vs.containerRef : undefined}
              onScroll={virtualEnabled ? vs.onScroll : undefined}
            >
              <table className="console-table">
                <thead>
                  <tr>
                    <th className="row-num">#</th>
                    {result.columns.map((col) => (
                      <th
                        key={col}
                        style={{ width: resize.getWidth(col), minWidth: 60 }}
                        onClick={() => handleSort(col)}
                        className="sortable-header"
                      >
                        <div className="th-content">
                          <span className="th-name">
                            {col}{sortArrow(col)}
                          </span>
                          {columnTypes[col] && (
                            <span className="th-type">{columnTypes[col]}</span>
                          )}
                        </div>
                        <div
                          className="resize-handle"
                          onMouseDown={(e) => resize.onMouseDown(col, e)}
                          onClick={(e) => e.stopPropagation()}
                        />
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {virtualEnabled && vs.offsetY > 0 && (
                    <tr style={{ height: vs.offsetY }} aria-hidden />
                  )}
                  {visibleRows.map((row, vi) => {
                    const i = virtualEnabled ? vs.startIndex + vi : vi
                    const isPinned = compareRows.includes(i)
                    const pinIndex = isPinned ? compareRows.indexOf(i) + 1 : null
                    return (
                      <tr
                        key={i}
                        className={[
                          selectedRow === i && !isPinned ? 'selected-row' : '',
                          isPinned ? `compare-pinned-row compare-pin-${pinIndex}` : '',
                          compareRows.length > 0 && !isPinned ? 'compare-mode-dim' : '',
                        ].filter(Boolean).join(' ')}
                        onClick={() => onRowClick(i)}
                        onContextMenu={(e) => onContextMenu(e, i)}
                      >
                        <td className="row-num">
                          <div className="row-num-content">
                            {isPinned
                              ? <span className={`compare-pin-badge compare-pin-badge-${pinIndex}`}>{pinIndex}</span>
                              : <span>{i + 1}</span>
                            }
                            <button
                              className="row-view-btn"
                              onClick={(e) => {
                                e.stopPropagation()
                                onRowView(i)
                              }}
                              title="View row detail"
                            >
                              <IconEye />
                            </button>
                            <button
                              className={`row-compare-btn${isPinned ? ' active' : ''}`}
                              onClick={(e) => handleCompareClick(i, e)}
                              title={
                                isPinned
                                  ? 'Unpin from comparison'
                                  : compareRows.length === 1
                                    ? `Compare with Row ${compareRows[0] + 1}`
                                    : 'Select for row comparison'
                              }
                            >
                              <IconCompare />
                            </button>
                          </div>
                        </td>
                        {result.columns.map((col) => {
                          const isEditing =
                            editingCell &&
                            editingCell.rowIndex === i &&
                            editingCell.columnName === col
                          const cellKey = `${i}-${col}`
                          const isCopied = copiedCell === cellKey
                          return (
                            <td
                              key={col}
                              onDoubleClick={() => onCellDoubleClick(i, col)}
                              className={`${isEditing ? 'editing-cell' : ''} ${isCopied ? 'cell-copied' : ''}`}
                              style={{ maxWidth: resize.getWidth(col) }}
                            >
                              {isEditing ? (
                                <input
                                  className="ws-cell-input"
                                  value={editingCell!.currentValue}
                                  onChange={(e) => onCellEditChange(e.target.value)}
                                  onKeyDown={(e) => {
                                    if (e.key === 'Enter') onCellEditCommit()
                                    if (e.key === 'Escape') onCellEditCancel()
                                  }}
                                  autoFocus
                                  onBlur={onCellEditCancel}
                                />
                              ) : (
                                renderCell(i, row[col], col)
                              )}
                            </td>
                          )
                        })}
                      </tr>
                    )
                  })}
                  {virtualEnabled && (
                    <tr style={{ height: Math.max(0, vs.totalHeight - vs.offsetY - visibleRows.length * 30) }} aria-hidden />
                  )}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="panel-empty">
              <span className="text-muted">Query executed successfully — no rows returned</span>
            </div>
          )}
        </>
      )}

      {!result && !error && !loading && (
        <div className="panel-empty">
          <span className="text-muted">Write a query and press Run or Cmd+Enter</span>
        </div>
      )}

      {inspectedCell && (
        <CellInspector
          columnName={inspectedCell.column}
          value={inspectedCell.value}
          onClose={() => setInspectedCell(null)}
        />
      )}

      {showRowDiff && compareRows.length === 2 && (
        <RowDiffModal
          rowA={sortedRows[compareRows[0]] as Record<string, unknown>}
          rowB={sortedRows[compareRows[1]] as Record<string, unknown>}
          rowALabel={`Row ${compareRows[0] + 1}`}
          rowBLabel={`Row ${compareRows[1] + 1}`}
          columns={columns}
          onClose={() => setShowRowDiff(false)}
        />
      )}
    </div>
  )
}
