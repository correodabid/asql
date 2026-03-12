import { useCallback, useState } from 'react'
import { api } from '../lib/api'

type Props = {
  domain: string
  tableName: string
  maxLSN: number
  onClose: () => void
}

type DiffRow = {
  key: string
  status: 'added' | 'removed' | 'changed' | 'unchanged'
  before: Record<string, unknown> | null
  after: Record<string, unknown> | null
  changedFields: string[]
}

export function TimeTravelDiff({ domain, tableName, maxLSN, onClose }: Props) {
  const [lsnA, setLsnA] = useState(Math.max(1, maxLSN - 10))
  const [lsnB, setLsnB] = useState(maxLSN)
  const [rows, setRows] = useState<DiffRow[]>([])
  const [columns, setColumns] = useState<string[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const computeDiff = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const sql = `SELECT * FROM ${tableName} LIMIT 1000;`

      const [respA, respB] = await Promise.all([
        api<{ rows?: Record<string, unknown>[] }>('/api/time-travel', 'POST', {
          sql,
          domains: [domain],
          lsn: lsnA,
        }),
        api<{ rows?: Record<string, unknown>[] }>('/api/time-travel', 'POST', {
          sql,
          domains: [domain],
          lsn: lsnB,
        }),
      ])

      const rowsA = respA.rows || []
      const rowsB = respB.rows || []

      // Detect columns
      const cols = new Set<string>()
      for (const r of [...rowsA, ...rowsB]) {
        for (const k of Object.keys(r)) cols.add(k)
      }
      const colArr = Array.from(cols)
      setColumns(colArr)

      // Build key map (use first column as key or combine all)
      const keyCol = colArr[0] || 'id'
      const makeKey = (r: Record<string, unknown>) => String(r[keyCol] ?? JSON.stringify(r))

      const mapA = new Map<string, Record<string, unknown>>()
      for (const r of rowsA) mapA.set(makeKey(r), r)

      const mapB = new Map<string, Record<string, unknown>>()
      for (const r of rowsB) mapB.set(makeKey(r), r)

      const allKeys = new Set([...mapA.keys(), ...mapB.keys()])
      const diffRows: DiffRow[] = []

      for (const key of allKeys) {
        const before = mapA.get(key) ?? null
        const after = mapB.get(key) ?? null

        if (!before && after) {
          diffRows.push({ key, status: 'added', before: null, after, changedFields: colArr })
        } else if (before && !after) {
          diffRows.push({ key, status: 'removed', before, after: null, changedFields: colArr })
        } else if (before && after) {
          const changed: string[] = []
          for (const col of colArr) {
            if (JSON.stringify(before[col]) !== JSON.stringify(after[col])) {
              changed.push(col)
            }
          }
          diffRows.push({
            key,
            status: changed.length > 0 ? 'changed' : 'unchanged',
            before,
            after,
            changedFields: changed,
          })
        }
      }

      // Sort: removed first, then changed, then added, then unchanged
      const order = { removed: 0, changed: 1, added: 2, unchanged: 3 }
      diffRows.sort((a, b) => order[a.status] - order[b.status])

      setRows(diffRows)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }, [domain, tableName, lsnA, lsnB])

  const changedCount = rows.filter(r => r.status !== 'unchanged').length

  return (
    <div className="tt-diff-panel">
      <div className="tt-diff-header">
        <span className="tt-diff-title">Time-Travel Diff: {tableName}</span>
        <button className="icon-btn" onClick={onClose}>Close</button>
      </div>

      <div className="tt-diff-controls">
        <label className="tt-diff-label">
          LSN A:
          <input
            type="number"
            className="tt-diff-input"
            value={lsnA}
            min={1}
            max={maxLSN}
            onChange={(e) => setLsnA(Number(e.target.value))}
          />
        </label>
        <label className="tt-diff-label">
          LSN B:
          <input
            type="number"
            className="tt-diff-input"
            value={lsnB}
            min={1}
            max={maxLSN}
            onChange={(e) => setLsnB(Number(e.target.value))}
          />
        </label>
        <button className="toolbar-btn primary" onClick={computeDiff} disabled={loading}>
          {loading ? 'Computing...' : 'Compare'}
        </button>
        {rows.length > 0 && (
          <span className="tt-diff-summary">
            {changedCount} change{changedCount !== 1 ? 's' : ''} found
          </span>
        )}
      </div>

      {error && (
        <div className="console-status-bar error-bar">
          <span className="error-icon">!</span> {error}
        </div>
      )}

      {rows.length > 0 && (
        <div className="console-table-wrap">
          <table className="console-table tt-diff-table">
            <thead>
              <tr>
                <th className="row-num">Status</th>
                {columns.map((col) => (
                  <th key={col}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.filter(r => r.status !== 'unchanged').map((row) => (
                <tr key={row.key} className={`tt-diff-row tt-diff-${row.status}`}>
                  <td className="row-num">
                    <span className={`tt-diff-badge ${row.status}`}>
                      {row.status === 'added' ? '+' : row.status === 'removed' ? '-' : '~'}
                    </span>
                  </td>
                  {columns.map((col) => {
                    const isChanged = row.changedFields.includes(col)
                    const val = row.status === 'removed'
                      ? row.before?.[col]
                      : row.after?.[col]
                    const prevVal = row.status === 'changed' && isChanged
                      ? row.before?.[col]
                      : null

                    return (
                      <td
                        key={col}
                        className={isChanged ? 'tt-diff-changed-cell' : ''}
                      >
                        {prevVal !== null && prevVal !== undefined && (
                          <span className="tt-diff-old">{formatVal(prevVal)}</span>
                        )}
                        <span>{formatVal(val)}</span>
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {rows.length > 0 && changedCount === 0 && (
        <div className="panel-empty">
          <span className="text-muted">No differences between LSN {lsnA} and LSN {lsnB}</span>
        </div>
      )}
    </div>
  )
}

function formatVal(v: unknown): string {
  if (v === null || v === undefined) return 'NULL'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}
