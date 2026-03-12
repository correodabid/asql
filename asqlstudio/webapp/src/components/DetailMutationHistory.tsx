import { useCallback, useEffect, useState } from 'react'
import { api } from '../lib/api'
import { formatCell, formatSQLValue } from '../lib/sql'

type Props = {
  tableName: string
  pkColumns: string[]
  row: Record<string, unknown>
  domain: string
}

type MutationRow = Record<string, unknown>

export function DetailMutationHistory({ tableName, pkColumns, row, domain }: Props) {
  const [mutations, setMutations] = useState<MutationRow[]>([])
  const [loading, setLoading] = useState(false)
  const [expanded, setExpanded] = useState(false)

  const loadHistory = useCallback(async () => {
    if (mutations.length > 0) return
    setLoading(true)
    try {
      const whereClauses = pkColumns
        .map((pk) => `${pk} = ${formatSQLValue(row[pk])}`)
        .join(' AND ')
      const sql = `SELECT * FROM ${tableName} FOR HISTORY WHERE ${whereClauses}`
      const response = await api<{
        status: string
        rows?: MutationRow[]
      }>('/api/row-history', 'POST', {
        sql,
        domains: [domain],
      })
      setMutations(response.rows || [])
    } catch {
      // ignore
    } finally {
      setLoading(false)
    }
  }, [tableName, pkColumns, row, domain, mutations.length])

  useEffect(() => {
    if (expanded && mutations.length === 0) {
      loadHistory()
    }
  }, [expanded, mutations.length, loadHistory])

  return (
    <div className="detail-section">
      <button
        className="detail-section-toggle"
        onClick={() => setExpanded(!expanded)}
      >
        <span className="detail-section-title">Mutation History</span>
        <span>{expanded ? '-' : '+'}</span>
      </button>

      {expanded && (
        <div className="detail-section-body">
          {loading && <div className="text-muted">Loading...</div>}
          {!loading && mutations.length === 0 && (
            <div className="text-muted">No mutations found</div>
          )}
          {mutations.map((m, i) => {
            const op = (m.__operation as string) || (m._operation as string) || 'unknown'
            const commitLSN = (m.__commit_lsn as string | number | undefined) ?? (m._lsn as string | number | undefined)
            return (
              <div key={i} className={`mutation-entry mutation-${op.toLowerCase()}`}>
                <div className="mutation-header">
                  <span className={`mutation-badge ${op.toLowerCase()}`}>{op}</span>
                  {commitLSN !== undefined && commitLSN !== null && (
                    <span className="mutation-ts mono">LSN {String(commitLSN)}</span>
                  )}
                </div>
                <div className="mutation-fields">
                  {Object.entries(m)
                    .filter(([k]) => k !== '__operation' && k !== '__commit_lsn' && !k.startsWith('_'))
                    .map(([k, v]) => (
                      <span key={k} className="mutation-field">
                        <span className="mutation-field-name">{k}:</span>{' '}
                        {formatCell(v)}
                      </span>
                    ))}
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
