import { useCallback, useState } from 'react'
import { api } from '../lib/api'
import type { TableInfo } from '../types/workspace'

type Props = {
  domain: string
  tables: TableInfo[]
  onClose: () => void
}

type AuditEntry = {
  table: string
  operation: string
  timestamp: string
  lsn: number
  data: Record<string, unknown>
}

export function AuditTrail({ domain, tables, onClose }: Props) {
  const [entries, setEntries] = useState<AuditEntry[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedTable, setSelectedTable] = useState<string>('__all__')
  const [filterOp, setFilterOp] = useState<string>('__all__')
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null)

  const loadAudit = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const tablesToQuery = selectedTable === '__all__'
        ? tables.map(t => t.name)
        : [selectedTable]

      const allEntries: AuditEntry[] = []

      for (const tableName of tablesToQuery) {
        const sql = `SELECT * FROM ${tableName} FOR HISTORY LIMIT 100;`
        try {
          const resp = await api<{ rows?: Record<string, unknown>[] }>(
            '/api/row-history', 'POST',
            { sql, domains: [domain] }
          )
          const rows = resp.rows || []
          for (const row of rows) {
            const op = String(row._operation || row.operation || 'unknown')
            const ts = String(row._timestamp || row.timestamp || '')
            const lsn = Number(row._lsn || row.lsn || 0)
            // Build data without meta fields
            const data: Record<string, unknown> = {}
            for (const [k, v] of Object.entries(row)) {
              if (!k.startsWith('_')) data[k] = v
            }
            allEntries.push({ table: tableName, operation: op, timestamp: ts, lsn, data })
          }
        } catch {
          // skip tables that don't support FOR HISTORY
        }
      }

      // Sort by LSN descending
      allEntries.sort((a, b) => b.lsn - a.lsn)
      setEntries(allEntries)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }, [domain, tables, selectedTable])

  const filteredEntries = filterOp === '__all__'
    ? entries
    : entries.filter(e => e.operation.toLowerCase() === filterOp)

  const opClass = (op: string) => {
    const lower = op.toLowerCase()
    if (lower.includes('insert') || lower.includes('create')) return 'insert'
    if (lower.includes('update') || lower.includes('modify')) return 'update'
    if (lower.includes('delete') || lower.includes('drop')) return 'delete'
    return 'update'
  }

  return (
    <div className="audit-panel">
      <div className="audit-header">
        <span className="audit-title">Audit Trail</span>
        <button className="icon-btn" onClick={onClose}>Close</button>
      </div>

      <div className="audit-controls">
        <select
          className="audit-filter-select"
          value={selectedTable}
          onChange={(e) => setSelectedTable(e.target.value)}
        >
          <option value="__all__">All Tables</option>
          {tables.map(t => (
            <option key={t.name} value={t.name}>{t.name}</option>
          ))}
        </select>

        <select
          className="audit-filter-select"
          value={filterOp}
          onChange={(e) => setFilterOp(e.target.value)}
        >
          <option value="__all__">All Operations</option>
          <option value="insert">INSERT</option>
          <option value="update">UPDATE</option>
          <option value="delete">DELETE</option>
        </select>

        <button className="toolbar-btn primary" onClick={loadAudit} disabled={loading}>
          {loading ? 'Loading...' : 'Load'}
        </button>

        {entries.length > 0 && (
          <span className="tt-diff-summary">
            {filteredEntries.length} entr{filteredEntries.length === 1 ? 'y' : 'ies'}
          </span>
        )}
      </div>

      {error && (
        <div className="console-status-bar error-bar">
          <span className="error-icon">!</span> {error}
        </div>
      )}

      <div className="audit-list">
        {entries.length === 0 && !loading && (
          <div className="panel-empty">
            <span className="text-muted">Select a table and click Load to view audit trail</span>
          </div>
        )}
        {filteredEntries.map((entry, i) => (
          <div
            key={i}
            className="audit-entry"
            onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}
          >
            <div className="audit-entry-header">
              <span className={`audit-op-badge ${opClass(entry.operation)}`}>
                {entry.operation}
              </span>
              <span className="audit-entry-table">{entry.table}</span>
              {entry.lsn > 0 && (
                <span className="audit-entry-table">LSN: {entry.lsn}</span>
              )}
              <span className="audit-entry-time">{entry.timestamp}</span>
            </div>
            <div className="audit-entry-data">
              {Object.entries(entry.data).slice(0, 4).map(([k, v]) =>
                `${k}=${v === null ? 'NULL' : typeof v === 'object' ? JSON.stringify(v) : v}`
              ).join(', ')}
            </div>
            {expandedIdx === i && (
              <div className="audit-detail">
                {JSON.stringify(entry.data, null, 2)}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
