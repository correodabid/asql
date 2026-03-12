import { useEffect, useRef, useState } from 'react'
import { api } from '../lib/api'

type Props = {
  tableName: string
  domain: string
  x: number
  y: number
}

type PreviewData = {
  columns: string[]
  rows: Record<string, unknown>[]
}

export function TablePreview({ tableName, domain, x, y }: Props) {
  const [data, setData] = useState<PreviewData | null>(null)
  const [loading, setLoading] = useState(true)
  const mounted = useRef(true)

  useEffect(() => {
    mounted.current = true
    setLoading(true)

    const sql = `SELECT * FROM ${tableName} LIMIT 5;`
    api<{ rows?: Record<string, unknown>[] }>('/api/read-query', 'POST', {
      sql,
      domains: [domain],
      consistency: 'strong',
    })
      .then((resp) => {
        if (!mounted.current) return
        const rows = resp.rows || []
        const columns = rows.length > 0 ? Object.keys(rows[0]) : []
        setData({ columns, rows })
      })
      .catch(() => {
        if (mounted.current) setData(null)
      })
      .finally(() => {
        if (mounted.current) setLoading(false)
      })

    return () => {
      mounted.current = false
    }
  }, [tableName, domain])

  // Position the preview card
  const style: React.CSSProperties = {
    position: 'fixed',
    left: x,
    top: y,
    zIndex: 999,
  }

  return (
    <div className="table-preview" style={style}>
      <div className="table-preview-header">{tableName}</div>
      {loading && (
        <div className="table-preview-loading">Loading...</div>
      )}
      {!loading && data && data.rows.length > 0 && (
        <table className="table-preview-table">
          <thead>
            <tr>
              {data.columns.map((col) => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.rows.map((row, i) => (
              <tr key={i}>
                {data.columns.map((col) => (
                  <td key={col}>
                    {row[col] === null
                      ? 'NULL'
                      : typeof row[col] === 'object'
                        ? JSON.stringify(row[col]).slice(0, 30)
                        : String(row[col]).slice(0, 30)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {!loading && data && data.rows.length === 0 && (
        <div className="table-preview-empty">No rows</div>
      )}
      {!loading && !data && (
        <div className="table-preview-empty">Failed to load</div>
      )}
    </div>
  )
}
