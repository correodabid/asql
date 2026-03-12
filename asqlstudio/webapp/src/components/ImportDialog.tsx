import { useCallback, useRef, useState } from 'react'
import { parseCSV, parseJSON, generateInserts } from '../lib/import'

type Props = {
  tables: { name: string }[]
  onInsertSQL: (sql: string) => void
  onClose: () => void
}

export function ImportDialog({ tables, onInsertSQL, onClose }: Props) {
  const [step, setStep] = useState<'pick' | 'preview'>('pick')
  const [fileName, setFileName] = useState('')
  const [columns, setColumns] = useState<string[]>([])
  const [rows, setRows] = useState<Record<string, unknown>[]>([])
  const [targetTable, setTargetTable] = useState(tables[0]?.name || '')
  const [error, setError] = useState<string | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)

  const handleFile = useCallback(async (file: File) => {
    setError(null)
    setFileName(file.name)
    const text = await file.text()

    try {
      if (file.name.endsWith('.json')) {
        const parsed = parseJSON(text)
        setColumns(parsed.columns)
        setRows(parsed.rows)
      } else {
        const parsed = parseCSV(text)
        setColumns(parsed.columns)
        setRows(parsed.rows)
      }
      setStep('preview')
    } catch (err) {
      setError((err as Error).message)
    }
  }, [])

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      const file = e.dataTransfer.files[0]
      if (file) handleFile(file)
    },
    [handleFile],
  )

  const handleImport = () => {
    if (!targetTable || rows.length === 0) return
    const sql = generateInserts(targetTable, columns, rows)
    onInsertSQL(sql)
    onClose()
  }

  return (
    <div className="import-overlay" onClick={onClose}>
      <div className="import-dialog" onClick={(e) => e.stopPropagation()}>
        <div className="import-header">
          <span className="import-title">Import Data</span>
          <button className="icon-btn" onClick={onClose}>Close</button>
        </div>

        {error && (
          <div className="console-status-bar error-bar" style={{ margin: '0 16px 8px' }}>
            <span className="error-icon">!</span> {error}
          </div>
        )}

        {step === 'pick' && (
          <div
            className="import-drop-zone"
            onDragOver={(e) => e.preventDefault()}
            onDrop={handleDrop}
            onClick={() => fileRef.current?.click()}
          >
            <input
              ref={fileRef}
              type="file"
              accept=".csv,.json"
              style={{ display: 'none' }}
              onChange={(e) => {
                const file = e.target.files?.[0]
                if (file) handleFile(file)
              }}
            />
            <div className="import-drop-label">
              Drop a CSV or JSON file here, or click to browse
            </div>
            <div className="import-drop-hint">
              CSV: first row = column headers. JSON: array of objects.
            </div>
          </div>
        )}

        {step === 'preview' && (
          <>
            <div className="import-controls">
              <span className="import-file-name">{fileName}</span>
              <span className="import-meta">
                {columns.length} columns, {rows.length} rows
              </span>
              <select
                className="audit-filter-select"
                value={targetTable}
                onChange={(e) => setTargetTable(e.target.value)}
              >
                {tables.map((t) => (
                  <option key={t.name} value={t.name}>{t.name}</option>
                ))}
              </select>
              <button className="toolbar-btn primary" onClick={handleImport}>
                Generate INSERT SQL
              </button>
              <button className="toolbar-btn" onClick={() => { setStep('pick'); setRows([]); setColumns([]) }}>
                Back
              </button>
            </div>

            <div className="import-preview">
              <table className="console-table">
                <thead>
                  <tr>
                    <th className="row-num">#</th>
                    {columns.map((col) => (
                      <th key={col}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {rows.slice(0, 20).map((row, i) => (
                    <tr key={i}>
                      <td className="row-num">{i + 1}</td>
                      {columns.map((col) => (
                        <td key={col}>
                          {row[col] === null || row[col] === undefined
                            ? 'NULL'
                            : typeof row[col] === 'object'
                              ? JSON.stringify(row[col])
                              : String(row[col])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              {rows.length > 20 && (
                <div className="import-more">
                  ...and {rows.length - 20} more rows
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  )
}
