import type { QueryResult } from '../types/workspace'

/** Export query results as CSV string with proper quoting. */
export function exportCSV(result: QueryResult): string {
  const lines: string[] = []

  // Header
  lines.push(result.columns.map(quoteCSV).join(','))

  // Rows
  for (const row of result.rows) {
    const cells = result.columns.map((col) => {
      const val = row[col]
      if (val === null || val === undefined) return ''
      if (typeof val === 'object') return quoteCSV(JSON.stringify(val))
      return quoteCSV(String(val))
    })
    lines.push(cells.join(','))
  }

  return lines.join('\n')
}

/** Export query results as pretty-printed JSON. */
export function exportJSON(result: QueryResult): string {
  return JSON.stringify(result.rows, null, 2)
}

/** Trigger a file download — uses Wails native save dialog when running inside the desktop app. */
export async function downloadFile(content: string, filename: string, _mimeType: string): Promise<void> {
  // In the Wails desktop app there is no browser download manager.
  // Use the Go IPC method that opens a native Save dialog.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const w = window as any
  if (typeof w?.go?.main?.App?.SaveTextFile === 'function') {
    await w.go.main.App.SaveTextFile(filename, content)
    return
  }
  // Fallback: standard browser download
  const blob = new Blob([content], { type: _mimeType })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

function quoteCSV(value: string): string {
  if (value.includes(',') || value.includes('"') || value.includes('\n')) {
    return '"' + value.replace(/"/g, '""') + '"'
  }
  return value
}
