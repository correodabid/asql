import { formatSQLValue } from './sql'

/** Parse CSV text into an array of row objects (first line = headers). */
export function parseCSV(text: string): { columns: string[]; rows: Record<string, string>[] } {
  const lines = text.split(/\r?\n/).filter((l) => l.trim())
  if (lines.length === 0) return { columns: [], rows: [] }

  const parseLine = (line: string): string[] => {
    const fields: string[] = []
    let current = ''
    let inQuotes = false

    for (let i = 0; i < line.length; i++) {
      const ch = line[i]
      if (inQuotes) {
        if (ch === '"' && line[i + 1] === '"') {
          current += '"'
          i++
        } else if (ch === '"') {
          inQuotes = false
        } else {
          current += ch
        }
      } else {
        if (ch === '"') {
          inQuotes = true
        } else if (ch === ',') {
          fields.push(current.trim())
          current = ''
        } else {
          current += ch
        }
      }
    }
    fields.push(current.trim())
    return fields
  }

  const columns = parseLine(lines[0])
  const rows: Record<string, string>[] = []

  for (let i = 1; i < lines.length; i++) {
    const values = parseLine(lines[i])
    const row: Record<string, string> = {}
    columns.forEach((col, ci) => {
      row[col] = values[ci] ?? ''
    })
    rows.push(row)
  }

  return { columns, rows }
}

/** Parse JSON text into row objects. Accepts array of objects or { data: [...] }. */
export function parseJSON(text: string): { columns: string[]; rows: Record<string, unknown>[] } {
  const parsed = JSON.parse(text)
  let arr: Record<string, unknown>[]

  if (Array.isArray(parsed)) {
    arr = parsed
  } else if (parsed && typeof parsed === 'object' && Array.isArray(parsed.data)) {
    arr = parsed.data
  } else if (parsed && typeof parsed === 'object' && Array.isArray(parsed.rows)) {
    arr = parsed.rows
  } else {
    throw new Error('JSON must be an array of objects or { data: [...] }')
  }

  if (arr.length === 0) return { columns: [], rows: [] }

  const colSet = new Set<string>()
  for (const row of arr) {
    for (const key of Object.keys(row)) colSet.add(key)
  }

  return { columns: Array.from(colSet), rows: arr }
}

/** Generate INSERT statements from parsed rows. */
export function generateInserts(
  table: string,
  columns: string[],
  rows: Record<string, unknown>[],
): string {
  return rows
    .map((row) => {
      const vals = columns.map((col) => formatSQLValue(row[col]))
      return `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${vals.join(', ')});`
    })
    .join('\n')
}
