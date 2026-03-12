/** Format a JS value into a SQL literal for use in generated SQL statements. */
export function formatSQLValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'number') return String(value)
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE'
  if (typeof value === 'object') return `'${JSON.stringify(value).replace(/'/g, "''")}'`
  return `'${String(value).replace(/'/g, "''")}'`
}

/** Returns true if the SQL statement is a read-only query (SELECT / EXPLAIN / IMPORT). */
export function isReadQuery(sql: string): boolean {
  const upper = sql.replace(/^[\s;]+/, '').toUpperCase()
  return upper.startsWith('SELECT') || upper.startsWith('EXPLAIN') || upper.startsWith('IMPORT')
}

/** Returns true if the SQL statement is an EXPLAIN query. */
export function isExplainQuery(sql: string): boolean {
  return sql.replace(/^[\s;]+/, '').toUpperCase().startsWith('EXPLAIN')
}

/** Returns true if the SQL statement contains FOR HISTORY clause. */
export function isForHistoryQuery(sql: string): boolean {
  return /\bFOR\s+HISTORY\b/i.test(sql)
}

export function generateInsert(table: string, row: Record<string, unknown>): string {
  const cols = Object.keys(row)
  const vals = cols.map((c) => formatSQLValue(row[c]))
  return `INSERT INTO ${table} (${cols.join(', ')}) VALUES (${vals.join(', ')});`
}

export function generateUpdate(
  table: string,
  row: Record<string, unknown>,
  pkColumns: string[],
): string {
  const setClauses = Object.keys(row)
    .filter((c) => !pkColumns.includes(c))
    .map((c) => `${c} = ${formatSQLValue(row[c])}`)
    .join(', ')
  const whereClauses = pkColumns.map((c) => `${c} = ${formatSQLValue(row[c])}`).join(' AND ')
  return `UPDATE ${table} SET ${setClauses} WHERE ${whereClauses};`
}

export function generateDelete(
  table: string,
  row: Record<string, unknown>,
  pkColumns: string[],
): string {
  if (pkColumns.length === 0) {
    const allCols = Object.keys(row)
    const whereClauses = allCols.map((c) => `${c} = ${formatSQLValue(row[c])}`).join(' AND ')
    return `DELETE FROM ${table} WHERE ${whereClauses};`
  }
  const whereClauses = pkColumns.map((c) => `${c} = ${formatSQLValue(row[c])}`).join(' AND ')
  return `DELETE FROM ${table} WHERE ${whereClauses};`
}

export function formatCell(value: unknown): string {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}
