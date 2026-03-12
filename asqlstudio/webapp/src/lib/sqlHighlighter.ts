// SQL tokenizer and syntax highlighter for ASQL Studio.
// Produces HTML spans with CSS classes for keyword coloring.

export type TokenType = 'keyword' | 'string' | 'number' | 'comment' | 'operator' | 'identifier' | 'punctuation' | 'function'

export type Token = {
  type: TokenType
  text: string
  start: number
  end: number
}

const SQL_KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'UPDATE', 'DELETE', 'SET',
  'CREATE', 'ALTER', 'DROP', 'TABLE', 'INDEX', 'ENTITY',
  'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'ON',
  'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'EXISTS', 'IS',
  'AS', 'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
  'ASC', 'DESC', 'DISTINCT', 'UNION', 'ALL',
  'BEGIN', 'COMMIT', 'ROLLBACK', 'DOMAIN',
  'VALUES', 'RETURNING', 'DEFAULT',
  'PRIMARY', 'KEY', 'UNIQUE', 'REFERENCES', 'FOREIGN', 'VERSIONED',
  'IF', 'NOT', 'NULL', 'TRUE', 'FALSE',
  'FOR', 'HISTORY', 'IMPORT', 'EXPLAIN',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'WITH', 'USING', 'OF',
  'INT', 'TEXT', 'FLOAT', 'BOOL', 'BOOLEAN', 'TIMESTAMP', 'JSON',
  'INCLUDES', 'ROOT',
  'HASH', 'BTREE',
  'ADD', 'COLUMN', 'CHECK',
  'UUID_V7', 'AUTOINCREMENT',
])

const SQL_FUNCTIONS = new Set([
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
  'COALESCE', 'NULLIF', 'CAST',
  'UPPER', 'LOWER', 'LENGTH', 'TRIM', 'SUBSTR', 'SUBSTRING',
  'ABS', 'ROUND', 'FLOOR', 'CEIL',
  'NOW', 'DATE', 'TIME',
  'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD',
  'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE',
  'OVER', 'PARTITION',
])

/** Tokenize a SQL string into typed tokens with position info. */
export function tokenizeSQL(sql: string): Token[] {
  const tokens: Token[] = []
  let i = 0

  while (i < sql.length) {
    const ch = sql[i]

    // Whitespace — preserve as identifier (transparent)
    if (/\s/.test(ch)) {
      const start = i
      while (i < sql.length && /\s/.test(sql[i])) i++
      tokens.push({ type: 'identifier', text: sql.slice(start, i), start, end: i })
      continue
    }

    // Single-line comment: --
    if (ch === '-' && i + 1 < sql.length && sql[i + 1] === '-') {
      const start = i
      while (i < sql.length && sql[i] !== '\n') i++
      tokens.push({ type: 'comment', text: sql.slice(start, i), start, end: i })
      continue
    }

    // Multi-line comment: /* ... */
    if (ch === '/' && i + 1 < sql.length && sql[i + 1] === '*') {
      const start = i
      i += 2
      while (i < sql.length && !(sql[i] === '*' && i + 1 < sql.length && sql[i + 1] === '/')) i++
      if (i < sql.length) i += 2 // skip */
      tokens.push({ type: 'comment', text: sql.slice(start, i), start, end: i })
      continue
    }

    // Strings: 'single quoted' with '' escape
    if (ch === "'") {
      const start = i
      i++ // skip opening quote
      while (i < sql.length) {
        if (sql[i] === "'" && i + 1 < sql.length && sql[i + 1] === "'") {
          i += 2 // escaped quote
        } else if (sql[i] === "'") {
          i++ // closing quote
          break
        } else {
          i++
        }
      }
      tokens.push({ type: 'string', text: sql.slice(start, i), start, end: i })
      continue
    }

    // Numbers: integer or decimal
    if (/\d/.test(ch) || (ch === '.' && i + 1 < sql.length && /\d/.test(sql[i + 1]))) {
      const start = i
      while (i < sql.length && /[\d.]/.test(sql[i])) i++
      tokens.push({ type: 'number', text: sql.slice(start, i), start, end: i })
      continue
    }

    // JSON operators: ->> and ->
    if (ch === '-' && i + 1 < sql.length && sql[i + 1] === '>') {
      const start = i
      if (i + 2 < sql.length && sql[i + 2] === '>') {
        i += 3
      } else {
        i += 2
      }
      tokens.push({ type: 'operator', text: sql.slice(start, i), start, end: i })
      continue
    }

    // Multi-char operators: !=, <>, <=, >=, ||
    if (i + 1 < sql.length) {
      const two = sql.slice(i, i + 2)
      if (two === '!=' || two === '<>' || two === '<=' || two === '>=' || two === '||') {
        tokens.push({ type: 'operator', text: two, start: i, end: i + 2 })
        i += 2
        continue
      }
    }

    // Single-char operators
    if ('=<>+-*/%'.includes(ch)) {
      tokens.push({ type: 'operator', text: ch, start: i, end: i + 1 })
      i++
      continue
    }

    // Punctuation: ( ) , ; .
    if ('(),;.'.includes(ch)) {
      tokens.push({ type: 'punctuation', text: ch, start: i, end: i + 1 })
      i++
      continue
    }

    // Identifiers and keywords
    if (/[a-zA-Z_]/.test(ch)) {
      const start = i
      while (i < sql.length && /[a-zA-Z0-9_]/.test(sql[i])) i++
      const word = sql.slice(start, i)
      const upper = word.toUpperCase()

      // Check if it's followed by '(' to detect functions
      let nextNonSpace = i
      while (nextNonSpace < sql.length && sql[nextNonSpace] === ' ') nextNonSpace++
      const isFunc = nextNonSpace < sql.length && sql[nextNonSpace] === '(' && SQL_FUNCTIONS.has(upper)

      if (isFunc) {
        tokens.push({ type: 'function', text: word, start, end: i })
      } else if (SQL_KEYWORDS.has(upper)) {
        tokens.push({ type: 'keyword', text: word, start, end: i })
      } else {
        tokens.push({ type: 'identifier', text: word, start, end: i })
      }
      continue
    }

    // Anything else — treat as identifier
    tokens.push({ type: 'identifier', text: ch, start: i, end: i + 1 })
    i++
  }

  return tokens
}

const escapeMap: Record<string, string> = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
}

function escapeHTML(text: string): string {
  return text.replace(/[&<>"]/g, (ch) => escapeMap[ch] || ch)
}

const classMap: Record<TokenType, string> = {
  keyword: 'sql-kw',
  string: 'sql-str',
  number: 'sql-num',
  comment: 'sql-cmt',
  operator: 'sql-op',
  identifier: 'sql-id',
  punctuation: 'sql-op',
  function: 'sql-fn',
}

/** Convert SQL text to syntax-highlighted HTML string. */
export function highlightSQL(sql: string): string {
  if (!sql) return ''
  const tokens = tokenizeSQL(sql)
  const parts: string[] = []
  for (const token of tokens) {
    const cls = classMap[token.type]
    if (token.type === 'identifier' && /^\s+$/.test(token.text)) {
      // Whitespace — no span wrapper needed
      parts.push(escapeHTML(token.text))
    } else {
      parts.push(`<span class="${cls}">${escapeHTML(token.text)}</span>`)
    }
  }
  return parts.join('')
}

/** Split SQL into individual statements by semicolons, respecting strings and comments. */
export function detectStatements(sql: string): { start: number; end: number }[] {
  const statements: { start: number; end: number }[] = []
  const tokens = tokenizeSQL(sql)
  let stmtStart = -1

  for (const token of tokens) {
    // Skip leading whitespace before a statement
    if (stmtStart === -1 && token.type === 'identifier' && /^\s+$/.test(token.text)) {
      continue
    }
    if (stmtStart === -1) {
      stmtStart = token.start
    }
    if (token.type === 'punctuation' && token.text === ';') {
      statements.push({ start: stmtStart, end: token.end })
      stmtStart = -1
    }
  }

  // Trailing statement without semicolon
  if (stmtStart !== -1) {
    // Find the last non-whitespace token
    let lastEnd = stmtStart
    for (const token of tokens) {
      if (token.start >= stmtStart && !(token.type === 'identifier' && /^\s+$/.test(token.text))) {
        lastEnd = token.end
      }
    }
    if (lastEnd > stmtStart) {
      statements.push({ start: stmtStart, end: lastEnd })
    }
  }

  return statements
}

/** Return the index of the statement containing the cursor position. */
export function activeStatementIndex(
  statements: { start: number; end: number }[],
  cursorPos: number,
): number {
  for (let i = 0; i < statements.length; i++) {
    if (cursorPos >= statements[i].start && cursorPos <= statements[i].end) {
      return i
    }
  }
  // If cursor is between statements, find the nearest one
  for (let i = 0; i < statements.length; i++) {
    if (i + 1 < statements.length && cursorPos > statements[i].end && cursorPos < statements[i + 1].start) {
      return i // belong to the previous statement
    }
  }
  // If cursor is past all statements, return last
  if (statements.length > 0 && cursorPos >= statements[statements.length - 1].end) {
    return statements.length - 1
  }
  return 0
}

/** Highlight SQL with active statement background marking. */
export function highlightSQLWithActive(sql: string, cursorPos: number): string {
  if (!sql) return ''
  const statements = detectStatements(sql)
  if (statements.length <= 1) return highlightSQL(sql)

  const activeIdx = activeStatementIndex(statements, cursorPos)
  const active = statements[activeIdx]
  if (!active) return highlightSQL(sql)

  // Build HTML in three segments: before, active, after
  const before = sql.slice(0, active.start)
  const stmt = sql.slice(active.start, active.end)
  const after = sql.slice(active.end)

  return highlightSQL(before) +
    '<span class="sql-active-stmt">' + highlightSQL(stmt) + '</span>' +
    highlightSQL(after)
}
