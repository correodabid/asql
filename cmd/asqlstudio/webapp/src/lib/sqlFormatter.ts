import { tokenizeSQL, type Token } from './sqlHighlighter'

// Keywords that start a new major clause (get a newline before them)
const CLAUSE_KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS',
  'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'UNION',
  'INSERT', 'INTO', 'UPDATE', 'DELETE', 'SET', 'VALUES', 'RETURNING',
  'CREATE', 'ALTER', 'DROP', 'BEGIN', 'COMMIT', 'ROLLBACK',
  'AND', 'OR', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'WITH', 'FOR',
])

// Keywords that start a top-level clause (no indent)
const TOP_LEVEL = new Set([
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE',
  'CREATE', 'ALTER', 'DROP', 'BEGIN', 'COMMIT', 'ROLLBACK',
  'UNION', 'WITH', 'RETURNING', 'VALUES',
  'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
])

// Keywords that get indented as sub-clauses
const SUB_CLAUSE = new Set([
  'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS',
  'AND', 'OR',
  'SET', 'INTO', 'FOR',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
])

const INDENT = '  '

/** Check if a dot token is a qualified-name separator (table.column) */
function isDotSeparator(tokens: Token[], dotIndex: number): boolean {
  // Look back for an identifier (skip whitespace)
  let prev = dotIndex - 1
  while (prev >= 0 && tokens[prev].type === 'identifier' && /^\s+$/.test(tokens[prev].text)) prev--
  const hasPrev = prev >= 0 && (tokens[prev].type === 'identifier' || tokens[prev].type === 'keyword')

  // Look forward for an identifier (skip whitespace)
  let next = dotIndex + 1
  while (next < tokens.length && tokens[next].type === 'identifier' && /^\s+$/.test(tokens[next].text)) next++
  const hasNext = next < tokens.length && (tokens[next].type === 'identifier' || tokens[next].type === 'keyword')

  return hasPrev && hasNext
}

/** Format a SQL string with proper indentation and keyword uppercasing. */
export function formatSQL(sql: string): string {
  const trimmed = sql.trim()
  if (!trimmed) return ''

  // Split into multiple statements
  const statements = splitStatements(trimmed)
  return statements.map(formatSingleStatement).join('\n\n')
}

function splitStatements(sql: string): string[] {
  const tokens = tokenizeSQL(sql)
  const stmts: string[] = []
  let current: Token[] = []

  for (const token of tokens) {
    current.push(token)
    if (token.type === 'punctuation' && token.text === ';') {
      stmts.push(tokensToText(current))
      current = []
    }
  }

  if (current.length > 0) {
    const text = tokensToText(current).trim()
    if (text) stmts.push(text)
  }

  return stmts
}

function tokensToText(tokens: Token[]): string {
  return tokens.map(t => t.text).join('')
}

function formatSingleStatement(sql: string): string {
  const tokens = tokenizeSQL(sql.trim())
  if (tokens.length === 0) return ''

  const parts: string[] = []
  let depth = 0
  let parenDepth = 0
  let lineStart = true
  let prevToken: Token | null = null

  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i]

    // Skip whitespace tokens — we control spacing
    if (token.type === 'identifier' && /^\s+$/.test(token.text)) {
      continue
    }

    // Comments: preserve as-is with newline
    if (token.type === 'comment') {
      if (!lineStart) parts.push('\n')
      parts.push(INDENT.repeat(depth) + token.text)
      parts.push('\n')
      lineStart = true
      prevToken = token
      continue
    }

    const upper = token.text.toUpperCase()

    // Track parentheses
    if (token.type === 'punctuation' && token.text === '(') {
      parenDepth++
    }
    if (token.type === 'punctuation' && token.text === ')') {
      parenDepth = Math.max(0, parenDepth - 1)
    }

    // Semicolons
    if (token.type === 'punctuation' && token.text === ';') {
      parts.push(';')
      lineStart = false
      prevToken = token
      continue
    }

    // Dot handling: never add spaces around dots used as qualified-name separators
    if (token.type === 'punctuation' && token.text === '.' && isDotSeparator(tokens, i)) {
      parts.push('.')
      lineStart = false
      prevToken = token
      continue
    }

    // Token immediately after a dot separator: no space before it
    const prevIsDot = prevToken?.type === 'punctuation' && prevToken?.text === '.'

    // Inside parentheses: don't reformat, just normalize spacing
    if (parenDepth > 0) {
      if (!lineStart && prevToken && !(prevToken.type === 'punctuation' && prevToken.text === '(') && !prevIsDot) {
        if (token.type === 'punctuation' && token.text === ')') {
          // no space before closing paren
        } else if (token.type === 'punctuation' && token.text === ',') {
          // no space before comma
        } else if (token.type === 'punctuation' && token.text === '.' && isDotSeparator(tokens, i)) {
          // no space before dot separator
        } else {
          parts.push(' ')
        }
      }
      // Uppercase keywords even inside parens
      if (token.type === 'keyword') {
        parts.push(upper)
      } else if (token.type === 'function') {
        parts.push(upper)
      } else {
        parts.push(token.text)
      }
      // Space after comma inside parens
      if (token.type === 'punctuation' && token.text === ',') {
        parts.push(' ')
      }
      lineStart = false
      prevToken = token
      continue
    }

    // Major clause keywords → newline + indent
    if (token.type === 'keyword' && CLAUSE_KEYWORDS.has(upper)) {
      // Determine indent level
      if (TOP_LEVEL.has(upper)) {
        depth = 0
      } else if (SUB_CLAUSE.has(upper)) {
        depth = 1
      }

      // Compound keywords: don't break between ORDER/BY, GROUP/BY, INSERT/INTO, etc.
      const prevUpper = prevToken?.text?.toUpperCase() || ''
      const isCompound =
        (upper === 'BY' && (prevUpper === 'ORDER' || prevUpper === 'GROUP' || prevUpper === 'PARTITION')) ||
        (upper === 'INTO' && prevUpper === 'INSERT') ||
        (upper === 'JOIN' && (prevUpper === 'INNER' || prevUpper === 'LEFT' || prevUpper === 'RIGHT' || prevUpper === 'OUTER' || prevUpper === 'CROSS')) ||
        (upper === 'OUTER' && (prevUpper === 'LEFT' || prevUpper === 'RIGHT' || prevUpper === 'CROSS')) ||
        (upper === 'HISTORY' && prevUpper === 'FOR') ||
        (upper === 'EXISTS' && (prevUpper === 'IF' || prevUpper === 'NOT'))

      if (isCompound) {
        parts.push(' ' + upper)
      } else {
        if (!lineStart && parts.length > 0) {
          parts.push('\n')
        }
        parts.push(INDENT.repeat(depth) + upper)
      }
      lineStart = false
      prevToken = token
      continue
    }

    // Commas in SELECT list → newline
    if (token.type === 'punctuation' && token.text === ',' && parenDepth === 0) {
      parts.push(',\n' + INDENT.repeat(Math.max(1, depth)))
      lineStart = true
      prevToken = token
      continue
    }

    // Default: add space separator (but not after a dot)
    if (!lineStart && prevToken && !prevIsDot) {
      parts.push(' ')
    }

    // Uppercase keywords and functions
    if (token.type === 'keyword') {
      parts.push(upper)
    } else if (token.type === 'function') {
      parts.push(upper)
    } else {
      parts.push(token.text)
    }

    lineStart = false
    prevToken = token
  }

  return parts.join('')
}
