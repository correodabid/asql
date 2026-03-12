import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { SchemaTable } from '../schema'
import type { TableInfo } from '../types/workspace'

type Suggestion = {
  label: string
  type: 'table' | 'column' | 'keyword'
  detail?: string
}

type Props = {
  currentWord: string
  contextBefore: string
  tables?: TableInfo[]
  getTableSchema?: (name: string) => SchemaTable | undefined
  sql: string
  cursorPos: number
  position: { top: number; left: number }
  onComplete: (text: string) => void
  onDismiss: () => void
}

const SQL_KEYWORD_LIST = [
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'UPDATE', 'DELETE', 'SET',
  'CREATE', 'ALTER', 'DROP', 'TABLE', 'INDEX',
  'JOIN', 'INNER', 'LEFT', 'RIGHT', 'CROSS', 'ON',
  'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'EXISTS', 'IS',
  'AS', 'ORDER', 'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
  'ASC', 'DESC', 'DISTINCT', 'UNION', 'ALL',
  'BEGIN', 'COMMIT', 'ROLLBACK', 'DOMAIN',
  'VALUES', 'RETURNING', 'DEFAULT',
  'PRIMARY', 'KEY', 'UNIQUE', 'REFERENCES',
  'NULL', 'TRUE', 'FALSE',
  'FOR', 'HISTORY', 'EXPLAIN',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'WITH', 'USING',
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
  'COALESCE', 'UPPER', 'LOWER', 'LENGTH',
]

// Keywords after which we suggest table names
const TABLE_CONTEXT_KEYWORDS = new Set([
  'FROM', 'JOIN', 'INTO', 'UPDATE', 'TABLE', 'INNER', 'LEFT', 'RIGHT', 'CROSS',
])

export function SQLAutocomplete({
  currentWord,
  contextBefore,
  tables,
  getTableSchema,
  sql,
  cursorPos,
  position,
  onComplete,
  onDismiss,
}: Props) {
  const [selectedIndex, setSelectedIndex] = useState(0)
  const listRef = useRef<HTMLDivElement>(null)

  // Determine what type of suggestion to show
  const suggestions = useMemo((): Suggestion[] => {
    const word = currentWord.toLowerCase()
    const results: Suggestion[] = []

    // Check if we have table.column access
    const dotIdx = currentWord.lastIndexOf('.')
    if (dotIdx > 0 && getTableSchema) {
      const tablePart = currentWord.slice(0, dotIdx).toLowerCase()
      const colPart = currentWord.slice(dotIdx + 1).toLowerCase()
      // Find table by name or alias
      const schema = getTableSchema(tablePart)
      if (schema) {
        for (const col of schema.columns) {
          if (!colPart || col.name.toLowerCase().startsWith(colPart)) {
            results.push({
              label: col.name,
              type: 'column',
              detail: col.type.toLowerCase(),
            })
          }
        }
        return results
      }
    }

    // After FROM/JOIN/etc → suggest table names first
    const suggestTables = TABLE_CONTEXT_KEYWORDS.has(contextBefore)

    if (suggestTables && tables) {
      for (const t of tables) {
        if (!word || t.name.toLowerCase().startsWith(word)) {
          results.push({ label: t.name, type: 'table' })
        }
      }
      return results
    }

    // After SELECT/WHERE → suggest columns from most recent table
    if ((contextBefore === 'SELECT' || contextBefore === 'WHERE' || contextBefore === ',') && getTableSchema) {
      // Try to identify the table from SQL
      const tableMatch = sql.slice(0, cursorPos).match(/\bFROM\s+(\w+)/i)
      if (tableMatch) {
        const schema = getTableSchema(tableMatch[1].toLowerCase())
        if (schema) {
          for (const col of schema.columns) {
            if (!word || col.name.toLowerCase().startsWith(word)) {
              results.push({
                label: col.name,
                type: 'column',
                detail: col.type.toLowerCase(),
              })
            }
          }
        }
      }
    }

    // Table name suggestions (fuzzy)
    if (tables) {
      for (const t of tables) {
        if (word && t.name.toLowerCase().includes(word) && !results.some((r) => r.label === t.name)) {
          results.push({ label: t.name, type: 'table' })
        }
      }
    }

    // SQL keyword suggestions
    if (word.length >= 1) {
      for (const kw of SQL_KEYWORD_LIST) {
        if (kw.toLowerCase().startsWith(word) && !results.some((r) => r.label === kw)) {
          results.push({ label: kw, type: 'keyword' })
        }
      }
    }

    return results.slice(0, 20) // limit to 20 suggestions
  }, [currentWord, contextBefore, tables, getTableSchema, sql, cursorPos])

  // Reset selection when suggestions change
  useEffect(() => {
    setSelectedIndex(0)
  }, [suggestions])

  // Auto-scroll selected item into view
  useEffect(() => {
    const list = listRef.current
    if (!list) return
    const item = list.children[selectedIndex] as HTMLElement | undefined
    if (item) {
      item.scrollIntoView({ block: 'nearest' })
    }
  }, [selectedIndex])

  // Keyboard handling (captured at the document level to work alongside textarea)
  const handleDocKeyDown = useCallback((e: KeyboardEvent) => {
    if (suggestions.length === 0) return

    if (e.key === 'ArrowDown') {
      e.preventDefault()
      e.stopPropagation()
      setSelectedIndex((i) => Math.min(i + 1, suggestions.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      e.stopPropagation()
      setSelectedIndex((i) => Math.max(i - 1, 0))
    } else if (e.key === 'Tab' || e.key === 'Enter') {
      e.preventDefault()
      e.stopPropagation()
      if (suggestions[selectedIndex]) {
        onComplete(suggestions[selectedIndex].label)
      }
    } else if (e.key === 'Escape') {
      e.preventDefault()
      onDismiss()
    }
  }, [suggestions, selectedIndex, onComplete, onDismiss])

  useEffect(() => {
    document.addEventListener('keydown', handleDocKeyDown, true)
    return () => document.removeEventListener('keydown', handleDocKeyDown, true)
  }, [handleDocKeyDown])

  if (suggestions.length === 0) return null

  return (
    <div
      className="sql-autocomplete"
      style={{ top: position.top, left: position.left }}
      ref={listRef}
    >
      {suggestions.map((s, i) => (
        <div
          key={`${s.type}-${s.label}`}
          className={`sql-autocomplete-item ${i === selectedIndex ? 'selected' : ''}`}
          onMouseEnter={() => setSelectedIndex(i)}
          onMouseDown={(e) => {
            e.preventDefault()
            onComplete(s.label)
          }}
        >
          <span className={`sql-autocomplete-type ${s.type}`}>
            {s.type === 'table' ? 'TBL' : s.type === 'column' ? 'COL' : 'KW'}
          </span>
          <span className="sql-autocomplete-label">{s.label}</span>
          {s.detail && <span className="sql-autocomplete-detail">{s.detail}</span>}
        </div>
      ))}
    </div>
  )
}
