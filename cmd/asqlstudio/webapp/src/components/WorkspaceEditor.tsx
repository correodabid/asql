import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { highlightSQLWithActive } from '../lib/sqlHighlighter'
import { SQLAutocomplete } from './SQLAutocomplete'
import type { SchemaTable } from '../schema'
import type { TableInfo } from '../types/workspace'

type Props = {
  sql: string
  onChange: (sql: string) => void
  onExecute: () => void
  loading: boolean
  tables?: TableInfo[]
  getTableSchema?: (name: string) => SchemaTable | undefined
}

export function WorkspaceEditor({ sql, onChange, onExecute, loading, tables, getTableSchema }: Props) {
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const highlightRef = useRef<HTMLPreElement>(null)
  const gutterRef = useRef<HTMLDivElement>(null)
  const [cursorPos, setCursorPos] = useState(0)
  const [showAutocomplete, setShowAutocomplete] = useState(false)

  // Sync scroll between textarea and highlight overlay + gutter
  const syncScroll = useCallback(() => {
    const ta = textareaRef.current
    const hl = highlightRef.current
    const gt = gutterRef.current
    if (ta && hl) {
      hl.scrollTop = ta.scrollTop
      hl.scrollLeft = ta.scrollLeft
    }
    if (ta && gt) {
      gt.scrollTop = ta.scrollTop
    }
  }, [])

  // Line numbers
  const lineCount = useMemo(() => {
    const count = sql.split('\n').length
    return Math.max(count, 1)
  }, [sql])

  const lineNumbers = useMemo(() => {
    const lines: string[] = []
    for (let i = 1; i <= lineCount; i++) lines.push(String(i))
    return lines
  }, [lineCount])

  // Highlighted HTML
  const highlightedHTML = useMemo(() => {
    return highlightSQLWithActive(sql, cursorPos)
  }, [sql, cursorPos])

  // Track cursor position
  const updateCursor = useCallback(() => {
    const ta = textareaRef.current
    if (ta) {
      setCursorPos(ta.selectionStart)
    }
  }, [])

  // Handle keydown for editor shortcuts
  const handleKeyDown = useCallback((e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    // Cmd/Ctrl+Enter → execute
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault()
      if (!loading) onExecute()
      return
    }

    // Tab → insert 2 spaces
    if (e.key === 'Tab' && !e.shiftKey && !showAutocomplete) {
      e.preventDefault()
      const ta = e.currentTarget
      const start = ta.selectionStart
      const end = ta.selectionEnd
      const newVal = sql.slice(0, start) + '  ' + sql.slice(end)
      onChange(newVal)
      requestAnimationFrame(() => {
        ta.selectionStart = ta.selectionEnd = start + 2
        updateCursor()
      })
      return
    }

    // Cmd/Ctrl+/ → toggle line comment
    if ((e.metaKey || e.ctrlKey) && e.key === '/') {
      e.preventDefault()
      const ta = e.currentTarget
      const start = ta.selectionStart
      const lines = sql.split('\n')
      let lineStart = 0
      let lineIdx = 0
      for (let i = 0; i < lines.length; i++) {
        if (lineStart + lines[i].length >= start) {
          lineIdx = i
          break
        }
        lineStart += lines[i].length + 1
      }
      const line = lines[lineIdx]
      if (line.trimStart().startsWith('-- ')) {
        const idx = line.indexOf('-- ')
        lines[lineIdx] = line.slice(0, idx) + line.slice(idx + 3)
      } else if (line.trimStart().startsWith('--')) {
        const idx = line.indexOf('--')
        lines[lineIdx] = line.slice(0, idx) + line.slice(idx + 2)
      } else {
        lines[lineIdx] = '-- ' + line
      }
      onChange(lines.join('\n'))
      return
    }

    // Enter → auto-indent
    if (e.key === 'Enter' && !showAutocomplete) {
      e.preventDefault()
      const ta = e.currentTarget
      const pos = ta.selectionStart
      // Find current line's leading whitespace
      const beforeCursor = sql.slice(0, pos)
      const currentLineStart = beforeCursor.lastIndexOf('\n') + 1
      const currentLine = beforeCursor.slice(currentLineStart)
      const match = currentLine.match(/^(\s*)/)
      const indent = match ? match[1] : ''
      const insert = '\n' + indent
      const newVal = sql.slice(0, pos) + insert + sql.slice(ta.selectionEnd)
      onChange(newVal)
      requestAnimationFrame(() => {
        ta.selectionStart = ta.selectionEnd = pos + insert.length
        updateCursor()
        syncScroll()
      })
      return
    }

    // Show autocomplete on Ctrl+Space
    if (e.key === ' ' && e.ctrlKey) {
      e.preventDefault()
      setShowAutocomplete(true)
      return
    }

    // Dismiss autocomplete on Escape
    if (e.key === 'Escape') {
      setShowAutocomplete(false)
    }
  }, [sql, onChange, onExecute, loading, showAutocomplete, updateCursor, syncScroll])

  // Handle input change
  const handleChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
    onChange(e.target.value)
    // Auto-show autocomplete when typing identifiers
    requestAnimationFrame(() => {
      updateCursor()
    })
  }, [onChange, updateCursor])

  // Handle autocomplete selection
  const handleComplete = useCallback((text: string) => {
    const ta = textareaRef.current
    if (!ta) return
    const pos = ta.selectionStart
    // Find the start of the current word
    let wordStart = pos
    while (wordStart > 0 && /[a-zA-Z0-9_.]/.test(sql[wordStart - 1])) wordStart--
    const newVal = sql.slice(0, wordStart) + text + sql.slice(pos)
    onChange(newVal)
    setShowAutocomplete(false)
    requestAnimationFrame(() => {
      const newPos = wordStart + text.length
      ta.selectionStart = ta.selectionEnd = newPos
      ta.focus()
      updateCursor()
    })
  }, [sql, onChange, updateCursor])

  // Get the current word fragment for autocomplete
  const currentWord = useMemo(() => {
    let wordStart = cursorPos
    while (wordStart > 0 && /[a-zA-Z0-9_.]/.test(sql[wordStart - 1])) wordStart--
    return sql.slice(wordStart, cursorPos)
  }, [sql, cursorPos])

  // Get the context word before the current word (for column suggestions)
  const contextBefore = useMemo(() => {
    // Look for the keyword before the current word
    const before = sql.slice(0, cursorPos).replace(/[a-zA-Z0-9_.]*$/, '').trimEnd()
    const lastWord = before.split(/\s+/).pop()?.toUpperCase() || ''
    return lastWord
  }, [sql, cursorPos])

  // Auto-show autocomplete when typing after certain keywords
  useEffect(() => {
    if (currentWord.length >= 2 && !showAutocomplete) {
      const triggerKeywords = ['FROM', 'JOIN', 'INTO', 'UPDATE', 'TABLE']
      if (triggerKeywords.includes(contextBefore) || currentWord.includes('.')) {
        setShowAutocomplete(true)
      }
    }
    if (currentWord.length === 0) {
      setShowAutocomplete(false)
    }
  }, [currentWord, contextBefore, showAutocomplete])

  // Compute autocomplete position (approximate)
  const autocompletePos = useMemo(() => {
    if (!textareaRef.current) return { top: 0, left: 0 }
    const ta = textareaRef.current
    const lines = sql.slice(0, cursorPos).split('\n')
    const lineIdx = lines.length - 1
    const colIdx = lines[lineIdx].length
    const lineHeight = 20.8 // 13px * 1.6
    const charWidth = 7.8 // approximate monospace char width at 13px
    const top = (lineIdx + 1) * lineHeight - ta.scrollTop + 4
    const left = colIdx * charWidth + 48 - ta.scrollLeft // 48 = gutter width + padding
    return { top: Math.max(top, 20), left: Math.max(left, 48) }
  }, [sql, cursorPos])

  return (
    <div className="sql-editor-container">
      {/* Line number gutter */}
      <div className="sql-line-numbers" ref={gutterRef}>
        {lineNumbers.map((num, i) => (
          <div key={i} className="sql-line-num">{num}</div>
        ))}
      </div>

      {/* Editor area */}
      <div className="sql-editor-scroll">
        {/* Hidden textarea for input */}
        <textarea
          ref={textareaRef}
          className="sql-editor-input"
          value={sql}
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onKeyUp={updateCursor}
          onClick={updateCursor}
          onScroll={syncScroll}
          placeholder={'SELECT * FROM my_table WHERE id = 1;\n\n-- Cmd+Enter to execute'}
          spellCheck={false}
          autoCapitalize="off"
          autoCorrect="off"
        />

        {/* Highlighted overlay */}
        <pre
          ref={highlightRef}
          className="sql-editor-highlight"
          dangerouslySetInnerHTML={{ __html: highlightedHTML + '\n' }}
        />
      </div>

      {/* Autocomplete dropdown */}
      {showAutocomplete && tables && (
        <SQLAutocomplete
          currentWord={currentWord}
          contextBefore={contextBefore}
          tables={tables}
          getTableSchema={getTableSchema}
          sql={sql}
          cursorPos={cursorPos}
          position={autocompletePos}
          onComplete={handleComplete}
          onDismiss={() => setShowAutocomplete(false)}
        />
      )}
    </div>
  )
}
