import { useCallback, useState } from 'react'
import { formatCell, formatSQLValue, generateDelete, generateInsert, generateUpdate } from '../lib/sql'
import type { CellEdit, QueryResult } from '../types/workspace'

type UseInlineEditOptions = {
  result: QueryResult | null
  tableName: string | null
  pkColumns: string[]
  onSetSql: (sql: string) => void
}

export function useInlineEdit({ result, tableName, pkColumns, onSetSql }: UseInlineEditOptions) {
  const [editingCell, setEditingCell] = useState<CellEdit | null>(null)
  const [contextMenu, setContextMenu] = useState<{
    x: number
    y: number
    rowIndex: number
  } | null>(null)

  const startEdit = useCallback(
    (rowIndex: number, columnName: string) => {
      if (!result || !tableName) return
      const row = result.rows[rowIndex]
      if (!row) return
      setEditingCell({
        rowIndex,
        columnName,
        originalValue: row[columnName],
        currentValue: formatCell(row[columnName]),
      })
    },
    [result, tableName],
  )

  const commitEdit = useCallback(() => {
    if (!editingCell || !result || !tableName) return
    const row = result.rows[editingCell.rowIndex]
    if (!row) return

    // Build UPDATE SQL with the new value
    const updatedRow = { ...row, [editingCell.columnName]: editingCell.currentValue }
    const sql = generateUpdate(tableName, updatedRow, pkColumns)
    onSetSql(sql)
    setEditingCell(null)
  }, [editingCell, result, tableName, pkColumns, onSetSql])

  const cancelEdit = useCallback(() => {
    setEditingCell(null)
  }, [])

  const handleContextMenu = useCallback(
    (e: React.MouseEvent, rowIndex: number) => {
      if (!tableName) return
      e.preventDefault()
      setContextMenu({ x: e.clientX, y: e.clientY, rowIndex })
    },
    [tableName],
  )

  const closeContextMenu = useCallback(() => {
    setContextMenu(null)
  }, [])

  const contextAction = useCallback(
    (action: 'insert' | 'update' | 'delete') => {
      if (!contextMenu || !result || !tableName) return
      const row = result.rows[contextMenu.rowIndex]
      if (!row) return

      let sql = ''
      switch (action) {
        case 'insert':
          sql = generateInsert(tableName, row)
          break
        case 'update':
          sql = generateUpdate(tableName, row, pkColumns)
          break
        case 'delete':
          sql = generateDelete(tableName, row, pkColumns)
          break
      }
      onSetSql(sql)
      setContextMenu(null)
    },
    [contextMenu, result, tableName, pkColumns, onSetSql],
  )

  const generateAddRow = useCallback(() => {
    if (!result || !tableName || result.columns.length === 0) return
    const placeholders: Record<string, unknown> = {}
    for (const col of result.columns) {
      placeholders[col] = `<${col}>`
    }
    const cols = result.columns.join(', ')
    const vals = result.columns.map((c) => formatSQLValue(placeholders[c])).join(', ')
    onSetSql(`INSERT INTO ${tableName} (${cols}) VALUES (${vals});`)
  }, [result, tableName, onSetSql])

  return {
    editingCell,
    startEdit,
    commitEdit,
    cancelEdit,
    setEditingValue: (value: string) =>
      setEditingCell((prev) => (prev ? { ...prev, currentValue: value } : null)),
    contextMenu,
    handleContextMenu,
    closeContextMenu,
    contextAction,
    generateAddRow,
  }
}
