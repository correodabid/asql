import type { Dispatch, SetStateAction } from 'react'
import { clone, type SchemaColumn, type SchemaModel, type SchemaTable } from '../schema'
import { IconDatabase, IconKey, IconLink, IconPlus, IconTable, IconTrash } from './Icons'

type Props = {
  model: SchemaModel
  setModel: Dispatch<SetStateAction<SchemaModel>>
  selectedTable: number
  setSelectedTable: Dispatch<SetStateAction<number>>
  setSelectedColumn: Dispatch<SetStateAction<number>>
  normalizeSelection: (next: SchemaModel) => void
}

function columnBadge(col: SchemaColumn) {
  if (col.primary_key) return <span className="sidebar-badge pk" title="Primary Key"><IconKey /></span>
  if (col.references) return <span className="sidebar-badge fk" title="Foreign Key"><IconLink /></span>
  return null
}

function typeLabel(type: string) {
  const map: Record<string, string> = { INT: 'int', TEXT: 'text', JSON: 'json', BOOL: 'bool', TIMESTAMP: 'ts' }
  return map[type] || type.toLowerCase()
}

export function Sidebar({ model, setModel, selectedTable, setSelectedTable, setSelectedColumn, normalizeSelection }: Props) {
  const addTable = () => {
    setModel((current) => {
      const next = clone(current)
      next.tables.push({
        name: `table_${next.tables.length + 1}`,
        columns: [{ name: 'id', type: 'INT', nullable: false, primary_key: true, unique: false, default_value: '' }],
      })
      normalizeSelection(next)
      return next
    })
    setSelectedTable(model.tables.length)
    setSelectedColumn(0)
  }

  const removeTable = (index: number, e: React.MouseEvent) => {
    e.stopPropagation()
    if (model.tables.length <= 1) return
    setModel((current) => {
      const next = clone(current)
      next.tables.splice(index, 1)
      normalizeSelection(next)
      return next
    })
  }

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <div className="sidebar-logo">
          <IconDatabase />
          <span className="sidebar-domain">{model.domain || 'untitled'}</span>
        </div>
      </div>

      <div className="sidebar-section-label">
        <span>Tables</span>
        <button className="sidebar-icon-btn" onClick={addTable} title="Add table">
          <IconPlus />
        </button>
      </div>

      <nav className="sidebar-tree">
        {model.tables.map((table: SchemaTable, tableIndex: number) => (
          <div key={`${table.name}-${tableIndex}`} className="sidebar-tree-group">
            <button
              className={`sidebar-tree-item table-item ${tableIndex === selectedTable ? 'active' : ''}`}
              onClick={() => { setSelectedTable(tableIndex); setSelectedColumn(0) }}
            >
              <span className="sidebar-tree-icon"><IconTable /></span>
              <span className="sidebar-tree-label">{table.name || 'untitled'}</span>
              <span className="sidebar-tree-meta">{table.columns.length}</span>
              {model.tables.length > 1 && (
                <button className="sidebar-icon-btn danger" onClick={(e) => removeTable(tableIndex, e)} title="Remove table">
                  <IconTrash />
                </button>
              )}
            </button>

            {tableIndex === selectedTable && (
              <div className="sidebar-columns">
                {table.columns.map((col: SchemaColumn, colIndex: number) => (
                  <button
                    key={`${col.name}-${colIndex}`}
                    className="sidebar-tree-item column-item"
                    onClick={() => setSelectedColumn(colIndex)}
                  >
                    {columnBadge(col)}
                    <span className="sidebar-col-name">{col.name}</span>
                    <span className="sidebar-col-type">{typeLabel(col.type)}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
        ))}
      </nav>
    </aside>
  )
}
