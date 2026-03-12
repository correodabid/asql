import { useRef, useState } from 'react'
import type { TableInfo } from '../types/workspace'
import { IconKey, IconLink, IconRefresh, IconTable } from './Icons'
import type { SchemaColumn, SchemaIndex } from '../schema'
import { TablePreview } from './TablePreview'

type Props = {
  tables: TableInfo[]
  loading: boolean
  onRefresh: () => void
  onSelectTable: (name: string) => void
  activeTableName: string | null
  getTableSchema?: (name: string) => { columns: SchemaColumn[]; indexes?: SchemaIndex[] } | undefined
  tableCounts?: Record<string, number>
  domain?: string
}

export function WorkspaceTableSidebar({
  tables,
  loading,
  onRefresh,
  onSelectTable,
  activeTableName,
  getTableSchema,
  tableCounts,
  domain,
}: Props) {
  const [expanded, setExpanded] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [hoverPreview, setHoverPreview] = useState<{ name: string; x: number; y: number } | null>(null)
  const hoverTimer = useRef<ReturnType<typeof setTimeout>>(undefined)

  const filteredTables = search
    ? tables.filter((t) => t.name.toLowerCase().includes(search.toLowerCase()))
    : tables

  return (
    <div className="ws-sidebar">
      <div className="ws-sidebar-header">
        <span className="editor-label">Tables</span>
        <button className="icon-btn" onClick={onRefresh} title="Refresh tables" disabled={loading}>
          <IconRefresh />
        </button>
      </div>
      <div className="ws-sidebar-search">
        <input
          className="ws-search-input"
          type="text"
          placeholder="Filter tables..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          spellCheck={false}
        />
      </div>
      <div className="ws-table-list">
        {filteredTables.length === 0 && !loading && (
          <div className="text-muted" style={{ padding: 12, textAlign: 'center', fontSize: 12 }}>
            {search ? 'No matching tables' : 'No tables found'}
          </div>
        )}
        {filteredTables.map((t) => (
          <div key={t.name} className="ws-table-group">
            <button
              className={`ws-table-item ${t.name === activeTableName ? 'active' : ''}`}
              onClick={() => onSelectTable(t.name)}
              onMouseEnter={(e) => {
                if (!domain) return
                const rect = e.currentTarget.getBoundingClientRect()
                clearTimeout(hoverTimer.current)
                hoverTimer.current = setTimeout(() => {
                  setHoverPreview({ name: t.name, x: rect.right + 8, y: rect.top })
                }, 400)
              }}
              onMouseLeave={() => {
                clearTimeout(hoverTimer.current)
                setHoverPreview(null)
              }}
            >
              <span className="ws-table-icon"><IconTable /></span>
              <span className="ws-table-name">{t.name}</span>
              {t.pk_columns.length > 0 && (
                <span className="ws-table-pk">{t.pk_columns.length} PK</span>
              )}
              {tableCounts?.[t.name] !== undefined && (
                <span className="ws-table-count">{tableCounts[t.name].toLocaleString()}</span>
              )}
              <button
                className="ws-expand-btn"
                onClick={(e) => {
                  e.stopPropagation()
                  setExpanded(expanded === t.name ? null : t.name)
                }}
                title="Show columns"
              >
                {expanded === t.name ? '-' : '+'}
              </button>
            </button>
            {expanded === t.name && getTableSchema && (() => {
              const schema = getTableSchema(t.name)
              if (!schema) return null
              return (
                <div className="ws-table-columns">
                  {schema.columns.map((col) => (
                    <div key={col.name} className="ws-col-item">
                      {col.primary_key ? (
                        <span className="sidebar-badge pk"><IconKey /></span>
                      ) : col.references ? (
                        <span className="sidebar-badge fk"><IconLink /></span>
                      ) : (
                        <span className="ws-col-dot" />
                      )}
                      <span className="ws-col-name">{col.name}</span>
                      <span className="ws-col-type">{col.type.toLowerCase()}</span>
                    </div>
                  ))}
                  {schema.indexes && schema.indexes.length > 0 && (
                    <>
                      <div className="ws-idx-label">Indexes</div>
                      {schema.indexes.map((idx) => (
                        <div key={idx.name} className="ws-idx-item">
                          <span className="ws-idx-method">{idx.method}</span>
                          <span className="ws-col-name">{idx.name}</span>
                          <span className="ws-col-type">{idx.columns.join(', ')}</span>
                        </div>
                      ))}
                    </>
                  )}
                </div>
              )
            })()}
          </div>
        ))}
      </div>
      {hoverPreview && domain && (
        <TablePreview
          tableName={hoverPreview.name}
          domain={domain}
          x={hoverPreview.x}
          y={hoverPreview.y}
        />
      )}
    </div>
  )
}
