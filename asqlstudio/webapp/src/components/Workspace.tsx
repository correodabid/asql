import { useEffect, useState } from 'react'
import { useSchemaCache } from '../hooks/useSchemaCache'
import { useWorkspace } from '../hooks/useWorkspace'
import { useInlineEdit } from '../hooks/useInlineEdit'
import { useResizable } from '../hooks/useResizable'
import { useSavedQueries } from '../hooks/useSavedQueries'
import { formatSQLValue } from '../lib/sql'
import { formatSQL } from '../lib/sqlFormatter'
import { WorkspaceTabBar } from './WorkspaceTabBar'
import { WorkspaceToolbar } from './WorkspaceToolbar'
import { WorkspaceEditor } from './WorkspaceEditor'
import { WorkspaceResults } from './WorkspaceResults'
import { WorkspaceTableSidebar } from './WorkspaceTableSidebar'
import { SavedQueries } from './SavedQueries'
import { ImportDialog } from './ImportDialog'
import { QueryBuilder } from './QueryBuilder'
import { DetailPanel } from './DetailPanel'
import { ContextMenu } from './ContextMenu'
import { ResizeHandle } from './ResizeHandle'
import type { HistoryEntry, TableInfo } from '../types/workspace'

type Props = {
  domain: string
}

export function Workspace({ domain }: Props) {
  const schema = useSchemaCache(domain)
  const workspace = useWorkspace(domain)
  const savedQueries = useSavedQueries()
  const tab = workspace.activeTab
  const [showHistory, setShowHistory] = useState(false)
  const [showImport, setShowImport] = useState(false)
  const [showSaved, setShowSaved] = useState(false)
  const [showBuilder, setShowBuilder] = useState(false)

  const sidebarResize = useResizable({ key: 'ws-sidebar', initial: 200, min: 140, max: 400, direction: 'horizontal' })
  const detailResize = useResizable({ key: 'ws-detail', initial: 320, min: 250, max: 600, direction: 'horizontal' })
  const historyResize = useResizable({ key: 'ws-history', initial: 200, min: 140, max: 360, direction: 'horizontal' })

  const inlineEdit = useInlineEdit({
    result: tab.result,
    tableName: tab.tableName,
    pkColumns: tab.tableName ? schema.getPKColumns(tab.tableName) : [],
    onSetSql: (sql) => workspace.setTabSql(tab.id, sql),
  })

  // Listen for global keyboard shortcuts (Cmd+N, Cmd+W)
  useEffect(() => {
    const onNewTab = () => workspace.addTab()
    const onCloseTab = () => workspace.closeTab(workspace.activeTabId)
    window.addEventListener('asql:new-tab', onNewTab)
    window.addEventListener('asql:close-tab', onCloseTab)
    return () => {
      window.removeEventListener('asql:new-tab', onNewTab)
      window.removeEventListener('asql:close-tab', onCloseTab)
    }
  }, [workspace.activeTabId, workspace.addTab, workspace.closeTab])

  // Load tables on mount and domain change
  useEffect(() => {
    schema.loadTables()
    schema.loadBaseline()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [domain])

  // Load table counts when tables are available
  useEffect(() => {
    if (schema.tables.length > 0) {
      schema.loadTableCounts(schema.tables.map(t => t.name))
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [schema.tables])

  const handleNavigateFK = (table: string, column: string, value: unknown) => {
    const sql = `SELECT * FROM ${table} WHERE ${column} = ${formatSQLValue(value)} LIMIT 100;`
    workspace.navigateToQuery(sql, table)
  }

  const detailOpen = workspace.detailPanelOpen && tab.result && tab.selectedRow !== null && tab.tableName

  return (
    <div
      className="workspace-layout"
      style={{
        gridTemplateColumns: `${sidebarResize.size}px auto 1fr ${detailOpen ? `auto ${detailResize.size}px` : ''} ${showHistory ? `auto ${historyResize.size}px` : ''}`,
      }}
    >
      {/* Left: Table Sidebar */}
      <WorkspaceTableSidebar
        tables={schema.tables}
        loading={schema.loading}
        onRefresh={() => schema.loadTables()}
        onSelectTable={(name) => workspace.selectTableIntoTab(name)}
        activeTableName={tab.tableName}
        getTableSchema={(name) => schema.getTable(name)}
        tableCounts={schema.tableCounts}
        domain={domain}
      />
      <ResizeHandle direction="horizontal" onMouseDown={sidebarResize.startDrag} />

      {/* Center: Editor + Results */}
      <div className="workspace-center">
        <WorkspaceTabBar
          tabs={workspace.tabs}
          activeTabId={workspace.activeTabId}
          onSelect={workspace.setActiveTabId}
          onClose={workspace.closeTab}
          onAdd={workspace.addTab}
        />

        <WorkspaceToolbar
          loading={tab.loading}
          sql={tab.sql}
          explainEnabled={tab.explainEnabled}
          txState={workspace.txState}
          onRun={() => workspace.executeTab(tab.id)}
          onToggleExplain={(enabled) => workspace.setTabExplainEnabled(tab.id, enabled)}
          onFormat={() => workspace.setTabSql(tab.id, formatSQL(tab.sql))}
          onBegin={workspace.beginTransaction}
          onCommit={workspace.commitTransaction}
          onRollback={workspace.rollbackTransaction}
          showHistory={showHistory}
          onToggleHistory={() => setShowHistory(!showHistory)}
          onImport={() => setShowImport(true)}
          onSaved={() => setShowSaved(true)}
          onBuilder={() => setShowBuilder(true)}
        />

        <WorkspaceEditor
          sql={tab.sql}
          onChange={(sql) => workspace.setTabSql(tab.id, sql)}
          onExecute={() => workspace.executeTab(tab.id)}
          loading={tab.loading}
          tables={schema.tables}
          getTableSchema={(name) => schema.getTable(name)}
        />

        <WorkspaceResults
          result={tab.result}
          results={tab.results}
          error={tab.error}
          loading={tab.loading}
          explainEnabled={tab.explainEnabled}
          explainPlan={tab.explainPlan}
          selectedRow={tab.selectedRow}
          onRowClick={(i) => workspace.setSelectedRow(tab.id, i)}
          onRowView={(i) => {
            workspace.setSelectedRow(tab.id, i)
            workspace.setDetailPanelOpen(true)
          }}
          editingCell={inlineEdit.editingCell}
          onCellDoubleClick={inlineEdit.startEdit}
          onCellEditChange={inlineEdit.setEditingValue}
          onCellEditCommit={inlineEdit.commitEdit}
          onCellEditCancel={inlineEdit.cancelEdit}
          onContextMenu={inlineEdit.handleContextMenu}
          onAddRow={inlineEdit.generateAddRow}
          tableName={tab.tableName}
          tableSchema={tab.tableName ? schema.getTable(tab.tableName) : undefined}
        />

        {/* Context menu */}
        {inlineEdit.contextMenu && (
          <ContextMenu
            x={inlineEdit.contextMenu.x}
            y={inlineEdit.contextMenu.y}
            onAction={inlineEdit.contextAction}
            onClose={inlineEdit.closeContextMenu}
          />
        )}

        {/* Import Dialog */}
        {showImport && (
          <ImportDialog
            tables={schema.tables}
            onInsertSQL={(sql) => workspace.setTabSql(tab.id, sql)}
            onClose={() => setShowImport(false)}
          />
        )}

        {/* Saved Queries Panel */}
        {showSaved && (
          <div className="saved-queries-overlay" onClick={(e) => { if (e.target === e.currentTarget) setShowSaved(false) }}>
            <div className="saved-queries-dialog">
              <div className="saved-queries-header">
                <span className="saved-queries-title">Saved Queries</span>
                <button className="icon-btn" onClick={() => setShowSaved(false)}>x</button>
              </div>
              <SavedQueries
                queries={savedQueries.queries}
                currentSQL={tab.sql}
                onLoad={(sql) => { workspace.setTabSql(tab.id, sql); setShowSaved(false) }}
                onSave={savedQueries.addQuery}
                onUpdate={savedQueries.updateQuery}
                onDelete={savedQueries.deleteQuery}
                onDuplicate={savedQueries.duplicateQuery}
              />
            </div>
          </div>
        )}

        {/* Query Builder */}
        {showBuilder && (
          <QueryBuilder
            tables={schema.tables}
            getTableSchema={(name) => schema.getTable(name)}
            onGenerateSQL={(sql) => { workspace.setTabSql(tab.id, sql); setShowBuilder(false) }}
            onClose={() => setShowBuilder(false)}
          />
        )}
      </div>

      {/* Right: Detail Panel */}
      {detailOpen && tab.tableName && (
        <>
          <ResizeHandle direction="horizontal" onMouseDown={detailResize.startDragInverse} />
          <DetailPanel
            result={tab.result!}
            selectedRow={tab.selectedRow!}
            tableName={tab.tableName}
            pkColumns={schema.getPKColumns(tab.tableName)}
            foreignKeys={schema.getForeignKeys(tab.tableName)}
            referencedBy={schema.getReferencedBy(tab.tableName)}
            domain={domain}
            entity={schema.getEntityForTable(tab.tableName)}
            onNavigateFK={handleNavigateFK}
            onClose={() => workspace.setDetailPanelOpen(false)}
            onLoadBaseline={() => schema.loadBaseline()}
          />
        </>
      )}

      {/* Sidebar: Favorites + History (toggle via toolbar) */}
      {showHistory && (
        <>
          <ResizeHandle direction="horizontal" onMouseDown={historyResize.startDragInverse} />
          <WorkspaceHistorySidebar
            history={workspace.history}
            favorites={workspace.favorites}
            onSelectSql={(sql) => workspace.setTabSql(tab.id, sql)}
            onToggleFavorite={workspace.toggleFavorite}
            onClearHistory={workspace.clearHistory}
          />
        </>
      )}
    </div>
  )
}

// ─── History Sidebar (inline) ─────────────────────────────

import { IconStar, IconStarFilled, IconTrash } from './Icons'

function WorkspaceHistorySidebar({
  history,
  favorites,
  onSelectSql,
  onToggleFavorite,
  onClearHistory,
}: {
  history: HistoryEntry[]
  favorites: HistoryEntry[]
  onSelectSql: (sql: string) => void
  onToggleFavorite: (entry: HistoryEntry) => void
  onClearHistory: () => void
}) {
  const isFavorite = (sql: string) => favorites.some((f) => f.sql === sql)

  return (
    <div className="ws-history">
      {/* Favorites */}
      {favorites.length > 0 && (
        <>
          <div className="editor-label" style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '0 12px 8px' }}>
            <IconStarFilled /> Favorites
          </div>
          <div className="history-list" style={{ marginBottom: 8 }}>
            {favorites.map((entry, i) => (
              <div key={`fav-${i}`} className="history-item">
                <button className="history-star on" onClick={() => onToggleFavorite(entry)}>
                  <IconStarFilled />
                </button>
                <button className="history-item-body" onClick={() => onSelectSql(entry.sql)} title={entry.sql}>
                  <span className="history-sql">{entry.sql}</span>
                </button>
              </div>
            ))}
          </div>
          <div style={{ height: 1, background: 'var(--border)', margin: '0 12px 8px' }} />
        </>
      )}

      {/* History */}
      <div className="history-header">
        <span className="editor-label">History</span>
        {history.length > 0 && (
          <button className="icon-btn danger" onClick={onClearHistory} title="Clear history">
            <IconTrash />
          </button>
        )}
      </div>
      <div className="history-list">
        {history.length === 0 && (
          <div className="text-muted" style={{ padding: 12, textAlign: 'center' }}>No queries yet</div>
        )}
        {history.map((entry, i) => (
          <div key={i} className={`history-item ${entry.ok ? '' : 'failed'}`}>
            <button className={`history-star ${isFavorite(entry.sql) ? 'on' : ''}`} onClick={() => onToggleFavorite(entry)}>
              {isFavorite(entry.sql) ? <IconStarFilled /> : <IconStar />}
            </button>
            <button className="history-item-body" onClick={() => onSelectSql(entry.sql)} title={entry.sql}>
              <span className={`history-dot ${entry.ok ? 'ok' : 'err'}`} />
              <span className="history-sql">{entry.sql}</span>
              <span className="history-time">{entry.duration.toFixed(0)}ms</span>
            </button>
          </div>
        ))}
      </div>
    </div>
  )
}

// Re-export workspace hook types for convenience
export type { TableInfo }
