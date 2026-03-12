import { Component, type ErrorInfo, useEffect, useState } from 'react'
import { ClusterPanel } from './components/ClusterPanel'
import { ColumnEditor } from './components/ColumnEditor'
import { CommandPalette } from './components/CommandPalette'
import { Dashboard } from './components/Dashboard'
import { DDLPanel } from './components/DDLPanel'
import { DiffPanel } from './components/DiffPanel'
import { ERDiagram } from './components/ERDiagram'
import { IconDatabase, IconDownload, IconGrid, IconMoon, IconSchema, IconShield, IconSun, IconTerminal, IconTimeline } from './components/Icons'
import { IndexEditor } from './components/IndexEditor'
import { KeyboardShortcuts } from './components/KeyboardShortcuts'
import { RecoveryPanel } from './components/RecoveryPanel'
import { Sidebar } from './components/Sidebar'
import { StatusBar } from './components/StatusBar'
import { TabBar, type TabId } from './components/Tabs'
import { TimeExplorer } from './components/TimeExplorer'
import { ToastContainer } from './components/Toast'
import { Workspace } from './components/Workspace'
import { useCommandPalette } from './hooks/useCommandPalette'
import { useDomains } from './hooks/useDomains'
import { useSchemaStudio } from './hooks/useSchemaStudio'
import { ALL_DOMAINS_KEY } from './hooks/useSchemaStudio'
import { useToast } from './hooks/useToast'
import { useTheme } from './hooks/useTheme'
import { useHeartbeat } from './hooks/useHeartbeat'
import { clone } from './schema'
import { api } from './lib/api'
import './App.css'

const TABS: { id: TabId; label: string; icon: React.ReactNode }[] = [
  { id: 'workspace',     label: 'Workspace',     icon: <IconTerminal /> },
  { id: 'designer',     label: 'Designer',      icon: <IconSchema /> },
  { id: 'dashboard',    label: 'Dashboard',     icon: <IconGrid /> },
  { id: 'cluster',      label: 'Cluster',       icon: <IconShield /> },
  { id: 'time-explorer', label: 'Time Explorer', icon: <IconTimeline /> },
  { id: 'recovery',      label: 'Recovery',      icon: <IconDownload /> },
]

type DashboardBoundaryProps = {
  children: React.ReactNode
}

type DashboardBoundaryState = {
  hasError: boolean
  message: string
}

class DashboardBoundary extends Component<DashboardBoundaryProps, DashboardBoundaryState> {
  state: DashboardBoundaryState = {
    hasError: false,
    message: '',
  }

  static getDerivedStateFromError(error: unknown): DashboardBoundaryState {
    const message = error instanceof Error ? error.message : 'Unknown dashboard error'
    return { hasError: true, message }
  }

  componentDidCatch(error: unknown, info: ErrorInfo) {
    console.error('Dashboard render error:', error, info)
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="panel" style={{ margin: 16, padding: 16 }}>
          <h3 style={{ marginTop: 0 }}>Dashboard failed to render</h3>
          <p className="text-muted" style={{ marginBottom: 12 }}>
            Error: {this.state.message}
          </p>
          <button className="toolbar-btn" onClick={() => window.location.reload()}>
            Reload page
          </button>
        </div>
      )
    }

    return this.props.children
  }
}

function App() {
  const [activeTab, setActiveTab] = useState<TabId>('workspace')
  const [showShortcuts, setShowShortcuts] = useState(false)
  const [designerTableCounts, setDesignerTableCounts] = useState<Record<string, number>>({})
  const { toasts, addToast: _addToast, dismiss: dismissToast } = useToast()
  void _addToast
  const { theme, toggleTheme } = useTheme()
  const heartbeat = useHeartbeat()
  const {
    model,
    setModel,
    selectedTable,
    setSelectedTable,
    selectedColumn,
    setSelectedColumn,
    selectedIndex,
    setSelectedIndex,
    activeTable,
    activeColumn,
    designerStatus,
    designerView,
    setDesignerView,
    ddl,
    ddlStatements,
    diffSummary,
    diffSafe,
    diffOperations,
    diffWarnings,
    health,
    loading,
    updateTable,
    updateColumn,
    normalizeSelection,
    onGenerateDDL,
    onLoadBaseline,
    onSetBaseline,
    changeDomain,
    onPreviewDiff,
    onApplySafeDiff,
    onRefreshAutoDiff,
    onRefreshAutoDiffApplySafe,
    allDomainsModel,
    isAllDomains,
    statementStates,
    onExecuteStatement,
    onExecuteAll,
    onApplySelectedDiff,
  } = useSchemaStudio()

  const { domains, refresh: refreshDomains } = useDomains()
  const cmdPalette = useCommandPalette()

  // Global keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      // Cmd+K — command palette (works even in inputs)
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        cmdPalette.toggle()
        return
      }

      // Cmd+1/2/3/4 — main tab switching (works even in inputs)
      if ((e.metaKey || e.ctrlKey) && !e.shiftKey && !e.altKey) {
        const tabMap: Record<string, TabId> = { '1': 'workspace', '2': 'designer', '3': 'dashboard', '4': 'cluster', '5': 'time-explorer', '6': 'recovery' }
        const tab = tabMap[e.key]
        if (tab) {
          e.preventDefault()
          setActiveTab(tab)
          return
        }
      }

      // Cmd+N — new workspace query tab
      if ((e.metaKey || e.ctrlKey) && e.key === 'n' && !e.shiftKey) {
        e.preventDefault()
        setActiveTab('workspace')
        window.dispatchEvent(new CustomEvent('asql:new-tab'))
        return
      }

      // Cmd+W — close current workspace query tab
      if ((e.metaKey || e.ctrlKey) && e.key === 'w' && !e.shiftKey) {
        e.preventDefault()
        window.dispatchEvent(new CustomEvent('asql:close-tab'))
        return
      }

      // Skip remaining shortcuts when focused in an input
      const tag = (e.target as HTMLElement)?.tagName
      if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return

      if (e.key === '?' && !e.metaKey && !e.ctrlKey) {
        e.preventDefault()
        setShowShortcuts((v) => !v)
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [cmdPalette])

  // Fetch row counts per table when the canvas designer is active
  useEffect(() => {
    if (activeTab !== 'designer' || designerView !== 'canvas' || isAllDomains) return
    if (model.tables.length === 0) return
    const domain = model.domain || 'default'
    const fetchCounts = async () => {
      const counts: Record<string, number> = {}
      await Promise.all(
        model.tables.map(async (t) => {
          try {
            const resp = await api<{ rows: Array<{ cnt: number }> }>(
              '/api/read-query',
              'POST',
              { sql: `SELECT COUNT(*) AS cnt FROM "${t.name}";`, domains: [domain], consistency: 'strong' },
            )
            if (resp.rows?.[0]?.cnt !== undefined) counts[t.name] = Number(resp.rows[0].cnt)
          } catch { /* ignore */ }
        }),
      )
      setDesignerTableCounts(counts)
    }
    void fetchCounts()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, designerView, isAllDomains, model.domain, model.tables.length])

  const tabsWithBadges = TABS.map((t) => ({
    ...t,
    badge:
      t.id === 'designer' && diffOperations.length > 0
        ? diffOperations.length
        : undefined,
  }))

  return (
    <div className="app-shell">
      {/* Title bar */}
      <header className="title-bar">
        <div className="title-bar-left">
          <div className="title-logo">
            <IconDatabase />
            <span className="title-text">ASQL Studio</span>
          </div>
          <span className="title-separator" />
          <select
            className="title-domain-select"
            value={model.domain || ''}
            onChange={(e) => changeDomain(e.target.value)}
            onFocus={refreshDomains}
          >
            <option value={ALL_DOMAINS_KEY}>All Domains</option>
            {domains.length === 0 && model.domain !== ALL_DOMAINS_KEY && (
              <option value={model.domain || 'default'}>
                {model.domain || 'default'}
              </option>
            )}
            {domains.map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
          <button className="title-cmd-k" onClick={cmdPalette.toggle} title="Command Palette (Cmd+K)">
            <span className="mono">Cmd+K</span>
          </button>
          <button className="title-theme-toggle" onClick={toggleTheme} title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
            {theme === 'dark' ? <IconSun /> : <IconMoon />}
          </button>
        </div>
        {loading && <div className="loading-bar" />}
      </header>

      {/* Body */}
      <div className="app-body">
        {/* Sidebar only shown for Designer tab (not in all-domains mode) */}
        {activeTab === 'designer' && !isAllDomains && (
          <Sidebar
            model={model}
            setModel={setModel}
            selectedTable={selectedTable}
            setSelectedTable={setSelectedTable}
            setSelectedColumn={setSelectedColumn}
            normalizeSelection={normalizeSelection}
          />
        )}

        <div className="main-area">
          <TabBar tabs={tabsWithBadges} active={activeTab} onChange={setActiveTab} />

          <div className="main-content">
            {activeTab === 'workspace' && (
              <Workspace domain={model.domain || 'default'} />
            )}

            {activeTab === 'dashboard' && (
              <DashboardBoundary>
                <Dashboard />
              </DashboardBoundary>
            )}

            {activeTab === 'designer' && (
              <div className="designer-layout-full">
                {/* Designer sub-nav */}
                {!isAllDomains && (
                <div className="designer-sub-nav">
                  <button
                    className={`designer-sub-btn ${designerView === 'canvas' ? 'active' : ''}`}
                    onClick={() => setDesignerView('canvas')}
                  >
                    Canvas
                  </button>
                  <button
                    className={`designer-sub-btn ${designerView === 'ddl' ? 'active' : ''}`}
                    onClick={() => setDesignerView('ddl')}
                  >
                    DDL
                  </button>
                  <button
                    className={`designer-sub-btn ${designerView === 'diff' ? 'active' : ''}`}
                    onClick={() => setDesignerView('diff')}
                  >
                    Diff
                    {diffOperations.length > 0 && (
                      <span className="tab-badge" style={{ marginLeft: 6 }}>
                        {diffOperations.length}
                      </span>
                    )}
                  </button>
                </div>
                )}

                {isAllDomains ? (
                  <div className="designer-layout" style={{ flex: 1 }}>
                    <div className="designer-canvas" style={{ flex: 1 }}>
                      <ERDiagram
                        model={model}
                        selectedTable={-1}
                        onSelectTable={() => {}}
                        multiModel={allDomainsModel ?? undefined}
                        onDomainClick={(domain) => changeDomain(domain)}
                      />
                    </div>
                  </div>
                ) : (
                <>
                {designerView === 'canvas' && (
                  <div className="designer-layout">
                    <div className="designer-canvas">
                      <ERDiagram
                        model={model}
                        selectedTable={selectedTable}
                        onSelectTable={(i) => {
                          setSelectedTable(i)
                          setSelectedColumn(0)
                        }}
                        tableCounts={designerTableCounts}
                        onAddColumn={(tableName) => {
                          const idx = model.tables.findIndex((t) => t.name === tableName)
                          if (idx !== -1) { setSelectedTable(idx); setSelectedColumn(0) }
                        }}
                        onCreateFK={(fromTable, fromCol, toTable, toCol) => {
                          const tableIdx = model.tables.findIndex((t) => t.name === fromTable)
                          if (tableIdx < 0) return
                          const colIdx = model.tables[tableIdx].columns.findIndex((c) => c.name === fromCol)
                          if (colIdx < 0) return
                          setModel((prev) => {
                            const next = clone(prev)
                            next.tables[tableIdx].columns[colIdx] = {
                              ...next.tables[tableIdx].columns[colIdx],
                              references: { table: toTable, column: toCol },
                            }
                            return next
                          })
                        }}
                      />
                    </div>
                    <div className="designer-editor">
                      <ColumnEditor
                        model={model}
                        setModel={setModel}
                        selectedTable={selectedTable}
                        selectedColumn={selectedColumn}
                        setSelectedColumn={setSelectedColumn}
                        activeTable={activeTable}
                        activeColumn={activeColumn}
                        updateTable={updateTable}
                        updateColumn={updateColumn}
                      />
                      <div className="designer-editor-divider" />
                      <IndexEditor
                        activeTable={activeTable}
                        updateTable={updateTable}
                        selectedIndex={selectedIndex}
                        setSelectedIndex={setSelectedIndex}
                      />
                    </div>
                  </div>
                )}

                {designerView === 'ddl' && (
                  <DDLPanel
                    ddl={ddl}
                    ddlStatements={ddlStatements}
                    statementStates={statementStates}
                    onGenerateDDL={onGenerateDDL}
                    onLoadBaseline={onLoadBaseline}
                    onSetBaseline={onSetBaseline}
                    onPreviewDiff={onPreviewDiff}
                    onApplySafeDiff={onApplySafeDiff}
                    onRefreshAutoDiff={onRefreshAutoDiff}
                    onRefreshAutoDiffApplySafe={onRefreshAutoDiffApplySafe}
                    onExecuteStatement={onExecuteStatement}
                    onExecuteAll={onExecuteAll}
                  />
                )}

                {designerView === 'diff' && (
                  <DiffPanel
                    diffSummary={diffSummary}
                    diffSafe={diffSafe}
                    diffOperations={diffOperations}
                    diffWarnings={diffWarnings}
                    onApplySelected={onApplySelectedDiff}
                    onRefreshDiff={onPreviewDiff}
                  />
                )}
                </>
                )}
              </div>
            )}

            {activeTab === 'cluster' && <ClusterPanel />}

            {activeTab === 'time-explorer' && (
              <TimeExplorer domain={model.domain || 'default'} />
            )}

            {activeTab === 'recovery' && <RecoveryPanel />}
          </div>
        </div>
      </div>

      <StatusBar
        health={health}
        designerStatus={designerStatus}
        tableCount={isAllDomains ? (allDomainsModel?.domains.reduce((n, d) => n + d.tables.length, 0) ?? 0) : model.tables.length}
        domain={isAllDomains ? 'All Domains' : model.domain}
        heartbeat={heartbeat.status}
        heartbeatLatency={heartbeat.latency}
      />

      {/* Command Palette */}
      {cmdPalette.open && (
        <CommandPalette
          tables={[]}
          history={[]}
          favorites={[]}
          onSelectTable={() => setActiveTab('workspace')}
          onSetSql={() => {}}
          onAddTab={() => {}}
          onToggleTimeTravel={() => {}}
          onToggleDetailPanel={() => {}}
          onNavigate={(tab) => setActiveTab(tab)}
          onClose={cmdPalette.close}
        />
      )}

      {/* Keyboard Shortcuts */}
      {showShortcuts && (
        <KeyboardShortcuts onClose={() => setShowShortcuts(false)} />
      )}

      {/* Toast Notifications */}
      <ToastContainer toasts={toasts} onDismiss={dismissToast} />
    </div>
  )
}

export default App
