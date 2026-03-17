import { Component, type ErrorInfo, useEffect, useState } from 'react'
import { ClusterPanel } from './components/ClusterPanel'
import { CommandPalette } from './components/CommandPalette'
import { ConnectionDialog, type ConnectionConfig, type ConnectionSwitchRequest } from './components/ConnectionDialog'
import { Dashboard } from './components/Dashboard'
import { EntityExplorer } from './components/EntityExplorer'
import { DesignerWorkbench } from './components/DesignerWorkbench'
import { ERDiagram } from './components/ERDiagram'
import { FixturePanel } from './components/FixturePanel'
import { IconDatabase, IconDownload, IconGrid, IconKey, IconLayers, IconMoon, IconRefresh, IconSchema, IconShield, IconSQLDoc, IconSun, IconTerminal, IconTimeline, IconZap } from './components/Icons'
import { KeyboardShortcuts } from './components/KeyboardShortcuts'
import { RecoveryPanel } from './components/RecoveryPanel'
import { SecurityPanel } from './components/SecurityPanel'
import { SchemaPanel } from './components/SchemaPanel'
import { StartHerePanel } from './components/StartHerePanel'
import { StatusBar } from './components/StatusBar'
import { TabBar, type GroupDef, type TabId } from './components/Tabs'
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
import { rememberRecentConnection } from './lib/connectionHistory'
import { api } from './lib/api'
import './App.css'

const GROUPS: GroupDef[] = [
  { kind: 'standalone', id: 'home', label: 'Start Here', icon: <IconZap /> },
  {
    kind: 'group', id: 'query', label: 'Query', icon: <IconTerminal />,
    items: [
      { id: 'workspace',     label: 'Workspace',     icon: <IconTerminal /> },
      { id: 'time-explorer', label: 'Time Explorer', icon: <IconTimeline /> },
      { id: 'entities',      label: 'Entities',      icon: <IconLayers /> },
    ],
  },
  {
    kind: 'group', id: 'schema', label: 'Schema', icon: <IconSchema />,
    items: [
      { id: 'designer',    label: 'Builder',    icon: <IconSchema /> },
      { id: 'schema-ddl', label: 'Schema DDL', icon: <IconSQLDoc /> },
    ],
  },
  {
    kind: 'group', id: 'ops', label: 'Ops', icon: <IconGrid />,
    items: [
      { id: 'dashboard', label: 'Dashboard', icon: <IconGrid /> },
      { id: 'cluster',   label: 'Cluster',   icon: <IconShield /> },
      { id: 'security',  label: 'Security',  icon: <IconKey /> },
      { id: 'recovery',  label: 'Recovery',  icon: <IconRefresh /> },
      { id: 'fixtures',  label: 'Fixtures',  icon: <IconDownload /> },
    ],
  },
]

const QUERY_HISTORY_KEY = 'asql_query_history'

function readQueryHistoryCount() {
  try {
    const raw = localStorage.getItem(QUERY_HISTORY_KEY)
    if (!raw) return 0
    const parsed = JSON.parse(raw)
    return Array.isArray(parsed) ? parsed.length : 0
  } catch {
    return 0
  }
}

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
  const [activeTab, setActiveTab] = useState<TabId>('home')
  const [connectionEpoch, setConnectionEpoch] = useState(0)
  const [connectionInfo, setConnectionInfo] = useState<ConnectionConfig | null>(null)
  const [showConnectionDialog, setShowConnectionDialog] = useState(false)
  const [connectionBusy, setConnectionBusy] = useState(false)
  const [connectionError, setConnectionError] = useState('')
  const [showShortcuts, setShowShortcuts] = useState(false)
  const [designerTableCounts, setDesignerTableCounts] = useState<Record<string, number>>({})
  const [queryHistoryCount, setQueryHistoryCount] = useState(() => readQueryHistoryCount())
  const { toasts, addToast: _addToast, dismiss: dismissToast } = useToast()
  void _addToast
  const { theme, toggleTheme } = useTheme()
  const heartbeat = useHeartbeat()
  const {
    model,
    setModel,
    undo,
    redo,
    canUndo,
    canRedo,
    selectedTable,
    setSelectedTable,
    selectedColumn,
    setSelectedColumn,
    selectedIndex,
    setSelectedIndex,
    activeTable,
    activeColumn,
    designerStatus,
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
    onSilentDiff,
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
  } = useSchemaStudio(connectionEpoch)

  const { domains, refresh: refreshDomains } = useDomains(connectionEpoch)
  const cmdPalette = useCommandPalette()

  useEffect(() => {
    api<ConnectionConfig>('/api/connection', 'GET')
      .then((resp) => {
        setConnectionInfo(resp)
        rememberRecentConnection(resp)
        setConnectionError('')
      })
      .catch((error) => {
        setConnectionError(error instanceof Error ? error.message : String(error))
      })
  }, [connectionEpoch])

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
        const tabMap: Record<string, TabId> = { '1': 'home', '2': 'workspace', '3': 'designer', '4': 'dashboard', '5': 'cluster', '6': 'time-explorer', '7': 'fixtures', '8': 'recovery', '9': 'security' }
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

  useEffect(() => {
    const syncHistoryCount = () => setQueryHistoryCount(readQueryHistoryCount())
    const handleHistoryUpdate = (event: Event) => {
      const detail = (event as CustomEvent<{ count?: number }>).detail
      if (typeof detail?.count === 'number') {
        setQueryHistoryCount(detail.count)
        return
      }
      syncHistoryCount()
    }

    window.addEventListener('focus', syncHistoryCount)
    window.addEventListener('asql:query-history-updated', handleHistoryUpdate as EventListener)

    return () => {
      window.removeEventListener('focus', syncHistoryCount)
      window.removeEventListener('asql:query-history-updated', handleHistoryUpdate as EventListener)
    }
  }, [])

  // Fetch row counts per table when the canvas designer is active
  useEffect(() => {
    if (activeTab !== 'designer' || isAllDomains) return
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
  }, [activeTab, isAllDomains, model.domain, model.tables.length])

  const groups: GroupDef[] = GROUPS.map((g) => {
    if (g.kind === 'standalone') return g
    return {
      ...g,
      items: g.items.map((item) => ({
        ...item,
        badge: item.id === 'designer' && diffOperations.length > 0 ? diffOperations.length
             : item.id === 'schema-ddl' && diffOperations.length > 0 ? diffOperations.length
             : undefined,
      })),
    }
  })

  const tableCountInScope = isAllDomains
    ? (allDomainsModel?.domains.reduce((n, d) => n + d.tables.length, 0) ?? 0)
    : model.tables.length

  const openDesignerCanvas = () => {
    setActiveTab('designer')
  }

  const openDesignerDDL = () => {
    setActiveTab('schema-ddl')
  }

  const openConnectionDialog = () => {
    setConnectionError('')
    setShowConnectionDialog(true)
  }

  const handleConnectionSwitch = async (request: ConnectionSwitchRequest) => {
    setConnectionBusy(true)
    setConnectionError('')
    try {
      const response = await api<{ status: string; connection: ConnectionConfig }>('/api/connection/switch', 'POST', request)
      setConnectionInfo(response.connection)
      rememberRecentConnection(response.connection)
      setShowConnectionDialog(false)
      setActiveTab('home')
      setConnectionEpoch((value) => value + 1)
      await heartbeat.check()
      void refreshDomains()
    } catch (error) {
      setConnectionError(error instanceof Error ? error.message : String(error))
      throw error
    } finally {
      setConnectionBusy(false)
    }
  }

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
          <button className="title-connection-btn" onClick={openConnectionDialog} title="Switch Studio connection">
            <IconDatabase />
            <span>{connectionInfo?.pgwire_endpoint || 'Connection'}</span>
          </button>
          <button className="title-theme-toggle" onClick={toggleTheme} title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}>
            {theme === 'dark' ? <IconSun /> : <IconMoon />}
          </button>
        </div>
        {loading && <div className="loading-bar" />}
      </header>

      {/* Body */}
      <div className="app-body">
        <div className="main-area">
          <TabBar groups={groups} active={activeTab} onChange={setActiveTab} />

          <div className="main-content" key={connectionEpoch}>
            {activeTab === 'home' && (
              <StartHerePanel
                heartbeatStatus={heartbeat.status}
                heartbeatLatency={heartbeat.latency}
                connectionEndpoint={connectionInfo?.pgwire_endpoint ?? ''}
                currentDomain={isAllDomains ? 'All Domains' : (model.domain || 'default')}
                isAllDomains={isAllDomains}
                domainCount={domains.length}
                tableCount={tableCountInScope}
                diffCount={diffOperations.length}
                queryHistoryCount={queryHistoryCount}
                onNavigate={setActiveTab}
                onOpenConnection={openConnectionDialog}
                onOpenDesignerCanvas={openDesignerCanvas}
                onOpenDesignerDDL={openDesignerDDL}
              />
            )}

            {activeTab === 'workspace' && (
              <Workspace domain={model.domain || 'default'} />
            )}

            {activeTab === 'dashboard' && (
              <DashboardBoundary>
                <Dashboard />
              </DashboardBoundary>
            )}

			{activeTab === 'fixtures' && (
			  <FixturePanel domain={model.domain || 'default'} />
			)}

            {activeTab === 'designer' && (
              <div className="designer-layout-full">
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
                  <DesignerWorkbench
                    model={model}
                    setModel={setModel}
                    selectedTable={selectedTable}
                    setSelectedTable={setSelectedTable}
                    selectedColumn={selectedColumn}
                    setSelectedColumn={setSelectedColumn}
                    selectedIndex={selectedIndex}
                    setSelectedIndex={setSelectedIndex}
                    activeTable={activeTable}
                    activeColumn={activeColumn}
                    updateTable={updateTable}
                    updateColumn={updateColumn}
                    designerTableCounts={designerTableCounts}
                    ddlStatements={ddlStatements}
                    statementStates={statementStates}
                    diffSafe={diffSafe}
                    diffOperations={diffOperations}
                    onGenerateDDL={onGenerateDDL}
                    onSilentDiff={onSilentDiff}
                    onPreviewDiff={onPreviewDiff}
                    onApplySafeDiff={onApplySafeDiff}
                    onExecuteAll={onExecuteAll}
                    onOpenDDL={() => setActiveTab('schema-ddl')}
                    onOpenDiff={() => setActiveTab('schema-ddl')}
                    undo={undo}
                    redo={redo}
                    canUndo={canUndo}
                    canRedo={canRedo}
                    normalizeSelection={normalizeSelection}
                  />
                )}
              </div>
            )}

            {activeTab === 'schema-ddl' && (
              <SchemaPanel
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
                diffSummary={diffSummary}
                diffSafe={diffSafe}
                diffOperations={diffOperations}
                diffWarnings={diffWarnings}
                onApplySelected={onApplySelectedDiff}
              />
            )}

            {activeTab === 'cluster' && <ClusterPanel />}

            {activeTab === 'security' && <SecurityPanel />}

            {activeTab === 'time-explorer' && (
              <TimeExplorer domain={model.domain || 'default'} />
            )}

            {activeTab === 'entities' && (
              <EntityExplorer domain={model.domain || 'default'} />
            )}

            {activeTab === 'recovery' && <RecoveryPanel />}
          </div>
        </div>
      </div>

      <StatusBar
        health={health}
        designerStatus={designerStatus}
        tableCount={tableCountInScope}
        domain={isAllDomains ? 'All Domains' : model.domain}
        heartbeat={heartbeat.status}
        heartbeatLatency={heartbeat.latency}
        endpoint={connectionInfo?.pgwire_endpoint ?? ''}
        onOpenConnection={openConnectionDialog}
      />

      {showConnectionDialog && (
        <ConnectionDialog
          key={connectionInfo?.pgwire_endpoint || 'connection'}
          current={connectionInfo}
          busy={connectionBusy}
          error={connectionError}
          onClose={() => setShowConnectionDialog(false)}
          onSubmit={handleConnectionSwitch}
        />
      )}

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
