import type { HeartbeatStatus } from '../hooks/useHeartbeat'
import type { TabId } from './Tabs'
import {
  IconArrowRight,
  IconCheckCircle,
  IconDatabase,
  IconDiff,
  IconDownload,
  IconGrid,
  IconKey,
  IconRefresh,
  IconSchema,
  IconShield,
  IconTerminal,
  IconTimeline,
} from './Icons'

type Props = {
  heartbeatStatus: HeartbeatStatus
  heartbeatLatency: number | null
  currentDomain: string
  isAllDomains: boolean
  domainCount: number
  tableCount: number
  diffCount: number
  queryHistoryCount: number
  onNavigate: (tab: TabId) => void
  onOpenDesignerCanvas: () => void
  onOpenDesignerDDL: () => void
}

export function StartHerePanel({
  heartbeatStatus,
  heartbeatLatency,
  currentDomain,
  isAllDomains,
  domainCount,
  tableCount,
  diffCount,
  queryHistoryCount,
  onNavigate,
  onOpenDesignerCanvas,
  onOpenDesignerDDL,
}: Props) {
  const connected = heartbeatStatus === 'connected'
  const hasDomain  = !isAllDomains && currentDomain.trim() !== ''
  const hasSchema  = tableCount > 0
  const hasQuery   = queryHistoryCount > 0
  const progress   = [connected, hasDomain, hasSchema, hasQuery].filter(Boolean).length

  const steps = [
    {
      id: 'engine',
      title: 'Connect engine',
      desc: connected
        ? `Connected${heartbeatLatency != null ? ` · ${heartbeatLatency}ms` : ''}`
        : 'Engine not reachable — check the endpoint.',
      done: connected,
      cta: 'Dashboard',
      action: () => onNavigate('dashboard'),
    },
    {
      id: 'domain',
      title: 'Pick a domain',
      desc: hasDomain ? `Working in "${currentDomain}"` : 'Select a domain in the top bar.',
      done: hasDomain,
      cta: 'Designer',
      action: onOpenDesignerCanvas,
    },
    {
      id: 'schema',
      title: 'Build schema',
      desc: hasSchema
        ? `${tableCount} table${tableCount === 1 ? '' : 's'} in scope`
        : 'Model tables in Designer or load a fixture.',
      done: hasSchema,
      cta: hasSchema ? 'Review DDL' : 'Fixtures',
      action: hasSchema ? onOpenDesignerDDL : () => onNavigate('fixtures'),
    },
    {
      id: 'query',
      title: 'Run a query',
      desc: hasQuery
        ? `${queryHistoryCount} quer${queryHistoryCount === 1 ? 'y' : 'ies'} in history`
        : 'Write SQL and see deterministic results.',
      done: hasQuery,
      cta: 'Workspace',
      action: () => onNavigate('workspace'),
    },
  ]

  const nav = [
    { id: 'workspace',     icon: <IconTerminal />, label: 'Workspace',     hint: 'SQL queries',          action: () => onNavigate('workspace'),     featured: false },
    { id: 'time-explorer', icon: <IconTimeline />, label: 'Time Explorer', hint: 'Temporal history',      action: () => onNavigate('time-explorer'), featured: false },
    { id: 'designer',      icon: <IconSchema />,   label: 'Designer',      hint: 'Schema canvas',         action: onOpenDesignerCanvas,             featured: true  },
    { id: 'fixtures',      icon: <IconDownload />, label: 'Fixtures',      hint: 'Sample data',           action: () => onNavigate('fixtures'),     featured: false },
    { id: 'dashboard',     icon: <IconGrid />,     label: 'Dashboard',     hint: 'Engine metrics',        action: () => onNavigate('dashboard'),    featured: false },
    { id: 'cluster',       icon: <IconShield />,   label: 'Cluster',       hint: 'Replication & routing', action: () => onNavigate('cluster'),      featured: false },
    { id: 'security',      icon: <IconKey />,      label: 'Security',      hint: 'Users, roles, grants',  action: () => onNavigate('security'),     featured: false },
    { id: 'recovery',      icon: <IconRefresh />,  label: 'Recovery',      hint: 'WAL replay',            action: () => onNavigate('recovery'),     featured: false },
  ]

  const primaryLabel  = hasSchema || hasQuery ? 'Open Workspace' : 'Load Fixture'
  const primaryAction = hasSchema || hasQuery ? () => onNavigate('workspace') : () => onNavigate('fixtures')
  const statusCls     = connected ? 'sh-status--ok' : heartbeatStatus === 'checking' ? 'sh-status--checking' : 'sh-status--error'

  return (
    <div className="sh-page">

      {/* ── Status strip ──────────────────────────────── */}
      <div className={`sh-status ${statusCls}`}>
        <div className="sh-status-left">
          <span className="sh-status-dot" />
          <span className="sh-status-label">
            {connected ? 'Engine connected' : heartbeatStatus === 'checking' ? 'Checking…' : 'Engine unreachable'}
          </span>
          {heartbeatLatency != null && <span className="sh-status-sep">·</span>}
          {heartbeatLatency != null && <span className="sh-status-latency">{heartbeatLatency}ms</span>}
        </div>

        <div className="sh-status-chips">
          <span className="sh-chip">
            <IconDatabase />
            {isAllDomains ? `${domainCount} domain${domainCount !== 1 ? 's' : ''}` : currentDomain}
          </span>
          <span className="sh-chip"><IconSchema /> {tableCount} table{tableCount !== 1 ? 's' : ''}</span>
          {queryHistoryCount > 0 && (
            <span className="sh-chip"><IconTerminal /> {queryHistoryCount} quer{queryHistoryCount === 1 ? 'y' : 'ies'}</span>
          )}
          {diffCount > 0 && (
            <span className="sh-chip sh-chip--warn"><IconDiff /> {diffCount} pending</span>
          )}
        </div>

        <div className="sh-status-right">
          <button className="toolbar-btn primary" onClick={primaryAction}>{primaryLabel}</button>
        </div>
      </div>

      {/* ── Adoption stepper ──────────────────────────── */}
      <div className="sh-stepper-block">
        <div className="sh-stepper-header">
          <span className="sh-stepper-title">Adoption checklist</span>
          <div className="sh-stepper-header-right">
            <span className="sh-stepper-fraction">{progress} / 4</span>
            <div className="sh-progress-bar">
              <div className="sh-progress-fill" style={{ width: `${(progress / 4) * 100}%` }} />
            </div>
          </div>
        </div>
        <div className="sh-stepper">
          {steps.map((step, i) => (
            <button
              key={step.id}
              className={`sh-step${step.done ? ' sh-step--done' : i === progress ? ' sh-step--active' : ''}`}
              onClick={step.action}
            >
              <div className="sh-step-num">
                {step.done ? <IconCheckCircle /> : <span>{i + 1}</span>}
              </div>
              <div className="sh-step-body">
                <div className="sh-step-title">{step.title}</div>
                <div className="sh-step-desc">{step.desc}</div>
              </div>
              <div className="sh-step-cta">
                <span>{step.cta}</span>
                <IconArrowRight />
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* ── Navigate to ───────────────────────────────── */}
      <div className="sh-nav-block">
        <div className="sh-nav-heading">Go to</div>
        <div className="sh-nav-grid">
          {nav.map((item) => (
            <button
              key={item.id}
              className={`sh-nav-tile${item.featured ? ' sh-nav-tile--featured' : ''}`}
              onClick={item.action}
            >
              <div className="sh-nav-icon">{item.icon}</div>
              <div className="sh-nav-text">
                <div className="sh-nav-name">{item.label}</div>
                <div className="sh-nav-hint">{item.hint}</div>
              </div>
              <div className="sh-nav-arrow"><IconArrowRight /></div>
            </button>
          ))}
        </div>
      </div>

    </div>
  )
}
