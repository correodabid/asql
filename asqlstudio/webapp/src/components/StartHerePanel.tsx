import type { ReactNode } from 'react'
import type { HeartbeatStatus } from '../hooks/useHeartbeat'
import type { TabId } from './Tabs'
import {
  IconActivity,
  IconArrowRight,
  IconCheckCircle,
  IconClock,
  IconDatabase,
  IconDownload,
  IconGrid,
  IconSchema,
  IconShield,
  IconTerminal,
  IconTimeline,
  IconZap,
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

type JourneyStep = {
  id: string
  title: string
  description: string
  done: boolean
  cta: string
  action: () => void
}

type QuickAction = {
  id: string
  title: string
  description: string
  icon: ReactNode
  cta: string
  action: () => void
  tone?: 'default' | 'accent'
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
  const hasConnectedEngine = heartbeatStatus === 'connected'
  const hasSelectedDomain = !isAllDomains && currentDomain.trim() !== ''
  const hasSchema = tableCount > 0
  const hasFirstQuery = queryHistoryCount > 0
  const completedSteps = [hasConnectedEngine, hasSelectedDomain, hasSchema, hasFirstQuery].filter(Boolean).length

  const journey: JourneyStep[] = [
    {
      id: 'engine',
      title: 'Confirm the engine is reachable',
      description: hasConnectedEngine
        ? `Studio is connected${heartbeatLatency != null ? ` · ${heartbeatLatency}ms` : ''}.`
        : 'If the engine is not reachable, the rest of the flow becomes guesswork.',
      done: hasConnectedEngine,
      cta: 'Open Dashboard',
      action: () => onNavigate('dashboard'),
    },
    {
      id: 'domain',
      title: 'Work inside one domain',
      description: hasSelectedDomain
        ? `Current working domain: ${currentDomain}.`
        : 'Pick one domain from the top bar so schema, queries, and history stay focused.',
      done: hasSelectedDomain,
      cta: 'Open Designer',
      action: onOpenDesignerCanvas,
    },
    {
      id: 'schema',
      title: 'Create schema or load a deterministic fixture',
      description: hasSchema
        ? `${tableCount} table(s) already available in the current scope.`
        : 'Use Designer for the schema path or Fixtures for the fastest guided sample-data path.',
      done: hasSchema,
      cta: hasSchema ? 'Review DDL' : 'Load Fixture',
      action: hasSchema ? onOpenDesignerDDL : () => onNavigate('fixtures'),
    },
    {
      id: 'query',
      title: 'Run the first query and see real rows',
      description: hasFirstQuery
        ? `${queryHistoryCount} query run(s) already recorded in Studio history.`
        : 'The adoption moment happens when schema, data, and deterministic behavior connect in one place.',
      done: hasFirstQuery,
      cta: 'Open Workspace',
      action: () => onNavigate('workspace'),
    },
  ]

  const quickActions: QuickAction[] = [
    {
      id: 'designer',
      title: 'Model the schema visually',
      description: 'Use Designer when the team is still learning domains, entities, and safe schema evolution.',
      icon: <IconSchema />,
      cta: 'Open Designer',
      action: onOpenDesignerCanvas,
      tone: 'accent',
    },
    {
      id: 'fixture',
      title: 'Start from deterministic sample data',
      description: 'Fixtures reduce adoption friction because teams can see the shape of the system before wiring APIs or apps.',
      icon: <IconDownload />,
      cta: 'Open Fixtures',
      action: () => onNavigate('fixtures'),
    },
    {
      id: 'workspace',
      title: 'Query the live model',
      description: 'Move to Workspace once you want feedback on SQL, transactions, and result shapes.',
      icon: <IconTerminal />,
      cta: 'Open Workspace',
      action: () => onNavigate('workspace'),
    },
    {
      id: 'time',
      title: 'Show the temporal “aha” moment',
      description: 'Time Explorer is where the deterministic history model becomes obvious to new users.',
      icon: <IconTimeline />,
      cta: 'Open Time Explorer',
      action: () => onNavigate('time-explorer'),
    },
  ]

  const operatorActions: QuickAction[] = [
    {
      id: 'dashboard',
      title: 'Check health and engine signals',
      description: 'Use Dashboard for readiness, metrics, and quick confidence checks.',
      icon: <IconGrid />,
      cta: 'Open Dashboard',
      action: () => onNavigate('dashboard'),
    },
    {
      id: 'cluster',
      title: 'Inspect replication and routing',
      description: 'Cluster is powerful, but it should stay secondary until the base workflow already feels natural.',
      icon: <IconShield />,
      cta: 'Open Cluster',
      action: () => onNavigate('cluster'),
    },
  ]

  return (
    <div className="start-here">
      <section className="start-here-hero">
        <div className="start-here-hero-copy">
          <div className="start-here-kicker">
            <IconZap />
            <span>Start Here</span>
          </div>
          <h1>Make the first ASQL journey feel obvious.</h1>
          <p>
            Studio should teach the product while people use it: choose a domain, shape schema,
            run one real query, then reveal deterministic history.
          </p>
          <div className="start-here-hero-actions">
            <button className="toolbar-btn primary" onClick={() => onNavigate(hasSchema ? 'workspace' : 'fixtures')}>
              {hasSchema ? 'Resume with Workspace' : 'Start with Fixtures'}
            </button>
            <button className="toolbar-btn accent" onClick={onOpenDesignerCanvas}>
              Open Designer
            </button>
          </div>
        </div>

        <div className="start-here-hero-stats">
          <StatCard
            icon={<IconActivity />}
            label="Engine"
            value={hasConnectedEngine ? 'Connected' : heartbeatStatus === 'checking' ? 'Checking' : 'Needs attention'}
            meta={heartbeatLatency != null ? `${heartbeatLatency}ms` : 'health surface'}
            tone={hasConnectedEngine ? 'good' : 'warn'}
          />
          <StatCard
            icon={<IconDatabase />}
            label="Domains"
            value={String(domainCount)}
            meta={hasSelectedDomain ? currentDomain : 'pick a working domain'}
          />
          <StatCard
            icon={<IconSchema />}
            label="Schema"
            value={String(tableCount)}
            meta={tableCount === 1 ? 'table in scope' : 'tables in scope'}
          />
          <StatCard
            icon={<IconClock />}
            label="Adoption progress"
            value={`${completedSteps}/4`}
            meta={diffCount > 0 ? `${diffCount} pending diff op(s)` : 'no pending schema diff'}
            tone={completedSteps >= 3 ? 'good' : 'neutral'}
          />
        </div>
      </section>

      <section className="start-here-grid">
        <div className="start-here-panel">
          <div className="start-here-panel-header">
            <div>
              <h2>Recommended first journey</h2>
              <p>Guide the team to the first useful outcome before exposing advanced surfaces.</p>
            </div>
            <div className="start-here-progress">{completedSteps} of {journey.length} complete</div>
          </div>

          <div className="start-here-steps">
            {journey.map((step, index) => (
              <button key={step.id} className={`start-here-step ${step.done ? 'done' : ''}`} onClick={step.action}>
                <div className="start-here-step-index">
                  {step.done ? <IconCheckCircle /> : <span>{index + 1}</span>}
                </div>
                <div className="start-here-step-copy">
                  <div className="start-here-step-title">{step.title}</div>
                  <div className="start-here-step-description">{step.description}</div>
                </div>
                <div className="start-here-step-action">
                  <span>{step.cta}</span>
                  <IconArrowRight />
                </div>
              </button>
            ))}
          </div>
        </div>

        <div className="start-here-side">
          <div className="start-here-panel">
            <div className="start-here-panel-header compact">
              <div>
                <h2>Quick actions</h2>
                <p>Four high-value paths that reduce adoption friction.</p>
              </div>
            </div>
            <div className="start-here-action-list">
              {quickActions.map((action) => (
                <ActionCard key={action.id} action={action} />
              ))}
            </div>
          </div>

          <div className="start-here-panel start-here-operator-panel">
            <div className="start-here-panel-header compact">
              <div>
                <h2>Operator surfaces</h2>
                <p>Useful, but intentionally secondary to the first-run product journey.</p>
              </div>
            </div>
            <div className="start-here-action-list compact">
              {operatorActions.map((action) => (
                <ActionCard key={action.id} action={action} compact />
              ))}
            </div>
          </div>
        </div>
      </section>
    </div>
  )
}

function StatCard({
  icon,
  label,
  value,
  meta,
  tone = 'neutral',
}: {
  icon: ReactNode
  label: string
  value: string
  meta: string
  tone?: 'neutral' | 'good' | 'warn'
}) {
  return (
    <div className={`start-here-stat tone-${tone}`}>
      <div className="start-here-stat-icon">{icon}</div>
      <div className="start-here-stat-copy">
        <div className="start-here-stat-label">{label}</div>
        <div className="start-here-stat-value">{value}</div>
        <div className="start-here-stat-meta">{meta}</div>
      </div>
    </div>
  )
}

function ActionCard({ action, compact = false }: { action: QuickAction; compact?: boolean }) {
  return (
    <button className={`start-here-action ${action.tone === 'accent' ? 'accent' : ''} ${compact ? 'compact' : ''}`} onClick={action.action}>
      <div className="start-here-action-icon">{action.icon}</div>
      <div className="start-here-action-copy">
        <div className="start-here-action-title">{action.title}</div>
        <div className="start-here-action-description">{action.description}</div>
      </div>
      <div className="start-here-action-cta">
        <span>{action.cta}</span>
        <IconArrowRight />
      </div>
    </button>
  )
}
