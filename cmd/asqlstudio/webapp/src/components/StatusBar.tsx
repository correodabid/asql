import { IconDot } from './Icons'
import type { HeartbeatStatus } from '../hooks/useHeartbeat'

type Props = {
  health: string
  designerStatus: string
  tableCount: number
  domain: string
  heartbeat?: HeartbeatStatus
  heartbeatLatency?: number | null
}

export function StatusBar({ health, designerStatus, tableCount, domain, heartbeat, heartbeatLatency }: Props) {
  const dotColor = heartbeat === 'connected'
    ? 'var(--text-safe)'
    : heartbeat === 'disconnected'
      ? 'var(--text-unsafe)'
      : 'var(--text-warning)'

  const statusLabel = heartbeat === 'connected'
    ? health
    : heartbeat === 'disconnected'
      ? 'Disconnected'
      : 'Connecting...'

  return (
    <footer className="status-bar">
      <div className="status-left">
        <span className="status-item">
          <IconDot color={dotColor} />
          <span>{statusLabel}</span>
        </span>
        {heartbeat === 'connected' && heartbeatLatency !== null && heartbeatLatency !== undefined && (
          <>
            <span className="status-separator" />
            <span className="status-item status-hint">{heartbeatLatency}ms</span>
          </>
        )}
        <span className="status-separator" />
        <span className="status-item">{designerStatus}</span>
      </div>
      <div className="status-right">
        <span className="status-item status-hint">Cmd+K commands</span>
        <span className="status-separator" />
        <span className="status-item status-hint">? shortcuts</span>
        <span className="status-separator" />
        <span className="status-item">Domain: {domain || '\u2014'}</span>
        <span className="status-separator" />
        <span className="status-item">{tableCount} table{tableCount !== 1 ? 's' : ''}</span>
      </div>
    </footer>
  )
}
