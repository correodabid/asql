import { useState } from 'react'
import { IconDatabase, IconLink, IconRefresh, IconServer, IconShield, IconX } from './Icons'

export type ConnectionConfig = {
  pgwire_endpoint: string
  follower_endpoint?: string
  peer_endpoints?: string[]
  admin_endpoints?: string[]
  auth_token_configured?: boolean
  admin_auth_token_configured?: boolean
  data_dir?: string
}

export type ConnectionSwitchRequest = {
  pgwire_endpoint: string
  follower_endpoint?: string
  peer_endpoints?: string[]
  admin_endpoints?: string[]
  auth_token?: string
  admin_auth_token?: string
  data_dir?: string
}

type Props = {
  current: ConnectionConfig | null
  busy: boolean
  error: string
  onClose: () => void
  onSubmit: (request: ConnectionSwitchRequest) => Promise<void>
}

function joinEndpoints(values?: string[]) {
  return (values ?? []).join(', ')
}

function parseEndpoints(value: string) {
  return value
    .split(/[\n,]/)
    .map((entry) => entry.trim())
    .filter(Boolean)
}

export function ConnectionDialog({ current, busy, error, onClose, onSubmit }: Props) {
  const [pgwireEndpoint, setPgwireEndpoint] = useState(() => current?.pgwire_endpoint ?? '')
  const [followerEndpoint, setFollowerEndpoint] = useState(() => current?.follower_endpoint ?? '')
  const [peerEndpoints, setPeerEndpoints] = useState(() => joinEndpoints(current?.peer_endpoints))
  const [adminEndpoints, setAdminEndpoints] = useState(() => joinEndpoints(current?.admin_endpoints))
  const [authToken, setAuthToken] = useState('')
  const [adminAuthToken, setAdminAuthToken] = useState('')
  const [dataDir, setDataDir] = useState(() => current?.data_dir ?? '')

  const handleSubmit = async () => {
    await onSubmit({
      pgwire_endpoint: pgwireEndpoint.trim(),
      follower_endpoint: followerEndpoint.trim(),
      peer_endpoints: parseEndpoints(peerEndpoints),
      admin_endpoints: parseEndpoints(adminEndpoints),
      auth_token: authToken,
      admin_auth_token: adminAuthToken,
      data_dir: dataDir.trim(),
    })
  }

  return (
    <div className="conn-overlay" onClick={busy ? undefined : onClose}>
      <div className="conn-modal" onClick={(event) => event.stopPropagation()}>
        <div className="conn-header">
          <div>
            <div className="conn-title">Switch connection</div>
            <div className="conn-subtitle">Retarget Studio to a different pgwire or admin endpoint without relaunching the desktop app.</div>
          </div>
          <button className="icon-btn conn-close" onClick={onClose} disabled={busy} aria-label="Close connection dialog">
            <IconX />
          </button>
        </div>

        <div className="conn-grid">
          <label className="conn-field conn-field-wide">
            <span className="conn-label"><IconDatabase /> Pgwire endpoint</span>
            <input
              className="conn-input"
              value={pgwireEndpoint}
              onChange={(event) => setPgwireEndpoint(event.target.value)}
              placeholder="127.0.0.1:5433"
              disabled={busy}
            />
          </label>

          <label className="conn-field conn-field-wide">
            <span className="conn-label"><IconServer /> Follower endpoint</span>
            <input
              className="conn-input"
              value={followerEndpoint}
              onChange={(event) => setFollowerEndpoint(event.target.value)}
              placeholder="Optional replica endpoint"
              disabled={busy}
            />
          </label>

          <label className="conn-field conn-field-wide">
            <span className="conn-label"><IconLink /> Peer endpoints</span>
            <textarea
              className="conn-input conn-textarea"
              value={peerEndpoints}
              onChange={(event) => setPeerEndpoints(event.target.value)}
              placeholder="Comma or newline separated peer pgwire endpoints"
              disabled={busy}
            />
          </label>

          <label className="conn-field conn-field-wide">
            <span className="conn-label"><IconShield /> Admin endpoints</span>
            <textarea
              className="conn-input conn-textarea"
              value={adminEndpoints}
              onChange={(event) => setAdminEndpoints(event.target.value)}
              placeholder="Comma or newline separated admin HTTP endpoints"
              disabled={busy}
            />
          </label>

          <label className="conn-field">
            <span className="conn-label"><IconKeyBadge configured={current?.auth_token_configured === true} /> Pgwire token</span>
            <input
              className="conn-input"
              type="password"
              value={authToken}
              onChange={(event) => setAuthToken(event.target.value)}
              placeholder={current?.auth_token_configured ? 'Leave blank to reuse current token' : 'Optional'}
              disabled={busy}
            />
          </label>

          <label className="conn-field">
            <span className="conn-label"><IconKeyBadge configured={current?.admin_auth_token_configured === true} /> Admin token</span>
            <input
              className="conn-input"
              type="password"
              value={adminAuthToken}
              onChange={(event) => setAdminAuthToken(event.target.value)}
              placeholder={current?.admin_auth_token_configured ? 'Leave blank to reuse current admin token' : 'Optional'}
              disabled={busy}
            />
          </label>

          <label className="conn-field conn-field-wide">
            <span className="conn-label"><IconRefresh /> Recovery data dir</span>
            <input
              className="conn-input"
              value={dataDir}
              onChange={(event) => setDataDir(event.target.value)}
              placeholder=".asql"
              disabled={busy}
            />
          </label>
        </div>

        <div className="conn-note">
          Token fields are optional. If left blank, Studio keeps using the currently configured secret for that surface.
        </div>

        {error && <div className="conn-error">{error}</div>}

        <div className="conn-footer">
          <button className="toolbar-btn" onClick={onClose} disabled={busy}>Cancel</button>
          <button className="toolbar-btn primary" onClick={() => void handleSubmit()} disabled={busy || !pgwireEndpoint.trim()}>
            <IconRefresh /> {busy ? 'Switching…' : 'Save & reconnect'}
          </button>
        </div>
      </div>
    </div>
  )
}

function IconKeyBadge({ configured }: { configured: boolean }) {
  return (
    <span className={`conn-token-indicator ${configured ? 'configured' : ''}`}>
      <span className="conn-token-dot" />
      <span>{configured ? 'Stored' : 'Unset'}</span>
    </span>
  )
}
