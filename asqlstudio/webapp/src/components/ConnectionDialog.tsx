import { useState } from 'react'
import {
  deleteRecentConnection,
  deleteSavedConnectionProfile,
  readRecentConnections,
  readSavedConnectionProfiles,
  saveConnectionProfile,
  type RecentConnection,
  type SavedConnectionProfile,
} from '../lib/connectionHistory'
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

function buildSwitchRequest(config: ConnectionConfig, authToken = '', adminAuthToken = ''): ConnectionSwitchRequest {
  return {
    pgwire_endpoint: (config.pgwire_endpoint ?? '').trim(),
    follower_endpoint: (config.follower_endpoint ?? '').trim(),
    peer_endpoints: config.peer_endpoints ?? [],
    admin_endpoints: config.admin_endpoints ?? [],
    auth_token: authToken,
    admin_auth_token: adminAuthToken,
    data_dir: (config.data_dir ?? '').trim(),
  }
}

export function ConnectionDialog({ current, busy, error, onClose, onSubmit }: Props) {
  const [pgwireEndpoint, setPgwireEndpoint] = useState(() => current?.pgwire_endpoint ?? '')
  const [followerEndpoint, setFollowerEndpoint] = useState(() => current?.follower_endpoint ?? '')
  const [peerEndpoints, setPeerEndpoints] = useState(() => joinEndpoints(current?.peer_endpoints))
  const [adminEndpoints, setAdminEndpoints] = useState(() => joinEndpoints(current?.admin_endpoints))
  const [authToken, setAuthToken] = useState('')
  const [adminAuthToken, setAdminAuthToken] = useState('')
  const [dataDir, setDataDir] = useState(() => current?.data_dir ?? '')
  const [recentConnections, setRecentConnections] = useState<RecentConnection[]>(() => readRecentConnections())
  const [savedProfiles, setSavedProfiles] = useState<SavedConnectionProfile[]>(() => readSavedConnectionProfiles())
  const [profileName, setProfileName] = useState('')
  const [editingProfileId, setEditingProfileId] = useState<string | null>(null)

  const applyConnection = (config: ConnectionConfig) => {
    setPgwireEndpoint(config.pgwire_endpoint ?? '')
    setFollowerEndpoint(config.follower_endpoint ?? '')
    setPeerEndpoints(joinEndpoints(config.peer_endpoints))
    setAdminEndpoints(joinEndpoints(config.admin_endpoints))
    setAuthToken('')
    setAdminAuthToken('')
    setDataDir(config.data_dir ?? '')
  }

  const handleDeleteRecent = (id: string) => {
    deleteRecentConnection(id)
    setRecentConnections((entries) => entries.filter((entry) => entry.id !== id))
  }

  const handleReconnectRecent = async (config: RecentConnection) => {
    await onSubmit(buildSwitchRequest(config))
  }

  const handleDeleteProfile = (id: string) => {
    deleteSavedConnectionProfile(id)
    setSavedProfiles((entries) => entries.filter((entry) => entry.id !== id))
    if (editingProfileId === id) {
      setEditingProfileId(null)
      setProfileName('')
    }
  }

  const handleReconnectProfile = async (config: SavedConnectionProfile) => {
    await onSubmit(buildSwitchRequest(config))
  }

  const handleSaveProfile = () => {
    const trimmedName = profileName.trim()
    const config: ConnectionConfig = {
      pgwire_endpoint: pgwireEndpoint,
      follower_endpoint: followerEndpoint,
      peer_endpoints: parseEndpoints(peerEndpoints),
      admin_endpoints: parseEndpoints(adminEndpoints),
      data_dir: dataDir,
    }
    saveConnectionProfile(trimmedName, config, editingProfileId ?? undefined)
    setSavedProfiles(readSavedConnectionProfiles())
    setProfileName(trimmedName)
    setEditingProfileId(null)
  }

  const handleRenameProfile = (profile: SavedConnectionProfile) => {
    setEditingProfileId(profile.id)
    setProfileName(profile.name)
    applyConnection(profile)
  }

  const handleCancelProfileEdit = () => {
    setEditingProfileId(null)
    setProfileName('')
  }

  const handleSubmit = async () => {
    await onSubmit(buildSwitchRequest({
      pgwire_endpoint: pgwireEndpoint,
      follower_endpoint: followerEndpoint,
      peer_endpoints: parseEndpoints(peerEndpoints),
      admin_endpoints: parseEndpoints(adminEndpoints),
      data_dir: dataDir,
    }, authToken, adminAuthToken))
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
          <div className="conn-field conn-field-wide">
            <span className="conn-label"><IconDatabase /> Saved profiles</span>
            {editingProfileId && (
              <div className="conn-inline-note">Editing the selected profile name and endpoints. Saving will replace the existing profile.</div>
            )}
            <div className="conn-profile-save-row">
              <input
                className="conn-input"
                value={profileName}
                onChange={(event) => setProfileName(event.target.value)}
                placeholder="e.g. local dev, demo cluster"
                disabled={busy}
              />
              <button className="toolbar-btn" onClick={handleSaveProfile} disabled={busy || !profileName.trim() || !pgwireEndpoint.trim()}>
                {editingProfileId ? 'Save changes' : 'Save profile'}
              </button>
              {editingProfileId && (
                <button className="toolbar-btn" onClick={handleCancelProfileEdit} disabled={busy}>
                  Cancel
                </button>
              )}
            </div>
            {savedProfiles.length > 0 ? (
              <div className="conn-recent-list conn-profile-list">
                {savedProfiles.map((entry) => {
                  const isCurrent = current?.pgwire_endpoint?.trim() === entry.pgwire_endpoint.trim()
                  const isEditing = editingProfileId === entry.id
                  return (
                    <div key={entry.id} className={`conn-recent-card conn-profile-card${isCurrent ? ' current' : ''}${isEditing ? ' editing' : ''}`}>
                      <div className="conn-recent-main">
                        <div className="conn-recent-title-row">
                          <div className="conn-recent-title">{entry.name}</div>
                          {isCurrent && <span className="conn-recent-badge">Current</span>}
                          {isEditing && <span className="conn-recent-badge">Editing</span>}
                        </div>
                        <div className="conn-profile-endpoint">{entry.pgwire_endpoint}</div>
                        <div className="conn-recent-meta">
                          {entry.follower_endpoint ? <span>Follower: {entry.follower_endpoint}</span> : <span>No follower</span>}
                          <span>Admin: {(entry.admin_endpoints ?? []).length || 0}</span>
                          <span>Peers: {(entry.peer_endpoints ?? []).length || 0}</span>
                          {entry.data_dir ? <span>Data dir: {entry.data_dir}</span> : null}
                        </div>
                        <div className="conn-recent-time">Updated {formatRecentTime(entry.updated_at)}</div>
                      </div>
                      <div className="conn-recent-actions">
                        <button className="toolbar-btn primary" onClick={() => void handleReconnectProfile(entry)} disabled={busy || isCurrent}>
                          <IconRefresh /> {isCurrent ? 'Connected' : 'Reconnect'}
                        </button>
                        <button className="toolbar-btn" onClick={() => handleRenameProfile(entry)} disabled={busy}>Rename</button>
                        <button className="toolbar-btn" onClick={() => {
                          setEditingProfileId(null)
                          setProfileName(entry.name)
                          applyConnection(entry)
                        }} disabled={busy}>Use</button>
                        <button className="icon-btn" onClick={() => handleDeleteProfile(entry.id)} disabled={busy} aria-label={`Delete saved profile ${entry.name}`}>
                          <IconX />
                        </button>
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div className="conn-empty-state">Save the current endpoints as a named profile for fast switching later.</div>
            )}
          </div>

          {recentConnections.length > 0 && (
            <div className="conn-field conn-field-wide">
              <span className="conn-label"><IconDatabase /> Recent connections</span>
              <div className="conn-recent-list">
                {recentConnections.map((entry) => {
                  const isCurrent = current?.pgwire_endpoint?.trim() === entry.pgwire_endpoint.trim()
                  return (
                    <div key={entry.id} className={`conn-recent-card${isCurrent ? ' current' : ''}`}>
                      <div className="conn-recent-main">
                        <div className="conn-recent-title-row">
                          <div className="conn-recent-title">{entry.pgwire_endpoint}</div>
                          {isCurrent && <span className="conn-recent-badge">Current</span>}
                        </div>
                        <div className="conn-recent-meta">
                          {entry.follower_endpoint ? <span>Follower: {entry.follower_endpoint}</span> : <span>No follower</span>}
                          <span>Admin: {(entry.admin_endpoints ?? []).length || 0}</span>
                          <span>Peers: {(entry.peer_endpoints ?? []).length || 0}</span>
                          {entry.data_dir ? <span>Data dir: {entry.data_dir}</span> : null}
                        </div>
                        <div className="conn-recent-time">Used {formatRecentTime(entry.last_used_at)}</div>
                      </div>
                      <div className="conn-recent-actions">
                        <button className="toolbar-btn primary" onClick={() => void handleReconnectRecent(entry)} disabled={busy || isCurrent}>
                          <IconRefresh /> {isCurrent ? 'Connected' : 'Reconnect'}
                        </button>
                        <button className="toolbar-btn" onClick={() => applyConnection(entry)} disabled={busy}>Use</button>
                        <button className="icon-btn" onClick={() => handleDeleteRecent(entry.id)} disabled={busy} aria-label={`Remove ${entry.pgwire_endpoint} from recent connections`}>
                          <IconX />
                        </button>
                      </div>
                    </div>
                  )
                })}
              </div>
            </div>
          )}

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
          Token fields are optional. If left blank, Studio keeps using the currently configured secret for that surface. Recent entries can also reconnect directly with one click using the currently stored tokens.
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

function formatRecentTime(value: string) {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    return 'recently'
  }
  return parsed.toLocaleString()
}

function IconKeyBadge({ configured }: { configured: boolean }) {
  return (
    <span className={`conn-token-indicator ${configured ? 'configured' : ''}`}>
      <span className="conn-token-dot" />
      <span>{configured ? 'Stored' : 'Unset'}</span>
    </span>
  )
}
