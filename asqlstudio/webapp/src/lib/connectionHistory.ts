import type { ConnectionConfig } from '../components/ConnectionDialog'

export type RecentConnection = ConnectionConfig & {
  id: string
  last_used_at: string
}

const RECENT_CONNECTIONS_KEY = 'asql_recent_connections_v1'
const MAX_RECENT_CONNECTIONS = 6

function canUseStorage() {
  return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined'
}

function normalizeList(values?: string[]) {
  return (values ?? []).map((value) => value.trim()).filter(Boolean)
}

function connectionSignature(config: ConnectionConfig) {
  return JSON.stringify({
    pgwire_endpoint: config.pgwire_endpoint.trim(),
    follower_endpoint: (config.follower_endpoint ?? '').trim(),
    peer_endpoints: normalizeList(config.peer_endpoints),
    admin_endpoints: normalizeList(config.admin_endpoints),
    data_dir: (config.data_dir ?? '').trim(),
  })
}

function sanitize(config: ConnectionConfig): ConnectionConfig {
  return {
    pgwire_endpoint: config.pgwire_endpoint.trim(),
    follower_endpoint: (config.follower_endpoint ?? '').trim(),
    peer_endpoints: normalizeList(config.peer_endpoints),
    admin_endpoints: normalizeList(config.admin_endpoints),
    auth_token_configured: config.auth_token_configured === true,
    admin_auth_token_configured: config.admin_auth_token_configured === true,
    data_dir: (config.data_dir ?? '').trim(),
  }
}

export function readRecentConnections(): RecentConnection[] {
  if (!canUseStorage()) {
    return []
  }
  try {
    const raw = window.localStorage.getItem(RECENT_CONNECTIONS_KEY)
    if (!raw) {
      return []
    }
    const parsed = JSON.parse(raw)
    if (!Array.isArray(parsed)) {
      return []
    }
    return parsed
      .filter((entry): entry is RecentConnection => !!entry && typeof entry === 'object' && typeof entry.pgwire_endpoint === 'string' && typeof entry.id === 'string')
      .map((entry) => ({
        ...sanitize(entry),
        id: entry.id,
        last_used_at: typeof entry.last_used_at === 'string' ? entry.last_used_at : new Date(0).toISOString(),
      }))
      .sort((a, b) => b.last_used_at.localeCompare(a.last_used_at))
  } catch {
    return []
  }
}

function writeRecentConnections(entries: RecentConnection[]) {
  if (!canUseStorage()) {
    return
  }
  window.localStorage.setItem(RECENT_CONNECTIONS_KEY, JSON.stringify(entries.slice(0, MAX_RECENT_CONNECTIONS)))
}

export function rememberRecentConnection(config: ConnectionConfig) {
  const normalized = sanitize(config)
  if (!normalized.pgwire_endpoint) {
    return
  }
  const id = connectionSignature(normalized)
  const next: RecentConnection = {
    ...normalized,
    id,
    last_used_at: new Date().toISOString(),
  }
  const entries = readRecentConnections().filter((entry) => entry.id !== id)
  writeRecentConnections([next, ...entries])
}

export function deleteRecentConnection(id: string) {
  writeRecentConnections(readRecentConnections().filter((entry) => entry.id !== id))
}
