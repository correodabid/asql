import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from '../lib/api'
import { EventsOn } from '../wailsjs/wailsjs/runtime/runtime'

export type GroupStatus = {
  group: string
  leader_id: string
  term: number
  fencing_token: string
  lease_active: boolean
  last_lsn: number
}

export type NodeInfo = {
  node_id: string
  addr: string       // pgwire address, always "host:port"
  role: 'leader' | 'follower' | 'unknown'
  lsn: number
  lag: number
  reachable: boolean
}

export type LsnPoint = { ts: number; lsn: number }

export type ClusterEvent = {
  id: number
  ts: number
  type: 'leader-elected' | 'lease-lost' | 'lag-spike' | 'node-down' | 'node-up' | 'info'
  msg: string
}

export type RoutingStats = {
  requests_total: number
  route_leader: number
  route_follower: number
  consistency_strong: number
  consistency_bounded_stale: number
  fallback_follower_unavailable: number
  fallback_lag_exceeded: number
}

export type ClusterEngineStats = {
  commit_latency_p95_ms?: number
  commit_latency_p99_ms?: number
  fsync_latency_p95_ms?: number
  total_fsync_errors?: number
  total_audit_errors?: number
  replay_duration_ms?: number
  snapshot_duration_ms?: number
  commit_throughput_per_sec?: number
  read_throughput_per_sec?: number
  wal_file_size_bytes?: number
  snapshot_file_size_bytes?: number
  audit_file_size_bytes?: number
}

export type ClusterAdminNode = {
  endpoint: string
  node_id?: string
  status: string
  ready: boolean
  live: boolean
  raft_role?: string
  leader_id?: string
  current_term?: number
  last_durable_lsn?: number
  reasons?: string[]
  head_lsn?: number
  oldest_retained_lsn?: number
  last_retained_lsn?: number
  segment_count?: number
  disk_snapshot_count?: number
  snapshot_catalog_len?: number
  max_disk_snapshots?: number
  error?: string
}

export type ClusterFailoverTransition = {
  phase: string
  group_name: string
  term: number
  node_id: string
}

export type ClusterDiagnostics = {
  engine_stats?: ClusterEngineStats
  admin_nodes?: ClusterAdminNode[]
  failover_history?: ClusterFailoverTransition[]
  summary?: {
    reachable_nodes?: number
    ready_nodes?: number
    total_segments?: number
    total_snapshots?: number
    highest_durable_lsn?: number
    worst_replication_lag?: number
  }
}

const LSN_HISTORY_MAX = 60
const EVENT_MAX = 30
const LAG_SPIKE_THRESHOLD = 500

export function useCluster() {
  const [configuredGroups, setConfiguredGroups] = useState<string[]>([])
  const [groups, setGroups] = useState<GroupStatus[]>([])
  const [nodeStatus, setNodeStatus] = useState<NodeInfo[]>([])
  const [lsnHistory, setLsnHistory] = useState<Record<string, LsnPoint[]>>({})
  const [routingStats, setRoutingStats] = useState<RoutingStats | null>(null)
  const [diagnostics, setDiagnostics] = useState<ClusterDiagnostics | null>(null)
  const [events, setEvents] = useState<ClusterEvent[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [lastRefresh, setLastRefresh] = useState(0)

  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const prevGroupsRef = useRef<GroupStatus[]>([])
  const prevNodesRef = useRef<NodeInfo[]>([])
  const eventIdRef = useRef(0)
  const prevLagRef = useRef<Record<string, number>>({})

  // Fetch configured groups on mount.
  useEffect(() => {
    let cancelled = false
    api<{ groups: string[] }>('/api/cluster/groups', 'GET')
      .then((resp) => { if (!cancelled) setConfiguredGroups(resp.groups || []) })
      .catch(() => {})
    return () => { cancelled = true }
  }, [])

  const pushEvent = useCallback((type: ClusterEvent['type'], msg: string) => {
    setEvents((prev) => {
      const event: ClusterEvent = { id: ++eventIdRef.current, ts: Date.now(), type, msg }
      return [event, ...prev].slice(0, EVENT_MAX)
    })
  }, [])

  const refresh = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const now = Date.now()
      // Node status is driven by the push event (cluster:node-status), so we
      // only need groups (leadership/term) and routing stats here.
      // When no groups are configured we still fetch routing stats; groups call is skipped.
      const [statusResp, routingResp, diagnosticsResp] = await Promise.all([
        configuredGroups.length > 0
          ? api<{ groups: GroupStatus[] }>(
              `/api/cluster/status?groups=${encodeURIComponent(configuredGroups.join(','))}`,
              'GET',
            )
          : Promise.resolve({ groups: [] as GroupStatus[] }),
        api<{ counts: Record<string, number> }>('/api/read-routing-stats', 'GET').catch(() => ({ counts: {} as Record<string, number> })),
		api<ClusterDiagnostics>('/api/cluster/diagnostics', 'GET').catch(() => ({ admin_nodes: [], failover_history: [], summary: {} })),
      ])

      const newGroups = statusResp.groups || []
      const prevGroups = prevGroupsRef.current

      // ── Detect group-level events ───────────────────────────────────────────
      newGroups.forEach((g) => {
        const old = prevGroups.find((p) => p.group === g.group)
        if (old) {
          if (old.leader_id !== g.leader_id && g.leader_id) {
            pushEvent('leader-elected', `${g.group}: ${g.leader_id} elected (term ${g.term})`)
          }
          if (old.lease_active && !g.lease_active) {
            pushEvent('lease-lost', `${g.group}: lease lost`)
          }
        }
      })
      prevGroupsRef.current = newGroups

      setGroups(newGroups)

      // ── LSN history ring buffer per group ───────────────────────────────────
      setLsnHistory((prev) => {
        const next = { ...prev }
        newGroups.forEach((g) => {
          const history = prev[g.group] ?? []
          const last = history[history.length - 1]
          if (!last || last.lsn !== g.last_lsn) {
            next[g.group] = [...history, { ts: now, lsn: g.last_lsn }].slice(-LSN_HISTORY_MAX)
          }
        })
        return next
      })

      // ── Parse routing stats ─────────────────────────────────────────────────
      const counts = routingResp.counts || {}
      setRoutingStats({
        requests_total:               counts['requests_total'] ?? 0,
        route_leader:                 counts['route_leader'] ?? 0,
        route_follower:               counts['route_follower'] ?? 0,
        consistency_strong:           counts['consistency_strong'] ?? 0,
        consistency_bounded_stale:    counts['consistency_bounded_stale'] ?? 0,
        fallback_follower_unavailable: counts['fallback_follower_unavailable'] ?? 0,
        fallback_lag_exceeded:        counts['fallback_lag_exceeded'] ?? 0,
      })

	  setDiagnostics(diagnosticsResp)

      setLastRefresh(now)
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setLoading(false)
    }
  }, [configuredGroups, pushEvent])

  // Subscribe to the push event for near-real-time node status.
  // The Go watcher emits "cluster:node-status" whenever any LSN changes (≤100ms latency)
  // so we don't need to poll the node list from here.
  useEffect(() => {
    const off = EventsOn('cluster:node-status', (payload: { nodes: NodeInfo[] }) => {
      const newNodes: NodeInfo[] = payload?.nodes ?? []
      setNodeStatus(newNodes)
      // ── Detect node-level events ──────────────────────────────────────────
      const prevNodes = prevNodesRef.current
      newNodes.forEach((n) => {
        const old = prevNodes.find((p) => p.node_id === n.node_id)
        if (old && old.reachable && !n.reachable) {
          pushEvent('node-down', `${n.node_id || n.addr}: unreachable`)
        }
        if (old && !old.reachable && n.reachable) {
          pushEvent('node-up', `${n.node_id || n.addr}: back online`)
        }
        if (n.role === 'follower' && n.reachable) {
          const prevLag = prevLagRef.current[n.node_id] ?? 0
          if (n.lag > LAG_SPIKE_THRESHOLD && prevLag <= LAG_SPIKE_THRESHOLD) {
            pushEvent('lag-spike', `Follower lag spiked: ${n.lag.toLocaleString()} entries behind`)
          }
          prevLagRef.current[n.node_id] = n.lag
        }
      })
      prevNodesRef.current = newNodes
    })
    return off
  }, [pushEvent])

  useEffect(() => { refresh() }, [refresh])

  useEffect(() => {
    if (autoRefresh) {
  // Poll groups (leadership/term info) on a relaxed 3 s interval.
  // Node LSN/lag is driven by the push event above so no need to include it here.
      intervalRef.current = setInterval(refresh, 3000)
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current) }
  }, [autoRefresh, refresh])

  return {
    configuredGroups,
    groups,
    nodeStatus,
    lsnHistory,
    routingStats,
  	diagnostics,
    events,
    loading,
    error,
    autoRefresh,
    setAutoRefresh,
    lastRefresh,
    refresh,
  }
}
