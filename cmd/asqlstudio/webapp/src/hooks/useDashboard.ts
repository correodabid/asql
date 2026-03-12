import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from '../lib/api'

type HealthStatus = 'ok' | 'error' | 'loading'

type ScanStats = Record<string, number>

type ReplicationInfo = {
  leaderLSN: number
  followerLSN: number
  lag: number
  available: boolean
}

type RoutingStats = Record<string, number>

export type PIDControllerState = {
  name: string
  setpoint: number
  measured: number
  error: number
  output: number
  integral: number
  total_updates: number
}

export type SystemInfo = {
  hostname: string
  os: string
  arch: string
  num_cpu: number
  pid: number
  go_version: string
  num_goroutine: number
  uptime_ms: number
  heap_alloc_bytes: number
  heap_sys_bytes: number
  heap_inuse_bytes: number
  heap_objects: number
  stack_inuse_bytes: number
  total_alloc_bytes: number
  sys_bytes: number
  gc_cycles: number
  last_gc_pause_ns: number
  gc_pause_total_ns: number
  gc_cpu_fraction: number
}

export type EngineStats = {
  total_commits: number
  total_reads: number
  total_rollbacks: number
  total_begins: number
  total_time_travel_queries: number
  active_transactions: number
  commit_latency_p50_ms: number
  commit_latency_p95_ms: number
  commit_latency_p99_ms: number
  read_latency_p50_ms: number
  read_latency_p95_ms: number
  read_latency_p99_ms: number
  time_travel_latency_p50_ms: number
  time_travel_latency_p95_ms: number
  time_travel_latency_p99_ms: number
  commit_throughput_per_sec: number
  read_throughput_per_sec: number
  wal_file_size_bytes: number
  snapshot_file_size_bytes: number
  audit_file_size_bytes: number
  pid_controllers?: PIDControllerState[]
  system?: SystemInfo
}

const MAX_HISTORY = 60 // 60 samples * 5s = 5 minutes of history

const emptyEngineStats: EngineStats = {
  total_commits: 0,
  total_reads: 0,
  total_rollbacks: 0,
  total_begins: 0,
  total_time_travel_queries: 0,
  active_transactions: 0,
  commit_latency_p50_ms: 0,
  commit_latency_p95_ms: 0,
  commit_latency_p99_ms: 0,
  read_latency_p50_ms: 0,
  read_latency_p95_ms: 0,
  read_latency_p99_ms: 0,
  time_travel_latency_p50_ms: 0,
  time_travel_latency_p95_ms: 0,
  time_travel_latency_p99_ms: 0,
  commit_throughput_per_sec: 0,
  read_throughput_per_sec: 0,
  wal_file_size_bytes: 0,
  snapshot_file_size_bytes: 0,
  audit_file_size_bytes: 0,
}

export function useDashboard() {
  const [health, setHealth] = useState<HealthStatus>('loading')
  const [scanStats, setScanStats] = useState<ScanStats>({})
  const [replication, setReplication] = useState<ReplicationInfo>({
    leaderLSN: 0,
    followerLSN: 0,
    lag: 0,
    available: false,
  })
  const [routingStats, setRoutingStats] = useState<RoutingStats>({})
  const [engineStats, setEngineStats] = useState<EngineStats>(emptyEngineStats)
  const [prevEngineStats, setPrevEngineStats] = useState<EngineStats>(emptyEngineStats)
  const [statsHistory, setStatsHistory] = useState<EngineStats[]>([])
  const [lastRefresh, setLastRefresh] = useState<number>(Date.now())
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [replicationLagSupported, setReplicationLagSupported] = useState(true)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const refresh = useCallback(async () => {
    // Health
    try {
      await api<{ status: string }>('/api/health', 'GET')
      setHealth('ok')
    } catch {
      setHealth('error')
    }

    // Scan strategy stats
    try {
      const resp = await api<{ counts?: Record<string, number> }>('/api/scan-strategy-stats', 'GET')
      setScanStats(resp.counts || {})
    } catch {
      // leave previous
    }

    // Replication lag (optional if follower endpoint is not configured)
    if (replicationLagSupported) {
      try {
        const resp = await api<{ leader_lsn: number; follower_lsn: number; lag: number }>('/api/replication/lag', 'GET')
        setReplication({ leaderLSN: resp.leader_lsn, followerLSN: resp.follower_lsn, lag: resp.lag, available: true })
      } catch (error) {
        const message = error instanceof Error ? error.message : ''
        if (message.includes('HTTP 412') || message.includes('follower-grpc-endpoint') || message.includes('follower-engine-endpoint')) {
          setReplicationLagSupported(false)
        }

        try {
          const resp = await api<{ lsn: number }>('/api/replication/last-lsn', 'GET')
          setReplication({ leaderLSN: resp.lsn || 0, followerLSN: 0, lag: 0, available: false })
        } catch {
          setReplication({ leaderLSN: 0, followerLSN: 0, lag: 0, available: false })
        }
      }
    } else {
      try {
        const resp = await api<{ lsn: number }>('/api/replication/last-lsn', 'GET')
        setReplication({ leaderLSN: resp.lsn || 0, followerLSN: 0, lag: 0, available: false })
      } catch {
        setReplication({ leaderLSN: 0, followerLSN: 0, lag: 0, available: false })
      }
    }

    // Routing stats
    try {
      const resp = await api<{ counts?: Record<string, number> }>('/api/read-routing-stats', 'GET')
      setRoutingStats(resp.counts || {})
    } catch {
      // leave previous
    }

    // Engine stats
    try {
      const resp = await api<EngineStats>('/api/engine-stats', 'GET')
      setEngineStats(prev => {
        setPrevEngineStats(prev)
        return resp
      })
      setStatsHistory(prev => {
        const next = [...prev, resp]
        return next.length > MAX_HISTORY ? next.slice(next.length - MAX_HISTORY) : next
      })
    } catch {
      // leave previous
    }

    setLastRefresh(Date.now())
  }, [replicationLagSupported])

  useEffect(() => {
    refresh()
  }, [refresh])

  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(refresh, 5000)
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [autoRefresh, refresh])

  return {
    health,
    scanStats,
    replication,
    routingStats,
    engineStats,
    prevEngineStats,
    statsHistory,
    lastRefresh,
    autoRefresh,
    setAutoRefresh,
    refresh,
  }
}
