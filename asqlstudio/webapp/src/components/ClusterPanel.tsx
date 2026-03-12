import React, { useState, useMemo, useCallback } from 'react'
import { useCluster, type GroupStatus, type LsnPoint, type NodeInfo, type RoutingStats, type ClusterEvent, type ClusterDiagnostics, type ClusterAdminNode, type ClusterFailoverTransition } from '../hooks/useCluster'
import { IconRefresh, IconShield } from './Icons'

// ─────────────────────────────── helpers ────────────────────────────────────

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`
  return n.toString()
}

function relative(ts: number): string {
  const d = Math.floor((Date.now() - ts) / 1000)
  if (d < 5) return 'just now'
  if (d < 60) return `${d}s ago`
  if (d < 3600) return `${Math.floor(d / 60)}m ago`
  return `${Math.floor(d / 3600)}h ago`
}

function lagClass(lag: number) {
  if (lag === 0) return 'cl2-lag-sync'
  if (lag < 100) return 'cl2-lag-good'
  if (lag < 500) return 'cl2-lag-warn'
  return 'cl2-lag-bad'
}

function lagSeverity(lag: number): 'ok' | 'good' | 'warn' | 'bad' {
  if (lag === 0) return 'ok'
  if (lag < 100) return 'good'
  if (lag < 500) return 'warn'
  return 'bad'
}

function fmtMs(n?: number): string {
  if (!n || Number.isNaN(n)) return '—'
  if (n >= 1000) return `${(n / 1000).toFixed(2)}s`
  return `${n.toFixed(n >= 100 ? 0 : 1)}ms`
}

function fmtBytes(n?: number): string {
  if (!n || Number.isNaN(n)) return '—'
  if (n >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(2)} GB`
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)} MB`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)} KB`
  return `${n} B`
}

function nodeLabel(n: NodeInfo): { primary: string; secondary: string | null } {
  if (n.addr && n.addr !== n.node_id) {
    const m = n.addr.match(/:(\d+)$/)
    return { primary: n.node_id, secondary: m ? `:${m[1]}` : null }
  }
  const m = n.node_id.match(/:(\d+)$/)
  return { primary: m ? `:${m[1]}` : n.node_id, secondary: null }
}

// ─────────────────────────────── Ring Gauge (SVG) ──────────────────────────

function RingGauge({ value, max, size = 80, stroke = 6, color = 'var(--accent)', label, sublabel, animate = true }: {
  value: number; max: number; size?: number; stroke?: number
  color?: string; label?: string; sublabel?: string; animate?: boolean
}) {
  const r = (size - stroke) / 2
  const circ = 2 * Math.PI * r
  const pct = max > 0 ? Math.min(value / max, 1) : 0
  const offset = circ * (1 - pct)
  return (
    <div className="cl2-ring-gauge">
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <circle cx={size / 2} cy={size / 2} r={r}
          fill="none" stroke="rgba(255,255,255,0.04)" strokeWidth={stroke} />
        <circle cx={size / 2} cy={size / 2} r={r}
          fill="none" stroke={color} strokeWidth={stroke}
          strokeLinecap="round"
          strokeDasharray={circ} strokeDashoffset={offset}
          transform={`rotate(-90 ${size / 2} ${size / 2})`}
          style={animate ? { transition: 'stroke-dashoffset 0.8s cubic-bezier(.4,0,.2,1)' } : undefined} />
      </svg>
      {label && (
        <div className="cl2-ring-label">
          <span className="cl2-ring-val">{label}</span>
          {sublabel && <span className="cl2-ring-sub">{sublabel}</span>}
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────── Sparkline ──────────────────────────────────

function LsnSparkline({ history, width = 140, height = 36, color = 'var(--accent)', id }: {
  history: LsnPoint[]; width?: number; height?: number; color?: string; id: string
}) {
  const W = width, H = height, PAD = 2
  if (history.length < 2) {
    return (
      <svg className="cl2-sparkline" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
        <line x1={PAD} y1={H / 2} x2={W - PAD} y2={H / 2}
          stroke="var(--border-strong)" strokeWidth="1" strokeDasharray="3 3" />
      </svg>
    )
  }
  const pts = history.slice(-60)
  const minV = Math.min(...pts.map(p => p.lsn))
  const maxV = Math.max(...pts.map(p => p.lsn))
  const range = maxV - minV || 1
  const coords = pts.map((p, i) => {
    const x = PAD + (i / (pts.length - 1)) * (W - PAD * 2)
    const y = H - PAD - ((p.lsn - minV) / range) * (H - PAD * 2)
    return [x, y] as [number, number]
  })

  const d = coords.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x},${y}`).join(' ')
  const fillD = `M${coords[0][0]},${H} ${coords.map(([x, y]) => `L${x},${y}`).join(' ')} L${coords[coords.length - 1][0]},${H} Z`
  const [lx, ly] = coords[coords.length - 1]

  return (
    <svg className="cl2-sparkline" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
      <defs>
        <linearGradient id={`sg-${id}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.3" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={fillD} fill={`url(#sg-${id})`} />
      <path d={d} fill="none" stroke={color} strokeWidth="1.5"
        strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={lx} cy={ly} r="3" fill={color} className="cl2-spark-dot" />
    </svg>
  )
}

// ─────────────────────────────── Health Score ───────────────────────────────

function computeHealthScore(nodes: NodeInfo[], groups: GroupStatus[], diagnostics: ClusterDiagnostics | null): {
  score: number; max: number; status: 'healthy' | 'degraded' | 'critical' | 'unknown'; details: string[]
} {
  if (nodes.length === 0 && groups.length === 0) {
    return { score: 0, max: 100, status: 'unknown', details: ['No cluster data available'] }
  }

  let score = 0
  let max = 0
  const details: string[] = []

  // Nodes reachable (40 points)
  const reachable = nodes.filter(n => n.reachable).length
  max += 40
  const nodePct = nodes.length > 0 ? reachable / nodes.length : 0
  score += Math.round(nodePct * 40)
  if (nodePct < 1) details.push(`${nodes.length - reachable} node(s) unreachable`)

  // Active lease (30 points)
  const hasLease = groups.some(g => g.lease_active)
  max += 30
  if (hasLease) score += 30
  else if (groups.length > 0) details.push('No active lease')

  // Replication lag (20 points)
  const followers = nodes.filter(n => n.role === 'follower' && n.reachable)
  if (followers.length > 0) {
    max += 20
    const worstLag = Math.max(...followers.map(n => n.lag))
    if (worstLag === 0) score += 20
    else if (worstLag < 100) { score += 15; details.push(`Lag: ${fmt(worstLag)}`) }
    else if (worstLag < 500) { score += 8; details.push(`High lag: ${fmt(worstLag)}`) }
    else details.push(`Critical lag: ${fmt(worstLag)}`)
  }

  // Errors (10 points)
  const stats = diagnostics?.engine_stats
  max += 10
  const errors = (stats?.total_fsync_errors ?? 0) + (stats?.total_audit_errors ?? 0)
  if (errors === 0) score += 10
  else details.push(`${errors} error(s) detected`)

  const pct = max > 0 ? score / max : 0
  const status = pct >= 0.9 ? 'healthy' : pct >= 0.6 ? 'degraded' : pct > 0 ? 'critical' : 'unknown'
  return { score, max, status, details }
}

// ─────────────────────────────── Status Banner ─────────────────────────────

function StatusBanner({ health, nodeCount, leaderNode, term }: {
  health: ReturnType<typeof computeHealthScore>
  nodeCount: number; leaderNode: string; term: number
}) {
  const statusMap = {
    healthy: { label: 'ALL SYSTEMS OPERATIONAL', cls: 'cl2-status-healthy' },
    degraded: { label: 'DEGRADED PERFORMANCE', cls: 'cl2-status-degraded' },
    critical: { label: 'CRITICAL — ACTION REQUIRED', cls: 'cl2-status-critical' },
    unknown: { label: 'AWAITING DATA', cls: 'cl2-status-unknown' },
  }
  const s = statusMap[health.status]
  return (
    <div className={`cl2-status-banner ${s.cls}`}>
      <div className="cl2-status-banner-glow" />
      <div className="cl2-status-content">
        <div className="cl2-status-left">
          <div className={`cl2-status-indicator ${s.cls}`}>
            <div className="cl2-status-indicator-dot" />
          </div>
          <div className="cl2-status-text">
            <span className="cl2-status-label">{s.label}</span>
            <span className="cl2-status-sub">
              {nodeCount} node{nodeCount !== 1 ? 's' : ''}
              {leaderNode && <> · leader <span className="mono">{leaderNode}</span></>}
              {term > 0 && <> · term {term}</>}
            </span>
          </div>
        </div>
        <div className="cl2-status-right">
          <RingGauge value={health.score} max={health.max} size={44} stroke={4}
            color={health.status === 'healthy' ? 'var(--text-safe)' : health.status === 'degraded' ? 'var(--text-warning)' : 'var(--text-unsafe)'}
            label={`${Math.round((health.score / (health.max || 1)) * 100)}`}
            sublabel="%" />
        </div>
      </div>
      {health.details.length > 0 && (
        <div className="cl2-status-details">
          {health.details.map((d, i) => <span key={i} className="cl2-status-detail-pill">{d}</span>)}
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────── Radial Topology ───────────────────────────

function RadialTopology({ nodes, groups }: { nodes: NodeInfo[]; groups: GroupStatus[] }) {
  const [hoveredNode, setHoveredNode] = useState<string | null>(null)

  const activeGroup = groups.find(g => g.lease_active) ?? groups[0] ?? null
  const authLeaderID = activeGroup?.leader_id ?? ''

  const effectiveRole = useCallback((n: NodeInfo): 'leader' | 'follower' => {
    if (authLeaderID && (n.node_id === authLeaderID || n.addr === authLeaderID)) return 'leader'
    return n.role === 'leader' ? 'leader' : 'follower'
  }, [authLeaderID])

  const sorted = useMemo(() => [...nodes].sort((a, b) => {
    const rank = (n: NodeInfo) => effectiveRole(n) === 'leader' ? 0 : n.reachable ? 1 : 2
    return rank(a) - rank(b)
  }), [nodes, effectiveRole])

  const leaderNode = sorted.find(n => effectiveRole(n) === 'leader') ?? null
  const followerNodes = sorted.filter(n => effectiveRole(n) !== 'leader')

  // Radial positions for SVG
  const CX = 220, CY = 130, ORBIT_R = 105
  const followerPositions = followerNodes.map((_, i) => {
    const total = followerNodes.length
    const startAngle = -Math.PI / 2
    const spread = total === 1 ? 0 : Math.PI * 0.75
    const angle = total === 1 ? startAngle + Math.PI * 0.5 : startAngle + (i / (total - 1)) * spread + (Math.PI - spread) / 2
    return {
      x: CX + ORBIT_R * Math.cos(angle),
      y: CY + ORBIT_R * Math.sin(angle),
    }
  })

  const termSuffix = activeGroup ? `T${activeGroup.term}` : ''
  const lbl = leaderNode ? nodeLabel(leaderNode) : { primary: authLeaderID || '—', secondary: null }

  return (
    <div className="cl2-topology-container">
      <div className="cl2-topology-bg-mesh" />
      <svg className="cl2-topology-svg" viewBox="0 0 440 280" preserveAspectRatio="xMidYMid meet">
        <defs>
          {/* Glow filter for leader */}
          <filter id="leader-glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="8" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {/* Orbital ring */}
        {followerNodes.length > 0 && (
          <circle cx={CX} cy={CY} r={ORBIT_R} fill="none"
            stroke="rgba(99,91,255,0.06)" strokeWidth="1" strokeDasharray="4 4" />
        )}

        {/* Replication lines from leader to followers */}
        {followerNodes.map((f, i) => {
          const pos = followerPositions[i]
          const alive = f.reachable
          return (
            <g key={`line-${f.node_id}`}>
              {/* Background track */}
              <line x1={CX} y1={CY} x2={pos.x} y2={pos.y}
                stroke={alive ? 'rgba(99,91,255,0.08)' : 'rgba(248,113,113,0.08)'}
                strokeWidth="1" strokeDasharray={alive ? 'none' : '3 3'} />
              {/* Animated data-flow particles */}
              {alive && (
                <>
                  <circle r="2" fill="var(--accent)" opacity="0.8">
                    <animateMotion dur="2.5s" repeatCount="indefinite"
                      path={`M${CX},${CY} L${pos.x},${pos.y}`} />
                    <animate attributeName="opacity" values="0;0.9;0" dur="2.5s" repeatCount="indefinite" />
                  </circle>
                  <circle r="1.5" fill="var(--accent)" opacity="0.5">
                    <animateMotion dur="2.5s" repeatCount="indefinite" begin="0.8s"
                      path={`M${CX},${CY} L${pos.x},${pos.y}`} />
                    <animate attributeName="opacity" values="0;0.7;0" dur="2.5s" repeatCount="indefinite" begin="0.8s" />
                  </circle>
                </>
              )}
              {/* Lag badge on line */}
              {alive && f.lag > 0 && (
                <g transform={`translate(${(CX + pos.x) / 2}, ${(CY + pos.y) / 2 + 5})`}>
                  <rect x="-14" y="-5" width="28" height="10" rx="3"
                    fill={f.lag < 100 ? 'rgba(52,211,153,0.15)' : f.lag < 500 ? 'rgba(251,191,36,0.15)' : 'rgba(248,113,113,0.15)'}
                    stroke={f.lag < 100 ? 'rgba(52,211,153,0.3)' : f.lag < 500 ? 'rgba(251,191,36,0.3)' : 'rgba(248,113,113,0.3)'}
                    strokeWidth="0.5" />
                  <text x="0" y="2.5" fontSize="5.5" textAnchor="middle"
                    fill={f.lag < 100 ? 'var(--text-safe)' : f.lag < 500 ? 'var(--text-warning)' : 'var(--text-unsafe)'}
                    fontFamily="'JetBrains Mono', monospace">
                    +{fmt(f.lag)}
                  </text>
                </g>
              )}
            </g>
          )
        })}

        {/* LEADER node */}
        <g className={`cl2-svg-node ${leaderNode?.reachable !== false ? 'cl2-svg-node-alive' : ''}`}
           onMouseEnter={() => setHoveredNode(leaderNode?.node_id ?? 'leader')}
           onMouseLeave={() => setHoveredNode(null)}>
          {/* Outer pulse rings */}
          {leaderNode?.reachable !== false && (
            <>
              <circle cx={CX} cy={CY} r="20" fill="none"
                stroke="rgba(99,91,255,0.2)" strokeWidth="0.8"
                className="cl2-pulse-ring-1" />
              <circle cx={CX} cy={CY} r="24" fill="none"
                stroke="rgba(99,91,255,0.1)" strokeWidth="0.8"
                className="cl2-pulse-ring-2" />
            </>
          )}
          {/* Glow background */}
          <circle cx={CX} cy={CY} r="15"
            fill="rgba(99,91,255,0.06)" filter="url(#leader-glow)" />
          {/* Main circle */}
          <circle cx={CX} cy={CY} r="13"
            fill="rgba(99,91,255,0.08)"
            stroke={leaderNode?.reachable !== false ? 'rgba(99,91,255,0.5)' : 'rgba(248,113,113,0.3)'}
            strokeWidth="2" />
          {/* Crown icon */}
          <text x={CX} y={CY - 19} fontSize="10" textAnchor="middle"
            fill="#fbbf24" className="cl2-crown-float">&#9819;</text>
          {/* Hexagon icon */}
          <path d={`M${CX} ${CY - 5} l4 2.3v4.6l-4 2.3-4-2.3v-4.6z`}
            fill="none" stroke="var(--accent)" strokeWidth="1" opacity="0.8" />
          {/* Node name */}
          <text x={CX} y={CY + 19} fontSize="7" fontWeight="700"
            fontFamily="'JetBrains Mono', monospace"
            fill="var(--text-primary)" textAnchor="middle">
            {lbl.primary}
          </text>
          {lbl.secondary && (
            <text x={CX} y={CY + 26} fontSize="5.5"
              fontFamily="'JetBrains Mono', monospace"
              fill="var(--text-muted)" textAnchor="middle" opacity="0.7">
              {lbl.secondary}
            </text>
          )}
          {/* Role badge */}
          <rect x={CX - 22} y={CY + 28} width="44" height="10" rx="3"
            fill="rgba(99,91,255,0.15)" stroke="rgba(99,91,255,0.25)" strokeWidth="0.5" />
          <text x={CX} y={CY + 35} fontSize="5" fontWeight="700" letterSpacing="0.08em"
            fill="var(--text-accent)" textAnchor="middle">
            LEADER {termSuffix}
          </text>
          {/* LSN */}
          {leaderNode?.reachable && (
            <text x={CX} y={CY + 44} fontSize="5.5"
              fontFamily="'JetBrains Mono', monospace"
              fill="var(--text-muted)" textAnchor="middle">
              LSN {fmt(leaderNode.lsn)}
            </text>
          )}
        </g>

        {/* FOLLOWER nodes */}
        {followerNodes.map((follower, i) => {
          const pos = followerPositions[i]
          const fl = nodeLabel(follower)
          const isHovered = hoveredNode === follower.node_id
          return (
            <g key={follower.node_id}
               className={`cl2-svg-node ${follower.reachable ? 'cl2-svg-node-alive' : ''}`}
               onMouseEnter={() => setHoveredNode(follower.node_id)}
               onMouseLeave={() => setHoveredNode(null)}>
              {/* Pulse ring */}
              {follower.reachable && (
                <circle cx={pos.x} cy={pos.y} r="15" fill="none"
                  stroke="rgba(52,211,153,0.2)" strokeWidth="0.8"
                  className="cl2-pulse-ring-follower" />
              )}
              {/* Main circle */}
              <circle cx={pos.x} cy={pos.y} r="11"
                fill={follower.reachable ? 'rgba(52,211,153,0.06)' : 'rgba(248,113,113,0.04)'}
                stroke={follower.reachable ? 'rgba(52,211,153,0.4)' : 'rgba(248,113,113,0.25)'}
                strokeWidth="1.5"
                style={{ transition: 'all 0.3s ease', transform: isHovered ? 'scale(1.08)' : 'scale(1)', transformOrigin: `${pos.x}px ${pos.y}px` }} />
              {/* Clock icon for follower */}
              <circle cx={pos.x} cy={pos.y} r="4" fill="none"
                stroke={follower.reachable ? 'var(--text-safe)' : 'var(--text-unsafe)'}
                strokeWidth="0.8" opacity="0.6" />
              <line x1={pos.x} y1={pos.y - 2} x2={pos.x} y2={pos.y}
                stroke={follower.reachable ? 'var(--text-safe)' : 'var(--text-unsafe)'}
                strokeWidth="0.8" strokeLinecap="round" opacity="0.6" />
              <line x1={pos.x} y1={pos.y} x2={pos.x + 1.5} y2={pos.y + 1.5}
                stroke={follower.reachable ? 'var(--text-safe)' : 'var(--text-unsafe)'}
                strokeWidth="0.8" strokeLinecap="round" opacity="0.6" />

              {/* Name */}
              <text x={pos.x} y={pos.y + 17} fontSize="7" fontWeight="700"
                fontFamily="'JetBrains Mono', monospace"
                fill="var(--text-primary)" textAnchor="middle">
                {fl.primary}
              </text>
              {fl.secondary && (
                <text x={pos.x} y={pos.y + 24} fontSize="5"
                  fontFamily="'JetBrains Mono', monospace"
                  fill="var(--text-muted)" textAnchor="middle" opacity="0.7">
                  {fl.secondary}
                </text>
              )}
              {/* Role badge */}
              <rect x={pos.x - 16} y={pos.y + 26} width="32" height="9" rx="2.5"
                fill="rgba(52,211,153,0.1)" stroke="rgba(52,211,153,0.2)" strokeWidth="0.5" />
              <text x={pos.x} y={pos.y + 33} fontSize="5" fontWeight="700" letterSpacing="0.06em"
                fill="var(--text-safe)" textAnchor="middle">
                REPLICA
              </text>
              {/* LSN + lag */}
              {follower.reachable ? (
                <>
                  <text x={pos.x} y={pos.y + 42} fontSize="5.5"
                    fontFamily="'JetBrains Mono', monospace"
                    fill="var(--text-muted)" textAnchor="middle">
                    LSN {fmt(follower.lsn)}
                  </text>
                  <text x={pos.x} y={pos.y + 49} fontSize="5"
                    fontFamily="'JetBrains Mono', monospace"
                    fill={follower.lag === 0 ? 'var(--text-safe)' : follower.lag < 500 ? 'var(--text-warning)' : 'var(--text-unsafe)'}
                    textAnchor="middle" fontWeight="600">
                    {follower.lag === 0 ? '\u2713 synced' : `+${fmt(follower.lag)} lag`}
                  </text>
                </>
              ) : (
                <text x={pos.x} y={pos.y + 42} fontSize="5.5"
                  fill="var(--text-unsafe)" textAnchor="middle" opacity="0.7">
                  unreachable
                </text>
              )}

              {/* Expanded detail tooltip on hover */}
              {isHovered && follower.reachable && (
                <g className="cl2-node-tooltip">
                  <rect x={pos.x - 45} y={pos.y - 55} width="90" height="40" rx="6"
                    fill="var(--bg-elevated)" stroke="var(--border-strong)" strokeWidth="0.5"
                    filter="url(#leader-glow)" />
                  <text x={pos.x} y={pos.y - 44} fontSize="5.5" textAnchor="middle" fill="var(--text-muted)">
                    Replication Details
                  </text>
                  <text x={pos.x - 36} y={pos.y - 35} fontSize="5.5" fill="var(--text-secondary)">
                    Lag entries:
                  </text>
                  <text x={pos.x + 36} y={pos.y - 35} fontSize="5.5" textAnchor="end"
                    fill="var(--text-primary)" fontFamily="'JetBrains Mono', monospace">
                    {follower.lag.toLocaleString()}
                  </text>
                  <text x={pos.x - 36} y={pos.y - 26} fontSize="5.5" fill="var(--text-secondary)">
                    Status:
                  </text>
                  <text x={pos.x + 36} y={pos.y - 26} fontSize="5.5" textAnchor="end"
                    fill={follower.lag === 0 ? 'var(--text-safe)' : 'var(--text-warning)'}
                    fontWeight="600">
                    {lagSeverity(follower.lag).toUpperCase()}
                  </text>
                </g>
              )}
            </g>
          )
        })}

        {/* Empty state */}
        {followerNodes.length === 0 && (
          <text x={CX} y={CY + 50} fontSize="7" textAnchor="middle"
            fill="var(--text-muted)" fontStyle="italic" opacity="0.5">
            single-node · no follower configured
          </text>
        )}
      </svg>
    </div>
  )
}

// ─────────────────────────────── KPI Cards Row ─────────────────────────────

function KPICard({ label, value, sub, icon, color, warn }: {
  label: string; value: string; sub?: string; icon: React.ReactNode; color?: string; warn?: boolean
}) {
  return (
    <div className={`cl2-kpi-card ${warn ? 'cl2-kpi-warn' : ''}`}>
      <div className="cl2-kpi-icon" style={color ? { color } : undefined}>{icon}</div>
      <div className="cl2-kpi-body">
        <div className="cl2-kpi-label">{label}</div>
        <div className="cl2-kpi-value">{value}</div>
        {sub && <div className="cl2-kpi-sub">{sub}</div>}
      </div>
    </div>
  )
}

function KPIRow({ diagnostics, nodes }: { diagnostics: ClusterDiagnostics | null; nodes: NodeInfo[] }) {
  const stats = diagnostics?.engine_stats
  const summary = diagnostics?.summary
  const reachable = nodes.filter(n => n.reachable).length
  const followers = nodes.filter(n => n.role === 'follower' && n.reachable)
  const worstLag = followers.length > 0 ? Math.max(...followers.map(n => n.lag)) : null

  return (
    <div className="cl2-kpi-row">
      <KPICard label="Throughput" value={stats?.commit_throughput_per_sec ? `${stats.commit_throughput_per_sec.toFixed(1)}/s` : '\u2014'} sub="commits"
        icon={<svg viewBox="0 0 20 20" width="18" height="18"><path d="M3 17V7l4-4 4 4 4-4v10" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>} />
      <KPICard label="Commit p95" value={fmtMs(stats?.commit_latency_p95_ms)} sub="latency"
        icon={<svg viewBox="0 0 20 20" width="18" height="18"><circle cx="10" cy="10" r="7" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="M10 6v4l3 2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>}
        warn={(stats?.commit_latency_p95_ms ?? 0) > 100} />
      <KPICard label="Fsync p95" value={fmtMs(stats?.fsync_latency_p95_ms)} sub="latency"
        icon={<svg viewBox="0 0 20 20" width="18" height="18"><path d="M4 14l4-4 3 3 5-5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" /></svg>}
        warn={(stats?.fsync_latency_p95_ms ?? 0) > 50} />
      <KPICard label="Nodes" value={`${reachable}/${nodes.length}`} sub="reachable"
        icon={<svg viewBox="0 0 20 20" width="18" height="18"><circle cx="6" cy="10" r="2.5" fill="none" stroke="currentColor" strokeWidth="1.5" /><circle cx="14" cy="10" r="2.5" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="M8.5 10h3" stroke="currentColor" strokeWidth="1.5" /></svg>}
        color={reachable < nodes.length ? 'var(--text-unsafe)' : 'var(--text-safe)'}
        warn={reachable < nodes.length} />
      <KPICard label="WAL Size" value={fmtBytes(stats?.wal_file_size_bytes)} sub="on disk"
        icon={<svg viewBox="0 0 20 20" width="18" height="18"><rect x="4" y="3" width="12" height="14" rx="2" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="M7 7h6M7 10h4" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" /></svg>} />
      <KPICard label="Worst Lag" value={worstLag !== null ? fmt(worstLag) : (summary?.worst_replication_lag !== undefined ? fmt(summary.worst_replication_lag) : '\u2014')} sub="entries"
        icon={<svg viewBox="0 0 20 20" width="18" height="18"><path d="M3 10a7 7 0 1114 0A7 7 0 013 10z" fill="none" stroke="currentColor" strokeWidth="1.5" /><path d="M10 6v5l3 2" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" /></svg>}
        color={worstLag !== null && worstLag > 500 ? 'var(--text-unsafe)' : worstLag !== null && worstLag > 100 ? 'var(--text-warning)' : undefined}
        warn={worstLag !== null && worstLag > 500} />
    </div>
  )
}

// ─────────────────────────────── Group Cards ───────────────────────────────

function GroupCard({ group, history, followerLag }: {
  group: GroupStatus; history: LsnPoint[]; followerLag: number | null
}) {
  return (
    <div className={`cl2-group-card ${group.lease_active ? 'cl2-group-healthy' : 'cl2-group-inactive'}`}>
      <div className="cl2-group-glow" />

      <div className="cl2-group-head">
        <div className="cl2-group-head-left">
          <div className={`cl2-group-status-dot ${group.lease_active ? 'dot-ok' : 'dot-err'}`} />
          <span className="cl2-group-name">{group.group}</span>
        </div>
        <span className={`cl2-lease-badge ${group.lease_active ? 'cl2-lease-active' : 'cl2-lease-inactive'}`}>
          {group.lease_active ? '\u25CF ACTIVE' : '\u25CB INACTIVE'}
        </span>
      </div>

      <div className="cl2-group-detail-grid">
        <div className="cl2-group-detail">
          <span className="cl2-detail-label">Leader</span>
          <span className="cl2-detail-value">
            <span className="cl2-crown-sm">&#9819;</span> {group.leader_id || '\u2014'}
          </span>
        </div>
        <div className="cl2-group-detail">
          <span className="cl2-detail-label">Term</span>
          <span className="cl2-detail-value mono">{group.term}</span>
        </div>
        <div className="cl2-group-detail">
          <span className="cl2-detail-label">Fencing</span>
          <span className="cl2-detail-value mono" title={group.fencing_token}>
            {group.fencing_token ? (group.fencing_token.length > 8 ? group.fencing_token.slice(0, 8) + '\u2026' : group.fencing_token) : '\u2014'}
          </span>
        </div>
        <div className="cl2-group-detail">
          <span className="cl2-detail-label">Head LSN</span>
          <span className="cl2-detail-value mono">{fmt(group.last_lsn)}</span>
        </div>
      </div>

      <div className="cl2-group-sparkline-row">
        <LsnSparkline history={history} id={`group-${group.group}`}
          color={group.lease_active ? 'var(--text-safe)' : 'var(--text-unsafe)'} />
        <div className="cl2-sparkline-meta">
          <span className="cl2-sparkline-meta-label">WAL velocity</span>
          {history.length >= 2 && (
            <span className="cl2-sparkline-meta-val">
              +{fmt(history[history.length - 1].lsn - history[Math.max(0, history.length - 10)].lsn)} / 30s
            </span>
          )}
        </div>
      </div>

      {followerLag !== null && (
        <div className="cl2-group-lag-row">
          <span className="cl2-detail-label">Replication</span>
          <div className={`cl2-lag-indicator ${lagClass(followerLag)}`}>
            <div className="cl2-lag-bar">
              <div className="cl2-lag-bar-fill" style={{ width: `${Math.min(followerLag / 10, 100)}%` }} />
            </div>
            <span className="cl2-lag-text">
              {followerLag === 0 ? '\u2713 in sync' : `+${fmt(followerLag)}`}
            </span>
          </div>
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────── Routing Panel ─────────────────────────────

function RoutingPanel({ stats }: { stats: RoutingStats | null }) {
  if (!stats || stats.requests_total === 0) {
    return (
      <div className="cl2-card">
        <div className="cl2-card-hdr">
          <span className="cl2-card-title">Read Routing</span>
        </div>
        <div className="cl2-card-empty">No read traffic yet</div>
      </div>
    )
  }

  const total = stats.requests_total
  const leaderPct = total > 0 ? Math.round((stats.route_leader / total) * 100) : 0
  const followerPct = total > 0 ? Math.round((stats.route_follower / total) * 100) : 0

  return (
    <div className="cl2-card">
      <div className="cl2-card-hdr">
        <span className="cl2-card-title">Read Routing</span>
        <span className="cl2-card-meta">{total.toLocaleString()} requests</span>
      </div>

      <div className="cl2-routing-visual">
        <div className="cl2-routing-donut-container">
          <RingGauge value={stats.route_leader} max={total} size={64} stroke={6}
            color="var(--accent)" label={`${leaderPct}%`} sublabel="leader" />
        </div>
        <div className="cl2-routing-breakdown">
          <div className="cl2-routing-bar-row">
            <div className="cl2-routing-bar-label">
              <span className="cl2-routing-dot" style={{ background: 'var(--accent)' }} />
              Leader
            </div>
            <div className="cl2-routing-bar-track">
              <div className="cl2-routing-bar-fill cl2-fill-leader" style={{ width: `${leaderPct}%` }} />
            </div>
            <span className="cl2-routing-bar-val">{stats.route_leader.toLocaleString()}</span>
          </div>
          <div className="cl2-routing-bar-row">
            <div className="cl2-routing-bar-label">
              <span className="cl2-routing-dot" style={{ background: 'var(--text-safe)' }} />
              Follower
            </div>
            <div className="cl2-routing-bar-track">
              <div className="cl2-routing-bar-fill cl2-fill-follower" style={{ width: `${followerPct}%` }} />
            </div>
            <span className="cl2-routing-bar-val">{stats.route_follower.toLocaleString()}</span>
          </div>
        </div>
      </div>

      {(stats.fallback_follower_unavailable > 0 || stats.fallback_lag_exceeded > 0) && (
        <div className="cl2-routing-alerts">
          {stats.fallback_follower_unavailable > 0 && (
            <span className="cl2-alert-pill cl2-alert-warn">{stats.fallback_follower_unavailable} unavailable fallbacks</span>
          )}
          {stats.fallback_lag_exceeded > 0 && (
            <span className="cl2-alert-pill cl2-alert-lag">{stats.fallback_lag_exceeded} lag-exceeded</span>
          )}
        </div>
      )}

      <div className="cl2-routing-consistency-row">
        <div className="cl2-consistency-chip">
          <span className="cl2-con-label">Strong</span>
          <span className="cl2-con-val">{stats.consistency_strong.toLocaleString()}</span>
        </div>
        <div className="cl2-consistency-chip">
          <span className="cl2-con-label">Bounded-stale</span>
          <span className="cl2-con-val">{stats.consistency_bounded_stale.toLocaleString()}</span>
        </div>
      </div>
    </div>
  )
}

// ─────────────────────────────── Event Timeline ────────────────────────────

function EventTimeline({ events }: { events: ClusterEvent[] }) {
  const iconMap: Record<string, string> = {
    'leader-elected': '\u265B', 'lease-lost': '\u25CC', 'lag-spike': '\u26A1',
    'node-down': '\u2715', 'node-up': '\u2713', info: '\u2139',
  }
  const colorMap: Record<string, string> = {
    'leader-elected': 'var(--accent)', 'lease-lost': 'var(--text-warning)',
    'lag-spike': 'var(--text-warning)', 'node-down': 'var(--text-unsafe)',
    'node-up': 'var(--text-safe)', info: 'var(--text-secondary)',
  }

  return (
    <div className="cl2-card cl2-event-card">
      <div className="cl2-card-hdr">
        <span className="cl2-card-title">Event Timeline</span>
        {events.length > 0 && <span className="cl2-card-badge">{events.length}</span>}
      </div>
      <div className="cl2-event-timeline">
        {events.length === 0 ? (
          <div className="cl2-card-empty">Cluster is quiet — no events</div>
        ) : events.map((ev) => (
          <div key={ev.id} className="cl2-event-item" style={{ '--ev-color': colorMap[ev.type] || 'var(--text-secondary)' } as React.CSSProperties}>
            <div className="cl2-event-timeline-dot">
              <div className="cl2-event-timeline-line" />
              <div className="cl2-event-dot" />
            </div>
            <div className="cl2-event-body">
              <span className="cl2-event-icon">{iconMap[ev.type] || '\u2139'}</span>
              <span className="cl2-event-msg">{ev.msg}</span>
              <span className="cl2-event-ts">{relative(ev.ts)}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ─────────────────────────────── Admin Nodes ───────────────────────────────

function AdminNodesPanel({ nodes }: { nodes: ClusterAdminNode[] }) {
  const [expanded, setExpanded] = useState<string | null>(null)

  if (nodes.length === 0) {
    return (
      <div className="cl2-card">
        <div className="cl2-card-hdr">
          <span className="cl2-card-title">Node Health</span>
        </div>
        <div className="cl2-card-empty">No admin endpoints configured</div>
      </div>
    )
  }

  const highestLSN = Math.max(...nodes.map(n => n.last_durable_lsn ?? 0))

  return (
    <div className="cl2-card">
      <div className="cl2-card-hdr">
        <span className="cl2-card-title">Node Health & Retention</span>
        <span className="cl2-card-meta">{nodes.length} endpoint{nodes.length > 1 ? 's' : ''}</span>
      </div>
      <div className="cl2-admin-nodes">
        {nodes.map((node) => {
          const lag = Math.max(0, highestLSN - (node.last_durable_lsn ?? 0))
          const isExpanded = expanded === node.endpoint
          return (
            <div key={node.endpoint}
              className={`cl2-admin-node ${isExpanded ? 'cl2-admin-node-expanded' : ''}`}
              onClick={() => setExpanded(isExpanded ? null : node.endpoint)}>
              <div className="cl2-admin-node-head">
                <div className="cl2-admin-node-info">
                  <div className="cl2-admin-node-name">{node.node_id || node.endpoint}</div>
                  <div className="cl2-admin-node-endpoint">{node.endpoint}</div>
                </div>
                <div className="cl2-admin-node-badges">
                  <span className={`cl2-health-badge ${node.ready ? 'cl2-badge-ok' : 'cl2-badge-err'}`}>
                    {node.ready ? 'READY' : 'NOT READY'}
                  </span>
                  <span className={`cl2-health-badge ${node.live ? 'cl2-badge-ok' : 'cl2-badge-err'}`}>
                    {node.live ? 'LIVE' : 'DOWN'}
                  </span>
                </div>
              </div>
              {/* Always-visible summary row */}
              <div className="cl2-admin-node-summary">
                <span className="cl2-admin-mini">{node.raft_role || '\u2014'}</span>
                <span className="cl2-admin-mini">T{node.current_term ?? '\u2014'}</span>
                <span className="cl2-admin-mini">LSN {fmt(node.last_durable_lsn ?? 0)}</span>
                {lag > 0 && <span className="cl2-admin-mini cl2-admin-lag">+{fmt(lag)} lag</span>}
              </div>
              {/* Expandable details */}
              {isExpanded && (
                <div className="cl2-admin-node-details">
                  <div className="cl2-admin-detail-grid">
                    <div className="cl2-admin-detail"><span>WAL segments</span><span className="mono">{fmt(node.segment_count ?? 0)}</span></div>
                    <div className="cl2-admin-detail"><span>Snapshots</span><span className="mono">{fmt(node.snapshot_catalog_len ?? node.disk_snapshot_count ?? 0)}</span></div>
                    <div className="cl2-admin-detail"><span>Head LSN</span><span className="mono">{fmt(node.head_lsn ?? 0)}</span></div>
                    <div className="cl2-admin-detail"><span>Oldest retained</span><span className="mono">{fmt(node.oldest_retained_lsn ?? 0)}</span></div>
                  </div>
                  {node.error && <div className="cl2-admin-error">{node.error}</div>}
                  {!!node.reasons?.length && <div className="cl2-admin-reasons">{node.reasons.join(' \u00B7 ')}</div>}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ─────────────────────────────── Failover History ──────────────────────────

function FailoverHistoryPanel({ history }: { history: ClusterFailoverTransition[] }) {
  return (
    <div className="cl2-card cl2-failover-card">
      <div className="cl2-card-hdr">
        <span className="cl2-card-title">Failover History</span>
        <span className="cl2-card-meta">{history.length} transition{history.length !== 1 ? 's' : ''}</span>
      </div>
      {history.length === 0 ? (
        <div className="cl2-card-empty">No recorded failover transitions</div>
      ) : (
        <div className="cl2-failover-list">
          {history.slice().reverse().map((entry, idx) => (
            <div key={`${entry.group_name}-${entry.term}-${entry.node_id}-${idx}`}
              className="cl2-failover-item">
              <div className="cl2-failover-icon">\u21C4</div>
              <div className="cl2-failover-body">
                <span className="cl2-failover-group">{entry.group_name || 'default'}</span>
                <span className="cl2-failover-phase">{entry.phase}</span>
                <span className="cl2-failover-node">{entry.node_id || '\u2014'}</span>
              </div>
              <div className="cl2-failover-term">T{entry.term}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────── Metrics Deep Dive ─────────────────────────

/* ── Metric SVG Icons ─────────────────────────────────────────────────── */

const MetricIcon = ({ type }: { type: string }) => {
  const s = { width: 14, height: 14, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.8, strokeLinecap: 'round' as const, strokeLinejoin: 'round' as const }
  switch (type) {
    case 'bolt':
      return <svg {...s}><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" fill="rgba(251,191,36,0.15)" stroke="var(--text-warning)" /></svg>
    case 'read':
      return <svg {...s}><path d="M2 3h6a4 4 0 014 4v14a3 3 0 00-3-3H2z" /><path d="M22 3h-6a4 4 0 00-4 4v14a3 3 0 013-3h7z" /></svg>
    case 'timer':
      return <svg {...s}><circle cx="12" cy="13" r="8" /><path d="M12 9v4l2.5 1.5" /><path d="M10 2h4" /><path d="M12 2v2" /></svg>
    case 'disk':
      return <svg {...s}><path d="M19 21H5a2 2 0 01-2-2V5a2 2 0 012-2h11l5 5v11a2 2 0 01-2 2z" /><polyline points="17 21 17 13 7 13 7 21" /><polyline points="7 3 7 8 15 8" /></svg>
    case 'refresh':
      return <svg {...s}><path d="M21 2v6h-6" /><path d="M3 12a9 9 0 0115.36-6.36L21 8" /><path d="M3 22v-6h6" /><path d="M21 12a9 9 0 01-15.36 6.36L3 16" /></svg>
    case 'camera':
      return <svg {...s}><path d="M23 19a2 2 0 01-2 2H3a2 2 0 01-2-2V8a2 2 0 012-2h4l2-3h6l2 3h4a2 2 0 012 2z" /><circle cx="12" cy="13" r="4" /></svg>
    case 'file':
      return <svg {...s}><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" /><polyline points="14 2 14 8 20 8" /><line x1="8" y1="13" x2="16" y2="13" /><line x1="8" y1="17" x2="16" y2="17" /></svg>
    case 'box':
      return <svg {...s}><path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z" /><polyline points="3.27 6.96 12 12.01 20.73 6.96" /><line x1="12" y1="22.08" x2="12" y2="12" /></svg>
    case 'search':
      return <svg {...s}><circle cx="11" cy="11" r="7" /><line x1="21" y1="21" x2="16.65" y2="16.65" /></svg>
    case 'warning':
      return <svg {...s}><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z" fill="rgba(248,113,113,0.12)" stroke="var(--text-unsafe)" /><line x1="12" y1="9" x2="12" y2="13" stroke="var(--text-unsafe)" /><circle cx="12" cy="16.5" r="0.5" fill="var(--text-unsafe)" stroke="none" /></svg>
    default:
      return null
  }
}

function MetricsDeepDive({ diagnostics }: { diagnostics: ClusterDiagnostics | null }) {
  const stats = diagnostics?.engine_stats
  if (!stats) return null

  const sections = [
    {
      title: 'Performance',
      metrics: [
        { label: 'Commit throughput', value: stats.commit_throughput_per_sec ? `${stats.commit_throughput_per_sec.toFixed(1)}/s` : '\u2014', icon: 'bolt' },
        { label: 'Read throughput', value: stats.read_throughput_per_sec ? `${stats.read_throughput_per_sec.toFixed(1)}/s` : '\u2014', icon: 'read' },
        { label: 'Commit p95', value: fmtMs(stats.commit_latency_p95_ms), icon: 'timer' },
        { label: 'Commit p99', value: fmtMs(stats.commit_latency_p99_ms), icon: 'timer' },
        { label: 'Fsync p95', value: fmtMs(stats.fsync_latency_p95_ms), icon: 'disk' },
      ],
    },
    {
      title: 'Recovery',
      metrics: [
        { label: 'Replay duration', value: fmtMs(stats.replay_duration_ms), icon: 'refresh' },
        { label: 'Snapshot duration', value: fmtMs(stats.snapshot_duration_ms), icon: 'camera' },
      ],
    },
    {
      title: 'Storage',
      metrics: [
        { label: 'WAL size', value: fmtBytes(stats.wal_file_size_bytes), icon: 'file' },
        { label: 'Snapshots size', value: fmtBytes(stats.snapshot_file_size_bytes), icon: 'box' },
        { label: 'Audit size', value: fmtBytes(stats.audit_file_size_bytes), icon: 'search' },
      ],
    },
    {
      title: 'Errors',
      metrics: [
        { label: 'Fsync errors', value: stats.total_fsync_errors !== undefined ? fmt(stats.total_fsync_errors) : '\u2014', icon: 'warning', warn: (stats.total_fsync_errors ?? 0) > 0 },
        { label: 'Audit errors', value: stats.total_audit_errors !== undefined ? fmt(stats.total_audit_errors) : '\u2014', icon: 'warning', warn: (stats.total_audit_errors ?? 0) > 0 },
      ],
    },
  ]

  return (
    <div className="cl2-card cl2-metrics-deep">
      <div className="cl2-card-hdr">
        <span className="cl2-card-title">Operator Metrics</span>
        <span className="cl2-card-meta">real-time signals</span>
      </div>
      <div className="cl2-metrics-sections">
        {sections.map(section => (
          <div key={section.title} className="cl2-metrics-section">
            <div className="cl2-metrics-section-title">{section.title}</div>
            <div className="cl2-metrics-chips">
              {section.metrics.map(m => (
                <div key={m.label} className={`cl2-metric-chip ${(m as {warn?: boolean}).warn ? 'cl2-metric-warn' : ''}`}>
                  <span className="cl2-metric-icon"><MetricIcon type={m.icon} /></span>
                  <div className="cl2-metric-body">
                    <span className="cl2-metric-label">{m.label}</span>
                    <span className="cl2-metric-value">{m.value}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}

// ─────────────────────────────── Main Panel ─────────────────────────────────

export function ClusterPanel() {
  const {
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
  } = useCluster()

  const health = useMemo(
    () => computeHealthScore(nodeStatus, groups, diagnostics),
    [nodeStatus, groups, diagnostics]
  )

  const followers = nodeStatus.filter(n => n.role === 'follower')
  const followerLag = followers.some(n => n.reachable)
    ? Math.max(...followers.filter(n => n.reachable).map(n => n.lag))
    : null

  const activeGroup = groups.find(g => g.lease_active) ?? groups[0] ?? null
  const leaderLabel = activeGroup?.leader_id || ''
  const term = activeGroup?.term ?? 0

  return (
    <div className="cl2-panel">
      {/* Header */}
      <div className="cl2-header">
        <div className="cl2-header-left">
          <div className="cl2-header-brand">
            <svg viewBox="0 0 24 24" width="20" height="20" fill="none" className="cl2-header-icon">
              <path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"
                stroke="var(--accent)" strokeWidth="1.5" />
              <polyline points="3.27 6.96 12 12.01 20.73 6.96"
                stroke="var(--accent)" strokeWidth="1.5" opacity="0.6" />
              <line x1="12" y1="22.08" x2="12" y2="12"
                stroke="var(--accent)" strokeWidth="1.5" opacity="0.4" />
            </svg>
            <h2 className="cl2-title">Cluster</h2>
          </div>
          {lastRefresh > 0 && (
            <span className="cl2-last-refresh">
              {new Date(lastRefresh).toLocaleTimeString()}
            </span>
          )}
          {loading && <span className="cl2-loading-pulse">refreshing</span>}
        </div>
        <div className="cl2-header-right">
          <label className={`auto-refresh-toggle ${autoRefresh ? 'on' : ''}`}>
            <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
            <span className="auto-refresh-track"><span className="auto-refresh-thumb" /></span>
            <span className="auto-refresh-label">Live</span>
          </label>
          <button className="cl2-refresh-btn" onClick={refresh} disabled={loading} title="Refresh now">
            <IconRefresh />
          </button>
        </div>
      </div>

      {error && (
        <div className="cl2-error-bar">
          <span>\u26A0</span> {error}
        </div>
      )}

      <div className="cl2-body">
        {/* Status Banner */}
        <StatusBanner health={health} nodeCount={nodeStatus.length}
          leaderNode={leaderLabel} term={term} />

        {/* KPI Cards */}
        <KPIRow diagnostics={diagnostics} nodes={nodeStatus} />

        {/* Topology + Groups + Routing side by side */}
        <div className="cl2-main-grid">
          <div className="cl2-main-left">
            <MetricsDeepDive diagnostics={diagnostics} />
            <RadialTopology nodes={nodeStatus} groups={groups} />
          </div>
          <div className="cl2-main-right">
            {configuredGroups.length > 0 && groups.length > 0 ? (
              groups.map((g) => (
                <GroupCard key={g.group} group={g}
                  history={lsnHistory[g.group] ?? []}
                  followerLag={followerLag} />
              ))
            ) : configuredGroups.length === 0 ? (
              <div className="cl2-card">
                <div className="cl2-card-empty" style={{ padding: 20 }}>
                  <IconShield />
                  <span style={{ marginLeft: 8 }}>
                    Start with <code className="mono">--groups g1,g2</code> to enable group monitoring
                  </span>
                </div>
              </div>
            ) : null}
            <div className="cl2-routing-health-row">
              <RoutingPanel stats={routingStats} />
              <AdminNodesPanel nodes={diagnostics?.admin_nodes ?? []} />
            </div>
            <EventTimeline events={events} />
            <FailoverHistoryPanel history={diagnostics?.failover_history ?? []} />
          </div>
        </div>
      </div>
    </div>
  )
}
