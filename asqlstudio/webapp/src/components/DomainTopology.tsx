import { useMemo } from 'react'
import type { GroupStatus } from '../hooks/useCluster'

type Props = {
  groups: GroupStatus[]
}

type TopoNode = {
  id: string
  label: string
  x: number
  y: number
  isLeader: boolean
  leaseActive: boolean
  term: number
  lastLSN: number
  leaderId: string
}

const NODE_R = 40
const CENTER_X = 250
const CENTER_Y = 180
const ORBIT_R = 120

export function DomainTopology({ groups }: Props) {
  const nodes = useMemo<TopoNode[]>(() => {
    if (groups.length === 0) return []
    const count = groups.length
    return groups.map((g, i) => {
      const angle = (2 * Math.PI * i) / count - Math.PI / 2
      return {
        id: g.group,
        label: g.group,
        x: CENTER_X + ORBIT_R * Math.cos(angle),
        y: CENTER_Y + ORBIT_R * Math.sin(angle),
        isLeader: g.lease_active,
        leaseActive: g.lease_active,
        term: g.term,
        lastLSN: g.last_lsn,
        leaderId: g.leader_id,
      }
    })
  }, [groups])

  if (nodes.length === 0) {
    return (
      <div className="panel-empty">
        <span className="text-muted">No groups to display</span>
      </div>
    )
  }

  // Find max LSN for relative sizing
  const maxLSN = Math.max(...nodes.map(n => n.lastLSN), 1)

  return (
    <div className="domain-topology">
      <svg width="500" height="360" className="domain-topology-svg">
        <defs>
          <radialGradient id="topo-glow-active" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="#10b981" stopOpacity="0.3" />
            <stop offset="100%" stopColor="#10b981" stopOpacity="0" />
          </radialGradient>
          <radialGradient id="topo-glow-inactive" cx="50%" cy="50%" r="50%">
            <stop offset="0%" stopColor="#ef4444" stopOpacity="0.3" />
            <stop offset="100%" stopColor="#ef4444" stopOpacity="0" />
          </radialGradient>
        </defs>

        {/* Replication links between groups */}
        {nodes.length > 1 && nodes.map((from, i) => {
          const to = nodes[(i + 1) % nodes.length]
          const dx = to.x - from.x
          const dy = to.y - from.y
          const dist = Math.sqrt(dx * dx + dy * dy)
          const nx = dx / dist
          const ny = dy / dist
          const x1 = from.x + nx * (NODE_R + 4)
          const y1 = from.y + ny * (NODE_R + 4)
          const x2 = to.x - nx * (NODE_R + 4)
          const y2 = to.y - ny * (NODE_R + 4)

          return (
            <g key={`link-${i}`}>
              <line
                x1={x1} y1={y1} x2={x2} y2={y2}
                stroke="var(--border-strong)"
                strokeWidth={1.5}
                strokeDasharray="6 4"
                opacity={0.4}
              />
              {/* Midpoint arrow */}
              <polygon
                points={`${(x1 + x2) / 2},${(y1 + y2) / 2 - 4} ${(x1 + x2) / 2 + 4},${(y1 + y2) / 2} ${(x1 + x2) / 2},${(y1 + y2) / 2 + 4}`}
                fill="var(--text-muted)"
                opacity={0.3}
                transform={`rotate(${Math.atan2(dy, dx) * 180 / Math.PI}, ${(x1 + x2) / 2}, ${(y1 + y2) / 2})`}
              />
            </g>
          )
        })}

        {/* Central hub label */}
        <text
          x={CENTER_X} y={CENTER_Y}
          textAnchor="middle" dominantBaseline="middle"
          fontSize="11" fontWeight="600"
          fill="var(--text-muted)"
          fontFamily="Inter, sans-serif"
          opacity={0.5}
        >
          CLUSTER
        </text>

        {/* Domain nodes */}
        {nodes.map((node) => {
          const lsnRatio = node.lastLSN / maxLSN
          return (
            <g key={node.id}>
              {/* Glow */}
              <circle
                cx={node.x} cy={node.y} r={NODE_R + 12}
                fill={node.leaseActive ? 'url(#topo-glow-active)' : 'url(#topo-glow-inactive)'}
              />

              {/* Background circle */}
              <circle
                cx={node.x} cy={node.y} r={NODE_R}
                fill="var(--bg-elevated)"
                stroke={node.leaseActive ? '#10b981' : '#ef4444'}
                strokeWidth={2}
              />

              {/* LSN progress ring */}
              <circle
                cx={node.x} cy={node.y} r={NODE_R - 5}
                fill="none"
                stroke={node.leaseActive ? '#10b981' : '#ef4444'}
                strokeWidth={3}
                strokeDasharray={`${lsnRatio * 2 * Math.PI * (NODE_R - 5)} ${2 * Math.PI * (NODE_R - 5)}`}
                strokeDashoffset={2 * Math.PI * (NODE_R - 5) * 0.25}
                opacity={0.3}
              />

              {/* Group name */}
              <text
                x={node.x} y={node.y - 10}
                textAnchor="middle" dominantBaseline="middle"
                fontSize="12" fontWeight="700"
                fill="var(--text-primary)"
                fontFamily="Inter, sans-serif"
              >
                {node.label}
              </text>

              {/* Lease status */}
              <text
                x={node.x} y={node.y + 6}
                textAnchor="middle" dominantBaseline="middle"
                fontSize="8" fontWeight="600"
                fill={node.leaseActive ? '#10b981' : '#ef4444'}
                fontFamily="Inter, sans-serif"
              >
                {node.leaseActive ? 'ACTIVE' : 'INACTIVE'}
              </text>

              {/* Term */}
              <text
                x={node.x} y={node.y + 18}
                textAnchor="middle" dominantBaseline="middle"
                fontSize="8"
                fill="var(--text-muted)"
                fontFamily="'JetBrains Mono', monospace"
              >
                T{node.term}
              </text>

              {/* Leader ID below node */}
              {node.leaderId && (
                <text
                  x={node.x} y={node.y + NODE_R + 14}
                  textAnchor="middle" dominantBaseline="middle"
                  fontSize="9"
                  fill="var(--text-muted)"
                  fontFamily="'JetBrains Mono', monospace"
                >
                  {node.leaderId.length > 14 ? node.leaderId.slice(0, 12) + '..' : node.leaderId}
                </text>
              )}

              {/* LSN below leader */}
              <text
                x={node.x} y={node.y + NODE_R + 26}
                textAnchor="middle" dominantBaseline="middle"
                fontSize="8"
                fill="var(--text-muted)"
                fontFamily="'JetBrains Mono', monospace"
                opacity={0.7}
              >
                LSN: {node.lastLSN.toLocaleString()}
              </text>
            </g>
          )
        })}
      </svg>
    </div>
  )
}
