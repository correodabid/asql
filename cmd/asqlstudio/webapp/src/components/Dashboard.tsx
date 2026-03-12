import { useState, useEffect, useRef, type ReactNode } from 'react'
import { useDashboard, type PIDControllerState } from '../hooks/useDashboard'
import { useAnimatedNumber } from '../hooks/useAnimatedNumber'
import { ChartModal } from './ChartModal'
import { DonutChart, type DonutSegment } from './DonutChart'
import { RadialGauge } from './RadialGauge'
import { IconRefresh, IconAlertTriangle, IconTrendingUp, IconTrendingDown, IconInfo, IconActivity, IconDatabase, IconZap, IconShield, IconCpu, IconServer } from './Icons'

/* ── Formatting helpers ──────────────────────────────────── */

function fmtLatency(ms: number): string {
  if (ms === 0) return '-'
  if (ms < 1) return `${(ms * 1000).toFixed(0)} \u00b5s`
  return `${ms.toFixed(2)} ms`
}

function fmtRate(rate: number): string {
  if (rate === 0) return '0'
  if (rate < 0.01) return '< 0.01'
  return rate.toFixed(2)
}

function fmtCompact(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toLocaleString()
}

function fmtBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

function fmtPIDOutput(name: string, value: number): string {
  if (name.includes('delay')) return `${value.toFixed(0)} \u00b5s`
  return value.toFixed(0)
}

function latencyBgClass(ms: number): string {
  if (ms === 0) return 'lat-cell-neutral'
  if (ms < 1) return 'lat-cell-good'
  if (ms <= 5) return 'lat-cell-ok'
  if (ms <= 15) return 'lat-cell-warn'
  return 'lat-cell-bad'
}

function pidDisplayName(name: string): string {
  return name.replace(/_/g, ' ').toUpperCase()
}

function fmtUptime(ms: number): string {
  const secs = Math.floor(ms / 1000)
  if (secs < 60) return `${secs}s`
  const mins = Math.floor(secs / 60)
  if (mins < 60) return `${mins}m ${secs % 60}s`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ${mins % 60}m`
  const days = Math.floor(hrs / 24)
  return `${days}d ${hrs % 24}h`
}

function fmtNanos(ns: number): string {
  if (ns === 0) return '-'
  if (ns < 1000) return `${ns} ns`
  if (ns < 1_000_000) return `${(ns / 1000).toFixed(1)} \u00b5s`
  return `${(ns / 1_000_000).toFixed(2)} ms`
}

function fmtPct(fraction: number): string {
  return `${(fraction * 100).toFixed(2)}%`
}

/* ── Threshold constants ─────────────────────────────────── */

const THRESH = {
  ROLLBACK_RATIO_WARN: 0.05,
  ROLLBACK_RATIO_BAD: 0.15,
}

/* ── Chart color palette ─────────────────────────────────── */

const PALETTE = ['#635bff', '#34d399', '#f59e0b', '#ec4899', '#06b6d4', '#8b5cf6', '#f87171', '#14b8a6']

/* ── Sparkline ───────────────────────────────────────────── */

type ChartInfo = { data: number[]; title: string; color: string; formatValue: (v: number) => string }

function Sparkline({ data, color = 'var(--accent)', width = 120, height = 32, onClick, showDot = false }: {
  data: number[]
  color?: string
  width?: number
  height?: number
  onClick?: () => void
  showDot?: boolean
}) {
  if (data.length < 2) return <div className="sparkline-empty" style={{ width, height }} />

  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1
  const padding = 2
  const gradId = `spark-${width}-${height}-${color.replace(/[^a-z0-9]/gi, '')}`

  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * width
    const y = height - padding - ((v - min) / range) * (height - padding * 2)
    return `${x},${y}`
  }).join(' ')

  const areaPoints = `0,${height} ${points} ${width},${height}`
  const lastPoint = data.length > 0 ? {
    x: width,
    y: height - padding - ((data[data.length - 1] - min) / range) * (height - padding * 2),
  } : null

  return (
    <svg
      className={`sparkline ${onClick ? 'sparkline-clickable' : ''}`}
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      onClick={onClick}
    >
      <defs>
        <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.25" />
          <stop offset="100%" stopColor={color} stopOpacity="0.02" />
        </linearGradient>
      </defs>
      <polygon fill={`url(#${gradId})`} points={areaPoints} />
      <polyline fill="none" stroke={color} strokeWidth="1.5" strokeLinejoin="round" points={points} />
      {showDot && lastPoint && (
        <circle cx={lastPoint.x} cy={lastPoint.y} r="2.5" fill={color} className="spark-dot-pulse" />
      )}
    </svg>
  )
}

/* ── Animated Number ─────────────────────────────────────── */

function AnimNum({ value, format = fmtCompact }: { value: number; format?: (v: number) => string }) {
  const animated = useAnimatedNumber(value)
  return <>{format(Math.round(animated))}</>
}

/* ── Health Ring ──────────────────────────────────────────── */

function HealthRing({ health, size = 40 }: { health: 'ok' | 'error' | 'loading'; size?: number }) {
  const strokeW = 3
  const r = (size - strokeW) / 2
  const circ = 2 * Math.PI * r
  const color = health === 'ok' ? 'var(--text-safe)' : health === 'error' ? 'var(--text-unsafe)' : 'var(--text-warning)'
  const pct = health === 'ok' ? 1 : health === 'error' ? 0.15 : 0.5

  return (
    <div className={`health-ring ${health === 'ok' ? 'health-ring-ok' : health === 'error' ? 'health-ring-err' : 'health-ring-loading'}`}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="var(--border)" strokeWidth={strokeW} />
        <circle
          className="health-ring-arc"
          cx={size / 2} cy={size / 2} r={r}
          fill="none" stroke={color} strokeWidth={strokeW}
          strokeDasharray={circ}
          strokeDashoffset={circ * (1 - pct)}
          strokeLinecap="round"
          transform={`rotate(-90 ${size / 2} ${size / 2})`}
        />
      </svg>
      <div className="health-ring-center">
        <span className={`health-ring-dot ${health === 'ok' ? 'dot-ok' : health === 'error' ? 'dot-err' : 'dot-loading'}`} />
      </div>
    </div>
  )
}

/* ── Mini KPI card (for metrics row) ─────────────────────── */

function KPICard({ icon, label, value, sub, trend, sparkData, sparkColor, accentColor, onSparkClick, delay = 0 }: {
  icon: ReactNode
  label: string
  value: ReactNode
  sub?: ReactNode
  trend?: ReactNode
  sparkData?: number[]
  sparkColor?: string
  accentColor?: string
  onSparkClick?: () => void
  delay?: number
}) {
  return (
    <div className="kpi-card" style={{ '--kpi-accent': accentColor || 'var(--accent)', animationDelay: `${delay}ms` } as React.CSSProperties}>
      <div className="kpi-card-glow" />
      <div className="kpi-icon-wrap" style={{ color: accentColor }}>
        {icon}
      </div>
      <div className="kpi-content">
        <span className="kpi-label">{label}</span>
        <div className="kpi-value">{value}</div>
        {sub && <div className="kpi-sub">{sub}</div>}
        {trend && <div className="kpi-trend">{trend}</div>}
      </div>
      {sparkData && sparkData.length >= 2 && (
        <div className="kpi-spark">
          <Sparkline data={sparkData} color={sparkColor || 'var(--accent)'} width={80} height={28} onClick={onSparkClick} showDot />
        </div>
      )}
    </div>
  )
}

/* ── Throughput chart ─────────────────────────────────────── */

function ThroughputChart({ commitData, readData, onClickCommit, onClickRead }: {
  commitData: number[]
  readData: number[]
  onClickCommit?: () => void
  onClickRead?: () => void
}) {
  const [hover, setHover] = useState<{ x: number; commitY: number; readY: number; commitVal: number; readVal: number; idx: number } | null>(null)

  const hasData = commitData.length >= 2 || readData.length >= 2
  if (!hasData) return (
    <div className="throughput-empty">
      <IconActivity />
      <span>Collecting throughput data...</span>
    </div>
  )

  const len = Math.max(commitData.length, readData.length)
  const allValues = [...commitData, ...readData]
  const maxVal = Math.max(...allValues, 0.1)

  const W = 520
  const H = 100
  const padL = 42
  const padR = 12
  const padT = 8
  const padB = 18
  const chartW = W - padL - padR
  const chartH = H - padT - padB

  const toX = (i: number) => padL + (i / Math.max(1, len - 1)) * chartW
  const toY = (v: number) => padT + chartH - (v / maxVal) * chartH

  const makeLine = (data: number[]) =>
    data.map((v, i) => `${toX(i)},${toY(v)}`).join(' ')
  const makeArea = (data: number[]) =>
    `${toX(0)},${padT + chartH} ${makeLine(data)} ${toX(data.length - 1)},${padT + chartH}`

  const gridLines = [0.25, 0.5, 0.75, 1].map(f => f * maxVal)

  const currentCommit = commitData.length > 0 ? commitData[commitData.length - 1] : 0
  const currentRead = readData.length > 0 ? readData[readData.length - 1] : 0

  const timeLabels: { x: number; label: string }[] = []
  const labelCount = Math.min(6, len)
  for (let i = 0; i < labelCount; i++) {
    const idx = Math.round((i / (labelCount - 1)) * (len - 1))
    const secsAgo = (len - 1 - idx) * 5
    const label = secsAgo === 0 ? 'now' : secsAgo >= 60 ? `${Math.round(secsAgo / 60)}m` : `${secsAgo}s`
    timeLabels.push({ x: toX(idx), label })
  }

  const handleMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const svg = e.currentTarget
    const rect = svg.getBoundingClientRect()
    const mouseX = ((e.clientX - rect.left) / rect.width) * W
    const idx = Math.round(((mouseX - padL) / chartW) * (len - 1))
    if (idx < 0 || idx >= len) { setHover(null); return }
    const cVal = idx < commitData.length ? commitData[idx] : 0
    const rVal = idx < readData.length ? readData[idx] : 0
    setHover({ x: toX(idx), commitY: toY(cVal), readY: toY(rVal), commitVal: cVal, readVal: rVal, idx })
  }

  return (
    <div className="throughput-chart-wrap v2">
      <svg
        className="throughput-chart"
        viewBox={`0 0 ${W} ${H}`}
        preserveAspectRatio="xMidYMid meet"
        onMouseMove={handleMouseMove}
        onMouseLeave={() => setHover(null)}
      >
        <defs>
          <linearGradient id="thr2-commit-grad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--text-safe)" stopOpacity="0.35" />
            <stop offset="100%" stopColor="var(--text-safe)" stopOpacity="0.02" />
          </linearGradient>
          <linearGradient id="thr2-read-grad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.3" />
            <stop offset="100%" stopColor="var(--accent)" stopOpacity="0.02" />
          </linearGradient>
          <filter id="thr-glow-c" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
          <filter id="thr-glow-r" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
        </defs>

        {gridLines.map((v, i) => (
          <g key={i}>
            <line x1={padL} y1={toY(v)} x2={W - padR} y2={toY(v)} stroke="var(--border)" strokeDasharray="2 4" opacity="0.5" />
            <text x={padL - 8} y={toY(v) + 3} textAnchor="end" className="chart-axis-label">{fmtRate(v)}</text>
          </g>
        ))}

        {readData.length >= 2 && (
          <>
            <polygon fill="url(#thr2-read-grad)" points={makeArea(readData)} />
            <polyline fill="none" stroke="var(--accent)" strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" points={makeLine(readData)} />
          </>
        )}
        {commitData.length >= 2 && (
          <>
            <polygon fill="url(#thr2-commit-grad)" points={makeArea(commitData)} />
            <polyline fill="none" stroke="var(--text-safe)" strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" points={makeLine(commitData)} />
          </>
        )}

        {commitData.length >= 2 && (
          <g filter="url(#thr-glow-c)">
            <circle cx={toX(commitData.length - 1)} cy={toY(currentCommit)} r="3.5" fill="var(--text-safe)" opacity="0.9" />
          </g>
        )}
        {readData.length >= 2 && (
          <g filter="url(#thr-glow-r)">
            <circle cx={toX(readData.length - 1)} cy={toY(currentRead)} r="3.5" fill="var(--accent)" opacity="0.9" />
          </g>
        )}

        {hover && (
          <g>
            <line x1={hover.x} y1={padT} x2={hover.x} y2={padT + chartH} stroke="var(--text-muted)" strokeWidth="1" strokeDasharray="3 3" opacity="0.3" />
            <circle cx={hover.x} cy={hover.commitY} r="4" fill="var(--bg-elevated)" stroke="var(--text-safe)" strokeWidth="2" />
            <circle cx={hover.x} cy={hover.readY} r="4" fill="var(--bg-elevated)" stroke="var(--accent)" strokeWidth="2" />
            {/* Tooltip glass card */}
            <rect
              x={hover.x > W / 2 ? hover.x - 110 : hover.x + 14}
              y={Math.max(padT, Math.min(padT + chartH - 52, Math.min(hover.commitY, hover.readY) - 10))}
              width="96"
              height="48"
              rx="6"
              fill="var(--bg-code)"
              stroke="var(--border-strong)"
              strokeWidth="1"
              opacity="0.96"
            />
            <text
              x={hover.x > W / 2 ? hover.x - 62 : hover.x + 62}
              y={Math.max(padT, Math.min(padT + chartH - 52, Math.min(hover.commitY, hover.readY) - 10)) + 18}
              textAnchor="middle"
              className="chart-tooltip-label"
              fill="var(--text-safe)"
            >
              Commit {fmtRate(hover.commitVal)}/s
            </text>
            <text
              x={hover.x > W / 2 ? hover.x - 62 : hover.x + 62}
              y={Math.max(padT, Math.min(padT + chartH - 52, Math.min(hover.commitY, hover.readY) - 10)) + 36}
              textAnchor="middle"
              className="chart-tooltip-label"
              fill="var(--accent)"
            >
              Read {fmtRate(hover.readVal)}/s
            </text>
          </g>
        )}

        {timeLabels.map((lbl, i) => (
          <text key={i} x={lbl.x} y={H - 3} textAnchor="middle" className="chart-axis-label">{lbl.label}</text>
        ))}
      </svg>

      <div className="throughput-legend v2">
        <span className="throughput-legend-item v2" onClick={onClickCommit} style={{ cursor: onClickCommit ? 'pointer' : undefined }}>
          <span className="legend-dot-glow" style={{ '--dot-color': 'var(--text-safe)' } as React.CSSProperties} />
          <span className="legend-label">Commits/sec</span>
          <strong>{hover ? fmtRate(hover.commitVal) : fmtRate(currentCommit)}</strong>
        </span>
        <span className="throughput-legend-item v2" onClick={onClickRead} style={{ cursor: onClickRead ? 'pointer' : undefined }}>
          <span className="legend-dot-glow" style={{ '--dot-color': 'var(--accent)' } as React.CSSProperties} />
          <span className="legend-label">Reads/sec</span>
          <strong>{hover ? fmtRate(hover.readVal) : fmtRate(currentRead)}</strong>
        </span>
      </div>
    </div>
  )
}

/* ── PID Controller Card ─────────────────────────────────── */

function PIDCard({ pid, history, delay = 0 }: { pid: PIDControllerState; history: PIDControllerState[]; delay?: number }) {
  const isDelay = pid.name.includes('delay')
  const outMax = isDelay ? 2000 : 2000
  const outMin = isDelay ? 0 : 200
  const fillPct = Math.min(100, Math.max(0, ((pid.output - outMin) / (outMax - outMin)) * 100))
  const absError = Math.abs(pid.error)
  const gaugeColor = absError < 1 ? 'var(--text-safe)' : absError < 3 ? 'var(--accent)' : 'var(--text-warning)'

  const outputHistory = history.map(h => h.output)
  const animatedOutput = useAnimatedNumber(pid.output)

  return (
    <div className="pid-card v2" style={{ animationDelay: `${delay}ms` }}>
      <div className="pid-card-border-glow" />
      <div className="pid-header">
        <div className="pid-header-left">
          <span className="pid-status-dot dot-ok" />
          <span className="pid-status-label">Active</span>
          <span className="pid-name">{pidDisplayName(pid.name)}</span>
        </div>
        <span className="pid-updates">{pid.total_updates.toLocaleString()} updates</span>
      </div>

      <div className="pid-metrics v2">
        <div className="pid-metric v2">
          <span className="pid-metric-label">Setpoint</span>
          <span className="pid-metric-value">{pid.setpoint.toFixed(2)} ms</span>
        </div>
        <div className="pid-metric v2">
          <span className="pid-metric-label">Measured</span>
          <span className="pid-metric-value">{pid.measured.toFixed(2)} ms</span>
        </div>
        <div className="pid-metric v2">
          <span className="pid-metric-label">Error</span>
          <span className={`pid-metric-value ${pid.error > 0 ? 'pid-error-pos' : 'pid-error-neg'}`}>
            {pid.error > 0 ? '+' : ''}{pid.error.toFixed(2)}
          </span>
        </div>
        <div className="pid-metric v2">
          <span className="pid-metric-label">Output</span>
          <span className="pid-metric-value pid-output">{fmtPIDOutput(pid.name, animatedOutput)}</span>
        </div>
      </div>

      <div className="pid-gauge-section v2">
        <div className="pid-gauge-row">
          <div className="pid-gauge">
            <div className="pid-gauge-track v2">
              <div className="pid-gauge-fill v2" style={{ width: `${fillPct}%`, background: `linear-gradient(90deg, ${gaugeColor}66, ${gaugeColor})` }} />
              <div className="pid-gauge-thumb v2" style={{ left: `${fillPct}%`, borderColor: gaugeColor, boxShadow: `0 0 8px ${gaugeColor}55` }} />
            </div>
            <div className="pid-gauge-labels">
              <span>{fmtPIDOutput(pid.name, outMin)}</span>
              <span>{fmtPIDOutput(pid.name, outMax)}</span>
            </div>
          </div>
          {outputHistory.length >= 2 && (
            <div className="pid-sparkline-wrap">
              <Sparkline data={outputHistory} color={gaugeColor} width={100} height={28} showDot />
            </div>
          )}
        </div>
      </div>

      <div className="pid-footer v2">
        <span className="pid-integral">Integral: <strong>{pid.integral.toFixed(4)}</strong></span>
      </div>
    </div>
  )
}

/* ── Tooltip ─────────────────────────────────────────────── */

function Tooltip({ children, text }: { children: ReactNode; text: string }) {
  const [show, setShow] = useState(false)
  return (
    <div className="tooltip-wrap" onMouseEnter={() => setShow(true)} onMouseLeave={() => setShow(false)}>
      {children}
      {show && <div className="tooltip-bubble">{text}</div>}
    </div>
  )
}

/* ── Insight Card (v2 - glass morphism) ──────────────────── */

function InsightCard({ title, value, trend, trendValue, color, description, icon, sparkData, sparkColor, delay = 0 }: {
  title: string
  value: string | number
  trend?: 'up' | 'down' | 'neutral'
  trendValue?: string
  color?: string
  description?: string
  icon?: ReactNode
  sparkData?: number[]
  sparkColor?: string
  delay?: number
}) {
  const Icon = trend === 'up' ? IconTrendingUp : trend === 'down' ? IconTrendingDown : null
  return (
    <div className="insight-card v2" style={{ '--insight-accent': color || 'var(--accent)', animationDelay: `${delay}ms` } as React.CSSProperties}>
      <div className="insight-card-glow" />
      <div className="insight-header v2">
        {icon && <div className="insight-icon" style={{ color: color }}>{icon}</div>}
        <div className="insight-header-text">
          <span className="insight-title v2">{title}</span>
          {description && (
            <Tooltip text={description}>
              <IconInfo className="insight-info-icon" />
            </Tooltip>
          )}
        </div>
      </div>
      <div className="insight-body">
        <div className="insight-value v2">{value}</div>
        {sparkData && sparkData.length >= 2 && (
          <Sparkline data={sparkData} color={sparkColor || color || 'var(--accent)'} width={64} height={24} showDot />
        )}
      </div>
      {trend && trendValue && (
        <div className={`insight-trend v2 trend-${trend}`}>
          {Icon && <Icon />}
          <span>{trendValue}</span>
        </div>
      )}
    </div>
  )
}

/* ── Alert Banner ────────────────────────────────────────── */

type Alert = {
  level: 'warning' | 'error' | 'info'
  message: string
}

function AlertBanner({ alerts }: { alerts: Alert[] }) {
  if (alerts.length === 0) return null
  return (
    <div className="alert-banner v2">
      {alerts.map((alert, i) => (
        <div key={i} className={`alert-item v2 alert-${alert.level}`} style={{ animationDelay: `${i * 100}ms` }}>
          <div className="alert-icon-wrap">
            <IconAlertTriangle />
          </div>
          <span className="alert-msg">{alert.message}</span>
        </div>
      ))}
    </div>
  )
}

/* ── Latency Heatmap Cell ─────────────────────────────────── */

function LatHeatCell({ ms, label }: { ms: number; label: string }) {
  return (
    <Tooltip text={`${label}: ${fmtLatency(ms)}`}>
      <div className={`lat-heat-cell ${latencyBgClass(ms)}`}>
        <span className="lat-heat-val">{fmtLatency(ms)}</span>
      </div>
    </Tooltip>
  )
}

/* ── Animated counter for uptime ─────────────────────────── */

function useUptime() {
  const [elapsed, setElapsed] = useState(0)
  const startRef = useRef<number>(0)
  useEffect(() => {
    startRef.current = Date.now()
    const id = setInterval(() => setElapsed(Date.now() - startRef.current), 1000)
    return () => clearInterval(id)
  }, [])
  return elapsed
}

/* ── Section Wrapper ─────────────────────────────────────── */

function GlassSection({ title, children, className = '', delay = 0 }: { title?: string; children: ReactNode; className?: string; delay?: number }) {
  return (
    <div className={`glass-section ${className}`} style={{ animationDelay: `${delay}ms` }}>
      {title && <div className="glass-section-header"><span className="glass-section-title">{title}</span></div>}
      <div className="glass-section-body">{children}</div>
    </div>
  )
}

/* ── Main Dashboard ───────────────────────────────────────── */

export function Dashboard() {
  const {
    health,
    scanStats,
    replication,
    routingStats,
    engineStats,
    prevEngineStats,
    statsHistory,
    autoRefresh,
    setAutoRefresh,
    refresh,
  } = useDashboard()

  const [chartModal, setChartModal] = useState<ChartInfo | null>(null)
  const uptime = useUptime()

  const sys = engineStats.system

  const commitHistory = statsHistory.map(s => s.commit_throughput_per_sec)
  const readHistory = statsHistory.map(s => s.read_throughput_per_sec)
  const commitP95History = statsHistory.map(s => s.commit_latency_p95_ms)
  const readP95History = statsHistory.map(s => s.read_latency_p95_ms)
  const walSizeHistory = statsHistory.map(s => s.wal_file_size_bytes)
  const snapSizeHistory = statsHistory.map(s => s.snapshot_file_size_bytes)
  const auditSizeHistory = statsHistory.map(s => s.audit_file_size_bytes)

  const pidControllers = engineStats.pid_controllers || []
  const pidHistory = (name: string): PIDControllerState[] =>
    statsHistory
      .map(s => (s.pid_controllers || []).find(p => p.name === name))
      .filter((p): p is PIDControllerState => p !== undefined)

  const rollbackRatio = engineStats.total_commits > 0 ? engineStats.total_rollbacks / engineStats.total_commits : 0
  const rollbackBad = rollbackRatio >= THRESH.ROLLBACK_RATIO_BAD
  const rollbackWarn = rollbackRatio >= THRESH.ROLLBACK_RATIO_WARN

  const totalStorage = engineStats.wal_file_size_bytes + engineStats.snapshot_file_size_bytes + engineStats.audit_file_size_bytes
  const walPct = totalStorage > 0 ? (engineStats.wal_file_size_bytes / totalStorage) * 100 : 33
  const snapPct = totalStorage > 0 ? (engineStats.snapshot_file_size_bytes / totalStorage) * 100 : 33
  const auditPct = totalStorage > 0 ? (engineStats.audit_file_size_bytes / totalStorage) * 100 : 33

  const totalScans = Object.values(scanStats).reduce((a, b) => a + b, 0)
  const scanEntries = Object.entries(scanStats).sort(([, a], [, b]) => b - a)
  const totalRoutes = Object.values(routingStats).reduce((a, b) => a + b, 0)
  const routeEntries = Object.entries(routingStats).sort(([, a], [, b]) => b - a)

  const commitDelta = engineStats.total_commits - prevEngineStats.total_commits
  const readDelta = engineStats.total_reads - prevEngineStats.total_reads

  // Calculate trends
  const commitTrend = statsHistory.length >= 2
    ? commitHistory[commitHistory.length - 1] - commitHistory[commitHistory.length - 2]
    : 0
  const readTrend = statsHistory.length >= 2
    ? readHistory[readHistory.length - 1] - readHistory[readHistory.length - 2]
    : 0

  // Calculate averages
  const avgCommitThroughput = commitHistory.length > 0
    ? commitHistory.reduce((a, b) => a + b, 0) / commitHistory.length
    : 0
  const avgReadThroughput = readHistory.length > 0
    ? readHistory.reduce((a, b) => a + b, 0) / readHistory.length
    : 0
  const avgCommitLatency = commitP95History.length > 0
    ? commitP95History.reduce((a, b) => a + b, 0) / commitP95History.length
    : 0

  // Generate alerts
  const alerts: Alert[] = []
  if (health === 'error') {
    alerts.push({ level: 'error', message: 'Engine unreachable - connection lost' })
  }
  if (rollbackBad) {
    alerts.push({ level: 'error', message: `High rollback ratio (${(rollbackRatio * 100).toFixed(1)}%) - investigate transaction conflicts` })
  } else if (rollbackWarn) {
    alerts.push({ level: 'warning', message: `Elevated rollback ratio (${(rollbackRatio * 100).toFixed(1)}%) - monitor transaction patterns` })
  }
  if (engineStats.commit_latency_p99_ms > 50) {
    alerts.push({ level: 'warning', message: `High P99 commit latency (${fmtLatency(engineStats.commit_latency_p99_ms)}) - check system load` })
  }
  if (replication.available && replication.lag > 100) {
    alerts.push({ level: 'warning', message: `High replication lag (${replication.lag} ops) - follower may be struggling` })
  }

  const openChart = (data: number[], title: string, color: string, formatValue: (v: number) => string) => {
    if (data.length >= 2) setChartModal({ data, title, color, formatValue })
  }

  const healthText = health === 'ok' ? 'Connected' : health === 'error' ? 'Unreachable' : 'Checking...'

  // Prepare donut chart data
  const scanDonutData: DonutSegment[] = scanEntries.map(([label, value], i) => ({
    label,
    value,
    color: PALETTE[i % PALETTE.length]
  }))

  const routeDonutData: DonutSegment[] = routeEntries.map(([label, value], i) => ({
    label,
    value,
    color: PALETTE[(i + 3) % PALETTE.length]
  }))

  return (
    <div className="dashboard modern-dashboard v2">
      {/* ── Animated background ───────────────────────────── */}
      <div className="dash-bg-mesh" />
      <div className="dash-bg-glow glow-1" />
      <div className="dash-bg-glow glow-2" />
      <div className="dash-bg-glow glow-3" />

      {/* ── Alert Banner ───────────────────────────────────── */}
      <AlertBanner alerts={alerts} />

      {/* ── Hero Header ────────────────────────────────────── */}
      <div className="dash-hero">
        <div className="dash-hero-left">
          <HealthRing health={health} size={44} />
          <div className="dash-hero-info">
            <div className="dash-hero-title">
              <span className="dash-hero-brand">ASQL</span>
              <span className="dash-hero-subtitle">Engine Dashboard</span>
            </div>
            <div className="dash-hero-meta">
              <span className={`dash-hero-status ${health}`}>{healthText}</span>
              <span className="dash-hero-sep">/</span>
              <span className="dash-hero-uptime">Uptime {fmtUptime(sys?.uptime_ms ?? uptime)}</span>
            </div>
          </div>
        </div>
        <div className="dash-hero-right">
          <div className="dash-hero-stat">
            <span className="dash-hero-stat-label">LSN</span>
            <span className="dash-hero-stat-value"><AnimNum value={replication.leaderLSN} format={v => v.toLocaleString()} /></span>
          </div>
          <div className="dash-hero-divider" />
          <div className="dash-hero-stat">
            <span className="dash-hero-stat-label">Lag</span>
            {replication.available ? (
              <span className={`dash-hero-stat-value ${replication.lag > 100 ? 'latency-bad' : replication.lag > 10 ? 'latency-warn' : 'latency-good'}`}>
                {replication.lag}
              </span>
            ) : (
              <span className="dash-hero-stat-value text-muted">&mdash;</span>
            )}
          </div>
          <div className="dash-hero-divider" />
          <div className="dash-hero-stat">
            <span className="dash-hero-stat-label">Storage</span>
            <span className="dash-hero-stat-value">{fmtBytes(totalStorage)}</span>
          </div>
          <div className="dash-hero-controls">
            <label className={`auto-refresh-toggle ${autoRefresh ? 'on' : ''}`}>
              <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
              <span className="auto-refresh-track">
                <span className="auto-refresh-thumb" />
              </span>
              <span className="auto-refresh-label">Live</span>
            </label>
            <button className="dash-refresh-btn" onClick={refresh} title="Refresh now">
              <IconRefresh />
            </button>
          </div>
        </div>
      </div>

      {/* ── KPI Cards (Insight Row) ────────────────────────── */}
      <div className="insights-grid v2">
        <InsightCard
          title="Commit Rate"
          value={`${fmtRate(avgCommitThroughput)}/s`}
          trend={commitTrend > 0 ? 'up' : commitTrend < 0 ? 'down' : 'neutral'}
          trendValue={commitTrend !== 0 ? `${commitTrend > 0 ? '+' : ''}${fmtRate(Math.abs(commitTrend))}/s` : undefined}
          color="var(--text-safe)"
          description="Average commit throughput over the last 5 minutes"
          icon={<IconZap />}
          sparkData={commitHistory}
          sparkColor="var(--text-safe)"
          delay={0}
        />
        <InsightCard
          title="Read Rate"
          value={`${fmtRate(avgReadThroughput)}/s`}
          trend={readTrend > 0 ? 'up' : readTrend < 0 ? 'down' : 'neutral'}
          trendValue={readTrend !== 0 ? `${readTrend > 0 ? '+' : ''}${fmtRate(Math.abs(readTrend))}/s` : undefined}
          color="var(--accent)"
          description="Average read throughput over the last 5 minutes"
          icon={<IconActivity />}
          sparkData={readHistory}
          sparkColor="var(--accent)"
          delay={60}
        />
        <InsightCard
          title="Commit P95"
          value={fmtLatency(avgCommitLatency)}
          color="var(--text-warning)"
          description="Average P95 commit latency"
          icon={<IconDatabase />}
          sparkData={commitP95History}
          sparkColor="var(--text-warning)"
          delay={120}
        />
        <InsightCard
          title="Success Rate"
          value={`${((1 - rollbackRatio) * 100).toFixed(1)}%`}
          trend={rollbackRatio < 0.05 ? 'up' : rollbackRatio > 0.15 ? 'down' : 'neutral'}
          trendValue={rollbackRatio > 0 ? `${rollbackRatio > 0.15 ? 'Critical' : 'Normal'}` : undefined}
          color={rollbackBad ? 'var(--text-unsafe)' : rollbackWarn ? 'var(--text-warning)' : 'var(--text-safe)'}
          description="Transaction success rate (1 - rollback ratio)"
          icon={<IconShield />}
          delay={180}
        />
      </div>

      {/* ── Live Metrics Row ───────────────────────────────── */}
      <div className="kpi-row">
        <KPICard
          icon={<IconZap />}
          label="Total Commits"
          value={<AnimNum value={engineStats.total_commits} />}
          sub={<>{fmtRate(engineStats.commit_throughput_per_sec)}/s</>}
          trend={commitDelta > 0 ? <span className="delta-badge">+{fmtCompact(commitDelta)}</span> : undefined}
          sparkData={commitHistory}
          sparkColor="var(--text-safe)"
          accentColor="var(--text-safe)"
          onSparkClick={() => openChart(commitHistory, 'Commits/sec', 'var(--text-safe)', fmtRate)}
          delay={0}
        />
        <KPICard
          icon={<IconActivity />}
          label="Total Reads"
          value={<AnimNum value={engineStats.total_reads} />}
          sub={<>{fmtRate(engineStats.read_throughput_per_sec)}/s</>}
          trend={readDelta > 0 ? <span className="delta-badge">+{fmtCompact(readDelta)}</span> : undefined}
          sparkData={readHistory}
          sparkColor="var(--accent)"
          accentColor="var(--accent)"
          onSparkClick={() => openChart(readHistory, 'Reads/sec', 'var(--accent)', fmtRate)}
          delay={60}
        />
        <KPICard
          icon={<IconDatabase />}
          label="Active Tx"
          value={<AnimNum value={engineStats.active_transactions} />}
          sub={<>{engineStats.total_begins.toLocaleString()} begins</>}
          accentColor="var(--text-warning)"
          delay={120}
        />
        <KPICard
          icon={<IconShield />}
          label="Rollbacks"
          value={<AnimNum value={engineStats.total_rollbacks} />}
          sub={
            rollbackRatio > 0 ? (
              <span className={`rollback-ratio-badge ${rollbackBad ? 'bad' : rollbackWarn ? 'warn' : ''}`}>
                {(rollbackRatio * 100).toFixed(1)}%
              </span>
            ) : undefined
          }
          trend={<span className="text-muted">{engineStats.total_time_travel_queries.toLocaleString()} tt queries</span>}
          accentColor="var(--text-unsafe)"
          delay={180}
        />
      </div>

      {/* ── Throughput + Latency ──────────────────────────── */}
      <div className="dash-analytics-row">
        <GlassSection title="Throughput" className="dash-throughput-section v2" delay={200}>
          <ThroughputChart
            commitData={commitHistory}
            readData={readHistory}
            onClickCommit={() => openChart(commitHistory, 'Commits/sec', 'var(--text-safe)', fmtRate)}
            onClickRead={() => openChart(readHistory, 'Reads/sec', 'var(--accent)', fmtRate)}
          />
        </GlassSection>

        <GlassSection title="Latency" className="dash-latency-section v2" delay={260}>
          <div className="lat-heatmap">
            <div className="lat-heatmap-row lat-heatmap-header">
              <span className="lat-heatmap-label" />
              <span className="lat-heatmap-col-header">P50</span>
              <span className="lat-heatmap-col-header">P95</span>
              <span className="lat-heatmap-col-header">P99</span>
              <span className="lat-heatmap-col-header">Trend</span>
            </div>
            <div className="lat-heatmap-row">
              <span className="lat-heatmap-label">Commit</span>
              <LatHeatCell ms={engineStats.commit_latency_p50_ms} label="Commit P50" />
              <LatHeatCell ms={engineStats.commit_latency_p95_ms} label="Commit P95" />
              <LatHeatCell ms={engineStats.commit_latency_p99_ms} label="Commit P99" />
              <Sparkline data={commitP95History} color="var(--text-safe)" width={80} height={24} showDot
                onClick={() => openChart(commitP95History, 'Commit P95', 'var(--text-safe)', fmtLatency)} />
            </div>
            <div className="lat-heatmap-row">
              <span className="lat-heatmap-label">Read</span>
              <LatHeatCell ms={engineStats.read_latency_p50_ms} label="Read P50" />
              <LatHeatCell ms={engineStats.read_latency_p95_ms} label="Read P95" />
              <LatHeatCell ms={engineStats.read_latency_p99_ms} label="Read P99" />
              <Sparkline data={readP95History} color="var(--accent)" width={80} height={24} showDot
                onClick={() => openChart(readP95History, 'Read P95', 'var(--accent)', fmtLatency)} />
            </div>
            <div className="lat-heatmap-row">
              <span className="lat-heatmap-label">Time Travel</span>
              <LatHeatCell ms={engineStats.time_travel_latency_p50_ms} label="TT P50" />
              <LatHeatCell ms={engineStats.time_travel_latency_p95_ms} label="TT P95" />
              <LatHeatCell ms={engineStats.time_travel_latency_p99_ms} label="TT P99" />
              <div />
            </div>
          </div>
        </GlassSection>
      </div>

      {/* ── Storage & Distributions ────────────────────────── */}
      <div className="dash-bottom-row v2">
        <GlassSection title="Storage Overview" delay={320}>
          <div className="storage-gauges v2">
            <RadialGauge
              value={engineStats.wal_file_size_bytes}
              max={Math.max(engineStats.wal_file_size_bytes, engineStats.snapshot_file_size_bytes, engineStats.audit_file_size_bytes, 1024 * 1024)}
              label="WAL"
              color="var(--text-safe)"
              format={fmtBytes}
              size={110}
            />
            <RadialGauge
              value={engineStats.snapshot_file_size_bytes}
              max={Math.max(engineStats.wal_file_size_bytes, engineStats.snapshot_file_size_bytes, engineStats.audit_file_size_bytes, 1024 * 1024)}
              label="Snapshot"
              color="var(--accent)"
              format={fmtBytes}
              size={110}
            />
            <RadialGauge
              value={engineStats.audit_file_size_bytes}
              max={Math.max(engineStats.wal_file_size_bytes, engineStats.snapshot_file_size_bytes, engineStats.audit_file_size_bytes, 1024 * 1024)}
              label="Audit"
              color="var(--text-warning)"
              format={fmtBytes}
              size={110}
            />
          </div>
          <div className="storage-bars v2">
            <div className="storage-bar-row">
              <span className="storage-bar-label">WAL</span>
              <div className="storage-bar-track">
                <div className="storage-bar-fill wal" style={{ width: `${walPct}%` }}>
                  <span className="storage-bar-shine" />
                </div>
              </div>
              <span className="storage-bar-value">{fmtBytes(engineStats.wal_file_size_bytes)}</span>
            </div>
            <div className="storage-bar-row">
              <span className="storage-bar-label">Snap</span>
              <div className="storage-bar-track">
                <div className="storage-bar-fill snap" style={{ width: `${snapPct}%` }}>
                  <span className="storage-bar-shine" />
                </div>
              </div>
              <span className="storage-bar-value">{fmtBytes(engineStats.snapshot_file_size_bytes)}</span>
            </div>
            <div className="storage-bar-row">
              <span className="storage-bar-label">Audit</span>
              <div className="storage-bar-track">
                <div className="storage-bar-fill audit" style={{ width: `${auditPct}%` }}>
                  <span className="storage-bar-shine" />
                </div>
              </div>
              <span className="storage-bar-value">{fmtBytes(engineStats.audit_file_size_bytes)}</span>
            </div>
          </div>
          {(walSizeHistory.length >= 2 || snapSizeHistory.length >= 2 || auditSizeHistory.length >= 2) && (
            <div className="storage-trends">
              {walSizeHistory.length >= 2 && (
                <div className="storage-trend-row">
                  <span className="storage-trend-label">WAL trend</span>
                  <Sparkline data={walSizeHistory} color="var(--text-safe)" width={160} height={24} showDot
                    onClick={() => openChart(walSizeHistory, 'WAL Size', 'var(--text-safe)', fmtBytes)} />
                </div>
              )}
              {snapSizeHistory.length >= 2 && (
                <div className="storage-trend-row">
                  <span className="storage-trend-label">Snap trend</span>
                  <Sparkline data={snapSizeHistory} color="var(--accent)" width={160} height={24} showDot
                    onClick={() => openChart(snapSizeHistory, 'Snapshot Size', 'var(--accent)', fmtBytes)} />
                </div>
              )}
              {auditSizeHistory.length >= 2 && (
                <div className="storage-trend-row">
                  <span className="storage-trend-label">Audit trend</span>
                  <Sparkline data={auditSizeHistory} color="var(--text-warning)" width={160} height={24} showDot
                    onClick={() => openChart(auditSizeHistory, 'Audit Size', 'var(--text-warning)', fmtBytes)} />
                </div>
              )}
            </div>
          )}
        </GlassSection>

        <GlassSection title="Distributions" delay={380}>
          <div className="distributions-container v2">
            {totalScans > 0 && (
              <div className="dash-dist-group v2">
                <span className="dash-dist-title v2">Scan Strategy</span>
                <DonutChart segments={scanDonutData} size={140} innerLabel="scans" />
              </div>
            )}
            {totalRoutes > 0 && (
              <div className="dash-dist-group v2">
                <span className="dash-dist-title v2">Read Routing</span>
                <DonutChart segments={routeDonutData} size={140} innerLabel="routes" />
              </div>
            )}
            {totalScans === 0 && totalRoutes === 0 && (
              <div className="dist-empty">
                <IconActivity />
                <span>Collecting distribution data...</span>
              </div>
            )}
          </div>
        </GlassSection>
      </div>

      {/* ── PID Controllers ────────────────────────────────── */}
      {pidControllers.length > 0 && (
        <GlassSection title="Adaptive Controllers (PID)" delay={440}>
          <div className="pid-grid v2">
            {pidControllers.map((pid, i) => (
              <PIDCard key={pid.name} pid={pid} history={pidHistory(pid.name)} delay={i * 80} />
            ))}
          </div>
        </GlassSection>
      )}

      {/* ── System Health ──────────────────────────────────── */}
      {sys && (
        <GlassSection title="System Health" className="dash-system-section" delay={500}>
          <div className="system-health-grid">
            {/* Host info */}
            <div className="sys-card">
              <div className="sys-card-header"><IconServer /><span>Host</span></div>
              <div className="sys-card-body">
                <div className="sys-row"><span className="sys-label">Hostname</span><span className="sys-value">{sys.hostname}</span></div>
                <div className="sys-row"><span className="sys-label">OS / Arch</span><span className="sys-value">{sys.os}/{sys.arch}</span></div>
                <div className="sys-row"><span className="sys-label">CPUs</span><span className="sys-value">{sys.num_cpu}</span></div>
                <div className="sys-row"><span className="sys-label">PID</span><span className="sys-value mono">{sys.pid}</span></div>
                <div className="sys-row"><span className="sys-label">Go</span><span className="sys-value mono">{sys.go_version}</span></div>
                <div className="sys-row"><span className="sys-label">Uptime</span><span className="sys-value">{fmtUptime(sys.uptime_ms)}</span></div>
              </div>
            </div>
            {/* Memory */}
            <div className="sys-card">
              <div className="sys-card-header"><IconCpu /><span>Memory</span></div>
              <div className="sys-card-body">
                <div className="sys-row"><span className="sys-label">Heap Alloc</span><span className="sys-value">{fmtBytes(sys.heap_alloc_bytes)}</span></div>
                <div className="sys-row"><span className="sys-label">Heap In-use</span><span className="sys-value">{fmtBytes(sys.heap_inuse_bytes)}</span></div>
                <div className="sys-row"><span className="sys-label">Heap Sys</span><span className="sys-value">{fmtBytes(sys.heap_sys_bytes)}</span></div>
                <div className="sys-row"><span className="sys-label">Stack</span><span className="sys-value">{fmtBytes(sys.stack_inuse_bytes)}</span></div>
                <div className="sys-row"><span className="sys-label">Total Sys</span><span className="sys-value">{fmtBytes(sys.sys_bytes)}</span></div>
                <div className="sys-row"><span className="sys-label">Heap Objects</span><span className="sys-value">{sys.heap_objects.toLocaleString()}</span></div>
                <div className="sys-row"><span className="sys-label">Total Alloc</span><span className="sys-value">{fmtBytes(sys.total_alloc_bytes)}</span></div>
              </div>
            </div>
            {/* Runtime */}
            <div className="sys-card">
              <div className="sys-card-header"><IconActivity /><span>Runtime</span></div>
              <div className="sys-card-body">
                <div className="sys-row"><span className="sys-label">Goroutines</span><span className="sys-value">{sys.num_goroutine}</span></div>
                <div className="sys-row"><span className="sys-label">GC Cycles</span><span className="sys-value">{sys.gc_cycles.toLocaleString()}</span></div>
                <div className="sys-row"><span className="sys-label">Last GC Pause</span><span className="sys-value">{fmtNanos(sys.last_gc_pause_ns)}</span></div>
                <div className="sys-row"><span className="sys-label">Total GC Pause</span><span className="sys-value">{fmtNanos(sys.gc_pause_total_ns)}</span></div>
                <div className="sys-row"><span className="sys-label">GC CPU</span><span className="sys-value">{fmtPct(sys.gc_cpu_fraction)}</span></div>
              </div>
            </div>
          </div>
        </GlassSection>
      )}

      {/* Chart Modal */}
      {chartModal && (
        <ChartModal
          data={chartModal.data}
          title={chartModal.title}
          color={chartModal.color}
          formatValue={chartModal.formatValue}
          onClose={() => setChartModal(null)}
        />
      )}
    </div>
  )
}
