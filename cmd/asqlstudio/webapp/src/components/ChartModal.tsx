import { useCallback, useRef, useState } from 'react'
import { IconX } from './Icons'

type ChartModalProps = {
  data: number[]
  title: string
  color: string
  formatValue: (v: number) => string
  onClose: () => void
}

export function ChartModal({ data, title, color, formatValue, onClose }: ChartModalProps) {
  const [hover, setHover] = useState<{ x: number; y: number; value: number; idx: number } | null>(null)
  const svgRef = useRef<SVGSVGElement>(null)

  if (data.length < 2) return null

  const W = 520
  const H = 200
  const padL = 60
  const padR = 16
  const padT = 20
  const padB = 32
  const chartW = W - padL - padR
  const chartH = H - padT - padB

  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1

  const toX = (i: number) => padL + (i / (data.length - 1)) * chartW
  const toY = (v: number) => padT + chartH - ((v - min) / range) * chartH

  const linePoints = data.map((v, i) => `${toX(i)},${toY(v)}`).join(' ')
  const areaPoints = `${padL},${padT + chartH} ${linePoints} ${toX(data.length - 1)},${padT + chartH}`

  const gridValues = [min, min + range * 0.25, min + range * 0.5, min + range * 0.75, max]

  // Time labels
  const totalSec = (data.length - 1) * 5
  const xLabels: { x: number; label: string }[] = []
  const labelCount = Math.min(6, data.length)
  for (let i = 0; i < labelCount; i++) {
    const idx = Math.round((i / (labelCount - 1)) * (data.length - 1))
    const secsAgo = (data.length - 1 - idx) * 5
    const label = secsAgo === 0 ? 'now' : secsAgo >= 60 ? `${Math.round(secsAgo / 60)}m ago` : `${secsAgo}s`
    xLabels.push({ x: toX(idx), label })
  }

  const currentValue = data[data.length - 1]
  const avg = data.reduce((a, b) => a + b, 0) / data.length
  const gradientId = `chart-grad-${title.replace(/\s+/g, '')}`

  const handleMouseMove = useCallback((e: React.MouseEvent<SVGSVGElement>) => {
    const svg = svgRef.current
    if (!svg) return
    const rect = svg.getBoundingClientRect()
    const mouseX = ((e.clientX - rect.left) / rect.width) * W
    // Find closest data point
    const idx = Math.round(((mouseX - padL) / chartW) * (data.length - 1))
    if (idx < 0 || idx >= data.length) { setHover(null); return }
    setHover({ x: toX(idx), y: toY(data[idx]), value: data[idx], idx })
  }, [data, chartW])

  return (
    <div className="chart-modal-overlay" onClick={(e) => { if (e.target === e.currentTarget) onClose() }}>
      <div className="chart-modal">
        <div className="chart-modal-header">
          <span className="chart-modal-title">{title}</span>
          <span className="chart-modal-current" style={{ color }}>{formatValue(currentValue)}</span>
          <button className="icon-btn" onClick={onClose}><IconX /></button>
        </div>
        <svg
          ref={svgRef}
          width={W}
          height={H}
          viewBox={`0 0 ${W} ${H}`}
          className="chart-modal-svg"
          onMouseMove={handleMouseMove}
          onMouseLeave={() => setHover(null)}
        >
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity="0.3" />
              <stop offset="100%" stopColor={color} stopOpacity="0.02" />
            </linearGradient>
          </defs>

          {/* Grid lines */}
          {gridValues.map((v, i) => {
            const y = toY(v)
            return (
              <g key={i}>
                <line x1={padL} y1={y} x2={W - padR} y2={y} stroke="var(--border)" strokeDasharray="3 3" />
                <text x={padL - 8} y={y + 4} textAnchor="end" className="chart-axis-label">{formatValue(v)}</text>
              </g>
            )
          })}

          {/* Gradient area */}
          <polygon fill={`url(#${gradientId})`} points={areaPoints} />

          {/* Line with animated drawing */}
          <polyline
            className="chart-line-animated"
            fill="none"
            stroke={color}
            strokeWidth="2"
            strokeLinejoin="round"
            strokeLinecap="round"
            points={linePoints}
          />

          {/* Current value dot - glow */}
          <circle cx={toX(data.length - 1)} cy={toY(currentValue)} r="8" fill={color} opacity="0.2" />
          <circle cx={toX(data.length - 1)} cy={toY(currentValue)} r="4" fill={color} />

          {/* Hover crosshair */}
          {hover && (
            <g className="chart-crosshair">
              <line x1={hover.x} y1={padT} x2={hover.x} y2={padT + chartH} stroke={color} strokeWidth="1" strokeDasharray="4 4" opacity="0.5" />
              <circle cx={hover.x} cy={hover.y} r="5" fill="var(--bg-elevated)" stroke={color} strokeWidth="2" />
              {/* Value label */}
              <rect
                x={hover.x - 35}
                y={hover.y - 26}
                width={70}
                height={20}
                rx={4}
                fill="var(--bg-elevated)"
                stroke="var(--border-strong)"
                strokeWidth={1}
              />
              <text
                x={hover.x}
                y={hover.y - 13}
                textAnchor="middle"
                className="chart-hover-value"
                fill="var(--text-primary)"
              >
                {formatValue(hover.value)}
              </text>
            </g>
          )}

          {/* X-axis labels */}
          {xLabels.map((lbl, i) => (
            <text key={i} x={lbl.x} y={H - 6} textAnchor="middle" className="chart-axis-label">{lbl.label}</text>
          ))}
        </svg>
        <div className="chart-modal-stats">
          <span>Min: <strong>{formatValue(min)}</strong></span>
          <span>Avg: <strong>{formatValue(avg)}</strong></span>
          <span>Max: <strong>{formatValue(max)}</strong></span>
          <span>Samples: <strong>{data.length}</strong></span>
          <span>Window: <strong>{totalSec >= 60 ? `${(totalSec / 60).toFixed(0)}m` : `${totalSec}s`}</strong></span>
        </div>
      </div>
    </div>
  )
}
