import { useState } from 'react'

export type DonutSegment = {
  label: string
  value: number
  color: string
}

type DonutChartProps = {
  segments: DonutSegment[]
  size?: number
  strokeWidth?: number
  innerLabel?: string
}

export function DonutChart({ segments, size = 140, strokeWidth = 20, innerLabel }: DonutChartProps) {
  const [hovered, setHovered] = useState<number | null>(null)
  const total = segments.reduce((a, s) => a + s.value, 0)
  if (total === 0) return <div className="text-muted">No data</div>

  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const cx = size / 2
  const cy = size / 2

  let accumulated = 0
  const arcs = segments
    .filter(s => s.value > 0)
    .map((s, i) => {
      const pct = s.value / total
      const dashLen = circumference * pct
      const dashGap = circumference - dashLen
      const rotation = (accumulated / total) * 360 - 90
      accumulated += s.value
      return { ...s, i, pct, dashLen, dashGap, rotation }
    })

  return (
    <div className="donut-chart-wrap">
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} className="donut-chart-svg">
        {arcs.map(arc => (
          <circle
            key={arc.i}
            cx={cx}
            cy={cy}
            r={radius}
            fill="none"
            stroke={arc.color}
            strokeWidth={hovered === arc.i ? strokeWidth + 4 : strokeWidth}
            strokeDasharray={`${arc.dashLen} ${arc.dashGap}`}
            transform={`rotate(${arc.rotation} ${cx} ${cy})`}
            className="donut-segment"
            opacity={hovered !== null && hovered !== arc.i ? 0.35 : 1}
            onMouseEnter={() => setHovered(arc.i)}
            onMouseLeave={() => setHovered(null)}
          />
        ))}
        {/* Center text */}
        <text x={cx} y={cy - 4} textAnchor="middle" dominantBaseline="central" className="donut-center-value" fill="var(--text-primary)">
          {total.toLocaleString()}
        </text>
        {innerLabel && (
          <text x={cx} y={cy + 14} textAnchor="middle" dominantBaseline="central" className="donut-center-label" fill="var(--text-muted)">
            {innerLabel}
          </text>
        )}
      </svg>

      {/* Legend */}
      <div className="donut-legend">
        {arcs.map(arc => (
          <div
            key={arc.i}
            className={`donut-legend-item ${hovered === arc.i ? 'active' : ''}`}
            onMouseEnter={() => setHovered(arc.i)}
            onMouseLeave={() => setHovered(null)}
          >
            <span className="donut-legend-dot" style={{ background: arc.color }} />
            <span className="donut-legend-label">{arc.label}</span>
            <span className="donut-legend-value">{arc.value.toLocaleString()}</span>
            <span className="donut-legend-pct">{(arc.pct * 100).toFixed(1)}%</span>
          </div>
        ))}
      </div>

      {/* Tooltip on hover */}
      {hovered !== null && arcs[hovered] && (
        <div className="donut-tooltip">
          <strong>{arcs[hovered].label}</strong>: {arcs[hovered].value.toLocaleString()} ({(arcs[hovered].pct * 100).toFixed(1)}%)
        </div>
      )}
    </div>
  )
}
