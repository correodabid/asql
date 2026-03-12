type RadialGaugeProps = {
  value: number
  max: number
  label?: string
  color?: string
  format?: (v: number) => string
  size?: number
  strokeWidth?: number
}

export function RadialGauge({
  value,
  max,
  label,
  color = 'var(--accent)',
  format = (v) => v.toFixed(0),
  size = 100,
  strokeWidth = 8,
}: RadialGaugeProps) {
  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius * 0.75 // 270-degree arc
  const pct = max > 0 ? Math.min(value / max, 1) : 0
  const offset = circumference * (1 - pct)
  const cx = size / 2
  const cy = size / 2

  // Arc starts at 135deg (bottom-left) and sweeps 270deg clockwise
  const gradId = `rg-${label?.replace(/\s+/g, '') || 'g'}-${size}`

  return (
    <div className="radial-gauge">
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
        <defs>
          <linearGradient id={gradId} x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor={color} stopOpacity="0.6" />
            <stop offset="100%" stopColor={color} stopOpacity="1" />
          </linearGradient>
        </defs>
        {/* Track */}
        <circle
          cx={cx}
          cy={cy}
          r={radius}
          fill="none"
          stroke="var(--border-strong)"
          strokeWidth={strokeWidth}
          strokeDasharray={circumference}
          strokeDashoffset={0}
          strokeLinecap="round"
          transform={`rotate(135 ${cx} ${cy})`}
          opacity={0.4}
        />
        {/* Fill */}
        <circle
          className="radial-gauge-fill"
          cx={cx}
          cy={cy}
          r={radius}
          fill="none"
          stroke={`url(#${gradId})`}
          strokeWidth={strokeWidth}
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          transform={`rotate(135 ${cx} ${cy})`}
        />
        {/* Value */}
        <text
          x={cx}
          y={cy - 2}
          textAnchor="middle"
          dominantBaseline="central"
          className="radial-gauge-value"
          fill="var(--text-primary)"
        >
          {format(value)}
        </text>
        {/* Label */}
        {label && (
          <text
            x={cx}
            y={cy + 14}
            textAnchor="middle"
            dominantBaseline="central"
            className="radial-gauge-label"
            fill="var(--text-muted)"
          >
            {label}
          </text>
        )}
      </svg>
    </div>
  )
}
