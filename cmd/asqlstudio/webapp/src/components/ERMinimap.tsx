import { useCallback, useRef } from 'react'

type TableRect = { x: number; y: number; w: number; h: number; domainColor?: string }

type Props = {
  tables: TableRect[]
  zoom: number
  pan: { x: number; y: number }
  containerWidth: number
  containerHeight: number
  onPan: (pan: { x: number; y: number }) => void
}

const MINIMAP_W = 160
const MINIMAP_H = 110
const MINIMAP_PAD = 6

export function ERMinimap({ tables, zoom, pan, containerWidth, containerHeight, onPan }: Props) {
  const dragging = useRef(false)

  if (tables.length === 0) return null

  // Compute content bounds
  const minX = Math.min(...tables.map(t => t.x))
  const minY = Math.min(...tables.map(t => t.y))
  const maxX = Math.max(...tables.map(t => t.x + t.w))
  const maxY = Math.max(...tables.map(t => t.y + t.h))

  const contentW = maxX - minX || 1
  const contentH = maxY - minY || 1

  // Add margin to content bounds
  const margin = 40
  const totalW = contentW + margin * 2
  const totalH = contentH + margin * 2
  const originX = minX - margin
  const originY = minY - margin

  // Scale to fit minimap
  const scale = Math.min((MINIMAP_W - MINIMAP_PAD * 2) / totalW, (MINIMAP_H - MINIMAP_PAD * 2) / totalH)

  // Viewport rectangle in content space
  const vpX = (-pan.x / zoom)
  const vpY = (-pan.y / zoom)
  const vpW = containerWidth / zoom
  const vpH = containerHeight / zoom

  // Viewport in minimap space
  const mvpX = MINIMAP_PAD + (vpX - originX) * scale
  const mvpY = MINIMAP_PAD + (vpY - originY) * scale
  const mvpW = vpW * scale
  const mvpH = vpH * scale

  const minimapToContent = useCallback((clientX: number, clientY: number, rect: DOMRect) => {
    const mx = clientX - rect.left
    const my = clientY - rect.top
    const cx = (mx - MINIMAP_PAD) / scale + originX
    const cy = (my - MINIMAP_PAD) / scale + originY
    return { cx, cy }
  }, [scale, originX, originY])

  const handleMouseDown = useCallback((e: React.MouseEvent<SVGSVGElement>) => {
    e.preventDefault()
    e.stopPropagation()
    dragging.current = true

    const rect = e.currentTarget.getBoundingClientRect()
    const { cx, cy } = minimapToContent(e.clientX, e.clientY, rect)

    // Center viewport on clicked point
    onPan({
      x: -(cx - containerWidth / zoom / 2) * zoom,
      y: -(cy - containerHeight / zoom / 2) * zoom,
    })

    const onMove = (me: MouseEvent) => {
      if (!dragging.current) return
      const { cx: mcx, cy: mcy } = minimapToContent(me.clientX, me.clientY, rect)
      onPan({
        x: -(mcx - containerWidth / zoom / 2) * zoom,
        y: -(mcy - containerHeight / zoom / 2) * zoom,
      })
    }

    const onUp = () => {
      dragging.current = false
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }

    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }, [minimapToContent, onPan, containerWidth, containerHeight, zoom])

  return (
    <div className="er-minimap">
      <svg
        width={MINIMAP_W}
        height={MINIMAP_H}
        onMouseDown={handleMouseDown}
        style={{ cursor: 'crosshair' }}
      >
        {/* Background */}
        <rect width={MINIMAP_W} height={MINIMAP_H} rx={6} fill="var(--bg-elevated)" />

        {/* Table rectangles */}
        {tables.map((t, i) => (
          <rect
            key={i}
            x={MINIMAP_PAD + (t.x - originX) * scale}
            y={MINIMAP_PAD + (t.y - originY) * scale}
            width={Math.max(t.w * scale, 2)}
            height={Math.max(t.h * scale, 1)}
            rx={1}
            fill={t.domainColor || 'var(--text-muted)'}
            opacity={0.6}
          />
        ))}

        {/* Viewport indicator */}
        <rect
          x={mvpX}
          y={mvpY}
          width={mvpW}
          height={mvpH}
          rx={2}
          fill="rgba(99, 91, 255, 0.08)"
          stroke="var(--accent)"
          strokeWidth={1.5}
          strokeOpacity={0.7}
        />

        {/* Border */}
        <rect
          width={MINIMAP_W}
          height={MINIMAP_H}
          rx={6}
          fill="none"
          stroke="var(--border-strong)"
          strokeWidth={1}
        />
      </svg>
    </div>
  )
}
