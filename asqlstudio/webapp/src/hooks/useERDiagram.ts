import { useCallback, useEffect, useRef, useState } from 'react'
import type { SchemaModel } from '../schema'

type Point = { x: number; y: number }

type DragInfo = {
  tableKey: string
  originPos: Point
  originMouse: Point
  moved: boolean
}

type PanInfo = {
  startMouse: Point
  startPan: Point
}

function clamp(v: number, min: number, max: number) {
  return Math.max(min, Math.min(max, v))
}

const LAYOUT_KEY_PREFIX = 'asql_er_layout_'

function loadLayout(domain: string): { positions: Record<string, Point>; zoom: number; pan: Point } | null {
  try {
    const raw = localStorage.getItem(LAYOUT_KEY_PREFIX + domain)
    return raw ? JSON.parse(raw) : null
  } catch {
    return null
  }
}

function saveLayout(domain: string, positions: Record<string, Point>, zoom: number, pan: Point): void {
  try {
    localStorage.setItem(LAYOUT_KEY_PREFIX + domain, JSON.stringify({ positions, zoom, pan }))
  } catch {
    // quota exceeded — ignore
  }
}

export function useERDiagram(
  model: SchemaModel,
  onSelectTable: (index: number) => void,
) {
  const [positions, setPositions] = useState<Record<string, Point>>(() => {
    return loadLayout(model.domain)?.positions ?? {}
  })
  const [zoom, setZoom] = useState(() => {
    return loadLayout(model.domain)?.zoom ?? 1
  })
  const [pan, setPan] = useState<Point>(() => {
    return loadLayout(model.domain)?.pan ?? { x: 0, y: 0 }
  })
  const [hoveredRelationship, setHoveredRelationship] = useState<number | null>(null)
  const [hoveredEntity, setHoveredEntity] = useState<string | null>(null)
  const [isDragging, setIsDragging] = useState(false)
  const [isPanning, setIsPanning] = useState(false)
  const [draggedTable, setDraggedTable] = useState<string | null>(null)

  const dragRef = useRef<DragInfo | null>(null)
  const panRef = useRef<PanInfo | null>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  // Keep refs in sync for animation callbacks
  const zoomRef = useRef(zoom)
  const panRefSync = useRef(pan)
  const animFrameRef = useRef<number | null>(null)
  useEffect(() => { zoomRef.current = zoom }, [zoom])
  useEffect(() => { panRefSync.current = pan }, [pan])

  // Reset when domain changes (adjust state during render)
  const [prevDomain, setPrevDomain] = useState(model.domain)
  if (model.domain !== prevDomain) {
    setPrevDomain(model.domain)
    const saved = loadLayout(model.domain)
    setPositions(saved?.positions ?? {})
    setPan(saved?.pan ?? { x: 0, y: 0 })
    setZoom(saved?.zoom ?? 1)
  }

  // Persist layout to localStorage whenever positions, zoom, or pan change
  useEffect(() => {
    saveLayout(model.domain, positions, zoom, pan)
  }, [model.domain, positions, zoom, pan])

  const screenToContent = useCallback((clientX: number, clientY: number): Point => {
    const rect = containerRef.current?.getBoundingClientRect()
    if (!rect) return { x: clientX, y: clientY }
    return {
      x: (clientX - rect.left - panRefSync.current.x) / zoomRef.current,
      y: (clientY - rect.top - panRefSync.current.y) / zoomRef.current,
    }
  }, [])

  const handleTableMouseDown = useCallback((tableKey: string, fallbackPos: Point, e: React.MouseEvent) => {
    if (e.button !== 0) return
    e.stopPropagation()
    const content = screenToContent(e.clientX, e.clientY)
    const currentPos = positions[tableKey] || fallbackPos
    dragRef.current = {
      tableKey,
      originPos: { ...currentPos },
      originMouse: content,
      moved: false,
    }
    setIsDragging(true)
    setDraggedTable(tableKey)
  }, [positions, screenToContent])

  const handleMouseMove = useCallback((e: React.MouseEvent) => {
    const drag = dragRef.current
    if (drag) {
      const content = screenToContent(e.clientX, e.clientY)
      const dx = content.x - drag.originMouse.x
      const dy = content.y - drag.originMouse.y
      if (!drag.moved && Math.abs(dx) + Math.abs(dy) > 4) {
        drag.moved = true
      }
      if (drag.moved) {
        const key = drag.tableKey
        const newPos = {
          x: drag.originPos.x + dx,
          y: drag.originPos.y + dy,
        }
        setPositions(prev => ({ ...prev, [key]: newPos }))
      }
    } else if (panRef.current) {
      const dx = e.clientX - panRef.current.startMouse.x
      const dy = e.clientY - panRef.current.startMouse.y
      setPan({
        x: panRef.current.startPan.x + dx,
        y: panRef.current.startPan.y + dy,
      })
    }
  }, [screenToContent])

  const finishInteraction = useCallback(() => {
    if (dragRef.current) {
      if (!dragRef.current.moved) {
        const key = dragRef.current.tableKey
        const idx = model.tables.findIndex(t => t.name === key)
        if (idx >= 0) onSelectTable(idx)
      }
      dragRef.current = null
      setIsDragging(false)
      setDraggedTable(null)
    }
    if (panRef.current) {
      panRef.current = null
      setIsPanning(false)
    }
  }, [model.tables, onSelectTable])

  const handleMouseUp = useCallback(() => {
    finishInteraction()
  }, [finishInteraction])

  const handleBackgroundMouseDown = useCallback((e: React.MouseEvent) => {
    if (e.button !== 0 && e.button !== 1) return
    panRef.current = {
      startMouse: { x: e.clientX, y: e.clientY },
      startPan: { ...panRefSync.current },
    }
    setIsPanning(true)
  }, [])

  // Wheel handler registered imperatively for { passive: false }
  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    const handler = (e: WheelEvent) => {
      e.preventDefault()

      const rect = el.getBoundingClientRect()
      const mouseX = e.clientX - rect.left
      const mouseY = e.clientY - rect.top

      // Zoom toward cursor
      const direction = e.deltaY < 0 ? 1 : -1
      const factor = 1 + direction * 0.1
      const currentZoom = zoomRef.current
      const newZoom = clamp(currentZoom * factor, 0.25, 3)
      const scale = newZoom / currentZoom
      const currentPan = panRefSync.current

      const newPanX = mouseX - (mouseX - currentPan.x) * scale
      const newPanY = mouseY - (mouseY - currentPan.y) * scale

      setZoom(newZoom)
      setPan({ x: newPanX, y: newPanY })
    }

    el.addEventListener('wheel', handler, { passive: false })
    return () => el.removeEventListener('wheel', handler)
  }, [])

  // Global mouseup to catch releases outside the canvas
  useEffect(() => {
    const handler = () => finishInteraction()
    window.addEventListener('mouseup', handler)
    return () => window.removeEventListener('mouseup', handler)
  }, [finishInteraction])

  // Smooth animation helper
  const animateTo = useCallback((targetZoom: number, targetPan: Point, duration = 300) => {
    if (animFrameRef.current) {
      cancelAnimationFrame(animFrameRef.current)
    }

    const startZoom = zoomRef.current
    const startPan = { ...panRefSync.current }
    const startTime = performance.now()

    const tick = (now: number) => {
      const t = Math.min((now - startTime) / duration, 1)
      // easeInOutQuad
      const ease = t < 0.5 ? 2 * t * t : 1 - (-2 * t + 2) ** 2 / 2

      setZoom(startZoom + (targetZoom - startZoom) * ease)
      setPan({
        x: startPan.x + (targetPan.x - startPan.x) * ease,
        y: startPan.y + (targetPan.y - startPan.y) * ease,
      })

      if (t < 1) {
        animFrameRef.current = requestAnimationFrame(tick)
      } else {
        animFrameRef.current = null
      }
    }

    animFrameRef.current = requestAnimationFrame(tick)
  }, [])

  const zoomIn = useCallback(() => {
    const newZoom = clamp(zoomRef.current * 1.2, 0.25, 3)
    // Zoom toward center of container
    const rect = containerRef.current?.getBoundingClientRect()
    if (rect) {
      const cx = rect.width / 2
      const cy = rect.height / 2
      const scale = newZoom / zoomRef.current
      const currentPan = panRefSync.current
      const targetPan = {
        x: cx - (cx - currentPan.x) * scale,
        y: cy - (cy - currentPan.y) * scale,
      }
      animateTo(newZoom, targetPan, 150)
    } else {
      setZoom(newZoom)
    }
  }, [animateTo])

  const zoomOut = useCallback(() => {
    const newZoom = clamp(zoomRef.current / 1.2, 0.25, 3)
    const rect = containerRef.current?.getBoundingClientRect()
    if (rect) {
      const cx = rect.width / 2
      const cy = rect.height / 2
      const scale = newZoom / zoomRef.current
      const currentPan = panRefSync.current
      const targetPan = {
        x: cx - (cx - currentPan.x) * scale,
        y: cy - (cy - currentPan.y) * scale,
      }
      animateTo(newZoom, targetPan, 150)
    } else {
      setZoom(newZoom)
    }
  }, [animateTo])

  const fitToScreen = useCallback((tables: Array<{ x: number; y: number; w: number; h: number }>) => {
    const rect = containerRef.current?.getBoundingClientRect()
    if (!rect || tables.length === 0) return

    const margin = 60
    const minX = Math.min(...tables.map(t => t.x))
    const minY = Math.min(...tables.map(t => t.y))
    const maxX = Math.max(...tables.map(t => t.x + t.w))
    const maxY = Math.max(...tables.map(t => t.y + t.h))

    const contentW = maxX - minX
    const contentH = maxY - minY
    if (contentW <= 0 || contentH <= 0) return

    const availW = rect.width - margin * 2
    const availH = rect.height - margin * 2

    const newZoom = clamp(Math.min(availW / contentW, availH / contentH), 0.1, 2)
    const newPanX = margin + (availW - contentW * newZoom) / 2 - minX * newZoom
    const newPanY = margin + (availH - contentH * newZoom) / 2 - minY * newZoom

    animateTo(newZoom, { x: newPanX, y: newPanY })
  }, [animateTo])

  const resetLayout = useCallback(() => {
    setPositions({})
    animateTo(1, { x: 0, y: 0 })
  }, [animateTo])

  // Cleanup animation on unmount
  useEffect(() => {
    return () => {
      if (animFrameRef.current) cancelAnimationFrame(animFrameRef.current)
    }
  }, [])

  return {
    containerRef,
    positions,
    zoom,
    pan,
    hoveredRelationship,
    hoveredEntity,
    isDragging,
    isPanning,
    draggedTable,

    handleTableMouseDown,
    handleMouseMove,
    handleMouseUp,
    handleBackgroundMouseDown,

    zoomIn,
    zoomOut,
    fitToScreen,
    resetLayout,

    setHoveredRelationship,
    setHoveredEntity,
    setPan,
  }
}
