import { useCallback, useMemo, useRef, useState } from 'react'

const ROW_HEIGHT = 30
const BUFFER_ROWS = 10

type VirtualScrollResult = {
  startIndex: number
  endIndex: number
  offsetY: number
  totalHeight: number
  containerRef: React.RefObject<HTMLDivElement | null>
  onScroll: () => void
}

export function useVirtualScroll(rowCount: number, enabled: boolean): VirtualScrollResult {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [scrollTop, setScrollTop] = useState(0)

  const onScroll = useCallback(() => {
    const el = containerRef.current
    if (el) setScrollTop(el.scrollTop)
  }, [])

  const totalHeight = rowCount * ROW_HEIGHT

  const { startIndex, endIndex, offsetY } = useMemo(() => {
    if (!enabled || rowCount === 0) {
      return { startIndex: 0, endIndex: rowCount, offsetY: 0 }
    }

    const containerHeight = containerRef.current?.clientHeight ?? 600
    const visibleCount = Math.ceil(containerHeight / ROW_HEIGHT)

    const start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - BUFFER_ROWS)
    const end = Math.min(rowCount, start + visibleCount + BUFFER_ROWS * 2)

    return { startIndex: start, endIndex: end, offsetY: start * ROW_HEIGHT }
  }, [scrollTop, rowCount, enabled])

  return { startIndex, endIndex, offsetY, totalHeight, containerRef, onScroll }
}
