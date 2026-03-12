import { useCallback, useRef, useState } from 'react'

type ColumnWidths = Record<string, number>

const MIN_WIDTH = 60
const DEFAULT_WIDTH = 150

export function useColumnResize(columns: string[]) {
  const [widths, setWidths] = useState<ColumnWidths>({})
  const dragRef = useRef<{ col: string; startX: number; startW: number } | null>(null)

  const getWidth = useCallback(
    (col: string) => widths[col] ?? DEFAULT_WIDTH,
    [widths],
  )

  const onMouseDown = useCallback(
    (col: string, e: React.MouseEvent) => {
      e.preventDefault()
      e.stopPropagation()
      const startX = e.clientX
      const startW = widths[col] ?? DEFAULT_WIDTH
      dragRef.current = { col, startX, startW }

      const onMove = (ev: MouseEvent) => {
        if (!dragRef.current) return
        const delta = ev.clientX - dragRef.current.startX
        const newW = Math.max(MIN_WIDTH, dragRef.current.startW + delta)
        setWidths((prev) => ({ ...prev, [dragRef.current!.col]: newW }))
      }

      const onUp = () => {
        dragRef.current = null
        document.removeEventListener('mousemove', onMove)
        document.removeEventListener('mouseup', onUp)
        document.body.style.cursor = ''
        document.body.style.userSelect = ''
      }

      document.body.style.cursor = 'col-resize'
      document.body.style.userSelect = 'none'
      document.addEventListener('mousemove', onMove)
      document.addEventListener('mouseup', onUp)
    },
    [widths],
  )

  // Reset widths when columns change structurally
  const resetWidths = useCallback(() => setWidths({}), [])

  return { getWidth, onMouseDown, resetWidths, columns }
}
