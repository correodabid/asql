import { useCallback, useEffect, useRef, useState } from 'react'

type Direction = 'horizontal' | 'vertical'

type Options = {
  key: string           // localStorage key for persistence
  initial: number       // initial size in px
  min: number           // minimum size in px
  max: number           // maximum size in px
  direction: Direction  // horizontal = width, vertical = height
}

export function useResizable({ key, initial, min, max, direction }: Options) {
  const [size, setSize] = useState(() => {
    try {
      const saved = localStorage.getItem(`asql-resize-${key}`)
      if (saved) {
        const n = Number(saved)
        if (n >= min && n <= max) return n
      }
    } catch { /* ignore */ }
    return initial
  })

  const dragging = useRef(false)
  const startPos = useRef(0)
  const startSize = useRef(0)

  // Persist
  useEffect(() => {
    try {
      localStorage.setItem(`asql-resize-${key}`, String(size))
    } catch { /* ignore */ }
  }, [key, size])

  const startDrag = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    dragging.current = true
    startPos.current = direction === 'horizontal' ? e.clientX : e.clientY
    startSize.current = size

    const onMove = (me: MouseEvent) => {
      if (!dragging.current) return
      const delta = (direction === 'horizontal' ? me.clientX : me.clientY) - startPos.current
      const next = Math.max(min, Math.min(max, startSize.current + delta))
      setSize(next)
    }

    const onUp = () => {
      dragging.current = false
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }

    document.body.style.cursor = direction === 'horizontal' ? 'col-resize' : 'row-resize'
    document.body.style.userSelect = 'none'
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }, [direction, min, max, size])

  // For panels that grow from the right edge (detail panel), delta is inverted
  const startDragInverse = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    dragging.current = true
    startPos.current = direction === 'horizontal' ? e.clientX : e.clientY
    startSize.current = size

    const onMove = (me: MouseEvent) => {
      if (!dragging.current) return
      const delta = startPos.current - (direction === 'horizontal' ? me.clientX : me.clientY)
      const next = Math.max(min, Math.min(max, startSize.current + delta))
      setSize(next)
    }

    const onUp = () => {
      dragging.current = false
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }

    document.body.style.cursor = direction === 'horizontal' ? 'col-resize' : 'row-resize'
    document.body.style.userSelect = 'none'
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }, [direction, min, max, size])

  return { size, startDrag, startDragInverse }
}
