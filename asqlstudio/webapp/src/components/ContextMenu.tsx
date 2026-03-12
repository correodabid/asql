import { useEffect, useRef } from 'react'

type Props = {
  x: number
  y: number
  onAction: (action: 'insert' | 'update' | 'delete') => void
  onClose: () => void
}

export function ContextMenu({ x, y, onAction, onClose }: Props) {
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose()
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [onClose])

  return (
    <div className="ctx-menu" ref={ref} style={{ left: x, top: y }}>
      <button className="ctx-menu-item" onClick={() => onAction('insert')}>
        Copy as INSERT
      </button>
      <button className="ctx-menu-item" onClick={() => onAction('update')}>
        Copy as UPDATE
      </button>
      <button className="ctx-menu-item" onClick={() => onAction('delete')}>
        Copy as DELETE
      </button>
    </div>
  )
}
