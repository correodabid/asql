import { useEffect, useRef, useState } from 'react'
import { JsonTreeView } from './JsonTreeView'
import { IconCopy, IconX } from './Icons'

type Props = {
  columnName: string
  value: unknown
  onClose: () => void
}

export function CellInspector({ columnName, value, onClose }: Props) {
  const [copied, setCopied] = useState(false)
  const overlayRef = useRef<HTMLDivElement>(null)

  const isObject = value !== null && typeof value === 'object'
  const valueType = value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value
  const displayText = isObject ? JSON.stringify(value, null, 2) : String(value ?? 'NULL')

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [onClose])

  const handleCopy = () => {
    navigator.clipboard.writeText(displayText).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    }).catch(() => {})
  }

  return (
    <div
      className="cell-inspector-overlay"
      ref={overlayRef}
      onClick={(e) => { if (e.target === overlayRef.current) onClose() }}
    >
      <div className="cell-inspector-modal">
        <div className="cell-inspector-header">
          <div className="cell-inspector-title-wrap">
            <span className="cell-inspector-title">{columnName}</span>
            <span className="cell-inspector-meta">{isObject ? 'JSON' : valueType}</span>
          </div>
          <div className="cell-inspector-actions">
            {copied && <span className="cell-inspector-copied">Copied</span>}
            <button className="icon-btn" onClick={handleCopy} title="Copy value">
              <IconCopy />
            </button>
            <button className="icon-btn" onClick={onClose} title="Close (Esc)">
              <IconX />
            </button>
          </div>
        </div>
        <div className="cell-inspector-body">
          {isObject ? (
            <div className="cell-inspector-json-wrap">
              <JsonTreeView data={value} />
            </div>
          ) : (
            <pre className="cell-inspector-text">{displayText}</pre>
          )}
        </div>
      </div>
    </div>
  )
}
