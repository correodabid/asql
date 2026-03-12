import { useMemo } from 'react'
import { IconCopy } from './Icons'

type Props = {
  rowA: Record<string, unknown>
  rowB: Record<string, unknown>
  rowALabel?: string
  rowBLabel?: string
  columns: string[]
  onClose: () => void
}

type FieldStatus = 'changed' | 'added' | 'removed' | 'unchanged'

type FieldDiff = {
  field: string
  valueA: unknown
  valueB: unknown
  status: FieldStatus
}

function stringify(v: unknown): string {
  if (v === null || v === undefined) return 'NULL'
  if (typeof v === 'object') return JSON.stringify(v, null, 2)
  return String(v)
}

function isNullish(v: unknown): boolean {
  return v === null || v === undefined
}

function CellValue({ value, highlight }: { value: unknown; highlight: boolean }) {
  if (isNullish(value)) {
    return <span className={`rdiff-null ${highlight ? 'rdiff-highlight' : ''}`}>NULL</span>
  }
  if (typeof value === 'boolean') {
    return <span className={`rdiff-bool ${highlight ? 'rdiff-highlight' : ''}`}>{value ? 'true' : 'false'}</span>
  }
  if (typeof value === 'object') {
    return (
      <pre className={`rdiff-json ${highlight ? 'rdiff-highlight' : ''}`}>
        {JSON.stringify(value, null, 2)}
      </pre>
    )
  }
  return <span className={highlight ? 'rdiff-highlight' : ''}>{String(value)}</span>
}

export function RowDiffModal({ rowA, rowB, rowALabel, rowBLabel, columns, onClose }: Props) {
  const diffs = useMemo<FieldDiff[]>(() => {
    // Use passed columns order, then add any extra keys from either row
    const allFields = [
      ...columns,
      ...Object.keys(rowA).filter(k => !columns.includes(k)),
      ...Object.keys(rowB).filter(k => !columns.includes(k)),
    ]
    // Deduplicate preserving order
    const seen = new Set<string>()
    const fields: string[] = []
    for (const f of allFields) {
      if (!seen.has(f)) { seen.add(f); fields.push(f) }
    }

    return fields.map(field => {
      const va = Object.prototype.hasOwnProperty.call(rowA, field) ? rowA[field] : undefined
      const vb = Object.prototype.hasOwnProperty.call(rowB, field) ? rowB[field] : undefined

      let status: FieldStatus
      if (va === undefined && vb !== undefined) {
        status = 'added'
      } else if (va !== undefined && vb === undefined) {
        status = 'removed'
      } else if (JSON.stringify(va) !== JSON.stringify(vb)) {
        status = 'changed'
      } else {
        status = 'unchanged'
      }

      return { field, valueA: va, valueB: vb, status }
    })
  }, [rowA, rowB, columns])

  const changedCount = diffs.filter(d => d.status !== 'unchanged').length
  const labelA = rowALabel ?? 'Row A'
  const labelB = rowBLabel ?? 'Row B'

  const handleCopyDiff = () => {
    const lines: string[] = [`# Row Diff: ${labelA} vs ${labelB}`, '']
    for (const d of diffs) {
      if (d.status === 'unchanged') continue
      lines.push(`## ${d.field} [${d.status}]`)
      if (d.status !== 'added') lines.push(`- ${stringify(d.valueA)}`)
      if (d.status !== 'removed') lines.push(`+ ${stringify(d.valueB)}`)
      lines.push('')
    }
    navigator.clipboard.writeText(lines.join('\n')).catch(() => {})
  }

  return (
    <div className="rdiff-overlay" onClick={e => { if (e.target === e.currentTarget) onClose() }}>
      <div className="rdiff-modal">
        {/* Header */}
        <div className="rdiff-header">
          <div className="rdiff-title">
            <span className="rdiff-title-icon">⇄</span>
            Row Diff
            {changedCount > 0 && (
              <span className="rdiff-badge rdiff-badge-changed">{changedCount} change{changedCount !== 1 ? 's' : ''}</span>
            )}
            {changedCount === 0 && (
              <span className="rdiff-badge rdiff-badge-equal">identical</span>
            )}
          </div>
          <div className="rdiff-header-actions">
            <button className="rdiff-copy-btn" onClick={handleCopyDiff} title="Copy diff to clipboard">
              <IconCopy /> Copy diff
            </button>
            <button className="rdiff-close-btn" onClick={onClose}>✕</button>
          </div>
        </div>

        {/* Column labels */}
        <div className="rdiff-col-labels">
          <div className="rdiff-col-field" />
          <div className="rdiff-col-a">{labelA}</div>
          <div className="rdiff-col-b">{labelB}</div>
        </div>

        {/* Diff table */}
        <div className="rdiff-body">
          {diffs.length === 0 && (
            <div className="rdiff-empty">No fields to compare</div>
          )}
          {diffs.map(({ field, valueA, valueB, status }) => (
            <div key={field} className={`rdiff-row rdiff-row-${status}`}>
              <div className="rdiff-field">
                <span className="rdiff-field-name">{field}</span>
                {status !== 'unchanged' && (
                  <span className={`rdiff-status-dot rdiff-dot-${status}`} title={status} />
                )}
              </div>
              <div className={`rdiff-val rdiff-val-a ${status === 'changed' || status === 'removed' ? 'rdiff-val-old' : ''}`}>
                {status === 'added'
                  ? <span className="rdiff-absent">—</span>
                  : <CellValue value={valueA} highlight={status === 'changed' || status === 'removed'} />
                }
              </div>
              <div className={`rdiff-val rdiff-val-b ${status === 'changed' || status === 'added' ? 'rdiff-val-new' : ''}`}>
                {status === 'removed'
                  ? <span className="rdiff-absent">—</span>
                  : <CellValue value={valueB} highlight={status === 'changed' || status === 'added'} />
                }
              </div>
            </div>
          ))}
        </div>

        {/* Footer summary */}
        <div className="rdiff-footer">
          <span className="rdiff-footer-stat rdiff-stat-changed">
            {diffs.filter(d => d.status === 'changed').length} changed
          </span>
          <span className="rdiff-footer-stat rdiff-stat-added">
            {diffs.filter(d => d.status === 'added').length} added
          </span>
          <span className="rdiff-footer-stat rdiff-stat-removed">
            {diffs.filter(d => d.status === 'removed').length} removed
          </span>
          <span className="rdiff-footer-stat rdiff-stat-unchanged">
            {diffs.filter(d => d.status === 'unchanged').length} unchanged
          </span>
        </div>
      </div>
    </div>
  )
}
