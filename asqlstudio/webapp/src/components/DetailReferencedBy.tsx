import type { ReverseFK } from '../types/workspace'
import { formatCell } from '../lib/sql'

type Props = {
  referencedBy: ReverseFK[]
  row: Record<string, unknown>
  onNavigate: (table: string, column: string, value: unknown) => void
}

export function DetailReferencedBy({ referencedBy, row, onNavigate }: Props) {
  return (
    <div className="detail-section">
      <div className="detail-section-title" style={{ padding: '8px 0 4px' }}>
        Referenced By
      </div>
      <div className="detail-ref-list">
        {referencedBy.map((ref, i) => {
          const value = row[ref.refColumn]
          return (
            <button
              key={i}
              className="detail-ref-item"
              onClick={() => onNavigate(ref.table, ref.column, value)}
              title={`${ref.table}.${ref.column} → ${formatCell(value)}`}
            >
              <span className="detail-ref-table">{ref.table}</span>
              <span className="detail-ref-col mono">.{ref.column}</span>
            </button>
          )
        })}
      </div>
    </div>
  )
}
