import { useEffect } from 'react'
import { formatCell } from '../lib/sql'
import type { EntityDefinition } from '../schema'
import type { ForeignKeyLink, QueryResult, ReverseFK } from '../types/workspace'
import { IconKey, IconLink } from './Icons'
import { DetailTemporalMetadata } from './DetailTemporalMetadata'
import { DetailMutationHistory } from './DetailMutationHistory'
import { DetailEntityHistory } from './DetailEntityHistory'
import { DetailReferencedBy } from './DetailReferencedBy'
import { EntityGraph } from './EntityGraph'
import { JsonTreeView } from './JsonTreeView'

type Props = {
  result: QueryResult
  selectedRow: number
  tableName: string
  pkColumns: string[]
  foreignKeys: ForeignKeyLink[]
  referencedBy: ReverseFK[]
  domain: string
  entity?: EntityDefinition
  onNavigateFK: (table: string, column: string, value: unknown) => void
  onClose: () => void
  onLoadBaseline: () => Promise<unknown>
}

export function DetailPanel({
  result,
  selectedRow,
  tableName,
  pkColumns,
  foreignKeys,
  referencedBy,
  domain,
  entity,
  onNavigateFK,
  onClose,
  onLoadBaseline,
}: Props) {
  const row = result.rows[selectedRow]
  if (!row) return null

  const fkColumnSet = new Set(foreignKeys.map((fk) => fk.column))

  // Lazy-load baseline on first open
  useEffect(() => {
    onLoadBaseline()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  return (
    <div className="detail-panel">
      <div className="detail-header">
        <span className="detail-title">Row Detail</span>
        <button className="icon-btn" onClick={onClose} title="Close">
          x
        </button>
      </div>

      <div className="detail-body">
        <div className="detail-fields">
          {result.columns.map((col) => {
            const isPK = pkColumns.includes(col)
            const fk = foreignKeys.find((f) => f.column === col)
            const value = row[col]

            return (
              <div key={col} className="detail-field">
                <div className="detail-field-label">
                  {isPK && <span className="detail-badge pk"><IconKey /></span>}
                  {fkColumnSet.has(col) && <span className="detail-badge fk"><IconLink /></span>}
                  {col}
                </div>
                <div className="detail-field-value mono">
                  {fk ? (
                    <button
                      className="detail-fk-link"
                      onClick={() => onNavigateFK(fk.refTable, fk.refColumn, value)}
                      title={`Go to ${fk.refTable}.${fk.refColumn} = ${formatCell(value)}`}
                    >
                      {formatCell(value)} → {fk.refTable}
                    </button>
                  ) : typeof value === 'object' && value !== null ? (
                    <JsonTreeView data={value} />
                  ) : (
                    formatCell(value)
                  )}
                </div>
              </div>
            )
          })}
        </div>

        {pkColumns.length > 0 && (
          <DetailTemporalMetadata
          domain={domain}
          tableName={tableName}
          pkColumns={pkColumns}
          row={row}
          entityName={entity?.name}
          entityRootTable={entity?.root_table}
          foreignKeys={foreignKeys}
          />
        )}

        {pkColumns.length > 0 && (
          <DetailMutationHistory
            tableName={tableName}
            pkColumns={pkColumns}
            row={row}
            domain={domain}
          />
        )}

        {entity && pkColumns.length > 0 && (
          <DetailEntityHistory
            entityName={entity.name}
            rootTable={entity.root_table}
            tableName={tableName}
            pkColumns={pkColumns}
            row={row}
            domain={domain}
            foreignKeys={foreignKeys}
          />
        )}

        {entity && (
          <EntityGraph
            entity={entity}
            tableName={tableName}
            foreignKeys={foreignKeys}
            pkColumns={pkColumns}
            row={row}
            domain={domain}
            onNavigateFK={onNavigateFK}
          />
        )}

        {referencedBy.length > 0 && (
          <DetailReferencedBy
            referencedBy={referencedBy}
            row={row}
            onNavigate={onNavigateFK}
          />
        )}
      </div>
    </div>
  )
}
