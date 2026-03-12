import type { EntityDefinition } from '../schema'
import type { ForeignKeyLink } from '../types/workspace'

type Props = {
  entity: EntityDefinition
  tableName: string
  foreignKeys: ForeignKeyLink[]
  pkColumns: string[]
  row: Record<string, unknown>
  domain: string
  onNavigateFK: (table: string, column: string, value: unknown) => void
}

export function EntityGraph({ entity, tableName, foreignKeys, onNavigateFK, row }: Props) {
  const rootTable = entity.root_table

  // Sort: root first, then current table, then rest alphabetically
  const sortedTables = [...entity.tables].sort((a, b) => {
    if (a === rootTable) return -1
    if (b === rootTable) return 1
    if (a === tableName) return -1
    if (b === tableName) return 1
    return a.localeCompare(b)
  })

  const handleTableClick = (table: string) => {
    if (table === tableName) return
    const fk = foreignKeys.find(f => f.refTable === table)
    if (fk) {
      onNavigateFK(fk.refTable, fk.refColumn, row[fk.column])
    }
  }

  return (
    <div className="entity-graph-section">
      <div className="detail-section-title">Entity: {entity.name}</div>
      <div className="entity-breadcrumb">
        {sortedTables.map((table) => {
          const isRoot = table === rootTable
          const isCurrent = table === tableName
          const fk = foreignKeys.find(f => f.refTable === table)
          const isNavigable = !isCurrent && !!fk

          return (
            <button
              key={table}
              className={
                'entity-breadcrumb-item' +
                (isCurrent ? ' current' : '') +
                (isRoot ? ' root' : '') +
                (isNavigable ? ' navigable' : '')
              }
              onClick={() => handleTableClick(table)}
              disabled={!isNavigable}
              title={
                isCurrent
                  ? 'Current table'
                  : isNavigable
                    ? `Navigate to ${table}`
                    : isRoot
                      ? 'Root table'
                      : table
              }
            >
              {isRoot && <span className="entity-root-dot" />}
              {table}
            </button>
          )
        })}
      </div>
    </div>
  )
}
