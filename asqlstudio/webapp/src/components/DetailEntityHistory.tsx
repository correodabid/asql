import { useCallback, useEffect, useState } from 'react'
import { api } from '../lib/api'

type Props = {
  entityName: string
  rootTable: string
  tableName: string
  pkColumns: string[]
  row: Record<string, unknown>
  domain: string
  foreignKeys: { column: string; refTable: string; refColumn: string }[]
}

type VersionEntry = {
  version: number
  commit_lsn: number
  tables: string[]
}

type VersionResponse = {
  status: string
  entity: string
  root_pk: string
  versions: VersionEntry[]
}

function resolveRootPK(
  row: Record<string, unknown>,
  tableName: string,
  rootTable: string,
  pkColumns: string[],
  foreignKeys: { column: string; refTable: string; refColumn: string }[],
): string | null {
  if (tableName === rootTable) {
    // Current table IS the root table — use PK directly
    if (pkColumns.length === 0) return null
    const val = row[pkColumns[0]]
    if (val === null || val === undefined) return null
    return String(val)
  }

  // Child table — try to find FK column that points to root table
  const fk = foreignKeys.find((f) => f.refTable === rootTable)
  if (fk) {
    const val = row[fk.column]
    if (val === null || val === undefined) return null
    return String(val)
  }

  return null
}

export function DetailEntityHistory({
  entityName,
  rootTable,
  tableName,
  pkColumns,
  row,
  domain,
  foreignKeys,
}: Props) {
  const [versions, setVersions] = useState<VersionEntry[]>([])
  const [loading, setLoading] = useState(false)
  const [expanded, setExpanded] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const rootPK = resolveRootPK(row, tableName, rootTable, pkColumns, foreignKeys)

  const loadHistory = useCallback(async () => {
    if (versions.length > 0 || !rootPK) return
    setLoading(true)
    setError(null)
    try {
      const response = await api<VersionResponse>(
        '/api/entity-version-history',
        'POST',
        {
          domain,
          entity_name: entityName,
          root_pk: rootPK,
        },
      )
      setVersions(response.versions || [])
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load')
    } finally {
      setLoading(false)
    }
  }, [entityName, rootPK, domain, versions.length])

  useEffect(() => {
    if (expanded && versions.length === 0 && rootPK) {
      loadHistory()
    }
  }, [expanded, versions.length, rootPK, loadHistory])

  if (!rootPK) return null

  return (
    <div className="detail-section">
      <button
        className="detail-section-toggle"
        onClick={() => setExpanded(!expanded)}
      >
        <span className="detail-section-title">
          Entity History
          <span className="entity-name-badge">{entityName}</span>
        </span>
        <span>{expanded ? '-' : '+'}</span>
      </button>

      {expanded && (
        <div className="detail-section-body">
          {loading && <div className="text-muted">Loading...</div>}
          {error && <div className="text-muted">{error}</div>}
          {!loading && !error && versions.length === 0 && (
            <div className="text-muted">No entity versions recorded</div>
          )}
          {[...versions].reverse().map((v, i) => (
            <div key={i} className="entity-version-entry">
              <div className="entity-version-header">
                <span className="entity-version-badge">v{v.version}</span>
                <span className="entity-version-lsn mono">LSN {v.commit_lsn}</span>
              </div>
              {v.tables.length > 0 && (
                <div className="entity-version-tables">
                  {v.tables.map((t) => (
                    <span key={t} className="entity-version-table-tag">{t}</span>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
