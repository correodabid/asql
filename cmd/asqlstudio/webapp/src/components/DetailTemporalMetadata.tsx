import { useCallback, useEffect, useMemo, useState } from 'react'
import { api } from '../lib/api'

type Props = {
  domain: string
  tableName: string
  pkColumns: string[]
  row: Record<string, unknown>
  entityName?: string
  entityRootTable?: string
  foreignKeys: { column: string; refTable: string; refColumn: string }[]
}

type TemporalLookupResponse = {
  status: string
  current_lsn?: number | null
  row_lsn?: number | null
  resolve_reference?: number | null
  resolve_reference_error?: string
  entity_version?: number | null
  entity_head_lsn?: number | null
  entity_version_lsn?: number | null
}

function resolveEntityRootPK(
  row: Record<string, unknown>,
  tableName: string,
  rootTable: string,
  pkColumns: string[],
  foreignKeys: { column: string; refTable: string; refColumn: string }[],
): string | null {
  if (tableName === rootTable) {
    if (pkColumns.length === 0) return null
    const value = row[pkColumns[0]]
    if (value === null || value === undefined) return null
    return String(value)
  }
  const fk = foreignKeys.find((candidate) => candidate.refTable === rootTable)
  if (!fk) return null
  const value = row[fk.column]
  if (value === null || value === undefined) return null
  return String(value)
}

function renderValue(value: number | null | undefined, error?: string): string {
  if (error) return error
  if (value === null || value === undefined) return 'NULL'
  return value.toLocaleString()
}

export function DetailTemporalMetadata({
  domain,
  tableName,
  pkColumns,
  row,
  entityName,
  entityRootTable,
  foreignKeys,
}: Props) {
  const [expanded, setExpanded] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<TemporalLookupResponse | null>(null)

  const rowPK = useMemo(() => {
    if (pkColumns.length !== 1) return null
    const value = row[pkColumns[0]]
    if (value === null || value === undefined) return null
    return String(value)
  }, [pkColumns, row])

  const entityRootPK = useMemo(() => {
    if (!entityName || !entityRootTable) return null
    return resolveEntityRootPK(row, tableName, entityRootTable, pkColumns, foreignKeys)
  }, [entityName, entityRootTable, foreignKeys, pkColumns, row, tableName])

  const load = useCallback(async () => {
    if (!rowPK || data) return
    setLoading(true)
    setError(null)
    try {
      const response = await api<TemporalLookupResponse>('/api/temporal-lookup', 'POST', {
        domain,
        table_name: tableName,
        primary_key: rowPK,
        entity_name: entityName,
        entity_root_pk: entityRootPK,
      })
      setData(response)
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load temporal metadata')
    } finally {
      setLoading(false)
    }
  }, [data, domain, entityName, entityRootPK, rowPK, tableName])

  useEffect(() => {
    if (expanded && !data && rowPK) {
      load()
    }
  }, [data, expanded, load, rowPK])

  if (!rowPK) {
    return (
      <div className="detail-section">
        <button className="detail-section-toggle" onClick={() => setExpanded(!expanded)}>
          <span className="detail-section-title">Temporal Metadata</span>
          <span>{expanded ? '-' : '+'}</span>
        </button>
        {expanded && (
          <div className="detail-section-body">
            <div className="text-muted">Temporal helper shortcuts are currently available for single-column primary keys.</div>
          </div>
        )}
      </div>
    )
  }

  const items = [
    { label: 'current_lsn()', value: data?.current_lsn },
    { label: 'row_lsn(...)', value: data?.row_lsn },
    { label: 'resolve_reference(...)', value: data?.resolve_reference, error: data?.resolve_reference_error },
  ]

  if (entityName && entityRootPK) {
    items.push(
      { label: 'entity_version(...)', value: data?.entity_version },
      { label: 'entity_head_lsn(...)', value: data?.entity_head_lsn },
      {
        label: data?.entity_version ? `entity_version_lsn(..., v${data.entity_version})` : 'entity_version_lsn(...)',
        value: data?.entity_version_lsn,
      },
    )
  }

  return (
    <div className="detail-section">
      <button className="detail-section-toggle" onClick={() => setExpanded(!expanded)}>
        <span className="detail-section-title">Temporal Metadata</span>
        <span>{expanded ? '-' : '+'}</span>
      </button>

      {expanded && (
        <div className="detail-section-body">
          {loading && <div className="text-muted">Loading...</div>}
          {error && <div className="text-muted">{error}</div>}
          {!loading && !error && (
            <div style={{ display: 'grid', gap: 8 }}>
              <div className="text-muted" style={{ fontSize: 12 }}>
                Row PK <span className="mono">{rowPK}</span>
                {entityRootPK && entityRootPK !== rowPK && (
                  <>
                    {' '}
                    · Entity root PK <span className="mono">{entityRootPK}</span>
                  </>
                )}
              </div>
              {items.map((item) => (
                <div
                  key={item.label}
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    gap: 12,
                    padding: '8px 10px',
                    border: '1px solid var(--border)',
                    borderRadius: '8px',
                    background: 'var(--bg-elevated)',
                  }}
                >
                  <span className="mono" style={{ color: 'var(--text-secondary)', fontSize: 11 }}>{item.label}</span>
                  <span
                    className="mono"
                    style={{
                      color: item.error ? 'var(--text-warning)' : 'var(--text-primary)',
                      textAlign: 'right',
                      fontSize: 11,
                    }}
                  >
                    {renderValue(item.value, item.error)}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
