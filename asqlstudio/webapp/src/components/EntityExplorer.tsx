import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../lib/api'
import { formatCell } from '../lib/sql'
import { IconChevronRight, IconLayers, IconRefresh, IconSearch, IconTable } from './Icons'

// ─── Types ──────────────────────────────────────────────────────────────────

type EntityDef = { name: string; root_table: string; tables: string[] }

type VersionEntry = { version: number; commit_lsn: number; tables: string[] }

type SnapTable = { name: string; columns: string[]; rows: Record<string, unknown>[] }

type Props = {
  domain: string
  onOpenChangeDebugger?: (entityName: string, rootPK?: string) => void
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function renderCell(v: unknown): React.ReactNode {
  if (v == null) return <span className="cell-null">NULL</span>
  if (typeof v === 'object') return <span className="cell-json">{JSON.stringify(v)}</span>
  if (typeof v === 'boolean') return <span className="cell-bool">{v ? 'true' : 'false'}</span>
  return <>{formatCell(v)}</>
}

function truncate(s: string, max = 28) {
  return s.length > max ? s.slice(0, max) + '…' : s
}

// ─── Diff helpers ──────────────────────────────────────────────────────────

type CellDiff  = { old: unknown; new: unknown }
type RowDiff   = 'added' | 'deleted' | Map<string, CellDiff>
type TableDiff = Map<string, RowDiff>

function guessPk(cols: string[]): string {
  return cols.find((c) => c === 'id') ?? cols.find((c) => /^.+_id$/.test(c)) ?? cols[0] ?? ''
}

function diffSnapTable(prev: SnapTable, curr: SnapTable): TableDiff {
  const pk = guessPk(curr.columns)
  if (!pk) return new Map()
  const prevMap = new Map(prev.rows.map((r) => [String(r[pk] ?? ''), r]))
  const currMap = new Map(curr.rows.map((r) => [String(r[pk] ?? ''), r]))
  const td: TableDiff = new Map()
  for (const [pkv, row] of currMap) {
    if (!prevMap.has(pkv)) {
      td.set(pkv, 'added')
    } else {
      const pr = prevMap.get(pkv)!
      const changed = new Map<string, CellDiff>()
      for (const col of curr.columns) {
        if (JSON.stringify(row[col]) !== JSON.stringify(pr[col])) {
          changed.set(col, { old: pr[col], new: row[col] })
        }
      }
      if (changed.size > 0) td.set(pkv, changed)
    }
  }
  for (const pkv of prevMap.keys()) {
    if (!currMap.has(pkv)) td.set(pkv, 'deleted')
  }
  return td
}

// ─── Component ──────────────────────────────────────────────────────────────

export function EntityExplorer({ domain, onOpenChangeDebugger }: Props) {
  // Entity list
  const [entities, setEntities]         = useState<EntityDef[]>([])
  const [loadingEntities, setLoadingEntities] = useState(false)
  const [selectedEntity, setSelectedEntity]   = useState<EntityDef | null>(null)

  // Instances (root table rows)
  const [instances, setInstances]       = useState<Record<string, unknown>[]>([])
  const [instanceCols, setInstanceCols] = useState<string[]>([])
  const [pkCol, setPkCol]               = useState('')
  const [loadingInst, setLoadingInst]   = useState(false)
  const [instanceSearch, setInstanceSearch] = useState('')
  const [selectedPK, setSelectedPK]     = useState<string | null>(null)

  // Version history
  const [versions, setVersions]         = useState<VersionEntry[]>([])
  const [loadingVer, setLoadingVer]     = useState(false)
  const [selectedVersion, setSelectedVersion] = useState<VersionEntry | null>(null)

  // Snapshot at version
  const [snapTables, setSnapTables]         = useState<SnapTable[]>([])
  const [prevSnapTables, setPrevSnapTables] = useState<SnapTable[]>([])
  const [loadingSnap, setLoadingSnap]       = useState(false)

  const seqRef = useRef(0)

  // ── Load entities ──────────────────────────────────────────────────────────

  const loadEntities = useCallback(async () => {
    setLoadingEntities(true)
    setEntities([])
    setSelectedEntity(null)
    setInstances([])
    setVersions([])
    setSelectedPK(null)
    setSelectedVersion(null)
    setSnapTables([])
    try {
      const resp = await api<{ baseline?: { entities?: EntityDef[] } }>(
        '/api/schema/load-baseline', 'POST', { domain },
      )
      setEntities(resp.baseline?.entities ?? [])
    } catch { /* ignore */ } finally {
      setLoadingEntities(false)
    }
  }, [domain])

  useEffect(() => { loadEntities() }, [loadEntities])

  // ── Load instances ─────────────────────────────────────────────────────────

  const loadInstances = useCallback(async (entity: EntityDef) => {
    setLoadingInst(true)
    setInstances([])
    setInstanceCols([])
    setPkCol('')
    setSelectedPK(null)
    setVersions([])
    setSelectedVersion(null)
    setSnapTables([])
    setInstanceSearch('')
    const seq = ++seqRef.current
    try {
      const resp = await api<{ rows?: Record<string, unknown>[]; columns?: string[] }>(
        '/api/read-query', 'POST', {
          sql: `SELECT * FROM ${entity.root_table} LIMIT 200;`,
          domains: [domain],
          consistency: 'strong',
        },
      )
      if (seq !== seqRef.current) return
      const rows = resp.rows ?? []
      const cols: string[] = resp.columns ??
        (rows.length > 0 ? Object.keys(rows[0]) : [])
      setInstances(rows)
      setInstanceCols(cols)
      // Prefer 'id' as PK column, fall back to first column
      setPkCol(cols.find((c) => c === 'id') ?? cols[0] ?? '')
    } catch { /* ignore */ } finally {
      if (seq === seqRef.current) setLoadingInst(false)
    }
  }, [domain])

  const handleSelectEntity = useCallback((entity: EntityDef) => {
    setSelectedEntity(entity)
    loadInstances(entity)
  }, [loadInstances])

  // ── Load version history ────────────────────────────────────────────────────

  const loadVersions = useCallback(async (entity: EntityDef, rootPK: string) => {
    setLoadingVer(true)
    setVersions([])
    setSelectedVersion(null)
    setSnapTables([])
    try {
      const resp = await api<{ versions?: VersionEntry[] }>(
        '/api/entity-version-history', 'POST', {
          domain,
          entity_name: entity.name,
          root_pk: rootPK,
        },
      )
      setVersions(resp.versions ?? [])
    } catch { /* ignore */ } finally {
      setLoadingVer(false)
    }
  }, [domain])

  const handleSelectInstance = useCallback((row: Record<string, unknown>, col: string) => {
    const pk = String(row[col] ?? '')
    setSelectedVersion(null)
    setSnapTables([])
    setPrevSnapTables([])
    setSelectedPK(pk)
    if (selectedEntity) loadVersions(selectedEntity, pk)
  }, [selectedEntity, loadVersions])

  // Close modal on Escape
  useEffect(() => {
    if (!selectedPK) return
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') setSelectedPK(null) }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [selectedPK])

  // ── Load snapshot at version LSN ─────────────────────────────────────────

  const loadSnapshot = useCallback(async (
    entity: EntityDef,
    lsn: number,
    prevLsn?: number,
  ) => {
    setLoadingSnap(true)
    setSnapTables([])
    setPrevSnapTables([])
    const seq = ++seqRef.current
    try {
      const fetchSnap = (l: number) =>
        Promise.all(
          entity.tables.map((tableName) =>
            api<{ rows?: Record<string, unknown>[]; columns?: string[] }>(
              '/api/time-travel', 'POST', {
                sql: `SELECT * FROM ${tableName} LIMIT 200;`,
                domains: [domain],
                lsn: l,
              },
            ).then((r) => ({
              name: tableName,
              columns: r.columns ??
                (r.rows && r.rows.length > 0 ? Object.keys(r.rows[0]) : []),
              rows: r.rows ?? [],
            })).catch(() => ({ name: tableName, columns: [], rows: [] })),
          ),
        )
      const [curr, prev] = await Promise.all([
        fetchSnap(lsn),
        prevLsn !== undefined ? fetchSnap(prevLsn) : Promise.resolve([] as SnapTable[]),
      ])
      if (seq !== seqRef.current) return
      setSnapTables(curr)
      setPrevSnapTables(prev)
    } catch { /* ignore */ } finally {
      if (seq === seqRef.current) setLoadingSnap(false)
    }
  }, [domain])

  const handleSelectVersion = useCallback((v: VersionEntry) => {
    if (!selectedEntity) return
    setSelectedVersion(v)
    const idx = versions.findIndex((ver) => ver.version === v.version)
    const prevV = idx > 0 ? versions[idx - 1] : undefined
    loadSnapshot(selectedEntity, v.commit_lsn, prevV?.commit_lsn)
  }, [selectedEntity, versions, loadSnapshot])

  // ── Filtered instances ─────────────────────────────────────────────────────

  const filteredInstances = useMemo(() => {
    if (!instanceSearch.trim()) return instances
    const q = instanceSearch.toLowerCase()
    return instances.filter((row) =>
      instanceCols.some((c) => String(row[c] ?? '').toLowerCase().includes(q)),
    )
  }, [instances, instanceCols, instanceSearch])

  // Limit visible columns to keep the table scannable
  const visibleCols = useMemo(() => instanceCols.slice(0, 6), [instanceCols])

  // ── Snapshot diff (current vs previous version) ────────────────────────

  const snapDiff = useMemo((): Map<string, TableDiff> => {
    if (prevSnapTables.length === 0) return new Map()
    const result = new Map<string, TableDiff>()
    for (const ct of snapTables) {
      const pt = prevSnapTables.find((p) => p.name === ct.name)
      if (!pt) continue
      const td = diffSnapTable(pt, ct)
      if (td.size > 0) result.set(ct.name, td)
    }
    return result
  }, [snapTables, prevSnapTables])

  // ─── Render ───────────────────────────────────────────────────────────────

  return (
    <div className="ee-layout">

      {/* ── Sidebar: entity list ───────────────────────────────────────── */}
      <div className="ee-sidebar">
        <div className="ee-sidebar-header">
          <span className="ee-sidebar-title">
            <IconLayers />
            Entities
          </span>
          <button
            className="toolbar-btn icon-only"
            onClick={loadEntities}
            disabled={loadingEntities}
            title="Refresh entity list"
          >
            <IconRefresh />
          </button>
        </div>

        <div className="ee-entity-list">
          {loadingEntities && (
            <div className="ee-placeholder">Loading…</div>
          )}
          {!loadingEntities && entities.length === 0 && (
            <div className="ee-placeholder ee-placeholder-empty">
              <IconLayers />
              <span>No entities defined<br />in domain <strong>{domain}</strong></span>
            </div>
          )}
          {entities.map((e) => (
            <button
              key={e.name}
              className={`ee-entity-item ${selectedEntity?.name === e.name ? 'active' : ''}`}
              onClick={() => handleSelectEntity(e)}
            >
              <span className="ee-entity-name">{e.name}</span>
              <span className="ee-entity-meta">
                <span className="ee-entity-root">{e.root_table}</span>
                <span className="ee-entity-tbadge">{e.tables.length} tables</span>
              </span>
            </button>
          ))}
        </div>
      </div>

      {/* ── Main area (top + bottom split) ────────────────────────────── */}
      <div className="ee-main">
        {!selectedEntity ? (
          <div className="ee-empty-state">
            <IconLayers />
            <p>Select an entity from the sidebar<br />to browse its instances</p>
          </div>
        ) : (
          <>
            {/* ── Top pane: instances ─────────────────────────────────── */}
            <div className="ee-top-pane">
              <div className="ee-panel-header">
                <div className="ee-breadcrumb">
                  <span className="ee-breadcrumb-entity">{selectedEntity.name}</span>
                  <IconChevronRight />
                  <span className="ee-breadcrumb-root">{selectedEntity.root_table}</span>
                  {selectedEntity.tables.length > 1 && (
                    <span className="ee-breadcrumb-extra">
                      +{selectedEntity.tables.length - 1} more tables
                    </span>
                  )}
                </div>
                <div className="ee-panel-actions">
                  <div className="ee-search-wrap">
                    <IconSearch />
                    <input
                      className="ee-search"
                      placeholder="Filter instances…"
                      value={instanceSearch}
                      onChange={(e) => setInstanceSearch(e.target.value)}
                    />
                  </div>
                  <button
                    className="toolbar-btn icon-only"
                    onClick={() => loadInstances(selectedEntity)}
                    disabled={loadingInst}
                    title="Refresh"
                  >
                    <IconRefresh />
                  </button>
                </div>
              </div>

              {loadingInst ? (
                <div className="ee-placeholder">Loading instances…</div>
              ) : filteredInstances.length === 0 ? (
                <div className="ee-placeholder">
                  {instanceSearch ? 'No matching instances' : 'No rows found'}
                </div>
              ) : (
                <div className="ee-table-wrap">
                  <table className="ee-table">
                    <thead>
                      <tr>
                        {visibleCols.map((c) => (
                          <th key={c} className={c === pkCol ? 'ee-th-pk' : ''}>{c}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {filteredInstances.map((row, i) => {
                        const pk = String(row[pkCol] ?? '')
                        const isSelected = pk === selectedPK
                        return (
                          <tr
                            key={i}
                            className={isSelected ? 'ee-row-selected' : ''}
                            onClick={() => handleSelectInstance(row, pkCol)}
                          >
                            {visibleCols.map((c) => (
                              <td key={c} className="mono">{renderCell(row[c])}</td>
                            ))}
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              )}

              {!loadingInst && instances.length > 0 && (
                <div className="ee-table-footer">
                  {filteredInstances.length} of {instances.length} instances
                  {instances.length === 200 && <span className="text-muted"> (limit 200)</span>}
                  {!selectedPK && <span className="ee-footer-hint"> · click a row to explore versions</span>}
                </div>
              )}
            </div>

          </>
        )}
      </div>

      {/* ── Detail modal ─────────────────────────────────────────────── */}
      {selectedPK && selectedEntity && (
        <div className="ee-modal-backdrop" onClick={() => setSelectedPK(null)}>
          <div className="ee-modal" onClick={(e) => e.stopPropagation()}>

            {/* Modal header */}
            <div className="ee-modal-header">
              <div className="ee-modal-title">
                <span className="ee-ver-entity-label">{selectedEntity.name}</span>
                <span className="ee-modal-pk mono" title={selectedPK}>pk: {truncate(selectedPK, 48)}</span>
              </div>
              <div className="ee-modal-header-right">
                <button
                  className="toolbar-btn"
                  onClick={() => onOpenChangeDebugger?.(selectedEntity.name, selectedPK)}
                  title="Open change stream debugger for this entity root"
                >
                  <IconLayers />
                  Stream Debugger
                </button>
                {!loadingSnap && selectedVersion && prevSnapTables.length > 0 && (() => {
                  const added   = [...snapDiff.values()].reduce((n, td) => n + [...td.values()].filter((v) => v === 'added').length, 0)
                  const deleted = [...snapDiff.values()].reduce((n, td) => n + [...td.values()].filter((v) => v === 'deleted').length, 0)
                  const changed = [...snapDiff.values()].reduce((n, td) => n + [...td.values()].filter((v) => v instanceof Map).length, 0)
                  if (added + deleted + changed === 0)
                    return <span className="ee-diff-none">no changes vs v{selectedVersion.version - 1}</span>
                  return (
                    <span className="ee-diff-summary">
                      {added   > 0 && <span className="ee-diff-added">+{added} added</span>}
                      {changed > 0 && <span className="ee-diff-changed">~{changed} changed</span>}
                      {deleted > 0 && <span className="ee-diff-deleted">−{deleted} deleted</span>}
                    </span>
                  )
                })()}
                <button
                  className="ee-modal-close"
                  onClick={() => setSelectedPK(null)}
                  title="Close (Esc)"
                >
                  ✕
                </button>
              </div>
            </div>

            {/* Modal body: version list + snapshot */}
            <div className="ee-modal-body">

              {/* Left: version timeline */}
              <div className="ee-ver-col">
                <div className="ee-ver-col-header">
                  <div className="ee-ver-col-title">
                    <span style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.06em', color: 'var(--text-muted)' }}>Version Timeline</span>
                    {!loadingVer && versions.length > 0 && (
                      <span className="ee-ver-count">{versions.length} versions</span>
                    )}
                  </div>
                </div>

                {loadingVer ? (
                  <div className="ee-placeholder">Loading versions…</div>
                ) : versions.length === 0 ? (
                  <div className="ee-bottom-empty"><span>No versions recorded</span></div>
                ) : (
                  <div className="ee-ver-list">
                    {[...versions].reverse().map((v) => (
                      <button
                        key={v.version}
                        className={`ee-ver-entry ${selectedVersion?.version === v.version ? 'active' : ''}`}
                        onClick={() => handleSelectVersion(v)}
                      >
                        <div className="ee-ver-entry-top">
                          <span className="ee-ver-badge">v{v.version}</span>
                          <span className="ee-ver-lsn mono">LSN {v.commit_lsn.toLocaleString()}</span>
                        </div>
                        <div className="ee-ver-tables">
                          {v.tables.map((t) => (
                            <span key={t} className="ee-ver-tag"><IconTable />{t}</span>
                          ))}
                        </div>
                      </button>
                    ))}
                  </div>
                )}
              </div>

              {/* Right: snapshot at selected version */}
              <div className="ee-snap-col">
                {!selectedVersion ? (
                  <div className="ee-bottom-empty"><span>Select a version to view the entity snapshot</span></div>
                ) : (
                  <>
                    <div className="ee-snap-col-header">
                      <span>
                        Snapshot at <strong>v{selectedVersion.version}</strong>
                        <span className="ee-snap-lsn mono"> · LSN {selectedVersion.commit_lsn.toLocaleString()}</span>
                      </span>
                      {loadingSnap && <span className="text-muted ee-snap-loading">Loading…</span>}
                    </div>
                    <div className="ee-snap-body">
                      {!loadingSnap && snapTables.map((t) => {
                        const tableDiff   = snapDiff.get(t.name)
                        const pk          = guessPk(t.columns)
                        const prevTable   = prevSnapTables.find((p) => p.name === t.name)
                        const deletedRows = tableDiff
                          ? (prevTable?.rows ?? []).filter(
                              (r) => tableDiff.get(String(r[pk] ?? '')) === 'deleted',
                            )
                          : []
                        return (
                          <div key={t.name} className="ee-snap-table-block">
                            <div className="ee-snap-table-name">
                              <IconTable />{t.name}
                              <span className="ee-snap-row-count">{t.rows.length} rows</span>
                              {tableDiff && tableDiff.size > 0 && (
                                <span className="ee-diff-badge">{tableDiff.size} change{tableDiff.size > 1 ? 's' : ''}</span>
                              )}
                            </div>
                            {t.columns.length === 0 ? (
                              <div className="ee-placeholder">No data at this LSN</div>
                            ) : (
                              <div className="ee-snap-table-wrap">
                                <table className="ee-table ee-snap-table">
                                  <thead><tr>
                                    {t.columns.map((c) => <th key={c}>{c}</th>)}
                                  </tr></thead>
                                  <tbody>
                                    {t.rows.map((row, i) => {
                                      const pkv     = String(row[pk] ?? '')
                                      const rowDiff = tableDiff?.get(pkv)
                                      const rowCls  = rowDiff === 'added' ? 'ee-row-added'
                                        : rowDiff instanceof Map ? 'ee-row-changed' : ''
                                      return (
                                        <tr key={i} className={rowCls}>
                                          {t.columns.map((c) => {
                                            const cellDiff = rowDiff instanceof Map ? rowDiff.get(c) : undefined
                                            return (
                                              <td
                                                key={c}
                                                className={`mono${cellDiff ? ' ee-cell-changed' : ''}`}
                                                title={cellDiff ? `was: ${String(cellDiff.old ?? 'NULL')}` : undefined}
                                              >
                                                {renderCell(row[c])}
                                                {cellDiff && (
                                                  <span className="ee-cell-old">{String(cellDiff.old ?? 'NULL')}</span>
                                                )}
                                              </td>
                                            )
                                          })}
                                        </tr>
                                      )
                                    })}
                                    {deletedRows.map((row, i) => (
                                      <tr key={`del-${i}`} className="ee-row-deleted">
                                        {t.columns.map((c) => (
                                          <td key={c} className="mono">{renderCell(row[c])}</td>
                                        ))}
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            )}
                          </div>
                        )
                      })}
                    </div>
                  </>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
