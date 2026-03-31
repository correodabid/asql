import { useEffect, useMemo, useRef, useState } from 'react'
import { StartEntityChangeStream, StopEntityChangeStream } from '../wailsjs/wailsjs/go/studioapp/App'
import { EventsOn } from '../wailsjs/wailsjs/runtime/runtime'
import { ALL_DOMAINS_KEY } from '../hooks/useSchemaStudio'
import { useSchemaCache } from '../hooks/useSchemaCache'
import { api } from '../lib/api'
import { IconActivity, IconAlertTriangle, IconClock, IconLayers, IconPause, IconPlay, IconRefresh, IconSearch, IconSkipForward, IconTable } from './Icons'

type Props = {
  domain: string
  preset?: {
    entityName: string
    rootPK?: string
    token: number
  } | null
}

type EntityChangeRow = {
  key: string
  commit_lsn: number
  commit_timestamp: string | number
  domain: string
  entity: string
  root_pk: string
  entity_version: number
  tables: string[]
}

type ReadQueryResponse = {
  status: string
  rows?: Record<string, unknown>[]
  as_of_lsn?: number
}

type StreamStartResponse = {
  status: string
  stream_id: string
  event_name: string
  sql: string
}

type StreamEvent = {
  kind: 'started' | 'row' | 'error' | 'end' | 'stopped'
  stream_id?: string
  row?: Record<string, unknown>
  error?: string
  sql?: string
}

type SnapTable = {
  name: string
  columns: string[]
  rows: Record<string, unknown>[]
}

type CellDiff = { old: unknown; new: unknown }
type RowDiff = 'added' | 'deleted' | Map<string, CellDiff>
type TableDiff = Map<string, RowDiff>

function toUint(value: unknown): number {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : 0
}

function toText(value: unknown): string {
  if (value == null) return ''
  return String(value)
}

function toTables(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((entry) => String(entry))
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) return []
    if (trimmed.startsWith('[')) {
      try {
        const parsed = JSON.parse(trimmed)
        if (Array.isArray(parsed)) {
          return parsed.map((entry) => String(entry))
        }
      } catch {
        // Fall through to comma-split handling.
      }
    }
  }
  const text = toText(value).trim()
  if (!text) return []
  return text.split(',').map((entry) => entry.trim()).filter(Boolean)
}

function isMeaningfulEvent(row: Record<string, unknown>): boolean {
  return toUint(row.commit_lsn) > 0
    && toUint(row.entity_version) > 0
    && toText(row.domain).trim() !== ''
    && toText(row.entity).trim() !== ''
    && toText(row.root_pk).trim() !== ''
}

function quoteIdentifier(value: string): string {
  if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) return value
  return `"${value.replace(/"/g, '""')}"`
}

function quoteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`
}

function normalizeEvent(row: Record<string, unknown>): EntityChangeRow {
  const commitLSN = toUint(row.commit_lsn)
  const rootPK = toText(row.root_pk)
  const version = toUint(row.entity_version)
  return {
    key: `${commitLSN}:${rootPK}:${version}`,
    commit_lsn: commitLSN,
    commit_timestamp: typeof row.commit_timestamp === 'number' ? row.commit_timestamp : toText(row.commit_timestamp),
    domain: toText(row.domain),
    entity: toText(row.entity),
    root_pk: rootPK,
    entity_version: version,
    tables: toTables(row.tables),
  }
}

function formatTimestamp(value: string | number): string {
  if (!value) return 'n/a'
  const parsed = typeof value === 'number'
    ? new Date(value / 1000)
    : new Date(value)
  if (Number.isNaN(parsed.getTime())) return String(value)
  return parsed.toLocaleString()
}

function guessPk(columns: string[]): string {
  return columns.find((column) => column === 'id') ?? columns.find((column) => /^.+_id$/.test(column)) ?? columns[0] ?? ''
}

function diffSnapTable(prev: SnapTable, current: SnapTable): TableDiff {
  const pk = guessPk(current.columns)
  if (!pk) return new Map()
  const prevMap = new Map(prev.rows.map((row) => [String(row[pk] ?? ''), row]))
  const currentMap = new Map(current.rows.map((row) => [String(row[pk] ?? ''), row]))
  const diff: TableDiff = new Map()
  for (const [pkValue, row] of currentMap) {
    if (!prevMap.has(pkValue)) {
      diff.set(pkValue, 'added')
      continue
    }
    const prevRow = prevMap.get(pkValue)!
    const changed = new Map<string, CellDiff>()
    for (const column of current.columns) {
      if (JSON.stringify(row[column]) !== JSON.stringify(prevRow[column])) {
        changed.set(column, { old: prevRow[column], new: row[column] })
      }
    }
    if (changed.size > 0) {
      diff.set(pkValue, changed)
    }
  }
  for (const pkValue of prevMap.keys()) {
    if (!currentMap.has(pkValue)) {
      diff.set(pkValue, 'deleted')
    }
  }
  return diff
}

function renderValue(value: unknown): string {
  if (value == null) return 'NULL'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

export function EntityChangeDebugger({ domain, preset }: Props) {
  const schema = useSchemaCache(domain)
  const entities = schema.baseline?.entities ?? []
  const [entityName, setEntityName] = useState('')
  const [rootPK, setRootPK] = useState('')
  const [fromLSNInput, setFromLSNInput] = useState('0')
  const [toLSNInput, setToLSNInput] = useState('')
  const [batchSizeInput, setBatchSizeInput] = useState('50')
  const [events, setEvents] = useState<EntityChangeRow[]>([])
  const [selectedKey, setSelectedKey] = useState('')
  const [loading, setLoading] = useState(false)
  const [following, setFollowing] = useState(false)
  const [error, setError] = useState('')
  const [resumeLSN, setResumeLSN] = useState(0)
  const [observedHeadLSN, setObservedHeadLSN] = useState(0)
  const [lastEventAt, setLastEventAt] = useState(0)
  const [streamSQL, setStreamSQL] = useState('')
  const [snapTables, setSnapTables] = useState<SnapTable[]>([])
  const [prevSnapTables, setPrevSnapTables] = useState<SnapTable[]>([])
  const [loadingDiff, setLoadingDiff] = useState(false)
  const activeStreamRef = useRef<{ streamId: string; eventName: string } | null>(null)
  const stopListenerRef = useRef<(() => void) | null>(null)
  const resumeRef = useRef(0)

  const selectedEvent = useMemo(
    () => events.find((event) => event.key === selectedKey) ?? null,
    [events, selectedKey],
  )

  const selectedEntity = useMemo(
    () => entities.find((entity) => entity.name === entityName) ?? null,
    [entities, entityName],
  )

  const previousComparableEvent = useMemo(() => {
    if (!selectedEvent) return null
    const currentIndex = events.findIndex((event) => event.key === selectedEvent.key)
    if (currentIndex <= 0) return null
    for (let index = currentIndex - 1; index >= 0; index -= 1) {
      const candidate = events[index]
      if (candidate.entity === selectedEvent.entity && candidate.root_pk === selectedEvent.root_pk) {
        return candidate
      }
    }
    return null
  }, [events, selectedEvent])

  const fromLSN = useMemo(() => Math.max(0, toUint(fromLSNInput)), [fromLSNInput])
  const toLSN = useMemo(() => Math.max(0, toUint(toLSNInput)), [toLSNInput])
  const batchSize = useMemo(() => Math.max(1, toUint(batchSizeInput) || 50), [batchSizeInput])

  const diffByTable = useMemo(() => {
    const next = new Map<string, TableDiff>()
    if (snapTables.length === 0 || prevSnapTables.length === 0) return next
    for (const table of snapTables) {
      const previous = prevSnapTables.find((candidate) => candidate.name === table.name)
      if (!previous) continue
      const diff = diffSnapTable(previous, table)
      if (diff.size > 0) {
        next.set(table.name, diff)
      }
    }
    return next
  }, [snapTables, prevSnapTables])

  const diffSummary = useMemo(() => {
    let added = 0
    let deleted = 0
    let changed = 0
    for (const tableDiff of diffByTable.values()) {
      for (const value of tableDiff.values()) {
        if (value === 'added') added += 1
        else if (value === 'deleted') deleted += 1
        else changed += 1
      }
    }
    return { added, deleted, changed }
  }, [diffByTable])

  const queryPreview = useMemo(() => {
    if (!selectedEntity || domain === ALL_DOMAINS_KEY) return ''
    const parts = ['TAIL ENTITY CHANGES', `${quoteIdentifier(domain)}.${quoteIdentifier(selectedEntity.name)}`]
    if (rootPK.trim()) {
      parts.push('FOR', quoteLiteral(rootPK.trim()))
    }
    if (fromLSN > 0) {
      parts.push('FROM LSN', String(fromLSN))
    }
    if (toLSN > 0) {
      parts.push('TO LSN', String(toLSN))
    }
    parts.push('LIMIT', String(batchSize))
    return parts.join(' ')
  }, [batchSize, domain, fromLSN, rootPK, selectedEntity, toLSN])

  useEffect(() => {
    void schema.loadBaseline()
  }, [schema, domain])

  useEffect(() => {
    if (!entityName && entities.length > 0) {
      setEntityName(entities[0].name)
    }
  }, [entities, entityName])

  useEffect(() => {
    if (!preset || domain === ALL_DOMAINS_KEY) return
    setEntityName(preset.entityName)
    setRootPK(preset.rootPK ?? '')
    setSelectedKey('')
  }, [preset, domain])

  useEffect(() => {
    resumeRef.current = resumeLSN
  }, [resumeLSN])

  useEffect(() => {
    return () => {
      if (stopListenerRef.current) {
        stopListenerRef.current()
        stopListenerRef.current = null
      }
      const stream = activeStreamRef.current
      if (stream) {
        void StopEntityChangeStream({ stream_id: stream.streamId })
      }
    }
  }, [])

  useEffect(() => {
    if (!selectedEntity || !selectedEvent || !previousComparableEvent) {
      setSnapTables([])
      setPrevSnapTables([])
      return
    }
    let cancelled = false
    const fetchSnapshots = async () => {
      setLoadingDiff(true)
      try {
        const fetchTables = async (lsn: number) => {
          const snapshots = await Promise.all(
            selectedEntity.tables.map(async (table) => {
              const response = await api<{ rows?: Record<string, unknown>[]; columns?: string[] }>('/api/time-travel', 'POST', {
                sql: `SELECT * FROM ${quoteIdentifier(table)} LIMIT 200;`,
                domains: [domain],
                lsn,
              })
              const rows = response.rows ?? []
              const columns = response.columns ?? (rows.length > 0 ? Object.keys(rows[0]) : [])
              return { name: table, columns, rows }
            }),
          )
          return snapshots
        }

        const [current, previous] = await Promise.all([
          fetchTables(selectedEvent.commit_lsn),
          fetchTables(previousComparableEvent.commit_lsn),
        ])
        if (cancelled) return
        setSnapTables(current)
        setPrevSnapTables(previous)
      } catch (snapshotError) {
        if (!cancelled) {
          setError(snapshotError instanceof Error ? snapshotError.message : String(snapshotError))
          setSnapTables([])
          setPrevSnapTables([])
        }
      } finally {
        if (!cancelled) setLoadingDiff(false)
      }
    }

    void fetchSnapshots()
    return () => {
      cancelled = true
    }
  }, [domain, previousComparableEvent, selectedEntity, selectedEvent])

  const stopFollowing = async () => {
    setFollowing(false)
    if (stopListenerRef.current) {
      stopListenerRef.current()
      stopListenerRef.current = null
    }
    const stream = activeStreamRef.current
    activeStreamRef.current = null
    if (stream) {
      try {
        await StopEntityChangeStream({ stream_id: stream.streamId })
      } catch {
        // best effort
      }
    }
  }

  const runFetch = async (mode: 'reset' | 'resume') => {
    if (!selectedEntity) {
      setError('Select an entity before running the debugger.')
      return
    }
    if (domain === ALL_DOMAINS_KEY) {
      setError('Change stream debugging requires a concrete domain.')
      return
    }

    const effectiveFrom = mode === 'reset'
      ? fromLSN
      : Math.max(fromLSN, resumeRef.current > 0 ? resumeRef.current + 1 : 0)

    setLoading(true)
    setError('')
    try {
      const response = await api<ReadQueryResponse>('/api/read-query', 'POST', {
        sql: [
          'TAIL ENTITY CHANGES',
          `${quoteIdentifier(domain)}.${quoteIdentifier(selectedEntity.name)}`,
          ...(rootPK.trim() ? ['FOR', quoteLiteral(rootPK.trim())] : []),
          ...(effectiveFrom > 0 ? ['FROM LSN', String(effectiveFrom)] : []),
          ...(toLSN > 0 ? ['TO LSN', String(toLSN)] : []),
          'LIMIT',
          String(batchSize),
        ].join(' '),
        domains: [domain],
        consistency: 'strong',
      })
      const nextEvents = (response.rows ?? []).filter(isMeaningfulEvent).map(normalizeEvent)
      setObservedHeadLSN(response.as_of_lsn ?? 0)
      setLastEventAt(Date.now())
      setStreamSQL('')
      if (mode === 'reset') {
        setEvents(nextEvents)
        setSelectedKey(nextEvents[nextEvents.length - 1]?.key ?? '')
      } else {
        setEvents((current) => {
          const seen = new Set(current.map((event) => event.key))
          const merged = [...current]
          for (const event of nextEvents) {
            if (!seen.has(event.key)) {
              merged.push(event)
            }
          }
          return merged
        })
        if (nextEvents.length > 0) {
          setSelectedKey(nextEvents[nextEvents.length - 1].key)
        }
      }
      if (nextEvents.length > 0) {
        setResumeLSN(nextEvents[nextEvents.length - 1].commit_lsn)
      } else if (mode === 'reset') {
        setResumeLSN(Math.max(0, effectiveFrom - 1))
      }
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : String(requestError))
    } finally {
      setLoading(false)
    }
  }

  const handleReset = async () => {
    await stopFollowing()
    setResumeLSN(0)
    setObservedHeadLSN(0)
    setStreamSQL('')
    await runFetch('reset')
  }

  const handleResume = async () => {
    await stopFollowing()
    await runFetch('resume')
  }

  const handleStartFollowing = async () => {
    if (!selectedEntity) {
      setError('Select an entity before running the debugger.')
      return
    }
    await stopFollowing()
    setError('')

    if (events.length === 0) {
      await runFetch('reset')
    }

    const response = await StartEntityChangeStream({
      domain,
      entity_name: selectedEntity.name,
      root_pk: rootPK.trim(),
      from_lsn: Math.max(fromLSN, resumeRef.current > 0 ? resumeRef.current + 1 : 0),
      to_lsn: toLSN,
    }) as StreamStartResponse

    setFollowing(true)
    setStreamSQL(response.sql)
    activeStreamRef.current = { streamId: response.stream_id, eventName: response.event_name }
    stopListenerRef.current = EventsOn(response.event_name, (payload: StreamEvent) => {
      if (!payload) return
      if (payload.kind === 'row' && payload.row) {
        if (!isMeaningfulEvent(payload.row)) {
          return
        }
        const event = normalizeEvent(payload.row)
        setEvents((current) => {
          if (current.some((entry) => entry.key === event.key)) {
            return current
          }
          return [...current, event]
        })
        setSelectedKey(event.key)
        setResumeLSN(event.commit_lsn)
        setObservedHeadLSN((current) => Math.max(current, event.commit_lsn))
        setLastEventAt(Date.now())
        return
      }
      if (payload.kind === 'error') {
        setError(payload.error ?? 'Entity change stream failed')
        void stopFollowing()
        return
      }
      if (payload.kind === 'end' || payload.kind === 'stopped') {
        void stopFollowing()
      }
    })
  }

  const handleClear = async () => {
    await stopFollowing()
    setEvents([])
    setSelectedKey('')
    setResumeLSN(0)
    setObservedHeadLSN(0)
    setLastEventAt(0)
    setError('')
    setStreamSQL('')
    setSnapTables([])
    setPrevSnapTables([])
  }

  if (domain === ALL_DOMAINS_KEY) {
    return (
      <div className="ecd-layout ecd-layout-empty">
        <div className="ecd-empty-card">
          <IconAlertTriangle />
          <h3>Change stream debugger needs one domain</h3>
          <p>Select a concrete domain to build `TAIL ENTITY CHANGES` queries and inspect resume behavior.</p>
        </div>
      </div>
    )
  }

  return (
    <div className="ecd-layout">
      <aside className="ecd-sidebar">
        <div className="ecd-sidebar-header">
          <div>
            <div className="ecd-eyebrow">Temporal Debugging</div>
            <h2>Entity Change Stream</h2>
          </div>
          <button className="toolbar-btn icon-only" onClick={() => void schema.loadBaseline()} title="Reload entity catalog">
            <IconRefresh />
          </button>
        </div>

        <label className="ecd-field">
          <span>Entity</span>
          <select value={entityName} onChange={(event) => setEntityName(event.target.value)}>
            {entities.length === 0 && <option value="">No entities</option>}
            {entities.map((entity) => (
              <option key={entity.name} value={entity.name}>{entity.name}</option>
            ))}
          </select>
        </label>

        {selectedEntity && (
          <div className="ecd-entity-meta">
            <div><span>Root table</span><strong>{selectedEntity.root_table}</strong></div>
            <div><span>Tables</span><strong>{selectedEntity.tables.join(', ')}</strong></div>
          </div>
        )}

        <label className="ecd-field">
          <span>Root PK filter</span>
          <input value={rootPK} onChange={(event) => setRootPK(event.target.value)} placeholder="Optional aggregate root" />
        </label>

        <div className="ecd-field-grid">
          <label className="ecd-field">
            <span>From LSN</span>
            <input value={fromLSNInput} onChange={(event) => setFromLSNInput(event.target.value)} inputMode="numeric" />
          </label>
          <label className="ecd-field">
            <span>To LSN</span>
            <input value={toLSNInput} onChange={(event) => setToLSNInput(event.target.value)} inputMode="numeric" placeholder="Open" />
          </label>
        </div>

        <label className="ecd-field">
          <span>Backlog limit</span>
          <input value={batchSizeInput} onChange={(event) => setBatchSizeInput(event.target.value)} inputMode="numeric" />
        </label>

        <div className="ecd-preview">
          <div className="ecd-preview-label">Backlog query</div>
          <code>{queryPreview || 'Select an entity to generate a TAIL query.'}</code>
          {streamSQL && (
            <>
              <div className="ecd-preview-label ecd-preview-secondary">Live stream</div>
              <code>{streamSQL}</code>
            </>
          )}
        </div>

        <div className="ecd-actions">
          <button className="toolbar-btn primary" onClick={() => void handleReset()} disabled={loading || !selectedEntity}>
            <IconSearch />
            Backlog
          </button>
          <button className="toolbar-btn" onClick={() => void handleResume()} disabled={loading || !selectedEntity}>
            <IconSkipForward />
            Resume
          </button>
          {!following ? (
            <button className="toolbar-btn" onClick={() => void handleStartFollowing()} disabled={loading || !selectedEntity}>
              <IconPlay />
              Follow
            </button>
          ) : (
            <button className="toolbar-btn" onClick={() => void stopFollowing()}>
              <IconPause />
              Stop
            </button>
          )}
          <button className="toolbar-btn" onClick={() => void handleClear()}>
            Clear
          </button>
        </div>

        <div className="ecd-stats-grid">
          <div className="ecd-stat-card">
            <span>Resume token</span>
            <strong>{resumeLSN || '0'}</strong>
          </div>
          <div className="ecd-stat-card">
            <span>Observed head</span>
            <strong>{observedHeadLSN || '0'}</strong>
          </div>
          <div className="ecd-stat-card">
            <span>Rows collected</span>
            <strong>{events.length}</strong>
          </div>
          <div className="ecd-stat-card">
            <span>Selected version</span>
            <strong>{selectedEvent?.entity_version ?? 'n/a'}</strong>
          </div>
        </div>

        <div className="ecd-footnote">
          <IconClock />
          <span>
            {following ? 'Live push stream over Wails events' : 'Idle'}
            {lastEventAt > 0 ? ` · last event ${new Date(lastEventAt).toLocaleTimeString()}` : ''}
          </span>
        </div>
      </aside>

      <section className="ecd-main">
        {error && <div className="ecd-banner ecd-banner-error">{error}</div>}

        <div className="ecd-results-header">
          <div>
            <div className="ecd-eyebrow">Event feed</div>
            <h3>{selectedEntity ? `${domain}.${selectedEntity.name}` : 'No entity selected'}</h3>
          </div>
          <div className={`ecd-live-badge${following ? ' live' : ''}`}>
            <IconActivity />
            {following ? 'Following' : 'Idle'}
          </div>
        </div>

        <div className="ecd-results-grid">
          <div className="ecd-table-wrap">
            <table className="ecd-table">
              <thead>
                <tr>
                  <th>LSN</th>
                  <th>Timestamp</th>
                  <th>Root PK</th>
                  <th>Version</th>
                  <th>Tables</th>
                </tr>
              </thead>
              <tbody>
                {events.length === 0 && (
                  <tr>
                    <td colSpan={5} className="ecd-empty-row">
                      <IconLayers />
                      <span>No change rows collected yet.</span>
                    </td>
                  </tr>
                )}
                {events.map((event) => (
                  <tr key={event.key} className={event.key === selectedKey ? 'selected' : ''} onClick={() => setSelectedKey(event.key)}>
                    <td className="mono">{event.commit_lsn}</td>
                    <td>{formatTimestamp(event.commit_timestamp)}</td>
                    <td className="mono">{event.root_pk}</td>
                    <td>{event.entity_version}</td>
                    <td>{event.tables.join(', ') || 'n/a'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="ecd-detail-card">
            <div className="ecd-detail-header">
              <span>Selected event</span>
              {selectedEvent && <strong className="mono">LSN {selectedEvent.commit_lsn}</strong>}
            </div>
            {!selectedEvent ? (
              <div className="ecd-detail-empty">Pick a row to inspect its entity transition metadata.</div>
            ) : (
              <>
                <dl className="ecd-detail-list">
                  <div><dt>Domain</dt><dd>{selectedEvent.domain}</dd></div>
                  <div><dt>Entity</dt><dd>{selectedEvent.entity}</dd></div>
                  <div><dt>Root PK</dt><dd className="mono">{selectedEvent.root_pk}</dd></div>
                  <div><dt>Entity version</dt><dd>{selectedEvent.entity_version}</dd></div>
                  <div><dt>Commit timestamp</dt><dd>{formatTimestamp(selectedEvent.commit_timestamp)}</dd></div>
                </dl>
                <div className="ecd-detail-section">
                  <span>Impacted tables</span>
                  <div className="ecd-table-chip-row">
                    {selectedEvent.tables.map((table) => (
                      <span key={table} className="ecd-table-chip">{table}</span>
                    ))}
                    {selectedEvent.tables.length === 0 && <span className="ecd-table-chip muted">n/a</span>}
                  </div>
                </div>
                <div className="ecd-detail-section">
                  <span>Resume next from</span>
                  <code>{selectedEvent.commit_lsn + 1}</code>
                </div>
              </>
            )}
          </div>
        </div>

        <div className="ecd-diff-panel">
          <div className="ecd-diff-header">
            <div>
              <div className="ecd-eyebrow">Consecutive aggregate diff</div>
              <h3>{previousComparableEvent ? `v${previousComparableEvent.entity_version} -> v${selectedEvent?.entity_version ?? 'n/a'}` : 'No previous event for this root'}</h3>
            </div>
            {previousComparableEvent && (
              <div className="ecd-diff-summary">
                {diffSummary.added > 0 && <span className="ecd-diff-added">+{diffSummary.added} added</span>}
                {diffSummary.changed > 0 && <span className="ecd-diff-changed">~{diffSummary.changed} changed</span>}
                {diffSummary.deleted > 0 && <span className="ecd-diff-deleted">-{diffSummary.deleted} deleted</span>}
                {diffSummary.added + diffSummary.changed + diffSummary.deleted === 0 && !loadingDiff && <span className="ecd-diff-none">No row-level diff</span>}
              </div>
            )}
          </div>

          {!selectedEvent ? (
            <div className="ecd-diff-empty">Select an event to compare it with the previous event for the same entity root.</div>
          ) : !previousComparableEvent ? (
            <div className="ecd-diff-empty">This is the first visible event for this root PK in the current feed.</div>
          ) : loadingDiff ? (
            <div className="ecd-diff-empty">Loading snapshots…</div>
          ) : (
            <div className="ecd-snap-body">
              {snapTables.map((table) => {
                const tableDiff = diffByTable.get(table.name)
                const pk = guessPk(table.columns)
                const previousTable = prevSnapTables.find((candidate) => candidate.name === table.name)
                const deletedRows = tableDiff
                  ? (previousTable?.rows ?? []).filter((row) => tableDiff.get(String(row[pk] ?? '')) === 'deleted')
                  : []
                return (
                  <div key={table.name} className="ecd-snap-table-block">
                    <div className="ecd-snap-table-name">
                      <span><IconTable />{table.name}</span>
                      <span className="ecd-snap-row-count">{table.rows.length} rows</span>
                      {tableDiff && tableDiff.size > 0 && (
                        <span className="ecd-diff-badge">{tableDiff.size} change{tableDiff.size > 1 ? 's' : ''}</span>
                      )}
                    </div>
                    {table.columns.length === 0 ? (
                      <div className="ecd-diff-empty">No rows at this LSN.</div>
                    ) : (
                      <div className="ecd-snap-table-wrap">
                        <table className="ecd-table ecd-snap-table">
                          <thead>
                            <tr>
                              {table.columns.map((column) => <th key={column}>{column}</th>)}
                            </tr>
                          </thead>
                          <tbody>
                            {table.rows.map((row, index) => {
                              const pkValue = String(row[pk] ?? '')
                              const rowDiff = tableDiff?.get(pkValue)
                              const rowClass = rowDiff === 'added' ? 'ecd-row-added' : rowDiff instanceof Map ? 'ecd-row-changed' : ''
                              return (
                                <tr key={index} className={rowClass}>
                                  {table.columns.map((column) => {
                                    const cellDiff = rowDiff instanceof Map ? rowDiff.get(column) : undefined
                                    return (
                                      <td key={column} className={`mono${cellDiff ? ' ecd-cell-changed' : ''}`}>
                                        <span>{renderValue(row[column])}</span>
                                        {cellDiff && <span className="ecd-cell-old">was {renderValue(cellDiff.old)}</span>}
                                      </td>
                                    )
                                  })}
                                </tr>
                              )
                            })}
                            {deletedRows.map((row, index) => (
                              <tr key={`deleted-${index}`} className="ecd-row-deleted">
                                {table.columns.map((column) => (
                                  <td key={column} className="mono">{renderValue(row[column])}</td>
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
          )}
        </div>
      </section>
    </div>
  )
}