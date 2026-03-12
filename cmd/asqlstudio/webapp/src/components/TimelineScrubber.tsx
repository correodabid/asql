import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from '../lib/api'
import {
  IconRefresh,
  IconPlay,
  IconPause,
  IconSkipBack,
  IconSkipForward,
  IconChevronLeft,
  IconChevronRight,
  IconZoomOut,
  IconClock,
} from './Icons'

// ─── Types ──────────────────────────────────────────────────────────────────

type TimelineCommitMutation = {
  domain: string
  table: string
  operation: string
}

type TimelineCommit = {
  lsn: number
  tx_id: string
  timestamp: number
  tables: TimelineCommitMutation[]
}

type SnapshotPoint = { lsn: number }

type TimelineEvents = {
  memory_snapshots: SnapshotPoint[]
  disk_snapshots: SnapshotPoint[]
}

type HoveredItem =
  | { kind: 'commit'; commit: TimelineCommit }
  | { kind: 'snapshot'; lsn: number; source: 'memory' | 'disk' }

type Props = {
  maxLSN: number
  currentLSN: number
  domain: string
  onScrub: (lsn: number) => void
  onRefresh: () => void
}

// ─── Constants ──────────────────────────────────────────────────────────────

const DENSITY_BUCKETS = 80
const TOOLTIP_DELAY_MS = 180
const DDL_OPS = new Set(['create_table', 'drop_table', 'alter_table', 'create_index', 'drop_index'])

const PLAYBACK_SPEEDS: { label: string; ms: number }[] = [
  { label: '0.5×', ms: 800 },
  { label: '1×', ms: 400 },
  { label: '2×', ms: 200 },
  { label: '4×', ms: 100 },
]
const DEFAULT_SPEED_IDX = 1

// ─── Helpers ────────────────────────────────────────────────────────────────

function formatTimestamp(ts: number): string {
  if (!ts) return ''
  const d = new Date(ts / 1_000_000)
  if (!isNaN(d.getTime()) && d.getFullYear() > 2000) {
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  }
  const d2 = new Date(ts)
  return isNaN(d2.getTime()) ? '' : d2.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

function isDDLCommit(c: TimelineCommit): boolean {
  return c.tables?.some(t => DDL_OPS.has(t.operation)) ?? false
}

function opBadgeClass(op: string): string {
  switch (op.toLowerCase()) {
    case 'insert': return 'tl-op-insert'
    case 'update': return 'tl-op-update'
    case 'delete': return 'tl-op-delete'
    case 'create_table': return 'tl-op-ddl-create'
    case 'drop_table': return 'tl-op-ddl-drop'
    case 'alter_table': return 'tl-op-ddl-alter'
    default: return 'tl-op-other'
  }
}

function ddlVerb(op: string): string {
  switch (op) {
    case 'create_table': return 'CREATE TABLE'
    case 'drop_table': return 'DROP TABLE'
    case 'alter_table': return 'ALTER TABLE'
    case 'create_index': return 'CREATE INDEX'
    case 'drop_index': return 'DROP INDEX'
    default: return op.replace('_', ' ').toUpperCase()
  }
}

function generateTicks(start: number, end: number): number[] {
  const span = end - start
  if (span <= 0) return []
  const count = Math.min(8, Math.max(2, Math.floor(span / 100)))
  const step = Math.max(1, Math.round(span / count))
  const magnitude = Math.pow(10, Math.floor(Math.log10(step)))
  const niceStep = Math.ceil(step / magnitude) * magnitude
  const ticks: number[] = []
  const firstTick = Math.ceil(start / niceStep) * niceStep
  for (let t = firstTick; t <= end; t += niceStep) {
    if (t >= start) ticks.push(t)
  }
  return ticks
}

function formatTickLabel(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(n >= 10_000 ? 0 : 1)}K`
  return String(n)
}

// ─── Component ──────────────────────────────────────────────────────────────

export function TimelineScrubber({ maxLSN, currentLSN, domain, onScrub, onRefresh }: Props) {
  const trackRef = useRef<HTMLDivElement>(null)
  const minimapRef = useRef<HTMLDivElement>(null)
  const isDragging = useRef(false)
  const debounceRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  const playRef = useRef<ReturnType<typeof setInterval> | undefined>(undefined)
  const tooltipTimeout = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)

  const [commits, setCommits] = useState<TimelineCommit[]>([])
  const [memSnapshots, setMemSnapshots] = useState<number[]>([])
  const [diskSnapshots, setDiskSnapshots] = useState<number[]>([])
  const [playing, setPlaying] = useState(false)
  const [speedIdx, setSpeedIdx] = useState(DEFAULT_SPEED_IDX)
  const [hoveredItem, setHoveredItem] = useState<HoveredItem | null>(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0 })
  const [showTooltip, setShowTooltip] = useState(false)
  const [zoomRange, setZoomRange] = useState<[number, number]>([1, Math.max(1, maxLSN)])
  const [loadingCommits, setLoadingCommits] = useState(false)
  const [jumpInput, setJumpInput] = useState('')
  const [lsnLabelMode, setLsnLabelMode] = useState<'lsn' | 'time'>('lsn')

  useEffect(() => {
    setZoomRange(prev => {
      if (prev[1] <= 1 || prev[1] < maxLSN * 0.3) return [1, Math.max(1, maxLSN)]
      return [prev[0], Math.max(prev[1], maxLSN)]
    })
  }, [maxLSN])

  // ─── Load data ────────────────────────────────────────────────────────────

  const loadData = useCallback(async () => {
    if (maxLSN <= 0) return
    setLoadingCommits(true)
    try {
      const [commitsResp, eventsResp] = await Promise.all([
        api<{ commits: TimelineCommit[] }>('/api/timeline-commits', 'POST', {
          from_lsn: 1,
          to_lsn: maxLSN,
          limit: 2000,
          domain,
        }),
        api<TimelineEvents>('/api/timeline-events', 'GET').catch(() => ({
          memory_snapshots: [] as SnapshotPoint[],
          disk_snapshots: [] as SnapshotPoint[],
        })),
      ])
      setCommits(commitsResp.commits || [])
      setMemSnapshots((eventsResp.memory_snapshots || []).map(s => s.lsn))
      setDiskSnapshots((eventsResp.disk_snapshots || []).map(s => s.lsn))
    } catch {
      // non-fatal
    } finally {
      setLoadingCommits(false)
    }
  }, [maxLSN, domain])

  useEffect(() => { loadData() }, [loadData])

  // ─── Derived geometry ─────────────────────────────────────────────────────

  const visibleStart = zoomRange[0]
  const visibleEnd = zoomRange[1]
  const visibleSpan = Math.max(1, visibleEnd - visibleStart)

  const lsnToPercent = (lsn: number) => ((lsn - visibleStart) / visibleSpan) * 100

  const lsnFromClientX = (clientX: number, ref: React.RefObject<HTMLDivElement | null>, start = visibleStart, span = visibleSpan) => {
    const el = ref.current
    if (!el) return currentLSN
    const rect = el.getBoundingClientRect()
    const pct = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width))
    return Math.max(1, Math.round(start + pct * span))
  }

  // ─── Scrub ────────────────────────────────────────────────────────────────

  const debouncedScrub = useCallback((lsn: number) => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    debounceRef.current = setTimeout(() => onScrub(lsn), 80)
  }, [onScrub])

  const handleTrackMouseDown = (e: React.MouseEvent) => {
    if ((e.target as HTMLElement).closest('.tl-playback-controls,.tl-snap-marker,.tl-ddl-marker')) return
    isDragging.current = true
    debouncedScrub(lsnFromClientX(e.clientX, trackRef))
    const onMove = (ev: MouseEvent) => {
      if (!isDragging.current) return
      debouncedScrub(lsnFromClientX(ev.clientX, trackRef))
    }
    const onUp = () => {
      isDragging.current = false
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
    }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }

  const handleMinimapClick = (e: React.MouseEvent) => {
    const lsn = lsnFromClientX(e.clientX, minimapRef, 1, maxLSN)
    const half = Math.round(visibleSpan / 2)
    const newStart = Math.max(1, lsn - half)
    const newEnd = Math.min(maxLSN, newStart + visibleSpan)
    setZoomRange([newStart, newEnd])
    onScrub(lsn)
  }

  // ─── Commit navigation ────────────────────────────────────────────────────

  const allLSNs = commits.map(c => c.lsn).sort((a, b) => a - b)

  const stepToNextCommit = useCallback(() => {
    const next = allLSNs.find(l => l > currentLSN)
    if (next !== undefined) onScrub(next)
    else if (allLSNs.length) onScrub(allLSNs[allLSNs.length - 1])
  }, [allLSNs, currentLSN, onScrub])

  const stepToPrevCommit = useCallback(() => {
    const prev = [...allLSNs].reverse().find(l => l < currentLSN)
    if (prev !== undefined) onScrub(prev)
    else if (allLSNs.length) onScrub(allLSNs[0])
  }, [allLSNs, currentLSN, onScrub])

  const jumpToStart = useCallback(() => {
    onScrub(allLSNs.length ? allLSNs[0] : 1)
  }, [allLSNs, onScrub])

  const jumpToEnd = useCallback(() => {
    onScrub(allLSNs.length ? allLSNs[allLSNs.length - 1] : maxLSN)
  }, [allLSNs, maxLSN, onScrub])

  // ─── Playback ─────────────────────────────────────────────────────────────

  const togglePlayback = useCallback(() => setPlaying(p => !p), [])

  useEffect(() => {
    if (playing) {
      const ms = PLAYBACK_SPEEDS[speedIdx].ms
      playRef.current = setInterval(stepToNextCommit, ms)
    } else {
      if (playRef.current) clearInterval(playRef.current)
    }
    return () => { if (playRef.current) clearInterval(playRef.current) }
  }, [playing, speedIdx, stepToNextCommit])

  useEffect(() => {
    if (playing && allLSNs.length && currentLSN >= allLSNs[allLSNs.length - 1]) {
      setPlaying(false)
    }
  }, [playing, currentLSN, allLSNs])

  // ─── Keyboard ─────────────────────────────────────────────────────────────

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const t = e.target as HTMLElement
      if (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA') return
      if (e.key === 'ArrowLeft') { e.preventDefault(); e.shiftKey ? jumpToStart() : stepToPrevCommit() }
      else if (e.key === 'ArrowRight') { e.preventDefault(); e.shiftKey ? jumpToEnd() : stepToNextCommit() }
      else if (e.key === ' ') { e.preventDefault(); togglePlayback() }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [stepToNextCommit, stepToPrevCommit, jumpToStart, jumpToEnd, togglePlayback])

  // ─── Zoom ─────────────────────────────────────────────────────────────────

  const handleWheel = useCallback((e: React.WheelEvent) => {
    e.preventDefault()
    const factor = e.deltaY > 0 ? 1.3 : 0.7
    const track = trackRef.current
    if (!track) return
    const rect = track.getBoundingClientRect()
    const mouseRatio = (e.clientX - rect.left) / rect.width
    const mouseLSN = visibleStart + mouseRatio * visibleSpan
    const newSpan = Math.max(20, Math.min(maxLSN, visibleSpan * factor))
    let newStart = Math.max(1, Math.round(mouseLSN - mouseRatio * newSpan))
    let newEnd = Math.round(newStart + newSpan)
    if (newEnd > maxLSN) { newEnd = maxLSN; newStart = Math.max(1, newEnd - Math.round(newSpan)) }
    setZoomRange([newStart, newEnd])
  }, [visibleStart, visibleSpan, maxLSN])

  const resetZoom = useCallback(() => setZoomRange([1, Math.max(1, maxLSN)]), [maxLSN])

  // ─── Density heatmap ──────────────────────────────────────────────────────

  const { buckets: densityBuckets, maxDensity } = (() => {
    const buckets = new Array(DENSITY_BUCKETS).fill(0) as number[]
    let maxDensity = 0
    for (const c of commits) {
      if (c.lsn < visibleStart || c.lsn > visibleEnd || isDDLCommit(c)) continue
      const idx = Math.min(DENSITY_BUCKETS - 1, Math.floor(((c.lsn - visibleStart) / visibleSpan) * DENSITY_BUCKETS))
      buckets[idx] += c.tables?.length || 1
      if (buckets[idx] > maxDensity) maxDensity = buckets[idx]
    }
    return { buckets, maxDensity }
  })()

  // ─── Tooltip ──────────────────────────────────────────────────────────────

  const showItemTooltip = (item: HoveredItem, clientX: number) => {
    if (tooltipTimeout.current) clearTimeout(tooltipTimeout.current)
    tooltipTimeout.current = setTimeout(() => {
      setHoveredItem(item)
      const rect = trackRef.current?.getBoundingClientRect()
      setTooltipPos({ x: rect ? clientX - rect.left : clientX })
      setShowTooltip(true)
    }, TOOLTIP_DELAY_MS)
  }

  const hideTooltip = () => {
    if (tooltipTimeout.current) clearTimeout(tooltipTimeout.current)
    setShowTooltip(false)
    setHoveredItem(null)
  }

  // ─── Derived state ────────────────────────────────────────────────────────

  const handlePct = Math.max(0, Math.min(100, lsnToPercent(currentLSN)))
  const isZoomed = visibleStart > 1 || visibleEnd < maxLSN

  const visibleDMLCommits = commits.filter(c =>
    c.lsn >= visibleStart && c.lsn <= visibleEnd && !isDDLCommit(c),
  )
  const visibleDDLCommits = commits.filter(c =>
    c.lsn >= visibleStart && c.lsn <= visibleEnd && isDDLCommit(c),
  )
  const visibleMemSnaps  = memSnapshots.filter(l => l >= visibleStart && l <= visibleEnd)
  const visibleDiskSnaps = diskSnapshots.filter(l => l >= visibleStart && l <= visibleEnd)

  const showDMLMarkers  = visibleDMLCommits.length <= 300
  const showDDLMarkers  = visibleDDLCommits.length <= 300

  const currentCommit = commits.find(c => c.lsn === currentLSN) ||
    [...commits].reverse().find(c => c.lsn <= currentLSN)
  const commitIndex = currentCommit ? allLSNs.indexOf(currentCommit.lsn) : -1

  const handleJumpSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const parsed = parseInt(jumpInput.replace(/[,_]/g, ''), 10)
    if (!isNaN(parsed) && parsed >= 1) {
      onScrub(Math.min(parsed, maxLSN))
      setJumpInput('')
    }
  }

  const currentLabel = lsnLabelMode === 'lsn'
    ? `LSN ${currentLSN.toLocaleString()}`
    : (currentCommit?.timestamp ? formatTimestamp(currentCommit.timestamp) : `LSN ${currentLSN.toLocaleString()}`)

  const mmViewLeft = ((visibleStart - 1) / Math.max(1, maxLSN - 1)) * 100
  const mmViewWidth = ((visibleEnd - visibleStart) / Math.max(1, maxLSN - 1)) * 100

  return (
    <div className="tl-container">
      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <div className="tl-header">
        <div
          className="tl-position-info"
          onClick={() => setLsnLabelMode(m => m === 'lsn' ? 'time' : 'lsn')}
          title="Click to toggle LSN / time display"
        >
          <IconClock />
          <span className="tl-lsn-display mono">
            <strong>{currentLabel}</strong>
            <span className="tl-lsn-max"> / {maxLSN.toLocaleString()}</span>
          </span>
          {currentCommit && commitIndex >= 0 && (
            <span className="tl-commit-num">#{commitIndex + 1} of {allLSNs.length}</span>
          )}
        </div>

        <div className="tl-playback-controls">
          <button className="tl-btn" onClick={jumpToStart} title="First commit (Shift+←)"><IconSkipBack /></button>
          <button className="tl-btn" onClick={stepToPrevCommit} title="Prev commit (←)"><IconChevronLeft /></button>
          <button
            className={`tl-btn tl-btn-play${playing ? ' active' : ''}`}
            onClick={togglePlayback}
            title="Play / Pause (Space)"
          >
            {playing ? <IconPause /> : <IconPlay />}
          </button>
          <button className="tl-btn" onClick={stepToNextCommit} title="Next commit (→)"><IconChevronRight /></button>
          <button className="tl-btn" onClick={jumpToEnd} title="Last commit (Shift+→)"><IconSkipForward /></button>

          <select
            className="tl-speed-select"
            value={speedIdx}
            onChange={e => setSpeedIdx(Number(e.target.value))}
            title="Playback speed"
          >
            {PLAYBACK_SPEEDS.map((s, i) => (
              <option key={i} value={i}>{s.label}</option>
            ))}
          </select>

          <div className="tl-separator" />

          <button className="tl-btn" onClick={resetZoom} title="Reset zoom" disabled={!isZoomed}>
            <IconZoomOut />
          </button>
          <button className="tl-btn" onClick={() => { onRefresh(); loadData() }} title="Refresh">
            <IconRefresh />
          </button>
        </div>

        <form className="tl-jump-form" onSubmit={handleJumpSubmit}>
          <input
            className="tl-jump-input mono"
            type="text"
            value={jumpInput}
            onChange={e => setJumpInput(e.target.value)}
            placeholder="Jump to LSN…"
          />
        </form>
      </div>

      {/* ── Active commit detail strip ─────────────────────────────────────── */}
      {currentCommit && currentCommit.tables && currentCommit.tables.length > 0 && (
        <div className="tl-commit-strip">
          {currentCommit.tables.map((t, i) => (
            <span key={i} className={`tl-mutation-badge ${opBadgeClass(t.operation)}`}>
              <span className="tl-mutation-op">
                {DDL_OPS.has(t.operation) ? ddlVerb(t.operation) : t.operation.toUpperCase()}
              </span>
              <span className="tl-mutation-table">{t.table}</span>
            </span>
          ))}
          <span className="tl-commit-txid mono">tx:{currentCommit.tx_id.slice(0, 12)}</span>
        </div>
      )}

      {/* ── 3-lane track ───────────────────────────────────────────────────── */}
      <div
        className="tl-track-area"
        ref={trackRef}
        onMouseDown={handleTrackMouseDown}
        onWheel={handleWheel}
      >
        {/* Lane labels */}
        <div className="tl-lane-labels">
          <div className="tl-lane-label">DML</div>
          <div className="tl-lane-label">DDL</div>
          <div className="tl-lane-label">Snap</div>
        </div>

        {/* Lane 1 — DML commits */}
        <div className="tl-lane tl-lane-dml">
          <div className="tl-density-container">
            {densityBuckets.map((count, i) => {
              const intensity = maxDensity > 0 ? count / maxDensity : 0
              return (
                <div
                  key={i}
                  className="tl-density-bar"
                  style={{
                    left: `${(i / DENSITY_BUCKETS) * 100}%`,
                    width: `${100 / DENSITY_BUCKETS}%`,
                    opacity: intensity * 0.55,
                    background: intensity > 0.7
                      ? 'var(--accent)'
                      : intensity > 0.3 ? 'var(--accent-subtle)' : 'var(--border-strong)',
                  }}
                />
              )
            })}
          </div>

          <div className="tl-rail" />
          <div className="tl-fill" style={{ width: `${handlePct}%` }} />

          {showDMLMarkers && visibleDMLCommits.map(c => {
            const pct = lsnToPercent(c.lsn)
            const isActive = currentCommit?.lsn === c.lsn
            const hasDel = c.tables?.some(t => t.operation === 'delete')
            const hasIns = c.tables?.some(t => t.operation === 'insert')
            const h = Math.min(18, 5 + (c.tables?.length || 1) * 2)
            return (
              <div
                key={c.lsn}
                className={`tl-marker${isActive ? ' active' : ''}${hasDel ? ' danger' : hasIns ? ' safe' : ''}`}
                style={{ left: `${pct}%`, height: `${h}px` }}
                onMouseEnter={ev => showItemTooltip({ kind: 'commit', commit: c }, ev.clientX)}
                onMouseLeave={hideTooltip}
                onClick={ev => { ev.stopPropagation(); onScrub(c.lsn) }}
              />
            )
          })}

          <div className="tl-ticks">
            {generateTicks(visibleStart, visibleEnd).map(tick => (
              <div key={tick} className="tl-tick" style={{ left: `${lsnToPercent(tick)}%` }}>
                <div className="tl-tick-line" />
                <span className="tl-tick-label mono">{formatTickLabel(tick)}</span>
              </div>
            ))}
          </div>

          {/* Playhead */}
          <div className={`tl-handle${playing ? ' playing' : ''}`} style={{ left: `${handlePct}%` }}>
            <div className="tl-handle-line tl-handle-line-full" />
            <div className="tl-handle-dot" />
          </div>
        </div>

        {/* Lane 2 — DDL events */}
        <div className="tl-lane tl-lane-ddl">
          <div className="tl-rail tl-rail-ddl" />
          <div className="tl-handle-line tl-handle-line-ghost" style={{ left: `${handlePct}%` }} />

          {showDDLMarkers && visibleDDLCommits.map(c => {
            const pct = lsnToPercent(c.lsn)
            const isActive = currentCommit?.lsn === c.lsn
            const ddlOp = c.tables.find(t => DDL_OPS.has(t.operation))
            const opType = ddlOp?.operation ?? 'ddl'
            return (
              <div
                key={c.lsn}
                className={`tl-ddl-marker tl-ddl-${opType.replace('_', '-')}${isActive ? ' active' : ''}`}
                style={{ left: `${pct}%` }}
                onMouseEnter={ev => showItemTooltip({ kind: 'commit', commit: c }, ev.clientX)}
                onMouseLeave={hideTooltip}
                onClick={ev => { ev.stopPropagation(); onScrub(c.lsn) }}
                title={`${ddlVerb(opType)} at LSN ${c.lsn}`}
              >
                <span className="tl-ddl-label">{ddlVerb(opType).split(' ')[0]}</span>
              </div>
            )
          })}
        </div>

        {/* Lane 3 — Snapshots */}
        <div className="tl-lane tl-lane-snaps">
          <div className="tl-rail tl-rail-snap" />
          <div className="tl-handle-line tl-handle-line-ghost" style={{ left: `${handlePct}%` }} />

          {visibleMemSnaps.map(lsn => (
            <div
              key={`m-${lsn}`}
              className={`tl-snap-marker tl-snap-memory${lsn === currentLSN ? ' active' : ''}`}
              style={{ left: `${lsnToPercent(lsn)}%` }}
              onMouseEnter={ev => showItemTooltip({ kind: 'snapshot', lsn, source: 'memory' }, ev.clientX)}
              onMouseLeave={hideTooltip}
              onClick={ev => { ev.stopPropagation(); onScrub(lsn) }}
              title={`Memory snapshot @ LSN ${lsn}`}
            >⚡</div>
          ))}

          {visibleDiskSnaps.map(lsn => (
            <div
              key={`d-${lsn}`}
              className={`tl-snap-marker tl-snap-disk${lsn === currentLSN ? ' active' : ''}`}
              style={{ left: `${lsnToPercent(lsn)}%` }}
              onMouseEnter={ev => showItemTooltip({ kind: 'snapshot', lsn, source: 'disk' }, ev.clientX)}
              onMouseLeave={hideTooltip}
              onClick={ev => { ev.stopPropagation(); onScrub(lsn) }}
              title={`Disk checkpoint @ LSN ${lsn}`}
            >💾</div>
          ))}
        </div>

        {/* ── Tooltip ──────────────────────────────────────────────── */}
        {showTooltip && hoveredItem && (
          <div
            className="tl-tooltip"
            style={{
              left: `${Math.min(tooltipPos.x, (trackRef.current?.offsetWidth ?? 400) - 220)}px`,
              bottom: '100%',
            }}
          >
            {hoveredItem.kind === 'commit' ? (
              <>
                <div className="tl-tooltip-header">
                  <span className="tl-tooltip-lsn mono">LSN {hoveredItem.commit.lsn.toLocaleString()}</span>
                  {hoveredItem.commit.timestamp > 0 && (
                    <span className="tl-tooltip-time">{formatTimestamp(hoveredItem.commit.timestamp)}</span>
                  )}
                  {isDDLCommit(hoveredItem.commit) && (
                    <span className="tl-tooltip-badge tl-tooltip-ddl-badge">DDL</span>
                  )}
                </div>
                <div className="tl-tooltip-txid mono">tx:{hoveredItem.commit.tx_id.slice(0, 16)}</div>
                {hoveredItem.commit.tables && hoveredItem.commit.tables.length > 0 && (
                  <div className="tl-tooltip-mutations">
                    {hoveredItem.commit.tables.slice(0, 8).map((t, i) => (
                      <div key={i} className={`tl-tooltip-mutation ${opBadgeClass(t.operation)}`}>
                        <span className="tl-tooltip-op">
                          {DDL_OPS.has(t.operation) ? ddlVerb(t.operation) : t.operation.toUpperCase()}
                        </span>
                        <span className="tl-tooltip-table">{t.domain}.{t.table}</span>
                      </div>
                    ))}
                    {hoveredItem.commit.tables.length > 8 && (
                      <div className="tl-tooltip-more">+{hoveredItem.commit.tables.length - 8} more</div>
                    )}
                  </div>
                )}
              </>
            ) : (
              <>
                <div className="tl-tooltip-header">
                  <span className="tl-tooltip-lsn mono">LSN {hoveredItem.lsn.toLocaleString()}</span>
                  <span className={`tl-tooltip-badge ${hoveredItem.source === 'disk' ? 'tl-tooltip-disk-badge' : 'tl-tooltip-mem-badge'}`}>
                    {hoveredItem.source === 'disk' ? '💾 Disk checkpoint' : '⚡ Memory snapshot'}
                  </span>
                </div>
                <div className="tl-tooltip-hint">Click to restore to this LSN</div>
              </>
            )}
          </div>
        )}
      </div>

      {/* ── Minimap ─────────────────────────────────────────────────────────── */}
      <div
        className="tl-minimap"
        ref={minimapRef}
        onClick={handleMinimapClick}
        title="Click to navigate · full WAL range"
      >
        {(() => {
          const MINI_BUCKETS = 120
          const miniSpan = Math.max(1, maxLSN - 1)
          const mb = new Array(MINI_BUCKETS).fill(0) as number[]
          let mMax = 0
          for (const c of commits) {
            const idx = Math.min(MINI_BUCKETS - 1, Math.floor(((c.lsn - 1) / miniSpan) * MINI_BUCKETS))
            mb[idx] += c.tables?.length || 1
            if (mb[idx] > mMax) mMax = mb[idx]
          }
          return mb.map((count, i) => {
            const intensity = mMax > 0 ? count / mMax : 0
            return (
              <div
                key={i}
                className="tl-minimap-bar"
                style={{
                  left: `${(i / MINI_BUCKETS) * 100}%`,
                  width: `${100 / MINI_BUCKETS}%`,
                  opacity: 0.15 + intensity * 0.75,
                }}
              />
            )
          })
        })()}

        {[...memSnapshots, ...diskSnapshots].map(lsn => (
          <div
            key={lsn}
            className="tl-minimap-snap-tick"
            style={{ left: `${((lsn - 1) / Math.max(1, maxLSN - 1)) * 100}%` }}
          />
        ))}

        {commits.filter(isDDLCommit).map(c => (
          <div
            key={c.lsn}
            className="tl-minimap-ddl-tick"
            style={{ left: `${((c.lsn - 1) / Math.max(1, maxLSN - 1)) * 100}%` }}
          />
        ))}

        <div
          className="tl-minimap-playhead"
          style={{ left: `${((currentLSN - 1) / Math.max(1, maxLSN - 1)) * 100}%` }}
        />

        {isZoomed && (
          <div
            className="tl-minimap-viewport"
            style={{ left: `${mmViewLeft}%`, width: `${Math.max(0.5, mmViewWidth)}%` }}
          />
        )}
      </div>

      {/* ── Legend ──────────────────────────────────────────────────────────── */}
      <div className="tl-legend">
        <span className="tl-legend-item tl-legend-insert">INSERT</span>
        <span className="tl-legend-item tl-legend-update">UPDATE</span>
        <span className="tl-legend-item tl-legend-delete">DELETE</span>
        <span className="tl-legend-sep" />
        <span className="tl-legend-item tl-legend-ddl">DDL event</span>
        <span className="tl-legend-sep" />
        <span className="tl-legend-item">⚡ mem ({memSnapshots.length})</span>
        <span className="tl-legend-item">💾 disk ({diskSnapshots.length})</span>
        {loadingCommits && <span className="tl-legend-loading">loading…</span>}
      </div>
    </div>
  )
}
