import { type ReactNode, useEffect, useMemo, useState } from 'react'
import { api } from '../lib/api'
import {
  IconAlertTriangle,
  IconCheck,
  IconCheckCircle,
  IconDatabase,
  IconDownload,
  IconLayers,
  IconRefresh,
  IconSearch,
  IconUpload,
} from './Icons'

/* ── Types ────────────────────────────────────────────────── */

type Props = { domain: string }

type FixtureResponse = {
  status?: string
  file?: string
  name?: string
  steps?: number
}

type Toast = { id: number; message: string; kind: 'success' | 'error' }
let toastSeq = 0

/* ── Main Panel ───────────────────────────────────────────── */

export function FixturePanel({ domain }: Props) {
  const [availableDomains, setAvailableDomains] = useState<string[]>([])
  const [selectedDomains, setSelectedDomains] = useState<string[]>([])
  const [fixturePath, setFixturePath] = useState('')
  const [exportPath, setExportPath] = useState('')
  const [exportName, setExportName] = useState('')
  const [exportDescription, setExportDescription] = useState('')
  const [busy, setBusy] = useState('')
  const [lastResult, setLastResult] = useState<FixtureResponse | null>(null)
  const [toasts, setToasts] = useState<Toast[]>([])

  const toast = (message: string, kind: 'success' | 'error') => {
    const id = ++toastSeq
    setToasts((t) => [...t, { id, message, kind }])
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 3200)
  }

  useEffect(() => {
    let active = true
    void api<{ domains?: string[] }>('/api/domains', 'GET').then((response) => {
      if (!active) return
      const domains = response.domains || []
      setAvailableDomains(domains)
      if (domain && domain !== '__all__') {
        setSelectedDomains((current) => current.length > 0 ? current : [domain])
      }
    }).catch((err) => {
      if (!active) return
      toast(err instanceof Error ? err.message : String(err), 'error')
    })
    return () => { active = false }
  }, [domain])

  const normalizedExportName = useMemo(() => {
    const trimmed = exportName.trim()
    if (trimmed) return trimmed
    if (selectedDomains.length > 0) return `${selectedDomains.join('-')}-export`
    return 'fixture-export'
  }, [exportName, selectedDomains])

  const run = async (label: string, fn: () => Promise<FixtureResponse>) => {
    setBusy(label)
    try {
      const response = await fn()
      setLastResult(response)
      const msg = `${response.status || 'OK'} · ${response.name || ''} ${response.steps ? `(${response.steps} steps)` : ''}`.trim()
      toast(msg, 'success')
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'error')
    } finally {
      setBusy('')
    }
  }

  const toggleDomain = (name: string) => {
    setSelectedDomains((current) => current.includes(name)
      ? current.filter((c) => c !== name)
      : [...current, name].sort())
  }

  return (
    <div className="sec-page">
      {/* ── Header ──────────────────────────────────────── */}
      <div className="sec-header">
        <div className="sec-header-text">
          <h2 className="sec-title">Fixtures</h2>
          <p className="sec-subtitle">
            Validate, load, and export deterministic fixture packs. Uses stable primary keys and explicit domain dependency ordering.
          </p>
        </div>
      </div>

      {/* ── KPI row ─────────────────────────────────────── */}
      <div className="sec-kpi-row">
        <FixKPI icon={<IconDatabase />} label="Domains" value={availableDomains.length} color="var(--accent)" delay={0} />
        <FixKPI icon={<IconLayers />} label="Selected" value={selectedDomains.length} color="var(--text-safe)" delay={60} />
        <FixKPI icon={<IconCheckCircle />} label="Steps" value={lastResult?.steps ?? 0} color="var(--text-warning)" delay={120} />
      </div>

      {/* ── Validate / Load ─────────────────────────────── */}
      <div className="glass-section" style={{ animationDelay: '60ms' }}>
        <div className="glass-section-header">
          <span className="glass-section-title">Validate &amp; Load</span>
        </div>
        <div className="glass-section-body">
          <div className="fix-file-row">
            <label className="sec-field" style={{ flex: 1 }}>
              <span className="sec-field-label">Fixture file</span>
              <input
                className="sec-input"
                value={fixturePath}
                onChange={(e) => setFixturePath(e.target.value)}
                placeholder="/path/to/fixture.json"
              />
            </label>
            <button
              className="sec-action-chip"
              style={{ alignSelf: 'flex-end' }}
              disabled={busy !== ''}
              onClick={() => {
                void api<string>('/api/fixtures/pick-file', 'GET').then((path) => {
                  if (path) setFixturePath(path)
                }).catch((err) => toast(err instanceof Error ? err.message : String(err), 'error'))
              }}
            >
              <IconSearch /> <span>Browse…</span>
            </button>
          </div>

          <div className="sec-actions-strip" style={{ marginTop: 12 }}>
            <ActionChip
              icon={<IconCheckCircle />}
              label="Validate"
              loading={busy === 'validate'}
              onClick={() => run('validate', () => api<FixtureResponse>('/api/fixtures/validate', 'POST', { file_path: fixturePath }))}
              disabled={busy !== '' || !fixturePath.trim()}
            />
            <ActionChip
              icon={<IconUpload />}
              label="Load fixture"
              loading={busy === 'load'}
              onClick={() => run('load', () => api<FixtureResponse>('/api/fixtures/load', 'POST', { file_path: fixturePath }))}
              disabled={busy !== '' || !fixturePath.trim()}
            />
          </div>
        </div>
      </div>

      {/* ── Export ──────────────────────────────────────── */}
      <div className="glass-section" style={{ animationDelay: '120ms' }}>
        <div className="glass-section-header">
          <span className="glass-section-title">Export</span>
        </div>
        <div className="glass-section-body">
          {/* Domain chips */}
          <div style={{ marginBottom: 12 }}>
            <span className="sec-field-label" style={{ display: 'block', marginBottom: 8 }}>Domains</span>
            <div className="fix-domain-chips">
              {availableDomains.length === 0 && (
                <span className="text-muted" style={{ fontSize: 12 }}>No domains available.</span>
              )}
              {availableDomains.map((name) => (
                <button
                  key={name}
                  className={`fix-domain-chip ${selectedDomains.includes(name) ? 'selected' : ''}`}
                  onClick={() => toggleDomain(name)}
                >
                  {selectedDomains.includes(name) && <IconCheck />}
                  {name}
                </button>
              ))}
            </div>
          </div>

          {/* Export file path */}
          <div className="fix-file-row">
            <label className="sec-field" style={{ flex: 1 }}>
              <span className="sec-field-label">Export file path</span>
              <input
                className="sec-input"
                value={exportPath}
                onChange={(e) => setExportPath(e.target.value)}
                placeholder="/path/to/exported-fixture.json"
              />
            </label>
            <button
              className="sec-action-chip"
              style={{ alignSelf: 'flex-end' }}
              disabled={busy !== ''}
              onClick={() => {
                void api<string>('/api/fixtures/pick-export-file', 'POST', {
                  suggested_name: `${normalizedExportName}.json`,
                }).then((path) => {
                  if (path) setExportPath(path)
                }).catch((err) => toast(err instanceof Error ? err.message : String(err), 'error'))
              }}
            >
              <IconSearch /> <span>Save as…</span>
            </button>
          </div>

          {/* Name + description */}
          <div className="rec-dir-grid" style={{ marginTop: 12 }}>
            <label className="sec-field">
              <span className="sec-field-label">Fixture name</span>
              <input className="sec-input" value={exportName} onChange={(e) => setExportName(e.target.value)} placeholder="Optional name" />
            </label>
            <label className="sec-field">
              <span className="sec-field-label">Description</span>
              <input className="sec-input" value={exportDescription} onChange={(e) => setExportDescription(e.target.value)} placeholder="Optional description" />
            </label>
          </div>

          <div className="sec-actions-strip" style={{ marginTop: 12 }}>
            <ActionChip
              icon={<IconDownload />}
              label="Export fixture"
              loading={busy === 'export'}
              onClick={() => run('export', () => api<FixtureResponse>('/api/fixtures/export', 'POST', {
                file_path: exportPath,
                domains: selectedDomains,
                name: exportName,
                description: exportDescription,
              }))}
              disabled={busy !== '' || !exportPath.trim() || selectedDomains.length === 0}
            />
            <ActionChip
              icon={<IconRefresh />}
              label="Reset"
              onClick={() => {
                setLastResult(null)
                setExportDescription('')
                setExportName('')
                setExportPath('')
              }}
              disabled={busy !== ''}
            />
          </div>

          <p className="fix-note">
            Export is intentionally strict: selected domains must include dependency domains, and exported tables must have stable primary keys.
          </p>
        </div>
      </div>

      {/* ── Last result ─────────────────────────────────── */}
      <div className="glass-section" style={{ animationDelay: '180ms' }}>
        <div className="glass-section-header">
          <span className="glass-section-title">Last Result</span>
          {lastResult?.status && (
            <span className="sec-catalog-count">{lastResult.status}</span>
          )}
        </div>
        <div className="glass-section-body">
          {lastResult ? (
            <div className="fix-result-grid">
              <ResultRow label="Status" value={lastResult.status ?? '—'} />
              <ResultRow label="File" value={lastResult.file ?? '—'} mono />
              <ResultRow label="Name" value={lastResult.name ?? '—'} />
              <ResultRow label="Steps" value={String(lastResult.steps ?? 0)} />
            </div>
          ) : (
            <div className="rec-empty-diag">
              <IconLayers />
              <span>Run a validate, load, or export operation to see results.</span>
            </div>
          )}
        </div>
      </div>

      {/* ── Toast rail ──────────────────────────────────── */}
      {toasts.length > 0 && (
        <div className="sec-toast-rail">
          {toasts.map((t) => (
            <div key={t.id} className={`sec-toast sec-toast-${t.kind}`}>
              {t.kind === 'success' ? <IconCheck /> : <IconAlertTriangle />}
              <span>{t.message}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

/* ── Sub-components ───────────────────────────────────────── */

function FixKPI({
  icon,
  label,
  value,
  color,
  delay = 0,
}: {
  icon: ReactNode
  label: string
  value: number
  color: string
  delay?: number
}) {
  return (
    <div className="kpi-card" style={{ '--kpi-accent': color, animationDelay: `${delay}ms` } as React.CSSProperties}>
      <div className="kpi-card-glow" />
      <div className="kpi-icon-wrap" style={{ color }}>{icon}</div>
      <div className="kpi-content">
        <span className="kpi-label">{label}</span>
        <div className="kpi-value">{value}</div>
      </div>
    </div>
  )
}

function ActionChip({
  icon,
  label,
  loading,
  onClick,
  disabled,
}: {
  icon: ReactNode
  label: string
  loading?: boolean
  onClick: () => void
  disabled: boolean
}) {
  return (
    <button
      className={`sec-action-chip ${loading ? 'loading' : ''}`}
      onClick={onClick}
      disabled={disabled}
    >
      {icon}
      <span>{label}</span>
    </button>
  )
}

function ResultRow({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="fix-result-row">
      <span className="fix-result-label">{label}</span>
      <span className={`fix-result-value ${mono ? 'mono' : ''}`}>{value}</span>
    </div>
  )
}
