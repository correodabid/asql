import { useEffect, useMemo, useState } from 'react'
import { api } from '../lib/api'
import { IconCheck, IconDownload, IconRefresh, IconUpload } from './Icons'

type Props = {
  domain: string
}

type FixtureResponse = {
  status?: string
  file?: string
  name?: string
  steps?: number
}

export function FixturePanel({ domain }: Props) {
  const [availableDomains, setAvailableDomains] = useState<string[]>([])
  const [selectedDomains, setSelectedDomains] = useState<string[]>([])
  const [fixturePath, setFixturePath] = useState('')
  const [exportPath, setExportPath] = useState('')
  const [exportName, setExportName] = useState('')
  const [exportDescription, setExportDescription] = useState('')
  const [busy, setBusy] = useState('')
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [lastResult, setLastResult] = useState<FixtureResponse | null>(null)

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
      setError(err instanceof Error ? err.message : String(err))
    })
    return () => {
      active = false
    }
  }, [domain])

  const normalizedExportName = useMemo(() => {
    const trimmed = exportName.trim()
    if (trimmed) return trimmed
    if (selectedDomains.length > 0) return `${selectedDomains.join('-')}-export`
    return 'fixture-export'
  }, [exportName, selectedDomains])

  const run = async (label: string, fn: () => Promise<FixtureResponse>) => {
    setBusy(label)
    setError('')
    setMessage('')
    try {
      const response = await fn()
      setLastResult(response)
      setMessage(`${response.status || 'OK'} · ${response.name || ''} ${response.steps ? `(${response.steps} steps)` : ''}`.trim())
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy('')
    }
  }

  const toggleDomain = (name: string) => {
    setSelectedDomains((current) => current.includes(name)
      ? current.filter((candidate) => candidate !== name)
      : [...current, name].sort())
  }

  return (
    <div className="panel" style={{ margin: 16, padding: 16, display: 'grid', gap: 16 }}>
      <div>
        <h2 style={{ margin: '0 0 8px' }}>Fixtures</h2>
        <p className="text-muted" style={{ margin: 0 }}>
          Validate, load, and export deterministic fixture packs directly from Studio.
        </p>
      </div>

      <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
        <h3 style={{ marginTop: 0 }}>Validate / Load</h3>
        <div style={{ display: 'grid', gap: 10, gridTemplateColumns: '1fr auto' }}>
          <input
            className="title-domain-select"
            value={fixturePath}
            onChange={(e) => setFixturePath(e.target.value)}
            placeholder="/path/to/fixture.json"
          />
          <button
            className="toolbar-btn"
            disabled={busy !== ''}
            onClick={() => {
              void api<string>('/api/fixtures/pick-file', 'GET').then((path) => {
                if (path) setFixturePath(path)
              }).catch((err) => setError(err instanceof Error ? err.message : String(err)))
            }}
          >
            Browse…
          </button>
        </div>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginTop: 12 }}>
          <button
            className="toolbar-btn"
            disabled={busy !== '' || !fixturePath.trim()}
            onClick={() => run('validate', () => api<FixtureResponse>('/api/fixtures/validate', 'POST', { file_path: fixturePath }))}
          >
            <IconCheck /> Validate fixture
          </button>
          <button
            className="toolbar-btn"
            disabled={busy !== '' || !fixturePath.trim()}
            onClick={() => run('load', () => api<FixtureResponse>('/api/fixtures/load', 'POST', { file_path: fixturePath }))}
          >
            <IconUpload /> Load fixture
          </button>
        </div>
      </div>

      <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
        <h3 style={{ marginTop: 0 }}>Export</h3>
        <div style={{ display: 'grid', gap: 12 }}>
          <div>
            <div className="text-muted" style={{ fontSize: 12, marginBottom: 8 }}>Domains</div>
            <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
              {availableDomains.map((name) => (
                <label
                  key={name}
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: 6,
                    padding: '6px 10px',
                    border: '1px solid var(--border)',
                    borderRadius: 999,
                    background: selectedDomains.includes(name) ? 'var(--accent-subtle)' : 'var(--bg-elevated)',
                    cursor: 'pointer',
                    fontSize: 12,
                  }}
                >
                  <input
                    type="checkbox"
                    checked={selectedDomains.includes(name)}
                    onChange={() => toggleDomain(name)}
                  />
                  {name}
                </label>
              ))}
            </div>
          </div>

          <div style={{ display: 'grid', gap: 10, gridTemplateColumns: '1fr auto' }}>
            <input
              className="title-domain-select"
              value={exportPath}
              onChange={(e) => setExportPath(e.target.value)}
              placeholder="/path/to/exported-fixture.json"
            />
            <button
              className="toolbar-btn"
              disabled={busy !== ''}
              onClick={() => {
                void api<string>('/api/fixtures/pick-export-file', 'POST', {
                  suggested_name: `${normalizedExportName}.json`,
                }).then((path) => {
                  if (path) setExportPath(path)
                }).catch((err) => setError(err instanceof Error ? err.message : String(err)))
              }}
            >
              Save as…
            </button>
          </div>

          <div style={{ display: 'grid', gap: 10, gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))' }}>
            <input
              className="title-domain-select"
              value={exportName}
              onChange={(e) => setExportName(e.target.value)}
              placeholder="Fixture name (optional)"
            />
            <input
              className="title-domain-select"
              value={exportDescription}
              onChange={(e) => setExportDescription(e.target.value)}
              placeholder="Description (optional)"
            />
          </div>

          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
            <button
              className="toolbar-btn"
              disabled={busy !== '' || !exportPath.trim() || selectedDomains.length === 0}
              onClick={() => run('export', () => api<FixtureResponse>('/api/fixtures/export', 'POST', {
                file_path: exportPath,
                domains: selectedDomains,
                name: exportName,
                description: exportDescription,
              }))}
            >
              <IconDownload /> Export fixture
            </button>
            <button
              className="toolbar-btn"
              disabled={busy !== ''}
              onClick={() => {
                setError('')
                setMessage('')
                setLastResult(null)
                setExportDescription('')
                setExportName('')
              }}
            >
              <IconRefresh /> Reset
            </button>
          </div>

          <div className="text-muted" style={{ fontSize: 12 }}>
            Export is intentionally strict: selected domains must include dependency domains, and exported tables must have stable primary keys.
          </div>
        </div>
      </div>

      {message && (
        <div className="console-status-bar success-bar">
          <IconCheck />
          <span>{message}</span>
        </div>
      )}

      {error && (
        <div className="console-status-bar error-bar">
          <span className="error-icon">!</span>
          <span>{error}</span>
        </div>
      )}

      <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
        <h3 style={{ marginTop: 0 }}>Last result</h3>
        <pre style={{ margin: 0, whiteSpace: 'pre-wrap', overflowX: 'auto' }}>
          {lastResult ? JSON.stringify(lastResult, null, 2) : 'No fixture operation run yet.'}
        </pre>
      </div>
    </div>
  )
}
