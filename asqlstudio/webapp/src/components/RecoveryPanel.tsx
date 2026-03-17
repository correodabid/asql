import { type ReactNode, useEffect, useMemo, useState } from 'react'
import {
  IconAlertTriangle,
  IconCheck,
  IconCheckCircle,
  IconDatabase,
  IconDownload,
  IconRefresh,
  IconServer,
  IconUpload,
} from './Icons'
import { api } from '../lib/api'

/* ── Types ────────────────────────────────────────────────── */

type BackupFileMetadata = {
  relative_path?: string
  bytes?: number
  sha256?: string
}

type BackupManifest = {
  version?: number
  head_lsn?: number
  head_timestamp?: number
  snapshots?: Array<BackupFileMetadata & { sequence?: number; lsn?: number; logical_ts?: number }>
  wal_segments?: Array<BackupFileMetadata & { seq_num?: number; first_lsn?: number; last_lsn?: number; record_count?: number }>
  timestamp_index?: BackupFileMetadata
}

type RestoreResult = {
  AppliedLSN?: number
  AppliedTimestamp?: number
}

type SnapshotCatalogEntry = {
  file_name?: string
  sequence?: number
  lsn?: number
  logical_ts?: number
  bytes?: number
  is_full?: boolean
}

type WALSegment = {
  file_name?: string
  first_lsn?: number
  last_lsn?: number
  bytes?: number
}

type WALRetentionState = {
  data_dir?: string
  retain_wal?: boolean
  head_lsn?: number
  oldest_retained_lsn?: number
  last_retained_lsn?: number
  segment_count?: number
  disk_snapshot_count?: number
  max_disk_snapshots?: number
  segments?: WALSegment[]
}

/* ── Helpers ──────────────────────────────────────────────── */

function fmtBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

type Toast = { id: number; message: string; kind: 'success' | 'error' }
let toastSeq = 0

type DrawerId = 'restore-lsn' | 'restore-timestamp' | null

/* ── Main Panel ───────────────────────────────────────────── */

export function RecoveryPanel() {
  const [dataDir, setDataDir] = useState('')
  const [backupDir, setBackupDir] = useState('')
  const [restoreDir, setRestoreDir] = useState('')
  const [restoreLSN, setRestoreLSN] = useState('')
  const [restoreTimestamp, setRestoreTimestamp] = useState('')
  const [manifest, setManifest] = useState<BackupManifest | null>(null)
  const [snapshotCatalog, setSnapshotCatalog] = useState<SnapshotCatalogEntry[]>([])
  const [walRetention, setWalRetention] = useState<WALRetentionState | null>(null)
  const [verifyStatus, setVerifyStatus] = useState('')
  const [lastRestore, setLastRestore] = useState<RestoreResult | null>(null)
  const [busy, setBusy] = useState('')
  const [toasts, setToasts] = useState<Toast[]>([])
  const [activeDrawer, setActiveDrawer] = useState<DrawerId>(null)

  /* toast helper */
  const toast = (message: string, kind: 'success' | 'error') => {
    const id = ++toastSeq
    setToasts((t) => [...t, { id, message, kind }])
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 3200)
  }

  useEffect(() => {
    let active = true
    void api<{ data_dir?: string }>('/api/recovery/defaults').then((resp) => {
      if (!active) return
      const resolved = resp.data_dir || '.asql'
      setDataDir(resolved)
      setBackupDir(`${resolved}-backup`)
      setRestoreDir(`${resolved}-restore`)
    }).catch((err) => {
      if (!active) return
      toast(err instanceof Error ? err.message : String(err), 'error')
    })
    return () => { active = false }
  }, [])

  const summary = useMemo(() => ({
    snapshots: manifest?.snapshots?.length ?? 0,
    walSegments: manifest?.wal_segments?.length ?? 0,
    headLSN: manifest?.head_lsn ?? 0,
    headTimestamp: manifest?.head_timestamp ?? 0,
  }), [manifest])

  /* mutation runner */
  const run = async (label: string, fn: () => Promise<string>) => {
    setBusy(label)
    try {
      const msg = await fn()
      toast(msg, 'success')
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), 'error')
    } finally {
      setBusy('')
    }
  }

  const toggleDrawer = (id: DrawerId) => setActiveDrawer((prev) => (prev === id ? null : id))

  return (
    <div className="sec-page">
      {/* ── Header ──────────────────────────────────────── */}
      <div className="sec-header">
        <div className="sec-header-text">
          <h2 className="sec-title">Recovery</h2>
          <p className="sec-subtitle">
            Create, verify, inspect, and restore WAL-based backups. Point-in-time restore by LSN or logical timestamp.
          </p>
        </div>
      </div>

      {/* ── Backup KPI row ──────────────────────────────── */}
      <div className="sec-kpi-row">
        <RecKPI icon={<IconDatabase />} label="Snapshots" value={summary.snapshots} color="var(--accent)" delay={0} />
        <RecKPI icon={<IconServer />} label="WAL segments" value={summary.walSegments} color="var(--text-safe)" delay={60} />
        <RecKPI icon={<IconUpload />} label="Head LSN" value={summary.headLSN} color="var(--text-warning)" delay={120} />
        <RecKPI icon={<IconRefresh />} label="Head timestamp" value={summary.headTimestamp} color="#06b6d4" delay={180} />
      </div>

      {/* ── Directories config ──────────────────────────── */}
      <div className="glass-section" style={{ animationDelay: '60ms' }}>
        <div className="glass-section-header">
          <span className="glass-section-title">Directories</span>
        </div>
        <div className="glass-section-body">
          <div className="rec-dir-grid">
            <label className="sec-field">
              <span className="sec-field-label">Source data directory</span>
              <input className="sec-input" value={dataDir} onChange={(e) => setDataDir(e.target.value)} />
            </label>
            <label className="sec-field">
              <span className="sec-field-label">Backup directory</span>
              <input className="sec-input" value={backupDir} onChange={(e) => setBackupDir(e.target.value)} />
            </label>
            <label className="sec-field">
              <span className="sec-field-label">Restore target</span>
              <input className="sec-input" value={restoreDir} onChange={(e) => setRestoreDir(e.target.value)} />
            </label>
          </div>
        </div>
      </div>

      {/* ── Quick actions ───────────────────────────────── */}
      <div className="glass-section" style={{ animationDelay: '120ms' }}>
        <div className="glass-section-header">
          <span className="glass-section-title">Backup Operations</span>
        </div>
        <div className="glass-section-body">
          <div className="sec-actions-strip">
            <ActionChip
              icon={<IconDownload />}
              label="Create backup"
              loading={busy === 'backup'}
              onClick={() => run('backup', async () => {
                const resp = await api<BackupManifest>('/api/recovery/create-backup', 'POST', { data_dir: dataDir, backup_dir: backupDir })
                setManifest(resp)
                setVerifyStatus('')
                setLastRestore(null)
                return 'Base backup created successfully.'
              })}
              disabled={busy !== ''}
            />
            <ActionChip
              icon={<IconRefresh />}
              label="Load manifest"
              loading={busy === 'manifest'}
              onClick={() => run('manifest', async () => {
                const resp = await api<BackupManifest>('/api/recovery/manifest', 'POST', { backup_dir: backupDir })
                setManifest(resp)
                return 'Backup manifest loaded.'
              })}
              disabled={busy !== ''}
            />
            <ActionChip
              icon={<IconCheckCircle />}
              label="Verify backup"
              loading={busy === 'verify'}
              onClick={() => run('verify', async () => {
                const resp = await api<{ status?: string; manifest?: BackupManifest }>('/api/recovery/verify', 'POST', { backup_dir: backupDir })
                setVerifyStatus(resp.status || 'OK')
                setManifest(resp.manifest || null)
                return `Backup verification: ${resp.status || 'OK'}`
              })}
              disabled={busy !== ''}
            />
            <ActionChip
              icon={<IconServer />}
              label="Inspect data dir"
              loading={busy === 'diagnostics'}
              onClick={() => run('diagnostics', async () => {
                const [catalogResp, retentionResp] = await Promise.all([
                  api<{ snapshots?: SnapshotCatalogEntry[] }>('/api/recovery/snapshot-catalog', 'POST', { data_dir: dataDir }),
                  api<WALRetentionState>('/api/recovery/wal-retention', 'POST', { data_dir: dataDir }),
                ])
                setSnapshotCatalog(catalogResp.snapshots || [])
                setWalRetention(retentionResp)
                return `Loaded ${(catalogResp.snapshots || []).length} snapshots, ${retentionResp.segment_count ?? 0} WAL segments.`
              })}
              disabled={busy !== ''}
            />
          </div>

          {/* Verification status */}
          {verifyStatus && (
            <div className={`rec-verify-banner ${verifyStatus === 'OK' ? 'ok' : 'warn'}`}>
              {verifyStatus === 'OK' ? <IconCheckCircle /> : <IconAlertTriangle />}
              <span>Verification: <strong>{verifyStatus}</strong></span>
            </div>
          )}
        </div>
      </div>

      {/* ── Restore section ─────────────────────────────── */}
      <div className="glass-section" style={{ animationDelay: '180ms' }}>
        <div className="glass-section-header">
          <span className="glass-section-title">Point-in-Time Restore</span>
          {lastRestore && (
            <span className="sec-catalog-count">
              Last restore: LSN {lastRestore.AppliedLSN ?? 0} / ts {lastRestore.AppliedTimestamp ?? 0}
            </span>
          )}
        </div>
        <div className="glass-section-body">
          <div className="sec-actions-strip">
            <ActionChip
              icon={<IconUpload />}
              label="Restore to LSN"
              active={activeDrawer === 'restore-lsn'}
              onClick={() => toggleDrawer('restore-lsn')}
              disabled={busy !== ''}
            />
            <ActionChip
              icon={<IconUpload />}
              label="Restore to timestamp"
              active={activeDrawer === 'restore-timestamp'}
              onClick={() => toggleDrawer('restore-timestamp')}
              disabled={busy !== ''}
            />
          </div>

          {activeDrawer === 'restore-lsn' && (
            <div className="sec-drawer">
              <div className="sec-drawer-header">
                <div>
                  <div className="sec-drawer-title">Restore to LSN</div>
                  <div className="sec-drawer-desc">Replay the WAL up to the specified LSN position into the restore target directory.</div>
                </div>
              </div>
              <div className="sec-drawer-fields">
                <label className="sec-field">
                  <span className="sec-field-label">Target LSN</span>
                  <input className="sec-input" value={restoreLSN} onChange={(e) => setRestoreLSN(e.target.value)} placeholder="123" />
                </label>
              </div>
              <div className="sec-drawer-footer">
                <button
                  className="toolbar-btn primary"
                  disabled={busy !== '' || !restoreLSN.trim()}
                  onClick={() => run('restore-lsn', async () => {
                    const resp = await api<RestoreResult>('/api/recovery/restore-lsn', 'POST', { backup_dir: backupDir, data_dir: restoreDir, lsn: Number(restoreLSN) })
                    setLastRestore(resp)
                    setActiveDrawer(null)
                    return `Restored to LSN ${restoreLSN}. Applied LSN: ${resp.AppliedLSN ?? 0}`
                  })}
                >
                  <IconUpload /> Restore
                </button>
                <button className="toolbar-btn" onClick={() => setActiveDrawer(null)}>Cancel</button>
              </div>
            </div>
          )}

          {activeDrawer === 'restore-timestamp' && (
            <div className="sec-drawer">
              <div className="sec-drawer-header">
                <div>
                  <div className="sec-drawer-title">Restore to timestamp</div>
                  <div className="sec-drawer-desc">Replay the WAL up to the specified logical timestamp into the restore target directory.</div>
                </div>
              </div>
              <div className="sec-drawer-fields">
                <label className="sec-field">
                  <span className="sec-field-label">Logical timestamp</span>
                  <input className="sec-input" value={restoreTimestamp} onChange={(e) => setRestoreTimestamp(e.target.value)} placeholder="123" />
                </label>
              </div>
              <div className="sec-drawer-footer">
                <button
                  className="toolbar-btn primary"
                  disabled={busy !== '' || !restoreTimestamp.trim()}
                  onClick={() => run('restore-ts', async () => {
                    const resp = await api<RestoreResult>('/api/recovery/restore-timestamp', 'POST', { backup_dir: backupDir, data_dir: restoreDir, logical_timestamp: Number(restoreTimestamp) })
                    setLastRestore(resp)
                    setActiveDrawer(null)
                    return `Restored to timestamp ${restoreTimestamp}. Applied timestamp: ${resp.AppliedTimestamp ?? 0}`
                  })}
                >
                  <IconUpload /> Restore
                </button>
                <button className="toolbar-btn" onClick={() => setActiveDrawer(null)}>Cancel</button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* ── Retention diagnostics ───────────────────────── */}
      <div className="glass-section" style={{ animationDelay: '240ms' }}>
        <div className="glass-section-header">
          <span className="glass-section-title">Retention Diagnostics</span>
        </div>
        <div className="glass-section-body">
          <div className="sec-kpi-row" style={{ marginBottom: snapshotCatalog.length > 0 || (walRetention?.segments ?? []).length > 0 ? 16 : 0 }}>
            <RecKPI icon={<IconDatabase />} label="Catalog snapshots" value={snapshotCatalog.length} color="var(--accent)" delay={0} />
            <RecKPI icon={<IconServer />} label="Retained WAL" value={walRetention?.segment_count ?? 0} color="var(--text-safe)" delay={40} />
            <RecKPI icon={<IconUpload />} label="Oldest retained LSN" value={walRetention?.oldest_retained_lsn ?? 0} color="var(--text-warning)" delay={80} />
            <RecKPI icon={<IconUpload />} label="Last retained LSN" value={walRetention?.last_retained_lsn ?? 0} color="#06b6d4" delay={120} />
          </div>

          {/* Snapshot table */}
          {snapshotCatalog.length > 0 && (
            <div className="rec-table-wrap">
              <div className="rec-table-label">Snapshots</div>
              <table className="rec-table">
                <thead>
                  <tr>
                    <th>File</th>
                    <th>Seq</th>
                    <th>LSN</th>
                    <th>Logical TS</th>
                    <th>Size</th>
                    <th>Full</th>
                  </tr>
                </thead>
                <tbody>
                  {snapshotCatalog.map((snap, i) => (
                    <tr key={i}>
                      <td className="rec-td-mono">{snap.file_name ?? '—'}</td>
                      <td>{snap.sequence ?? '—'}</td>
                      <td>{snap.lsn ?? '—'}</td>
                      <td>{snap.logical_ts ?? '—'}</td>
                      <td>{snap.bytes != null ? fmtBytes(snap.bytes) : '—'}</td>
                      <td>{snap.is_full ? <IconCheck /> : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* WAL segments table */}
          {(walRetention?.segments ?? []).length > 0 && (
            <div className="rec-table-wrap">
              <div className="rec-table-label">WAL Segments</div>
              <table className="rec-table">
                <thead>
                  <tr>
                    <th>File</th>
                    <th>First LSN</th>
                    <th>Last LSN</th>
                    <th>Size</th>
                  </tr>
                </thead>
                <tbody>
                  {(walRetention?.segments ?? []).map((seg, i) => (
                    <tr key={i}>
                      <td className="rec-td-mono">{seg.file_name ?? '—'}</td>
                      <td>{seg.first_lsn ?? '—'}</td>
                      <td>{seg.last_lsn ?? '—'}</td>
                      <td>{seg.bytes != null ? fmtBytes(seg.bytes) : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {snapshotCatalog.length === 0 && (walRetention?.segments ?? []).length === 0 && (
            <div className="rec-empty-diag">
              <IconServer />
              <span>Click "Inspect data dir" to load retention diagnostics.</span>
            </div>
          )}
        </div>
      </div>

      {/* ── Backup manifest ─────────────────────────────── */}
      {manifest && (
        <div className="glass-section" style={{ animationDelay: '300ms' }}>
          <div className="glass-section-header">
            <span className="glass-section-title">Backup Manifest</span>
            <span className="sec-catalog-count">v{manifest.version ?? 0}</span>
          </div>
          <div className="glass-section-body">
            {/* Manifest snapshots */}
            {(manifest.snapshots ?? []).length > 0 && (
              <div className="rec-table-wrap">
                <div className="rec-table-label">Manifest Snapshots</div>
                <table className="rec-table">
                  <thead>
                    <tr>
                      <th>Path</th>
                      <th>Seq</th>
                      <th>LSN</th>
                      <th>Size</th>
                      <th>SHA-256</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(manifest.snapshots ?? []).map((snap, i) => (
                      <tr key={i}>
                        <td className="rec-td-mono">{snap.relative_path ?? '—'}</td>
                        <td>{snap.sequence ?? '—'}</td>
                        <td>{snap.lsn ?? '—'}</td>
                        <td>{snap.bytes != null ? fmtBytes(snap.bytes) : '—'}</td>
                        <td className="rec-td-mono rec-td-hash">{snap.sha256 ? snap.sha256.slice(0, 12) + '…' : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Manifest WAL segments */}
            {(manifest.wal_segments ?? []).length > 0 && (
              <div className="rec-table-wrap">
                <div className="rec-table-label">Manifest WAL Segments</div>
                <table className="rec-table">
                  <thead>
                    <tr>
                      <th>Path</th>
                      <th>Seq</th>
                      <th>First LSN</th>
                      <th>Last LSN</th>
                      <th>Records</th>
                      <th>Size</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(manifest.wal_segments ?? []).map((seg, i) => (
                      <tr key={i}>
                        <td className="rec-td-mono">{seg.relative_path ?? '—'}</td>
                        <td>{seg.seq_num ?? '—'}</td>
                        <td>{seg.first_lsn ?? '—'}</td>
                        <td>{seg.last_lsn ?? '—'}</td>
                        <td>{seg.record_count ?? '—'}</td>
                        <td>{seg.bytes != null ? fmtBytes(seg.bytes) : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {(manifest.snapshots ?? []).length === 0 && (manifest.wal_segments ?? []).length === 0 && (
              <div className="rec-empty-diag">
                <IconDatabase />
                <span>Manifest is empty — no snapshots or WAL segments.</span>
              </div>
            )}
          </div>
        </div>
      )}

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

function RecKPI({
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
  active,
  loading,
  onClick,
  disabled,
}: {
  icon: ReactNode
  label: string
  active?: boolean
  loading?: boolean
  onClick: () => void
  disabled: boolean
}) {
  return (
    <button
      className={`sec-action-chip ${active ? 'active' : ''} ${loading ? 'loading' : ''}`}
      onClick={onClick}
      disabled={disabled}
    >
      {icon}
      <span>{label}</span>
    </button>
  )
}