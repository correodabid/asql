import { useEffect, useMemo, useState } from 'react'
import { IconAlertTriangle, IconCheck, IconDownload, IconRefresh, IconUpload } from './Icons'
import { api } from '../lib/api'

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
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')

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
      setError(err instanceof Error ? err.message : String(err))
    })
    return () => {
      active = false
    }
  }, [])

  const summary = useMemo(() => ({
    snapshots: manifest?.snapshots?.length ?? 0,
    walSegments: manifest?.wal_segments?.length ?? 0,
    headLSN: manifest?.head_lsn ?? 0,
    headTimestamp: manifest?.head_timestamp ?? 0,
  }), [manifest])

  const run = async (label: string, fn: () => Promise<void>) => {
    setBusy(label)
    setError('')
    setMessage('')
    try {
      await fn()
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setBusy('')
    }
  }

  return (
    <div className="panel" style={{ margin: 16, padding: 16, display: 'grid', gap: 16 }}>
      <div>
        <h2 style={{ margin: '0 0 8px' }}>Recovery</h2>
        <p className="text-muted" style={{ margin: 0 }}>
          Create, verify, inspect, and restore Epic X base backups directly from Studio.
        </p>
      </div>

      <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))' }}>
        <label style={{ display: 'grid', gap: 6 }}>
          <span>Source data directory</span>
          <input className="title-domain-select" value={dataDir} onChange={(e) => setDataDir(e.target.value)} />
        </label>
        <label style={{ display: 'grid', gap: 6 }}>
          <span>Backup directory</span>
          <input className="title-domain-select" value={backupDir} onChange={(e) => setBackupDir(e.target.value)} />
        </label>
        <label style={{ display: 'grid', gap: 6 }}>
          <span>Restore target directory</span>
          <input className="title-domain-select" value={restoreDir} onChange={(e) => setRestoreDir(e.target.value)} />
        </label>
      </div>

      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        <button
          className="toolbar-btn"
          disabled={busy !== ''}
          onClick={() => run('backup', async () => {
            const resp = await api<BackupManifest>('/api/recovery/create-backup', 'POST', { data_dir: dataDir, backup_dir: backupDir })
            setManifest(resp)
            setVerifyStatus('')
            setLastRestore(null)
            setMessage('Base backup created successfully.')
          })}
        >
          <IconDownload /> Create backup
        </button>
        <button
          className="toolbar-btn"
          disabled={busy !== ''}
          onClick={() => run('manifest', async () => {
            const resp = await api<BackupManifest>('/api/recovery/manifest', 'POST', { backup_dir: backupDir })
            setManifest(resp)
            setMessage('Loaded backup manifest.')
          })}
        >
          <IconRefresh /> Load manifest
        </button>
        <button
          className="toolbar-btn"
          disabled={busy !== ''}
          onClick={() => run('verify', async () => {
            const resp = await api<{ status?: string; manifest?: BackupManifest }>('/api/recovery/verify', 'POST', { backup_dir: backupDir })
            setVerifyStatus(resp.status || 'OK')
            setManifest(resp.manifest || null)
            setMessage('Backup verification passed.')
          })}
        >
          <IconCheck /> Verify backup
        </button>
        <button
          className="toolbar-btn"
          disabled={busy !== ''}
          onClick={() => run('diagnostics', async () => {
            const [catalogResp, retentionResp] = await Promise.all([
              api<{ snapshots?: SnapshotCatalogEntry[] }>('/api/recovery/snapshot-catalog', 'POST', { data_dir: dataDir }),
              api<WALRetentionState>('/api/recovery/wal-retention', 'POST', { data_dir: dataDir }),
            ])
            setSnapshotCatalog(catalogResp.snapshots || [])
            setWalRetention(retentionResp)
            setMessage('Loaded local recovery diagnostics.')
          })}
        >
          <IconRefresh /> Inspect data dir
        </button>
      </div>

      <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
        <MetricCard label="Snapshots" value={String(summary.snapshots)} />
        <MetricCard label="WAL segments" value={String(summary.walSegments)} />
        <MetricCard label="Head LSN" value={String(summary.headLSN)} />
        <MetricCard label="Head timestamp" value={String(summary.headTimestamp)} />
      </div>

	  <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))' }}>
		<MetricCard label="Catalog snapshots" value={String(snapshotCatalog.length)} />
		<MetricCard label="Retained WAL segments" value={String(walRetention?.segment_count ?? 0)} />
		<MetricCard label="Oldest retained LSN" value={String(walRetention?.oldest_retained_lsn ?? 0)} />
		<MetricCard label="Last retained LSN" value={String(walRetention?.last_retained_lsn ?? 0)} />
	  </div>

      <div style={{ display: 'grid', gap: 12, gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))', alignItems: 'end' }}>
        <label style={{ display: 'grid', gap: 6 }}>
          <span>Restore to LSN</span>
          <input className="title-domain-select" value={restoreLSN} onChange={(e) => setRestoreLSN(e.target.value)} placeholder="123" />
        </label>
        <button
          className="toolbar-btn"
          disabled={busy !== '' || !restoreLSN.trim()}
          onClick={() => run('restore-lsn', async () => {
            const resp = await api<RestoreResult>('/api/recovery/restore-lsn', 'POST', { backup_dir: backupDir, data_dir: restoreDir, lsn: Number(restoreLSN) })
            setLastRestore(resp)
            setMessage(`Restore to LSN ${restoreLSN} completed.`)
          })}
        >
          <IconUpload /> Restore LSN
        </button>
        <label style={{ display: 'grid', gap: 6 }}>
          <span>Restore to timestamp</span>
          <input className="title-domain-select" value={restoreTimestamp} onChange={(e) => setRestoreTimestamp(e.target.value)} placeholder="123" />
        </label>
        <button
          className="toolbar-btn"
          disabled={busy !== '' || !restoreTimestamp.trim()}
          onClick={() => run('restore-ts', async () => {
            const resp = await api<RestoreResult>('/api/recovery/restore-timestamp', 'POST', { backup_dir: backupDir, data_dir: restoreDir, logical_timestamp: Number(restoreTimestamp) })
            setLastRestore(resp)
            setMessage(`Restore to timestamp ${restoreTimestamp} completed.`)
          })}
        >
          <IconUpload /> Restore timestamp
        </button>
      </div>

      {verifyStatus && (
        <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
          <strong>Verification:</strong> {verifyStatus}
        </div>
      )}

      {lastRestore && (
        <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
          <strong>Last restore:</strong> applied LSN {lastRestore.AppliedLSN ?? 0}, timestamp {lastRestore.AppliedTimestamp ?? 0}
        </div>
      )}

      {message && (
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', color: 'var(--success, #10b981)' }}>
          <IconCheck />
          <span>{message}</span>
        </div>
      )}

      {error && (
        <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start', color: 'var(--danger, #ef4444)' }}>
          <IconAlertTriangle />
          <span>{error}</span>
        </div>
      )}

      <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
        <h3 style={{ marginTop: 0 }}>Manifest preview</h3>
        <pre style={{ margin: 0, whiteSpace: 'pre-wrap', overflowX: 'auto' }}>
          {manifest ? JSON.stringify(manifest, null, 2) : 'Load or create a backup to inspect its manifest.'}
        </pre>
      </div>

      <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
        <h3 style={{ marginTop: 0 }}>Retention and snapshot diagnostics</h3>
        <pre style={{ margin: 0, whiteSpace: 'pre-wrap', overflowX: 'auto' }}>
          {JSON.stringify({ snapshots: snapshotCatalog, wal_retention: walRetention }, null, 2)}
        </pre>
      </div>
    </div>
  )
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="panel" style={{ padding: 12, border: '1px solid var(--border)', background: 'var(--panel-2)' }}>
      <div className="text-muted" style={{ fontSize: 12, marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: 22, fontWeight: 700 }}>{value}</div>
    </div>
  )
}