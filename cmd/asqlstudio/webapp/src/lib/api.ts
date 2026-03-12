// All engine communication goes through the Wails IPC bridge – no HTTP.
import * as App from '../wailsjs/wailsjs/go/main/App'

function parseURL(path: string): { base: string; params: URLSearchParams } {
  const [base, search] = path.split('?')
  return { base, params: new URLSearchParams(search || '') }
}

export async function api<T>(path: string, _method = 'GET', body?: unknown): Promise<T> {
  const { base, params } = parseURL(path)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const b = (body ?? {}) as any

  switch (base) {
    // ── Lifecycle ──────────────────────────────────
    case '/api/health':
      return App.Health() as Promise<T>
    // ── Domains ────────────────────────────────────
    case '/api/domains':
      return App.Domains() as Promise<T>
    // ── Transactions ───────────────────────────────
    case '/api/begin':
      return App.Begin(b) as Promise<T>
    case '/api/execute':
      return App.Execute(b) as Promise<T>
    case '/api/execute-batch':
      return App.ExecuteBatch(b) as Promise<T>
    case '/api/commit':
      return App.Commit(b) as Promise<T>
    case '/api/rollback':
      return App.Rollback(b) as Promise<T>
    // ── Queries ────────────────────────────────────
    case '/api/read-query':
      return App.ReadQuery(b) as Promise<T>
    case '/api/time-travel':
      return App.TimeTravel(b) as Promise<T>
    case '/api/row-history':
      return App.RowHistory(b) as Promise<T>
    case '/api/entity-version-history':
      return App.EntityVersionHistory(b) as Promise<T>
    case '/api/temporal-lookup':
      return App.TemporalLookup(b) as Promise<T>
    case '/api/explain':
      return App.Explain(b) as Promise<T>
    // ── Fixtures ───────────────────────────────────
    case '/api/fixtures/pick-file':
      return App.PickFixtureFile() as Promise<T>
    case '/api/fixtures/pick-export-file':
      return App.PickFixtureExportFile(b.suggested_name ?? '') as Promise<T>
    case '/api/fixtures/validate':
      return App.FixtureValidate(b.file_path ?? '') as Promise<T>
    case '/api/fixtures/load':
      return App.FixtureLoad(b.file_path ?? '') as Promise<T>
    case '/api/fixtures/export':
      return App.FixtureExport(b) as Promise<T>
    // ── Stats ──────────────────────────────────────
    case '/api/read-routing-stats':
      return App.ReadRoutingStats() as Promise<T>
    case '/api/scan-strategy-stats':
      return App.ScanStrategyStats() as Promise<T>
    case '/api/engine-stats':
      return App.EngineStats() as Promise<T>
    case '/api/timeline-commits':
      return App.TimelineCommits(b) as Promise<T>
    // ── Replication ────────────────────────────────
    case '/api/replication/last-lsn':
      return App.ReplicationLastLSN() as Promise<T>
    case '/api/replication/lag':
      return App.ReplicationLag() as Promise<T>
    // ── Cluster ────────────────────────────────────
    case '/api/cluster/groups':
      return App.ClusterGroups() as Promise<T>
    case '/api/cluster/nodes':
      return App.ClusterNodeStatus() as Promise<T>
    case '/api/cluster/status':
      return App.ClusterStatus(params.get('groups') ?? '') as Promise<T>
    case '/api/cluster/diagnostics':
      return App.ClusterDiagnostics() as Promise<T>
    // ── Recovery ───────────────────────────────────
    case '/api/recovery/defaults':
      return App.RecoveryDefaults() as Promise<T>
    case '/api/recovery/create-backup':
      return App.RecoveryCreateBackup(b.data_dir ?? '', b.backup_dir ?? '') as Promise<T>
    case '/api/recovery/manifest':
      return App.RecoveryBackupManifest(b.backup_dir ?? '') as Promise<T>
    case '/api/recovery/verify':
      return App.RecoveryVerifyBackup(b.backup_dir ?? '') as Promise<T>
    case '/api/recovery/snapshot-catalog':
      return App.RecoverySnapshotCatalog(b.data_dir ?? '') as Promise<T>
    case '/api/recovery/wal-retention':
      return App.RecoveryWALRetention(b.data_dir ?? '') as Promise<T>
    case '/api/recovery/restore-lsn':
      return App.RecoveryRestoreLSN(b.backup_dir ?? '', b.data_dir ?? '', Number(b.lsn ?? 0)) as Promise<T>
    case '/api/recovery/restore-timestamp':
      return App.RecoveryRestoreTimestamp(b.backup_dir ?? '', b.data_dir ?? '', Number(b.logical_timestamp ?? 0)) as Promise<T>
    // ── Schema ─────────────────────────────────────
    case '/api/schema/tables':
      return App.SchemaTables(params.get('domain') ?? '') as Promise<T>
    case '/api/schema/load-baseline':
      return App.SchemaLoadBaseline(b) as Promise<T>
    case '/api/schema/load-all-baselines':
      return App.SchemaLoadAllBaselines() as Promise<T>
    case '/api/schema/ddl':
      return App.SchemaDDL(b) as Promise<T>
    case '/api/schema/diff':
      return App.SchemaDiff(b) as Promise<T>
    case '/api/schema/apply':
      return App.SchemaApplyStatements(b as { domain: string; statements: string[] }) as Promise<T>
    case '/api/schema/apply-safe-diff':
      return App.SchemaApplySafeDiff(b) as Promise<T>
    default:
      throw new Error(`[bridge] unknown API path: ${path}`)
  }
}
