import { useMemo, useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { api, type ChangeRecord } from '../services/api'
import DataTable from '../components/DataTable'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'
import KpiStrip from '../components/KpiStrip'
import PageHeader from '../components/PageHeader'

const DOMAINS = [
  'identity', 'staff', 'patients', 'clinical',
  'pharmacy', 'billing', 'scheduling', 'rehab',
  'messaging', 'documents',
]

const DOMAIN_TABLES: Record<string, string[]> = {
  identity:   ['users'],
  staff:      ['departments', 'staff'],
  patients:   ['patients'],
  clinical:   ['consultation_rooms', 'appointments', 'operating_rooms', 'surgeries', 'surgery_team', 'wards', 'beds', 'admissions', 'meal_orders', 'care_notes'],
  pharmacy:   ['medications', 'prescriptions', 'pharmacy_dispenses'],
  billing:    ['invoices', 'invoice_items'],
  scheduling: ['guard_shifts'],
  rehab:      ['rehab_plans', 'rehab_sessions'],
  messaging:  ['messages', 'patient_communications'],
  documents:  ['documents', 'document_access'],
}

const opConfig: Record<string, { label: string; bg: string }> = {
  INSERT: { label: 'INSERT', bg: 'bg-emerald-50 text-emerald-700 ring-1 ring-inset ring-emerald-200' },
  UPDATE: { label: 'UPDATE', bg: 'bg-amber-50 text-amber-700 ring-1 ring-inset ring-amber-200' },
  DELETE: { label: 'DELETE', bg: 'bg-red-50 text-red-700 ring-1 ring-inset ring-red-200' },
}

export default function AuditPage() {
  const [domain, setDomain] = useState(DOMAINS[0])
  const [table, setTable] = useState(DOMAIN_TABLES[DOMAINS[0]][0])
  const [entityId, setEntityId] = useState('')
  const [records, setRecords] = useState<ChangeRecord[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [operationFilter, setOperationFilter] = useState<'ALL' | string>('ALL')
  const [columnSearch, setColumnSearch] = useState('')
  const [selectedRecordId, setSelectedRecordId] = useState<string | null>(null)

  function handleDomainChange(d: string) {
    setDomain(d)
    setTable(DOMAIN_TABLES[d][0])
    setRecords([])
    setError('')
  }

  async function loadHistory() {
    setLoading(true)
    setError('')
    try {
      const path = entityId.trim()
        ? `/asql/audit/${domain}/${table}/${entityId.trim()}`
        : `/asql/audit/${domain}/${table}`
      const res = await api.get<ChangeRecord[]>(path)
      setRecords(res.data ?? [])
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Error al cargar historial')
    } finally {
      setLoading(false)
    }
  }

  const allColumns = useMemo(() => [...new Set(records.flatMap(r => Object.keys(r.columns)))], [records])
  const auditRows = useMemo(() => records.map((record, index) => ({ id: `${record.commit_lsn}-${index}`, ...record })), [records])
  const filteredRows = useMemo(() => {
    const query = columnSearch.trim().toLowerCase()
    return auditRows.filter((row) => {
      const matchesOperation = operationFilter === 'ALL' || row.operation === operationFilter
      const matchesQuery = !query || Object.entries(row.columns).some(([key, value]) => `${key} ${value}`.toLowerCase().includes(query))
      return matchesOperation && matchesQuery
    })
  }, [auditRows, columnSearch, operationFilter])
  const selectedRow = useMemo(() => filteredRows.find((row) => row.id === selectedRecordId) ?? filteredRows[0] ?? null, [filteredRows, selectedRecordId])
  const opCounts = useMemo(() => ({
    insert: records.filter((record) => record.operation === 'INSERT').length,
    update: records.filter((record) => record.operation === 'UPDATE').length,
    delete: records.filter((record) => record.operation === 'DELETE').length,
  }), [records])

  const columns = [
    { key: 'operation' as const, label: 'Operación', render: (v: string) => <span className={`inline-block text-[11px] font-bold px-2 py-0.5 rounded-md ${opConfig[v]?.bg ?? 'bg-surface-100 text-surface-700'}`}>{v}</span> },
    { key: 'commit_lsn' as const, label: 'LSN', render: (v: number) => <span className="font-mono text-xs text-primary-600 font-medium">{v}</span> },
    { key: 'id' as const, label: 'Columnas', render: (_v: string, row: { id: string; operation: string; commit_lsn: number; columns: Record<string, string> }) => <span className="text-surface-600 text-xs">{Object.keys(row.columns).length} campos</span> },
  ]

  return (
    <div>
      <PageHeader title="Auditoría — FOR HISTORY" subtitle="Registro completo de cambios (INSERT / UPDATE / DELETE) en cualquier tabla de cualquier dominio ASQL." />

      <KpiStrip
        items={[
          { label: 'Cambios', value: records.length, hint: 'cargados en vista', tone: 'info' },
          { label: 'INSERT', value: opCounts.insert, hint: 'altas', tone: 'success' },
          { label: 'UPDATE', value: opCounts.update, hint: 'mutaciones', tone: 'warning' },
          { label: 'DELETE', value: opCounts.delete, hint: 'bajas', tone: opCounts.delete > 0 ? 'danger' : 'default' },
        ]}
      />

      {/* Controls */}
      <div className="card p-5 mb-6">
        <p className="text-xs text-surface-400 mb-4">
          Cada fila muestra la operación, la posición WAL (<code className="text-[11px] bg-surface-100 px-1.5 py-0.5 rounded font-mono">commit_lsn</code>)
          y los valores de cada columna en ese punto.
        </p>
        <div className="flex flex-wrap gap-4 items-end">
          <div>
            <label className="block text-xs font-medium text-surface-500 mb-1.5 uppercase tracking-wider">Dominio</label>
            <select className="input" value={domain} onChange={e => handleDomainChange(e.target.value)}>
              {DOMAINS.map(d => <option key={d} value={d}>{d}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-surface-500 mb-1.5 uppercase tracking-wider">Tabla</label>
            <select className="input" value={table} onChange={e => { setTable(e.target.value); setRecords([]) }}>
              {(DOMAIN_TABLES[domain] ?? []).map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div className="flex-1 min-w-[200px]">
            <label className="block text-xs font-medium text-surface-500 mb-1.5 uppercase tracking-wider">ID Entidad (opcional)</label>
            <input className="input w-full" placeholder="UUID para filtrar por entidad..." value={entityId} onChange={e => setEntityId(e.target.value)} />
          </div>
          <button onClick={loadHistory} disabled={loading} className="btn-primary disabled:opacity-50">
            {loading ? (
              <span className="flex items-center gap-2">
                <svg className="w-4 h-4 animate-spin" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" /><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" /></svg>
                Cargando…
              </span>
            ) : 'Consultar historial'}
          </button>
        </div>
      </div>

      <div className="card p-4 mb-6 grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Contexto</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{domain}.{table}</p>
          <p className="text-sm text-surface-500 mt-1">{entityId.trim() ? `filtrado por ${entityId.trim().slice(0, 8)}…` : 'sin filtro de entidad'}</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Último LSN</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{records[0]?.commit_lsn ?? 'Sin datos'}</p>
          <p className="text-sm text-surface-500 mt-1">posición WAL del primer registro visible</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Cobertura</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{allColumns.length} columnas detectadas</p>
          <p className="text-sm text-surface-500 mt-1">profundidad del diff visible en la consulta actual</p>
        </div>
      </div>

      {records.length > 0 && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={operationFilter} onChange={(e) => setOperationFilter(e.target.value)}>
            <option value="ALL">Todas las operaciones</option>
            {Object.keys(opConfig).map((op) => (
              <option key={op} value={op}>{op}</option>
            ))}
          </select>
          <input className="input max-w-sm" placeholder="Buscar por columna o valor" value={columnSearch} onChange={(e) => setColumnSearch(e.target.value)} />
          <span className="text-xs text-surface-400">{filteredRows.length} cambios visibles</span>
        </div>
      )}

      <AnimatePresence>
        {error && (
          <motion.div initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            className="bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 mb-4 text-sm">
            {error}
          </motion.div>
        )}
      </AnimatePresence>

      {/* Results */}
      <AnimatePresence mode="wait">
        {records.length > 0 ? (
          <motion.div key="results" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }} className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.6fr)_420px] gap-6 items-start">
            <DataTable columns={columns} data={filteredRows} loading={loading} emptyMessage="No hay cambios para los filtros activos." emptyIcon="🧾" onRowClick={(row) => setSelectedRecordId(row.id)} selectedRowId={selectedRow?.id ?? null} />
            <DetailPanel isEmpty={!selectedRow} emptyTitle="Selecciona un cambio" emptyText="Consulta el detalle de columnas y valores del registro de auditoría." title={selectedRow ? `${selectedRow.operation} · LSN ${selectedRow.commit_lsn}` : undefined} subtitle={selectedRow ? `${domain}.${table}` : undefined}>
              {selectedRow && (
                <>
                  <DetailGroup title="Contexto">
                    <DetailRow label="Operación" value={selectedRow.operation} accent />
                    <DetailRow label="LSN" value={selectedRow.commit_lsn} mono />
                    <DetailRow label="Columnas" value={Object.keys(selectedRow.columns).length} />
                  </DetailGroup>
                  <DetailGroup title="Valores">
                    {Object.entries(selectedRow.columns).map(([key, value]) => (
                      <DetailRow key={key} label={key} value={value || '—'} mono />
                    ))}
                  </DetailGroup>
                </>
              )}
            </DetailPanel>
          </motion.div>
        ) : !loading && (
          <motion.div key="empty" initial={{ opacity: 0 }} animate={{ opacity: 1 }}
            className="card p-12 text-center">
            <div className="text-4xl mb-3">🔍</div>
            <p className="text-surface-400">Selecciona un dominio y tabla, luego pulsa "Consultar historial" para ver los cambios registrados por ASQL.</p>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}
