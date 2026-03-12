import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { api, type Snapshot, type PatientWithInvoices, type AppointmentWithDetails } from '../services/api'
import PageHeader, { TabButton } from '../components/PageHeader'

type Tab = 'time-travel' | 'cross-domain'

export default function TimeTravelPage() {
  const [tab, setTab] = useState<Tab>('time-travel')

  return (
    <div>
      <PageHeader title="ASQL Explorer" subtitle="Capacidades diferenciadoras de ASQL: Time Travel y Cross-Domain queries.">
        <div className="flex gap-1.5">
          <TabButton active={tab === 'time-travel'} onClick={() => setTab('time-travel')}>Time Travel</TabButton>
          <TabButton active={tab === 'cross-domain'} onClick={() => setTab('cross-domain')}>Cross-Domain</TabButton>
        </div>
      </PageHeader>

      <AnimatePresence mode="wait">
        {tab === 'time-travel' ? (
          <motion.div key="tt" initial={{ opacity: 0, x: -12 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: 12 }}>
            <TimeTravelPanel />
          </motion.div>
        ) : (
          <motion.div key="cd" initial={{ opacity: 0, x: 12 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -12 }}>
            <CrossDomainPanel />
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

// ── Time Travel Panel ───────────────────────────────────────────

type EntityType = 'patients' | 'admissions' | 'prescriptions'

function TimeTravelPanel() {
  const [entityType, setEntityType] = useState<EntityType>('patients')
  const [entityId, setEntityId] = useState('')
  const [lsn, setLsn] = useState('')
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function loadSnapshot() {
    if (!entityId.trim() || !lsn.trim()) {
      setError('ID y LSN son obligatorios')
      return
    }
    setLoading(true); setError(''); setSnapshot(null)
    try {
      const res = await api.get<Snapshot>(`/asql/time-travel/${entityType}/${entityId.trim()}?lsn=${lsn.trim()}`)
      setSnapshot(res.data)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Error al consultar snapshot')
    } finally { setLoading(false) }
  }

  const data = snapshot?.patient ?? snapshot?.data ?? {}
  const columns = Object.entries(data)

  return (
    <div>
      <div className="card p-5 mb-6">
        <p className="text-xs text-surface-400 mb-4">
          Consulta el estado de una entidad en un punto exacto del WAL (Log Sequence Number).
          ASQL devuelve la fila <strong className="text-surface-600">tal como era</strong> en ese LSN — sin necesidad de backups.
        </p>
        <div className="flex flex-wrap gap-4 items-end">
          <div>
            <label className="block text-xs font-medium text-surface-500 mb-1.5 uppercase tracking-wider">Tipo de entidad</label>
            <select className="input" value={entityType} onChange={e => setEntityType(e.target.value as EntityType)}>
              <option value="patients">Paciente</option>
              <option value="admissions">Ingreso</option>
              <option value="prescriptions">Prescripción</option>
            </select>
          </div>
          <div className="flex-1 min-w-[200px]">
            <label className="block text-xs font-medium text-surface-500 mb-1.5 uppercase tracking-wider">ID (UUID)</label>
            <input className="input w-full" placeholder="ej. 0193a1b2-c3d4-..." value={entityId} onChange={e => setEntityId(e.target.value)} />
          </div>
          <div className="w-32">
            <label className="block text-xs font-medium text-surface-500 mb-1.5 uppercase tracking-wider">LSN</label>
            <input className="input w-full" type="number" min="1" placeholder="42" value={lsn} onChange={e => setLsn(e.target.value)} />
          </div>
          <button onClick={loadSnapshot} disabled={loading} className="btn-primary disabled:opacity-50">
            {loading ? 'Cargando…' : 'Ver snapshot'}
          </button>
        </div>
      </div>

      <AnimatePresence>
        {error && (
          <motion.div initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            className="bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 mb-4 text-sm">{error}</motion.div>
        )}
      </AnimatePresence>

      <AnimatePresence mode="wait">
        {snapshot ? (
          <motion.div key="snap" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            className="card overflow-hidden">
            <div className="px-5 py-3 border-b border-surface-100 flex justify-between items-center">
              <span className="text-sm font-medium text-surface-700">
                Snapshot @ LSN <span className="font-mono text-primary-600 font-bold">{snapshot.lsn}</span>
              </span>
              <span className="font-mono text-xs text-surface-400 bg-surface-50 px-2 py-1 rounded-lg">{entityType}</span>
            </div>
            {columns.length > 0 ? (
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-surface-50/80">
                    <th className="px-5 py-3 text-left text-[11px] font-semibold text-surface-400 uppercase tracking-wider">Columna</th>
                    <th className="px-5 py-3 text-left text-[11px] font-semibold text-surface-400 uppercase tracking-wider">Valor</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-surface-100">
                  {columns.map(([col, val]) => (
                    <tr key={col} className="hover:bg-surface-50/50 transition-colors">
                      <td className="px-5 py-2.5 font-mono text-xs text-surface-500 w-48">{col}</td>
                      <td className="px-5 py-2.5 text-surface-800">{val}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p className="p-6 text-surface-400 text-sm">Sin datos en el snapshot.</p>
            )}
          </motion.div>
        ) : !loading && !error && (
          <motion.div key="empty" initial={{ opacity: 0 }} animate={{ opacity: 1 }}
            className="card p-12 text-center">
            <div className="text-4xl mb-3">⏳</div>
            <p className="text-surface-400">Introduce un ID y LSN para viajar en el tiempo a ese punto del WAL.</p>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

// ── Cross-Domain Panel ──────────────────────────────────────────

type CrossDomainQuery = 'patient-invoices' | 'appointment-details'

function CrossDomainPanel() {
  const [queryType, setQueryType] = useState<CrossDomainQuery>('patient-invoices')
  const [entityId, setEntityId] = useState('')
  const [result, setResult] = useState<PatientWithInvoices | AppointmentWithDetails | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  async function loadCrossDomain() {
    if (!entityId.trim()) { setError('El ID es obligatorio'); return }
    setLoading(true); setError(''); setResult(null)
    try {
      const path = queryType === 'patient-invoices'
        ? `/asql/cross-domain/patients/${entityId.trim()}/invoices`
        : `/asql/cross-domain/appointments/${entityId.trim()}/details`
      const res = await api.get(path)
      setResult(res.data as PatientWithInvoices | AppointmentWithDetails)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Error en consulta cross-domain')
    } finally { setLoading(false) }
  }

  return (
    <div>
      <div className="card p-5 mb-6">
        <p className="text-xs text-surface-400 mb-4">
          Las consultas <strong className="text-surface-600">cross-domain</strong> usan <code className="text-[11px] bg-surface-100 px-1.5 py-0.5 rounded font-mono">IMPORT</code> para
          leer datos de otro dominio ASQL sin romper el aislamiento.
        </p>
        <div className="flex flex-wrap gap-4 items-end">
          <div>
            <label className="block text-xs font-medium text-surface-500 mb-1.5 uppercase tracking-wider">Consulta</label>
            <select className="input" value={queryType} onChange={e => { setQueryType(e.target.value as CrossDomainQuery); setResult(null) }}>
              <option value="patient-invoices">Paciente + Facturas (patients → billing)</option>
              <option value="appointment-details">Cita + Detalles (clinical → patients + staff)</option>
            </select>
          </div>
          <div className="flex-1 min-w-[200px]">
            <label className="block text-xs font-medium text-surface-500 mb-1.5 uppercase tracking-wider">
              {queryType === 'patient-invoices' ? 'Patient ID' : 'Appointment ID'} (UUID)
            </label>
            <input className="input w-full" placeholder="UUID..." value={entityId} onChange={e => setEntityId(e.target.value)} />
          </div>
          <button onClick={loadCrossDomain} disabled={loading} className="btn-primary disabled:opacity-50">
            {loading ? 'Cargando…' : 'Consultar'}
          </button>
        </div>
      </div>

      <AnimatePresence>
        {error && (
          <motion.div initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
            className="bg-red-50 border border-red-200 text-red-700 rounded-xl p-4 mb-4 text-sm">{error}</motion.div>
        )}
      </AnimatePresence>

      <AnimatePresence mode="wait">
        {result && queryType === 'patient-invoices' && (
          <motion.div key="pi" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}>
            <PatientInvoicesCard data={result as PatientWithInvoices} />
          </motion.div>
        )}
        {result && queryType === 'appointment-details' && (
          <motion.div key="ad" initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}>
            <AppointmentDetailsCard data={result as AppointmentWithDetails} />
          </motion.div>
        )}
        {!result && !loading && !error && (
          <motion.div key="empty" initial={{ opacity: 0 }} animate={{ opacity: 1 }}
            className="card p-12 text-center">
            <div className="text-4xl mb-3">🔗</div>
            <p className="text-surface-400">Selecciona una consulta cross-domain e introduce un ID para ver datos combinados de múltiples dominios.</p>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

// ── Cards ────────────────────────────────────────────────────────

function PatientInvoicesCard({ data }: { data: PatientWithInvoices }) {
  return (
    <div className="card p-6">
      <h3 className="text-lg font-semibold text-surface-700 mb-5 flex items-center gap-2">
        <span className="w-8 h-8 rounded-lg bg-blue-50 flex items-center justify-center text-blue-600">🔗</span>
        Paciente + Facturación
        <span className="badge bg-blue-50 text-blue-600 ml-auto text-[11px]">IMPORT patients → billing</span>
      </h3>
      <div className="grid grid-cols-2 gap-5">
        <InfoRow label="ID Paciente" value={data.patient_id} mono />
        <InfoRow label="Nombre" value={`${data.first_name} ${data.last_name}`} />
        <InfoRow label="Nº Facturas" value={String(data.invoice_count)} />
        <InfoRow label="Total Facturado" value={`€${data.total_billed.toFixed(2)}`} highlight />
      </div>
    </div>
  )
}

function AppointmentDetailsCard({ data }: { data: AppointmentWithDetails }) {
  return (
    <div className="card p-6">
      <h3 className="text-lg font-semibold text-surface-700 mb-5 flex items-center gap-2">
        <span className="w-8 h-8 rounded-lg bg-purple-50 flex items-center justify-center text-purple-600">🔗</span>
        Cita + Detalles Cross-Domain
        <span className="badge bg-purple-50 text-purple-600 ml-auto text-[11px]">IMPORT clinical → patients + staff</span>
      </h3>
      <div className="grid grid-cols-2 gap-5">
        <InfoRow label="ID Cita" value={data.appointment_id} mono />
        <InfoRow label="Paciente" value={data.patient_name} />
        <InfoRow label="Médico" value={data.doctor_name} />
        <InfoRow label="Tipo" value={data.type} />
        <InfoRow label="Estado" value={data.status} />
        <InfoRow label="Fecha" value={new Date(data.scheduled_at).toLocaleString('es-ES')} />
      </div>
    </div>
  )
}

function InfoRow({ label, value, mono, highlight }: { label: string; value: string; mono?: boolean; highlight?: boolean }) {
  return (
    <div>
      <p className="text-[11px] text-surface-400 uppercase tracking-wider mb-1">{label}</p>
      <p className={`text-sm ${mono ? 'font-mono text-surface-500' : ''} ${highlight ? 'text-xl font-bold gradient-text' : 'text-surface-800'}`}>
        {value}
      </p>
    </div>
  )
}
