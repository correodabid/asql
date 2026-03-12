import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { AnimatePresence } from 'framer-motion'
import KpiStrip from '../components/KpiStrip'
import { deriveCareFlows, type CareFlow } from '../lib/careFlows'
import {
  api,
  type Admission,
  type Appointment,
  type Document,
  type Invoice,
  type Patient,
  type Prescription,
  type RehabPlan,
  type Surgery,
} from '../services/api'
import DataTable from '../components/DataTable'
import PageHeader, { SearchInput, FormCard } from '../components/PageHeader'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'

interface PatientCareContext {
  appointments: Appointment[]
  admissions: Admission[]
  surgeries: Surgery[]
  rehabPlans: RehabPlan[]
  prescriptions: Prescription[]
  invoices: Invoice[]
  documents: Document[]
}

const emptyCareContext: PatientCareContext = {
  appointments: [],
  admissions: [],
  surgeries: [],
  rehabPlans: [],
  prescriptions: [],
  invoices: [],
  documents: [],
}

const flowKindLabel: Record<CareFlow['kind'], string> = {
  hospitalization: 'Hospitalización',
  surgery: 'Quirúrgico',
  rehab: 'Rehabilitación',
  ambulatory: 'Ambulatorio',
  medication: 'Farmacológico',
  mixed: 'Mixto',
}

const flowStatusLabel: Record<CareFlow['status'], string> = {
  active: 'Activo',
  planned: 'Planificado',
  completed: 'Cerrado',
}

const flowStatusClass: Record<CareFlow['status'], string> = {
  active: 'bg-emerald-50 text-emerald-700 ring-1 ring-inset ring-emerald-200',
  planned: 'bg-blue-50 text-blue-700 ring-1 ring-inset ring-blue-200',
  completed: 'bg-surface-100 text-surface-600 ring-1 ring-inset ring-surface-200',
}

const eventTypeLabel = {
  admission: 'Ingreso',
  appointment: 'Cita',
  surgery: 'Cirugía',
  rehab: 'Rehab',
  prescription: 'Prescripción',
  invoice: 'Factura',
  document: 'Documento',
} as const

function formatDate(value?: string, withTime = false) {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return withTime ? date.toLocaleString('es-ES') : date.toLocaleDateString('es-ES')
}

export default function PatientsPage() {
  const location = useLocation()
  const focusState = location.state as { focusType?: string; focusId?: string } | null
  const navigate = useNavigate()
  const [patients, setPatients] = useState<Patient[]>([])
  const [loading, setLoading] = useState(true)
  const [search, setSearch] = useState('')
  const [showForm, setShowForm] = useState(false)
  const [selectedPatient, setSelectedPatient] = useState<Patient | null>(null)
  const [careContext, setCareContext] = useState<PatientCareContext>(emptyCareContext)
  const [careLoading, setCareLoading] = useState(false)
  const [selectedFlowId, setSelectedFlowId] = useState<string | null>(null)

  useEffect(() => { loadPatients() }, [])
  useEffect(() => {
    if (focusState?.focusType !== 'patient' || !focusState.focusId || patients.length === 0) return
    const focusedPatient = patients.find((patient) => patient.id === focusState.focusId)
    if (focusedPatient) {
      setSelectedPatient(focusedPatient)
    }
  }, [focusState, patients])
  useEffect(() => {
    if (!selectedPatient) {
      setCareContext(emptyCareContext)
      setSelectedFlowId(null)
      return
    }
    void loadPatientCareContext(selectedPatient.id)
  }, [selectedPatient?.id])

  const careFlows = useMemo(
    () => deriveCareFlows(selectedPatient, careContext),
    [selectedPatient, careContext],
  )

  const selectedFlow = useMemo(
    () => careFlows.find((flow) => flow.id === selectedFlowId) ?? careFlows[0] ?? null,
    [careFlows, selectedFlowId],
  )

  useEffect(() => {
    if (!careFlows.length) {
      setSelectedFlowId(null)
      return
    }
    setSelectedFlowId((current) => careFlows.some((flow) => flow.id === current) ? current : careFlows[0].id)
  }, [careFlows])

  async function loadPatients() {
    setLoading(true)
    try {
      const res = await api.get<Patient[]>('/patients')
      const items = res.data ?? []
      setPatients(items)
      setSelectedPatient((current) => current && items.find((item) => item.id === current.id) ? current : items[0] ?? null)
    } finally { setLoading(false) }
  }

  async function handleSearch() {
    if (!search.trim()) { loadPatients(); return }
    setLoading(true)
    try {
      const res = await api.get<Patient[]>(`/patients/search?q=${encodeURIComponent(search)}`)
      const items = res.data ?? []
      setPatients(items)
      setSelectedPatient(items[0] ?? null)
    } finally { setLoading(false) }
  }

  async function loadPatientCareContext(patientId: string) {
    setCareLoading(true)
    try {
      const [appointments, admissions, surgeries, rehabPlans, prescriptions, invoices, documents] = await Promise.allSettled([
        api.get<Appointment[]>(`/appointments/patient/${patientId}`),
        api.get<Admission[]>(`/admissions/patient/${patientId}`),
        api.get<Surgery[]>(`/surgery/procedures/patient/${patientId}`),
        api.get<RehabPlan[]>(`/rehab/plans/patient/${patientId}`),
        api.get<Prescription[]>(`/pharmacy/prescriptions/patient/${patientId}`),
        api.get<Invoice[]>(`/billing/invoices/patient/${patientId}`),
        api.get<Document[]>(`/documents/patient/${patientId}`),
      ])

      setCareContext({
        appointments: appointments.status === 'fulfilled' ? (appointments.value.data ?? []) : [],
        admissions: admissions.status === 'fulfilled' ? (admissions.value.data ?? []) : [],
        surgeries: surgeries.status === 'fulfilled' ? (surgeries.value.data ?? []) : [],
        rehabPlans: rehabPlans.status === 'fulfilled' ? (rehabPlans.value.data ?? []) : [],
        prescriptions: prescriptions.status === 'fulfilled' ? (prescriptions.value.data ?? []) : [],
        invoices: invoices.status === 'fulfilled' ? (invoices.value.data ?? []) : [],
        documents: documents.status === 'fulfilled' ? (documents.value.data ?? []) : [],
      })
    } finally {
      setCareLoading(false)
    }
  }

  const columns = [
    { key: 'medical_record_no' as const, label: 'Nº Historia', render: (v: string) =>
      <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-600">{v}</span>
    },
    { key: 'first_name' as const, label: 'Nombre', render: (_v: string, row: Patient) =>
      <div>
        <span className="font-medium text-surface-800">{row.first_name} {row.last_name}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{row.email}</p>
      </div>
    },
    { key: 'national_id' as const, label: 'DNI/NIE' },
    { key: 'date_of_birth' as const, label: 'Fecha Nac.', render: (v: string) => v?.slice(0, 10) },
    { key: 'gender' as const, label: 'Género', render: (v: string) =>
      <span className={`badge ${v === 'MALE' ? 'bg-blue-50 text-blue-600' : v === 'FEMALE' ? 'bg-pink-50 text-pink-600' : 'bg-surface-100 text-surface-600'}`}>
        {v === 'MALE' ? 'M' : v === 'FEMALE' ? 'F' : v}
      </span>
    },
    { key: 'phone' as const, label: 'Teléfono' },
    { key: 'blood_type' as const, label: 'Grupo', render: (v: string) =>
      v ? <span className="badge bg-red-50 text-red-600 ring-1 ring-inset ring-red-200">{v}</span> : <span className="text-surface-300">—</span>
    },
    { key: 'insurance_company' as const, label: 'Aseguradora' },
  ]

  return (
    <div>
      <PageHeader title="Pacientes y flujos asistenciales" subtitle="Cada paciente puede recorrer múltiples episodios asistenciales a lo largo de su vida clínica" action="Nuevo paciente" onAction={() => setShowForm(!showForm)}>
        <SearchInput value={search} onChange={setSearch} onSearch={handleSearch} placeholder="Buscar por nombre, DNI, historia clínica…" />
      </PageHeader>

    {selectedPatient && (
      <KpiStrip
        items={[
          { label: 'Flujos detectados', value: careFlows.length, hint: 'episodios longitudinales', tone: 'info' },
          { label: 'Flujo activo', value: careFlows.filter((flow) => flow.status === 'active').length, hint: 'requieren seguimiento', tone: 'success' },
          { label: 'Actividad clínica', value: careFlows.reduce((sum, flow) => sum + flow.eventCount, 0), hint: 'hitos agregados', tone: 'default' },
          { label: 'Facturación agregada', value: `${careFlows.reduce((sum, flow) => sum + flow.billedTotal, 0).toFixed(2)} €`, hint: 'importe cruzado por episodios', tone: 'warning' },
        ]}
      />
    )}

      <AnimatePresence>
        {showForm && (
          <PatientForm
            onSave={async (data) => { await api.post('/patients', data); setShowForm(false); loadPatients() }}
            onCancel={() => setShowForm(false)}
          />
        )}
      </AnimatePresence>

      <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_360px] gap-6 items-start">
        <DataTable
          columns={columns}
          data={patients}
          loading={loading}
          emptyMessage="No hay pacientes registrados."
          emptyIcon="❤️"
          onRowClick={setSelectedPatient}
          selectedRowId={selectedPatient?.id ?? null}
        />
        <DetailPanel
          isEmpty={!selectedPatient}
          title={selectedPatient ? `${selectedPatient.first_name} ${selectedPatient.last_name}` : undefined}
          subtitle={selectedPatient ? selectedPatient.medical_record_no : undefined}
          emptyText="Selecciona un paciente para ver datos clínicos y administrativos en contexto."
        >
          {selectedPatient && (
            <>
              <DetailGroup title="Identificación">
                <DetailRow label="DNI/NIE" value={selectedPatient.national_id || '—'} mono />
                <DetailRow label="Fecha nacimiento" value={selectedPatient.date_of_birth?.slice(0, 10) || '—'} />
                <DetailRow label="Género" value={selectedPatient.gender || '—'} />
                <DetailRow label="Grupo" value={selectedPatient.blood_type || 'No informado'} accent />
              </DetailGroup>
              <DetailGroup title="Contacto">
                <DetailRow label="Teléfono" value={selectedPatient.phone || '—'} mono />
                <DetailRow label="Email" value={selectedPatient.email || '—'} />
                <DetailRow label="Ciudad" value={selectedPatient.city || '—'} />
              </DetailGroup>
              <DetailGroup title="Riesgo y cobertura">
                <DetailRow label="Alergias" value={selectedPatient.allergies || 'Sin alergias registradas'} />
                <DetailRow label="Aseguradora" value={selectedPatient.insurance_company || 'Sin aseguradora'} />
                <DetailRow label="Póliza" value={selectedPatient.insurance_id || '—'} mono />
              </DetailGroup>
        <DetailGroup title="Portafolio de flujos asistenciales">
          {careLoading ? (
            <div className="rounded-2xl border border-dashed border-surface-200 bg-surface-50/70 px-4 py-5 text-sm text-surface-500">
              Construyendo episodios asistenciales del paciente…
            </div>
          ) : careFlows.length === 0 ? (
            <div className="rounded-2xl border border-dashed border-surface-200 bg-surface-50/70 px-4 py-5 text-sm text-surface-500">
              Todavía no hay actividad suficiente para inferir flujos asistenciales en este paciente.
            </div>
          ) : (
            <div className="space-y-2">
              {careFlows.map((flow) => (
                <button
                  key={flow.id}
                  type="button"
                  onClick={() => setSelectedFlowId(flow.id)}
                  className={`w-full rounded-2xl border px-4 py-3 text-left transition ${selectedFlow?.id === flow.id ? 'border-primary-300 bg-primary-50/80 shadow-[0_16px_40px_rgba(59,130,246,0.12)]' : 'border-surface-200 bg-white hover:border-surface-300 hover:bg-surface-50/80'}`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold text-surface-900">{flow.title}</p>
                      <p className="mt-1 text-xs text-surface-500">{formatDate(flow.start)}{flow.end ? ` → ${formatDate(flow.end)}` : ''}</p>
                    </div>
                    <div className="flex flex-col items-end gap-1">
                      <span className={`badge ${flowStatusClass[flow.status]}`}>{flowStatusLabel[flow.status]}</span>
                      <span className="text-[11px] uppercase tracking-[0.18em] text-surface-400">{flowKindLabel[flow.kind]}</span>
                    </div>
                  </div>
                  <p className="mt-2 text-xs text-surface-600">{flow.summary}</p>
                  <div className="mt-3 flex flex-wrap gap-2 text-[11px] text-surface-500">
                    <span className="badge bg-surface-100 text-surface-600">{flow.eventCount} hitos</span>
                    <span className="badge bg-amber-50 text-amber-700">{flow.billedTotal.toFixed(2)} €</span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </DetailGroup>
        {selectedFlow && (
          <>
            <DetailGroup title="Flujo seleccionado">
              <DetailRow label="Tipo" value={flowKindLabel[selectedFlow.kind]} accent />
              <DetailRow label="Estado" value={<span className={`badge ${flowStatusClass[selectedFlow.status]}`}>{flowStatusLabel[selectedFlow.status]}</span>} />
              <DetailRow label="Ventana temporal" value={`${formatDate(selectedFlow.start)}${selectedFlow.end ? ` → ${formatDate(selectedFlow.end)}` : ''}`} />
              <DetailRow label="Resumen" value={selectedFlow.summary} />
            </DetailGroup>
            <DetailGroup title="Cobertura del episodio">
              <DetailRow label="Ingresos" value={selectedFlow.counts.admission} mono />
              <DetailRow label="Cirugías" value={selectedFlow.counts.surgery} mono />
              <DetailRow label="Rehab" value={selectedFlow.counts.rehab} mono />
              <DetailRow label="Citas" value={selectedFlow.counts.appointment} mono />
              <DetailRow label="Prescripciones" value={selectedFlow.counts.prescription} mono />
              <DetailRow label="Documentos" value={selectedFlow.counts.document} mono />
              <DetailRow label="Facturación" value={`${selectedFlow.billedTotal.toFixed(2)} €`} accent />
            </DetailGroup>
            <DetailGroup title="Timeline del flujo">
              <div className="space-y-2 max-h-[28rem] overflow-auto pr-1">
                {selectedFlow.events.map((event) => (
                  <div key={event.id} className="rounded-2xl border border-surface-200 bg-white px-4 py-3">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <p className="text-sm font-semibold text-surface-900">{event.title}</p>
                        <p className="mt-1 text-xs text-surface-500">{event.subtitle}</p>
                      </div>
                      <div className="text-right">
                        <span className="badge bg-surface-100 text-surface-600">{eventTypeLabel[event.type]}</span>
                        <p className="mt-1 text-[11px] text-surface-500">{formatDate(event.date, true)}</p>
                      </div>
                    </div>
                    <div className="mt-3 flex flex-wrap items-center gap-2 text-[11px] text-surface-500">
                      <span className="badge bg-surface-100 text-surface-600">{event.status}</span>
                      {event.amount !== undefined && <span className="badge bg-amber-50 text-amber-700">{event.amount.toFixed(2)} €</span>}
                      {event.endDate && <span>{formatDate(event.endDate, true)}</span>}
                    </div>
                  </div>
                ))}
              </div>
            </DetailGroup>
          </>
        )}
        <DetailGroup title="Acciones relacionadas">
                  <DetailRow
                    label="Citas"
                    value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/appointments', { state: { focusType: 'patient', focusId: selectedPatient.id, openTab: 'appointments' } })}>Ver agenda</button>}
                  />
                  <DetailRow
                    label="Ingresos"
                    value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/admissions', { state: { focusType: 'patient', focusId: selectedPatient.id } })}>Ver hospitalización</button>}
                  />
                  <DetailRow
                    label="Cirugía"
                    value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/surgery', { state: { focusType: 'patient', focusId: selectedPatient.id } })}>Ver quirófano</button>}
                  />
                  <DetailRow
                    label="Facturación"
                    value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/billing', { state: { focusType: 'patient', focusId: selectedPatient.id, openTab: 'invoices' } })}>Ver facturas</button>}
                  />
                </DetailGroup>
            </>
          )}
        </DetailPanel>
      </div>
    </div>
  )
}

function PatientForm({ onSave, onCancel }: { onSave: (data: Record<string, string>) => void; onCancel: () => void }) {
  const [form, setForm] = useState({
    first_name: '', last_name: '', date_of_birth: '', gender: 'MALE', national_id: '',
    phone: '', email: '', address: '', city: '', postal_code: '', blood_type: '',
    allergies: '', emergency_contact_name: '', emergency_contact_phone: '',
    insurance_id: '', insurance_company: '',
  })

  const set = (field: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) =>
    setForm({ ...form, [field]: e.target.value })

  return (
    <FormCard title="Nuevo Paciente" onSave={() => onSave(form)} onCancel={onCancel}>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <input className="input" placeholder="Nombre" value={form.first_name} onChange={set('first_name')} />
        <input className="input" placeholder="Apellidos" value={form.last_name} onChange={set('last_name')} />
        <input className="input" placeholder="DNI/NIE" value={form.national_id} onChange={set('national_id')} />
        <input className="input" type="date" placeholder="Fecha Nacimiento" value={form.date_of_birth} onChange={set('date_of_birth')} />
        <select className="input" value={form.gender} onChange={set('gender')}>
          <option value="MALE">Masculino</option>
          <option value="FEMALE">Femenino</option>
          <option value="OTHER">Otro</option>
        </select>
        <select className="input" value={form.blood_type} onChange={set('blood_type')}>
          <option value="">Grupo Sanguíneo</option>
          <option value="A+">A+</option><option value="A-">A-</option>
          <option value="B+">B+</option><option value="B-">B-</option>
          <option value="AB+">AB+</option><option value="AB-">AB-</option>
          <option value="O+">O+</option><option value="O-">O-</option>
        </select>
        <input className="input" placeholder="Teléfono" value={form.phone} onChange={set('phone')} />
        <input className="input" placeholder="Email" value={form.email} onChange={set('email')} />
        <input className="input" placeholder="Dirección" value={form.address} onChange={set('address')} />
        <input className="input" placeholder="Ciudad" value={form.city} onChange={set('city')} />
        <input className="input" placeholder="Código Postal" value={form.postal_code} onChange={set('postal_code')} />
        <input className="input" placeholder="Alergias" value={form.allergies} onChange={set('allergies')} />
        <input className="input" placeholder="Contacto Emergencia" value={form.emergency_contact_name} onChange={set('emergency_contact_name')} />
        <input className="input" placeholder="Teléfono Emergencia" value={form.emergency_contact_phone} onChange={set('emergency_contact_phone')} />
        <input className="input" placeholder="Nº Póliza" value={form.insurance_id} onChange={set('insurance_id')} />
        <input className="input" placeholder="Aseguradora" value={form.insurance_company} onChange={set('insurance_company')} />
      </div>
    </FormCard>
  )
}
