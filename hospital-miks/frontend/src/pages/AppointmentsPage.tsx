import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { api, type ConsultationRoom } from '../services/api'
import DataTable from '../components/DataTable'
import PageHeader, { FormCard, StatusBadge, TabButton } from '../components/PageHeader'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'

interface Appointment {
  id: string
  patient_id: string
  doctor_id: string
  type: string
  status: string
  scheduled_at: string
  duration_minutes: number
  room: string
  notes: string
  diagnosis: string
}

const typeLabels: Record<string, string> = {
  CONSULTATION: 'Consulta', FOLLOW_UP: 'Seguimiento', EMERGENCY: 'Urgencia',
  LAB_TEST: 'Laboratorio', IMAGING: 'Imagen', PROCEDURE: 'Procedimiento',
}

export default function AppointmentsPage() {
  const location = useLocation()
  const focusState = location.state as { focusType?: string; focusId?: string; openTab?: 'appointments' | 'rooms' } | null
  const navigate = useNavigate()
  const [appointments, setAppointments] = useState<Appointment[]>([])
  const [rooms, setRooms] = useState<ConsultationRoom[]>([])
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'appointments' | 'rooms'>('appointments')
  const [showRoomForm, setShowRoomForm] = useState(false)
  const [selectedAppointment, setSelectedAppointment] = useState<Appointment | null>(null)
  const [selectedRoom, setSelectedRoom] = useState<ConsultationRoom | null>(null)
  const [contextFilter, setContextFilter] = useState<{ type: 'patient' | 'doctor' | null; id: string | null }>({ type: null, id: null })

  useEffect(() => { void Promise.all([loadAppointments(), loadRooms()]) }, [])
  useEffect(() => {
    if (focusState?.openTab) {
      setActiveTab(focusState.openTab)
    }
  }, [focusState])

  useEffect(() => {
    if (focusState?.focusType !== 'appointment' || !focusState.focusId || appointments.length === 0) return
    const focusedAppointment = appointments.find((appointment) => appointment.id === focusState.focusId)
    if (focusedAppointment) {
      setActiveTab('appointments')
      setSelectedAppointment(focusedAppointment)
    }
  }, [appointments, focusState])

  useEffect(() => {
    if (!focusState?.focusId || appointments.length === 0) return
    if (focusState.focusType === 'patient') {
      setContextFilter({ type: 'patient', id: focusState.focusId })
      const focusedAppointment = appointments.find((appointment) => appointment.patient_id === focusState.focusId)
      if (focusedAppointment) {
        setActiveTab('appointments')
        setSelectedAppointment(focusedAppointment)
      }
    }
    if (focusState.focusType === 'doctor') {
      setContextFilter({ type: 'doctor', id: focusState.focusId })
      const focusedAppointment = appointments.find((appointment) => appointment.doctor_id === focusState.focusId)
      if (focusedAppointment) {
        setActiveTab('appointments')
        setSelectedAppointment(focusedAppointment)
      }
    }
  }, [appointments, focusState])

  const filteredAppointments = useMemo(() => {
    if (!contextFilter.type || !contextFilter.id) return appointments
    if (contextFilter.type === 'patient') {
      return appointments.filter((appointment) => appointment.patient_id === contextFilter.id)
    }
    return appointments.filter((appointment) => appointment.doctor_id === contextFilter.id)
  }, [appointments, contextFilter])

  async function loadAppointments() {
    setLoading(true)
    try {
      const res = await api.get<Appointment[]>('/appointments')
      const items = res.data ?? []
      setAppointments(items)
      setSelectedAppointment((current) => current && items.find((item) => item.id === current.id) ? current : items[0] ?? null)
    } finally { setLoading(false) }
  }

  async function loadRooms() {
    try {
      const res = await api.get<ConsultationRoom[]>('/appointments/rooms')
      const items = res.data ?? []
      setRooms(items)
      setSelectedRoom((current) => current && items.find((item) => item.id === current.id) ? current : items[0] ?? null)
    } catch {
      setRooms([])
    }
  }

  const columns = [
    { key: 'scheduled_at' as const, label: 'Fecha/Hora', render: (v: string) =>
      <div>
        <span className="font-medium text-surface-800">{new Date(v).toLocaleDateString('es-ES')}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{new Date(v).toLocaleTimeString('es-ES', { hour: '2-digit', minute: '2-digit' })}</p>
      </div>
    },
    { key: 'type' as const, label: 'Tipo', render: (v: string) =>
      <span className="badge bg-surface-100 text-surface-600">{typeLabels[v] ?? v}</span>
    },
    { key: 'status' as const, label: 'Estado', render: (v: string) => <StatusBadge status={v} /> },
    { key: 'duration_minutes' as const, label: 'Duración', render: (v: number) =>
      <span className="text-surface-600">{v} min</span>
    },
    { key: 'room' as const, label: 'Sala', render: (v: string) =>
      v ? <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-500">{v}</span>
        : <span className="text-surface-300">—</span>
    },
    { key: 'notes' as const, label: 'Notas', render: (v: string) =>
      v ? <span className="text-surface-500 text-xs max-w-[200px] truncate block">{v}</span>
        : <span className="text-surface-300">—</span>
    },
  ]

  const roomColumns = [
    { key: 'code' as const, label: 'Código', render: (v: string) =>
      <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-600">{v}</span>
    },
    { key: 'name' as const, label: 'Sala', render: (v: string, row: ConsultationRoom) =>
      <div>
        <span className="font-medium text-surface-800">{v}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{row.building || 'Hospital'} · Planta {row.floor}</p>
      </div>
    },
    { key: 'equipment' as const, label: 'Equipamiento', render: (v: string) =>
      v ? <span className="text-surface-600 text-xs max-w-[260px] truncate block">{v}</span> : <span className="text-surface-300">—</span>
    },
    { key: 'active' as const, label: 'Estado', render: (v: boolean) =>
      <span className={`badge ${v ? 'bg-emerald-50 text-emerald-600 ring-1 ring-inset ring-emerald-200' : 'bg-surface-100 text-surface-400'}`}>{v ? 'Activa' : 'Inactiva'}</span>
    },
  ]

  return (
    <div>
      <PageHeader
        title="Gestión de Citas"
        subtitle="Programación de citas y gestión de salas de consulta"
        action={activeTab === 'rooms' ? 'Nueva sala' : undefined}
        onAction={activeTab === 'rooms' ? () => setShowRoomForm(!showRoomForm) : undefined}
      >
        <div className="flex gap-1.5">
          <TabButton active={activeTab === 'appointments'} onClick={() => setActiveTab('appointments')}>Citas</TabButton>
          <TabButton active={activeTab === 'rooms'} onClick={() => setActiveTab('rooms')}>Salas</TabButton>
        </div>
      </PageHeader>

      {activeTab === 'rooms' && showRoomForm && (
        <RoomForm
          onSave={async (data) => {
            await api.post('/appointments/rooms', data)
            setShowRoomForm(false)
            await loadRooms()
          }}
          onCancel={() => setShowRoomForm(false)}
        />
      )}

      {activeTab === 'appointments' && contextFilter.type && contextFilter.id && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <span className="badge bg-primary-50 text-primary-700 ring-1 ring-inset ring-primary-200">
            Contexto: {contextFilter.type === 'patient' ? 'Paciente' : 'Profesional'}
          </span>
          <span className="font-mono text-xs text-surface-500">{contextFilter.id.slice(0, 8)}…</span>
          <span className="text-xs text-surface-400">{filteredAppointments.length} citas visibles bajo este contexto</span>
          <button className="btn-secondary !px-3 !py-1.5" onClick={() => setContextFilter({ type: null, id: null })}>Limpiar contexto</button>
        </div>
      )}

      <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_360px] gap-6 items-start">
        {activeTab === 'appointments' ? (
          <DataTable
            columns={columns}
            data={filteredAppointments}
            loading={loading}
            emptyMessage="No hay citas programadas."
            emptyIcon="📅"
            onRowClick={setSelectedAppointment}
            selectedRowId={selectedAppointment?.id ?? null}
          />
        ) : (
          <DataTable
            columns={roomColumns}
            data={rooms}
            loading={loading}
            emptyMessage="No hay salas registradas."
            emptyIcon="🚪"
            onRowClick={setSelectedRoom}
            selectedRowId={selectedRoom?.id ?? null}
          />
        )}

        <DetailPanel
          isEmpty={activeTab === 'appointments' ? !selectedAppointment : !selectedRoom}
          title={activeTab === 'appointments' ? selectedAppointment?.type : selectedRoom?.name}
          subtitle={activeTab === 'appointments' ? selectedAppointment?.scheduled_at && new Date(selectedAppointment.scheduled_at).toLocaleString('es-ES') : selectedRoom?.code}
          emptyText={activeTab === 'appointments' ? 'Selecciona una cita para ver su detalle operativo.' : 'Selecciona una sala para ver su capacidad clínica.'}
        >
          {activeTab === 'appointments' && selectedAppointment && (
            <>
              <DetailGroup title="Agenda clínica">
                <DetailRow label="Estado" value={<StatusBadge status={selectedAppointment.status} />} />
                <DetailRow label="Duración" value={`${selectedAppointment.duration_minutes} min`} mono />
                <DetailRow label="Sala" value={selectedAppointment.room || 'Sin asignar'} mono />
              </DetailGroup>
              <DetailGroup title="Contexto">
                <DetailRow label="Paciente" value={selectedAppointment.patient_id} mono />
                <DetailRow label="Doctor" value={selectedAppointment.doctor_id} mono />
                <DetailRow label="Diagnóstico" value={selectedAppointment.diagnosis || 'Pendiente'} />
                <DetailRow label="Notas" value={selectedAppointment.notes || 'Sin observaciones'} />
              </DetailGroup>
              <DetailGroup title="Acciones relacionadas">
                <DetailRow
                  label="Paciente"
                  value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/patients', { state: { focusType: 'patient', focusId: selectedAppointment.patient_id } })}>Abrir ficha</button>}
                />
                <DetailRow
                  label="Doctor"
                  value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/staff', { state: { focusType: 'staff', focusId: selectedAppointment.doctor_id, openTab: 'staff' } })}>Abrir profesional</button>}
                />
              </DetailGroup>
            </>
          )}
          {activeTab === 'rooms' && selectedRoom && (
            <>
              <DetailGroup title="Infraestructura">
                <DetailRow label="Código" value={selectedRoom.code} mono />
                <DetailRow label="Planta" value={`P${selectedRoom.floor}`} />
                <DetailRow label="Edificio" value={selectedRoom.building || 'Hospital'} />
              </DetailGroup>
              <DetailGroup title="Operación">
                <DetailRow label="Activa" value={selectedRoom.active ? 'Sí' : 'No'} accent />
                <DetailRow label="Departamento" value={selectedRoom.department_id} mono />
                <DetailRow label="Equipamiento" value={selectedRoom.equipment || 'No especificado'} />
              </DetailGroup>
                <DetailGroup title="Acción relacionada">
                  <DetailRow
                    label="Departamento"
                    value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/staff', { state: { focusType: 'department', focusId: selectedRoom.department_id, openTab: 'departments' } })}>Abrir área</button>}
                  />
                </DetailGroup>
            </>
          )}
        </DetailPanel>
      </div>
    </div>
  )
}

function RoomForm({ onSave, onCancel }: { onSave: (data: Record<string, string | number | boolean>) => void; onCancel: () => void }) {
  const [form, setForm] = useState({
    name: '', code: '', department_id: '', floor: 0, building: '', equipment: '', active: true,
  })

  const set = (field: string) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm({
      ...form,
      [field]: field === 'floor' ? Number(e.target.value) : e.target.value,
    })

  return (
    <FormCard title="Nueva Sala de Consulta" onSave={() => onSave(form)} onCancel={onCancel}>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <input className="input" placeholder="Nombre" value={form.name} onChange={set('name')} />
        <input className="input" placeholder="Código" value={form.code} onChange={set('code')} />
        <input className="input" placeholder="Department ID" value={form.department_id} onChange={set('department_id')} />
        <input className="input" type="number" placeholder="Planta" value={form.floor} onChange={set('floor')} />
        <input className="input" placeholder="Edificio" value={form.building} onChange={set('building')} />
        <input className="input md:col-span-3" placeholder="Equipamiento" value={form.equipment} onChange={set('equipment')} />
      </div>
    </FormCard>
  )
}
