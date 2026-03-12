import { useEffect, useMemo, useState } from 'react'
import { api, type Surgery, type SurgeryTeamMember } from '../services/api'
import DataTable from '../components/DataTable'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'
import KpiStrip from '../components/KpiStrip'
import PageHeader, { StatusBadge, TabButton } from '../components/PageHeader'

interface OperatingRoom {
  id: string; name: string; code: string; floor: number; building: string; status: string; capacity: number;
}

export default function SurgeryPage() {
  const [rooms, setRooms] = useState<OperatingRoom[]>([])
  const [surgeries, setSurgeries] = useState<Surgery[]>([])
  const [team, setTeam] = useState<SurgeryTeamMember[]>([])
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)
  const [activeTab, setActiveTab] = useState<'rooms' | 'surgeries' | 'team'>('rooms')
  const [selectedRoom, setSelectedRoom] = useState<OperatingRoom | null>(null)
  const [selectedSurgery, setSelectedSurgery] = useState<Surgery | null>(null)
  const [selectedTeamMember, setSelectedTeamMember] = useState<SurgeryTeamMember | null>(null)
  const [roomStatusFilter, setRoomStatusFilter] = useState<'ALL' | string>('ALL')
  const [surgeryStatusFilter, setSurgeryStatusFilter] = useState<'ALL' | string>('ALL')
  const [teamRoleFilter, setTeamRoleFilter] = useState<'ALL' | string>('ALL')

  useEffect(() => { void loadCurrentTab() }, [activeTab])

  async function loadCurrentTab() {
    if (activeTab === 'rooms') {
      await loadRooms()
      return
    }
    if (activeTab === 'surgeries') {
      await loadSurgeries()
    }
  }

  async function loadRooms() {
    setLoading(true)
    try {
      const res = await api.get<OperatingRoom[]>('/surgery/rooms')
      const nextRooms = res.data ?? []
      setRooms(nextRooms)
      setSelectedRoom((current) => nextRooms.find((room) => room.id === current?.id) ?? current ?? nextRooms[0] ?? null)
    } finally { setLoading(false) }
  }

  async function loadSurgeries() {
    setLoading(true)
    try {
      const res = await api.get<Surgery[]>('/surgery/procedures')
      const nextSurgeries = res.data ?? []
      setSurgeries(nextSurgeries)
      setSelectedSurgery((current) => nextSurgeries.find((surgery) => surgery.id === current?.id) ?? current ?? nextSurgeries[0] ?? null)
    } finally { setLoading(false) }
  }

  async function loadTeamMembers(surgery: Surgery) {
    setSelectedSurgery(surgery)
    setSelectedTeamMember(null)
    setActiveTab('team')
    setDetailLoading(true)
    try {
      const res = await api.get<SurgeryTeamMember[]>(`/surgery/procedures/${surgery.id}/team`)
      const nextTeam = res.data ?? []
      setTeam(nextTeam)
      setSelectedTeamMember(nextTeam[0] ?? null)
    } finally {
      setDetailLoading(false)
    }
  }

  const columns = [
    { key: 'code' as const, label: 'Código', render: (v: string) =>
      <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-600">{v}</span>
    },
    { key: 'name' as const, label: 'Nombre', render: (v: string) =>
      <span className="font-medium text-surface-800">{v}</span>
    },
    { key: 'floor' as const, label: 'Planta', render: (v: number) =>
      <span className="badge bg-surface-100 text-surface-600">P{v}</span>
    },
    { key: 'building' as const, label: 'Edificio' },
    { key: 'status' as const, label: 'Estado', render: (v: string) => <StatusBadge status={v} /> },
    { key: 'capacity' as const, label: 'Capacidad', render: (v: number) =>
      <span className="text-surface-600">{v} pers.</span>
    },
  ]

  const surgeryColumns = [
    { key: 'procedure_name' as const, label: 'Procedimiento', render: (v: string, row: Surgery) =>
      <div>
        <span className="font-medium text-surface-800">{v}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{row.procedure_code || 'Sin código'}</p>
      </div>
    },
    { key: 'status' as const, label: 'Estado', render: (v: string) => <StatusBadge status={v} /> },
    { key: 'scheduled_start' as const, label: 'Inicio', render: (v: string) => <span className="text-surface-600">{new Date(v).toLocaleString('es-ES')}</span> },
    { key: 'scheduled_end' as const, label: 'Fin', render: (v: string) => <span className="text-surface-500">{new Date(v).toLocaleString('es-ES')}</span> },
    { key: 'id' as const, label: 'Equipo', render: (_v: string, row: Surgery) =>
      <button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadTeamMembers(row)}>Ver equipo</button>
    },
  ]

  const teamColumns = [
    { key: 'role' as const, label: 'Rol', render: (v: string) => <span className="badge bg-purple-50 text-purple-600">{v}</span> },
    { key: 'staff_id' as const, label: 'Staff', render: (v: string) => <span className="font-mono text-xs text-surface-500">{v.slice(0, 8)}…</span> },
    { key: 'created_at' as const, label: 'Añadido', render: (v: string) => <span className="text-surface-600">{new Date(v).toLocaleString('es-ES')}</span> },
  ]

  const availableRooms = useMemo(() => rooms.filter((room) => room.status === 'AVAILABLE').length, [rooms])
  const activeSurgeries = useMemo(() => surgeries.filter((surgery) => surgery.status === 'IN_PROGRESS').length, [surgeries])
  const scheduledSurgeries = useMemo(() => surgeries.filter((surgery) => surgery.status === 'SCHEDULED').length, [surgeries])
  const roomStatuses = useMemo(() => Array.from(new Set(rooms.map((room) => room.status))), [rooms])
  const surgeryStatuses = useMemo(() => Array.from(new Set(surgeries.map((surgery) => surgery.status))), [surgeries])
  const teamRoles = useMemo(() => Array.from(new Set(team.map((member) => member.role))), [team])

  const filteredRooms = useMemo(() => rooms.filter((room) => roomStatusFilter === 'ALL' || room.status === roomStatusFilter), [roomStatusFilter, rooms])
  const filteredSurgeries = useMemo(() => surgeries.filter((surgery) => surgeryStatusFilter === 'ALL' || surgery.status === surgeryStatusFilter), [surgeries, surgeryStatusFilter])
  const filteredTeam = useMemo(() => team.filter((member) => teamRoleFilter === 'ALL' || member.role === teamRoleFilter), [team, teamRoleFilter])

  return (
    <div>
      <PageHeader title="Gestión de Quirófanos" subtitle="Salas, procedimientos y equipos quirúrgicos">
        <div className="flex gap-1.5">
          <TabButton active={activeTab === 'rooms'} onClick={() => setActiveTab('rooms')}>Quirófanos</TabButton>
          <TabButton active={activeTab === 'surgeries'} onClick={() => setActiveTab('surgeries')}>Procedimientos</TabButton>
          <TabButton active={activeTab === 'team'} onClick={() => setActiveTab('team')}>Equipo</TabButton>
        </div>
      </PageHeader>

      <KpiStrip
        items={[
          { label: 'Quirófanos', value: rooms.length, hint: 'infraestructura cargada', tone: 'info' },
          { label: 'Disponibles', value: availableRooms, hint: 'listos para uso', tone: 'success' },
          { label: 'Cirugías en curso', value: activeSurgeries, hint: 'ahora mismo', tone: activeSurgeries > 0 ? 'warning' : 'default' },
          { label: 'Programadas', value: scheduledSurgeries, hint: 'pipeline quirúrgico', tone: 'default' },
        ]}
      />

      <div className="card p-4 mb-6 grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Quirófano foco</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{selectedRoom?.name ?? 'Sin selección'}</p>
          <p className="text-sm text-surface-500 mt-1">{selectedRoom ? `${selectedRoom.building} · Planta ${selectedRoom.floor} · ${selectedRoom.status}` : 'Selecciona una sala para ver contexto operativo'}</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Procedimiento foco</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{selectedSurgery?.procedure_name ?? 'Sin selección'}</p>
          <p className="text-sm text-surface-500 mt-1">{selectedSurgery ? `${selectedSurgery.status} · ${new Date(selectedSurgery.scheduled_start).toLocaleString('es-ES')}` : 'Selecciona una cirugía para activar la vista operativa'}</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Equipo visible</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{filteredTeam.length} miembros · {teamRoles.length} roles</p>
          <p className="text-sm text-surface-500 mt-1">composición del equipo bajo el filtro actual</p>
        </div>
      </div>

      {activeTab === 'rooms' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={roomStatusFilter} onChange={(e) => setRoomStatusFilter(e.target.value)}>
            <option value="ALL">Todos los estados</option>
            {roomStatuses.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredRooms.length} quirófanos visibles</span>
        </div>
      )}

      {activeTab === 'surgeries' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={surgeryStatusFilter} onChange={(e) => setSurgeryStatusFilter(e.target.value)}>
            <option value="ALL">Todos los estados</option>
            {surgeryStatuses.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredSurgeries.length} procedimientos visibles</span>
        </div>
      )}

      {activeTab === 'team' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={teamRoleFilter} onChange={(e) => setTeamRoleFilter(e.target.value)}>
            <option value="ALL">Todos los roles</option>
            {teamRoles.map((role) => (
              <option key={role} value={role}>{role}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredTeam.length} miembros visibles</span>
        </div>
      )}

      {activeTab === 'team' && selectedSurgery && (
        <div className="card p-4 mb-6 flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs uppercase tracking-wider text-surface-400">Cirugía seleccionada</p>
            <h3 className="text-sm font-semibold text-surface-800 mt-1">{selectedSurgery.procedure_name}</h3>
          </div>
          <div className="flex items-center gap-3">
            <StatusBadge status={selectedSurgery.status} />
            <span className="text-xs text-surface-400">{new Date(selectedSurgery.scheduled_start).toLocaleDateString('es-ES')}</span>
          </div>
        </div>
      )}

      {activeTab === 'rooms' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={columns}
            data={filteredRooms}
            loading={loading}
            emptyMessage="No hay quirófanos registrados."
            emptyIcon="🏥"
            onRowClick={setSelectedRoom}
            selectedRowId={selectedRoom?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedRoom}
            emptyTitle="Selecciona un quirófano"
            emptyText="Revisa capacidad, ubicación y disponibilidad desde este panel."
            title={selectedRoom?.name}
            subtitle={selectedRoom ? `${selectedRoom.building} · Planta ${selectedRoom.floor}` : undefined}
          >
            {selectedRoom && (
              <>
                <DetailGroup title="Ubicación">
                  <DetailRow label="Código" value={selectedRoom.code} mono />
                  <DetailRow label="Edificio" value={selectedRoom.building} />
                  <DetailRow label="Planta" value={selectedRoom.floor} />
                </DetailGroup>
                <DetailGroup title="Capacidad operativa">
                  <DetailRow label="Estado" value={selectedRoom.status} accent />
                  <DetailRow label="Capacidad" value={`${selectedRoom.capacity} pers.`} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
      {activeTab === 'surgeries' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={surgeryColumns}
            data={filteredSurgeries}
            loading={loading}
            emptyMessage="No hay cirugías registradas."
            emptyIcon="🩻"
            onRowClick={setSelectedSurgery}
            selectedRowId={selectedSurgery?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedSurgery}
            emptyTitle="Selecciona un procedimiento"
            emptyText="Consulta planning, estado y equipo asociado para cada intervención."
            title={selectedSurgery?.procedure_name}
            subtitle={selectedSurgery?.procedure_code || 'Sin código quirúrgico'}
          >
            {selectedSurgery && (
              <>
                <DetailGroup title="Planificación">
                  <DetailRow label="Estado" value={selectedSurgery.status} accent />
                  <DetailRow label="Inicio" value={new Date(selectedSurgery.scheduled_start).toLocaleString('es-ES')} />
                  <DetailRow label="Fin" value={new Date(selectedSurgery.scheduled_end).toLocaleString('es-ES')} />
                </DetailGroup>
                <DetailGroup title="Operativa">
                  <DetailRow label="Código" value={selectedSurgery.procedure_code || '—'} mono />
                  <DetailRow label="Equipo" value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadTeamMembers(selectedSurgery)}>Ver equipo</button>} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
      {activeTab === 'team' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={teamColumns}
            data={filteredTeam}
            loading={detailLoading}
            emptyMessage="Selecciona una cirugía para ver su equipo."
            emptyIcon="👩‍⚕️"
            onRowClick={setSelectedTeamMember}
            selectedRowId={selectedTeamMember?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedSurgery && !selectedTeamMember}
            emptyTitle="Sin miembro seleccionado"
            emptyText="Selecciona una intervención y luego un miembro para revisar su composición."
            title={selectedSurgery?.procedure_name ?? 'Equipo quirúrgico'}
            subtitle={selectedTeamMember ? selectedTeamMember.role : 'Composición del equipo'}
          >
            {selectedSurgery && (
              <DetailGroup title="Cirugía activa">
                <DetailRow label="Procedimiento" value={selectedSurgery.procedure_name} accent />
                <DetailRow label="Estado" value={selectedSurgery.status} />
                <DetailRow label="Inicio" value={new Date(selectedSurgery.scheduled_start).toLocaleString('es-ES')} />
              </DetailGroup>
            )}
            {selectedTeamMember && (
              <DetailGroup title="Miembro del equipo">
                <DetailRow label="Rol" value={selectedTeamMember.role} accent />
                <DetailRow label="Staff" value={selectedTeamMember.staff_id} mono />
                <DetailRow label="Asignado" value={new Date(selectedTeamMember.created_at).toLocaleString('es-ES')} />
              </DetailGroup>
            )}
          </DetailPanel>
        </div>
      )}
    </div>
  )
}
