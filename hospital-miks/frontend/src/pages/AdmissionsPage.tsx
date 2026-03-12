import { useEffect, useMemo, useState } from 'react'
import { api, type Bed, type CareNote, type MealOrder, type Ward } from '../services/api'
import DataTable from '../components/DataTable'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'
import KpiStrip from '../components/KpiStrip'
import PageHeader, { FormCard, StatusBadge, TabButton } from '../components/PageHeader'

interface Admission {
  id: string; patient_id: string; admitting_doctor_id: string; bed_id: string;
  status: string; admission_date: string; discharge_date: string;
  diagnosis: string; admission_reason: string; dietary_needs: string;
}

export default function AdmissionsPage() {
  const [admissions, setAdmissions] = useState<Admission[]>([])
  const [wards, setWards] = useState<Ward[]>([])
  const [beds, setBeds] = useState<Bed[]>([])
  const [careNotes, setCareNotes] = useState<CareNote[]>([])
  const [mealOrders, setMealOrders] = useState<MealOrder[]>([])
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)
  const [activeTab, setActiveTab] = useState<'admissions' | 'wards' | 'beds' | 'care' | 'meals'>('admissions')
  const [showWardForm, setShowWardForm] = useState(false)
  const [showBedForm, setShowBedForm] = useState(false)
  const [selectedWardId, setSelectedWardId] = useState('')
  const [selectedAdmission, setSelectedAdmission] = useState<Admission | null>(null)
  const [selectedWard, setSelectedWard] = useState<Ward | null>(null)
  const [selectedBed, setSelectedBed] = useState<Bed | null>(null)
  const [selectedCareNote, setSelectedCareNote] = useState<CareNote | null>(null)
  const [selectedMealOrder, setSelectedMealOrder] = useState<MealOrder | null>(null)
  const [admissionStatusFilter, setAdmissionStatusFilter] = useState<'ALL' | string>('ALL')
  const [wardActiveFilter, setWardActiveFilter] = useState<'ALL' | 'ACTIVE' | 'INACTIVE'>('ALL')
  const [bedStatusFilter, setBedStatusFilter] = useState<'ALL' | string>('ALL')
  const [mealDeliveryFilter, setMealDeliveryFilter] = useState<'ALL' | 'DELIVERED' | 'PENDING'>('ALL')

  useEffect(() => { void initialLoad() }, [])

  async function initialLoad() {
    setLoading(true)
    try {
      await Promise.all([loadAdmissions(), loadWards(), loadBeds('')])
    } finally {
      setLoading(false)
    }
  }

  async function loadAdmissions() {
    try {
      const res = await api.get<Admission[]>('/admissions')
      const nextAdmissions = res.data ?? []
      setAdmissions(nextAdmissions)
      setSelectedAdmission((current) => nextAdmissions.find((admission) => admission.id === current?.id) ?? current ?? nextAdmissions[0] ?? null)
    } catch {
      setAdmissions([])
    }
  }

  async function loadWards() {
    try {
      const res = await api.get<Ward[]>('/admissions/wards')
      const nextWards = res.data ?? []
      setWards(nextWards)
      setSelectedWard((current) => nextWards.find((ward) => ward.id === current?.id) ?? current ?? nextWards[0] ?? null)
    } catch {
      setWards([])
    }
  }

  async function loadBeds(wardId: string) {
    try {
      const path = wardId ? `/admissions/beds/ward/${wardId}` : '/admissions/beds/available'
      const res = await api.get<Bed[]>(path)
      const nextBeds = res.data ?? []
      setBeds(nextBeds)
      setSelectedBed((current) => nextBeds.find((bed) => bed.id === current?.id) ?? current ?? nextBeds[0] ?? null)
    } catch {
      setBeds([])
    }
  }

  async function loadCareNotes(admission: Admission) {
    setSelectedAdmission(admission)
    setSelectedCareNote(null)
    setActiveTab('care')
    setDetailLoading(true)
    try {
      const res = await api.get<CareNote[]>(`/admissions/${admission.id}/care-notes`)
      const nextNotes = res.data ?? []
      setCareNotes(nextNotes)
      setSelectedCareNote(nextNotes[0] ?? null)
    } finally {
      setDetailLoading(false)
    }
  }

  async function loadMeals(admission: Admission) {
    setSelectedAdmission(admission)
    setSelectedMealOrder(null)
    setActiveTab('meals')
    setDetailLoading(true)
    try {
      const res = await api.get<MealOrder[]>(`/admissions/${admission.id}/meals`)
      const nextMeals = res.data ?? []
      setMealOrders(nextMeals)
      setSelectedMealOrder(nextMeals[0] ?? null)
    } finally {
      setDetailLoading(false)
    }
  }

  const selectedAdmissionLabel = selectedAdmission?.diagnosis || selectedAdmission?.admission_reason || selectedAdmission?.id

  const activeAdmissionsCount = useMemo(() => admissions.filter((admission) => admission.status === 'ADMITTED' || admission.status === 'ACTIVE').length, [admissions])
  const activeWardsCount = useMemo(() => wards.filter((ward) => ward.active).length, [wards])
  const availableBedsCount = useMemo(() => beds.filter((bed) => bed.status === 'AVAILABLE').length, [beds])
  const pendingMealsCount = useMemo(() => mealOrders.filter((meal) => !meal.delivered).length, [mealOrders])
  const admissionStatuses = useMemo(() => Array.from(new Set(admissions.map((admission) => admission.status))), [admissions])
  const bedStatuses = useMemo(() => Array.from(new Set(beds.map((bed) => bed.status))), [beds])

  const filteredAdmissions = useMemo(() => admissions.filter((admission) => admissionStatusFilter === 'ALL' || admission.status === admissionStatusFilter), [admissionStatusFilter, admissions])
  const filteredWards = useMemo(() => wards.filter((ward) => wardActiveFilter === 'ALL' || (wardActiveFilter === 'ACTIVE' ? ward.active : !ward.active)), [wardActiveFilter, wards])
  const filteredBeds = useMemo(() => beds.filter((bed) => bedStatusFilter === 'ALL' || bed.status === bedStatusFilter), [bedStatusFilter, beds])
  const filteredMeals = useMemo(() => mealOrders.filter((meal) => mealDeliveryFilter === 'ALL' || (mealDeliveryFilter === 'DELIVERED' ? meal.delivered : !meal.delivered)), [mealDeliveryFilter, mealOrders])

  const columns = [
    { key: 'admission_date' as const, label: 'Fecha Ingreso', render: (v: string) =>
      <span className="font-medium text-surface-800">{new Date(v).toLocaleDateString('es-ES')}</span>
    },
    { key: 'status' as const, label: 'Estado', render: (v: string) => <StatusBadge status={v} /> },
    { key: 'diagnosis' as const, label: 'Diagnóstico', render: (v: string) =>
      v ? <span className="text-surface-700 text-xs max-w-[200px] truncate block">{v}</span>
        : <span className="text-surface-300">—</span>
    },
    { key: 'admission_reason' as const, label: 'Motivo', render: (v: string) =>
      v ? <span className="text-surface-600">{v}</span> : <span className="text-surface-300">—</span>
    },
    { key: 'dietary_needs' as const, label: 'Dieta', render: (v: string) =>
      v ? <span className="badge bg-amber-50 text-amber-600">{v}</span> : <span className="text-surface-300">—</span>
    },
    { key: 'discharge_date' as const, label: 'Fecha Alta', render: (v: string) =>
      v ? <span className="text-surface-600">{new Date(v).toLocaleDateString('es-ES')}</span>
        : <span className="badge bg-emerald-50 text-emerald-600">Ingresado</span>
    },
    { key: 'id' as const, label: 'Operativa', render: (_v: string, row: Admission) =>
      <div className="flex gap-2 justify-end">
        <button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadCareNotes(row)}>Notas</button>
        <button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadMeals(row)}>Comidas</button>
      </div>
    },
  ]

  const wardColumns = [
    { key: 'code' as const, label: 'Código', render: (v: string) =>
      <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-600">{v}</span>
    },
    { key: 'name' as const, label: 'Unidad', render: (v: string, row: Ward) =>
      <div>
        <span className="font-medium text-surface-800">{v}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{row.building || 'Hospital'} · Planta {row.floor}</p>
      </div>
    },
    { key: 'total_beds' as const, label: 'Capacidad', render: (v: number) =>
      <span className="badge bg-blue-50 text-blue-600">{v} camas</span>
    },
    { key: 'active' as const, label: 'Estado', render: (v: boolean) =>
      <span className={`badge ${v ? 'bg-emerald-50 text-emerald-600 ring-1 ring-inset ring-emerald-200' : 'bg-surface-100 text-surface-400'}`}>
        {v ? 'Activa' : 'Inactiva'}
      </span>
    },
  ]

  const bedColumns = [
    { key: 'number' as const, label: 'Cama', render: (v: string, row: Bed) =>
      <div>
        <span className="font-medium text-surface-800">{v}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">Habitación {row.room_no || '—'}</p>
      </div>
    },
    { key: 'status' as const, label: 'Estado', render: (v: string) => <StatusBadge status={v} /> },
    { key: 'ward_id' as const, label: 'Ward ID', render: (v: string) =>
      <span className="font-mono text-xs text-surface-500">{v.slice(0, 8)}…</span>
    },
    { key: 'features' as const, label: 'Equipamiento', render: (v: string) =>
      v ? <span className="text-surface-600 text-xs max-w-[260px] truncate block">{v}</span> : <span className="text-surface-300">—</span>
    },
  ]

  const careColumns = [
    { key: 'created_at' as const, label: 'Momento', render: (v: string) => <span className="text-surface-600 text-sm">{new Date(v).toLocaleString('es-ES')}</span> },
    { key: 'note_type' as const, label: 'Tipo', render: (v: string) => <span className="badge bg-blue-50 text-blue-600">{v}</span> },
    { key: 'content' as const, label: 'Contenido', render: (v: string) => <span className="text-surface-700 text-sm">{v}</span> },
    { key: 'staff_id' as const, label: 'Staff', render: (v: string) => <span className="font-mono text-xs text-surface-500">{v.slice(0, 8)}…</span> },
  ]

  const mealColumns = [
    { key: 'date' as const, label: 'Fecha', render: (v: string) => <span className="text-surface-600 text-sm">{new Date(v).toLocaleString('es-ES')}</span> },
    { key: 'meal_type' as const, label: 'Comida', render: (v: string) => <span className="badge bg-amber-50 text-amber-600">{v}</span> },
    { key: 'menu' as const, label: 'Menú', render: (v: string) => <span className="text-surface-700 text-sm">{v}</span> },
    { key: 'dietary_note' as const, label: 'Nota dietética', render: (v: string) => v ? <span className="text-surface-600 text-xs">{v}</span> : <span className="text-surface-300">—</span> },
    { key: 'delivered' as const, label: 'Entrega', render: (v: boolean) => <StatusBadge status={v ? 'DELIVERED' : 'PENDING'} label={v ? 'Entregada' : 'Pendiente'} /> },
  ]

  const actionLabel = activeTab === 'wards' ? 'Nueva unidad' : activeTab === 'beds' ? 'Nueva cama' : undefined
  const actionHandler = activeTab === 'wards'
    ? () => setShowWardForm(!showWardForm)
    : activeTab === 'beds'
      ? () => setShowBedForm(!showBedForm)
      : undefined

  return (
    <div>
      <PageHeader title="Gestión de Ingresos" subtitle="Admisiones, unidades hospitalarias y disponibilidad de camas" action={actionLabel} onAction={actionHandler}>
        <div className="flex gap-1.5">
          <TabButton active={activeTab === 'admissions'} onClick={() => setActiveTab('admissions')}>Ingresos</TabButton>
          <TabButton active={activeTab === 'wards'} onClick={() => setActiveTab('wards')}>Unidades</TabButton>
          <TabButton active={activeTab === 'beds'} onClick={() => setActiveTab('beds')}>Camas</TabButton>
          <TabButton active={activeTab === 'care'} onClick={() => setActiveTab('care')}>Notas</TabButton>
          <TabButton active={activeTab === 'meals'} onClick={() => setActiveTab('meals')}>Comidas</TabButton>
        </div>
      </PageHeader>

      <KpiStrip
        items={[
          { label: 'Ingresos', value: activeAdmissionsCount, hint: 'episodios activos', tone: 'info' },
          { label: 'Unidades activas', value: activeWardsCount, hint: 'operativas', tone: 'success' },
          { label: 'Camas disponibles', value: availableBedsCount, hint: 'según carga actual', tone: 'default' },
          { label: 'Comidas pendientes', value: pendingMealsCount, hint: 'servicio nutricional', tone: pendingMealsCount > 0 ? 'warning' : 'success' },
        ]}
      />

      {activeTab === 'admissions' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={admissionStatusFilter} onChange={(e) => setAdmissionStatusFilter(e.target.value)}>
            <option value="ALL">Todos los estados</option>
            {admissionStatuses.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredAdmissions.length} ingresos visibles</span>
        </div>
      )}

      {activeTab === 'wards' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={wardActiveFilter} onChange={(e) => setWardActiveFilter(e.target.value as 'ALL' | 'ACTIVE' | 'INACTIVE')}>
            <option value="ALL">Todas las unidades</option>
            <option value="ACTIVE">Solo activas</option>
            <option value="INACTIVE">Solo inactivas</option>
          </select>
          <span className="text-xs text-surface-400">{filteredWards.length} unidades visibles</span>
        </div>
      )}

      {activeTab === 'wards' && showWardForm && (
        <WardForm
          onSave={async (data) => {
            await api.post('/admissions/wards', data)
            setShowWardForm(false)
            await loadWards()
          }}
          onCancel={() => setShowWardForm(false)}
        />
      )}

      {activeTab === 'beds' && showBedForm && (
        <BedForm
          wards={wards}
          onSave={async (data) => {
            await api.post('/admissions/beds', data)
            setShowBedForm(false)
            await loadBeds(selectedWardId)
          }}
          onCancel={() => setShowBedForm(false)}
        />
      )}

      {activeTab === 'beds' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <span className="text-sm font-medium text-surface-700">Filtrar camas:</span>
          <select
            className="input max-w-xs"
            value={selectedWardId}
            onChange={async (e) => {
              const nextWardId = e.target.value
              setSelectedWardId(nextWardId)
              setLoading(true)
              try {
                await loadBeds(nextWardId)
              } finally {
                setLoading(false)
              }
            }}
          >
            <option value="">Solo disponibles</option>
            {wards.map((ward) => (
              <option key={ward.id} value={ward.id}>{ward.name}</option>
            ))}
          </select>
          <select className="input max-w-xs" value={bedStatusFilter} onChange={(e) => setBedStatusFilter(e.target.value)}>
            <option value="ALL">Todos los estados</option>
            {bedStatuses.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">Selecciona una unidad para ver todas sus camas.</span>
        </div>
      )}

      {activeTab === 'meals' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={mealDeliveryFilter} onChange={(e) => setMealDeliveryFilter(e.target.value as 'ALL' | 'DELIVERED' | 'PENDING')}>
            <option value="ALL">Todas las entregas</option>
            <option value="PENDING">Pendientes</option>
            <option value="DELIVERED">Entregadas</option>
          </select>
          <span className="text-xs text-surface-400">{filteredMeals.length} pedidos visibles</span>
        </div>
      )}

      {(activeTab === 'care' || activeTab === 'meals') && selectedAdmission && (
        <div className="card p-4 mb-6 flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs uppercase tracking-wider text-surface-400">Ingreso seleccionado</p>
            <h3 className="text-sm font-semibold text-surface-800 mt-1">{selectedAdmission.diagnosis || selectedAdmission.admission_reason || selectedAdmission.id}</h3>
          </div>
          <div className="flex items-center gap-3">
            <StatusBadge status={selectedAdmission.status} />
            <span className="text-xs text-surface-400">{new Date(selectedAdmission.admission_date).toLocaleDateString('es-ES')}</span>
          </div>
        </div>
      )}

      {activeTab === 'admissions' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={columns}
            data={filteredAdmissions}
            loading={loading}
            emptyMessage="No hay ingresos activos."
            emptyIcon="🛏️"
            onRowClick={setSelectedAdmission}
            selectedRowId={selectedAdmission?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedAdmission}
            emptyTitle="Selecciona un ingreso"
            emptyText="Consulta situación clínica, dieta y operativa del episodio asistencial."
            title={selectedAdmissionLabel}
            subtitle={selectedAdmission ? `${selectedAdmission.status} · ${new Date(selectedAdmission.admission_date).toLocaleDateString('es-ES')}` : undefined}
          >
            {selectedAdmission && (
              <>
                <DetailGroup title="Contexto clínico">
                  <DetailRow label="Estado" value={selectedAdmission.status} accent />
                  <DetailRow label="Motivo" value={selectedAdmission.admission_reason || '—'} />
                  <DetailRow label="Diagnóstico" value={selectedAdmission.diagnosis || '—'} />
                </DetailGroup>
                <DetailGroup title="Hospitalización">
                  <DetailRow label="Ingreso" value={new Date(selectedAdmission.admission_date).toLocaleString('es-ES')} />
                  <DetailRow label="Alta" value={selectedAdmission.discharge_date ? new Date(selectedAdmission.discharge_date).toLocaleString('es-ES') : 'Pendiente'} />
                  <DetailRow label="Dieta" value={selectedAdmission.dietary_needs || 'Sin restricciones'} />
                </DetailGroup>
                <DetailGroup title="Seguimiento">
                  <DetailRow label="Notas" value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadCareNotes(selectedAdmission)}>Ver notas</button>} />
                  <DetailRow label="Comidas" value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadMeals(selectedAdmission)}>Ver pedidos</button>} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
      {activeTab === 'wards' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={wardColumns}
            data={filteredWards}
            loading={loading}
            emptyMessage="No hay unidades registradas."
            emptyIcon="🏥"
            onRowClick={setSelectedWard}
            selectedRowId={selectedWard?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedWard}
            emptyTitle="Selecciona una unidad"
            emptyText="Consulta capacidad, ubicación y estado operativo de la unidad."
            title={selectedWard?.name}
            subtitle={selectedWard ? `${selectedWard.building || 'Hospital'} · Planta ${selectedWard.floor}` : undefined}
          >
            {selectedWard && (
              <>
                <DetailGroup title="Ubicación">
                  <DetailRow label="Código" value={selectedWard.code} mono />
                  <DetailRow label="Edificio" value={selectedWard.building || 'Hospital'} />
                  <DetailRow label="Planta" value={selectedWard.floor} />
                </DetailGroup>
                <DetailGroup title="Capacidad">
                  <DetailRow label="Camas" value={selectedWard.total_beds} accent />
                  <DetailRow label="Estado" value={selectedWard.active ? 'Activa' : 'Inactiva'} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
      {activeTab === 'beds' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={bedColumns}
            data={filteredBeds}
            loading={loading}
            emptyMessage="No hay camas para este filtro."
            emptyIcon="🛌"
            onRowClick={setSelectedBed}
            selectedRowId={selectedBed?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedBed}
            emptyTitle="Selecciona una cama"
            emptyText="Revisa el estado, la habitación y el equipamiento disponible."
            title={selectedBed?.number}
            subtitle={selectedBed?.room_no ? `Habitación ${selectedBed.room_no}` : 'Sin habitación asignada'}
          >
            {selectedBed && (
              <>
                <DetailGroup title="Estado asistencial">
                  <DetailRow label="Estado" value={selectedBed.status} accent />
                  <DetailRow label="Ward" value={selectedBed.ward_id} mono />
                  <DetailRow label="Habitación" value={selectedBed.room_no || '—'} />
                </DetailGroup>
                <DetailGroup title="Equipamiento">
                  <DetailRow label="Features" value={selectedBed.features || 'No informado'} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
      {activeTab === 'care' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={careColumns}
            data={careNotes}
            loading={detailLoading}
            emptyMessage="Selecciona un ingreso para ver sus notas asistenciales."
            emptyIcon="🩺"
            onRowClick={setSelectedCareNote}
            selectedRowId={selectedCareNote?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedAdmission && !selectedCareNote}
            emptyTitle="Sin nota seleccionada"
            emptyText="Selecciona un ingreso y una nota para revisar el seguimiento asistencial."
            title={selectedAdmissionLabel ?? 'Notas asistenciales'}
            subtitle={selectedCareNote?.note_type ?? 'Seguimiento clínico'}
          >
            {selectedAdmission && (
              <DetailGroup title="Ingreso activo">
                <DetailRow label="Estado" value={selectedAdmission.status} accent />
                <DetailRow label="Ingreso" value={new Date(selectedAdmission.admission_date).toLocaleString('es-ES')} />
                <DetailRow label="Dieta" value={selectedAdmission.dietary_needs || 'Sin restricciones'} />
              </DetailGroup>
            )}
            {selectedCareNote && (
              <DetailGroup title="Nota seleccionada">
                <DetailRow label="Tipo" value={selectedCareNote.note_type} accent />
                <DetailRow label="Staff" value={selectedCareNote.staff_id} mono />
                <DetailRow label="Momento" value={new Date(selectedCareNote.created_at).toLocaleString('es-ES')} />
                <DetailRow label="Contenido" value={selectedCareNote.content} />
              </DetailGroup>
            )}
          </DetailPanel>
        </div>
      )}
      {activeTab === 'meals' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={mealColumns}
            data={filteredMeals}
            loading={detailLoading}
            emptyMessage="Selecciona un ingreso para ver sus pedidos de comida."
            emptyIcon="🍽️"
            onRowClick={setSelectedMealOrder}
            selectedRowId={selectedMealOrder?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedAdmission && !selectedMealOrder}
            emptyTitle="Sin pedido seleccionado"
            emptyText="Selecciona un ingreso y una comida para analizar el servicio nutricional."
            title={selectedAdmissionLabel ?? 'Pedidos de comida'}
            subtitle={selectedMealOrder?.meal_type ?? 'Servicio nutricional'}
          >
            {selectedAdmission && (
              <DetailGroup title="Ingreso activo">
                <DetailRow label="Estado" value={selectedAdmission.status} accent />
                <DetailRow label="Dieta" value={selectedAdmission.dietary_needs || 'Sin restricciones'} />
              </DetailGroup>
            )}
            {selectedMealOrder && (
              <DetailGroup title="Pedido seleccionado">
                <DetailRow label="Comida" value={selectedMealOrder.meal_type} accent />
                <DetailRow label="Fecha" value={new Date(selectedMealOrder.date).toLocaleString('es-ES')} />
                <DetailRow label="Menú" value={selectedMealOrder.menu} />
                <DetailRow label="Nota dietética" value={selectedMealOrder.dietary_note || '—'} />
                <DetailRow label="Entrega" value={selectedMealOrder.delivered ? 'Entregada' : 'Pendiente'} />
              </DetailGroup>
            )}
          </DetailPanel>
        </div>
      )}
    </div>
  )
}

function WardForm({ onSave, onCancel }: { onSave: (data: Record<string, string | number | boolean>) => void; onCancel: () => void }) {
  const [form, setForm] = useState({
    name: '', code: '', department_id: '', floor: 0, building: '', total_beds: 0, active: true,
  })

  const set = (field: string) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm({
      ...form,
      [field]: field === 'floor' || field === 'total_beds' ? Number(e.target.value) : e.target.value,
    })

  return (
    <FormCard title="Nueva Unidad" onSave={() => onSave(form)} onCancel={onCancel}>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <input className="input" placeholder="Nombre" value={form.name} onChange={set('name')} />
        <input className="input" placeholder="Código" value={form.code} onChange={set('code')} />
        <input className="input" placeholder="Department ID" value={form.department_id} onChange={set('department_id')} />
        <input className="input" type="number" placeholder="Planta" value={form.floor} onChange={set('floor')} />
        <input className="input" placeholder="Edificio" value={form.building} onChange={set('building')} />
        <input className="input" type="number" placeholder="Nº camas" value={form.total_beds} onChange={set('total_beds')} />
      </div>
    </FormCard>
  )
}

function BedForm({ wards, onSave, onCancel }: { wards: Ward[]; onSave: (data: Record<string, string>) => void; onCancel: () => void }) {
  const [form, setForm] = useState({
    ward_id: wards[0]?.id ?? '', number: '', status: 'AVAILABLE', room_no: '', features: '',
  })

  const set = (field: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setForm({ ...form, [field]: e.target.value })

  return (
    <FormCard title="Nueva Cama" onSave={() => onSave(form)} onCancel={onCancel}>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <select className="input" value={form.ward_id} onChange={set('ward_id')}>
          <option value="">Selecciona unidad</option>
          {wards.map((ward) => (
            <option key={ward.id} value={ward.id}>{ward.name}</option>
          ))}
        </select>
        <input className="input" placeholder="Número de cama" value={form.number} onChange={set('number')} />
        <select className="input" value={form.status} onChange={set('status')}>
          <option value="AVAILABLE">Disponible</option>
          <option value="RESERVED">Reservada</option>
          <option value="CLEANING">Limpieza</option>
          <option value="OUT_OF_ORDER">Fuera de servicio</option>
        </select>
        <input className="input" placeholder="Habitación" value={form.room_no} onChange={set('room_no')} />
        <input className="input md:col-span-2" placeholder="Equipamiento / features" value={form.features} onChange={set('features')} />
      </div>
    </FormCard>
  )
}
