import { useEffect, useMemo, useState } from 'react'
import { AnimatePresence } from 'framer-motion'
import { useLocation, useNavigate } from 'react-router-dom'
import { api, type Department, type Staff, type UserAccount } from '../services/api'
import DataTable from '../components/DataTable'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'
import KpiStrip from '../components/KpiStrip'
import PageHeader, { FormCard, TabButton } from '../components/PageHeader'

const typeLabels: Record<string, string> = {
  DOCTOR: 'Médico', NURSE: 'Enfermero/a', SURGEON: 'Cirujano/a', ADMIN: 'Administrativo',
  TECHNICIAN: 'Técnico', PHARMACIST: 'Farmacéutico', THERAPIST: 'Terapeuta',
  RECEPTIONIST: 'Recepcionista', MAINTENANCE: 'Mantenimiento', OTHER: 'Otro',
}
const typeColors: Record<string, string> = {
  DOCTOR: 'bg-blue-50 text-blue-600', NURSE: 'bg-emerald-50 text-emerald-600',
  SURGEON: 'bg-rose-50 text-rose-600', ADMIN: 'bg-surface-100 text-surface-600',
  PHARMACIST: 'bg-purple-50 text-purple-600', THERAPIST: 'bg-amber-50 text-amber-600',
}

export default function StaffPage() {
  const location = useLocation()
  const focusState = location.state as { focusType?: string; focusId?: string; openTab?: 'staff' | 'departments' | 'users' } | null
  const navigate = useNavigate()
  const [staff, setStaff] = useState<Staff[]>([])
  const [departments, setDepartments] = useState<Department[]>([])
  const [users, setUsers] = useState<UserAccount[]>([])
  const [loading, setLoading] = useState(true)
  const [activeTab, setActiveTab] = useState<'staff' | 'departments' | 'users'>('staff')
  const [showStaffForm, setShowStaffForm] = useState(false)
  const [showDepartmentForm, setShowDepartmentForm] = useState(false)
  const [showUserForm, setShowUserForm] = useState(false)
  const [selectedStaff, setSelectedStaff] = useState<Staff | null>(null)
  const [selectedDepartment, setSelectedDepartment] = useState<Department | null>(null)
  const [selectedUser, setSelectedUser] = useState<UserAccount | null>(null)
  const [staffSearch, setStaffSearch] = useState('')
  const [staffTypeFilter, setStaffTypeFilter] = useState<'ALL' | string>('ALL')
  const [departmentFilter, setDepartmentFilter] = useState<'ALL' | 'ACTIVE' | 'INACTIVE'>('ALL')
  const [userRoleFilter, setUserRoleFilter] = useState<'ALL' | string>('ALL')

  useEffect(() => { void Promise.all([loadStaff(), loadDepartments(), loadUsers()]) }, [])
  useEffect(() => {
    if (focusState?.openTab) {
      setActiveTab(focusState.openTab)
    }
  }, [focusState])

  useEffect(() => {
    if (focusState?.focusType !== 'staff' || !focusState.focusId || staff.length === 0) return
    const focusedStaff = staff.find((member) => member.id === focusState.focusId)
    if (focusedStaff) {
      setActiveTab('staff')
      setSelectedStaff(focusedStaff)
    }
  }, [focusState, staff])

  useEffect(() => {
    if (focusState?.focusType !== 'department' || !focusState.focusId || departments.length === 0) return
    const focusedDepartment = departments.find((department) => department.id === focusState.focusId)
    if (focusedDepartment) {
      setActiveTab('departments')
      setSelectedDepartment(focusedDepartment)
    }
  }, [departments, focusState])

  useEffect(() => {
    if (focusState?.focusType !== 'userByStaff' || !focusState.focusId || users.length === 0) return
    const linkedUser = users.find((user) => user.staff_id === focusState.focusId)
    if (linkedUser) {
      setActiveTab('users')
      setSelectedUser(linkedUser)
    }
  }, [focusState, users])

  async function loadStaff() {
    setLoading(true)
    try {
      const res = await api.get<Staff[]>('/staff')
      const nextStaff = res.data ?? []
      setStaff(nextStaff)
      setSelectedStaff((current) => nextStaff.find((member) => member.id === current?.id) ?? current ?? nextStaff[0] ?? null)
    } finally { setLoading(false) }
  }

  async function loadDepartments() {
    try {
      const res = await api.get<Department[]>('/staff/departments')
      const nextDepartments = res.data ?? []
      setDepartments(nextDepartments)
      setSelectedDepartment((current) => nextDepartments.find((department) => department.id === current?.id) ?? current ?? nextDepartments[0] ?? null)
    } catch {
      setDepartments([])
    }
  }

  async function loadUsers() {
    try {
      const res = await api.get<UserAccount[]>('/auth/users')
      const nextUsers = res.data ?? []
      setUsers(nextUsers)
      setSelectedUser((current) => nextUsers.find((user) => user.id === current?.id) ?? current ?? nextUsers[0] ?? null)
    } catch {
      setUsers([])
    }
  }

  const columns = [
    { key: 'employee_code' as const, label: 'Código', render: (v: string) =>
      <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-600">{v}</span>
    },
    { key: 'first_name' as const, label: 'Nombre', render: (_v: string, row: Staff) =>
      <div>
        <span className="font-medium text-surface-800">{row.first_name} {row.last_name}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{row.email}</p>
      </div>
    },
    { key: 'staff_type' as const, label: 'Tipo', render: (v: string) =>
      <span className={`badge ${typeColors[v] ?? 'bg-surface-100 text-surface-600'}`}>{typeLabels[v] ?? v}</span>
    },
    { key: 'specialty' as const, label: 'Especialidad', render: (v: string) =>
      v ? <span className="text-surface-700">{v}</span> : <span className="text-surface-300">—</span>
    },
    { key: 'license_number' as const, label: 'Nº Colegiado', render: (v: string) =>
      v ? <span className="font-mono text-xs text-surface-500">{v}</span> : <span className="text-surface-300">—</span>
    },
    { key: 'active' as const, label: 'Estado', render: (v: boolean) =>
      <span className={`badge ${v ? 'bg-emerald-50 text-emerald-600 ring-1 ring-inset ring-emerald-200' : 'bg-surface-100 text-surface-400'}`}>
        {v ? 'Activo' : 'Inactivo'}
      </span>
    },
    { key: 'id' as const, label: 'Acción', render: (_v: string, row: Staff) =>
      <button className="btn-secondary !px-3 !py-1.5" onClick={() => setSelectedStaff(row)}>Ver perfil</button>
    },
  ]

  const departmentColumns = [
    { key: 'code' as const, label: 'Código', render: (v: string) =>
      <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-600">{v}</span>
    },
    { key: 'name' as const, label: 'Departamento', render: (v: string, row: Department) =>
      <div>
        <span className="font-medium text-surface-800">{v}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{row.building || 'Sin edificio'} · Planta {row.floor}</p>
      </div>
    },
    { key: 'head_id' as const, label: 'Responsable', render: (v: string) =>
      v ? <span className="font-mono text-xs text-surface-500">{v.slice(0, 8)}…</span> : <span className="text-surface-300">Pendiente</span>
    },
    { key: 'active' as const, label: 'Estado', render: (v: boolean) =>
      <span className={`badge ${v ? 'bg-blue-50 text-blue-600 ring-1 ring-inset ring-blue-200' : 'bg-surface-100 text-surface-400'}`}>
        {v ? 'Activo' : 'Inactivo'}
      </span>
    },
    { key: 'id' as const, label: 'Acción', render: (_v: string, row: Department) =>
      <button className="btn-secondary !px-3 !py-1.5" onClick={() => setSelectedDepartment(row)}>Ver unidad</button>
    },
  ]

  const userColumns = [
    { key: 'username' as const, label: 'Usuario', render: (v: string) =>
      <span className="font-medium text-surface-800">{v}</span>
    },
    { key: 'role' as const, label: 'Rol', render: (v: string) =>
      <span className="badge bg-purple-50 text-purple-600">{v}</span>
    },
    { key: 'staff_id' as const, label: 'Staff', render: (v: string) =>
      <span className="font-mono text-xs text-surface-500">{v.slice(0, 8)}…</span>
    },
    { key: 'last_login_at' as const, label: 'Último acceso', render: (v?: string) =>
      v ? <span className="text-surface-600 text-xs">{new Date(v).toLocaleString('es-ES')}</span> : <span className="text-surface-300">Nunca</span>
    },
    { key: 'active' as const, label: 'Estado', render: (v: boolean) =>
      <span className={`badge ${v ? 'bg-emerald-50 text-emerald-600 ring-1 ring-inset ring-emerald-200' : 'bg-surface-100 text-surface-400'}`}>{v ? 'Activo' : 'Inactivo'}</span>
    },
    { key: 'id' as const, label: 'Acción', render: (_v: string, row: UserAccount) =>
      <button className="btn-secondary !px-3 !py-1.5" onClick={() => setSelectedUser(row)}>Ver acceso</button>
    },
  ]

  const activeStaffCount = useMemo(() => staff.filter((member) => member.active).length, [staff])
  const departmentCount = useMemo(() => departments.filter((department) => department.active).length, [departments])
  const activeUsersCount = useMemo(() => users.filter((user) => user.active).length, [users])
  const clinicalStaffCount = useMemo(() => staff.filter((member) => ['DOCTOR', 'NURSE', 'SURGEON', 'THERAPIST', 'PHARMACIST'].includes(member.staff_type)).length, [staff])
  const staffTypes = useMemo(() => Array.from(new Set(staff.map((member) => member.staff_type))), [staff])
  const userRoles = useMemo(() => Array.from(new Set(users.map((user) => user.role))), [users])

  const filteredStaff = useMemo(() => {
    const query = staffSearch.trim().toLowerCase()
    return staff.filter((member) => {
      const matchesQuery = !query || [member.first_name, member.last_name, member.email, member.employee_code, member.specialty].some((value) => value?.toLowerCase().includes(query))
      const matchesType = staffTypeFilter === 'ALL' || member.staff_type === staffTypeFilter
      return matchesQuery && matchesType
    })
  }, [staff, staffSearch, staffTypeFilter])

  const filteredDepartments = useMemo(() => departments.filter((department) => departmentFilter === 'ALL' || (departmentFilter === 'ACTIVE' ? department.active : !department.active)), [departmentFilter, departments])
  const filteredUsers = useMemo(() => users.filter((user) => userRoleFilter === 'ALL' || user.role === userRoleFilter), [userRoleFilter, users])

  return (
    <div>
      <PageHeader
        title="Gestión de Personal"
        subtitle="Administración de empleados, departamentos y estructura organizativa"
        action={activeTab === 'staff' ? 'Nuevo empleado' : activeTab === 'departments' ? 'Nuevo departamento' : 'Nuevo acceso'}
        onAction={() => activeTab === 'staff' ? setShowStaffForm(!showStaffForm) : activeTab === 'departments' ? setShowDepartmentForm(!showDepartmentForm) : setShowUserForm(!showUserForm)}
      >
        <div className="flex gap-1.5">
          <TabButton active={activeTab === 'staff'} onClick={() => setActiveTab('staff')}>Personal</TabButton>
          <TabButton active={activeTab === 'departments'} onClick={() => setActiveTab('departments')}>Departamentos</TabButton>
          <TabButton active={activeTab === 'users'} onClick={() => setActiveTab('users')}>Accesos</TabButton>
        </div>
      </PageHeader>

      <KpiStrip
        items={[
          { label: 'Personal activo', value: activeStaffCount, hint: `${staff.length} en plantilla`, tone: 'info' },
          { label: 'Clínicos', value: clinicalStaffCount, hint: 'atención asistencial', tone: 'success' },
          { label: 'Departamentos', value: departmentCount, hint: 'áreas operativas', tone: 'default' },
          { label: 'Accesos activos', value: activeUsersCount, hint: 'identidad habilitada', tone: 'warning' },
        ]}
      />

      <div className="card p-4 mb-6 grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Cobertura</p>
          <p className="mt-2 text-xl font-semibold text-surface-900">{Math.round((activeUsersCount / Math.max(staff.length, 1)) * 100)}%</p>
          <p className="text-sm text-surface-500 mt-1">del personal tiene acceso operativo</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Foco ejecutivo</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{filteredStaff.filter((member) => !member.active).length} inactivos · {filteredDepartments.filter((department) => !department.active).length} áreas inactivas</p>
          <p className="text-sm text-surface-500 mt-1">control visual para staffing y estructura</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Último acceso</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{selectedUser?.last_login_at ? new Date(selectedUser.last_login_at).toLocaleString('es-ES') : 'Sin actividad reciente'}</p>
          <p className="text-sm text-surface-500 mt-1">según el acceso actualmente seleccionado</p>
        </div>
      </div>

      <AnimatePresence>
        {showStaffForm && activeTab === 'staff' && (
          <StaffForm
            onSave={async (data) => {
              await api.post('/staff', data)
              setShowStaffForm(false)
              await loadStaff()
            }}
            onCancel={() => setShowStaffForm(false)}
          />
        )}
        {showDepartmentForm && activeTab === 'departments' && (
          <DepartmentForm
            onSave={async (data) => {
              await api.post('/staff/departments', data)
              setShowDepartmentForm(false)
              await loadDepartments()
            }}
            onCancel={() => setShowDepartmentForm(false)}
          />
        )}
        {showUserForm && activeTab === 'users' && (
          <UserForm
            onSave={async (data) => {
              await api.post('/auth/users', data)
              setShowUserForm(false)
              await loadUsers()
            }}
            onCancel={() => setShowUserForm(false)}
          />
        )}
      </AnimatePresence>

      {activeTab === 'staff' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <input className="input max-w-sm" placeholder="Buscar por nombre, código, email o especialidad" value={staffSearch} onChange={(e) => setStaffSearch(e.target.value)} />
          <select className="input max-w-xs" value={staffTypeFilter} onChange={(e) => setStaffTypeFilter(e.target.value)}>
            <option value="ALL">Todos los perfiles</option>
            {staffTypes.map((type) => (
              <option key={type} value={type}>{typeLabels[type] ?? type}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredStaff.length} profesionales visibles</span>
        </div>
      )}

      {activeTab === 'departments' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={departmentFilter} onChange={(e) => setDepartmentFilter(e.target.value as 'ALL' | 'ACTIVE' | 'INACTIVE')}>
            <option value="ALL">Todos los estados</option>
            <option value="ACTIVE">Solo activos</option>
            <option value="INACTIVE">Solo inactivos</option>
          </select>
          <span className="text-xs text-surface-400">{filteredDepartments.length} departamentos visibles</span>
        </div>
      )}

      {activeTab === 'users' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={userRoleFilter} onChange={(e) => setUserRoleFilter(e.target.value)}>
            <option value="ALL">Todos los roles</option>
            {userRoles.map((role) => (
              <option key={role} value={role}>{role}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredUsers.length} accesos visibles</span>
        </div>
      )}

      {activeTab === 'staff' ? (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable columns={columns} data={filteredStaff} loading={loading} emptyMessage="No hay personal registrado." emptyIcon="👤" onRowClick={setSelectedStaff} selectedRowId={selectedStaff?.id ?? null} />
          <DetailPanel isEmpty={!selectedStaff} emptyTitle="Selecciona un profesional" emptyText="Consulta identidad, categoría y habilitación operativa." title={selectedStaff ? `${selectedStaff.first_name} ${selectedStaff.last_name}` : undefined} subtitle={selectedStaff ? `${typeLabels[selectedStaff.staff_type] ?? selectedStaff.staff_type} · ${selectedStaff.employee_code}` : undefined}>
            {selectedStaff && (
              <>
                <DetailGroup title="Identidad">
                  <DetailRow label="Email" value={selectedStaff.email || '—'} />
                  <DetailRow label="Teléfono" value={selectedStaff.phone || '—'} />
                  <DetailRow label="Colegiado" value={selectedStaff.license_number || '—'} mono />
                </DetailGroup>
                <DetailGroup title="Perfil profesional">
                  <DetailRow label="Tipo" value={typeLabels[selectedStaff.staff_type] ?? selectedStaff.staff_type} accent />
                  <DetailRow label="Especialidad" value={selectedStaff.specialty || 'General'} />
                  <DetailRow label="Alta" value={selectedStaff.hire_date ? new Date(selectedStaff.hire_date).toLocaleDateString('es-ES') : 'No informada'} />
                </DetailGroup>
                <DetailGroup title="Operativa">
                  <DetailRow label="Estado" value={selectedStaff.active ? 'Activo' : 'Inactivo'} accent={selectedStaff.active} />
                  <DetailRow label="Departamento" value={selectedStaff.department_id || 'Sin asignar'} mono />
                </DetailGroup>
                <DetailGroup title="Acciones relacionadas">
                  <DetailRow
                    label="Agenda"
                    value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/appointments', { state: { focusType: 'doctor', focusId: selectedStaff.id, openTab: 'appointments' } })}>Ver citas</button>}
                  />
                  <DetailRow
                    label="Acceso"
                    value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => setActiveTab('users')}>Ver identidad</button>}
                  />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      ) : activeTab === 'departments' ? (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable columns={departmentColumns} data={filteredDepartments} loading={loading} emptyMessage="No hay departamentos registrados." emptyIcon="🏥" onRowClick={setSelectedDepartment} selectedRowId={selectedDepartment?.id ?? null} />
          <DetailPanel isEmpty={!selectedDepartment} emptyTitle="Selecciona un departamento" emptyText="Consulta ubicación, liderazgo y estado de la unidad." title={selectedDepartment?.name} subtitle={selectedDepartment ? `${selectedDepartment.building || 'Hospital'} · Planta ${selectedDepartment.floor}` : undefined}>
            {selectedDepartment && (
              <>
                <DetailGroup title="Ubicación">
                  <DetailRow label="Código" value={selectedDepartment.code} mono />
                  <DetailRow label="Edificio" value={selectedDepartment.building || 'Hospital'} />
                  <DetailRow label="Planta" value={selectedDepartment.floor} />
                </DetailGroup>
                <DetailGroup title="Gobierno">
                  <DetailRow label="Responsable" value={selectedDepartment.head_id || 'Pendiente'} mono />
                  <DetailRow label="Estado" value={selectedDepartment.active ? 'Activo' : 'Inactivo'} accent={selectedDepartment.active} />
                </DetailGroup>
                {selectedDepartment.head_id && (
                  <DetailGroup title="Acción relacionada">
                    <DetailRow
                      label="Responsable"
                      value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/staff', { state: { focusType: 'staff', focusId: selectedDepartment.head_id, openTab: 'staff' } })}>Abrir perfil</button>}
                    />
                  </DetailGroup>
                )}
              </>
            )}
          </DetailPanel>
        </div>
      ) : (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable columns={userColumns} data={filteredUsers} loading={loading} emptyMessage="No hay accesos registrados." emptyIcon="🔐" onRowClick={setSelectedUser} selectedRowId={selectedUser?.id ?? null} />
          <DetailPanel isEmpty={!selectedUser} emptyTitle="Selecciona un acceso" emptyText="Consulta rol, trazabilidad y vinculación con personal." title={selectedUser?.username} subtitle={selectedUser?.role}>
            {selectedUser && (
              <>
                <DetailGroup title="Acceso">
                  <DetailRow label="Rol" value={selectedUser.role} accent />
                  <DetailRow label="Staff" value={selectedUser.staff_id} mono />
                  <DetailRow label="Estado" value={selectedUser.active ? 'Activo' : 'Inactivo'} accent={selectedUser.active} />
                </DetailGroup>
                <DetailGroup title="Trazabilidad">
                  <DetailRow label="Último acceso" value={selectedUser.last_login_at ? new Date(selectedUser.last_login_at).toLocaleString('es-ES') : 'Nunca'} />
                  <DetailRow label="Creado" value={new Date(selectedUser.created_at).toLocaleString('es-ES')} />
                </DetailGroup>
                <DetailGroup title="Acción relacionada">
                  <DetailRow
                    label="Profesional"
                    value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/staff', { state: { focusType: 'staff', focusId: selectedUser.staff_id, openTab: 'staff' } })}>Abrir perfil</button>}
                  />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
    </div>
  )
}

function UserForm({ onSave, onCancel }: { onSave: (data: Record<string, string>) => void; onCancel: () => void }) {
  const [form, setForm] = useState({
    staff_id: '', username: '', password: '', role: 'ADMIN',
  })

  const set = (field: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setForm({ ...form, [field]: e.target.value })

  return (
    <FormCard title="Nuevo Acceso" onSave={() => onSave(form)} onCancel={onCancel}>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <input className="input" placeholder="Staff ID" value={form.staff_id} onChange={set('staff_id')} />
        <input className="input" placeholder="Usuario" value={form.username} onChange={set('username')} />
        <input className="input" type="password" placeholder="Contraseña" value={form.password} onChange={set('password')} />
        <select className="input" value={form.role} onChange={set('role')}>
          <option value="ADMIN">Admin</option>
          <option value="DOCTOR">Doctor</option>
          <option value="NURSE">Nurse</option>
          <option value="PHARMACIST">Pharmacist</option>
          <option value="BILLING">Billing</option>
          <option value="RECEPTIONIST">Receptionist</option>
        </select>
      </div>
    </FormCard>
  )
}

function DepartmentForm({ onSave, onCancel }: { onSave: (data: Record<string, string | number | boolean>) => void; onCancel: () => void }) {
  const [form, setForm] = useState({
    name: '', code: '', floor: 0, building: '', head_id: '', active: true,
  })

  const set = (field: string) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm({
      ...form,
      [field]: field === 'floor' ? Number(e.target.value) : e.target.value,
    })

  return (
    <FormCard title="Nuevo Departamento" onSave={() => onSave(form)} onCancel={onCancel}>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <input className="input" placeholder="Nombre" value={form.name} onChange={set('name')} />
        <input className="input" placeholder="Código" value={form.code} onChange={set('code')} />
        <input className="input" type="number" placeholder="Planta" value={form.floor} onChange={set('floor')} />
        <input className="input" placeholder="Edificio" value={form.building} onChange={set('building')} />
        <input className="input md:col-span-2" placeholder="Head ID (opcional)" value={form.head_id} onChange={set('head_id')} />
      </div>
    </FormCard>
  )
}

function StaffForm({ onSave, onCancel }: { onSave: (data: Record<string, string>) => void; onCancel: () => void }) {
  const [form, setForm] = useState({
    employee_code: '', first_name: '', last_name: '', email: '', phone: '',
    staff_type: 'DOCTOR', specialty: '', license_number: '', hire_date: '',
  })

  const set = (field: string) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) =>
    setForm({ ...form, [field]: e.target.value })

  return (
    <FormCard title="Nuevo Empleado" onSave={() => onSave(form)} onCancel={onCancel}>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <input className="input" placeholder="Código Empleado" value={form.employee_code} onChange={set('employee_code')} />
        <input className="input" placeholder="Nombre" value={form.first_name} onChange={set('first_name')} />
        <input className="input" placeholder="Apellidos" value={form.last_name} onChange={set('last_name')} />
        <input className="input" placeholder="Email" value={form.email} onChange={set('email')} />
        <input className="input" placeholder="Teléfono" value={form.phone} onChange={set('phone')} />
        <select className="input" value={form.staff_type} onChange={set('staff_type')}>
          <option value="DOCTOR">Médico</option><option value="NURSE">Enfermero/a</option>
          <option value="SURGEON">Cirujano/a</option><option value="ADMIN">Administrativo</option>
          <option value="TECHNICIAN">Técnico</option><option value="PHARMACIST">Farmacéutico</option>
          <option value="THERAPIST">Terapeuta</option><option value="RECEPTIONIST">Recepcionista</option>
          <option value="MAINTENANCE">Mantenimiento</option><option value="OTHER">Otro</option>
        </select>
        <input className="input" placeholder="Especialidad" value={form.specialty} onChange={set('specialty')} />
        <input className="input" placeholder="Nº Colegiado" value={form.license_number} onChange={set('license_number')} />
        <input className="input" type="date" value={form.hire_date} onChange={set('hire_date')} />
      </div>
    </FormCard>
  )
}
