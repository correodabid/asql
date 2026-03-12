import { useEffect, useMemo, useState } from 'react'
import { api } from '../services/api'
import DataTable from '../components/DataTable'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'
import KpiStrip from '../components/KpiStrip'
import PageHeader, { StatusBadge, TabButton } from '../components/PageHeader'

interface RehabPlan {
  id: string; patient_id: string; therapist_id: string; doctor_id: string;
  type: string; diagnosis: string; goals: string; start_date: string;
  end_date: string; total_sessions: number; completed_sessions: number;
  active: boolean; notes: string; created_at: string;
}

interface RehabSession {
  id: string; plan_id: string; therapist_id: string; patient_id: string;
  status: string; scheduled_at: string; duration_minutes: number;
  room: string; exercises: string; progress: string;
  pain_level: number | null; notes: string; created_at: string;
}

const therapyLabels: Record<string, string> = {
  PHYSIOTHERAPY: 'Fisioterapia', OCCUPATIONAL: 'Terapia Ocupacional',
  SPEECH: 'Logopedia', CARDIAC_REHAB: 'Rehabilitación Cardíaca',
  NEUROLOGICAL: 'Neurológica', POST_SURGICAL: 'Post-Quirúrgica',
  MUSCULOSKELETAL: 'Musculoesquelética',
}
const therapyColors: Record<string, string> = {
  PHYSIOTHERAPY: 'bg-blue-50 text-blue-600', OCCUPATIONAL: 'bg-purple-50 text-purple-600',
  SPEECH: 'bg-amber-50 text-amber-600', CARDIAC_REHAB: 'bg-rose-50 text-rose-600',
  NEUROLOGICAL: 'bg-indigo-50 text-indigo-600', POST_SURGICAL: 'bg-cyan-50 text-cyan-600',
  MUSCULOSKELETAL: 'bg-emerald-50 text-emerald-600',
}

export default function RehabPage() {
  const [tab, setTab] = useState<'plans' | 'sessions'>('plans')
  const [plans, setPlans] = useState<RehabPlan[]>([])
  const [sessions, setSessions] = useState<RehabSession[]>([])
  const [selectedPlanId, setSelectedPlanId] = useState<string | null>(null)
  const [loadingPlans, setLoadingPlans] = useState(true)
  const [loadingSessions, setLoadingSessions] = useState(false)
  const [selectedPlan, setSelectedPlan] = useState<RehabPlan | null>(null)
  const [selectedSession, setSelectedSession] = useState<RehabSession | null>(null)
  const [planTypeFilter, setPlanTypeFilter] = useState<'ALL' | string>('ALL')
  const [sessionStatusFilter, setSessionStatusFilter] = useState<'ALL' | string>('ALL')

  useEffect(() => { loadPlans() }, [])

  useEffect(() => {
    if (selectedPlanId) loadSessions(selectedPlanId)
  }, [selectedPlanId])

  async function loadPlans() {
    setLoadingPlans(true)
    try {
      const res = await api.get<RehabPlan[]>('/rehab/plans')
      const nextPlans = res.data ?? []
      setPlans(nextPlans)
      setSelectedPlan((current) => nextPlans.find((plan) => plan.id === current?.id) ?? current ?? nextPlans[0] ?? null)
    } finally { setLoadingPlans(false) }
  }

  async function loadSessions(planId: string) {
    setLoadingSessions(true)
    try {
      const res = await api.get<RehabSession[]>(`/rehab/sessions/plan/${planId}`)
      const nextSessions = res.data ?? []
      setSessions(nextSessions)
      setSelectedSession((current) => nextSessions.find((session) => session.id === current?.id) ?? current ?? nextSessions[0] ?? null)
    } finally { setLoadingSessions(false) }
  }

  const planColumns = [
    { key: 'type' as const, label: 'Tipo', render: (v: string) =>
      <span className={`badge ${therapyColors[v] ?? 'bg-surface-100 text-surface-600'}`}>{therapyLabels[v] ?? v}</span>
    },
    { key: 'diagnosis' as const, label: 'Diagnóstico', render: (v: string) =>
      <span className="text-surface-700 text-xs max-w-[180px] truncate block">{v}</span>
    },
    { key: 'goals' as const, label: 'Objetivos', render: (v: string) =>
      <span className="text-surface-600 text-xs max-w-[180px] truncate block">{v}</span>
    },
    { key: 'total_sessions' as const, label: 'Progreso', render: (v: number, row: RehabPlan) => {
      const pct = v > 0 ? Math.round((row.completed_sessions / v) * 100) : 0
      return (
        <div className="flex items-center gap-2">
          <div className="w-20 bg-surface-100 rounded-full h-1.5">
            <div className={`h-1.5 rounded-full transition-all ${pct >= 100 ? 'bg-emerald-500' : 'bg-primary-500'}`}
                 style={{ width: `${Math.min(100, pct)}%` }} />
          </div>
          <span className="text-xs text-surface-500 font-medium">{row.completed_sessions}/{v}</span>
        </div>
      )
    }},
    { key: 'active' as const, label: 'Estado', render: (v: boolean) =>
      <span className={`badge ${v ? 'bg-emerald-50 text-emerald-600 ring-1 ring-inset ring-emerald-200' : 'bg-surface-100 text-surface-400'}`}>
        {v ? 'Activo' : 'Finalizado'}
      </span>
    },
    { key: 'start_date' as const, label: 'Inicio', render: (v: string) =>
      <span className="text-surface-600 text-sm">{new Date(v).toLocaleDateString('es-ES')}</span>
    },
    { key: 'id' as const, label: '', render: (_v: string, row: RehabPlan) =>
      <button
        onClick={(e) => { e.stopPropagation(); setSelectedPlanId(row.id); setTab('sessions') }}
        className="text-primary-600 text-xs font-medium hover:text-primary-700 transition-colors"
      >
        Ver sesiones →
      </button>
    },
  ]

  const sessionColumns = [
    { key: 'scheduled_at' as const, label: 'Fecha', render: (v: string) =>
      <div>
        <span className="font-medium text-surface-800">{new Date(v).toLocaleDateString('es-ES')}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{new Date(v).toLocaleTimeString('es-ES', { hour: '2-digit', minute: '2-digit' })}</p>
      </div>
    },
    { key: 'status' as const, label: 'Estado', render: (v: string) => <StatusBadge status={v} /> },
    { key: 'duration_minutes' as const, label: 'Duración', render: (v: number) =>
      <span className="text-surface-600">{v} min</span>
    },
    { key: 'room' as const, label: 'Sala', render: (v: string) =>
      v ? <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-500">{v}</span>
        : <span className="text-surface-300">—</span>
    },
    { key: 'exercises' as const, label: 'Ejercicios', render: (v: string) =>
      v ? <span className="text-surface-600 text-xs max-w-[150px] truncate block">{v}</span>
        : <span className="text-surface-300">—</span>
    },
    { key: 'pain_level' as const, label: 'Dolor', render: (v: number | null) => {
      if (v == null) return <span className="text-surface-300">—</span>
      const color = v <= 3 ? 'text-emerald-600' : v <= 6 ? 'text-amber-600' : 'text-red-600'
      const bg = v <= 3 ? 'bg-emerald-50' : v <= 6 ? 'bg-amber-50' : 'bg-red-50'
      return <span className={`badge ${bg} ${color} font-medium`}>{v}/10</span>
    }},
    { key: 'progress' as const, label: 'Progreso', render: (v: string) =>
      v ? <span className="text-surface-600 text-xs">{v}</span> : <span className="text-surface-300">—</span>
    },
  ]

  const activePlans = useMemo(() => plans.filter((plan) => plan.active).length, [plans])
  const completedSessions = useMemo(() => sessions.filter((session) => session.status === 'COMPLETED').length, [sessions])
  const avgPainLevel = useMemo(() => {
    const withPain = sessions.filter((session) => session.pain_level != null)
    if (withPain.length === 0) return '—'
    return (withPain.reduce((sum, session) => sum + (session.pain_level ?? 0), 0) / withPain.length).toFixed(1)
  }, [sessions])
  const planTypes = useMemo(() => Array.from(new Set(plans.map((plan) => plan.type))), [plans])
  const sessionStatuses = useMemo(() => Array.from(new Set(sessions.map((session) => session.status))), [sessions])

  const filteredPlans = useMemo(() => plans.filter((plan) => planTypeFilter === 'ALL' || plan.type === planTypeFilter), [planTypeFilter, plans])
  const filteredSessions = useMemo(() => sessions.filter((session) => sessionStatusFilter === 'ALL' || session.status === sessionStatusFilter), [sessionStatusFilter, sessions])

  return (
    <div>
      <PageHeader title="Rehabilitación y Fisioterapia" subtitle="Planes terapéuticos y seguimiento de sesiones" action="Nuevo plan" onAction={() => {}}>
        <div className="flex gap-1.5">
          <TabButton active={tab === 'plans'} onClick={() => setTab('plans')}>Planes</TabButton>
          <TabButton active={tab === 'sessions'} onClick={() => { setTab('sessions'); if (!selectedPlanId && plans.length > 0) setSelectedPlanId(plans[0].id) }}>Sesiones</TabButton>
        </div>
      </PageHeader>

      <KpiStrip
        items={[
          { label: 'Planes', value: plans.length, hint: 'terapéuticos cargados', tone: 'info' },
          { label: 'Activos', value: activePlans, hint: 'en seguimiento', tone: 'success' },
          { label: 'Sesiones completadas', value: completedSessions, hint: 'del plan visible', tone: 'default' },
          { label: 'Dolor medio', value: avgPainLevel === '—' ? '—' : `${avgPainLevel}/10`, hint: 'sesiones visibles', tone: 'warning' },
        ]}
      />

      <div className="card p-4 mb-6 grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Plan foco</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{selectedPlan?.diagnosis ?? 'Sin selección'}</p>
          <p className="text-sm text-surface-500 mt-1">{selectedPlan ? `${therapyLabels[selectedPlan.type] ?? selectedPlan.type} · ${selectedPlan.completed_sessions}/${selectedPlan.total_sessions}` : 'Selecciona un plan para contextualizar la vista'}</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Sesión foco</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{selectedSession?.room || 'Sin selección'}</p>
          <p className="text-sm text-surface-500 mt-1">{selectedSession ? `${selectedSession.status} · ${new Date(selectedSession.scheduled_at).toLocaleString('es-ES')}` : 'Selecciona una sesión para revisar progreso'}</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Cobertura</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{plans.filter((plan) => plan.completed_sessions >= plan.total_sessions && plan.total_sessions > 0).length} planes cerrados</p>
          <p className="text-sm text-surface-500 mt-1">estado global del pipeline de rehabilitación</p>
        </div>
      </div>

      {tab === 'plans' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={planTypeFilter} onChange={(e) => setPlanTypeFilter(e.target.value)}>
            <option value="ALL">Todos los tipos</option>
            {planTypes.map((type) => (
              <option key={type} value={type}>{therapyLabels[type] ?? type}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredPlans.length} planes visibles</span>
        </div>
      )}

      {tab === 'sessions' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={sessionStatusFilter} onChange={(e) => setSessionStatusFilter(e.target.value)}>
            <option value="ALL">Todos los estados</option>
            {sessionStatuses.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredSessions.length} sesiones visibles</span>
        </div>
      )}

      {tab === 'plans' ? (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable columns={planColumns} data={filteredPlans} loading={loadingPlans} emptyMessage="No hay planes de rehabilitación registrados." emptyIcon="🏃" onRowClick={(row) => { setSelectedPlan(row); setSelectedPlanId(row.id) }} selectedRowId={selectedPlan?.id ?? null} />
          <DetailPanel isEmpty={!selectedPlan} emptyTitle="Selecciona un plan" emptyText="Consulta objetivos, progreso y ventana terapéutica." title={selectedPlan ? therapyLabels[selectedPlan.type] ?? selectedPlan.type : undefined} subtitle={selectedPlan?.diagnosis}>
            {selectedPlan && (
              <>
                <DetailGroup title="Estrategia terapéutica">
                  <DetailRow label="Objetivos" value={selectedPlan.goals || 'Sin objetivos'} />
                  <DetailRow label="Inicio" value={new Date(selectedPlan.start_date).toLocaleDateString('es-ES')} />
                  <DetailRow label="Fin" value={selectedPlan.end_date ? new Date(selectedPlan.end_date).toLocaleDateString('es-ES') : 'Pendiente'} />
                </DetailGroup>
                <DetailGroup title="Progreso">
                  <DetailRow label="Sesiones" value={`${selectedPlan.completed_sessions}/${selectedPlan.total_sessions}`} accent />
                  <DetailRow label="Estado" value={selectedPlan.active ? 'Activo' : 'Finalizado'} />
                  <DetailRow label="Notas" value={selectedPlan.notes || 'Sin notas'} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      ) : (
        <div>
          {selectedPlanId && (
            <div className="mb-4 flex items-center gap-3">
              <button onClick={() => { setTab('plans'); setSelectedPlanId(null) }}
                className="text-sm text-primary-600 hover:text-primary-700 font-medium transition-colors">
                ← Volver a planes
              </button>
              <span className="text-xs text-surface-400 font-mono bg-surface-100 px-2 py-1 rounded-lg">Plan: {selectedPlanId.slice(0, 8)}…</span>
            </div>
          )}
          <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
            <DataTable columns={sessionColumns} data={filteredSessions} loading={loadingSessions} emptyMessage="No hay sesiones para este plan." emptyIcon="📋" onRowClick={setSelectedSession} selectedRowId={selectedSession?.id ?? null} />
            <DetailPanel isEmpty={!selectedSession} emptyTitle="Selecciona una sesión" emptyText="Consulta dolor, ejercicios y progreso funcional." title={selectedSession?.room || 'Sesión de rehab'} subtitle={selectedSession ? `${selectedSession.status} · ${new Date(selectedSession.scheduled_at).toLocaleString('es-ES')}` : undefined}>
              {selectedSession && (
                <>
                  <DetailGroup title="Sesión">
                    <DetailRow label="Duración" value={`${selectedSession.duration_minutes} min`} accent />
                    <DetailRow label="Sala" value={selectedSession.room || '—'} />
                    <DetailRow label="Dolor" value={selectedSession.pain_level != null ? `${selectedSession.pain_level}/10` : 'No informado'} />
                  </DetailGroup>
                  <DetailGroup title="Trabajo terapéutico">
                    <DetailRow label="Ejercicios" value={selectedSession.exercises || 'Sin ejercicios'} />
                    <DetailRow label="Progreso" value={selectedSession.progress || 'Sin progreso'} />
                    <DetailRow label="Notas" value={selectedSession.notes || 'Sin notas'} />
                  </DetailGroup>
                </>
              )}
            </DetailPanel>
          </div>
        </div>
      )}
    </div>
  )
}
