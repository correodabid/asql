import { useEffect, useState } from 'react'
import { api } from '../services/api'
import DataTable from '../components/DataTable'
import PageHeader, { StatusBadge } from '../components/PageHeader'

interface GuardShift {
  id: string; staff_id: string; department_id: string; type: string;
  status: string; start_time: string; end_time: string; notes: string;
}

const shiftTypeLabels: Record<string, string> = {
  DAY: 'Diurna', NIGHT: 'Nocturna', WEEKEND: 'Fin de Semana', HOLIDAY: 'Festivo', ON_CALL: 'Localizable',
}
const shiftTypeColors: Record<string, string> = {
  DAY: 'bg-amber-50 text-amber-600', NIGHT: 'bg-indigo-50 text-indigo-600',
  WEEKEND: 'bg-purple-50 text-purple-600', HOLIDAY: 'bg-rose-50 text-rose-600',
  ON_CALL: 'bg-cyan-50 text-cyan-600',
}

export default function GuardShiftsPage() {
  const [shifts, setShifts] = useState<GuardShift[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => { loadShifts() }, [])

  async function loadShifts() {
    setLoading(true)
    try {
      const res = await api.get<GuardShift[]>('/guard-shifts')
      setShifts(res.data ?? [])
    } finally { setLoading(false) }
  }

  const columns = [
    { key: 'type' as const, label: 'Tipo', render: (v: string) =>
      <span className={`badge ${shiftTypeColors[v] ?? 'bg-surface-100 text-surface-600'}`}>{shiftTypeLabels[v] ?? v}</span>
    },
    { key: 'status' as const, label: 'Estado', render: (v: string) => <StatusBadge status={v} /> },
    { key: 'start_time' as const, label: 'Inicio', render: (v: string) =>
      <div>
        <span className="font-medium text-surface-800">{new Date(v).toLocaleDateString('es-ES')}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{new Date(v).toLocaleTimeString('es-ES', { hour: '2-digit', minute: '2-digit' })}</p>
      </div>
    },
    { key: 'end_time' as const, label: 'Fin', render: (v: string) =>
      <div>
        <span className="font-medium text-surface-800">{new Date(v).toLocaleDateString('es-ES')}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{new Date(v).toLocaleTimeString('es-ES', { hour: '2-digit', minute: '2-digit' })}</p>
      </div>
    },
    { key: 'notes' as const, label: 'Notas', render: (v: string) =>
      v ? <span className="text-surface-500 text-xs">{v}</span> : <span className="text-surface-300">—</span>
    },
  ]

  return (
    <div>
      <PageHeader title="Gestión de Guardias" subtitle="Turnos de guardia y disponibilidad del personal" action="Nueva guardia" onAction={() => {}} />
      <DataTable columns={columns} data={shifts} loading={loading} emptyMessage="No hay guardias programadas." emptyIcon="⏰" />
    </div>
  )
}
