import { AnimatePresence, motion } from 'framer-motion'
import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api, type Patient, type Staff } from '../services/api'
import { flatNavItems } from './navigation'
import { MagnifyingGlassIcon, SparklesIcon, XMarkIcon } from './icons'

type AppointmentLite = {
  id: string
  type: string
  status: string
  scheduled_at: string
  room: string
}

type DocumentLite = {
  id: string
  title: string
  category: string
  file_name: string
  created_at: string
}

type CommandItem = {
  id: string
  category: 'Navegación' | 'Pacientes' | 'Personal' | 'Citas' | 'Documentos' | 'Acciones'
  label: string
  description: string
  keywords: string[]
  action: () => void
}

export default function GlobalCommandPalette({
  open,
  onClose,
  onLogout,
}: {
  open: boolean
  onClose: () => void
  onLogout: () => void
}) {
  const navigate = useNavigate()
  const inputRef = useRef<HTMLInputElement>(null)
  const [query, setQuery] = useState('')
  const [selectedIndex, setSelectedIndex] = useState(0)
  const [patients, setPatients] = useState<Patient[]>([])
  const [staff, setStaff] = useState<Staff[]>([])
  const [appointments, setAppointments] = useState<AppointmentLite[]>([])
  const [documents, setDocuments] = useState<DocumentLite[]>([])
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    if (!open) return
    const timeout = window.setTimeout(() => inputRef.current?.focus(), 10)
    return () => window.clearTimeout(timeout)
  }, [open])

  useEffect(() => {
    if (!open || loaded) return
    void Promise.all([
      api.get<Patient[]>('/patients').then((res) => setPatients((res.data ?? []).slice(0, 30))).catch(() => setPatients([])),
      api.get<Staff[]>('/staff').then((res) => setStaff((res.data ?? []).slice(0, 30))).catch(() => setStaff([])),
      api.get<AppointmentLite[]>('/appointments').then((res) => setAppointments((res.data ?? []).slice(0, 30))).catch(() => setAppointments([])),
      api.get<DocumentLite[]>('/documents').then((res) => setDocuments((res.data ?? []).slice(0, 30))).catch(() => setDocuments([])),
    ]).finally(() => setLoaded(true))
  }, [loaded, open])

  useEffect(() => {
    if (!open) {
      setQuery('')
      setSelectedIndex(0)
    }
  }, [open])

  const commands = useMemo<CommandItem[]>(() => {
    const navigationCommands: CommandItem[] = flatNavItems.map((item) => ({
      id: `nav-${item.to}`,
      category: 'Navegación',
      label: item.label,
      description: `${item.section} · ${item.to}`,
      keywords: [item.label, item.to, item.section],
      action: () => {
        navigate(item.to)
        onClose()
      },
    }))

    const actionCommands: CommandItem[] = [
      {
        id: 'action-dashboard',
        category: 'Acciones',
        label: 'Ir al dashboard ejecutivo',
        description: 'Vista global del hospital',
        keywords: ['dashboard', 'inicio', 'ejecutivo'],
        action: () => {
          navigate('/')
          onClose()
        },
      },
      {
        id: 'action-logout',
        category: 'Acciones',
        label: 'Cerrar sesión',
        description: 'Salir de Hospital MiKS',
        keywords: ['logout', 'salir', 'cerrar sesión'],
        action: () => {
          onClose()
          onLogout()
        },
      },
    ]

    const patientCommands: CommandItem[] = patients.map((patient) => ({
      id: `patient-${patient.id}`,
      category: 'Pacientes',
      label: `${patient.first_name} ${patient.last_name}`,
      description: `${patient.medical_record_no} · ${patient.insurance_company || 'sin aseguradora'}`,
      keywords: [patient.first_name, patient.last_name, patient.medical_record_no, patient.national_id, patient.email],
      action: () => {
        navigate('/patients', { state: { focusType: 'patient', focusId: patient.id } })
        onClose()
      },
    }))

    const staffCommands: CommandItem[] = staff.map((member) => ({
      id: `staff-${member.id}`,
      category: 'Personal',
      label: `${member.first_name} ${member.last_name}`,
      description: `${member.employee_code} · ${member.specialty || member.staff_type}`,
      keywords: [member.first_name, member.last_name, member.employee_code, member.specialty, member.staff_type, member.email],
      action: () => {
        navigate('/staff', { state: { focusType: 'staff', focusId: member.id, openTab: 'staff' } })
        onClose()
      },
    }))

    const appointmentCommands: CommandItem[] = appointments.map((appointment) => ({
      id: `appointment-${appointment.id}`,
      category: 'Citas',
      label: `${appointment.type} · ${new Date(appointment.scheduled_at).toLocaleDateString('es-ES')}`,
      description: `${appointment.status} · ${appointment.room || 'sin sala'}`,
      keywords: [appointment.type, appointment.status, appointment.room, appointment.scheduled_at],
      action: () => {
        navigate('/appointments', { state: { focusType: 'appointment', focusId: appointment.id, openTab: 'appointments' } })
        onClose()
      },
    }))

    const documentCommands: CommandItem[] = documents.map((document) => ({
      id: `document-${document.id}`,
      category: 'Documentos',
      label: document.title,
      description: `${document.category} · ${document.file_name}`,
      keywords: [document.title, document.category, document.file_name],
      action: () => {
        navigate('/documents', { state: { focusType: 'document', focusId: document.id, openTab: 'documents' } })
        onClose()
      },
    }))

    return [
      ...actionCommands,
      ...navigationCommands,
      ...patientCommands,
      ...staffCommands,
      ...appointmentCommands,
      ...documentCommands,
    ]
  }, [appointments, documents, navigate, onClose, onLogout, patients, staff])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return commands.slice(0, 12)
    return commands
      .filter((command) => [command.label, command.description, ...command.keywords].join(' ').toLowerCase().includes(q))
      .slice(0, 18)
  }, [commands, query])

  useEffect(() => {
    if (selectedIndex >= filtered.length) {
      setSelectedIndex(0)
    }
  }, [filtered.length, selectedIndex])

  useEffect(() => {
    if (!open) return
    function handleKeydown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        onClose()
        return
      }
      if (event.key === 'ArrowDown') {
        event.preventDefault()
        setSelectedIndex((current) => Math.min(current + 1, Math.max(filtered.length - 1, 0)))
      }
      if (event.key === 'ArrowUp') {
        event.preventDefault()
        setSelectedIndex((current) => Math.max(current - 1, 0))
      }
      if (event.key === 'Enter') {
        event.preventDefault()
        filtered[selectedIndex]?.action()
      }
    }

    window.addEventListener('keydown', handleKeydown)
    return () => window.removeEventListener('keydown', handleKeydown)
  }, [filtered, onClose, open, selectedIndex])

  return (
    <AnimatePresence>
      {open && (
        <>
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-[80] bg-slate-950/45 backdrop-blur-sm"
            onClick={onClose}
          />
          <motion.div
            initial={{ opacity: 0, y: 16, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 16, scale: 0.98 }}
            transition={{ duration: 0.18 }}
            className="fixed left-1/2 top-[10vh] z-[90] w-[min(760px,calc(100vw-2rem))] -translate-x-1/2"
          >
            <div className="overflow-hidden rounded-[28px] border border-white/60 bg-white/92 shadow-[0_30px_90px_rgba(15,23,42,0.25)] backdrop-blur-2xl">
              <div className="flex items-center gap-3 border-b border-slate-200/80 px-5 py-4">
                <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-primary-50 text-primary-600">
                  <SparklesIcon className="h-5 w-5" />
                </div>
                <div className="relative flex-1">
                  <MagnifyingGlassIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-surface-400" />
                  <input
                    ref={inputRef}
                    value={query}
                    onChange={(e) => {
                      setQuery(e.target.value)
                      setSelectedIndex(0)
                    }}
                    placeholder="Busca módulos, pacientes, personal, citas o documentos…"
                    className="w-full rounded-2xl border border-slate-200 bg-slate-50/80 py-3 pl-10 pr-4 text-sm text-surface-800 outline-none transition focus:border-primary-300 focus:bg-white focus:ring-4 focus:ring-primary-100"
                  />
                </div>
                <button onClick={onClose} className="rounded-xl border border-slate-200 p-2 text-surface-500 transition hover:bg-slate-100 hover:text-surface-800">
                  <XMarkIcon className="h-4 w-4" />
                </button>
              </div>

              <div className="flex items-center justify-between border-b border-slate-100 px-5 py-3 text-xs text-surface-400">
                <span>{loaded ? `${commands.length} comandos indexados` : 'Indexando contexto…'}</span>
                <span className="rounded-full bg-slate-100 px-2.5 py-1 font-medium text-surface-500">⌘K</span>
              </div>

              <div className="max-h-[60vh] overflow-y-auto p-2">
                {filtered.length === 0 ? (
                  <div className="px-4 py-12 text-center">
                    <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-2xl bg-surface-100 text-2xl">🔎</div>
                    <p className="text-sm font-medium text-surface-700">Sin coincidencias</p>
                    <p className="mt-1 text-sm text-surface-400">Prueba con un nombre, módulo, prioridad o categoría.</p>
                  </div>
                ) : (
                  filtered.map((command, index) => (
                    <button
                      key={command.id}
                      onClick={command.action}
                      className={`flex w-full items-start gap-3 rounded-2xl px-4 py-3 text-left transition ${index === selectedIndex ? 'bg-primary-50 ring-1 ring-inset ring-primary-100' : 'hover:bg-slate-50'}`}
                    >
                      <div className={`mt-0.5 rounded-xl px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] ${index === selectedIndex ? 'bg-primary-100 text-primary-700' : 'bg-slate-100 text-surface-500'}`}>
                        {command.category}
                      </div>
                      <div className="min-w-0 flex-1">
                        <p className="truncate text-sm font-medium text-surface-800">{command.label}</p>
                        <p className="truncate text-xs text-surface-500">{command.description}</p>
                      </div>
                    </button>
                  ))
                )}
              </div>
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  )
}
