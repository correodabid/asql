import { useEffect, useRef, useState } from 'react'
import { motion } from 'framer-motion'
import { Link } from 'react-router-dom'
import { api } from '../services/api'
import {
  UsersIcon, HeartIcon, CalendarIcon, BedIcon,
  BeakerIcon, CurrencyIcon, ClockIcon, ActivityIcon,
  DocumentIcon, ShieldCheckIcon, BoltIcon, ScissorsIcon,
} from '../components/icons'

interface Stats {
  staff: number
  patients: number
  appointments: number
  admissions: number
  invoices: number
  documents: number
  prescriptions: number
  surgeries: number
  users: number
  lowStock: number
  availableBeds: number
  overdueInvoices: number
  pendingSurgeries: number
}

interface InvoiceLite {
  status?: string
  total?: number
}

interface SurgeryLite {
  status?: string
}

interface PrescriptionLite {
  status?: string
}

function extractCount<T>(result: { data: T; meta?: { total: number } } | null | undefined) {
  if (!result) return 0
  return Array.isArray(result.data) ? result.data.length : result.meta?.total ?? 0
}

function extractArrayItems<T>(result: { data: T[] } | null | undefined): T[] {
  return Array.isArray(result?.data) ? result.data : []
}

function AnimatedNumber({ value, duration = 1200 }: { value: number; duration?: number }) {
  const [display, setDisplay] = useState(0)
  const ref = useRef<number>()

  useEffect(() => {
    const start = 0
    const startTime = performance.now()

    function tick(now: number) {
      const elapsed = now - startTime
      const progress = Math.min(elapsed / duration, 1)
      // ease-out quad
      const eased = 1 - (1 - progress) * (1 - progress)
      setDisplay(Math.round(start + (value - start) * eased))
      if (progress < 1) ref.current = requestAnimationFrame(tick)
    }

    ref.current = requestAnimationFrame(tick)
    return () => { if (ref.current) cancelAnimationFrame(ref.current) }
  }, [value, duration])

  return <>{display}</>
}

export default function Dashboard() {
  const [stats, setStats] = useState<Stats>({
    staff: 0, patients: 0, appointments: 0, admissions: 0,
    invoices: 0, documents: 0, prescriptions: 0, surgeries: 0,
    users: 0, lowStock: 0, availableBeds: 0, overdueInvoices: 0, pendingSurgeries: 0,
  })
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      try {
        const [staffRes, patientRes, apptRes, admRes, invoiceRes, documentRes, prescriptionRes, surgeryRes, userRes, lowStockRes, bedsRes] = await Promise.allSettled([
          api.get<unknown[]>('/staff'),
          api.get<unknown[]>('/patients'),
          api.get<unknown[]>('/appointments'),
          api.get<unknown[]>('/admissions'),
          api.get<InvoiceLite[]>('/billing/invoices'),
          api.get<unknown[]>('/documents'),
          api.get<PrescriptionLite[]>('/pharmacy/prescriptions'),
          api.get<SurgeryLite[]>('/surgery/procedures'),
          api.get<unknown[]>('/auth/users'),
          api.get<unknown[]>('/pharmacy/medications/low-stock'),
          api.get<unknown[]>('/admissions/beds/available'),
        ])

        const invoices: InvoiceLite[] = invoiceRes.status === 'fulfilled' ? extractArrayItems<InvoiceLite>(invoiceRes.value) : []
        const surgeries: SurgeryLite[] = surgeryRes.status === 'fulfilled' ? extractArrayItems<SurgeryLite>(surgeryRes.value) : []

        setStats({
          staff: staffRes.status === 'fulfilled' ? extractCount(staffRes.value) : 0,
          patients: patientRes.status === 'fulfilled' ? extractCount(patientRes.value) : 0,
          appointments: apptRes.status === 'fulfilled' ? extractCount(apptRes.value) : 0,
          admissions: admRes.status === 'fulfilled' ? extractCount(admRes.value) : 0,
          invoices: invoiceRes.status === 'fulfilled' ? extractCount(invoiceRes.value) : 0,
          documents: documentRes.status === 'fulfilled' ? extractCount(documentRes.value) : 0,
          prescriptions: prescriptionRes.status === 'fulfilled' ? extractCount(prescriptionRes.value) : 0,
          surgeries: surgeryRes.status === 'fulfilled' ? extractCount(surgeryRes.value) : 0,
          users: userRes.status === 'fulfilled' ? extractCount(userRes.value) : 0,
          lowStock: lowStockRes.status === 'fulfilled' ? extractCount(lowStockRes.value) : 0,
          availableBeds: bedsRes.status === 'fulfilled' ? extractCount(bedsRes.value) : 0,
          overdueInvoices: invoices.filter((item) => item.status === 'OVERDUE').length,
          pendingSurgeries: surgeries.filter((item) => item.status === 'SCHEDULED' || item.status === 'PRE_OP').length,
        })
      } finally {
        setLoading(false)
      }
    }
    load()
  }, [])

  const occupancyBase = stats.admissions + stats.availableBeds
  const occupancyRate = occupancyBase > 0 ? Math.round((stats.admissions / occupancyBase) * 100) : 0
  const clinicalLoad = stats.appointments + stats.admissions + stats.surgeries
  const financialPressure = stats.overdueInvoices * 12 + stats.invoices

  const statCards = [
    {
      label: 'Personal', value: stats.staff, icon: UsersIcon, link: '/staff',
      gradient: 'from-blue-500 to-blue-600', bg: 'bg-blue-50', text: 'text-blue-600',
      desc: 'médicos, enfermeros, administrativos',
    },
    {
      label: 'Pacientes', value: stats.patients, icon: HeartIcon, link: '/patients',
      gradient: 'from-emerald-500 to-emerald-600', bg: 'bg-emerald-50', text: 'text-emerald-600',
      desc: 'registros activos',
    },
    {
      label: 'Citas', value: stats.appointments, icon: CalendarIcon, link: '/appointments',
      gradient: 'from-amber-500 to-orange-500', bg: 'bg-amber-50', text: 'text-amber-600',
      desc: 'programadas',
    },
    {
      label: 'Ingresos Activos', value: stats.admissions, icon: BedIcon, link: '/admissions',
      gradient: 'from-rose-500 to-rose-600', bg: 'bg-rose-50', text: 'text-rose-600',
      desc: 'pacientes hospitalizados',
    },
    {
      label: 'Facturas', value: stats.invoices, icon: CurrencyIcon, link: '/billing',
      gradient: 'from-emerald-500 to-teal-600', bg: 'bg-emerald-50', text: 'text-emerald-600',
      desc: `${stats.overdueInvoices} con riesgo financiero`,
    },
    {
      label: 'Documentos', value: stats.documents, icon: DocumentIcon, link: '/documents',
      gradient: 'from-violet-500 to-purple-600', bg: 'bg-violet-50', text: 'text-violet-600',
      desc: 'trazabilidad clínica y legal',
    },
    {
      label: 'Prescripciones', value: stats.prescriptions, icon: BeakerIcon, link: '/pharmacy',
      gradient: 'from-amber-500 to-orange-500', bg: 'bg-amber-50', text: 'text-amber-600',
      desc: `${stats.lowStock} medicamentos en alerta`,
    },
    {
      label: 'Cirugías', value: stats.surgeries, icon: ScissorsIcon, link: '/surgery',
      gradient: 'from-rose-500 to-pink-600', bg: 'bg-rose-50', text: 'text-rose-600',
      desc: `${stats.pendingSurgeries} pendientes de quirófano`,
    },
  ]

  const quickLinks = [
    { label: 'Farmacia', icon: BeakerIcon, to: '/pharmacy', color: 'text-purple-500', bg: 'bg-purple-50' },
    { label: 'Facturación', icon: CurrencyIcon, to: '/billing', color: 'text-emerald-500', bg: 'bg-emerald-50' },
    { label: 'Guardias', icon: ClockIcon, to: '/guard-shifts', color: 'text-blue-500', bg: 'bg-blue-50' },
    { label: 'Rehabilitación', icon: ActivityIcon, to: '/rehab', color: 'text-amber-500', bg: 'bg-amber-50' },
    { label: 'Documentos', icon: DocumentIcon, to: '/documents', color: 'text-violet-500', bg: 'bg-violet-50' },
    { label: 'ASQL Explorer', icon: BoltIcon, to: '/explorer', color: 'text-slate-700', bg: 'bg-slate-100' },
  ]

  const priorityTiles = [
    {
      title: 'Ocupación hospitalaria',
      value: `${occupancyRate}%`,
      subtitle: `${stats.admissions} ingresos · ${stats.availableBeds} camas libres`,
      color: occupancyRate > 80 ? 'from-rose-500 to-orange-500' : 'from-emerald-500 to-teal-500',
      progress: occupancyRate,
    },
    {
      title: 'Riesgo farmacéutico',
      value: `${stats.lowStock}`,
      subtitle: 'medicamentos con stock bajo',
      color: stats.lowStock > 0 ? 'from-amber-500 to-red-500' : 'from-emerald-500 to-teal-500',
      progress: Math.min(100, stats.lowStock * 15),
    },
    {
      title: 'Gobernanza',
      value: `${stats.users}`,
      subtitle: 'cuentas de acceso activas',
      color: 'from-blue-500 to-indigo-600',
      progress: Math.min(100, stats.users * 8),
    },
  ]

  const moduleBars = [
    { label: 'Clínica', value: stats.appointments + stats.admissions + stats.surgeries, color: 'bg-blue-500' },
    { label: 'Farmacia', value: stats.prescriptions + stats.lowStock, color: 'bg-amber-500' },
    { label: 'Billing', value: financialPressure, color: 'bg-emerald-500' },
    { label: 'Documental', value: stats.documents + stats.users, color: 'bg-violet-500' },
  ]
  const moduleMax = Math.max(...moduleBars.map((item) => item.value), 1)

  return (
    <div className="space-y-6">
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        animate={{ opacity: 1, y: 0 }}
        className="relative overflow-hidden rounded-[28px] bg-[linear-gradient(135deg,#0f172a_0%,#111827_45%,#064e3b_100%)] p-7 text-white shadow-[0_24px_60px_rgba(15,23,42,0.28)]"
      >
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(52,211,153,0.18),transparent_28%),radial-gradient(circle_at_bottom_left,rgba(59,130,246,0.14),transparent_24%)]" />
        <div className="relative flex flex-col xl:flex-row xl:items-end xl:justify-between gap-6">
          <div className="max-w-3xl">
            <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-[11px] uppercase tracking-[0.2em] text-primary-300">
              <ShieldCheckIcon className="w-3.5 h-3.5" />
              Centro de mando hospitalario
            </div>
            <h2 className="mt-4 text-3xl md:text-4xl font-semibold tracking-tight">Una vista ejecutiva, clínica y operativa de toda la suite.</h2>
            <p className="mt-3 text-sm text-white/65 max-w-2xl">
              Hospital MiKS ahora refleja múltiples dominios reales del modelo: actividad clínica, farmacia, ingresos, cirugía, facturación, trazabilidad documental e identidad.
            </p>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 min-w-full xl:min-w-[520px]">
            {priorityTiles.map((tile, index) => (
              <motion.div
                key={tile.title}
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.1 + index * 0.08 }}
                className="rounded-2xl border border-white/10 bg-white/[0.06] backdrop-blur-xl p-4"
              >
                <p className="text-[11px] uppercase tracking-wider text-white/50">{tile.title}</p>
                <div className="mt-2 text-2xl font-semibold">{tile.value}</div>
                <p className="text-xs text-white/50 mt-1">{tile.subtitle}</p>
                <div className="mt-4 h-1.5 rounded-full bg-white/10 overflow-hidden">
                  <div className={`h-full rounded-full bg-gradient-to-r ${tile.color}`} style={{ width: `${tile.progress}%` }} />
                </div>
              </motion.div>
            ))}
          </div>
        </div>
      </motion.div>

      {/* Stat cards */}
      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-5">
          {[0, 1, 2, 3, 4, 5, 6, 7].map(i => (
            <div key={i} className="card p-6 animate-pulse">
              <div className="h-3 bg-surface-100 rounded-full w-20 mb-4" />
              <div className="h-8 bg-surface-100 rounded-lg w-16 mb-2" />
              <div className="h-2 bg-surface-50 rounded-full w-32" />
            </div>
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-5">
          {statCards.map((card, i) => (
            <motion.div
              key={card.label}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.1, duration: 0.4 }}
            >
              <Link to={card.link} className="card p-6 block group cursor-pointer hover:scale-[1.02] transition-transform duration-300">
                <div className="flex items-start justify-between mb-4">
                  <div className={`${card.bg} rounded-xl p-2.5 ${card.text} transition-transform group-hover:scale-110 duration-300`}>
                    <card.icon className="w-5 h-5" />
                  </div>
                  <div className={`w-12 h-6 rounded-full bg-gradient-to-r ${card.gradient} opacity-20 blur-md`} />
                </div>
                <div className="stat-value text-surface-900 mb-1">
                  <AnimatedNumber value={card.value} />
                </div>
                <p className="text-sm font-medium text-surface-600">{card.label}</p>
                <p className="text-xs text-surface-400 mt-0.5">{card.desc}</p>
                <div className="mt-4 flex items-center justify-between text-[11px] text-surface-400">
                  <span>Ver módulo</span>
                  <span className="group-hover:text-surface-600 transition-colors">→</span>
                </div>
              </Link>
            </motion.div>
          ))}
        </div>
      )}

      <div className="grid grid-cols-1 xl:grid-cols-12 gap-6">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.5 }}
          className="xl:col-span-5 card p-6"
        >
          <div className="flex items-center justify-between mb-5">
            <h3 className="text-sm font-semibold text-surface-800">Radar Operativo</h3>
            <span className="badge bg-primary-50 text-primary-600">Tiempo real</span>
          </div>
          <div className="space-y-4">
            {[
              {
                label: 'Carga clínica combinada',
                value: clinicalLoad,
                detail: `${stats.appointments} citas · ${stats.admissions} ingresos · ${stats.surgeries} cirugías`,
                color: 'bg-blue-500',
              },
              {
                label: 'Riesgo financiero',
                value: financialPressure,
                detail: `${stats.overdueInvoices} facturas vencidas`,
                color: 'bg-emerald-500',
              },
              {
                label: 'Gobierno documental',
                value: stats.documents + stats.users,
                detail: `${stats.documents} documentos · ${stats.users} cuentas`,
                color: 'bg-violet-500',
              },
              {
                label: 'Sensibilidad farmacia',
                value: stats.prescriptions + stats.lowStock,
                detail: `${stats.prescriptions} recetas · ${stats.lowStock} alertas`,
                color: 'bg-amber-500',
              },
            ].map((item, i) => (
              <motion.div
                key={item.label}
                initial={{ opacity: 0, y: 12 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.6 + i * 0.08 }}
                className="rounded-2xl border border-slate-200/80 bg-slate-50/70 p-4"
              >
                <div className="flex items-center justify-between gap-4 mb-3">
                  <div>
                    <p className="text-xs uppercase tracking-wider text-surface-400">{item.label}</p>
                    <p className="text-xl font-semibold text-surface-900 mt-1">{item.value}</p>
                  </div>
                  <div className={`w-3 h-3 rounded-full ${item.color}`} />
                </div>
                <div className="h-2 rounded-full bg-white overflow-hidden border border-slate-200/80">
                  <div className={`h-full ${item.color}`} style={{ width: `${Math.min(100, (item.value / Math.max(clinicalLoad, financialPressure, stats.documents + stats.users, stats.prescriptions + stats.lowStock, 1)) * 100)}%` }} />
                </div>
                <p className="text-xs text-surface-500 mt-2">{item.detail}</p>
              </motion.div>
            ))}
          </div>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.6 }}
          className="xl:col-span-4 card p-6"
        >
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold text-surface-800">Mapa de módulos</h3>
            <span className="text-[11px] text-surface-400">Cobertura funcional</span>
          </div>
          <div className="space-y-4 mb-6">
            {moduleBars.map((item, index) => (
              <motion.div key={item.label} initial={{ opacity: 0, x: 8 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: 0.7 + index * 0.07 }}>
                <div className="flex items-center justify-between text-xs mb-1.5">
                  <span className="font-medium text-surface-700">{item.label}</span>
                  <span className="text-surface-400">{item.value}</span>
                </div>
                <div className="h-2 rounded-full bg-slate-100 overflow-hidden">
                  <div className={`h-full rounded-full ${item.color}`} style={{ width: `${(item.value / moduleMax) * 100}%` }} />
                </div>
              </motion.div>
            ))}
          </div>

          <h4 className="text-xs font-semibold text-surface-500 uppercase tracking-wider mb-3">Acceso rápido</h4>
          <div className="space-y-2">
            {quickLinks.map((link, i) => (
              <motion.div
                key={link.to}
                initial={{ opacity: 0, x: 10 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.7 + i * 0.08 }}
              >
                <Link
                  to={link.to}
                  className="flex items-center gap-3 p-3 rounded-xl hover:bg-surface-50 transition-all duration-200 group"
                >
                  <div className={`${link.bg} ${link.color} rounded-lg p-2 transition-transform group-hover:scale-110 duration-200`}>
                    <link.icon className="w-4 h-4" />
                  </div>
                  <span className="text-sm font-medium text-surface-700 group-hover:text-surface-900">{link.label}</span>
                  <svg className="w-4 h-4 text-surface-300 ml-auto opacity-0 group-hover:opacity-100 transition-all transform translate-x-0 group-hover:translate-x-1" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
                  </svg>
                </Link>
              </motion.div>
            ))}
          </div>
          <div className="mt-5 p-4 rounded-xl bg-gradient-to-br from-surface-900 to-surface-800 text-white">
            <div className="flex items-center gap-2 mb-2">
              <div className="w-1.5 h-1.5 rounded-full bg-primary-400 animate-pulse-slow" />
              <span className="text-[10px] uppercase tracking-wider text-primary-400 font-semibold">ASQL Engine</span>
            </div>
            <p className="text-xs text-white/50 leading-relaxed">
              Base de datos determinista con time-travel, auditoría inmutable y aislamiento por dominio.
            </p>
            <Link to="/explorer" className="text-[11px] text-primary-400 hover:text-primary-300 mt-2 inline-block font-medium">
              Explorar →
            </Link>
          </div>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.7 }}
          className="xl:col-span-3 card p-6"
        >
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold text-surface-800">Centro de atención</h3>
            <span className="badge bg-rose-50 text-rose-600">Prioridades</span>
          </div>
          <div className="space-y-3">
            {[
              {
                title: 'Farmacia crítica',
                desc: `${stats.lowStock} referencias con stock por debajo del mínimo`,
                to: '/pharmacy',
                accent: 'border-amber-200 bg-amber-50/70 text-amber-700',
              },
              {
                title: 'Ingresos activos',
                desc: `${stats.admissions} pacientes hospitalizados con ocupación al ${occupancyRate}%`,
                to: '/admissions',
                accent: 'border-rose-200 bg-rose-50/70 text-rose-700',
              },
              {
                title: 'Gobernanza documental',
                desc: `${stats.documents} documentos auditables y ${stats.users} cuentas de acceso`,
                to: '/documents',
                accent: 'border-violet-200 bg-violet-50/70 text-violet-700',
              },
              {
                title: 'Cirugía planificada',
                desc: `${stats.pendingSurgeries} cirugías en planificación o preoperatorio`,
                to: '/surgery',
                accent: 'border-blue-200 bg-blue-50/70 text-blue-700',
              },
            ].map((item, index) => (
              <motion.div key={item.title} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.75 + index * 0.06 }}>
                <Link to={item.to} className={`block rounded-2xl border p-4 transition-transform hover:scale-[1.02] ${item.accent}`}>
                  <p className="text-sm font-semibold">{item.title}</p>
                  <p className="text-xs mt-1 opacity-80">{item.desc}</p>
                </Link>
              </motion.div>
            ))}
          </div>
        </motion.div>
      </div>

      <motion.div
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.8 }}
        className="card p-4"
      >
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-6">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-primary-400 animate-pulse-slow" />
              <span className="text-xs font-medium text-surface-600">Sistema operativo</span>
            </div>
            <div className="flex items-center gap-4 text-[11px] text-surface-400">
              <span>12+ vistas operativas activas</span>
              <span className="w-px h-3 bg-surface-200" />
              <span>WAL: determinista</span>
              <span className="w-px h-3 bg-surface-200" />
              <span>Identidad + auditoría integradas</span>
            </div>
          </div>
          <span className="text-[10px] text-surface-300 font-mono">ASQL v1.0 — Embedded</span>
        </div>
      </motion.div>
    </div>
  )
}
