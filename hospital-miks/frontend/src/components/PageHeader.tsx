import { type ReactNode } from 'react'
import { motion } from 'framer-motion'
import { PlusIcon } from './icons'

interface PageHeaderProps {
  title: string
  subtitle?: string
  action?: string
  onAction?: () => void
  children?: ReactNode
}

export default function PageHeader({ title, subtitle, action, onAction, children }: PageHeaderProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: -8 }}
      animate={{ opacity: 1, y: 0 }}
      className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-6"
    >
      <div>
        <h2 className="page-title">{title}</h2>
        {subtitle && <p className="page-subtitle">{subtitle}</p>}
      </div>
      <div className="flex items-center gap-3">
        {children}
        {action && onAction && (
          <button onClick={onAction} className="btn-primary">
            <PlusIcon className="w-4 h-4" />
            {action}
          </button>
        )}
      </div>
    </motion.div>
  )
}

// ── Reusable StatusBadge ────────────────────────────────────────

const STATUS_STYLES: Record<string, string> = {
  // Common statuses
  ACTIVE: 'bg-emerald-50 text-emerald-700 ring-emerald-600/10',
  ADMITTED: 'bg-emerald-50 text-emerald-700 ring-emerald-600/10',
  COMPLETED: 'bg-surface-100 text-surface-600 ring-surface-500/10',
  CANCELLED: 'bg-red-50 text-red-700 ring-red-600/10',
  SCHEDULED: 'bg-blue-50 text-blue-700 ring-blue-600/10',
  CONFIRMED: 'bg-emerald-50 text-emerald-700 ring-emerald-600/10',
  IN_PROGRESS: 'bg-amber-50 text-amber-700 ring-amber-600/10',
  PENDING: 'bg-amber-50 text-amber-700 ring-amber-600/10',
  NO_SHOW: 'bg-amber-50 text-amber-700 ring-amber-600/10',
  // Billing
  DRAFT: 'bg-surface-100 text-surface-600 ring-surface-500/10',
  ISSUED: 'bg-blue-50 text-blue-700 ring-blue-600/10',
  PAID: 'bg-emerald-50 text-emerald-700 ring-emerald-600/10',
  OVERDUE: 'bg-red-50 text-red-700 ring-red-600/10',
  // Room status
  AVAILABLE: 'bg-emerald-50 text-emerald-700 ring-emerald-600/10',
  IN_USE: 'bg-red-50 text-red-700 ring-red-600/10',
  CLEANING: 'bg-amber-50 text-amber-700 ring-amber-600/10',
  MAINTENANCE: 'bg-surface-100 text-surface-600 ring-surface-500/10',
  // Guard shifts
  SWAPPED: 'bg-amber-50 text-amber-700 ring-amber-600/10',
  // Admissions
  DISCHARGED: 'bg-surface-100 text-surface-600 ring-surface-500/10',
  TRANSFERRED: 'bg-blue-50 text-blue-700 ring-blue-600/10',
  // Messaging
  SENT: 'bg-blue-50 text-blue-700 ring-blue-600/10',
  DELIVERED: 'bg-emerald-50 text-emerald-700 ring-emerald-600/10',
  FAILED: 'bg-red-50 text-red-700 ring-red-600/10',
  READ: 'bg-surface-100 text-surface-600 ring-surface-500/10',
}

const STATUS_LABELS: Record<string, string> = {
  ACTIVE: 'Activo',
  ADMITTED: 'Ingresado',
  COMPLETED: 'Completado',
  CANCELLED: 'Cancelado',
  SCHEDULED: 'Programado',
  CONFIRMED: 'Confirmado',
  IN_PROGRESS: 'En curso',
  PENDING: 'Pendiente',
  NO_SHOW: 'No presentado',
  DRAFT: 'Borrador',
  ISSUED: 'Emitida',
  PAID: 'Pagada',
  OVERDUE: 'Vencida',
  AVAILABLE: 'Disponible',
  IN_USE: 'En uso',
  CLEANING: 'Limpieza',
  MAINTENANCE: 'Mantenimiento',
  SWAPPED: 'Intercambiado',
  DISCHARGED: 'Alta',
  TRANSFERRED: 'Trasladado',
  SENT: 'Enviado',
  DELIVERED: 'Entregado',
  FAILED: 'Fallido',
  READ: 'Leído',
}

export function StatusBadge({ status, label }: { status: string; label?: string }) {
  const style = STATUS_STYLES[status] ?? 'bg-surface-100 text-surface-600 ring-surface-500/10'
  const text = label ?? STATUS_LABELS[status] ?? status
  return (
    <span className={`badge ring-1 ring-inset ${style}`}>
      {text}
    </span>
  )
}

// ── Tab Button ──────────────────────────────────────────────────

interface TabButtonProps {
  active: boolean
  onClick: () => void
  children: ReactNode
  variant?: 'default' | 'danger'
}

export function TabButton({ active, onClick, children, variant = 'default' }: TabButtonProps) {
  const baseActive = variant === 'danger' ? 'bg-red-50 text-red-700 ring-1 ring-red-200' : 'bg-white text-surface-800 shadow-sm ring-1 ring-surface-200'
  return (
    <button
      onClick={onClick}
      className={`px-4 py-2 rounded-xl text-sm font-medium transition-all duration-200 ${
        active ? baseActive : 'text-surface-500 hover:text-surface-700 hover:bg-surface-100'
      }`}
    >
      {children}
    </button>
  )
}

// ── Search Input ───────────────────────────────────────────────

interface SearchInputProps {
  value: string
  onChange: (v: string) => void
  onSearch: () => void
  placeholder?: string
}

export function SearchInput({ value, onChange, onSearch, placeholder }: SearchInputProps) {
  return (
    <div className="relative flex-1">
      <svg className="absolute left-3.5 top-1/2 -translate-y-1/2 w-4 h-4 text-surface-400" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
      </svg>
      <input
        className="input pl-10"
        placeholder={placeholder ?? 'Buscar...'}
        value={value}
        onChange={e => onChange(e.target.value)}
        onKeyDown={e => e.key === 'Enter' && onSearch()}
      />
    </div>
  )
}

// ── Form Card ──────────────────────────────────────────────────

export function FormCard({ title, children, onSave, onCancel }: {
  title: string; children: ReactNode; onSave: () => void; onCancel: () => void
}) {
  return (
    <motion.div
      initial={{ opacity: 0, y: -10, height: 0 }}
      animate={{ opacity: 1, y: 0, height: 'auto' }}
      exit={{ opacity: 0, y: -10, height: 0 }}
      className="card p-6 mb-6"
    >
      <h3 className="text-base font-semibold text-surface-800 mb-4">{title}</h3>
      {children}
      <div className="flex gap-3 mt-5 pt-4 border-t border-surface-100">
        <button onClick={onSave} className="btn-primary">Guardar</button>
        <button onClick={onCancel} className="btn-secondary">Cancelar</button>
      </div>
    </motion.div>
  )
}
