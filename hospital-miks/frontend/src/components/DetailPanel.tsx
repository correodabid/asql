import { motion, AnimatePresence } from 'framer-motion'
import { type ReactNode } from 'react'

export function DetailPanel({
  title,
  subtitle,
  children,
  emptyTitle = 'Selecciona un registro',
  emptyText = 'Haz clic sobre una fila para ver su detalle.',
  isEmpty = false,
}: {
  title?: string
  subtitle?: string
  children?: ReactNode
  emptyTitle?: string
  emptyText?: string
  isEmpty?: boolean
}) {
  return (
    <AnimatePresence mode="wait">
      <motion.aside
        key={isEmpty ? 'empty' : title}
        initial={{ opacity: 0, x: 16 }}
        animate={{ opacity: 1, x: 0 }}
        exit={{ opacity: 0, x: 16 }}
        transition={{ duration: 0.2 }}
        className="card p-5 sticky top-6"
      >
        {isEmpty ? (
          <div className="py-10 text-center">
            <div className="w-12 h-12 mx-auto rounded-2xl bg-surface-100 flex items-center justify-center text-2xl mb-4">✨</div>
            <h3 className="text-sm font-semibold text-surface-800">{emptyTitle}</h3>
            <p className="text-sm text-surface-400 mt-2">{emptyText}</p>
          </div>
        ) : (
          <>
            <div className="pb-4 border-b border-surface-100 mb-4">
              <p className="text-[11px] uppercase tracking-wider text-surface-400">Detalle contextual</p>
              {title && <h3 className="text-lg font-semibold text-surface-900 mt-1">{title}</h3>}
              {subtitle && <p className="text-sm text-surface-500 mt-1">{subtitle}</p>}
            </div>
            <div className="space-y-4">{children}</div>
          </>
        )}
      </motion.aside>
    </AnimatePresence>
  )
}

export function DetailGroup({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div>
      <p className="text-[11px] uppercase tracking-wider text-surface-400 mb-2">{title}</p>
      <div className="space-y-2">{children}</div>
    </div>
  )
}

export function DetailRow({ label, value, mono = false, accent = false }: { label: string; value: ReactNode; mono?: boolean; accent?: boolean }) {
  return (
    <div className="flex items-start justify-between gap-3 rounded-xl bg-surface-50/80 px-3 py-2.5">
      <span className="text-xs text-surface-400">{label}</span>
      <div className={`text-right text-sm ${mono ? 'font-mono text-surface-600' : accent ? 'font-semibold text-surface-900' : 'text-surface-700'}`}>
        {value}
      </div>
    </div>
  )
}