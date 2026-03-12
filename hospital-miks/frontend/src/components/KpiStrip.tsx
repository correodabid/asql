import { motion } from 'framer-motion'

interface KpiItem {
  label: string
  value: string | number
  hint?: string
  tone?: 'default' | 'success' | 'warning' | 'danger' | 'info'
}

const toneStyles: Record<NonNullable<KpiItem['tone']>, string> = {
  default: 'from-white to-surface-50 border-surface-200 text-surface-900',
  success: 'from-emerald-50 to-white border-emerald-200 text-emerald-900',
  warning: 'from-amber-50 to-white border-amber-200 text-amber-900',
  danger: 'from-red-50 to-white border-red-200 text-red-900',
  info: 'from-blue-50 to-white border-blue-200 text-blue-900',
}

export default function KpiStrip({ items }: { items: KpiItem[] }) {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4 mb-6">
      {items.map((item, index) => (
        <motion.div
          key={`${item.label}-${index}`}
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.2, delay: index * 0.04 }}
          className={`rounded-2xl border bg-gradient-to-br p-4 shadow-[0_12px_40px_rgba(15,23,42,0.05)] ${toneStyles[item.tone ?? 'default']}`}
        >
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">{item.label}</p>
          <div className="mt-2 flex items-end justify-between gap-3">
            <span className="text-2xl font-semibold tabular-nums">{item.value}</span>
            {item.hint && <span className="text-xs text-surface-500 text-right">{item.hint}</span>}
          </div>
        </motion.div>
      ))}
    </div>
  )
}
