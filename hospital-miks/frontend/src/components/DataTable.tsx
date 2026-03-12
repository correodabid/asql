import { motion } from 'framer-motion'

interface Column<T> {
  key: keyof T
  label: string
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  render?: (value: any, row: T) => React.ReactNode
  align?: 'left' | 'center' | 'right'
  width?: string
}

interface DataTableProps<T> {
  columns: Column<T>[]
  data: T[]
  loading: boolean
  emptyMessage?: string
  emptyIcon?: string
  onRowClick?: (row: T) => void
  selectedRowId?: string | null
}

export default function DataTable<T extends { id: string }>({
  columns, data, loading, emptyMessage, emptyIcon = '📋', onRowClick, selectedRowId,
}: DataTableProps<T>) {
  if (loading) {
    return (
      <div className="card p-0 overflow-hidden">
        {/* Shimmer header */}
        <div className="bg-surface-50 border-b border-surface-100 px-5 py-3.5 flex gap-6">
          {columns.map((col) => (
            <div key={String(col.key)} className="flex-1">
              <div className="h-3 bg-surface-200 rounded-full w-16 animate-shimmer bg-[length:200%_100%] bg-gradient-to-r from-surface-200 via-surface-100 to-surface-200" />
            </div>
          ))}
        </div>
        {/* Shimmer rows */}
        {[...Array(5)].map((_, i) => (
          <div key={i} className="px-5 py-4 flex gap-6 border-b border-surface-50" style={{ animationDelay: `${i * 100}ms` }}>
            {columns.map((col) => (
              <div key={String(col.key)} className="flex-1">
                <div
                  className="h-3 bg-surface-100 rounded-full animate-shimmer bg-[length:200%_100%] bg-gradient-to-r from-surface-100 via-surface-50 to-surface-100"
                  style={{ width: `${40 + Math.random() * 40}%`, animationDelay: `${i * 150}ms` }}
                />
              </div>
            ))}
          </div>
        ))}
      </div>
    )
  }

  if (data.length === 0) {
    return (
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="card p-12 text-center"
      >
        <div className="text-4xl mb-3 opacity-40">{emptyIcon}</div>
        <p className="text-surface-400 text-sm">{emptyMessage ?? 'No hay datos disponibles.'}</p>
      </motion.div>
    )
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
      className="card overflow-hidden"
    >
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="bg-surface-50/80 border-b border-surface-100">
              {columns.map((col) => (
                <th
                  key={String(col.key)}
                  className={`px-5 py-3.5 text-[11px] font-semibold text-surface-400 uppercase tracking-wider
                    ${col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left'}
                  `}
                  style={col.width ? { width: col.width } : undefined}
                >
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.map((row, i) => (
              <motion.tr
                key={row.id}
                initial={{ opacity: 0, x: -8 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ duration: 0.2, delay: i * 0.03 }}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
                className={`border-b border-surface-50 last:border-0 transition-colors duration-150 group ${
                  selectedRowId === row.id
                    ? 'bg-primary-50/60 ring-1 ring-inset ring-primary-100'
                    : 'hover:bg-primary-50/30'
                } ${onRowClick ? 'cursor-pointer' : ''}`}
              >
                {columns.map((col) => (
                  <td
                    key={String(col.key)}
                    className={`px-5 py-3.5 text-sm text-surface-700
                      ${col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left'}
                    `}
                  >
                    {col.render ? col.render(row[col.key], row) : String(row[col.key] ?? '')}
                  </td>
                ))}
              </motion.tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Footer with count */}
      <div className="bg-surface-50/50 border-t border-surface-100 px-5 py-2.5 flex justify-between items-center">
        <span className="text-[11px] text-surface-400">
          {data.length} registro{data.length !== 1 ? 's' : ''}
        </span>
        <div className="flex items-center gap-1">
          <div className="w-1.5 h-1.5 rounded-full bg-primary-400" />
          <span className="text-[10px] text-surface-400">ASQL</span>
        </div>
      </div>
    </motion.div>
  )
}
