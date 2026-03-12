import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { api, type InvoiceItem } from '../services/api'
import DataTable from '../components/DataTable'
import PageHeader, { StatusBadge, TabButton } from '../components/PageHeader'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'

interface Invoice {
  id: string; invoice_number: string; patient_id: string; status: string;
  subtotal: number; tax: number; discount: number; total: number;
  currency: string; issued_at: string; due_date: string; paid_at: string;
  payment_method: string;
}

const paymentLabels: Record<string, string> = {
  CARD: 'Tarjeta', CASH: 'Efectivo', TRANSFER: 'Transferencia', INSURANCE: 'Seguro',
}

export default function BillingPage() {
  const location = useLocation()
  const [invoices, setInvoices] = useState<Invoice[]>([])
  const [items, setItems] = useState<InvoiceItem[]>([])
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)
  const [activeTab, setActiveTab] = useState<'invoices' | 'items'>('invoices')
  const [selectedInvoice, setSelectedInvoice] = useState<Invoice | null>(null)
  const navigate = useNavigate()
  const focusState = location.state as { focusType?: string; focusId?: string; openTab?: 'invoices' | 'items' } | null
  const [patientContextId, setPatientContextId] = useState<string | null>(null)

  useEffect(() => { loadInvoices() }, [])
  useEffect(() => {
    if (focusState?.openTab) {
      setActiveTab(focusState.openTab)
    }
  }, [focusState])

  useEffect(() => {
    if (!focusState?.focusId || invoices.length === 0) return
    if (focusState.focusType === 'invoice') {
      const focusedInvoice = invoices.find((invoice) => invoice.id === focusState.focusId)
      if (focusedInvoice) {
        setActiveTab('invoices')
        setSelectedInvoice(focusedInvoice)
      }
    }
    if (focusState.focusType === 'patient') {
      setPatientContextId(focusState.focusId)
      const focusedInvoice = invoices.find((invoice) => invoice.patient_id === focusState.focusId)
      if (focusedInvoice) {
        setActiveTab('invoices')
        setSelectedInvoice(focusedInvoice)
      }
    }
  }, [focusState, invoices])

  const filteredInvoices = useMemo(() => {
    if (!patientContextId) return invoices
    return invoices.filter((invoice) => invoice.patient_id === patientContextId)
  }, [invoices, patientContextId])

  async function loadInvoices() {
    setLoading(true)
    try {
      const res = await api.get<Invoice[]>('/billing/invoices')
      const items = res.data ?? []
      setInvoices(items)
      setSelectedInvoice((current) => current && items.find((item) => item.id === current.id) ? current : items[0] ?? null)
    } finally { setLoading(false) }
  }

  async function loadItems(invoice: Invoice) {
    setSelectedInvoice(invoice)
    setDetailLoading(true)
    setActiveTab('items')
    try {
      const res = await api.get<InvoiceItem[]>(`/billing/invoices/${invoice.id}/items`)
      setItems(res.data ?? [])
    } finally {
      setDetailLoading(false)
    }
  }

  const columns = [
    { key: 'invoice_number' as const, label: 'Nº Factura', render: (v: string) =>
      <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-600">{v}</span>
    },
    { key: 'status' as const, label: 'Estado', render: (v: string) => <StatusBadge status={v} /> },
    { key: 'subtotal' as const, label: 'Subtotal', render: (v: number) =>
      <span className="text-surface-600 tabular-nums">{v.toFixed(2)} €</span>
    },
    { key: 'tax' as const, label: 'IVA', render: (v: number) =>
      <span className="text-surface-500 tabular-nums text-xs">{v.toFixed(2)} €</span>
    },
    { key: 'total' as const, label: 'Total', render: (v: number) =>
      <span className="font-semibold text-surface-800 tabular-nums">{v.toFixed(2)} €</span>
    },
    { key: 'issued_at' as const, label: 'Emisión', render: (v: string) =>
      v ? <span className="text-surface-600">{new Date(v).toLocaleDateString('es-ES')}</span>
        : <span className="text-surface-300">—</span>
    },
    { key: 'due_date' as const, label: 'Vencimiento', render: (v: string) =>
      v ? <span className="text-surface-600">{new Date(v).toLocaleDateString('es-ES')}</span>
        : <span className="text-surface-300">—</span>
    },
    { key: 'payment_method' as const, label: 'Pago', render: (v: string) =>
      v ? <span className="badge bg-surface-100 text-surface-600">{paymentLabels[v] ?? v}</span>
        : <span className="text-surface-300">—</span>
    },
    { key: 'id' as const, label: 'Detalle', render: (_v: string, row: Invoice) =>
      <button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadItems(row)}>Ver líneas</button>
    },
  ]

  const itemColumns = [
    { key: 'description' as const, label: 'Concepto', render: (v: string, row: InvoiceItem) =>
      <div>
        <span className="font-medium text-surface-800">{v}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{row.category}</p>
      </div>
    },
    { key: 'quantity' as const, label: 'Cantidad', render: (v: number) => <span className="tabular-nums text-surface-600">{v}</span> },
    { key: 'unit_price' as const, label: 'Precio unit.', render: (v: number) => <span className="tabular-nums text-surface-600">{v.toFixed(2)} €</span> },
    { key: 'total' as const, label: 'Total', render: (v: number) => <span className="font-semibold tabular-nums text-surface-800">{v.toFixed(2)} €</span> },
    { key: 'created_at' as const, label: 'Fecha', render: (v: string) => <span className="text-surface-500 text-xs">{new Date(v).toLocaleString('es-ES')}</span> },
  ]

  return (
    <div>
      <PageHeader title="Facturación" subtitle="Gestión de facturas, cobros y líneas de detalle">
        <div className="flex gap-1.5">
          <TabButton active={activeTab === 'invoices'} onClick={() => setActiveTab('invoices')}>Facturas</TabButton>
          <TabButton active={activeTab === 'items'} onClick={() => setActiveTab('items')}>Líneas</TabButton>
        </div>
      </PageHeader>

      {activeTab === 'invoices' && patientContextId && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <span className="badge bg-primary-50 text-primary-700 ring-1 ring-inset ring-primary-200">Contexto: Paciente</span>
          <span className="font-mono text-xs text-surface-500">{patientContextId.slice(0, 8)}…</span>
          <span className="text-xs text-surface-400">{filteredInvoices.length} facturas visibles para este paciente</span>
          <button className="btn-secondary !px-3 !py-1.5" onClick={() => setPatientContextId(null)}>Limpiar contexto</button>
        </div>
      )}

      <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1fr)_360px] gap-6 items-start">
        {activeTab === 'invoices' ? (
          <DataTable
            columns={columns}
            data={filteredInvoices}
            loading={loading}
            emptyMessage="No hay facturas registradas."
            emptyIcon="💰"
            onRowClick={setSelectedInvoice}
            selectedRowId={selectedInvoice?.id ?? null}
          />
        ) : (
          <DataTable columns={itemColumns} data={items} loading={detailLoading} emptyMessage="Selecciona una factura para ver sus líneas." emptyIcon="🧾" />
        )}

        <DetailPanel
          isEmpty={!selectedInvoice}
          title={selectedInvoice?.invoice_number}
          subtitle={selectedInvoice?.due_date ? `Vence ${new Date(selectedInvoice.due_date).toLocaleDateString('es-ES')}` : 'Sin vencimiento'}
          emptyText="Selecciona una factura para ver su resumen financiero."
        >
          {selectedInvoice && (
            <>
              <DetailGroup title="Estado financiero">
                <DetailRow label="Estado" value={<StatusBadge status={selectedInvoice.status} />} />
                <DetailRow label="Total" value={`${selectedInvoice.total.toFixed(2)} €`} accent />
                <DetailRow label="Subtotal" value={`${selectedInvoice.subtotal.toFixed(2)} €`} />
                <DetailRow label="IVA" value={`${selectedInvoice.tax.toFixed(2)} €`} />
                <DetailRow label="Descuento" value={`${selectedInvoice.discount.toFixed(2)} €`} />
              </DetailGroup>
              <DetailGroup title="Trazabilidad">
                <DetailRow label="Paciente" value={selectedInvoice.patient_id} mono />
                <DetailRow label="Pago" value={(paymentLabels[selectedInvoice.payment_method] ?? selectedInvoice.payment_method) || 'Pendiente'} />
                <DetailRow label="Emitida" value={selectedInvoice.issued_at ? new Date(selectedInvoice.issued_at).toLocaleDateString('es-ES') : 'No emitida'} />
                <DetailRow label="Pagada" value={selectedInvoice.paid_at ? new Date(selectedInvoice.paid_at).toLocaleDateString('es-ES') : 'Pendiente'} />
              </DetailGroup>
              <DetailGroup title="Acciones relacionadas">
                <DetailRow
                  label="Paciente"
                  value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => navigate('/patients', { state: { focusType: 'patient', focusId: selectedInvoice.patient_id } })}>Abrir ficha</button>}
                />
                <DetailRow
                  label="Líneas"
                  value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadItems(selectedInvoice)}>Ver detalle</button>}
                />
              </DetailGroup>
            </>
          )}
        </DetailPanel>
      </div>
    </div>
  )
}
