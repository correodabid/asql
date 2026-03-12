import { useEffect, useMemo, useState } from 'react'
import { api, type PharmacyDispense, type Prescription } from '../services/api'
import DataTable from '../components/DataTable'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'
import KpiStrip from '../components/KpiStrip'
import PageHeader, { TabButton } from '../components/PageHeader'

interface Medication {
  id: string; name: string; generic_name: string; code: string; category: string;
  stock_quantity: number; min_stock: number; price: number; controlled: boolean; active: boolean;
}

export default function PharmacyPage() {
  const [medications, setMedications] = useState<Medication[]>([])
  const [prescriptions, setPrescriptions] = useState<Prescription[]>([])
  const [dispenses, setDispenses] = useState<PharmacyDispense[]>([])
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)
  const [tab, setTab] = useState<'medications' | 'low' | 'prescriptions' | 'dispenses'>('medications')
  const [selectedMedication, setSelectedMedication] = useState<Medication | null>(null)
  const [selectedPrescription, setSelectedPrescription] = useState<Prescription | null>(null)
  const [selectedDispense, setSelectedDispense] = useState<PharmacyDispense | null>(null)
  const [medicationSearch, setMedicationSearch] = useState('')
  const [medicationFilter, setMedicationFilter] = useState<'ALL' | 'LOW' | 'CONTROLLED'>('ALL')
  const [prescriptionStatusFilter, setPrescriptionStatusFilter] = useState<'ALL' | string>('ALL')
  const [dispenseSearch, setDispenseSearch] = useState('')

  useEffect(() => { loadMeds() }, [tab])

  async function loadMeds() {
    setLoading(true)
    try {
      if (tab === 'prescriptions') {
        const res = await api.get<Prescription[]>('/pharmacy/prescriptions')
        const nextPrescriptions = res.data ?? []
        setPrescriptions(nextPrescriptions)
        setSelectedPrescription((current) => nextPrescriptions.find((rx) => rx.id === current?.id) ?? current ?? nextPrescriptions[0] ?? null)
      } else if (tab === 'dispenses') {
        setDispenses([])
        setSelectedDispense(null)
      } else {
        const endpoint = tab === 'low' ? '/pharmacy/medications/low-stock' : '/pharmacy/medications'
        const res = await api.get<Medication[]>(endpoint)
        const nextMedications = res.data ?? []
        setMedications(nextMedications)
        setSelectedMedication((current) => nextMedications.find((med) => med.id === current?.id) ?? current ?? nextMedications[0] ?? null)
      }
    } finally { setLoading(false) }
  }

  async function loadDispenses(rx: Prescription) {
    setSelectedPrescription(rx)
    setSelectedDispense(null)
    setTab('dispenses')
    setDetailLoading(true)
    try {
      const res = await api.get<PharmacyDispense[]>(`/pharmacy/prescriptions/${rx.id}/dispenses`)
      const nextDispenses = res.data ?? []
      setDispenses(nextDispenses)
      setSelectedDispense(nextDispenses[0] ?? null)
    } finally {
      setDetailLoading(false)
    }
  }

  const selectedMedicationLow = selectedMedication ? selectedMedication.stock_quantity <= selectedMedication.min_stock : false

  const lowStockCount = useMemo(() => medications.filter((med) => med.stock_quantity <= med.min_stock).length, [medications])
  const controlledCount = useMemo(() => medications.filter((med) => med.controlled).length, [medications])
  const prescriptionStatuses = useMemo(() => Array.from(new Set(prescriptions.map((rx) => rx.status))), [prescriptions])

  const filteredMedications = useMemo(() => {
    const query = medicationSearch.trim().toLowerCase()
    return medications.filter((med) => {
      const matchesQuery = !query || [med.name, med.generic_name, med.code, med.category].some((value) => value?.toLowerCase().includes(query))
      const matchesFilter = medicationFilter === 'ALL'
        || (medicationFilter === 'LOW' && med.stock_quantity <= med.min_stock)
        || (medicationFilter === 'CONTROLLED' && med.controlled)
      return matchesQuery && matchesFilter
    })
  }, [medicationFilter, medicationSearch, medications])

  const filteredPrescriptions = useMemo(() => {
    return prescriptions.filter((rx) => prescriptionStatusFilter === 'ALL' || rx.status === prescriptionStatusFilter)
  }, [prescriptionStatusFilter, prescriptions])

  const filteredDispenses = useMemo(() => {
    const query = dispenseSearch.trim().toLowerCase()
    return dispenses.filter((dispense) => {
      if (!query) return true
      return [dispense.pharmacist_id, dispense.notes].some((value) => value?.toLowerCase().includes(query))
    })
  }, [dispenseSearch, dispenses])

  const columns = [
    { key: 'code' as const, label: 'Código', render: (v: string) =>
      <span className="font-mono text-xs bg-surface-100 px-2 py-1 rounded-lg text-surface-600">{v}</span>
    },
    { key: 'name' as const, label: 'Nombre', render: (v: string, row: Medication) =>
      <div>
        <span className="font-medium text-surface-800">{v}</span>
        {row.generic_name && <p className="text-[11px] text-surface-400 mt-0.5">{row.generic_name}</p>}
      </div>
    },
    { key: 'category' as const, label: 'Categoría', render: (v: string) =>
      <span className="badge bg-surface-100 text-surface-600">{v}</span>
    },
    { key: 'stock_quantity' as const, label: 'Stock', render: (v: number, row: Medication) => {
      const low = v <= row.min_stock
      return (
        <div className="flex items-center gap-2">
          <div className="w-16 bg-surface-100 rounded-full h-1.5">
            <div className={`h-1.5 rounded-full ${low ? 'bg-red-500' : 'bg-emerald-500'}`}
                 style={{ width: `${Math.min(100, (v / Math.max(row.min_stock * 3, 1)) * 100)}%` }} />
          </div>
          <span className={`text-xs font-medium ${low ? 'text-red-600' : 'text-surface-600'}`}>{v}</span>
        </div>
      )
    }},
    { key: 'min_stock' as const, label: 'Mín.' },
    { key: 'price' as const, label: 'Precio', render: (v: number) =>
      <span className="font-medium text-surface-700">{v.toFixed(2)} €</span>
    },
    { key: 'controlled' as const, label: 'Control', render: (v: boolean) =>
      v ? <span className="badge bg-amber-50 text-amber-600 ring-1 ring-inset ring-amber-200">Controlado</span>
        : <span className="text-surface-300">—</span>
    },
  ]

  const prescriptionColumns = [
    { key: 'prescribed_at' as const, label: 'Fecha', render: (v: string) => <span className="text-surface-600">{new Date(v).toLocaleDateString('es-ES')}</span> },
    { key: 'status' as const, label: 'Estado', render: (v: string) => <span className="badge bg-blue-50 text-blue-600">{v}</span> },
    { key: 'dosage' as const, label: 'Dosis', render: (v: string, row: Prescription) =>
      <div>
        <span className="font-medium text-surface-800">{v}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{row.frequency}</p>
      </div>
    },
    { key: 'quantity' as const, label: 'Cantidad', render: (v: number) => <span className="tabular-nums text-surface-700">{v}</span> },
    { key: 'refills_used' as const, label: 'Reposiciones', render: (v: number, row: Prescription) => <span className="text-surface-500 text-xs">{v}/{row.refills_allowed}</span> },
    { key: 'id' as const, label: 'Dispensaciones', render: (_v: string, row: Prescription) =>
      <button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadDispenses(row)}>Ver</button>
    },
  ]

  const dispenseColumns = [
    { key: 'dispensed_at' as const, label: 'Fecha', render: (v: string) => <span className="text-surface-600">{new Date(v).toLocaleString('es-ES')}</span> },
    { key: 'quantity' as const, label: 'Cantidad', render: (v: number) => <span className="font-semibold tabular-nums text-surface-800">{v}</span> },
    { key: 'pharmacist_id' as const, label: 'Farmacéutico', render: (v: string) => <span className="font-mono text-xs text-surface-500">{v.slice(0, 8)}…</span> },
    { key: 'notes' as const, label: 'Notas', render: (v: string) => v ? <span className="text-surface-600 text-xs">{v}</span> : <span className="text-surface-300">—</span> },
  ]

  return (
    <div>
      <PageHeader title="Gestión de Farmacia" subtitle="Inventario, prescripciones y trazabilidad de dispensación">
        <div className="flex gap-1.5">
          <TabButton active={tab === 'medications'} onClick={() => setTab('medications')}>Medicamentos</TabButton>
          <TabButton active={tab === 'low'} onClick={() => setTab('low')} variant="danger">Stock Bajo</TabButton>
          <TabButton active={tab === 'prescriptions'} onClick={() => setTab('prescriptions')}>Prescripciones</TabButton>
          <TabButton active={tab === 'dispenses'} onClick={() => setTab('dispenses')}>Dispensaciones</TabButton>
        </div>
      </PageHeader>

      <KpiStrip
        items={[
          { label: 'Medicamentos', value: medications.length, hint: 'catálogo cargado', tone: 'info' },
          { label: 'Stock bajo', value: lowStockCount, hint: 'requieren acción', tone: lowStockCount > 0 ? 'danger' : 'success' },
          { label: 'Controlados', value: controlledCount, hint: 'trazabilidad reforzada', tone: 'warning' },
          { label: 'Prescripciones', value: prescriptions.length, hint: 'activas cargadas', tone: 'default' },
        ]}
      />

      {(tab === 'medications' || tab === 'low') && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <input
            className="input max-w-sm"
            placeholder="Buscar por nombre, código, categoría o genérico"
            value={medicationSearch}
            onChange={(e) => setMedicationSearch(e.target.value)}
          />
          <select className="input max-w-xs" value={medicationFilter} onChange={(e) => setMedicationFilter(e.target.value as 'ALL' | 'LOW' | 'CONTROLLED')}>
            <option value="ALL">Todo el inventario</option>
            <option value="LOW">Solo críticos</option>
            <option value="CONTROLLED">Solo controlados</option>
          </select>
          <span className="text-xs text-surface-400">{filteredMedications.length} visibles</span>
        </div>
      )}

      {tab === 'prescriptions' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={prescriptionStatusFilter} onChange={(e) => setPrescriptionStatusFilter(e.target.value)}>
            <option value="ALL">Todos los estados</option>
            {prescriptionStatuses.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredPrescriptions.length} prescripciones visibles</span>
        </div>
      )}

      {tab === 'dispenses' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <input
            className="input max-w-sm"
            placeholder="Buscar por farmacéutico o notas"
            value={dispenseSearch}
            onChange={(e) => setDispenseSearch(e.target.value)}
          />
          <span className="text-xs text-surface-400">{filteredDispenses.length} dispensaciones visibles</span>
        </div>
      )}

      {tab === 'dispenses' && selectedPrescription && (
        <div className="card p-4 mb-6 flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs uppercase tracking-wider text-surface-400">Prescripción seleccionada</p>
            <h3 className="text-sm font-semibold text-surface-800 mt-1">{selectedPrescription.dosage}</h3>
          </div>
          <div className="flex items-center gap-3">
            <span className="badge bg-blue-50 text-blue-600">{selectedPrescription.status}</span>
            <span className="text-xs text-surface-400">{selectedPrescription.frequency}</span>
          </div>
        </div>
      )}

      {(tab === 'medications' || tab === 'low') && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={columns}
            data={filteredMedications}
            loading={loading}
            emptyMessage="No hay medicamentos registrados."
            emptyIcon="💊"
            onRowClick={setSelectedMedication}
            selectedRowId={selectedMedication?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedMedication}
            emptyTitle="Selecciona un medicamento"
            emptyText="Consulta stock, criticidad y trazabilidad comercial en tiempo real."
            title={selectedMedication?.name}
            subtitle={selectedMedication?.generic_name || selectedMedication?.category}
          >
            {selectedMedication && (
              <>
                <DetailGroup title="Inventario">
                  <DetailRow label="Código" value={selectedMedication.code} mono />
                  <DetailRow label="Stock actual" value={selectedMedication.stock_quantity} accent />
                  <DetailRow label="Stock mínimo" value={selectedMedication.min_stock} />
                </DetailGroup>
                <DetailGroup title="Riesgo operativo">
                  <DetailRow label="Estado" value={selectedMedicationLow ? 'Reposición necesaria' : 'Cobertura correcta'} accent={selectedMedicationLow} />
                  <DetailRow label="Controlado" value={selectedMedication.controlled ? 'Sí' : 'No'} />
                  <DetailRow label="Activo" value={selectedMedication.active ? 'Sí' : 'No'} />
                </DetailGroup>
                <DetailGroup title="Comercial">
                  <DetailRow label="Categoría" value={selectedMedication.category} />
                  <DetailRow label="Precio" value={`${selectedMedication.price.toFixed(2)} €`} accent />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
      {tab === 'prescriptions' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={prescriptionColumns}
            data={filteredPrescriptions}
            loading={loading}
            emptyMessage="No hay prescripciones activas."
            emptyIcon="📋"
            onRowClick={setSelectedPrescription}
            selectedRowId={selectedPrescription?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedPrescription}
            emptyTitle="Selecciona una prescripción"
            emptyText="Desde aquí puedes revisar dosis, frecuencia y ciclo de reposiciones."
            title={selectedPrescription?.dosage}
            subtitle={selectedPrescription?.status ? `${selectedPrescription.status} · ${selectedPrescription.frequency}` : undefined}
          >
            {selectedPrescription && (
              <>
                <DetailGroup title="Plan terapéutico">
                  <DetailRow label="Estado" value={selectedPrescription.status} accent />
                  <DetailRow label="Frecuencia" value={selectedPrescription.frequency} />
                  <DetailRow label="Cantidad" value={selectedPrescription.quantity} />
                </DetailGroup>
                <DetailGroup title="Ciclo de reposición">
                  <DetailRow label="Reposiciones usadas" value={`${selectedPrescription.refills_used}/${selectedPrescription.refills_allowed}`} />
                  <DetailRow label="Prescrita" value={new Date(selectedPrescription.prescribed_at).toLocaleString('es-ES')} />
                  <DetailRow label="Dispensaciones" value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadDispenses(selectedPrescription)}>Ver histórico</button>} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
      {tab === 'dispenses' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={dispenseColumns}
            data={filteredDispenses}
            loading={detailLoading}
            emptyMessage="Selecciona una prescripción para ver las dispensaciones."
            emptyIcon="🧪"
            onRowClick={setSelectedDispense}
            selectedRowId={selectedDispense?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedPrescription && !selectedDispense}
            emptyTitle="Sin dispensación seleccionada"
            emptyText="Selecciona una prescripción para analizar el detalle de entrega y trazabilidad."
            title={selectedPrescription?.dosage ?? 'Trazabilidad de dispensación'}
            subtitle={selectedDispense ? `${selectedDispense.quantity} unidades · ${new Date(selectedDispense.dispensed_at).toLocaleString('es-ES')}` : selectedPrescription?.frequency}
          >
            {selectedPrescription && (
              <DetailGroup title="Prescripción origen">
                <DetailRow label="Estado" value={selectedPrescription.status} accent />
                <DetailRow label="Frecuencia" value={selectedPrescription.frequency} />
                <DetailRow label="Cantidad total" value={selectedPrescription.quantity} />
              </DetailGroup>
            )}
            {selectedDispense && (
              <DetailGroup title="Entrega seleccionada">
                <DetailRow label="Cantidad" value={selectedDispense.quantity} accent />
                <DetailRow label="Farmacéutico" value={selectedDispense.pharmacist_id} mono />
                <DetailRow label="Momento" value={new Date(selectedDispense.dispensed_at).toLocaleString('es-ES')} />
                <DetailRow label="Notas" value={selectedDispense.notes || 'Sin observaciones'} />
              </DetailGroup>
            )}
          </DetailPanel>
        </div>
      )}
    </div>
  )
}
