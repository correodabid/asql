import { useEffect, useMemo, useState } from 'react'
import { useLocation } from 'react-router-dom'
import { api, type DocumentAccess } from '../services/api'
import DataTable from '../components/DataTable'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'
import KpiStrip from '../components/KpiStrip'
import PageHeader, { TabButton } from '../components/PageHeader'

interface Document {
  id: string; title: string; category: string; file_name: string;
  mime_type: string; size_bytes: number; version: number; tags: string;
  created_at: string;
}

const categoryConfig: Record<string, { label: string; bg: string }> = {
  MEDICAL_REPORT: { label: 'Informe Médico', bg: 'bg-blue-50 text-blue-600' },
  LAB_RESULT: { label: 'Resultado Lab.', bg: 'bg-emerald-50 text-emerald-600' },
  IMAGING: { label: 'Imagen', bg: 'bg-purple-50 text-purple-600' },
  CONSENT: { label: 'Consentimiento', bg: 'bg-amber-50 text-amber-600' },
  DISCHARGE: { label: 'Alta', bg: 'bg-surface-100 text-surface-600' },
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]
}

export default function DocumentsPage() {
  const location = useLocation()
  const focusState = location.state as { focusType?: string; focusId?: string; openTab?: 'documents' | 'access' } | null
  const [docs, setDocs] = useState<Document[]>([])
  const [accessLog, setAccessLog] = useState<DocumentAccess[]>([])
  const [loading, setLoading] = useState(true)
  const [detailLoading, setDetailLoading] = useState(false)
  const [activeTab, setActiveTab] = useState<'documents' | 'access'>('documents')
  const [selectedDoc, setSelectedDoc] = useState<Document | null>(null)
  const [selectedAccess, setSelectedAccess] = useState<DocumentAccess | null>(null)
  const [docSearch, setDocSearch] = useState('')
  const [docCategoryFilter, setDocCategoryFilter] = useState<'ALL' | string>('ALL')
  const [accessActionFilter, setAccessActionFilter] = useState<'ALL' | string>('ALL')

  useEffect(() => { loadDocs() }, [])
  useEffect(() => {
    if (focusState?.openTab) {
      setActiveTab(focusState.openTab)
    }
  }, [focusState])

  useEffect(() => {
    if (focusState?.focusType !== 'document' || !focusState.focusId || docs.length === 0) return
    const focusedDocument = docs.find((doc) => doc.id === focusState.focusId)
    if (focusedDocument) {
      setActiveTab('documents')
      setSelectedDoc(focusedDocument)
    }
  }, [docs, focusState])

  async function loadDocs() {
    setLoading(true)
    try {
      const res = await api.get<Document[]>('/documents')
      const nextDocs = res.data ?? []
      setDocs(nextDocs)
      setSelectedDoc((current) => nextDocs.find((doc) => doc.id === current?.id) ?? current ?? nextDocs[0] ?? null)
    } finally { setLoading(false) }
  }

  async function loadAccessLog(doc: Document) {
    setSelectedDoc(doc)
    setSelectedAccess(null)
    setDetailLoading(true)
    setActiveTab('access')
    try {
      const res = await api.get<DocumentAccess[]>(`/documents/${doc.id}/access-log`)
      const nextAccess = res.data ?? []
      setAccessLog(nextAccess)
      setSelectedAccess(nextAccess[0] ?? null)
    } finally {
      setDetailLoading(false)
    }
  }

  const columns = [
    { key: 'title' as const, label: 'Título', render: (v: string, row: Document) =>
      <div>
        <span className="font-medium text-surface-800">{v}</span>
        <p className="text-[11px] text-surface-400 mt-0.5">{row.file_name}</p>
      </div>
    },
    { key: 'category' as const, label: 'Categoría', render: (v: string) => {
      const cfg = categoryConfig[v]
      return <span className={`badge ${cfg?.bg ?? 'bg-surface-100 text-surface-600'}`}>{cfg?.label ?? v}</span>
    }},
    { key: 'size_bytes' as const, label: 'Tamaño', render: (v: number) =>
      <span className="text-surface-500 text-xs font-mono">{formatBytes(v)}</span>
    },
    { key: 'version' as const, label: 'Versión', render: (v: number) =>
      <span className="badge bg-surface-100 text-surface-500">v{v}</span>
    },
    { key: 'tags' as const, label: 'Etiquetas', render: (v: string) =>
      v ? <span className="text-surface-500 text-xs">{v}</span> : <span className="text-surface-300">—</span>
    },
    { key: 'created_at' as const, label: 'Fecha', render: (v: string) =>
      <span className="text-surface-600 text-sm">{new Date(v).toLocaleDateString('es-ES')}</span>
    },
    { key: 'id' as const, label: 'Actividad', render: (_v: string, row: Document) =>
      <button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadAccessLog(row)}>Ver accesos</button>
    },
  ]

  const accessColumns = [
    { key: 'action' as const, label: 'Acción', render: (v: string) =>
      <span className="badge bg-blue-50 text-blue-600">{v}</span>
    },
    { key: 'staff_id' as const, label: 'Staff', render: (v: string) =>
      <span className="font-mono text-xs text-surface-500">{v.slice(0, 8)}…</span>
    },
    { key: 'ip_address' as const, label: 'IP', render: (v: string) =>
      v ? <span className="font-mono text-xs text-surface-600">{v}</span> : <span className="text-surface-300">—</span>
    },
    { key: 'accessed_at' as const, label: 'Momento', render: (v: string) =>
      <span className="text-surface-600 text-sm">{new Date(v).toLocaleString('es-ES')}</span>
    },
  ]

  const totalStorageBytes = useMemo(() => docs.reduce((sum, doc) => sum + doc.size_bytes, 0), [docs])
  const categoryCount = useMemo(() => new Set(docs.map((doc) => doc.category)).size, [docs])
  const docCategories = useMemo(() => Array.from(new Set(docs.map((doc) => doc.category))), [docs])
  const accessActions = useMemo(() => Array.from(new Set(accessLog.map((event) => event.action))), [accessLog])

  const filteredDocs = useMemo(() => {
    const query = docSearch.trim().toLowerCase()
    return docs.filter((doc) => {
      const matchesQuery = !query || [doc.title, doc.file_name, doc.tags, doc.mime_type].some((value) => value?.toLowerCase().includes(query))
      const matchesCategory = docCategoryFilter === 'ALL' || doc.category === docCategoryFilter
      return matchesQuery && matchesCategory
    })
  }, [docCategoryFilter, docSearch, docs])

  const filteredAccessLog = useMemo(() => {
    return accessLog.filter((event) => accessActionFilter === 'ALL' || event.action === accessActionFilter)
  }, [accessActionFilter, accessLog])

  return (
    <div>
      <PageHeader title="Gestión Documental" subtitle="Documentos clínicos, administrativos y auditoría de accesos">
        <div className="flex gap-1.5">
          <TabButton active={activeTab === 'documents'} onClick={() => setActiveTab('documents')}>Documentos</TabButton>
          <TabButton active={activeTab === 'access'} onClick={() => setActiveTab('access')}>Accesos</TabButton>
        </div>
      </PageHeader>

      <KpiStrip
        items={[
          { label: 'Documentos', value: docs.length, hint: 'repositorio activo', tone: 'info' },
          { label: 'Almacenamiento', value: formatBytes(totalStorageBytes), hint: 'ocupación visible', tone: 'default' },
          { label: 'Categorías', value: categoryCount, hint: 'tipologías en uso', tone: 'success' },
          { label: 'Accesos cargados', value: accessLog.length, hint: 'auditoría actual', tone: 'warning' },
        ]}
      />

      <div className="card p-4 mb-6 grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Documento foco</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{selectedDoc?.title ?? 'Sin selección'}</p>
          <p className="text-sm text-surface-500 mt-1">{selectedDoc ? `${categoryConfig[selectedDoc.category]?.label ?? selectedDoc.category} · ${formatBytes(selectedDoc.size_bytes)}` : 'Selecciona un documento para priorizar contexto'}</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Atención</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{filteredDocs.filter((doc) => doc.category === 'CONSENT').length} consentimientos · {filteredDocs.filter((doc) => doc.category === 'DISCHARGE').length} altas</p>
          <p className="text-sm text-surface-500 mt-1">mix documental visible con el filtro actual</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Último evento</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{selectedAccess ? new Date(selectedAccess.accessed_at).toLocaleString('es-ES') : 'Sin evento seleccionado'}</p>
          <p className="text-sm text-surface-500 mt-1">{selectedAccess?.action ?? 'Carga un historial para revisar auditoría'}</p>
        </div>
      </div>

      {activeTab === 'documents' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <input
            className="input max-w-sm"
            placeholder="Buscar por título, archivo, tag o mime type"
            value={docSearch}
            onChange={(e) => setDocSearch(e.target.value)}
          />
          <select className="input max-w-xs" value={docCategoryFilter} onChange={(e) => setDocCategoryFilter(e.target.value)}>
            <option value="ALL">Todas las categorías</option>
            {docCategories.map((category) => (
              <option key={category} value={category}>{categoryConfig[category]?.label ?? category}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredDocs.length} visibles · {docs.length} totales</span>
        </div>
      )}

      {activeTab === 'access' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={accessActionFilter} onChange={(e) => setAccessActionFilter(e.target.value)}>
            <option value="ALL">Todas las acciones</option>
            {accessActions.map((action) => (
              <option key={action} value={action}>{action}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredAccessLog.length} eventos visibles</span>
        </div>
      )}

      {activeTab === 'access' && selectedDoc && (
        <div className="card p-4 mb-6 flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="text-xs uppercase tracking-wider text-surface-400">Documento seleccionado</p>
            <h3 className="text-sm font-semibold text-surface-800 mt-1">{selectedDoc.title}</h3>
          </div>
          <div className="flex items-center gap-2">
            <span className={`badge ${categoryConfig[selectedDoc.category]?.bg ?? 'bg-surface-100 text-surface-600'}`}>
              {categoryConfig[selectedDoc.category]?.label ?? selectedDoc.category}
            </span>
            <span className="text-xs text-surface-400 font-mono">v{selectedDoc.version}</span>
          </div>
        </div>
      )}

      {activeTab === 'documents' ? (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={columns}
            data={filteredDocs}
            loading={loading}
            emptyMessage="No hay documentos registrados."
            emptyIcon="📄"
            onRowClick={setSelectedDoc}
            selectedRowId={selectedDoc?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedDoc}
            emptyTitle="Selecciona un documento"
            emptyText="Explora versiones, metadatos y trazabilidad desde este panel contextual."
            title={selectedDoc?.title}
            subtitle={selectedDoc ? `${categoryConfig[selectedDoc.category]?.label ?? selectedDoc.category} · v${selectedDoc.version}` : undefined}
          >
            {selectedDoc && (
              <>
                <DetailGroup title="Identificación">
                  <DetailRow label="Archivo" value={selectedDoc.file_name} />
                  <DetailRow label="Mime type" value={selectedDoc.mime_type || '—'} mono />
                  <DetailRow label="Tamaño" value={formatBytes(selectedDoc.size_bytes)} accent />
                </DetailGroup>
                <DetailGroup title="Clasificación">
                  <DetailRow label="Categoría" value={categoryConfig[selectedDoc.category]?.label ?? selectedDoc.category} />
                  <DetailRow label="Etiquetas" value={selectedDoc.tags || 'Sin etiquetas'} />
                  <DetailRow label="Creado" value={new Date(selectedDoc.created_at).toLocaleString('es-ES')} />
                </DetailGroup>
                <DetailGroup title="Operativa">
                  <DetailRow label="Historial" value={<button className="btn-secondary !px-3 !py-1.5" onClick={() => void loadAccessLog(selectedDoc)}>Ver accesos</button>} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      ) : (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable
            columns={accessColumns}
            data={filteredAccessLog}
            loading={detailLoading}
            emptyMessage="Selecciona un documento para ver su historial de accesos."
            emptyIcon="🔐"
            onRowClick={setSelectedAccess}
            selectedRowId={selectedAccess?.id ?? null}
          />
          <DetailPanel
            isEmpty={!selectedDoc && !selectedAccess}
            emptyTitle="Sin evento seleccionado"
            emptyText="Selecciona un documento y un acceso para revisar la trazabilidad completa."
            title={selectedDoc?.title ?? 'Acceso documental'}
            subtitle={selectedAccess ? `${selectedAccess.action} · ${new Date(selectedAccess.accessed_at).toLocaleString('es-ES')}` : 'Trazabilidad de auditoría'}
          >
            {selectedDoc && (
              <DetailGroup title="Documento activo">
                <DetailRow label="Documento" value={selectedDoc.title} accent />
                <DetailRow label="Versión" value={`v${selectedDoc.version}`} />
                <DetailRow label="Categoría" value={categoryConfig[selectedDoc.category]?.label ?? selectedDoc.category} />
              </DetailGroup>
            )}
            {selectedAccess && (
              <DetailGroup title="Evento de acceso">
                <DetailRow label="Acción" value={selectedAccess.action} accent />
                <DetailRow label="Staff" value={selectedAccess.staff_id} mono />
                <DetailRow label="IP" value={selectedAccess.ip_address || 'No registrada'} mono />
                <DetailRow label="Momento" value={new Date(selectedAccess.accessed_at).toLocaleString('es-ES')} />
              </DetailGroup>
            )}
          </DetailPanel>
        </div>
      )}
    </div>
  )
}
