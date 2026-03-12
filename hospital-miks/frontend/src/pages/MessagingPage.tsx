import { useEffect, useMemo, useState } from 'react'
import { api } from '../services/api'
import DataTable from '../components/DataTable'
import { DetailGroup, DetailPanel, DetailRow } from '../components/DetailPanel'
import KpiStrip from '../components/KpiStrip'
import PageHeader, { TabButton } from '../components/PageHeader'

interface Message {
  id: string; sender_id: string; receiver_id: string; subject: string;
  body: string; priority: string; read: boolean; read_at: string;
  parent_id: string; created_at: string;
}

interface PatientComm {
  id: string; patient_id: string; staff_id: string; channel: string;
  subject: string; content: string; status: string; sent_at: string;
  delivered_at: string; created_at: string;
}

const DEMO_USER_ID = '00000000-0000-0000-0000-000000000001'
const DEMO_PATIENT_ID = '00000000-0000-0000-0000-000000000001'

const priorityConfig: Record<string, { label: string; bg: string }> = {
  LOW: { label: 'Baja', bg: 'bg-surface-100 text-surface-500' },
  NORMAL: { label: 'Normal', bg: 'bg-blue-50 text-blue-600' },
  HIGH: { label: 'Alta', bg: 'bg-amber-50 text-amber-600' },
  URGENT: { label: 'Urgente', bg: 'bg-red-50 text-red-600 ring-1 ring-inset ring-red-200' },
}

const channelConfig: Record<string, { label: string; bg: string }> = {
  SMS: { label: 'SMS', bg: 'bg-purple-50 text-purple-600' },
  EMAIL: { label: 'Email', bg: 'bg-blue-50 text-blue-600' },
  PHONE: { label: 'Teléfono', bg: 'bg-emerald-50 text-emerald-600' },
  IN_PERSON: { label: 'Presencial', bg: 'bg-surface-100 text-surface-600' },
  PORTAL: { label: 'Portal', bg: 'bg-indigo-50 text-indigo-600' },
}

export default function MessagingPage() {
  const [tab, setTab] = useState<'inbox' | 'sent' | 'patient'>('inbox')
  const [inbox, setInbox] = useState<Message[]>([])
  const [sent, setSent] = useState<Message[]>([])
  const [patientComms, setPatientComms] = useState<PatientComm[]>([])
  const [unreadCount, setUnreadCount] = useState(0)
  const [loadingInbox, setLoadingInbox] = useState(true)
  const [loadingSent, setLoadingSent] = useState(false)
  const [loadingPatient, setLoadingPatient] = useState(false)
  const [selectedInboxMessage, setSelectedInboxMessage] = useState<Message | null>(null)
  const [selectedSentMessage, setSelectedSentMessage] = useState<Message | null>(null)
  const [selectedPatientComm, setSelectedPatientComm] = useState<PatientComm | null>(null)
  const [priorityFilter, setPriorityFilter] = useState<'ALL' | string>('ALL')
  const [commStatusFilter, setCommStatusFilter] = useState<'ALL' | string>('ALL')
  const [messageSearch, setMessageSearch] = useState('')

  useEffect(() => { loadInbox() }, [])
  useEffect(() => {
    if (tab === 'sent' && sent.length === 0) loadSent()
    if (tab === 'patient' && patientComms.length === 0) loadPatientComms()
  }, [tab])

  async function loadInbox() {
    setLoadingInbox(true)
    try {
      const [inboxRes, countRes] = await Promise.all([
        api.get<Message[]>(`/messaging/inbox/${DEMO_USER_ID}`),
        api.get<{ count: number }>(`/messaging/unread-count/${DEMO_USER_ID}`),
      ])
      const nextInbox = inboxRes.data ?? []
      setInbox(nextInbox)
      setSelectedInboxMessage((current) => nextInbox.find((message) => message.id === current?.id) ?? current ?? nextInbox[0] ?? null)
      setUnreadCount((countRes.data as { count: number })?.count ?? 0)
    } finally { setLoadingInbox(false) }
  }

  async function loadSent() {
    setLoadingSent(true)
    try {
      const res = await api.get<Message[]>(`/messaging/sent/${DEMO_USER_ID}`)
      const nextSent = res.data ?? []
      setSent(nextSent)
      setSelectedSentMessage((current) => nextSent.find((message) => message.id === current?.id) ?? current ?? nextSent[0] ?? null)
    } finally { setLoadingSent(false) }
  }

  async function loadPatientComms() {
    setLoadingPatient(true)
    try {
      const res = await api.get<PatientComm[]>(`/messaging/patient-comms/patient/${DEMO_PATIENT_ID}`)
      const nextComms = res.data ?? []
      setPatientComms(nextComms)
      setSelectedPatientComm((current) => nextComms.find((comm) => comm.id === current?.id) ?? current ?? nextComms[0] ?? null)
    } finally { setLoadingPatient(false) }
  }

  const messageColumns = [
    { key: 'subject' as const, label: 'Asunto', render: (v: string, row: Message) =>
      <div className="flex items-center gap-2">
        {!row.read && <span className="w-2 h-2 rounded-full bg-primary-500 flex-shrink-0" />}
        <span className={row.read ? 'text-surface-500' : 'font-semibold text-surface-800'}>{v}</span>
      </div>
    },
    { key: 'priority' as const, label: 'Prioridad', render: (v: string) => {
      const cfg = priorityConfig[v]
      return <span className={`badge ${cfg?.bg ?? 'bg-surface-100 text-surface-600'}`}>{cfg?.label ?? v}</span>
    }},
    { key: 'read' as const, label: 'Estado', render: (v: boolean) =>
      v ? <span className="badge bg-surface-100 text-surface-400">Leído</span>
        : <span className="badge bg-primary-50 text-primary-600 ring-1 ring-inset ring-primary-200">No leído</span>
    },
    { key: 'created_at' as const, label: 'Fecha', render: (v: string) =>
      <span className="text-surface-500 text-sm">{new Date(v).toLocaleString('es-ES')}</span>
    },
  ]

  const sentColumns = [
    { key: 'subject' as const, label: 'Asunto', render: (v: string) =>
      <span className="font-medium text-surface-800">{v}</span>
    },
    { key: 'priority' as const, label: 'Prioridad', render: (v: string) => {
      const cfg = priorityConfig[v]
      return <span className={`badge ${cfg?.bg ?? 'bg-surface-100 text-surface-600'}`}>{cfg?.label ?? v}</span>
    }},
    { key: 'receiver_id' as const, label: 'Destinatario', render: (v: string) =>
      <span className="font-mono text-xs text-surface-400">{v.slice(0, 8)}…</span>
    },
    { key: 'created_at' as const, label: 'Fecha', render: (v: string) =>
      <span className="text-surface-500 text-sm">{new Date(v).toLocaleString('es-ES')}</span>
    },
  ]

  const commStatusConfig: Record<string, { label: string; bg: string }> = {
    SENT: { label: 'Enviado', bg: 'bg-blue-50 text-blue-600' },
    DELIVERED: { label: 'Entregado', bg: 'bg-emerald-50 text-emerald-600' },
    FAILED: { label: 'Error', bg: 'bg-red-50 text-red-600' },
    READ: { label: 'Leído', bg: 'bg-surface-100 text-surface-500' },
  }

  const commColumns = [
    { key: 'channel' as const, label: 'Canal', render: (v: string) => {
      const cfg = channelConfig[v]
      return <span className={`badge ${cfg?.bg ?? 'bg-surface-100 text-surface-600'}`}>{cfg?.label ?? v}</span>
    }},
    { key: 'subject' as const, label: 'Asunto', render: (v: string) =>
      <span className="font-medium text-surface-800">{v}</span>
    },
    { key: 'status' as const, label: 'Estado', render: (v: string) => {
      const cfg = commStatusConfig[v]
      return <span className={`badge ${cfg?.bg ?? 'bg-surface-100 text-surface-600'}`}>{cfg?.label ?? v}</span>
    }},
    { key: 'sent_at' as const, label: 'Enviado', render: (v: string) =>
      <span className="text-surface-500 text-sm">{new Date(v).toLocaleString('es-ES')}</span>
    },
  ]

  const messagePriorities = useMemo(() => Array.from(new Set([...inbox, ...sent].map((message) => message.priority))), [inbox, sent])
  const commStatuses = useMemo(() => Array.from(new Set(patientComms.map((comm) => comm.status))), [patientComms])
  const urgentMessages = useMemo(() => inbox.filter((message) => ['HIGH', 'URGENT'].includes(message.priority)).length, [inbox])
  const deliveredComms = useMemo(() => patientComms.filter((comm) => comm.status === 'DELIVERED' || comm.status === 'READ').length, [patientComms])

  const filteredInbox = useMemo(() => {
    const query = messageSearch.trim().toLowerCase()
    return inbox.filter((message) => {
      const matchesQuery = !query || [message.subject, message.body].some((value) => value?.toLowerCase().includes(query))
      const matchesPriority = priorityFilter === 'ALL' || message.priority === priorityFilter
      return matchesQuery && matchesPriority
    })
  }, [inbox, messageSearch, priorityFilter])

  const filteredSent = useMemo(() => {
    const query = messageSearch.trim().toLowerCase()
    return sent.filter((message) => {
      const matchesQuery = !query || [message.subject, message.body].some((value) => value?.toLowerCase().includes(query))
      const matchesPriority = priorityFilter === 'ALL' || message.priority === priorityFilter
      return matchesQuery && matchesPriority
    })
  }, [messageSearch, priorityFilter, sent])

  const filteredPatientComms = useMemo(() => {
    return patientComms.filter((comm) => commStatusFilter === 'ALL' || comm.status === commStatusFilter)
  }, [commStatusFilter, patientComms])

  return (
    <div>
      <PageHeader title="Mensajería" subtitle="Comunicación interna y con pacientes" action="Nuevo mensaje" onAction={() => {}}>
        <div className="flex items-center gap-3">
          {unreadCount > 0 && (
            <span className="bg-red-500 text-white text-[11px] font-bold rounded-full px-2.5 py-0.5 animate-pulse-slow">
              {unreadCount} sin leer
            </span>
          )}
          <div className="flex gap-1.5">
            <TabButton active={tab === 'inbox'} onClick={() => setTab('inbox')}>Entrada</TabButton>
            <TabButton active={tab === 'sent'} onClick={() => setTab('sent')}>Enviados</TabButton>
            <TabButton active={tab === 'patient'} onClick={() => setTab('patient')}>Pacientes</TabButton>
          </div>
        </div>
      </PageHeader>

      <KpiStrip
        items={[
          { label: 'No leídos', value: unreadCount, hint: 'entrada actual', tone: unreadCount > 0 ? 'danger' : 'success' },
          { label: 'Inbox', value: inbox.length, hint: 'mensajes recibidos', tone: 'info' },
          { label: 'Urgentes', value: urgentMessages, hint: 'alta prioridad', tone: urgentMessages > 0 ? 'warning' : 'default' },
          { label: 'Paciente entregados', value: deliveredComms, hint: 'comunicaciones efectivas', tone: 'success' },
        ]}
      />

      <div className="card p-4 mb-6 grid grid-cols-1 xl:grid-cols-3 gap-4">
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Foco entrada</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{selectedInboxMessage?.subject ?? 'Sin selección'}</p>
          <p className="text-sm text-surface-500 mt-1">{selectedInboxMessage ? `${priorityConfig[selectedInboxMessage.priority]?.label ?? selectedInboxMessage.priority} · ${selectedInboxMessage.read ? 'Leído' : 'No leído'}` : 'Selecciona un mensaje para activar contexto'}</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Foco paciente</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{selectedPatientComm?.subject ?? 'Sin selección'}</p>
          <p className="text-sm text-surface-500 mt-1">{selectedPatientComm ? `${channelConfig[selectedPatientComm.channel]?.label ?? selectedPatientComm.channel} · ${selectedPatientComm.status}` : 'Selecciona una comunicación paciente'}</p>
        </div>
        <div className="rounded-2xl border border-surface-200 bg-gradient-to-br from-white to-surface-50 p-4">
          <p className="text-[11px] uppercase tracking-[0.18em] text-surface-400">Pipeline</p>
          <p className="mt-2 text-sm font-medium text-surface-800">{sent.length} enviados · {patientComms.length} paciente</p>
          <p className="text-sm text-surface-500 mt-1">seguimiento operativo de comunicación interna y externa</p>
        </div>
      </div>

      {(tab === 'inbox' || tab === 'sent') && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <input className="input max-w-sm" placeholder="Buscar por asunto o contenido" value={messageSearch} onChange={(e) => setMessageSearch(e.target.value)} />
          <select className="input max-w-xs" value={priorityFilter} onChange={(e) => setPriorityFilter(e.target.value)}>
            <option value="ALL">Todas las prioridades</option>
            {messagePriorities.map((priority) => (
              <option key={priority} value={priority}>{priorityConfig[priority]?.label ?? priority}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{tab === 'inbox' ? filteredInbox.length : filteredSent.length} visibles</span>
        </div>
      )}

      {tab === 'patient' && (
        <div className="card p-4 mb-6 flex flex-wrap items-center gap-3">
          <select className="input max-w-xs" value={commStatusFilter} onChange={(e) => setCommStatusFilter(e.target.value)}>
            <option value="ALL">Todos los estados</option>
            {commStatuses.map((status) => (
              <option key={status} value={status}>{commStatusConfig[status]?.label ?? status}</option>
            ))}
          </select>
          <span className="text-xs text-surface-400">{filteredPatientComms.length} comunicaciones visibles</span>
        </div>
      )}

      {tab === 'inbox' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable columns={messageColumns} data={filteredInbox} loading={loadingInbox} emptyMessage="No hay mensajes en la bandeja de entrada." emptyIcon="📬" onRowClick={setSelectedInboxMessage} selectedRowId={selectedInboxMessage?.id ?? null} />
          <DetailPanel isEmpty={!selectedInboxMessage} emptyTitle="Selecciona un mensaje" emptyText="Consulta prioridad, lectura y contenido desde este panel." title={selectedInboxMessage?.subject} subtitle={selectedInboxMessage ? `${priorityConfig[selectedInboxMessage.priority]?.label ?? selectedInboxMessage.priority} · ${new Date(selectedInboxMessage.created_at).toLocaleString('es-ES')}` : undefined}>
            {selectedInboxMessage && (
              <>
                <DetailGroup title="Trazabilidad">
                  <DetailRow label="Remitente" value={selectedInboxMessage.sender_id} mono />
                  <DetailRow label="Prioridad" value={priorityConfig[selectedInboxMessage.priority]?.label ?? selectedInboxMessage.priority} accent />
                  <DetailRow label="Estado" value={selectedInboxMessage.read ? 'Leído' : 'No leído'} />
                </DetailGroup>
                <DetailGroup title="Contenido">
                  <DetailRow label="Mensaje" value={selectedInboxMessage.body || 'Sin contenido'} />
                  <DetailRow label="Creado" value={new Date(selectedInboxMessage.created_at).toLocaleString('es-ES')} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
      {tab === 'sent' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable columns={sentColumns} data={filteredSent} loading={loadingSent} emptyMessage="No hay mensajes enviados." emptyIcon="📤" onRowClick={setSelectedSentMessage} selectedRowId={selectedSentMessage?.id ?? null} />
          <DetailPanel isEmpty={!selectedSentMessage} emptyTitle="Selecciona un envío" emptyText="Revisa destinatario, prioridad y contenido de salida." title={selectedSentMessage?.subject} subtitle={selectedSentMessage ? `${priorityConfig[selectedSentMessage.priority]?.label ?? selectedSentMessage.priority} · ${new Date(selectedSentMessage.created_at).toLocaleString('es-ES')}` : undefined}>
            {selectedSentMessage && (
              <>
                <DetailGroup title="Entrega">
                  <DetailRow label="Destinatario" value={selectedSentMessage.receiver_id} mono />
                  <DetailRow label="Prioridad" value={priorityConfig[selectedSentMessage.priority]?.label ?? selectedSentMessage.priority} accent />
                  <DetailRow label="Creado" value={new Date(selectedSentMessage.created_at).toLocaleString('es-ES')} />
                </DetailGroup>
                <DetailGroup title="Contenido">
                  <DetailRow label="Mensaje" value={selectedSentMessage.body || 'Sin contenido'} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
      {tab === 'patient' && (
        <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.8fr)_380px] gap-6 items-start">
          <DataTable columns={commColumns} data={filteredPatientComms} loading={loadingPatient} emptyMessage="No hay comunicaciones con pacientes." emptyIcon="💬" onRowClick={setSelectedPatientComm} selectedRowId={selectedPatientComm?.id ?? null} />
          <DetailPanel isEmpty={!selectedPatientComm} emptyTitle="Selecciona una comunicación" emptyText="Consulta canal, estado y contenido dirigido al paciente." title={selectedPatientComm?.subject} subtitle={selectedPatientComm ? `${channelConfig[selectedPatientComm.channel]?.label ?? selectedPatientComm.channel} · ${commStatusConfig[selectedPatientComm.status]?.label ?? selectedPatientComm.status}` : undefined}>
            {selectedPatientComm && (
              <>
                <DetailGroup title="Canal y estado">
                  <DetailRow label="Canal" value={channelConfig[selectedPatientComm.channel]?.label ?? selectedPatientComm.channel} accent />
                  <DetailRow label="Estado" value={commStatusConfig[selectedPatientComm.status]?.label ?? selectedPatientComm.status} />
                  <DetailRow label="Staff" value={selectedPatientComm.staff_id} mono />
                </DetailGroup>
                <DetailGroup title="Contenido">
                  <DetailRow label="Texto" value={selectedPatientComm.content || 'Sin contenido'} />
                  <DetailRow label="Enviado" value={new Date(selectedPatientComm.sent_at).toLocaleString('es-ES')} />
                </DetailGroup>
              </>
            )}
          </DetailPanel>
        </div>
      )}
    </div>
  )
}
