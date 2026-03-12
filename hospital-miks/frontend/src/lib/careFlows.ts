import type {
	Admission,
	Appointment,
	Document,
	Invoice,
	Patient,
	Prescription,
	RehabPlan,
	Surgery,
} from '../services/api'

type CareEventType = 'admission' | 'appointment' | 'surgery' | 'rehab' | 'prescription' | 'invoice' | 'document'
type CareFlowKind = 'hospitalization' | 'surgery' | 'rehab' | 'ambulatory' | 'medication' | 'mixed'

export interface CareFlowEvent {
	id: string
	sourceId: string
	type: CareEventType
	title: string
	subtitle: string
	status: string
	date: string
	endDate?: string
	amount?: number
	metadata?: Record<string, string | number | boolean | undefined>
}

export interface CareFlow {
	id: string
	title: string
	kind: CareFlowKind
	status: 'active' | 'planned' | 'completed'
	start: string
	end?: string
	summary: string
	patientId: string
	billedTotal: number
	eventCount: number
	counts: Record<CareEventType, number>
	events: CareFlowEvent[]
}

export interface CareFlowSourceData {
	appointments: Appointment[]
	admissions: Admission[]
	surgeries: Surgery[]
	rehabPlans: RehabPlan[]
	prescriptions: Prescription[]
	invoices: Invoice[]
	documents: Document[]
}

interface MutableFlow {
	id: string
	patientId: string
	events: CareFlowEvent[]
	counts: Record<CareEventType, number>
	billedTotal: number
	startMs: number
	endMs: number
	anchorAdmissionIds: Set<string>
}

const DAY_MS = 24 * 60 * 60 * 1000

function asTime(value?: string): number | null {
	if (!value) return null
	const time = Date.parse(value)
	return Number.isNaN(time) ? null : time
}

function safeDate(value?: string): string {
	return value ?? new Date(0).toISOString()
}

function addEvent(events: CareFlowEvent[], event: CareFlowEvent | null) {
	if (event) {
		events.push(event)
	}
}

function normalizeEvents(data: CareFlowSourceData): CareFlowEvent[] {
	const events: CareFlowEvent[] = []

	for (const admission of data.admissions) {
		addEvent(events, {
			id: `admission-${admission.id}`,
			sourceId: admission.id,
			type: 'admission',
			title: admission.admission_reason || 'Ingreso hospitalario',
			subtitle: admission.diagnosis || admission.notes || 'Episodio de hospitalización',
			status: admission.status,
			date: safeDate(admission.admission_date),
			endDate: admission.discharge_date,
			metadata: {
				admissionId: admission.id,
				departmentId: admission.department_id,
			},
		})
	}

	for (const surgery of data.surgeries) {
		addEvent(events, {
			id: `surgery-${surgery.id}`,
			sourceId: surgery.id,
			type: 'surgery',
			title: surgery.procedure_name || 'Intervención quirúrgica',
			subtitle: surgery.procedure_code || surgery.post_op_notes || surgery.complications || 'Proceso quirúrgico',
			status: surgery.status,
			date: safeDate(surgery.scheduled_start),
			endDate: surgery.actual_end ?? surgery.scheduled_end,
			metadata: {
				procedureCode: surgery.procedure_code,
			},
		})
	}

	for (const plan of data.rehabPlans) {
		addEvent(events, {
			id: `rehab-${plan.id}`,
			sourceId: plan.id,
			type: 'rehab',
			title: plan.type || 'Plan de rehabilitación',
			subtitle: plan.diagnosis || plan.goals || 'Seguimiento terapéutico',
			status: plan.active ? 'ACTIVE' : 'COMPLETED',
			date: safeDate(plan.start_date),
			endDate: plan.end_date,
			metadata: {
				totalSessions: plan.total_sessions,
				completedSessions: plan.completed_sessions,
			},
		})
	}

	for (const appointment of data.appointments) {
		const endMs = asTime(appointment.scheduled_at)
		const durationMs = Math.max(appointment.duration_minutes || 0, 0) * 60 * 1000
		addEvent(events, {
			id: `appointment-${appointment.id}`,
			sourceId: appointment.id,
			type: 'appointment',
			title: appointment.type || 'Cita clínica',
			subtitle: appointment.diagnosis || appointment.notes || appointment.room || 'Actividad asistencial programada',
			status: appointment.status,
			date: safeDate(appointment.scheduled_at),
			endDate: endMs ? new Date(endMs + durationMs).toISOString() : undefined,
			metadata: {
				room: appointment.room,
				doctorId: appointment.doctor_id,
			},
		})
	}

	for (const prescription of data.prescriptions) {
		addEvent(events, {
			id: `prescription-${prescription.id}`,
			sourceId: prescription.id,
			type: 'prescription',
			title: `Prescripción ${prescription.status.toLowerCase()}`,
			subtitle: [prescription.dosage, prescription.frequency, prescription.duration].filter(Boolean).join(' · ') || prescription.instructions || 'Tratamiento farmacológico',
			status: prescription.status,
			date: safeDate(prescription.prescribed_at),
			endDate: prescription.dispensed_at,
		})
	}

	for (const invoice of data.invoices) {
		const invoiceDate = invoice.issued_at ?? invoice.created_at ?? invoice.due_date ?? invoice.paid_at
		addEvent(events, {
			id: `invoice-${invoice.id}`,
			sourceId: invoice.id,
			type: 'invoice',
			title: invoice.invoice_number || 'Factura asistencial',
			subtitle: invoice.notes || `${invoice.total.toFixed(2)} ${invoice.currency || 'EUR'}`,
			status: invoice.status,
			date: safeDate(invoiceDate),
			amount: invoice.total,
			metadata: {
				admissionId: invoice.admission_id,
			},
		})
	}

	for (const document of data.documents) {
		addEvent(events, {
			id: `document-${document.id}`,
			sourceId: document.id,
			type: 'document',
			title: document.title,
			subtitle: document.category || document.file_name || 'Documento clínico',
			status: 'AVAILABLE',
			date: safeDate(document.created_at),
		})
	}

	return events.sort((a, b) => asTime(a.date)! - asTime(b.date)!)
}

function createFlow(patientId: string, event: CareFlowEvent, index: number): MutableFlow {
	const eventStart = asTime(event.date) ?? 0
	const eventEnd = asTime(event.endDate) ?? eventStart
	const counts: Record<CareEventType, number> = {
		admission: 0,
		appointment: 0,
		surgery: 0,
		rehab: 0,
		prescription: 0,
		invoice: 0,
		document: 0,
	}
	counts[event.type] = 1
	const anchorAdmissionIds = new Set<string>()
	const eventAdmissionId = typeof event.metadata?.admissionId === 'string' ? event.metadata.admissionId : undefined
	if (event.type === 'admission' || eventAdmissionId) {
		anchorAdmissionIds.add(eventAdmissionId ?? event.sourceId)
	}
	return {
		id: `flow-${patientId}-${index}`,
		patientId,
		events: [event],
		counts,
		billedTotal: event.amount ?? 0,
		startMs: eventStart,
		endMs: eventEnd,
		anchorAdmissionIds,
	}
}

function eventGraceDays(event: CareFlowEvent): number {
	switch (event.type) {
		case 'invoice':
		case 'document':
			return 90
		case 'rehab':
			return 60
		case 'prescription':
			return 45
		default:
			return 30
	}
}

function findFlow(flows: MutableFlow[], event: CareFlowEvent): MutableFlow | null {
	const linkedAdmissionId = typeof event.metadata?.admissionId === 'string' ? event.metadata.admissionId : undefined
	if (linkedAdmissionId) {
		const linked = flows.find((flow) => flow.anchorAdmissionIds.has(linkedAdmissionId))
		if (linked) return linked
	}

	const eventStart = asTime(event.date) ?? 0
	for (let index = flows.length - 1; index >= 0; index -= 1) {
		const flow = flows[index]
		if (eventStart <= flow.endMs + eventGraceDays(event) * DAY_MS) {
			return flow
		}
	}
	return null
}

function pushIntoFlow(flow: MutableFlow, event: CareFlowEvent) {
	flow.events.push(event)
	flow.counts[event.type] += 1
	flow.billedTotal += event.amount ?? 0
	flow.startMs = Math.min(flow.startMs, asTime(event.date) ?? flow.startMs)
	flow.endMs = Math.max(flow.endMs, asTime(event.endDate) ?? asTime(event.date) ?? flow.endMs)
	const linkedAdmissionId = typeof event.metadata?.admissionId === 'string' ? event.metadata.admissionId : undefined
	if (event.type === 'admission' || linkedAdmissionId) {
		flow.anchorAdmissionIds.add(linkedAdmissionId ?? event.sourceId)
	}
}

function dominantKind(counts: Record<CareEventType, number>): CareFlowKind {
	if (counts.admission > 0 && counts.surgery > 0) return 'surgery'
	if (counts.admission > 0) return 'hospitalization'
	if (counts.rehab > 0) return 'rehab'
	if (counts.prescription > 0 && counts.appointment === 0) return 'medication'
	if (counts.appointment > 0) return 'ambulatory'
	return 'mixed'
}

function flowTitle(kind: CareFlowKind, flow: MutableFlow): string {
	const firstAdmission = flow.events.find((event) => event.type === 'admission')
	const firstSurgery = flow.events.find((event) => event.type === 'surgery')
	const firstRehab = flow.events.find((event) => event.type === 'rehab')
	const firstAppointment = flow.events.find((event) => event.type === 'appointment')
	const firstPrescription = flow.events.find((event) => event.type === 'prescription')

	switch (kind) {
		case 'surgery':
			return firstSurgery?.title ? `Flujo quirúrgico · ${firstSurgery.title}` : 'Flujo quirúrgico'
		case 'hospitalization':
			return firstAdmission?.title ? `Ingreso asistencial · ${firstAdmission.title}` : 'Ingreso asistencial'
		case 'rehab':
			return firstRehab?.title ? `Rehabilitación · ${firstRehab.title}` : 'Rehabilitación'
		case 'medication':
			return firstPrescription?.subtitle ? `Tratamiento farmacológico · ${firstPrescription.subtitle}` : 'Tratamiento farmacológico'
		case 'ambulatory':
			return firstAppointment?.title ? `Seguimiento ambulatorio · ${firstAppointment.title}` : 'Seguimiento ambulatorio'
		default:
			return 'Flujo asistencial mixto'
	}
}

function flowStatus(flow: MutableFlow): 'active' | 'planned' | 'completed' {
	const statuses = new Set(flow.events.map((event) => event.status))
	if (statuses.has('ADMITTED') || statuses.has('IN_CARE') || statuses.has('IN_PROGRESS') || statuses.has('ACTIVE')) {
		return 'active'
	}
	if (statuses.has('SCHEDULED') || statuses.has('CONFIRMED') || statuses.has('PRE_OP')) {
		return 'planned'
	}
	return 'completed'
}

function flowSummary(flow: MutableFlow): string {
	const parts: string[] = []
	if (flow.counts.admission > 0) parts.push(`${flow.counts.admission} ingreso${flow.counts.admission > 1 ? 's' : ''}`)
	if (flow.counts.surgery > 0) parts.push(`${flow.counts.surgery} cirugía${flow.counts.surgery > 1 ? 's' : ''}`)
	if (flow.counts.rehab > 0) parts.push(`${flow.counts.rehab} plan${flow.counts.rehab > 1 ? 'es' : ''} rehab`)
	if (flow.counts.appointment > 0) parts.push(`${flow.counts.appointment} cita${flow.counts.appointment > 1 ? 's' : ''}`)
	if (flow.counts.prescription > 0) parts.push(`${flow.counts.prescription} prescripción${flow.counts.prescription > 1 ? 'es' : ''}`)
	if (flow.counts.document > 0) parts.push(`${flow.counts.document} documento${flow.counts.document > 1 ? 's' : ''}`)
	if (flow.counts.invoice > 0) parts.push(`${flow.counts.invoice} factura${flow.counts.invoice > 1 ? 's' : ''}`)
	return parts.join(' · ') || 'Actividad asistencial consolidada'
}

export function deriveCareFlows(patient: Patient | null, data: CareFlowSourceData): CareFlow[] {
	if (!patient) return []
	const events = normalizeEvents(data)
	if (events.length === 0) return []

	const flows: MutableFlow[] = []
	for (const event of events) {
		const flow = findFlow(flows, event)
		if (!flow) {
			flows.push(createFlow(patient.id, event, flows.length + 1))
			continue
		}
		pushIntoFlow(flow, event)
	}

	return flows
		.map((flow) => {
			const kind = dominantKind(flow.counts)
			const eventsDesc = [...flow.events].sort((a, b) => (asTime(b.date) ?? 0) - (asTime(a.date) ?? 0))
			return {
				id: flow.id,
				title: flowTitle(kind, flow),
				kind,
				status: flowStatus(flow),
				start: new Date(flow.startMs).toISOString(),
				end: flow.endMs > flow.startMs ? new Date(flow.endMs).toISOString() : undefined,
				summary: flowSummary(flow),
				patientId: flow.patientId,
				billedTotal: flow.billedTotal,
				eventCount: flow.events.length,
				counts: flow.counts,
				events: eventsDesc,
			}
		})
		.sort((a, b) => (asTime(b.start) ?? 0) - (asTime(a.start) ?? 0))
}