const API_BASE = '/api/v1'

export interface Staff {
  id: string; employee_code: string; first_name: string; last_name: string;
  email: string; phone: string; staff_type: string; specialty: string;
  license_number: string; department_id: string; hire_date: string; active: boolean;
}

export interface Department {
  id: string
  name: string
  code: string
  floor: number
  building: string
  head_id: string
  active: boolean
}

export interface UserAccount {
  id: string
  staff_id: string
  username: string
  role: string
  active: boolean
  last_login_at?: string
  created_at: string
}

export interface Patient {
  id: string; medical_record_no: string; first_name: string; last_name: string;
  date_of_birth: string; gender: string; national_id: string; phone: string;
  email: string; address: string; city: string; postal_code: string;
  blood_type: string; allergies: string; insurance_id: string; insurance_company: string; active: boolean;
}

export interface Appointment {
  id: string
  patient_id: string
  doctor_id: string
  department_id: string
  type: string
  status: string
  scheduled_at: string
  duration_minutes: number
  room: string
  notes: string
  diagnosis: string
  created_at?: string
  updated_at?: string
}

export interface Admission {
  id: string
  patient_id: string
  admitting_doctor_id: string
  bed_id: string
  department_id: string
  status: string
  admission_date: string
  discharge_date?: string
  diagnosis: string
  admission_reason: string
  dietary_needs?: string
  notes?: string
  created_at?: string
  updated_at?: string
}

export interface Ward {
  id: string
  name: string
  code: string
  department_id: string
  floor: number
  building: string
  total_beds: number
  active: boolean
}

export interface Bed {
  id: string
  ward_id: string
  number: string
  status: string
  room_no: string
  features: string
}

export interface InvoiceItem {
  id: string
  invoice_id: string
  description: string
  category: string
  quantity: number
  unit_price: number
  total: number
  created_at: string
}

export interface Invoice {
  id: string
  invoice_number: string
  patient_id: string
  admission_id?: string
  status: string
  subtotal: number
  tax: number
  discount: number
  total: number
  currency: string
  issued_at?: string
  due_date?: string
  paid_at?: string
  payment_method?: string
  notes?: string
  created_at?: string
  updated_at?: string
}

export interface DocumentAccess {
  id: string
  document_id: string
  staff_id: string
  action: string
  ip_address: string
  accessed_at: string
}

export interface Document {
  id: string
  title: string
  category: string
  patient_id?: string
  uploaded_by?: string
  file_name: string
  mime_type: string
  size_bytes: number
  checksum?: string
  version: number
  tags: string
  notes?: string
  created_at: string
  updated_at?: string
}

export interface Prescription {
  id: string
  patient_id: string
  doctor_id: string
  medication_id: string
  status: string
  dosage: string
  frequency: string
  duration: string
  instructions: string
  quantity: number
  refills_allowed: number
  refills_used: number
  prescribed_at: string
  dispensed_at?: string
}

export interface PharmacyDispense {
  id: string
  prescription_id: string
  pharmacist_id: string
  quantity: number
  notes: string
  dispensed_at: string
  created_at: string
}

export interface MealOrder {
  id: string
  admission_id: string
  meal_type: string
  date: string
  menu: string
  dietary_note: string
  delivered: boolean
  delivered_at?: string
}

export interface CareNote {
  id: string
  admission_id: string
  staff_id: string
  note_type: string
  content: string
  created_at: string
}

export interface Surgery {
  id: string
  patient_id: string
  lead_surgeon_id: string
  anesthetist_id: string
  operating_room_id: string
  procedure_name: string
  procedure_code: string
  status: string
  scheduled_start: string
  scheduled_end: string
  actual_start?: string
  actual_end?: string
  pre_op_notes?: string
  post_op_notes?: string
  complications?: string
}

export interface SurgeryTeamMember {
  id: string
  surgery_id: string
  staff_id: string
  role: string
  created_at: string
}

export interface RehabPlan {
  id: string
  patient_id: string
  therapist_id: string
  doctor_id: string
  type: string
  diagnosis: string
  goals: string
  start_date: string
  end_date?: string
  total_sessions: number
  completed_sessions: number
  active: boolean
  notes?: string
  created_at?: string
  updated_at?: string
}

export interface ConsultationRoom {
  id: string
  name: string
  code: string
  department_id: string
  floor: number
  building: string
  equipment: string
  active: boolean
}

interface ApiResponse<T = unknown> {
  success: boolean
  data: T
  error?: string
  meta?: { total: number; page: number; page_size: number; total_pages: number }
}

class ApiClient {
  private token: string | null = null

  setToken(token: string) {
    this.token = token
  }

  private async request<T = unknown>(method: string, path: string, body?: unknown): Promise<ApiResponse<T>> {
    const headers: Record<string, string> = { 'Content-Type': 'application/json' }
    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`
    }

    const res = await fetch(`${API_BASE}${path}`, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    })

    const json = await res.json()
    if (!res.ok) {
      throw new Error(json.error || `HTTP ${res.status}`)
    }
    return json as ApiResponse<T>
  }

  async get<T = unknown>(path: string): Promise<ApiResponse<T>> {
    return this.request<T>('GET', path)
  }

  async post<T = unknown>(path: string, body: unknown): Promise<ApiResponse<T>> {
    return this.request<T>('POST', path, body)
  }

  async put<T = unknown>(path: string, body: unknown): Promise<ApiResponse<T>> {
    return this.request<T>('PUT', path, body)
  }

  async delete<T = unknown>(path: string): Promise<ApiResponse<T>> {
    return this.request<T>('DELETE', path)
  }

  async login(username: string, password: string): Promise<string> {
    const res = await this.post<{ token: string }>('/auth/login', { username, password })
    const token = res.data.token
    this.setToken(token)
    return token
  }
}

export const api = new ApiClient()

// ── ASQL-specific types ─────────────────────────────────────────

export interface ChangeRecord {
  operation: string   // INSERT, UPDATE, DELETE
  commit_lsn: number  // WAL position
  columns: Record<string, string>
}

export interface Snapshot {
  lsn: number
  timestamp?: string
  patient?: Record<string, string>
  data?: Record<string, string>
}

export interface PatientWithInvoices {
  patient_id: string
  first_name: string
  last_name: string
  invoice_count: number
  total_billed: number
}

export interface AppointmentWithDetails {
  appointment_id: string
  patient_name: string
  doctor_name: string
  type: string
  status: string
  scheduled_at: string
}
