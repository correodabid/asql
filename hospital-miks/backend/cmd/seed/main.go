package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

const defaultDSN = "postgres://asql:asql@127.0.0.1:5432/hospital_miks?sslmode=disable"

type seedCatalog struct {
	departments        []string
	staff              []string
	users              []string
	patients           []string
	consultationRooms  []string
	operatingRooms     []string
	wards              []string
	beds               []string
	appointments       []string
	admissions         []string
	mealOrders         []string
	careNotes          []string
	surgeries          []string
	surgeryTeamMembers []string
	medications        []string
	prescriptions      []string
	dispenses          []string
	invoices           []string
	invoiceItems       []string
	guardShifts        []string
	rehabPlans         []string
	rehabSessions      []string
	messages           []string
	patientComms       []string
	documents          []string
	documentAccesses   []string
}

var demo = seedCatalog{
	departments: []string{"20000000-0000-0000-0000-000000000001", "20000000-0000-0000-0000-000000000002", "20000000-0000-0000-0000-000000000003", "20000000-0000-0000-0000-000000000004"},
	staff: []string{"21000000-0000-0000-0000-000000000001", "21000000-0000-0000-0000-000000000002", "21000000-0000-0000-0000-000000000003", "21000000-0000-0000-0000-000000000004", "21000000-0000-0000-0000-000000000005", "21000000-0000-0000-0000-000000000006", "21000000-0000-0000-0000-000000000007", "21000000-0000-0000-0000-000000000008"},
	users: []string{"22000000-0000-0000-0000-000000000001", "22000000-0000-0000-0000-000000000002", "22000000-0000-0000-0000-000000000003", "22000000-0000-0000-0000-000000000004"},
	patients: []string{"23000000-0000-0000-0000-000000000001", "23000000-0000-0000-0000-000000000002", "23000000-0000-0000-0000-000000000003", "23000000-0000-0000-0000-000000000004"},
	consultationRooms: []string{"24000000-0000-0000-0000-000000000001", "24000000-0000-0000-0000-000000000002", "24000000-0000-0000-0000-000000000003"},
	operatingRooms: []string{"24100000-0000-0000-0000-000000000001", "24100000-0000-0000-0000-000000000002"},
	wards: []string{"24200000-0000-0000-0000-000000000001", "24200000-0000-0000-0000-000000000002"},
	beds: []string{"24300000-0000-0000-0000-000000000001", "24300000-0000-0000-0000-000000000002", "24300000-0000-0000-0000-000000000003"},
	appointments: []string{"24400000-0000-0000-0000-000000000001", "24400000-0000-0000-0000-000000000002", "24400000-0000-0000-0000-000000000003", "24400000-0000-0000-0000-000000000004", "24400000-0000-0000-0000-000000000005", "24400000-0000-0000-0000-000000000006"},
	admissions: []string{"24500000-0000-0000-0000-000000000001", "24500000-0000-0000-0000-000000000002", "24500000-0000-0000-0000-000000000003"},
	mealOrders: []string{"24600000-0000-0000-0000-000000000001", "24600000-0000-0000-0000-000000000002", "24600000-0000-0000-0000-000000000003"},
	careNotes: []string{"24700000-0000-0000-0000-000000000001", "24700000-0000-0000-0000-000000000002", "24700000-0000-0000-0000-000000000003"},
	surgeries: []string{"24800000-0000-0000-0000-000000000001", "24800000-0000-0000-0000-000000000002"},
	surgeryTeamMembers: []string{"24900000-0000-0000-0000-000000000001", "24900000-0000-0000-0000-000000000002", "24900000-0000-0000-0000-000000000003"},
	medications: []string{"25000000-0000-0000-0000-000000000001", "25000000-0000-0000-0000-000000000002", "25000000-0000-0000-0000-000000000003"},
	prescriptions: []string{"25100000-0000-0000-0000-000000000001", "25100000-0000-0000-0000-000000000002", "25100000-0000-0000-0000-000000000003"},
	dispenses: []string{"25200000-0000-0000-0000-000000000001", "25200000-0000-0000-0000-000000000002"},
	invoices: []string{"25300000-0000-0000-0000-000000000001", "25300000-0000-0000-0000-000000000002", "25300000-0000-0000-0000-000000000003", "25300000-0000-0000-0000-000000000004"},
	invoiceItems: []string{"25400000-0000-0000-0000-000000000001", "25400000-0000-0000-0000-000000000002", "25400000-0000-0000-0000-000000000003", "25400000-0000-0000-0000-000000000004", "25400000-0000-0000-0000-000000000005", "25400000-0000-0000-0000-000000000006"},
	guardShifts: []string{"25500000-0000-0000-0000-000000000001", "25500000-0000-0000-0000-000000000002", "25500000-0000-0000-0000-000000000003", "25500000-0000-0000-0000-000000000004"},
	rehabPlans: []string{"25600000-0000-0000-0000-000000000001", "25600000-0000-0000-0000-000000000002"},
	rehabSessions: []string{"25700000-0000-0000-0000-000000000001", "25700000-0000-0000-0000-000000000002", "25700000-0000-0000-0000-000000000003", "25700000-0000-0000-0000-000000000004"},
	messages: []string{"25800000-0000-0000-0000-000000000001", "25800000-0000-0000-0000-000000000002", "25800000-0000-0000-0000-000000000003"},
	patientComms: []string{"25900000-0000-0000-0000-000000000001", "25900000-0000-0000-0000-000000000002"},
	documents: []string{"26000000-0000-0000-0000-000000000001", "26000000-0000-0000-0000-000000000002", "26000000-0000-0000-0000-000000000003", "26000000-0000-0000-0000-000000000004", "26000000-0000-0000-0000-000000000005"},
	documentAccesses: []string{"26100000-0000-0000-0000-000000000001", "26100000-0000-0000-0000-000000000002", "26100000-0000-0000-0000-000000000003"},
}

type seedTimes struct {
	now              string
	historicalFlowA  string
	historicalFlowA2 string
	historicalFlowA3 string
	historicalFlowB  string
	historicalFlowB2 string
	recentFlow       string
	recentFlow2      string
	recentFlow3      string
	currentFlow      string
	currentFlow2     string
	currentFlow3     string
	futureFlow       string
	futureFlow2      string
	futureFlow3      string
}

func main() {
	dsn := defaultDSN
	if v := os.Getenv("DB_DSN"); v != "" {
		dsn = v
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	seed := buildSeedTimes(time.Now().UTC())

	resetDemoData(ctx, conn)
	seedStaff(ctx, conn, seed)
	seedIdentity(ctx, conn, seed)
	seedPatients(ctx, conn, seed)
	seedClinical(ctx, conn, seed)
	seedPharmacy(ctx, conn, seed)
	seedBilling(ctx, conn, seed)
	seedScheduling(ctx, conn, seed)
	seedRehab(ctx, conn, seed)
	seedMessaging(ctx, conn, seed)
	seedDocuments(ctx, conn, seed)

	fmt.Println("\n🏥 Hospital MiKS demo data ready")
	fmt.Println("   Users: admin/admin123 · doctor/doctor123 · nurse/nurse123 · pharmacist/pharma123")
	fmt.Println("   Coverage: staff, identity, patients, clinical, pharmacy, billing, scheduling, rehab, messaging, documents")
	fmt.Println("   Includes historical, active and planned flows to test longitudinal episodes")
}

func buildSeedTimes(now time.Time) seedTimes {
	return seedTimes{
		now:              ts(now),
		historicalFlowA:  ts(now.AddDate(-3, -2, 0)),
		historicalFlowA2: ts(now.AddDate(-3, -2, 2)),
		historicalFlowA3: ts(now.AddDate(-3, -2, 5)),
		historicalFlowB:  ts(now.AddDate(-2, -4, 0)),
		historicalFlowB2: ts(now.AddDate(-2, -4, 10)),
		recentFlow:       ts(now.AddDate(-1, -1, -10)),
		recentFlow2:      ts(now.AddDate(-1, -1, -5)),
		recentFlow3:      ts(now.AddDate(-1, -1, -2)),
		currentFlow:      ts(now.AddDate(0, 0, -4)),
		currentFlow2:     ts(now.AddDate(0, 0, -2)),
		currentFlow3:     ts(now.AddDate(0, 0, -1)),
		futureFlow:       ts(now.AddDate(0, 0, 3)),
		futureFlow2:      ts(now.AddDate(0, 0, 8)),
		futureFlow3:      ts(now.AddDate(0, 0, 14)),
	}
}

func resetDemoData(ctx context.Context, conn *pgx.Conn) {
	beginDomain(ctx, conn, "documents")
	deleteByIDs(ctx, conn, "document_access_log", demo.documentAccesses)
	deleteByIDs(ctx, conn, "documents", demo.documents)
	commitDomain(ctx, conn)

	beginDomain(ctx, conn, "messaging")
	deleteByIDs(ctx, conn, "patient_communications", demo.patientComms)
	deleteByIDs(ctx, conn, "messages", demo.messages)
	commitDomain(ctx, conn)

	beginDomain(ctx, conn, "rehab")
	deleteByIDs(ctx, conn, "rehab_sessions", demo.rehabSessions)
	deleteByIDs(ctx, conn, "rehab_plans", demo.rehabPlans)
	commitDomain(ctx, conn)

	beginDomain(ctx, conn, "scheduling")
	deleteByIDs(ctx, conn, "guard_shifts", demo.guardShifts)
	commitDomain(ctx, conn)

	beginDomain(ctx, conn, "billing")
	deleteByIDs(ctx, conn, "invoice_items", demo.invoiceItems)
	deleteByIDs(ctx, conn, "invoices", demo.invoices)
	commitDomain(ctx, conn)

	beginDomain(ctx, conn, "pharmacy")
	deleteByIDs(ctx, conn, "pharmacy_dispenses", demo.dispenses)
	deleteByIDs(ctx, conn, "prescriptions", demo.prescriptions)
	deleteByIDs(ctx, conn, "medications", demo.medications)
	commitDomain(ctx, conn)

	beginDomain(ctx, conn, "clinical")
	deleteByIDs(ctx, conn, "surgery_team_members", demo.surgeryTeamMembers)
	deleteByIDs(ctx, conn, "care_notes", demo.careNotes)
	deleteByIDs(ctx, conn, "meal_orders", demo.mealOrders)
	deleteByIDs(ctx, conn, "surgeries", demo.surgeries)
	deleteByIDs(ctx, conn, "admissions", demo.admissions)
	deleteByIDs(ctx, conn, "appointments", demo.appointments)
	deleteByIDs(ctx, conn, "beds", demo.beds)
	deleteByIDs(ctx, conn, "wards", demo.wards)
	deleteByIDs(ctx, conn, "operating_rooms", demo.operatingRooms)
	deleteByIDs(ctx, conn, "consultation_rooms", demo.consultationRooms)
	commitDomain(ctx, conn)

	beginDomain(ctx, conn, "patients")
	deleteWhereTextIn(ctx, conn, "patients", "national_id", []string{"12345678A", "87654321B", "11223344C", "99887766D"})
	deleteByIDs(ctx, conn, "patients", demo.patients)
	commitDomain(ctx, conn)

	beginDomain(ctx, conn, "identity")
	deleteWhereTextIn(ctx, conn, "users", "username", []string{"admin", "doctor", "nurse", "pharmacist"})
	deleteByIDs(ctx, conn, "users", demo.users)
	commitDomain(ctx, conn)

	beginDomain(ctx, conn, "staff")
	deleteWhereTextIn(ctx, conn, "staff", "employee_code", []string{"EMP-001", "EMP-002", "EMP-003", "EMP-004", "EMP-005", "EMP-006", "EMP-007", "EMP-008"})
	deleteWhereTextIn(ctx, conn, "departments", "code", []string{"MED-INT", "CARD", "TRAUMA", "PHARM"})
	deleteByIDs(ctx, conn, "staff", demo.staff)
	deleteByIDs(ctx, conn, "departments", demo.departments)
	commitDomain(ctx, conn)
}

func seedStaff(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	beginDomain(ctx, conn, "staff")
	execSQL(ctx, conn, `INSERT INTO departments (id, name, code, floor, building, head_id, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, demo.departments[0], "Medicina Interna", "MED-INT", 2, "Edificio A", demo.staff[0], true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO departments (id, name, code, floor, building, head_id, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, demo.departments[1], "Cardiología", "CARD", 3, "Edificio A", demo.staff[1], true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO departments (id, name, code, floor, building, head_id, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, demo.departments[2], "Cirugía Ortopédica", "TRAUMA", 4, "Edificio B", demo.staff[2], true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO departments (id, name, code, floor, building, head_id, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, demo.departments[3], "Farmacia Hospitalaria", "PHARM", 1, "Edificio C", demo.staff[6], true, seed.now, seed.now)

	rows := []struct{ id, code, first, last, email, phone, kind, specialty, license, dept string }{
		{demo.staff[0], "EMP-001", "Carlos", "Vega", "carlos.vega@hospital-miks.com", "+34600111001", "DOCTOR", "Medicina Interna", "COL-INT-001", demo.departments[0]},
		{demo.staff[1], "EMP-002", "Lucía", "Nadal", "lucia.nadal@hospital-miks.com", "+34600111002", "DOCTOR", "Cardiología", "COL-CAR-002", demo.departments[1]},
		{demo.staff[2], "EMP-003", "Pedro", "Salas", "pedro.salas@hospital-miks.com", "+34600111003", "DOCTOR", "Traumatología", "COL-TRM-003", demo.departments[2]},
		{demo.staff[3], "EMP-004", "Ana", "Mora", "ana.mora@hospital-miks.com", "+34600111004", "NURSE", "Hospitalización", "COL-NUR-004", demo.departments[0]},
		{demo.staff[4], "EMP-005", "Marta", "Gil", "marta.gil@hospital-miks.com", "+34600111005", "THERAPIST", "Fisioterapia", "COL-REB-005", demo.departments[2]},
		{demo.staff[5], "EMP-006", "Javier", "Ríos", "javier.rios@hospital-miks.com", "+34600111006", "ADMIN", "Admisión", "COL-ADM-006", demo.departments[0]},
		{demo.staff[6], "EMP-007", "Elena", "Pascual", "elena.pascual@hospital-miks.com", "+34600111007", "PHARMACIST", "Farmacia Clínica", "COL-PHA-007", demo.departments[3]},
		{demo.staff[7], "EMP-008", "Sofía", "Lara", "sofia.lara@hospital-miks.com", "+34600111008", "DOCTOR", "Anestesia", "COL-ANE-008", demo.departments[2]},
	}
	for _, row := range rows {
		execSQL(ctx, conn, `INSERT INTO staff (id, employee_code, first_name, last_name, email, phone, staff_type, specialty, license_number, department_id, hire_date, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, row.id, row.code, row.first, row.last, row.email, row.phone, row.kind, row.specialty, row.license, row.dept, seed.historicalFlowA, true, seed.now, seed.now)
	}
	commitDomain(ctx, conn)
	fmt.Println("✓ Staff seeded")
}

func seedIdentity(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	beginDomain(ctx, conn, "identity")
	execSQL(ctx, conn, `INSERT INTO users (id, staff_id, username, password_hash, role, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, demo.users[0], demo.staff[5], "admin", bcryptHash("admin123"), "ADMIN", true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO users (id, staff_id, username, password_hash, role, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, demo.users[1], demo.staff[0], "doctor", bcryptHash("doctor123"), "DOCTOR", true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO users (id, staff_id, username, password_hash, role, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, demo.users[2], demo.staff[3], "nurse", bcryptHash("nurse123"), "NURSE", true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO users (id, staff_id, username, password_hash, role, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, demo.users[3], demo.staff[6], "pharmacist", bcryptHash("pharma123"), "PHARMACIST", true, seed.now, seed.now)
	commitDomain(ctx, conn)
	fmt.Println("✓ Identity seeded")
}

func seedPatients(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	beginDomain(ctx, conn, "patients")
	rows := []struct{ id, mrn, first, last, dob, gender, nationalID, phone, email, address, city, postal, blood, allergies, emergencyName, emergencyPhone, insuranceID, insurer string }{
		{demo.patients[0], "MRN-000101", "Juan", "Pérez", seed.historicalFlowA, "MALE", "12345678A", "+34611100001", "juan.perez@email.com", "Calle Mayor 10", "Madrid", "28001", "A+", "Penicilina", "Laura Pérez", "+34622100001", "SEG-0001", "Sanitas"},
		{demo.patients[1], "MRN-000102", "María", "González", seed.historicalFlowB, "FEMALE", "87654321B", "+34611100002", "maria.gonzalez@email.com", "Avenida Europa 21", "Madrid", "28023", "O-", "", "Luis Gómez", "+34622100002", "SEG-0002", "Adeslas"},
		{demo.patients[2], "MRN-000103", "Antonio", "Ruiz", seed.recentFlow, "MALE", "11223344C", "+34611100003", "antonio.ruiz@email.com", "Paseo del Prado 8", "Madrid", "28014", "B+", "Latex", "Nuria Ruiz", "+34622100003", "SEG-0003", "Mapfre"},
		{demo.patients[3], "MRN-000104", "Laura", "Serrano", seed.recentFlow2, "FEMALE", "99887766D", "+34611100004", "laura.serrano@email.com", "Calle Alcalá 43", "Madrid", "28009", "AB+", "Ibuprofeno", "Mario Serrano", "+34622100004", "SEG-0004", "DKV"},
	}
	for _, row := range rows {
		execSQL(ctx, conn, `INSERT INTO patients (id, medical_record_no, first_name, last_name, date_of_birth, gender, national_id, phone, email, address, city, postal_code, blood_type, allergies, emergency_contact_name, emergency_contact_phone, insurance_id, insurance_company, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)`, row.id, row.mrn, row.first, row.last, row.dob, row.gender, row.nationalID, row.phone, row.email, row.address, row.city, row.postal, row.blood, row.allergies, row.emergencyName, row.emergencyPhone, row.insuranceID, row.insurer, true, seed.now, seed.now)
	}
	commitDomain(ctx, conn)
	fmt.Println("✓ Patients seeded")
}

func seedClinical(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	beginDomain(ctx, conn, "clinical")
	execSQL(ctx, conn, `INSERT INTO consultation_rooms (id, name, code, department_id, floor, building, equipment, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, demo.consultationRooms[0], "Consulta Interna 1", "CI-01", demo.departments[0], 2, "Edificio A", "ECG portátil, ecógrafo", true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO consultation_rooms (id, name, code, department_id, floor, building, equipment, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, demo.consultationRooms[1], "Consulta Cardio 2", "CC-02", demo.departments[1], 3, "Edificio A", "Holter, monitor tensión", true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO consultation_rooms (id, name, code, department_id, floor, building, equipment, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, demo.consultationRooms[2], "Consulta Trauma 3", "CT-03", demo.departments[2], 4, "Edificio B", "Camilla, arco de movilidad", true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO operating_rooms (id, name, code, floor, building, status, equipment, capacity, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, demo.operatingRooms[0], "Quirófano Central", "OR-01", 1, "Edificio B", "AVAILABLE", "Trauma, anestesia, RX intraoperatoria", 1, true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO operating_rooms (id, name, code, floor, building, status, equipment, capacity, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, demo.operatingRooms[1], "Quirófano Programado", "OR-02", 1, "Edificio B", "AVAILABLE", "Laparoscopia, anestesia general", 1, true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO wards (id, name, code, department_id, floor, building, total_beds, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, demo.wards[0], "Hospitalización General", "WARD-GEN", demo.departments[0], 2, "Edificio A", 18, true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO wards (id, name, code, department_id, floor, building, total_beds, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, demo.wards[1], "Trauma Postquirúrgico", "WARD-TRA", demo.departments[2], 4, "Edificio B", 12, true, seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO beds (id, ward_id, number, status, room_no, features, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, demo.beds[0], demo.wards[1], "401-A", "AVAILABLE", "401", "Oxígeno, monitorización", seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO beds (id, ward_id, number, status, room_no, features, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, demo.beds[1], demo.wards[0], "212-B", "OCCUPIED", "212", "Monitorización básica", seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO beds (id, ward_id, number, status, room_no, features, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, demo.beds[2], demo.wards[0], "215-A", "AVAILABLE", "215", "Aislamiento respiratorio", seed.now, seed.now)

	appointmentRows := []struct{ id, patientID, doctorID, deptID, apptType, status, scheduledAt, room, notes, diagnosis string; duration int }{
		{demo.appointments[0], demo.patients[0], demo.staff[2], demo.departments[2], "FOLLOW_UP", "COMPLETED", seed.recentFlow, demo.consultationRooms[2], "Revisión post cirugía rodilla", "Evolución satisfactoria", 30},
		{demo.appointments[1], demo.patients[0], demo.staff[1], demo.departments[1], "CONSULTATION", "COMPLETED", seed.recentFlow2, demo.consultationRooms[1], "Control de palpitaciones", "Arritmia leve en estudio", 40},
		{demo.appointments[2], demo.patients[0], demo.staff[1], demo.departments[1], "FOLLOW_UP", "SCHEDULED", seed.futureFlow, demo.consultationRooms[1], "Revisión Holter 72h", "", 30},
		{demo.appointments[3], demo.patients[1], demo.staff[0], demo.departments[0], "EMERGENCY", "COMPLETED", seed.currentFlow, demo.consultationRooms[0], "Ingreso por fiebre y disnea", "Neumonía comunitaria", 50},
		{demo.appointments[4], demo.patients[2], demo.staff[4], demo.departments[2], "REHABILITATION", "COMPLETED", seed.recentFlow3, demo.consultationRooms[2], "Valoración funcional inicial", "Limitación de movilidad hombro", 45},
		{demo.appointments[5], demo.patients[3], demo.staff[0], demo.departments[0], "CONSULTATION", "CONFIRMED", seed.futureFlow2, demo.consultationRooms[0], "Chequeo preventivo anual", "", 30},
	}
	for _, row := range appointmentRows {
		execSQL(ctx, conn, `INSERT INTO appointments (id, patient_id, doctor_id, department_id, type, status, scheduled_at, duration_minutes, room, notes, diagnosis, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, row.id, row.patientID, row.doctorID, row.deptID, row.apptType, row.status, row.scheduledAt, row.duration, row.room, row.notes, row.diagnosis, seed.now, seed.now)
	}

	execSQL(ctx, conn, `INSERT INTO admissions (id, patient_id, admitting_doctor_id, bed_id, department_id, status, admission_date, discharge_date, diagnosis, admission_reason, dietary_needs, notes, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, demo.admissions[0], demo.patients[0], demo.staff[2], demo.beds[0], demo.departments[2], "DISCHARGED", seed.historicalFlowA, seed.historicalFlowA3, "Rotura menisco interno", "Dolor y bloqueo articular", "Sin restricciones", "Alta sin incidencias", seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO admissions (id, patient_id, admitting_doctor_id, bed_id, department_id, status, admission_date, diagnosis, admission_reason, dietary_needs, notes, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, demo.admissions[1], demo.patients[1], demo.staff[0], demo.beds[1], demo.departments[0], "IN_CARE", seed.currentFlow, "Neumonía comunitaria", "Fiebre persistente y disnea", "Dieta blanda", "En observación con antibiótico IV", seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO admissions (id, patient_id, admitting_doctor_id, bed_id, department_id, status, admission_date, discharge_date, diagnosis, admission_reason, dietary_needs, notes, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, demo.admissions[2], demo.patients[2], demo.staff[2], demo.beds[2], demo.departments[2], "DISCHARGED", seed.recentFlow, seed.recentFlow2, "Tendinopatía hombro", "Dolor limitante tras sobreesfuerzo", "Sin restricciones", "Observación ambulatoria cerrada", seed.now, seed.now)

	mealRows := []struct{ id, admissionID, mealType, date, menu, dietaryNote string; delivered bool; deliveredAt any }{
		{demo.mealOrders[0], demo.admissions[1], "BREAKFAST", seed.currentFlow2, "Tostadas, infusión, yogur", "Dieta blanda", true, seed.currentFlow2},
		{demo.mealOrders[1], demo.admissions[1], "LUNCH", seed.currentFlow2, "Crema verduras, merluza, compota", "Hiposódica", true, seed.currentFlow2},
		{demo.mealOrders[2], demo.admissions[1], "DINNER", seed.currentFlow3, "Sopa, tortilla francesa, fruta", "Blanda", false, nil},
	}
	for _, row := range mealRows {
		execSQL(ctx, conn, `INSERT INTO meal_orders (id, admission_id, meal_type, date, menu, dietary_note, delivered, delivered_at, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, row.id, row.admissionID, row.mealType, row.date, row.menu, row.dietaryNote, row.delivered, row.deliveredAt, seed.now, seed.now)
	}

	careRows := []struct{ id, admissionID, staffID, noteType, content, createdAt string }{
		{demo.careNotes[0], demo.admissions[1], demo.staff[3], "OBSERVATION", "Paciente afebril desde la madrugada, mejor tolerancia oral.", seed.currentFlow2},
		{demo.careNotes[1], demo.admissions[1], demo.staff[0], "MEDICATION", "Se mantiene ceftriaxona IV y oxigenoterapia a bajo flujo.", seed.currentFlow3},
		{demo.careNotes[2], demo.admissions[0], demo.staff[3], "PROCEDURE", "Curas postquirúrgicas sin sangrado activo.", seed.historicalFlowA2},
	}
	for _, row := range careRows {
		execSQL(ctx, conn, `INSERT INTO care_notes (id, admission_id, staff_id, note_type, content, created_at) VALUES ($1,$2,$3,$4,$5,$6)`, row.id, row.admissionID, row.staffID, row.noteType, row.content, row.createdAt)
	}

	execSQL(ctx, conn, `INSERT INTO surgeries (id, patient_id, lead_surgeon_id, anesthetist_id, operating_room_id, procedure_name, procedure_code, status, scheduled_start, scheduled_end, actual_start, actual_end, pre_op_notes, post_op_notes, complications, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`, demo.surgeries[0], demo.patients[0], demo.staff[2], demo.staff[7], demo.operatingRooms[0], "Artroscopia de rodilla", "ORT-ART-01", "COMPLETED", seed.historicalFlowA2, seed.historicalFlowA2, seed.historicalFlowA2, seed.historicalFlowA2, "Ayuno correcto, consentimiento firmado", "Intervención sin incidencias", "", seed.now, seed.now)
	execSQL(ctx, conn, `INSERT INTO surgeries (id, patient_id, lead_surgeon_id, anesthetist_id, operating_room_id, procedure_name, procedure_code, status, scheduled_start, scheduled_end, pre_op_notes, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, demo.surgeries[1], demo.patients[1], demo.staff[2], demo.staff[7], demo.operatingRooms[1], "Broncoscopia diagnóstica", "PUL-BRO-02", "SCHEDULED", seed.futureFlow, seed.futureFlow, "Paciente pendiente de estabilización respiratoria", seed.now, seed.now)

	teamRows := []struct{ id, surgeryID, staffID, role, createdAt string }{
		{demo.surgeryTeamMembers[0], demo.surgeries[0], demo.staff[3], "SCRUB_NURSE", seed.historicalFlowA2},
		{demo.surgeryTeamMembers[1], demo.surgeries[0], demo.staff[7], "ANESTHETIST", seed.historicalFlowA2},
		{demo.surgeryTeamMembers[2], demo.surgeries[1], demo.staff[3], "CIRCULATING_NURSE", seed.futureFlow},
	}
	for _, row := range teamRows {
		execSQL(ctx, conn, `INSERT INTO surgery_team_members (id, surgery_id, staff_id, role, created_at) VALUES ($1,$2,$3,$4,$5)`, row.id, row.surgeryID, row.staffID, row.role, row.createdAt)
	}
	commitDomain(ctx, conn)
	fmt.Println("✓ Clinical seeded")
}

func seedPharmacy(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	beginDomain(ctx, conn, "pharmacy")
	medRows := []struct{ id, name, genericName, code, category, manufacturer, dosageForm, strength, unit string; stock, minStock int; price float64; requiresRx, controlled bool; expiration any }{
		{demo.medications[0], "Amoxicilina 500 mg", "Amoxicilina", "MED-AMOX-500", "ANTIBIOTIC", "MiKS Pharma", "CAPSULE", "500mg", "caps", 500, 50, 3.50, true, false, seed.futureFlow3},
		{demo.medications[1], "Ibuprofeno 600 mg", "Ibuprofeno", "MED-IBU-600", "ANTI_INFLAMMATORY", "MiKS Pharma", "TABLET", "600mg", "comp", 220, 40, 2.20, true, false, seed.futureFlow3},
		{demo.medications[2], "Bisoprolol 2.5 mg", "Bisoprolol", "MED-BISO-25", "CARDIOVASCULAR", "MiKS Pharma", "TABLET", "2.5mg", "comp", 180, 30, 6.80, true, false, seed.futureFlow3},
	}
	for _, row := range medRows {
		execSQL(ctx, conn, `INSERT INTO medications (id, name, generic_name, code, category, manufacturer, dosage_form, strength, unit, stock_quantity, min_stock, price, requires_rx, controlled, expiration_date, active, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`, row.id, row.name, row.genericName, row.code, row.category, row.manufacturer, row.dosageForm, row.strength, row.unit, row.stock, row.minStock, row.price, row.requiresRx, row.controlled, row.expiration, true, seed.now, seed.now)
	}
	prescriptionRows := []struct{ id, patientID, doctorID, medicationID, status, dosage, frequency, duration, instructions string; quantity, refills, refillsUsed int; prescribedAt string; dispensedAt any }{
		{demo.prescriptions[0], demo.patients[1], demo.staff[0], demo.medications[0], "ACTIVE", "1 cápsula", "Cada 8 horas", "7 días", "Tomar con comida", 21, 0, 0, seed.currentFlow, nil},
		{demo.prescriptions[1], demo.patients[0], demo.staff[1], demo.medications[2], "DISPENSED", "1 comprimido", "Cada 24 horas", "30 días", "Control de frecuencia cardíaca", 30, 1, 0, seed.recentFlow2, seed.recentFlow2},
		{demo.prescriptions[2], demo.patients[2], demo.staff[2], demo.medications[1], "DISPENSED", "1 comprimido", "Cada 12 horas", "10 días", "Dolor post lesión", 20, 0, 0, seed.recentFlow3, seed.recentFlow3},
	}
	for _, row := range prescriptionRows {
		execSQL(ctx, conn, `INSERT INTO prescriptions (id, patient_id, doctor_id, medication_id, status, dosage, frequency, duration, instructions, quantity, refills, refills_used, prescribed_at, dispensed_at, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, row.id, row.patientID, row.doctorID, row.medicationID, row.status, row.dosage, row.frequency, row.duration, row.instructions, row.quantity, row.refills, row.refillsUsed, row.prescribedAt, row.dispensedAt, seed.now, seed.now)
	}
	dispenseRows := []struct{ id, prescriptionID, pharmacistID string; quantity int; notes, dispensedAt string }{
		{demo.dispenses[0], demo.prescriptions[1], demo.staff[6], 30, "Entrega inicial ambulatoria", seed.recentFlow2},
		{demo.dispenses[1], demo.prescriptions[2], demo.staff[6], 20, "Entrega para tratamiento post lesión", seed.recentFlow3},
	}
	for _, row := range dispenseRows {
		execSQL(ctx, conn, `INSERT INTO pharmacy_dispenses (id, prescription_id, pharmacist_id, quantity, notes, dispensed_at, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7)`, row.id, row.prescriptionID, row.pharmacistID, row.quantity, row.notes, row.dispensedAt, seed.now)
	}
	commitDomain(ctx, conn)
	fmt.Println("✓ Pharmacy seeded")
}

func seedBilling(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	patientLSN := map[string]int64{
		demo.patients[0]: latestCommitLSN(ctx, conn, "patients", "patients", demo.patients[0]),
		demo.patients[1]: latestCommitLSN(ctx, conn, "patients", "patients", demo.patients[1]),
		demo.patients[2]: latestCommitLSN(ctx, conn, "patients", "patients", demo.patients[2]),
	}
	admissionLSN := map[string]int64{
		demo.admissions[0]: latestCommitLSN(ctx, conn, "clinical", "admissions", demo.admissions[0]),
		demo.admissions[1]: latestCommitLSN(ctx, conn, "clinical", "admissions", demo.admissions[1]),
		demo.admissions[2]: latestCommitLSN(ctx, conn, "clinical", "admissions", demo.admissions[2]),
	}
	beginDomain(ctx, conn, "billing")
	invoiceRows := []struct{ id, number, patientID, admissionID, status string; subtotal, tax, discount, total float64; currency string; issuedAt, dueDate, paidAt any; paymentMethod, notes string }{
		{demo.invoices[0], "INV-2023-0001", demo.patients[0], demo.admissions[0], "PAID", 5400, 1134, 0, 6534, "EUR", seed.historicalFlowA3, seed.historicalFlowB, seed.historicalFlowB, "CARD", "Episodio quirúrgico de rodilla"},
		{demo.invoices[1], "INV-2025-0042", demo.patients[0], demo.admissions[0], "ISSUED", 320, 67.2, 0, 387.2, "EUR", seed.recentFlow2, seed.recentFlow3, nil, "INSURANCE", "Seguimiento cardiológico"},
		{demo.invoices[2], "INV-2026-0015", demo.patients[1], demo.admissions[1], "DRAFT", 890, 186.9, 50, 1026.9, "EUR", seed.currentFlow2, seed.futureFlow, nil, "", "Ingreso actual en observación"},
		{demo.invoices[3], "INV-2025-0088", demo.patients[2], demo.admissions[2], "PAID", 420, 88.2, 20, 488.2, "EUR", seed.recentFlow3, seed.currentFlow, seed.currentFlow, "TRANSFER", "Rehabilitación y medicación"},
	}
	for _, row := range invoiceRows {
		execSQL(ctx, conn, `INSERT INTO invoices (id, invoice_number, patient_id, admission_id, status, subtotal, tax, discount, total, currency, issued_at, due_date, paid_at, payment_method, notes, created_at, updated_at, patient_lsn, admission_lsn) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`, row.id, row.number, row.patientID, row.admissionID, row.status, row.subtotal, row.tax, row.discount, row.total, row.currency, row.issuedAt, row.dueDate, row.paidAt, row.paymentMethod, row.notes, seed.now, seed.now, patientLSN[row.patientID], admissionLSN[row.admissionID])
	}
	itemRows := []struct{ id, invoiceID, description, category string; quantity int; unitPrice, total float64 }{
		{demo.invoiceItems[0], demo.invoices[0], "Artroscopia de rodilla", "SURGERY", 1, 4200, 4200},
		{demo.invoiceItems[1], demo.invoices[0], "Estancia postquirúrgica", "ROOM", 2, 600, 1200},
		{demo.invoiceItems[2], demo.invoices[1], "Consulta cardiología", "CONSULTATION", 1, 180, 180},
		{demo.invoiceItems[3], demo.invoices[1], "Holter 72h", "IMAGING", 1, 140, 140},
		{demo.invoiceItems[4], demo.invoices[2], "Ingreso observación respiratoria", "ROOM", 2, 280, 560},
		{demo.invoiceItems[5], demo.invoices[3], "Plan rehabilitación hombro", "REHAB", 1, 420, 420},
	}
	for _, row := range itemRows {
		execSQL(ctx, conn, `INSERT INTO invoice_items (id, invoice_id, description, category, quantity, unit_price, total, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, row.id, row.invoiceID, row.description, row.category, row.quantity, row.unitPrice, row.total, seed.now)
	}
	commitDomain(ctx, conn)
	fmt.Println("✓ Billing seeded")
}

func seedScheduling(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	beginDomain(ctx, conn, "scheduling")
	rows := []struct{ id, staffID, departmentID, shiftType, status, startTime, endTime, notes, swappedWith string }{
		{demo.guardShifts[0], demo.staff[3], demo.departments[0], "MORNING", "SCHEDULED", seed.currentFlow2, seed.currentFlow2, "Cobertura planta general", ""},
		{demo.guardShifts[1], demo.staff[0], demo.departments[0], "AFTERNOON", "CONFIRMED", seed.currentFlow3, seed.currentFlow3, "Pase de planta y altas", ""},
		{demo.guardShifts[2], demo.staff[2], demo.departments[2], "MORNING", "SCHEDULED", seed.futureFlow, seed.futureFlow, "Bloque quirúrgico programado", ""},
		{demo.guardShifts[3], demo.staff[7], demo.departments[2], "NIGHT", "SCHEDULED", seed.futureFlow2, seed.futureFlow2, "Soporte anestesia", ""},
	}
	for _, row := range rows {
		execSQL(ctx, conn, `INSERT INTO guard_shifts (id, staff_id, department_id, type, status, start_time, end_time, notes, swapped_with, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, row.id, row.staffID, row.departmentID, row.shiftType, row.status, row.startTime, row.endTime, row.notes, row.swappedWith, seed.now, seed.now)
	}
	commitDomain(ctx, conn)
	fmt.Println("✓ Scheduling seeded")
}

func seedRehab(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	beginDomain(ctx, conn, "rehab")
	planRows := []struct{ id, patientID, therapistID, doctorID, rehabType, diagnosis, goals, startDate string; endDate any; sessions, completed int; active bool; notes string }{
		{demo.rehabPlans[0], demo.patients[2], demo.staff[4], demo.staff[2], "PHYSIOTHERAPY", "Rigidez hombro derecho", "Recuperar movilidad y fuerza", seed.recentFlow, seed.currentFlow, 12, 8, true, "Buena adherencia al plan"},
		{demo.rehabPlans[1], demo.patients[0], demo.staff[4], demo.staff[2], "POST_SURGICAL", "Recuperación tras artroscopia", "Marcha normal y estabilidad", seed.historicalFlowA3, seed.historicalFlowB2, 10, 10, false, "Plan cerrado con objetivos cumplidos"},
	}
	for _, row := range planRows {
		execSQL(ctx, conn, `INSERT INTO rehab_plans (id, patient_id, therapist_id, doctor_id, type, diagnosis, goals, start_date, end_date, sessions, completed, active, notes, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, row.id, row.patientID, row.therapistID, row.doctorID, row.rehabType, row.diagnosis, row.goals, row.startDate, row.endDate, row.sessions, row.completed, row.active, row.notes, seed.now, seed.now)
	}
	sessionRows := []struct{ id, planID, therapistID, patientID, status, scheduledAt string; duration int; room, exercises, progress string; painLevel any; notes string }{
		{demo.rehabSessions[0], demo.rehabPlans[0], demo.staff[4], demo.patients[2], "COMPLETED", seed.recentFlow2, 45, "Sala Rehab 1", "Movilidad escapular, poleas", "Mejora de rango articular", 4, "Tolera bien ejercicio"},
		{demo.rehabSessions[1], demo.rehabPlans[0], demo.staff[4], demo.patients[2], "COMPLETED", seed.recentFlow3, 45, "Sala Rehab 1", "Fortalecimiento y movilidad", "Dolor moderado post sesión", 5, "Necesita continuidad"},
		{demo.rehabSessions[2], demo.rehabPlans[0], demo.staff[4], demo.patients[2], "SCHEDULED", seed.futureFlow, 45, "Sala Rehab 2", "Ejercicio funcional", "", nil, "Sesión pendiente"},
		{demo.rehabSessions[3], demo.rehabPlans[1], demo.staff[4], demo.patients[0], "COMPLETED", seed.historicalFlowB, 45, "Sala Rehab 3", "Reeducación de la marcha", "Alta de rehabilitación", 2, "Objetivos completados"},
	}
	for _, row := range sessionRows {
		execSQL(ctx, conn, `INSERT INTO rehab_sessions (id, plan_id, therapist_id, patient_id, status, scheduled_at, duration, room, exercises, progress, pain_level, notes, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, row.id, row.planID, row.therapistID, row.patientID, row.status, row.scheduledAt, row.duration, row.room, row.exercises, row.progress, row.painLevel, row.notes, seed.now, seed.now)
	}
	commitDomain(ctx, conn)
	fmt.Println("✓ Rehab seeded")
}

func seedMessaging(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	beginDomain(ctx, conn, "messaging")
	messageRows := []struct{ id, senderID, receiverID, subject, body, priority string; read bool; readAt, parentID any; createdAt string }{
		{demo.messages[0], demo.staff[0], demo.staff[3], "Paciente González en observación", "Revisar saturación y avisar si desciende de 93%.", "HIGH", false, nil, "", seed.currentFlow2},
		{demo.messages[1], demo.staff[2], demo.staff[4], "Plan de rehabilitación Ruiz", "Necesitamos continuidad 4 semanas más y valoración funcional semanal.", "NORMAL", true, seed.currentFlow, "", seed.recentFlow3},
		{demo.messages[2], demo.staff[6], demo.staff[0], "Interacción farmacológica", "Validar si mantenemos bisoprolol con la nueva pauta prescrita.", "URGENT", false, nil, "", seed.currentFlow3},
	}
	for _, row := range messageRows {
		execSQL(ctx, conn, `INSERT INTO messages (id, sender_id, receiver_id, subject, body, priority, read, read_at, parent_id, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, row.id, row.senderID, row.receiverID, row.subject, row.body, row.priority, row.read, row.readAt, row.parentID, row.createdAt)
	}
	patientCommRows := []struct{ id, patientID, staffID, channel, subject, content, status, sentAt string; deliveredAt any }{
		{demo.patientComms[0], demo.patients[0], demo.staff[5], "EMAIL", "Preparación consulta cardiología", "Recuerde acudir con el Holter y analítica previa.", "DELIVERED", seed.futureFlow, seed.futureFlow},
		{demo.patientComms[1], demo.patients[1], demo.staff[5], "SMS", "Recordatorio ingreso", "Su episodio sigue activo. Si empeora la disnea avise al control de planta.", "SENT", seed.currentFlow3, nil},
	}
	for _, row := range patientCommRows {
		execSQL(ctx, conn, `INSERT INTO patient_communications (id, patient_id, staff_id, channel, subject, content, status, sent_at, delivered_at, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, row.id, row.patientID, row.staffID, row.channel, row.subject, row.content, row.status, row.sentAt, row.deliveredAt, seed.now)
	}
	commitDomain(ctx, conn)
	fmt.Println("✓ Messaging seeded")
}

func seedDocuments(ctx context.Context, conn *pgx.Conn, seed seedTimes) {
	beginDomain(ctx, conn, "documents")
	docRows := []struct{ id, title, category, patientID, uploadedBy, fileName, mimeType string; size int; storagePath, checksum, tags, notes, createdAt string }{
		{demo.documents[0], "Informe quirúrgico rodilla", "SURGERY_REPORT", demo.patients[0], demo.staff[2], "informe_rodilla_juan.pdf", "application/pdf", 325000, "/docs/surgery/informe_rodilla_juan.pdf", "chk-juan-surgery-001", "cirugia,rodilla,alta", "Cierre de episodio quirúrgico histórico", seed.historicalFlowA3},
		{demo.documents[1], "Consentimiento informado broncoscopia", "CONSENT_FORM", demo.patients[1], demo.staff[0], "consent_bronco_maria.pdf", "application/pdf", 142000, "/docs/consent/consent_bronco_maria.pdf", "chk-maria-consent-002", "consentimiento,procedimiento", "Pendiente de firma final", seed.futureFlow},
		{demo.documents[2], "Alta de rehabilitación", "DISCHARGE_SUMMARY", demo.patients[0], demo.staff[4], "alta_rehab_juan.pdf", "application/pdf", 118000, "/docs/rehab/alta_rehab_juan.pdf", "chk-juan-rehab-003", "rehab,alta", "Cierre funcional post artroscopia", seed.historicalFlowB2},
		{demo.documents[3], "Analítica respiratoria", "LAB_RESULT", demo.patients[1], demo.staff[0], "analitica_respiratoria_maria.pdf", "application/pdf", 87000, "/docs/lab/analitica_respiratoria_maria.pdf", "chk-maria-lab-004", "laboratorio,respiratorio", "Analítica del ingreso activo", seed.currentFlow2},
		{demo.documents[4], "Plan terapéutico hombro", "MEDICAL_RECORD", demo.patients[2], demo.staff[4], "plan_hombro_antonio.pdf", "application/pdf", 96000, "/docs/rehab/plan_hombro_antonio.pdf", "chk-antonio-rehab-005", "rehab,hombro", "Plan vigente de fisioterapia", seed.recentFlow3},
	}
	for _, row := range docRows {
		execSQL(ctx, conn, `INSERT INTO documents (id, title, category, patient_id, uploaded_by, file_name, mime_type, size_bytes, storage_path, checksum, version, tags, notes, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, row.id, row.title, row.category, row.patientID, row.uploadedBy, row.fileName, row.mimeType, row.size, row.storagePath, row.checksum, 1, row.tags, row.notes, row.createdAt, seed.now)
	}
	accessRows := []struct{ id, documentID, staffID, action, ipAddress, accessedAt string }{
		{demo.documentAccesses[0], demo.documents[0], demo.staff[2], "VIEW", "10.0.0.21", seed.currentFlow},
		{demo.documentAccesses[1], demo.documents[3], demo.staff[3], "DOWNLOAD", "10.0.0.34", seed.currentFlow3},
		{demo.documentAccesses[2], demo.documents[4], demo.staff[4], "EDIT", "10.0.0.55", seed.futureFlow},
	}
	for _, row := range accessRows {
		execSQL(ctx, conn, `INSERT INTO document_access_log (id, document_id, staff_id, action, ip_address, accessed_at) VALUES ($1,$2,$3,$4,$5,$6)`, row.id, row.documentID, row.staffID, row.action, row.ipAddress, row.accessedAt)
	}
	commitDomain(ctx, conn)
	fmt.Println("✓ Documents seeded")
}

func beginDomain(ctx context.Context, conn *pgx.Conn, domain string) {
	execSQL(ctx, conn, fmt.Sprintf("BEGIN DOMAIN %s", domain))
}

func commitDomain(ctx context.Context, conn *pgx.Conn) {
	execSQL(ctx, conn, "COMMIT")
}

func deleteByIDs(ctx context.Context, conn *pgx.Conn, table string, ids []string) {
	if len(ids) == 0 {
		return
	}
	execSQL(ctx, conn, fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", table, quoteList(ids)))
}

func deleteWhereTextIn(ctx context.Context, conn *pgx.Conn, table, column string, values []string) {
	if len(values) == 0 {
		return
	}
	execSQL(ctx, conn, fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", table, column, quoteList(values)))
}

func execSQL(ctx context.Context, conn *pgx.Conn, sql string, args ...any) {
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		log.Fatalf("exec failed: %v\nSQL: %s\nARGS: %v", err, truncate(sql, 180), args)
	}
}

func latestCommitLSN(ctx context.Context, conn *pgx.Conn, domain, table, id string) int64 {
	beginDomain(ctx, conn, domain)
	defer commitDomain(ctx, conn)

	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id, __commit_lsn FROM %s FOR HISTORY", table))
	if err != nil {
		log.Fatalf("history query failed: %v", err)
	}
	defer rows.Close()

	var latest int64
	for rows.Next() {
		var rowID string
		var commitLSNRaw any
		if err := rows.Scan(&rowID, &commitLSNRaw); err != nil {
			log.Fatalf("history scan failed: %v", err)
		}
		commitLSN, ok := parseInt64(commitLSNRaw)
		if !ok {
			continue
		}
		if rowID == id && commitLSN > latest {
			latest = commitLSN
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("history rows failed: %v", err)
	}
	if latest == 0 {
		log.Fatalf("lsn not found for %s.%s id=%s", domain, table, id)
	}
	return latest
}

func bcryptHash(value string) string {
	hash, err := bcrypt.GenerateFromPassword([]byte(value), bcrypt.DefaultCost)
	if err != nil {
		log.Fatalf("bcrypt: %v", err)
	}
	return string(hash)
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func truncate(s string, n int) string {
	clean := strings.Join(strings.Fields(s), " ")
	if len(clean) > n {
		return clean[:n] + "…"
	}
	return clean
}

func parseInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case nil:
		return 0, false
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case string:
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return parsed, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func quoteList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, fmt.Sprintf("'%s'", strings.ReplaceAll(value, "'", "''")))
	}
	return strings.Join(quoted, ",")
}
