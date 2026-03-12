package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var identifierSanitizer = regexp.MustCompile(`[^a-zA-Z0-9]+`)

type patientRegistrationRequest struct {
	PatientID        string `json:"patient_id"`
	MedicalRecordNo  string `json:"medical_record_no"`
	FullName         string `json:"full_name"`
	DateOfBirth      string `json:"date_of_birth"`
	Sex              string `json:"sex"`
	NationalID       string `json:"national_id"`
	ConsentStatus    string `json:"consent_status"`
	RiskLevel        string `json:"risk_level"`
	ContactName      string `json:"contact_name"`
	ContactRelation  string `json:"contact_relation"`
	ContactPhone     string `json:"contact_phone"`
	ActorID          string `json:"actor_id"`
	Reason           string `json:"reason"`
	SignatureMeaning string `json:"signature_meaning"`
	RegisteredAt     string `json:"registered_at"`
}

type admissionRequest struct {
	EpisodeID          string `json:"episode_id"`
	PathwayCode        string `json:"pathway_code"`
	Origin             string `json:"origin"`
	AttendingClinician string `json:"attending_clinician"`
	EncounterType      string `json:"encounter_type"`
	EncounterStatus    string `json:"encounter_status"`
	EncounterStartedAt string `json:"encounter_started_at"`
	Acuity             string `json:"acuity"`
	Complaint          string `json:"complaint"`
	AssessedBy         string `json:"assessed_by"`
	Ward               string `json:"ward"`
	BedCode            string `json:"bed_code"`
	AssignedBy         string `json:"assigned_by"`
	RiskLevel          string `json:"risk_level"`
	ActorID            string `json:"actor_id"`
	Reason             string `json:"reason"`
}

type labOrderRequest struct {
	EpisodeID string `json:"episode_id"`
	OrderID   string `json:"order_id"`
	TestCode  string `json:"test_code"`
	OrderedBy string `json:"ordered_by"`
	Reason    string `json:"reason"`
	ActorID   string `json:"actor_id"`
	OrderedAt string `json:"ordered_at"`
}

type medicationOrderRequest struct {
	EpisodeID      string `json:"episode_id"`
	OrderID        string `json:"order_id"`
	MedicationCode string `json:"medication_code"`
	Dose           string `json:"dose"`
	Frequency      string `json:"frequency"`
	OrderedBy      string `json:"ordered_by"`
	DispensedBy    string `json:"dispensed_by"`
	Quantity       string `json:"quantity"`
	Reason         string `json:"reason"`
	ActorID        string `json:"actor_id"`
	OrderedAt      string `json:"ordered_at"`
}

type surgeryBookingRequest struct {
	EpisodeID     string `json:"episode_id"`
	BookingID     string `json:"booking_id"`
	ProcedureCode string `json:"procedure_code"`
	RoomCode      string `json:"room_code"`
	ScheduledAt   string `json:"scheduled_at"`
	SurgeonID     string `json:"surgeon_id"`
	Reason        string `json:"reason"`
	ActorID       string `json:"actor_id"`
}

type dischargeRequest struct {
	EpisodeID         string `json:"episode_id"`
	ChargeID          string `json:"charge_id"`
	ChargeType        string `json:"charge_type"`
	ChargeDescription string `json:"charge_description"`
	AmountCents       int    `json:"amount_cents"`
	DischargedAt      string `json:"discharged_at"`
	Reason            string `json:"reason"`
	ActorID           string `json:"actor_id"`
	RiskLevel         string `json:"risk_level"`
}

type mutationResponse struct {
	Status    string   `json:"status"`
	Domains   []string `json:"domains"`
	PatientID string   `json:"patient_id,omitempty"`
	EpisodeID string   `json:"episode_id,omitempty"`
	Artifact  string   `json:"artifact,omitempty"`
}

func (a *application) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.pool.Ping(ctx); err != nil {
		a.writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	a.writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (a *application) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	for _, plan := range schemaPlans() {
		if err := a.execASQLTx(ctx, plan.mode, plan.domains, plan.statements); err != nil {
			a.writeError(w, http.StatusBadRequest, fmt.Errorf("%s: %w", plan.name, err))
			return
		}
	}
	a.writeJSON(w, http.StatusCreated, map[string]any{
		"status":  "ready",
		"message": "hospital schema created",
	})
}

func (a *application) handleSeedDemo(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	register := patientRegistrationRequest{
		PatientID:        "patient-ana-lopez-001",
		MedicalRecordNo:  "MRN-2026-0001",
		FullName:         "Ana Lopez Garcia",
		DateOfBirth:      "1988-04-11",
		Sex:              "F",
		NationalID:       "ID-ES-ANA-0001",
		ConsentStatus:    "SIGNED",
		RiskLevel:        "ROUTINE",
		ContactName:      "Carlos Lopez",
		ContactRelation:  "BROTHER",
		ContactPhone:     "+34-600-100-100",
		ActorID:          "adm-001",
		Reason:           "Initial patient registration",
		SignatureMeaning: "ATTEST_PATIENT_IDENTITY",
		RegisteredAt:     "2026-03-01T08:00:00Z",
	}
	if err := a.registerPatient(ctx, register); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	admission := admissionRequest{
		EpisodeID:          "episode-ana-001",
		PathwayCode:        "EMERGENCY",
		Origin:             "ED",
		AttendingClinician: "clin-100",
		EncounterType:      "EMERGENCY",
		EncounterStatus:    "IN_PROGRESS",
		EncounterStartedAt: "2026-03-01T08:15:00Z",
		Acuity:             "ESI-2",
		Complaint:          "Chest pain and dyspnea",
		AssessedBy:         "nurse-200",
		Ward:               "WARD-A",
		BedCode:            "A-12",
		AssignedBy:         "bed-manager-1",
		RiskLevel:          "HIGH",
		ActorID:            "clin-100",
		Reason:             "Patient admitted through emergency pathway",
	}
	if err := a.admitPatient(ctx, register.PatientID, admission); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	lab := labOrderRequest{EpisodeID: "episode-ana-001", OrderID: "lab-ana-001", TestCode: "TROPONIN", OrderedBy: "clin-100", Reason: "Rule out acute coronary syndrome", ActorID: "clin-100", OrderedAt: "2026-03-01T09:00:00Z"}
	if err := a.createLabOrder(ctx, register.PatientID, lab); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	med := medicationOrderRequest{EpisodeID: "episode-ana-001", OrderID: "med-ana-001", MedicationCode: "ASPIRIN", Dose: "100mg", Frequency: "ONCE_DAILY", OrderedBy: "clin-100", DispensedBy: "pharm-010", Quantity: "30 tablets", Reason: "Initial anti-platelet therapy", ActorID: "clin-100", OrderedAt: "2026-03-01T09:10:00Z"}
	if err := a.createMedicationOrder(ctx, register.PatientID, med); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	surgery := surgeryBookingRequest{EpisodeID: "episode-ana-001", BookingID: "surg-ana-001", ProcedureCode: "ANGIOGRAM", RoomCode: "OR-3", ScheduledAt: "2026-03-02T10:00:00Z", SurgeonID: "surgeon-501", Reason: "Interventional cardiology follow-up", ActorID: "surgeon-501"}
	if err := a.bookSurgery(ctx, register.PatientID, surgery); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	discharge := dischargeRequest{EpisodeID: "episode-ana-001", ChargeID: "charge-ana-001", ChargeType: "EPISODE_CLOSE", ChargeDescription: "Emergency + cardiology pathway closeout", AmountCents: 245000, DischargedAt: "2026-03-05T16:00:00Z", Reason: "Patient stabilized and discharged with follow-up", ActorID: "clin-100", RiskLevel: "FOLLOW_UP"}
	if err := a.dischargePatient(ctx, register.PatientID, discharge); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	response := mutationResponse{Status: "seeded", PatientID: register.PatientID, EpisodeID: admission.EpisodeID, Artifact: "demo"}
	a.writeJSON(w, http.StatusCreated, response)
}

func (a *application) handleRegisterPatient(w http.ResponseWriter, r *http.Request) {
	var req patientRegistrationRequest
	if err := decodeJSON(r, &req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := a.registerPatient(ctx, req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	a.writeJSON(w, http.StatusCreated, mutationResponse{Status: "registered", Domains: []string{"patients", "compliance"}, PatientID: req.PatientID, Artifact: "patient_registration"})
}

func (a *application) handleAdmitPatient(w http.ResponseWriter, r *http.Request) {
	patientID := strings.TrimSpace(r.PathValue("patientID"))
	var req admissionRequest
	if err := decodeJSON(r, &req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := a.admitPatient(ctx, patientID, req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	a.writeJSON(w, http.StatusCreated, mutationResponse{Status: "admitted", Domains: []string{"patients", "careflow", "clinical", "operations", "compliance"}, PatientID: patientID, EpisodeID: req.EpisodeID, Artifact: "admission"})
}

func (a *application) handleCreateLabOrder(w http.ResponseWriter, r *http.Request) {
	patientID := strings.TrimSpace(r.PathValue("patientID"))
	var req labOrderRequest
	if err := decodeJSON(r, &req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := a.createLabOrder(ctx, patientID, req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	a.writeJSON(w, http.StatusCreated, mutationResponse{Status: "lab_ordered", Domains: []string{"patients", "careflow", "orders", "compliance"}, PatientID: patientID, EpisodeID: req.EpisodeID, Artifact: req.OrderID})
}

func (a *application) handleCreateMedicationOrder(w http.ResponseWriter, r *http.Request) {
	patientID := strings.TrimSpace(r.PathValue("patientID"))
	var req medicationOrderRequest
	if err := decodeJSON(r, &req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := a.createMedicationOrder(ctx, patientID, req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	a.writeJSON(w, http.StatusCreated, mutationResponse{Status: "medication_ordered", Domains: []string{"patients", "careflow", "orders", "pharmacy", "compliance"}, PatientID: patientID, EpisodeID: req.EpisodeID, Artifact: req.OrderID})
}

func (a *application) handleBookSurgery(w http.ResponseWriter, r *http.Request) {
	patientID := strings.TrimSpace(r.PathValue("patientID"))
	var req surgeryBookingRequest
	if err := decodeJSON(r, &req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := a.bookSurgery(ctx, patientID, req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	a.writeJSON(w, http.StatusCreated, mutationResponse{Status: "surgery_booked", Domains: []string{"patients", "careflow", "operations", "compliance"}, PatientID: patientID, EpisodeID: req.EpisodeID, Artifact: req.BookingID})
}

func (a *application) handleDischargePatient(w http.ResponseWriter, r *http.Request) {
	patientID := strings.TrimSpace(r.PathValue("patientID"))
	var req dischargeRequest
	if err := decodeJSON(r, &req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := a.dischargePatient(ctx, patientID, req); err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	a.writeJSON(w, http.StatusCreated, mutationResponse{Status: "discharged", Domains: []string{"patients", "careflow", "billing", "compliance"}, PatientID: patientID, EpisodeID: req.EpisodeID, Artifact: req.ChargeID})
}

func (a *application) handlePatientSnapshot(w http.ResponseWriter, r *http.Request) {
	patientID := strings.TrimSpace(r.PathValue("patientID"))
	lsn, err := parseOptionalLSN(r.URL.Query().Get("lsn"))
	if err != nil {
		a.writeError(w, http.StatusBadRequest, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	currentLSN, _, err := a.queryScalarInt64(ctx, "SELECT current_lsn() AS value")
	if err != nil {
		a.writeError(w, http.StatusBadGateway, err)
		return
	}
	patientVersion, _, err := a.queryScalarInt64(ctx, "SELECT entity_version('patients', 'patient_entity', $1) AS value", patientID)
	if err != nil {
		a.writeError(w, http.StatusBadGateway, err)
		return
	}
	patientRowLSN, _, err := a.queryScalarInt64(ctx, "SELECT row_lsn('patients.patients', $1) AS value", patientID)
	if err != nil {
		a.writeError(w, http.StatusBadGateway, err)
		return
	}
	response := map[string]any{"patient_id": patientID, "current_lsn": currentLSN, "as_of_lsn": lsn, "patient_version": patientVersion, "patient_row_lsn": patientRowLSN}
	queries := []struct{ key, query string }{
		{"patient", fmt.Sprintf("SELECT id, medical_record_no, full_name, dob, sex, national_id, consent_status, risk_level FROM patients.patients%s WHERE id = $1", asOfClause(lsn))},
		{"contacts", fmt.Sprintf("SELECT id, patient_id, contact_name, relation, phone FROM patients.patient_contacts%s WHERE patient_id = $1 ORDER BY id ASC", asOfClause(lsn))},
		{"episodes", fmt.Sprintf("SELECT id, patient_id, pathway_code, status, origin, attending_clinician FROM careflow.episodes%s WHERE patient_id = $1 ORDER BY id ASC", asOfClause(lsn))},
		{"encounters", fmt.Sprintf("SELECT id, episode_id, patient_id, encounter_type, status, started_at, clinician_id FROM careflow.encounters%s WHERE patient_id = $1 ORDER BY episode_id ASC, id ASC", asOfClause(lsn))},
		{"triage", fmt.Sprintf("SELECT id, episode_id, patient_id, acuity, complaint, assessed_by, status FROM clinical.triage_assessments%s WHERE patient_id = $1 ORDER BY episode_id ASC, id ASC", asOfClause(lsn))},
		{"lab_orders", fmt.Sprintf("SELECT id, episode_id, patient_id, status, test_code, ordered_by, clinical_reason, ordered_at FROM orders.lab_orders%s WHERE patient_id = $1 ORDER BY episode_id ASC, id ASC", asOfClause(lsn))},
		{"medication_orders", fmt.Sprintf("SELECT id, episode_id, patient_id, status, medication_code, dose, frequency, ordered_by, ordered_at FROM orders.medication_orders%s WHERE patient_id = $1 ORDER BY episode_id ASC, id ASC", asOfClause(lsn))},
		{"dispensations", fmt.Sprintf("SELECT id, episode_id, patient_id, medication_order_id, dispensed_by, quantity, status FROM pharmacy.dispensations%s WHERE patient_id = $1 ORDER BY episode_id ASC, id ASC", asOfClause(lsn))},
		{"bed_allocations", fmt.Sprintf("SELECT id, episode_id, patient_id, ward, bed_code, status, assigned_by FROM operations.bed_allocations%s WHERE patient_id = $1 ORDER BY episode_id ASC, id ASC", asOfClause(lsn))},
		{"surgery_bookings", fmt.Sprintf("SELECT id, episode_id, patient_id, procedure_code, room_code, scheduled_at, surgeon_id, status FROM operations.surgery_bookings%s WHERE patient_id = $1 ORDER BY episode_id ASC, id ASC", asOfClause(lsn))},
		{"charges", fmt.Sprintf("SELECT id, episode_id, patient_id, charge_type, description, amount_cents, status FROM billing.charges%s WHERE patient_id = $1 ORDER BY episode_id ASC, id ASC", asOfClause(lsn))},
		{"signatures", fmt.Sprintf("SELECT id, episode_id, patient_id, artifact_type, artifact_id, signer_id, meaning, reason, signed_at FROM compliance.signatures%s WHERE patient_id = $1 ORDER BY episode_id ASC, id ASC", asOfClause(lsn))},
		{"audit_events", fmt.Sprintf("SELECT id, episode_id, patient_id, event_type, actor_id, artifact_type, artifact_id, reason, occurred_at, payload_json FROM compliance.audit_events%s WHERE patient_id = $1 ORDER BY occurred_at ASC, id ASC", asOfClause(lsn))},
	}
	for _, item := range queries {
		rows, queryErr := a.queryRows(ctx, item.query, patientID)
		if queryErr != nil {
			a.writeError(w, http.StatusBadGateway, queryErr)
			return
		}
		response[item.key] = rows
	}
	a.writeJSON(w, http.StatusOK, response)
}

func (a *application) handlePatientHistory(w http.ResponseWriter, r *http.Request) {
	patientID := strings.TrimSpace(r.PathValue("patientID"))
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	queries := map[string]string{
		"patient_history":      "SELECT * FROM patients.patients FOR HISTORY WHERE id = $1",
		"episode_history":      "SELECT * FROM careflow.episodes FOR HISTORY WHERE patient_id = $1",
		"encounter_history":    "SELECT * FROM careflow.encounters FOR HISTORY WHERE patient_id = $1",
		"triage_history":       "SELECT * FROM clinical.triage_assessments FOR HISTORY WHERE patient_id = $1",
		"lab_order_history":    "SELECT * FROM orders.lab_orders FOR HISTORY WHERE patient_id = $1",
		"medication_history":   "SELECT * FROM orders.medication_orders FOR HISTORY WHERE patient_id = $1",
		"dispensation_history": "SELECT * FROM pharmacy.dispensations FOR HISTORY WHERE patient_id = $1",
		"surgery_history":      "SELECT * FROM operations.surgery_bookings FOR HISTORY WHERE patient_id = $1",
		"charge_history":       "SELECT * FROM billing.charges FOR HISTORY WHERE patient_id = $1",
		"audit_history":        "SELECT * FROM compliance.audit_events FOR HISTORY WHERE patient_id = $1",
	}
	response := map[string]any{"patient_id": patientID}
	for key, query := range queries {
		rows, err := a.queryRows(ctx, query, patientID)
		if err != nil {
			a.writeError(w, http.StatusBadGateway, err)
			return
		}
		response[key] = rows
	}
	a.writeJSON(w, http.StatusOK, response)
}

func (a *application) handlePatientAudit(w http.ResponseWriter, r *http.Request) {
	patientID := strings.TrimSpace(r.PathValue("patientID"))
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	currentLSN, _, err := a.queryScalarInt64(ctx, "SELECT current_lsn() AS value")
	if err != nil {
		a.writeError(w, http.StatusBadGateway, err)
		return
	}
	patientVersion, _, err := a.queryScalarInt64(ctx, "SELECT entity_version('patients', 'patient_entity', $1) AS value", patientID)
	if err != nil {
		a.writeError(w, http.StatusBadGateway, err)
		return
	}
	versionLSN, _, err := a.queryScalarInt64(ctx, "SELECT entity_version_lsn('patients', 'patient_entity', $1, $2) AS value", patientID, patientVersion)
	if err != nil {
		a.writeError(w, http.StatusBadGateway, err)
		return
	}
	resolvedReference, _, err := a.queryScalarInt64(ctx, "SELECT resolve_reference('patients.patients', $1) AS value", patientID)
	if err != nil {
		a.writeError(w, http.StatusBadGateway, err)
		return
	}
	currentEvents, err := a.queryRows(ctx, "SELECT id, episode_id, event_type, actor_id, artifact_type, artifact_id, reason, occurred_at, payload_json FROM compliance.audit_events WHERE patient_id = $1 ORDER BY occurred_at ASC, id ASC", patientID)
	if err != nil {
		a.writeError(w, http.StatusBadGateway, err)
		return
	}
	history, err := a.queryRows(ctx, "SELECT * FROM compliance.audit_events FOR HISTORY WHERE patient_id = $1", patientID)
	if err != nil {
		a.writeError(w, http.StatusBadGateway, err)
		return
	}
	a.writeJSON(w, http.StatusOK, map[string]any{"patient_id": patientID, "current_lsn": currentLSN, "patient_version": patientVersion, "patient_version_lsn": versionLSN, "resolved_reference": resolvedReference, "audit_events": currentEvents, "audit_history": history})
}

func (a *application) registerPatient(ctx context.Context, req patientRegistrationRequest) error {
	req.PatientID = strings.TrimSpace(req.PatientID)
	req.MedicalRecordNo = strings.TrimSpace(req.MedicalRecordNo)
	req.FullName = strings.TrimSpace(req.FullName)
	req.ActorID = strings.TrimSpace(req.ActorID)
	req.Reason = strings.TrimSpace(req.Reason)
	if req.PatientID == "" || req.MedicalRecordNo == "" || req.FullName == "" || req.ActorID == "" || req.Reason == "" {
		return errors.New("patient_id, medical_record_no, full_name, actor_id and reason are required")
	}
	req.ConsentStatus = defaultString(req.ConsentStatus, "SIGNED")
	req.RiskLevel = defaultString(req.RiskLevel, "ROUTINE")
	req.SignatureMeaning = defaultString(req.SignatureMeaning, "ATTEST_PATIENT_IDENTITY")
	req.RegisteredAt = defaultTimestamp(req.RegisteredAt)
	statements := []sqlStatement{{query: "INSERT INTO patients.patients (id, medical_record_no, full_name, dob, sex, national_id, consent_status, risk_level) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", args: []any{req.PatientID, req.MedicalRecordNo, req.FullName, req.DateOfBirth, req.Sex, req.NationalID, req.ConsentStatus, req.RiskLevel}}}
	if req.ContactName != "" || req.ContactPhone != "" {
		statements = append(statements, sqlStatement{query: "INSERT INTO patients.patient_contacts (id, patient_id, contact_name, relation, phone) VALUES ($1, $2, $3, $4, $5)", args: []any{prefixedID("contact", req.PatientID), req.PatientID, req.ContactName, req.ContactRelation, req.ContactPhone}})
	}
	statements = append(statements,
		sqlStatement{query: "INSERT INTO compliance.signatures (id, patient_id, artifact_type, artifact_id, signer_id, meaning, reason, signed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", args: []any{prefixedID("sig-register", req.PatientID), req.PatientID, "patient_registration", req.PatientID, req.ActorID, req.SignatureMeaning, req.Reason, req.RegisteredAt}},
		sqlStatement{query: "INSERT INTO compliance.audit_events (id, patient_id, event_type, actor_id, reason, artifact_type, artifact_id, occurred_at, payload_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", args: []any{prefixedID("audit-register", req.PatientID), req.PatientID, "PATIENT_REGISTERED", req.ActorID, req.Reason, "patient", req.PatientID, req.RegisteredAt, mustJSON(req)}},
	)
	return a.execASQLTx(ctx, "cross", []string{"patients", "compliance"}, statements)
}

func (a *application) admitPatient(ctx context.Context, patientID string, req admissionRequest) error {
	patientID = strings.TrimSpace(patientID)
	req.EpisodeID = defaultString(req.EpisodeID, prefixedID("episode", patientID))
	req.PathwayCode = defaultString(req.PathwayCode, "GENERAL")
	req.Origin = defaultString(req.Origin, "OUTPATIENT")
	req.AttendingClinician = strings.TrimSpace(req.AttendingClinician)
	req.EncounterType = defaultString(req.EncounterType, "CONSULT")
	req.EncounterStatus = defaultString(req.EncounterStatus, "IN_PROGRESS")
	req.EncounterStartedAt = defaultTimestamp(req.EncounterStartedAt)
	req.Acuity = defaultString(req.Acuity, "ROUTINE")
	req.Complaint = defaultString(req.Complaint, "Clinical intake")
	req.AssessedBy = defaultString(req.AssessedBy, req.ActorID)
	req.Ward = defaultString(req.Ward, "OBS")
	req.BedCode = defaultString(req.BedCode, "OBS-01")
	req.AssignedBy = defaultString(req.AssignedBy, req.ActorID)
	req.RiskLevel = defaultString(req.RiskLevel, "HIGH")
	req.ActorID = strings.TrimSpace(req.ActorID)
	req.Reason = strings.TrimSpace(req.Reason)
	if patientID == "" || req.AttendingClinician == "" || req.ActorID == "" || req.Reason == "" {
		return errors.New("patient id, attending_clinician, actor_id and reason are required")
	}
	encounterID := prefixedID("encounter", req.EpisodeID)
	triageID := prefixedID("triage", req.EpisodeID)
	bedID := prefixedID("bed", req.EpisodeID)
	statements := []sqlStatement{
		{query: "UPDATE patients.patients SET risk_level = $2 WHERE id = $1", args: []any{patientID, req.RiskLevel}},
		{query: "INSERT INTO careflow.episodes (id, patient_id, pathway_code, status, origin, attending_clinician) VALUES ($1, $2, $3, $4, $5, $6)", args: []any{req.EpisodeID, patientID, req.PathwayCode, "OPEN", req.Origin, req.AttendingClinician}},
		{query: "INSERT INTO careflow.encounters (id, episode_id, patient_id, encounter_type, status, started_at, clinician_id) VALUES ($1, $2, $3, $4, $5, $6, $7)", args: []any{encounterID, req.EpisodeID, patientID, req.EncounterType, req.EncounterStatus, req.EncounterStartedAt, req.AttendingClinician}},
		{query: "INSERT INTO clinical.triage_assessments (id, episode_id, patient_id, acuity, complaint, assessed_by, status) VALUES ($1, $2, $3, $4, $5, $6, $7)", args: []any{triageID, req.EpisodeID, patientID, req.Acuity, req.Complaint, req.AssessedBy, "DONE"}},
		{query: "INSERT INTO operations.bed_allocations (id, episode_id, patient_id, ward, bed_code, status, assigned_by) VALUES ($1, $2, $3, $4, $5, $6, $7)", args: []any{bedID, req.EpisodeID, patientID, req.Ward, req.BedCode, "ASSIGNED", req.AssignedBy}},
		{query: "INSERT INTO compliance.signatures (id, patient_id, episode_id, artifact_type, artifact_id, signer_id, meaning, reason, signed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", args: []any{prefixedID("sig-admit", req.EpisodeID), patientID, req.EpisodeID, "admission", req.EpisodeID, req.ActorID, "ATTEST_ADMISSION", req.Reason, req.EncounterStartedAt}},
		{query: "INSERT INTO compliance.audit_events (id, patient_id, episode_id, event_type, actor_id, reason, artifact_type, artifact_id, occurred_at, payload_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", args: []any{prefixedID("audit-admit", req.EpisodeID), patientID, req.EpisodeID, "PATIENT_ADMITTED", req.ActorID, req.Reason, "episode", req.EpisodeID, req.EncounterStartedAt, mustJSON(req)}},
	}
	return a.execASQLTx(ctx, "cross", []string{"patients", "careflow", "clinical", "operations", "compliance"}, statements)
}

func (a *application) createLabOrder(ctx context.Context, patientID string, req labOrderRequest) error {
	patientID = strings.TrimSpace(patientID)
	req.EpisodeID = strings.TrimSpace(req.EpisodeID)
	req.OrderID = defaultString(req.OrderID, prefixedID("lab", req.EpisodeID+"-"+req.TestCode))
	req.TestCode = strings.TrimSpace(req.TestCode)
	req.OrderedBy = strings.TrimSpace(req.OrderedBy)
	req.Reason = strings.TrimSpace(req.Reason)
	req.ActorID = strings.TrimSpace(req.ActorID)
	req.OrderedAt = defaultTimestamp(req.OrderedAt)
	if patientID == "" || req.EpisodeID == "" || req.TestCode == "" || req.OrderedBy == "" || req.ActorID == "" || req.Reason == "" {
		return errors.New("patient id, episode_id, test_code, ordered_by, actor_id and reason are required")
	}
	statements := []sqlStatement{
		{query: "INSERT INTO orders.lab_orders (id, episode_id, patient_id, status, test_code, ordered_by, clinical_reason, ordered_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", args: []any{req.OrderID, req.EpisodeID, patientID, "REQUESTED", req.TestCode, req.OrderedBy, req.Reason, req.OrderedAt}},
		{query: "INSERT INTO compliance.audit_events (id, patient_id, episode_id, event_type, actor_id, reason, artifact_type, artifact_id, occurred_at, payload_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", args: []any{prefixedID("audit-lab", req.OrderID), patientID, req.EpisodeID, "LAB_ORDERED", req.ActorID, req.Reason, "lab_order", req.OrderID, req.OrderedAt, mustJSON(req)}},
	}
	return a.execASQLTx(ctx, "cross", []string{"patients", "careflow", "orders", "compliance"}, statements)
}

func (a *application) createMedicationOrder(ctx context.Context, patientID string, req medicationOrderRequest) error {
	patientID = strings.TrimSpace(patientID)
	req.EpisodeID = strings.TrimSpace(req.EpisodeID)
	req.OrderID = defaultString(req.OrderID, prefixedID("med", req.EpisodeID+"-"+req.MedicationCode))
	req.MedicationCode = strings.TrimSpace(req.MedicationCode)
	req.Dose = defaultString(req.Dose, "1 unit")
	req.Frequency = defaultString(req.Frequency, "ONCE")
	req.OrderedBy = strings.TrimSpace(req.OrderedBy)
	req.DispensedBy = defaultString(req.DispensedBy, req.OrderedBy)
	req.Quantity = defaultString(req.Quantity, "1")
	req.Reason = strings.TrimSpace(req.Reason)
	req.ActorID = strings.TrimSpace(req.ActorID)
	req.OrderedAt = defaultTimestamp(req.OrderedAt)
	if patientID == "" || req.EpisodeID == "" || req.MedicationCode == "" || req.OrderedBy == "" || req.ActorID == "" || req.Reason == "" {
		return errors.New("patient id, episode_id, medication_code, ordered_by, actor_id and reason are required")
	}
	statements := []sqlStatement{
		{query: "INSERT INTO orders.medication_orders (id, episode_id, patient_id, status, medication_code, dose, frequency, ordered_by, ordered_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", args: []any{req.OrderID, req.EpisodeID, patientID, "ORDERED", req.MedicationCode, req.Dose, req.Frequency, req.OrderedBy, req.OrderedAt}},
		{query: "INSERT INTO pharmacy.dispensations (id, medication_order_id, episode_id, patient_id, dispensed_by, quantity, status) VALUES ($1, $2, $3, $4, $5, $6, $7)", args: []any{prefixedID("dispense", req.OrderID), req.OrderID, req.EpisodeID, patientID, req.DispensedBy, req.Quantity, "DISPENSED"}},
		{query: "INSERT INTO compliance.signatures (id, patient_id, episode_id, artifact_type, artifact_id, signer_id, meaning, reason, signed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", args: []any{prefixedID("sig-med", req.OrderID), patientID, req.EpisodeID, "medication_order", req.OrderID, req.ActorID, "AUTHORIZE_MEDICATION", req.Reason, req.OrderedAt}},
		{query: "INSERT INTO compliance.audit_events (id, patient_id, episode_id, event_type, actor_id, reason, artifact_type, artifact_id, occurred_at, payload_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", args: []any{prefixedID("audit-med", req.OrderID), patientID, req.EpisodeID, "MEDICATION_ORDERED", req.ActorID, req.Reason, "medication_order", req.OrderID, req.OrderedAt, mustJSON(req)}},
	}
	return a.execASQLTx(ctx, "cross", []string{"patients", "careflow", "orders", "pharmacy", "compliance"}, statements)
}

func (a *application) bookSurgery(ctx context.Context, patientID string, req surgeryBookingRequest) error {
	patientID = strings.TrimSpace(patientID)
	req.EpisodeID = strings.TrimSpace(req.EpisodeID)
	req.BookingID = defaultString(req.BookingID, prefixedID("surgery", req.EpisodeID+"-"+req.ProcedureCode))
	req.ProcedureCode = strings.TrimSpace(req.ProcedureCode)
	req.RoomCode = defaultString(req.RoomCode, "OR-1")
	req.ScheduledAt = defaultTimestamp(req.ScheduledAt)
	req.SurgeonID = strings.TrimSpace(req.SurgeonID)
	req.Reason = strings.TrimSpace(req.Reason)
	req.ActorID = strings.TrimSpace(req.ActorID)
	if patientID == "" || req.EpisodeID == "" || req.ProcedureCode == "" || req.SurgeonID == "" || req.ActorID == "" || req.Reason == "" {
		return errors.New("patient id, episode_id, procedure_code, surgeon_id, actor_id and reason are required")
	}
	statements := []sqlStatement{
		{query: "INSERT INTO operations.surgery_bookings (id, episode_id, patient_id, procedure_code, room_code, scheduled_at, surgeon_id, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", args: []any{req.BookingID, req.EpisodeID, patientID, req.ProcedureCode, req.RoomCode, req.ScheduledAt, req.SurgeonID, "PLANNED"}},
		{query: "INSERT INTO compliance.signatures (id, patient_id, episode_id, artifact_type, artifact_id, signer_id, meaning, reason, signed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", args: []any{prefixedID("sig-surgery", req.BookingID), patientID, req.EpisodeID, "surgery_booking", req.BookingID, req.ActorID, "AUTHORIZE_SURGERY", req.Reason, req.ScheduledAt}},
		{query: "INSERT INTO compliance.audit_events (id, patient_id, episode_id, event_type, actor_id, reason, artifact_type, artifact_id, occurred_at, payload_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", args: []any{prefixedID("audit-surgery", req.BookingID), patientID, req.EpisodeID, "SURGERY_BOOKED", req.ActorID, req.Reason, "surgery_booking", req.BookingID, req.ScheduledAt, mustJSON(req)}},
	}
	return a.execASQLTx(ctx, "cross", []string{"patients", "careflow", "operations", "compliance"}, statements)
}

func (a *application) dischargePatient(ctx context.Context, patientID string, req dischargeRequest) error {
	patientID = strings.TrimSpace(patientID)
	req.EpisodeID = strings.TrimSpace(req.EpisodeID)
	req.ChargeID = defaultString(req.ChargeID, prefixedID("charge", req.EpisodeID))
	req.ChargeType = defaultString(req.ChargeType, "DISCHARGE")
	req.ChargeDescription = defaultString(req.ChargeDescription, "Episode discharge")
	req.DischargedAt = defaultTimestamp(req.DischargedAt)
	req.Reason = strings.TrimSpace(req.Reason)
	req.ActorID = strings.TrimSpace(req.ActorID)
	req.RiskLevel = defaultString(req.RiskLevel, "FOLLOW_UP")
	if patientID == "" || req.EpisodeID == "" || req.ActorID == "" || req.Reason == "" || req.AmountCents <= 0 {
		return errors.New("patient id, episode_id, actor_id, reason and positive amount_cents are required")
	}
	statements := []sqlStatement{
		{query: "UPDATE patients.patients SET risk_level = $2 WHERE id = $1", args: []any{patientID, req.RiskLevel}},
		{query: "UPDATE careflow.episodes SET status = $2 WHERE id = $1", args: []any{req.EpisodeID, "CLOSED"}},
		{query: "UPDATE careflow.encounters SET status = $2 WHERE episode_id = $1", args: []any{req.EpisodeID, "CLOSED"}},
		{query: "INSERT INTO billing.charges (id, episode_id, patient_id, charge_type, description, amount_cents, status) VALUES ($1, $2, $3, $4, $5, $6, $7)", args: []any{req.ChargeID, req.EpisodeID, patientID, req.ChargeType, req.ChargeDescription, req.AmountCents, "POSTED"}},
		{query: "INSERT INTO compliance.signatures (id, patient_id, episode_id, artifact_type, artifact_id, signer_id, meaning, reason, signed_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", args: []any{prefixedID("sig-discharge", req.EpisodeID), patientID, req.EpisodeID, "discharge", req.EpisodeID, req.ActorID, "ATTEST_DISCHARGE", req.Reason, req.DischargedAt}},
		{query: "INSERT INTO compliance.audit_events (id, patient_id, episode_id, event_type, actor_id, reason, artifact_type, artifact_id, occurred_at, payload_json) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", args: []any{prefixedID("audit-discharge", req.EpisodeID), patientID, req.EpisodeID, "PATIENT_DISCHARGED", req.ActorID, req.Reason, "episode", req.EpisodeID, req.DischargedAt, mustJSON(req)}},
	}
	return a.execASQLTx(ctx, "cross", []string{"patients", "careflow", "billing", "compliance"}, statements)
}

func (a *application) execASQLTx(ctx context.Context, mode string, domains []string, statements []sqlStatement) error {
	conn, err := a.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()
	beginSQL, err := beginStatement(mode, domains)
	if err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, beginSQL); err != nil {
		return fmt.Errorf("begin ASQL tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer cancel()
			_, _ = conn.Exec(rollbackCtx, "ROLLBACK")
		}
	}()
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, statement.query, statement.args...); err != nil {
			return fmt.Errorf("exec %q: %w", statement.query, err)
		}
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit ASQL tx: %w", err)
	}
	committed = true
	return nil
}

func beginStatement(mode string, domains []string) (string, error) {
	clean := make([]string, 0, len(domains))
	for _, domain := range domains {
		trimmed := strings.TrimSpace(domain)
		if trimmed != "" {
			clean = append(clean, trimmed)
		}
	}
	if len(clean) == 0 {
		return "", errors.New("at least one domain is required")
	}
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "domain":
		if len(clean) != 1 {
			return "", errors.New("domain mode requires exactly one domain")
		}
		return "BEGIN DOMAIN " + clean[0], nil
	case "cross":
		return "BEGIN CROSS DOMAIN " + strings.Join(clean, ", "), nil
	default:
		return "", fmt.Errorf("unsupported mode %q", mode)
	}
}

func (a *application) queryRows(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	rows, err := a.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	result := make([]map[string]any, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]any, len(values))
		for i, value := range values {
			row[string(fields[i].Name)] = normalizeValue(value)
		}
		result = append(result, row)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return result, nil
}

func (a *application) queryScalarInt64(ctx context.Context, sql string, args ...any) (int64, bool, error) {
	rows, err := a.queryRows(ctx, sql, args...)
	if err != nil {
		return 0, false, err
	}
	if len(rows) == 0 {
		return 0, false, nil
	}
	for _, value := range rows[0] {
		converted, ok := toInt64(value)
		return converted, ok, nil
	}
	return 0, false, nil
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode json: %w", err)
	}
	return nil
}

func (a *application) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		a.logger.Error("write json failed", "error", err.Error())
	}
}

func (a *application) writeError(w http.ResponseWriter, status int, err error) {
	a.writeJSON(w, status, map[string]any{"error": err.Error()})
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func defaultTimestamp(value string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func prefixedID(prefix, raw string) string {
	sanitized := slug(raw)
	if sanitized == "" {
		sanitized = "item"
	}
	return prefix + "-" + sanitized
}

func slug(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = identifierSanitizer.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	return strings.ToLower(value)
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return `{}`
	}
	return string(encoded)
}

func asOfClause(lsn uint64) string {
	if lsn == 0 {
		return ""
	}
	return fmt.Sprintf(" AS OF LSN %d", lsn)
}

func parseOptionalLSN(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	lsn, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid lsn %q", raw)
	}
	return lsn, nil
}

func normalizeValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return string(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339Nano)
	default:
		return typed
	}
}

func toInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int32:
		return int64(typed), true
	case int:
		return int64(typed), true
	case float64:
		return int64(typed), true
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil
	case []byte:
		parsed, err := strconv.ParseInt(string(typed), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
