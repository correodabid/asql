package http

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
)

// ASQLHandler exposes ASQL-specific features: time travel, audit, cross-domain.
type ASQLHandler struct {
	uc *usecase.ASQLFeaturesUseCase
}

func NewASQLHandler(uc *usecase.ASQLFeaturesUseCase) *ASQLHandler {
	return &ASQLHandler{uc: uc}
}

func (h *ASQLHandler) Routes() chi.Router {
	r := chi.NewRouter()

	// ── Time Travel ──────────────────────────────────────────
	r.Get("/time-travel/patients/{id}", h.patientSnapshot)
	r.Get("/time-travel/admissions/{id}", h.admissionSnapshot)
	r.Get("/time-travel/prescriptions/{id}", h.prescriptionSnapshot)

	// ── Audit / FOR HISTORY ──────────────────────────────────
	r.Get("/audit/{domain}/{table}", h.tableHistory)
	r.Get("/audit/{domain}/{table}/{id}", h.entityHistory)

	// ── Cross-Domain Reads / IMPORT ──────────────────────────
	r.Get("/cross-domain/patients/{id}/invoices", h.patientWithInvoices)
	r.Get("/cross-domain/appointments/{id}/details", h.appointmentWithDetails)

	return r
}

// ── Time Travel handlers ────────────────────────────────────────

func (h *ASQLHandler) patientSnapshot(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	lsn, err := parseLSN(r)
	if err != nil {
		respondError(w, http.StatusBadRequest, "lsn query parameter is required (positive integer)")
		return
	}
	snap, err := h.uc.GetPatientSnapshot(r.Context(), id, lsn)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, snap)
}

func (h *ASQLHandler) admissionSnapshot(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid admission ID")
		return
	}
	lsn, err := parseLSN(r)
	if err != nil {
		respondError(w, http.StatusBadRequest, "lsn query parameter is required (positive integer)")
		return
	}
	snap, err := h.uc.GetAdmissionSnapshot(r.Context(), id, lsn)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, snap)
}

func (h *ASQLHandler) prescriptionSnapshot(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid prescription ID")
		return
	}
	lsn, err := parseLSN(r)
	if err != nil {
		respondError(w, http.StatusBadRequest, "lsn query parameter is required (positive integer)")
		return
	}
	snap, err := h.uc.GetPrescriptionSnapshot(r.Context(), id, lsn)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, snap)
}

// ── Audit handlers ──────────────────────────────────────────────

func (h *ASQLHandler) tableHistory(w http.ResponseWriter, r *http.Request) {
	domain := chi.URLParam(r, "domain")
	table := chi.URLParam(r, "table")
	if domain == "" || table == "" {
		respondError(w, http.StatusBadRequest, "domain and table are required")
		return
	}
	records, err := h.uc.GetTableHistory(r.Context(), domain, table)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, records)
}

func (h *ASQLHandler) entityHistory(w http.ResponseWriter, r *http.Request) {
	domain := chi.URLParam(r, "domain")
	table := chi.URLParam(r, "table")
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid entity ID")
		return
	}
	records, err := h.uc.GetEntityHistory(r.Context(), domain, table, id)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, records)
}

// ── Cross-Domain handlers ───────────────────────────────────────

func (h *ASQLHandler) patientWithInvoices(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	result, err := h.uc.GetPatientWithInvoices(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, result)
}

func (h *ASQLHandler) appointmentWithDetails(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid appointment ID")
		return
	}
	result, err := h.uc.GetAppointmentWithDetails(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, result)
}

// ── helpers ─────────────────────────────────────────────────────

func parseLSN(r *http.Request) (uint64, error) {
	s := r.URL.Query().Get("lsn")
	if s == "" {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseUint(s, 10, 64)
}
