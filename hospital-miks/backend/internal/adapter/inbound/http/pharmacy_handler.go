package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// PharmacyHandler handles pharmacy HTTP requests.
type PharmacyHandler struct {
	svc *usecase.PharmacyUseCase
}

func NewPharmacyHandler(svc *usecase.PharmacyUseCase) *PharmacyHandler {
	return &PharmacyHandler{svc: svc}
}

func (h *PharmacyHandler) Routes() chi.Router {
	r := chi.NewRouter()

	r.Route("/medications", func(r chi.Router) {
		r.Post("/", h.addMedication)
		r.Get("/", h.listMedications)
		r.Get("/search", h.searchMedications)
		r.Get("/low-stock", h.lowStock)
		r.Get("/{id}", h.getMedication)
		r.Put("/{id}", h.updateMedication)
	})

	r.Route("/prescriptions", func(r chi.Router) {
		r.Post("/", h.createPrescription)
		r.Get("/", h.listActivePrescriptions)
		r.Get("/{id}", h.getPrescription)
		r.Get("/{id}/dispenses", h.listDispenses)
		r.Post("/{id}/dispense", h.dispense)
		r.Get("/patient/{patientId}", h.listPatientPrescriptions)
	})

	return r
}

func (h *PharmacyHandler) addMedication(w http.ResponseWriter, r *http.Request) {
	var med model.Medication
	if err := decodeJSON(r, &med); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.AddMedication(r.Context(), &med); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, med)
}

func (h *PharmacyHandler) getMedication(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	med, err := h.svc.GetMedication(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, med)
}

func (h *PharmacyHandler) updateMedication(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var med model.Medication
	if err := decodeJSON(r, &med); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	med.ID = id
	if err := h.svc.UpdateMedication(r.Context(), &med); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, med)
}

func (h *PharmacyHandler) listMedications(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListMedications(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *PharmacyHandler) searchMedications(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	filter := parseListFilter(r)
	result, err := h.svc.SearchMedications(r.Context(), q, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *PharmacyHandler) lowStock(w http.ResponseWriter, r *http.Request) {
	meds, err := h.svc.GetLowStockMedications(r.Context())
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, meds)
}

func (h *PharmacyHandler) createPrescription(w http.ResponseWriter, r *http.Request) {
	var rx model.Prescription
	if err := decodeJSON(r, &rx); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreatePrescription(r.Context(), &rx); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, rx)
}

func (h *PharmacyHandler) getPrescription(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	rx, err := h.svc.GetPrescription(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, rx)
}

func (h *PharmacyHandler) listActivePrescriptions(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListActivePrescriptions(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *PharmacyHandler) listDispenses(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid prescription ID")
		return
	}
	items, err := h.svc.ListDispensesByPrescription(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, items)
}

func (h *PharmacyHandler) dispense(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var body struct {
		PharmacistID string `json:"pharmacist_id"`
		Quantity     int    `json:"quantity"`
		Notes        string `json:"notes"`
	}
	if err := decodeJSON(r, &body); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	pharmacistID, err := parseUUID(body.PharmacistID)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid pharmacist ID")
		return
	}
	if err := h.svc.DispensePrescription(r.Context(), id, pharmacistID, body.Quantity, body.Notes); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "dispensed"})
}

func (h *PharmacyHandler) listPatientPrescriptions(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "patientId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	rxs, err := h.svc.ListPatientPrescriptions(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, rxs)
}
