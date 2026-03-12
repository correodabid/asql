package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// AdmissionHandler handles HTTP requests for hospital admissions.
type AdmissionHandler struct {
	svc *usecase.AdmissionUseCase
}

// NewAdmissionHandler creates a new AdmissionHandler.
func NewAdmissionHandler(svc *usecase.AdmissionUseCase) *AdmissionHandler {
	return &AdmissionHandler{svc: svc}
}

// Routes registers admission routes.
func (h *AdmissionHandler) Routes() chi.Router {
	r := chi.NewRouter()

	r.Post("/", h.admitPatient)
	r.Get("/", h.listAdmissions)
	r.Get("/patient/{patientId}", h.listByPatient)
	r.Get("/{id}", h.getAdmission)
	r.Post("/{id}/discharge", h.dischargePatient)

	r.Route("/{admissionId}/meals", func(r chi.Router) {
		r.Post("/", h.orderMeal)
		r.Get("/", h.getMeals)
	})

	r.Route("/{admissionId}/care-notes", func(r chi.Router) {
		r.Post("/", h.addCareNote)
		r.Get("/", h.getCareNotes)
	})

	r.Route("/wards", func(r chi.Router) {
		r.Post("/", h.createWard)
		r.Get("/", h.listWards)
	})

	r.Route("/beds", func(r chi.Router) {
		r.Post("/", h.createBed)
		r.Get("/available", h.listAvailableBeds)
		r.Get("/ward/{wardId}", h.listBedsByWard)
	})

	return r
}

func (h *AdmissionHandler) admitPatient(w http.ResponseWriter, r *http.Request) {
	var adm model.Admission
	if err := decodeJSON(r, &adm); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.AdmitPatient(r.Context(), &adm); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, adm)
}

func (h *AdmissionHandler) listAdmissions(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListActiveAdmissions(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *AdmissionHandler) listByPatient(w http.ResponseWriter, r *http.Request) {
	patientID, err := parseUUID(chi.URLParam(r, "patientId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	items, err := h.svc.ListPatientAdmissions(r.Context(), patientID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, items)
}

func (h *AdmissionHandler) getAdmission(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	adm, err := h.svc.GetAdmission(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, adm)
}

func (h *AdmissionHandler) dischargePatient(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if err := h.svc.DischargePatient(r.Context(), id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "discharged"})
}

func (h *AdmissionHandler) orderMeal(w http.ResponseWriter, r *http.Request) {
	admissionID, err := parseUUID(chi.URLParam(r, "admissionId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid admission ID")
		return
	}
	var meal model.MealOrder
	if err := decodeJSON(r, &meal); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meal.AdmissionID = admissionID
	if err := h.svc.OrderMeal(r.Context(), &meal); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, meal)
}

func (h *AdmissionHandler) getMeals(w http.ResponseWriter, r *http.Request) {
	admissionID, err := parseUUID(chi.URLParam(r, "admissionId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid admission ID")
		return
	}
	meals, err := h.svc.GetMealOrders(r.Context(), admissionID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, meals)
}

func (h *AdmissionHandler) addCareNote(w http.ResponseWriter, r *http.Request) {
	admissionID, err := parseUUID(chi.URLParam(r, "admissionId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid admission ID")
		return
	}
	var note model.CareNote
	if err := decodeJSON(r, &note); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	note.AdmissionID = admissionID
	if err := h.svc.AddCareNote(r.Context(), &note); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, note)
}

func (h *AdmissionHandler) getCareNotes(w http.ResponseWriter, r *http.Request) {
	admissionID, err := parseUUID(chi.URLParam(r, "admissionId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid admission ID")
		return
	}
	notes, err := h.svc.GetCareNotes(r.Context(), admissionID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, notes)
}

func (h *AdmissionHandler) createWard(w http.ResponseWriter, r *http.Request) {
	var ward model.Ward
	if err := decodeJSON(r, &ward); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreateWard(r.Context(), &ward); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, ward)
}

func (h *AdmissionHandler) listWards(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListWards(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *AdmissionHandler) createBed(w http.ResponseWriter, r *http.Request) {
	var bed model.Bed
	if err := decodeJSON(r, &bed); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreateBed(r.Context(), &bed); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, bed)
}

func (h *AdmissionHandler) listBedsByWard(w http.ResponseWriter, r *http.Request) {
	wardID, err := parseUUID(chi.URLParam(r, "wardId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ward ID")
		return
	}
	beds, err := h.svc.ListBedsByWard(r.Context(), wardID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, beds)
}

func (h *AdmissionHandler) listAvailableBeds(w http.ResponseWriter, r *http.Request) {
	beds, err := h.svc.ListAvailableBeds(r.Context())
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, beds)
}
