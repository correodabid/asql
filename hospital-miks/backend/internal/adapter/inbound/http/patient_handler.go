package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// PatientHandler handles HTTP requests for patient management.
type PatientHandler struct {
	svc *usecase.PatientUseCase
}

func NewPatientHandler(svc *usecase.PatientUseCase) *PatientHandler {
	return &PatientHandler{svc: svc}
}

func (h *PatientHandler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Post("/", h.register)
	r.Get("/", h.list)
	r.Get("/search", h.search)
	r.Get("/{id}", h.get)
	r.Put("/{id}", h.update)
	return r
}

func (h *PatientHandler) register(w http.ResponseWriter, r *http.Request) {
	var p model.Patient
	if err := decodeJSON(r, &p); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.RegisterPatient(r.Context(), &p); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, p)
}

func (h *PatientHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	p, err := h.svc.GetPatient(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, p)
}

func (h *PatientHandler) update(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var p model.Patient
	if err := decodeJSON(r, &p); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	p.ID = id
	if err := h.svc.UpdatePatient(r.Context(), &p); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, p)
}

func (h *PatientHandler) list(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListPatients(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *PatientHandler) search(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		respondError(w, http.StatusBadRequest, "search query required")
		return
	}
	filter := parseListFilter(r)
	result, err := h.svc.SearchPatients(r.Context(), query, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}
