package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// SurgeryHandler handles HTTP requests for operating rooms and surgeries.
type SurgeryHandler struct {
	svc *usecase.SurgeryUseCase
}

// NewSurgeryHandler creates a new SurgeryHandler.
func NewSurgeryHandler(svc *usecase.SurgeryUseCase) *SurgeryHandler {
	return &SurgeryHandler{svc: svc}
}

// Routes registers surgery routes.
func (h *SurgeryHandler) Routes() chi.Router {
	r := chi.NewRouter()

	r.Route("/rooms", func(r chi.Router) {
		r.Post("/", h.createRoom)
		r.Get("/", h.listRooms)
		r.Get("/{id}", h.getRoom)
	})

	r.Route("/procedures", func(r chi.Router) {
		r.Post("/", h.scheduleSurgery)
		r.Get("/", h.listSurgeries)
		r.Get("/patient/{patientId}", h.listByPatient)
		r.Get("/{id}", h.getSurgery)
		r.Get("/{id}/team", h.getTeamMembers)
		r.Post("/{id}/start", h.startSurgery)
		r.Post("/{id}/complete", h.completeSurgery)
		r.Post("/{id}/team", h.addTeamMember)
	})

	return r
}

func (h *SurgeryHandler) createRoom(w http.ResponseWriter, r *http.Request) {
	var room model.OperatingRoom
	if err := decodeJSON(r, &room); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreateOperatingRoom(r.Context(), &room); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, room)
}

func (h *SurgeryHandler) listRooms(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListOperatingRooms(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *SurgeryHandler) getRoom(w http.ResponseWriter, r *http.Request) {
	// Not in UC yet — return list as fallback
	filter := parseListFilter(r)
	result, err := h.svc.ListOperatingRooms(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, result.Items)
}

func (h *SurgeryHandler) scheduleSurgery(w http.ResponseWriter, r *http.Request) {
	var surgery model.Surgery
	if err := decodeJSON(r, &surgery); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.ScheduleSurgery(r.Context(), &surgery); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, surgery)
}

func (h *SurgeryHandler) getSurgery(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	surgery, err := h.svc.GetSurgery(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, surgery)
}

func (h *SurgeryHandler) listByPatient(w http.ResponseWriter, r *http.Request) {
	patientID, err := parseUUID(chi.URLParam(r, "patientId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	items, err := h.svc.ListPatientSurgeries(r.Context(), patientID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, items)
}

func (h *SurgeryHandler) startSurgery(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if err := h.svc.StartSurgery(r.Context(), id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "started"})
}

func (h *SurgeryHandler) completeSurgery(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var body struct {
		PostOpNotes   string `json:"post_op_notes"`
		Complications string `json:"complications"`
	}
	if err := decodeJSON(r, &body); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CompleteSurgery(r.Context(), id, body.PostOpNotes, body.Complications); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func (h *SurgeryHandler) addTeamMember(w http.ResponseWriter, r *http.Request) {
	surgeryID, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid surgery ID")
		return
	}
	var body struct {
		StaffID string `json:"staff_id"`
		Role    string `json:"role"`
	}
	if err := decodeJSON(r, &body); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	staffID, err := parseUUID(body.StaffID)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid staff ID")
		return
	}
	if err := h.svc.AddTeamMember(r.Context(), surgeryID, staffID, body.Role); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, map[string]string{"status": "added"})
}

func (h *SurgeryHandler) getTeamMembers(w http.ResponseWriter, r *http.Request) {
	surgeryID, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid surgery ID")
		return
	}
	items, err := h.svc.GetTeamMembers(r.Context(), surgeryID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, items)
}

func (h *SurgeryHandler) listSurgeries(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListSurgeries(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}
