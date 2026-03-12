package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// AppointmentHandler handles appointment HTTP requests.
type AppointmentHandler struct {
	svc *usecase.AppointmentUseCase
}

func NewAppointmentHandler(svc *usecase.AppointmentUseCase) *AppointmentHandler {
	return &AppointmentHandler{svc: svc}
}

func (h *AppointmentHandler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Route("/rooms", func(r chi.Router) {
		r.Post("/", h.createRoom)
		r.Get("/", h.listRooms)
	})
	r.Post("/", h.schedule)
	r.Get("/", h.list)
	r.Get("/{id}", h.get)
	r.Put("/{id}/confirm", h.confirm)
	r.Put("/{id}/cancel", h.cancel)
	r.Put("/{id}/complete", h.complete)
	r.Get("/patient/{patientId}", h.listByPatient)
	r.Get("/doctor/{doctorId}", h.listByDoctor)
	return r
}

func (h *AppointmentHandler) createRoom(w http.ResponseWriter, r *http.Request) {
	var room model.ConsultationRoom
	if err := decodeJSON(r, &room); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreateConsultationRoom(r.Context(), &room); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, room)
}

func (h *AppointmentHandler) listRooms(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListConsultationRooms(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *AppointmentHandler) schedule(w http.ResponseWriter, r *http.Request) {
	var a model.Appointment
	if err := decodeJSON(r, &a); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.ScheduleAppointment(r.Context(), &a); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, a)
}

func (h *AppointmentHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	a, err := h.svc.GetAppointment(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, a)
}

func (h *AppointmentHandler) confirm(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if err := h.svc.ConfirmAppointment(r.Context(), id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "confirmed"})
}

func (h *AppointmentHandler) cancel(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if err := h.svc.CancelAppointment(r.Context(), id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (h *AppointmentHandler) complete(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var body struct {
		Diagnosis string `json:"diagnosis"`
		Notes     string `json:"notes"`
	}
	if err := decodeJSON(r, &body); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CompleteAppointment(r.Context(), id, body.Diagnosis, body.Notes); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func (h *AppointmentHandler) list(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListAppointments(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *AppointmentHandler) listByPatient(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "patientId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	filter := parseListFilter(r)
	result, err := h.svc.ListPatientAppointments(r.Context(), id, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *AppointmentHandler) listByDoctor(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "doctorId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid doctor ID")
		return
	}
	filter := parseListFilter(r)
	result, err := h.svc.ListDoctorAppointments(r.Context(), id, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}
