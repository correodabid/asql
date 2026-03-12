package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// RehabHandler handles HTTP requests for rehabilitation plans and sessions.
type RehabHandler struct {
	svc *usecase.RehabUseCase
}

// NewRehabHandler creates a new RehabHandler.
func NewRehabHandler(svc *usecase.RehabUseCase) *RehabHandler {
	return &RehabHandler{svc: svc}
}

// Routes registers rehabilitation routes.
func (h *RehabHandler) Routes() chi.Router {
	r := chi.NewRouter()

	r.Route("/plans", func(r chi.Router) {
		r.Post("/", h.createPlan)
		r.Get("/", h.listActivePlans)
		r.Get("/{id}", h.getPlan)
		r.Put("/{id}", h.updatePlan)
		r.Get("/patient/{patientId}", h.listPatientPlans)
	})

	r.Route("/sessions", func(r chi.Router) {
		r.Post("/", h.scheduleSession)
		r.Get("/plan/{planId}", h.listPlanSessions)
		r.Post("/{id}/complete", h.completeSession)
		r.Post("/{id}/cancel", h.cancelSession)
	})

	return r
}

func (h *RehabHandler) createPlan(w http.ResponseWriter, r *http.Request) {
	var plan model.RehabPlan
	if err := decodeJSON(r, &plan); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreatePlan(r.Context(), &plan); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, plan)
}

func (h *RehabHandler) getPlan(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	plan, err := h.svc.GetPlan(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, plan)
}

func (h *RehabHandler) updatePlan(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var plan model.RehabPlan
	if err := decodeJSON(r, &plan); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	plan.ID = id
	if err := h.svc.UpdatePlan(r.Context(), &plan); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, plan)
}

func (h *RehabHandler) listActivePlans(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListActivePlans(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *RehabHandler) listPatientPlans(w http.ResponseWriter, r *http.Request) {
	patientID, err := parseUUID(chi.URLParam(r, "patientId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	plans, err := h.svc.ListPatientPlans(r.Context(), patientID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, plans)
}

func (h *RehabHandler) scheduleSession(w http.ResponseWriter, r *http.Request) {
	var session model.RehabSession
	if err := decodeJSON(r, &session); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.ScheduleSession(r.Context(), &session); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, session)
}

func (h *RehabHandler) completeSession(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var body struct {
		Progress  string `json:"progress"`
		Exercises string `json:"exercises"`
		Notes     string `json:"notes"`
		PainLevel *int   `json:"pain_level"`
	}
	if err := decodeJSON(r, &body); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CompleteSession(r.Context(), id, body.Progress, body.Exercises, body.Notes, body.PainLevel); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "completed"})
}

func (h *RehabHandler) cancelSession(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if err := h.svc.CancelSession(r.Context(), id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

func (h *RehabHandler) listPlanSessions(w http.ResponseWriter, r *http.Request) {
	planID, err := parseUUID(chi.URLParam(r, "planId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid plan ID")
		return
	}
	sessions, err := h.svc.ListPlanSessions(r.Context(), planID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, sessions)
}
