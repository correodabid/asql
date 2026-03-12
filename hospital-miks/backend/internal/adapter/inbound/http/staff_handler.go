package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// StaffHandler handles HTTP requests for staff management.
type StaffHandler struct {
	svc *usecase.StaffUseCase
}

// NewStaffHandler creates a new StaffHandler.
func NewStaffHandler(svc *usecase.StaffUseCase) *StaffHandler {
	return &StaffHandler{svc: svc}
}

// Routes registers staff routes.
func (h *StaffHandler) Routes() chi.Router {
	r := chi.NewRouter()

	r.Post("/", h.createStaff)
	r.Get("/", h.listStaff)
	r.Get("/{id}", h.getStaff)
	r.Put("/{id}", h.updateStaff)
	r.Delete("/{id}", h.deactivateStaff)

	r.Route("/departments", func(r chi.Router) {
		r.Post("/", h.createDepartment)
		r.Get("/", h.listDepartments)
		r.Get("/{id}", h.getDepartment)
		r.Put("/{id}", h.updateDepartment)
	})

	return r
}

func (h *StaffHandler) createStaff(w http.ResponseWriter, r *http.Request) {
	var staff model.Staff
	if err := decodeJSON(r, &staff); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreateStaff(r.Context(), &staff); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, staff)
}

func (h *StaffHandler) getStaff(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	staff, err := h.svc.GetStaff(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, staff)
}

func (h *StaffHandler) updateStaff(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var staff model.Staff
	if err := decodeJSON(r, &staff); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	staff.ID = id
	if err := h.svc.UpdateStaff(r.Context(), &staff); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, staff)
}

func (h *StaffHandler) deactivateStaff(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if err := h.svc.DeactivateStaff(r.Context(), id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "deactivated"})
}

func (h *StaffHandler) listStaff(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListStaff(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *StaffHandler) createDepartment(w http.ResponseWriter, r *http.Request) {
	var dept model.Department
	if err := decodeJSON(r, &dept); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreateDepartment(r.Context(), &dept); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, dept)
}

func (h *StaffHandler) getDepartment(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	dept, err := h.svc.GetDepartment(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, dept)
}

func (h *StaffHandler) listDepartments(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListDepartments(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *StaffHandler) updateDepartment(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var dept model.Department
	if err := decodeJSON(r, &dept); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	dept.ID = id
	if err := h.svc.UpdateDepartment(r.Context(), &dept); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, dept)
}
