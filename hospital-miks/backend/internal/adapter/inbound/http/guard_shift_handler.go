package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// GuardShiftHandler handles HTTP requests for guard shifts.
type GuardShiftHandler struct {
	svc *usecase.GuardShiftUseCase
}

// NewGuardShiftHandler creates a new GuardShiftHandler.
func NewGuardShiftHandler(svc *usecase.GuardShiftUseCase) *GuardShiftHandler {
	return &GuardShiftHandler{svc: svc}
}

// Routes registers guard shift routes.
func (h *GuardShiftHandler) Routes() chi.Router {
	r := chi.NewRouter()

	r.Post("/", h.createShift)
	r.Get("/", h.listShifts)
	r.Get("/{id}", h.getShift)
	r.Post("/{id}/swap", h.swapShift)
	r.Get("/staff/{staffId}", h.listStaffShifts)
	r.Get("/department/{deptId}", h.listDepartmentShifts)

	return r
}

func (h *GuardShiftHandler) createShift(w http.ResponseWriter, r *http.Request) {
	var shift model.GuardShift
	if err := decodeJSON(r, &shift); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreateShift(r.Context(), &shift); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, shift)
}

func (h *GuardShiftHandler) listShifts(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListShifts(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *GuardShiftHandler) getShift(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	shift, err := h.svc.GetShift(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, shift)
}

func (h *GuardShiftHandler) swapShift(w http.ResponseWriter, r *http.Request) {
	shiftID, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid shift ID")
		return
	}
	var body struct {
		NewStaffID string `json:"new_staff_id"`
	}
	if err := decodeJSON(r, &body); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	newStaffID, err := parseUUID(body.NewStaffID)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid staff ID")
		return
	}
	if err := h.svc.SwapShift(r.Context(), shiftID, newStaffID); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "swapped"})
}

func (h *GuardShiftHandler) listStaffShifts(w http.ResponseWriter, r *http.Request) {
	staffID, err := parseUUID(chi.URLParam(r, "staffId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid staff ID")
		return
	}
	from, to := parseDateRange(r)
	shifts, err := h.svc.ListStaffShifts(r.Context(), staffID, from, to)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, shifts)
}

func (h *GuardShiftHandler) listDepartmentShifts(w http.ResponseWriter, r *http.Request) {
	deptID, err := parseUUID(chi.URLParam(r, "deptId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid department ID")
		return
	}
	from, to := parseDateRange(r)
	shifts, err := h.svc.ListDepartmentShifts(r.Context(), deptID, from, to)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, shifts)
}

// parseDateRange extracts from/to query params (RFC3339 or 2006-01-02 format).
func parseDateRange(r *http.Request) (time.Time, time.Time) {
	from, _ := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	to, _ := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	if from.IsZero() {
		from, _ = time.Parse("2006-01-02", r.URL.Query().Get("from"))
	}
	if to.IsZero() {
		to, _ = time.Parse("2006-01-02", r.URL.Query().Get("to"))
	}
	if from.IsZero() {
		from = time.Now().AddDate(0, -1, 0)
	}
	if to.IsZero() {
		to = time.Now().AddDate(0, 1, 0)
	}
	return from, to
}
