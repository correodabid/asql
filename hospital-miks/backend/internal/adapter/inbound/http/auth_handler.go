package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// AuthHandler handles authentication HTTP requests.
type AuthHandler struct {
	svc *usecase.AuthUseCase
}

func NewAuthHandler(svc *usecase.AuthUseCase) *AuthHandler {
	return &AuthHandler{svc: svc}
}

func (h *AuthHandler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Post("/login", h.login)
	r.Get("/users", h.listUsers)
	r.Post("/users", h.createUser)
	return r
}

func (h *AuthHandler) login(w http.ResponseWriter, r *http.Request) {
	var req usecase.LoginRequest
	if err := decodeJSON(r, &req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	resp, err := h.svc.Login(r.Context(), req)
	if err != nil {
		respondError(w, http.StatusUnauthorized, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, resp)
}

func (h *AuthHandler) listUsers(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListUsers(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *AuthHandler) createUser(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StaffID  string `json:"staff_id"`
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
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
	user := &model.User{StaffID: staffID, Username: body.Username, Role: body.Role}
	if err := h.svc.CreateUser(r.Context(), user, body.Password); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, user)
}
