package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// MessagingHandler handles HTTP requests for internal messaging.
type MessagingHandler struct {
	svc *usecase.MessagingUseCase
}

// NewMessagingHandler creates a new MessagingHandler.
func NewMessagingHandler(svc *usecase.MessagingUseCase) *MessagingHandler {
	return &MessagingHandler{svc: svc}
}

// Routes registers messaging routes.
func (h *MessagingHandler) Routes() chi.Router {
	r := chi.NewRouter()

	r.Post("/", h.sendMessage)
	r.Get("/{id}", h.getMessage)
	r.Post("/{id}/read", h.markAsRead)
	r.Get("/inbox/{userId}", h.getInbox)
	r.Get("/sent/{userId}", h.getSent)
	r.Get("/unread-count/{userId}", h.getUnreadCount)
	r.Get("/thread/{parentId}", h.getThread)

	r.Route("/patient-comms", func(r chi.Router) {
		r.Post("/", h.sendPatientComm)
		r.Get("/patient/{patientId}", h.listPatientComms)
	})

	return r
}

func (h *MessagingHandler) sendMessage(w http.ResponseWriter, r *http.Request) {
	var msg model.Message
	if err := decodeJSON(r, &msg); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.SendMessage(r.Context(), &msg); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, msg)
}

func (h *MessagingHandler) getMessage(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	msg, err := h.svc.GetMessage(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, msg)
}

func (h *MessagingHandler) markAsRead(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if err := h.svc.MarkAsRead(r.Context(), id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "read"})
}

func (h *MessagingHandler) getInbox(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUUID(chi.URLParam(r, "userId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid user ID")
		return
	}
	filter := parseListFilter(r)
	result, err := h.svc.GetInbox(r.Context(), userID, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *MessagingHandler) getSent(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUUID(chi.URLParam(r, "userId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid user ID")
		return
	}
	filter := parseListFilter(r)
	result, err := h.svc.GetSent(r.Context(), userID, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *MessagingHandler) getUnreadCount(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUUID(chi.URLParam(r, "userId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid user ID")
		return
	}
	count, err := h.svc.GetUnreadCount(r.Context(), userID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]int{"unread_count": count})
}

func (h *MessagingHandler) getThread(w http.ResponseWriter, r *http.Request) {
	parentID, err := parseUUID(chi.URLParam(r, "parentId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid parent ID")
		return
	}
	msgs, err := h.svc.GetThread(r.Context(), parentID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, msgs)
}

func (h *MessagingHandler) sendPatientComm(w http.ResponseWriter, r *http.Request) {
	var comm model.PatientCommunication
	if err := decodeJSON(r, &comm); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.SendPatientCommunication(r.Context(), &comm); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, comm)
}

func (h *MessagingHandler) listPatientComms(w http.ResponseWriter, r *http.Request) {
	patientID, err := parseUUID(chi.URLParam(r, "patientId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	filter := parseListFilter(r)
	result, err := h.svc.ListPatientCommunications(r.Context(), patientID, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}
