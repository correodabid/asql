package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// DocumentHandler handles HTTP requests for document management.
type DocumentHandler struct {
	svc *usecase.DocumentUseCase
}

// NewDocumentHandler creates a new DocumentHandler.
func NewDocumentHandler(svc *usecase.DocumentUseCase) *DocumentHandler {
	return &DocumentHandler{svc: svc}
}

// Routes registers document routes.
func (h *DocumentHandler) Routes() chi.Router {
	r := chi.NewRouter()

	r.Post("/", h.uploadDocument)
	r.Get("/", h.listDocuments)
	r.Get("/search", h.searchDocuments)
	r.Get("/category/{category}", h.listByCategory)
	r.Get("/patient/{patientId}", h.listPatientDocuments)
	r.Get("/{id}", h.getDocument)
	r.Put("/{id}", h.updateDocument)
	r.Delete("/{id}", h.deleteDocument)
	r.Get("/{id}/access-log", h.getAccessLog)

	return r
}

func (h *DocumentHandler) uploadDocument(w http.ResponseWriter, r *http.Request) {
	var doc model.Document
	if err := decodeJSON(r, &doc); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.UploadDocument(r.Context(), &doc); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, doc)
}

func (h *DocumentHandler) getDocument(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	// Use a zero UUID for accessedByUserID — in production this comes from auth context.
	doc, err := h.svc.GetDocument(r.Context(), id, [16]byte{})
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, doc)
}

func (h *DocumentHandler) updateDocument(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var doc model.Document
	if err := decodeJSON(r, &doc); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	doc.ID = id
	if err := h.svc.UpdateDocument(r.Context(), &doc); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, doc)
}

func (h *DocumentHandler) deleteDocument(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if err := h.svc.DeleteDocument(r.Context(), id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (h *DocumentHandler) listDocuments(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListDocuments(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *DocumentHandler) searchDocuments(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	filter := parseListFilter(r)
	result, err := h.svc.SearchDocuments(r.Context(), q, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *DocumentHandler) listByCategory(w http.ResponseWriter, r *http.Request) {
	category := model.DocumentCategory(chi.URLParam(r, "category"))
	filter := parseListFilter(r)
	result, err := h.svc.ListByCategory(r.Context(), category, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *DocumentHandler) listPatientDocuments(w http.ResponseWriter, r *http.Request) {
	patientID, err := parseUUID(chi.URLParam(r, "patientId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	filter := parseListFilter(r)
	result, err := h.svc.ListPatientDocuments(r.Context(), patientID, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *DocumentHandler) getAccessLog(w http.ResponseWriter, r *http.Request) {
	docID, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid document ID")
		return
	}
	log, err := h.svc.GetAccessLog(r.Context(), docID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, log)
}
