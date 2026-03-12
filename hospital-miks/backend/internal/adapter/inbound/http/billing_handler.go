package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// BillingHandler handles HTTP requests for billing and invoices.
type BillingHandler struct {
	svc *usecase.BillingUseCase
}

// NewBillingHandler creates a new BillingHandler.
func NewBillingHandler(svc *usecase.BillingUseCase) *BillingHandler {
	return &BillingHandler{svc: svc}
}

// Routes registers billing routes.
func (h *BillingHandler) Routes() chi.Router {
	r := chi.NewRouter()

	r.Route("/invoices", func(r chi.Router) {
		r.Post("/", h.createInvoice)
		r.Get("/", h.listInvoices)
		r.Get("/{id}", h.getInvoice)
		r.Get("/{id}/items", h.getInvoiceItems)
		r.Post("/{id}/items", h.addItem)
		r.Post("/{id}/issue", h.issueInvoice)
		r.Post("/{id}/pay", h.markPaid)
		r.Get("/patient/{patientId}", h.listPatientInvoices)
		r.Get("/status/{status}", h.listByStatus)
	})

	return r
}

func (h *BillingHandler) createInvoice(w http.ResponseWriter, r *http.Request) {
	var inv model.Invoice
	if err := decodeJSON(r, &inv); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.CreateInvoice(r.Context(), &inv); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, inv)
}

func (h *BillingHandler) getInvoice(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	inv, err := h.svc.GetInvoice(r.Context(), id)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, inv)
}

func (h *BillingHandler) listInvoices(w http.ResponseWriter, r *http.Request) {
	filter := parseListFilter(r)
	result, err := h.svc.ListInvoices(r.Context(), filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *BillingHandler) addItem(w http.ResponseWriter, r *http.Request) {
	invoiceID, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid invoice ID")
		return
	}
	var item model.InvoiceItem
	if err := decodeJSON(r, &item); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.AddInvoiceItem(r.Context(), invoiceID, &item); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, item)
}

func (h *BillingHandler) getInvoiceItems(w http.ResponseWriter, r *http.Request) {
	invoiceID, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid invoice ID")
		return
	}
	items, err := h.svc.GetInvoiceItems(r.Context(), invoiceID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, items)
}

func (h *BillingHandler) issueInvoice(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	if err := h.svc.IssueInvoice(r.Context(), id); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "issued"})
}

func (h *BillingHandler) markPaid(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid ID")
		return
	}
	var body struct {
		PaymentMethod string `json:"payment_method"`
	}
	if err := decodeJSON(r, &body); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.svc.MarkPaid(r.Context(), id, model.PaymentMethod(body.PaymentMethod)); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, map[string]string{"status": "paid"})
}

func (h *BillingHandler) listPatientInvoices(w http.ResponseWriter, r *http.Request) {
	patientID, err := parseUUID(chi.URLParam(r, "patientId"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid patient ID")
		return
	}
	filter := parseListFilter(r)
	result, err := h.svc.ListPatientInvoices(r.Context(), patientID, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}

func (h *BillingHandler) listByStatus(w http.ResponseWriter, r *http.Request) {
	status := model.InvoiceStatus(chi.URLParam(r, "status"))
	filter := parseListFilter(r)
	result, err := h.svc.ListInvoicesByStatus(r.Context(), status, filter)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondList(w, result.Items, result.Total, result.Page, result.PageSize, result.TotalPages)
}
