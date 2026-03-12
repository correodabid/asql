package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const taxRate = 0.21 // 21% IVA

// GenerateInvoiceNumber generates a unique invoice number.
func GenerateInvoiceNumber() string {
	return fmt.Sprintf("INV-%s", uuid.New().String()[:8])
}

// NewInvoice creates a new draft Invoice entity.
func NewInvoice(patientID uuid.UUID, admissionID *uuid.UUID) (*Invoice, error) {
	if patientID == uuid.Nil {
		return nil, fmt.Errorf("invoice: patient is required")
	}
	return &Invoice{
		ID:            uuid.New(),
		InvoiceNumber: GenerateInvoiceNumber(),
		PatientID:     patientID,
		AdmissionID:   admissionID,
		Status:        InvoiceStatusDraft,
		Currency:      "EUR",
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}, nil
}

// Validate enforces Invoice invariants.
func (inv *Invoice) Validate() error {
	if inv.PatientID == uuid.Nil {
		return fmt.Errorf("invoice: patient is required")
	}
	return nil
}

// CanAddItems returns true if invoice is still editable.
func (inv *Invoice) CanAddItems() bool {
	return inv.Status == InvoiceStatusDraft
}

// Recalculate recomputes subtotal, tax and total from line items.
func (inv *Invoice) Recalculate(items []InvoiceItem) {
	var subtotal float64
	for _, item := range items {
		subtotal += item.Total
	}
	inv.Subtotal = subtotal
	inv.Tax = subtotal * taxRate
	inv.Total = inv.Subtotal + inv.Tax - inv.Discount
	inv.UpdatedAt = time.Now()
}

// Issue transitions from DRAFT to ISSUED, setting due date 30 days out.
func (inv *Invoice) Issue() error {
	if inv.Status != InvoiceStatusDraft {
		return fmt.Errorf("invoice: only draft invoices can be issued")
	}
	now := time.Now()
	dueDate := now.Add(30 * 24 * time.Hour)
	inv.Status = InvoiceStatusIssued
	inv.IssuedAt = &now
	inv.DueDate = &dueDate
	inv.UpdatedAt = now
	return nil
}

// MarkPaid transitions to PAID with payment method.
func (inv *Invoice) MarkPaid(method PaymentMethod) error {
	if inv.Status != InvoiceStatusIssued && inv.Status != InvoiceStatusOverdue {
		return fmt.Errorf("invoice: cannot pay from status %s", inv.Status)
	}
	now := time.Now()
	inv.Status = InvoiceStatusPaid
	inv.PaidAt = &now
	inv.PaymentMethod = method
	inv.UpdatedAt = now
	return nil
}

// NewInvoiceItem creates a new line item with calculated total.
func NewInvoiceItem(invoiceID uuid.UUID, description, category string, quantity int, unitPrice float64) *InvoiceItem {
	return &InvoiceItem{
		ID:          uuid.New(),
		InvoiceID:   invoiceID,
		Description: description,
		Category:    category,
		Quantity:    quantity,
		UnitPrice:   unitPrice,
		Total:       float64(quantity) * unitPrice,
		CreatedAt:   time.Now(),
	}
}
