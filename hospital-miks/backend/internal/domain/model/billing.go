package model

import (
	"time"

	"github.com/google/uuid"
)

// InvoiceStatus represents the status of an invoice.
type InvoiceStatus string

const (
	InvoiceStatusDraft     InvoiceStatus = "DRAFT"
	InvoiceStatusIssued    InvoiceStatus = "ISSUED"
	InvoiceStatusPaid      InvoiceStatus = "PAID"
	InvoiceStatusOverdue   InvoiceStatus = "OVERDUE"
	InvoiceStatusCancelled InvoiceStatus = "CANCELLED"
	InvoiceStatusRefunded  InvoiceStatus = "REFUNDED"
)

// PaymentMethod represents a payment method.
type PaymentMethod string

const (
	PaymentMethodCash      PaymentMethod = "CASH"
	PaymentMethodCard      PaymentMethod = "CARD"
	PaymentMethodTransfer  PaymentMethod = "BANK_TRANSFER"
	PaymentMethodInsurance PaymentMethod = "INSURANCE"
	PaymentMethodMixed     PaymentMethod = "MIXED"
)

// Invoice represents a billing invoice.
type Invoice struct {
	ID            uuid.UUID     `json:"id"`
	InvoiceNumber string        `json:"invoice_number"`
	PatientID     uuid.UUID     `json:"patient_id"`
	AdmissionID   *uuid.UUID    `json:"admission_id,omitempty"`
	Status        InvoiceStatus `json:"status"`
	Subtotal      float64       `json:"subtotal"`
	Tax           float64       `json:"tax"`
	Discount      float64       `json:"discount"`
	Total         float64       `json:"total"`
	Currency      string        `json:"currency"`
	IssuedAt      *time.Time    `json:"issued_at,omitempty"`
	DueDate       *time.Time    `json:"due_date,omitempty"`
	PaidAt        *time.Time    `json:"paid_at,omitempty"`
	PaymentMethod PaymentMethod `json:"payment_method,omitempty"`
	Notes         string        `json:"notes,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

// InvoiceItem represents a line item in an invoice.
type InvoiceItem struct {
	ID          uuid.UUID `json:"id"`
	InvoiceID   uuid.UUID `json:"invoice_id"`
	Description string    `json:"description"`
	Category    string    `json:"category"` // "CONSULTATION", "SURGERY", "MEDICATION", "LAB", "ROOM", "MEAL", etc.
	Quantity    int       `json:"quantity"`
	UnitPrice   float64   `json:"unit_price"`
	Total       float64   `json:"total"`
	CreatedAt   time.Time `json:"created_at"`
}
