package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// InvoiceRepository defines the outbound port for invoice persistence.
type InvoiceRepository interface {
	Create(ctx context.Context, inv *model.Invoice) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Invoice, error)
	GetByNumber(ctx context.Context, number string) (*model.Invoice, error)
	Update(ctx context.Context, inv *model.Invoice) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Invoice], error)
	ListByPatient(ctx context.Context, patientID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Invoice], error)
	ListByStatus(ctx context.Context, status model.InvoiceStatus, filter model.ListFilter) (*model.ListResult[model.Invoice], error)
	AddItem(ctx context.Context, item *model.InvoiceItem) error
	GetItems(ctx context.Context, invoiceID uuid.UUID) ([]model.InvoiceItem, error)
	DeleteItem(ctx context.Context, itemID uuid.UUID) error
}
