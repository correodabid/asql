package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// BillingUseCase orchestrates invoice and billing operations.
type BillingUseCase struct {
	invoiceRepo port.InvoiceRepository
}

func NewBillingUseCase(r port.InvoiceRepository) *BillingUseCase {
	return &BillingUseCase{invoiceRepo: r}
}

func (uc *BillingUseCase) CreateInvoice(ctx context.Context, inv *model.Invoice) error {
	if err := inv.Validate(); err != nil {
		return err
	}
	return uc.invoiceRepo.Create(ctx, inv)
}

func (uc *BillingUseCase) GetInvoice(ctx context.Context, id uuid.UUID) (*model.Invoice, error) {
	return uc.invoiceRepo.GetByID(ctx, id)
}

func (uc *BillingUseCase) GetInvoiceByNumber(ctx context.Context, number string) (*model.Invoice, error) {
	return uc.invoiceRepo.GetByNumber(ctx, number)
}

// AddInvoiceItem checks the entity allows adding items, persists, then recalculates.
func (uc *BillingUseCase) AddInvoiceItem(ctx context.Context, invoiceID uuid.UUID, item *model.InvoiceItem) error {
	inv, err := uc.invoiceRepo.GetByID(ctx, invoiceID)
	if err != nil {
		return err
	}
	if !inv.CanAddItems() {
		return fmt.Errorf("cannot add items to invoice in status %s", inv.Status)
	}
	item.InvoiceID = invoiceID
	if err := uc.invoiceRepo.AddItem(ctx, item); err != nil {
		return err
	}
	// Recalculate totals from persisted items.
	items, err := uc.invoiceRepo.GetItems(ctx, invoiceID)
	if err != nil {
		return err
	}
	inv.Recalculate(items)
	return uc.invoiceRepo.Update(ctx, inv)
}

// IssueInvoice transitions the invoice to issued status via entity method.
func (uc *BillingUseCase) IssueInvoice(ctx context.Context, id uuid.UUID) error {
	inv, err := uc.invoiceRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if err := inv.Issue(); err != nil {
		return err
	}
	return uc.invoiceRepo.Update(ctx, inv)
}

// MarkPaid transitions the invoice to paid via entity method.
func (uc *BillingUseCase) MarkPaid(ctx context.Context, id uuid.UUID, method model.PaymentMethod) error {
	inv, err := uc.invoiceRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if err := inv.MarkPaid(method); err != nil {
		return err
	}
	return uc.invoiceRepo.Update(ctx, inv)
}

func (uc *BillingUseCase) ListInvoices(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Invoice], error) {
	filter.Normalize()
	return uc.invoiceRepo.List(ctx, filter)
}

func (uc *BillingUseCase) ListPatientInvoices(ctx context.Context, patientID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Invoice], error) {
	filter.Normalize()
	return uc.invoiceRepo.ListByPatient(ctx, patientID, filter)
}

func (uc *BillingUseCase) ListInvoicesByStatus(ctx context.Context, status model.InvoiceStatus, filter model.ListFilter) (*model.ListResult[model.Invoice], error) {
	filter.Normalize()
	return uc.invoiceRepo.ListByStatus(ctx, status, filter)
}

func (uc *BillingUseCase) GetInvoiceItems(ctx context.Context, invoiceID uuid.UUID) ([]model.InvoiceItem, error) {
	return uc.invoiceRepo.GetItems(ctx, invoiceID)
}
