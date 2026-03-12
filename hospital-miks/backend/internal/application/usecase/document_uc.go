package usecase

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// DocumentUseCase orchestrates document management with access logging.
type DocumentUseCase struct {
	docRepo port.DocumentRepository
}

func NewDocumentUseCase(r port.DocumentRepository) *DocumentUseCase {
	return &DocumentUseCase{docRepo: r}
}

func (uc *DocumentUseCase) UploadDocument(ctx context.Context, doc *model.Document) error {
	if err := doc.Validate(); err != nil {
		return err
	}
	return uc.docRepo.Create(ctx, doc)
}

// GetDocument retrieves the document and logs the access.
func (uc *DocumentUseCase) GetDocument(ctx context.Context, id, accessedByUserID uuid.UUID) (*model.Document, error) {
	doc, err := uc.docRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	access := model.NewDocumentAccess(id, accessedByUserID, "view", "")
	_ = uc.docRepo.LogAccess(ctx, access)

	return doc, nil
}

// UpdateDocument increments version via entity method and persists.
func (uc *DocumentUseCase) UpdateDocument(ctx context.Context, doc *model.Document) error {
	doc.IncrementVersion()
	doc.UpdatedAt = time.Now()
	return uc.docRepo.Update(ctx, doc)
}

func (uc *DocumentUseCase) DeleteDocument(ctx context.Context, id uuid.UUID) error {
	return uc.docRepo.Delete(ctx, id)
}

func (uc *DocumentUseCase) ListDocuments(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Document], error) {
	filter.Normalize()
	return uc.docRepo.List(ctx, filter)
}

func (uc *DocumentUseCase) ListPatientDocuments(ctx context.Context, patientID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Document], error) {
	filter.Normalize()
	return uc.docRepo.ListByPatient(ctx, patientID, filter)
}

func (uc *DocumentUseCase) ListByCategory(ctx context.Context, category model.DocumentCategory, filter model.ListFilter) (*model.ListResult[model.Document], error) {
	filter.Normalize()
	return uc.docRepo.ListByCategory(ctx, category, filter)
}

func (uc *DocumentUseCase) SearchDocuments(ctx context.Context, query string, filter model.ListFilter) (*model.ListResult[model.Document], error) {
	filter.Normalize()
	return uc.docRepo.Search(ctx, query, filter)
}

func (uc *DocumentUseCase) GetAccessLog(ctx context.Context, docID uuid.UUID) ([]model.DocumentAccess, error) {
	return uc.docRepo.GetAccessLog(ctx, docID)
}
