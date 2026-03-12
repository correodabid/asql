package port

import (
	"context"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// DocumentRepository defines the outbound port for document persistence.
type DocumentRepository interface {
	Create(ctx context.Context, doc *model.Document) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Document, error)
	Update(ctx context.Context, doc *model.Document) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Document], error)
	ListByPatient(ctx context.Context, patientID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Document], error)
	ListByCategory(ctx context.Context, category model.DocumentCategory, filter model.ListFilter) (*model.ListResult[model.Document], error)
	Search(ctx context.Context, query string, filter model.ListFilter) (*model.ListResult[model.Document], error)
	LogAccess(ctx context.Context, access *model.DocumentAccess) error
	GetAccessLog(ctx context.Context, docID uuid.UUID) ([]model.DocumentAccess, error)
}
