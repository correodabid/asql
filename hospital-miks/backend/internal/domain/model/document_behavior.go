package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewDocument creates a new Document entity with validation.
func NewDocument(title string, category DocumentCategory, patientID *uuid.UUID, uploadedBy uuid.UUID, fileName, mimeType string, sizeBytes int64, storagePath, checksum string) (*Document, error) {
	d := &Document{
		ID:          uuid.New(),
		Title:       title,
		Category:    category,
		PatientID:   patientID,
		UploadedBy:  uploadedBy,
		FileName:    fileName,
		MimeType:    mimeType,
		SizeBytes:   sizeBytes,
		StoragePath: storagePath,
		Checksum:    checksum,
		Version:     1,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return d, nil
}

// Validate enforces Document invariants.
func (d *Document) Validate() error {
	if d.Title == "" || d.FileName == "" {
		return fmt.Errorf("document: title and file name are required")
	}
	if d.UploadedBy == uuid.Nil {
		return fmt.Errorf("document: uploader is required")
	}
	return nil
}

// IncrementVersion bumps the document version.
func (d *Document) IncrementVersion() {
	d.Version++
	d.UpdatedAt = time.Now()
}

// NewDocumentAccess creates an access log entry.
func NewDocumentAccess(documentID, staffID uuid.UUID, action, ipAddress string) *DocumentAccess {
	return &DocumentAccess{
		ID:         uuid.New(),
		DocumentID: documentID,
		StaffID:    staffID,
		Action:     action,
		IPAddress:  ipAddress,
		AccessedAt: time.Now(),
	}
}
