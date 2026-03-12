package model

import (
	"time"

	"github.com/google/uuid"
)

// DocumentCategory represents a document category.
type DocumentCategory string

const (
	DocCatMedicalRecord  DocumentCategory = "MEDICAL_RECORD"
	DocCatLabResult      DocumentCategory = "LAB_RESULT"
	DocCatImaging        DocumentCategory = "IMAGING"
	DocCatPrescription   DocumentCategory = "PRESCRIPTION"
	DocCatConsent        DocumentCategory = "CONSENT_FORM"
	DocCatDischarge      DocumentCategory = "DISCHARGE_SUMMARY"
	DocCatSurgeryReport  DocumentCategory = "SURGERY_REPORT"
	DocCatInsurance      DocumentCategory = "INSURANCE"
	DocCatAdministrative DocumentCategory = "ADMINISTRATIVE"
	DocCatOther          DocumentCategory = "OTHER"
)

// Document represents a document in the document management system.
type Document struct {
	ID          uuid.UUID        `json:"id"`
	Title       string           `json:"title"`
	Category    DocumentCategory `json:"category"`
	PatientID   *uuid.UUID       `json:"patient_id,omitempty"`
	UploadedBy  uuid.UUID        `json:"uploaded_by"`
	FileName    string           `json:"file_name"`
	MimeType    string           `json:"mime_type"`
	SizeBytes   int64            `json:"size_bytes"`
	StoragePath string           `json:"-"` // internal storage path, not exposed
	Checksum    string           `json:"checksum"`
	Version     int              `json:"version"`
	Tags        string           `json:"tags,omitempty"`
	Notes       string           `json:"notes,omitempty"`
	CreatedAt   time.Time        `json:"created_at"`
	UpdatedAt   time.Time        `json:"updated_at"`
}

// DocumentAccess represents an access log entry for a document.
type DocumentAccess struct {
	ID         uuid.UUID `json:"id"`
	DocumentID uuid.UUID `json:"document_id"`
	StaffID    uuid.UUID `json:"staff_id"`
	Action     string    `json:"action"` // "VIEW", "DOWNLOAD", "PRINT", "EDIT"
	IPAddress  string    `json:"ip_address"`
	AccessedAt time.Time `json:"accessed_at"`
}
