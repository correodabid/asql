package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// NewMessage creates a new Message entity with validation.
func NewMessage(senderID, receiverID uuid.UUID, subject, body string, priority MessagePriority, parentID *uuid.UUID) (*Message, error) {
	m := &Message{
		ID:         uuid.New(),
		SenderID:   senderID,
		ReceiverID: receiverID,
		Subject:    subject,
		Body:       body,
		Priority:   priority,
		Read:       false,
		ParentID:   parentID,
		CreatedAt:  time.Now(),
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// Validate enforces Message invariants.
func (m *Message) Validate() error {
	if m.SenderID == uuid.Nil || m.ReceiverID == uuid.Nil {
		return fmt.Errorf("message: sender and receiver are required")
	}
	if m.Subject == "" || m.Body == "" {
		return fmt.Errorf("message: subject and body are required")
	}
	return nil
}

// MarkRead marks the message as read.
func (m *Message) MarkRead() {
	now := time.Now()
	m.Read = true
	m.ReadAt = &now
}

// NewPatientCommunication creates a new PatientCommunication entity.
func NewPatientCommunication(patientID, staffID uuid.UUID, channel, subject, content string) (*PatientCommunication, error) {
	if patientID == uuid.Nil || staffID == uuid.Nil {
		return nil, fmt.Errorf("patient communication: patient and staff are required")
	}
	return &PatientCommunication{
		ID:        uuid.New(),
		PatientID: patientID,
		StaffID:   staffID,
		Channel:   channel,
		Subject:   subject,
		Content:   content,
		Status:    "SENT",
		SentAt:    time.Now(),
		CreatedAt: time.Now(),
	}, nil
}
