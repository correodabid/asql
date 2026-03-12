package model

import (
	"time"

	"github.com/google/uuid"
)

// MessagePriority represents message urgency.
type MessagePriority string

const (
	MessagePriorityLow    MessagePriority = "LOW"
	MessagePriorityNormal MessagePriority = "NORMAL"
	MessagePriorityHigh   MessagePriority = "HIGH"
	MessagePriorityUrgent MessagePriority = "URGENT"
)

// Message represents an internal messaging system message.
type Message struct {
	ID         uuid.UUID       `json:"id"`
	SenderID   uuid.UUID       `json:"sender_id"`
	ReceiverID uuid.UUID       `json:"receiver_id"`
	Subject    string          `json:"subject"`
	Body       string          `json:"body"`
	Priority   MessagePriority `json:"priority"`
	Read       bool            `json:"read"`
	ReadAt     *time.Time      `json:"read_at,omitempty"`
	ParentID   *uuid.UUID      `json:"parent_id,omitempty"` // for reply threads
	CreatedAt  time.Time       `json:"created_at"`
}

// PatientCommunication represents a communication sent to a patient.
type PatientCommunication struct {
	ID          uuid.UUID `json:"id"`
	PatientID   uuid.UUID `json:"patient_id"`
	StaffID     uuid.UUID `json:"staff_id"`
	Channel     string    `json:"channel"` // "SMS", "EMAIL", "PHONE", "IN_PERSON", "PORTAL"
	Subject     string    `json:"subject"`
	Content     string    `json:"content"`
	Status      string    `json:"status"` // "SENT", "DELIVERED", "FAILED", "READ"
	SentAt      time.Time `json:"sent_at"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}
