package model

import (
	"time"

	"github.com/google/uuid"
)

// OperatingRoomStatus represents the status of an operating room.
type OperatingRoomStatus string

const (
	ORStatusAvailable   OperatingRoomStatus = "AVAILABLE"
	ORStatusInUse       OperatingRoomStatus = "IN_USE"
	ORStatusCleaning    OperatingRoomStatus = "CLEANING"
	ORStatusMaintenance OperatingRoomStatus = "MAINTENANCE"
	ORStatusReserved    OperatingRoomStatus = "RESERVED"
)

// SurgeryStatus represents the status of a surgery.
type SurgeryStatus string

const (
	SurgeryStatusScheduled  SurgeryStatus = "SCHEDULED"
	SurgeryStatusPreOp      SurgeryStatus = "PRE_OP"
	SurgeryStatusInProgress SurgeryStatus = "IN_PROGRESS"
	SurgeryStatusPostOp     SurgeryStatus = "POST_OP"
	SurgeryStatusCompleted  SurgeryStatus = "COMPLETED"
	SurgeryStatusCancelled  SurgeryStatus = "CANCELLED"
)

// OperatingRoom represents an operating room/theatre.
type OperatingRoom struct {
	ID         uuid.UUID           `json:"id"`
	Name       string              `json:"name"`
	Code       string              `json:"code"`
	Floor      int                 `json:"floor"`
	Building   string              `json:"building"`
	Status     OperatingRoomStatus `json:"status"`
	Equipment  string              `json:"equipment,omitempty"`
	Capacity   int                 `json:"capacity"`
	Active     bool                `json:"active"`
	CreatedAt  time.Time           `json:"created_at"`
	UpdatedAt  time.Time           `json:"updated_at"`
}

// Surgery represents a surgical procedure.
type Surgery struct {
	ID              uuid.UUID     `json:"id"`
	PatientID       uuid.UUID     `json:"patient_id"`
	LeadSurgeonID   uuid.UUID     `json:"lead_surgeon_id"`
	AnesthetistID   uuid.UUID     `json:"anesthetist_id"`
	OperatingRoomID uuid.UUID     `json:"operating_room_id"`
	ProcedureName   string        `json:"procedure_name"`
	ProcedureCode   string        `json:"procedure_code"`
	Status          SurgeryStatus `json:"status"`
	ScheduledStart  time.Time     `json:"scheduled_start"`
	ScheduledEnd    time.Time     `json:"scheduled_end"`
	ActualStart     *time.Time    `json:"actual_start,omitempty"`
	ActualEnd       *time.Time    `json:"actual_end,omitempty"`
	PreOpNotes      string        `json:"pre_op_notes,omitempty"`
	PostOpNotes     string        `json:"post_op_notes,omitempty"`
	Complications   string        `json:"complications,omitempty"`
	CreatedAt       time.Time     `json:"created_at"`
	UpdatedAt       time.Time     `json:"updated_at"`
}

// SurgeryTeamMember represents a member of a surgical team.
type SurgeryTeamMember struct {
	ID        uuid.UUID `json:"id"`
	SurgeryID uuid.UUID `json:"surgery_id"`
	StaffID   uuid.UUID `json:"staff_id"`
	Role      string    `json:"role"` // e.g. "ASSISTANT_SURGEON", "SCRUB_NURSE", "CIRCULATING_NURSE"
	CreatedAt time.Time `json:"created_at"`
}
