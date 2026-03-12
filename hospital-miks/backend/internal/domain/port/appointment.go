package port

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
)

// AppointmentRepository defines the outbound port for appointment persistence.
type AppointmentRepository interface {
	Create(ctx context.Context, appt *model.Appointment) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.Appointment, error)
	Update(ctx context.Context, appt *model.Appointment) error
	Cancel(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Appointment], error)
	ListByPatient(ctx context.Context, patientID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Appointment], error)
	ListByDoctor(ctx context.Context, doctorID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Appointment], error)
	ListByDateRange(ctx context.Context, from, to time.Time, filter model.ListFilter) (*model.ListResult[model.Appointment], error)
	ListByDoctorAndDate(ctx context.Context, doctorID uuid.UUID, date time.Time) ([]model.Appointment, error)
}

// ConsultationRoomRepository defines the outbound port for consultation room persistence.
type ConsultationRoomRepository interface {
	Create(ctx context.Context, room *model.ConsultationRoom) error
	GetByID(ctx context.Context, id uuid.UUID) (*model.ConsultationRoom, error)
	Update(ctx context.Context, room *model.ConsultationRoom) error
	Delete(ctx context.Context, id uuid.UUID) error
	List(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.ConsultationRoom], error)
	ListByDepartment(ctx context.Context, deptID uuid.UUID) ([]model.ConsultationRoom, error)
}
