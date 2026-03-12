package usecase

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hospital-miks/backend/internal/domain/model"
	"github.com/hospital-miks/backend/internal/domain/port"
)

// AppointmentUseCase orchestrates appointment scheduling and lifecycle.
type AppointmentUseCase struct {
	apptRepo port.AppointmentRepository
	roomRepo port.ConsultationRoomRepository
}

func NewAppointmentUseCase(a port.AppointmentRepository, r port.ConsultationRoomRepository) *AppointmentUseCase {
	return &AppointmentUseCase{apptRepo: a, roomRepo: r}
}

// ScheduleAppointment validates, checks for conflicts, and persists a new appointment.
func (uc *AppointmentUseCase) ScheduleAppointment(ctx context.Context, appt *model.Appointment) error {
	if err := appt.Validate(); err != nil {
		return err
	}
	// Conflict detection
	existing, err := uc.apptRepo.ListByDoctorAndDate(ctx, appt.DoctorID, appt.ScheduledAt)
	if err != nil {
		return fmt.Errorf("checking conflicts: %w", err)
	}
	for _, e := range existing {
		if appt.ConflictsWith(e) {
			return fmt.Errorf("scheduling conflict with appointment %s", e.ID)
		}
	}
	return uc.apptRepo.Create(ctx, appt)
}

func (uc *AppointmentUseCase) GetAppointment(ctx context.Context, id uuid.UUID) (*model.Appointment, error) {
	return uc.apptRepo.GetByID(ctx, id)
}

// ConfirmAppointment delegates state transition to the entity.
func (uc *AppointmentUseCase) ConfirmAppointment(ctx context.Context, id uuid.UUID) error {
	appt, err := uc.apptRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if err := appt.Confirm(); err != nil {
		return err
	}
	return uc.apptRepo.Update(ctx, appt)
}

func (uc *AppointmentUseCase) CancelAppointment(ctx context.Context, id uuid.UUID) error {
	return uc.apptRepo.Cancel(ctx, id)
}

// CompleteAppointment delegates completion to entity, then persists.
func (uc *AppointmentUseCase) CompleteAppointment(ctx context.Context, id uuid.UUID, diagnosis, notes string) error {
	appt, err := uc.apptRepo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if err := appt.Complete(diagnosis, notes); err != nil {
		return err
	}
	return uc.apptRepo.Update(ctx, appt)
}

func (uc *AppointmentUseCase) ListAppointments(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.Appointment], error) {
	filter.Normalize()
	return uc.apptRepo.List(ctx, filter)
}

func (uc *AppointmentUseCase) ListPatientAppointments(ctx context.Context, patientID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Appointment], error) {
	filter.Normalize()
	return uc.apptRepo.ListByPatient(ctx, patientID, filter)
}

func (uc *AppointmentUseCase) ListDoctorAppointments(ctx context.Context, doctorID uuid.UUID, filter model.ListFilter) (*model.ListResult[model.Appointment], error) {
	filter.Normalize()
	return uc.apptRepo.ListByDoctor(ctx, doctorID, filter)
}

func (uc *AppointmentUseCase) CreateConsultationRoom(ctx context.Context, room *model.ConsultationRoom) error {
	return uc.roomRepo.Create(ctx, room)
}

func (uc *AppointmentUseCase) ListConsultationRooms(ctx context.Context, filter model.ListFilter) (*model.ListResult[model.ConsultationRoom], error) {
	filter.Normalize()
	return uc.roomRepo.List(ctx, filter)
}
