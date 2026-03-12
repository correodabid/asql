package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hospital-miks/backend/internal/adapter/outbound/asqldb"
	"github.com/hospital-miks/backend/internal/application/usecase"
	"github.com/hospital-miks/backend/internal/config"

	httpAdapter "github.com/hospital-miks/backend/internal/adapter/inbound/http"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	// ── Database (ASQL via pgwire) ──────────────────────────────
	client, err := asqldb.NewClient(ctx, cfg.Database.DSN())
	if err != nil {
		log.Fatalf("failed to connect to ASQL: %v", err)
	}
	defer client.Close()

	log.Println("connected to ASQL database")

	// ── Repositories (outbound adapters / ASQL domain-scoped) ───
	userRepo := asqldb.NewUserRepo(client)
	staffRepo := asqldb.NewStaffRepo(client)
	deptRepo := asqldb.NewDepartmentRepo(client)
	patientRepo := asqldb.NewPatientRepo(client)
	apptRepo := asqldb.NewAppointmentRepo(client)
	roomRepo := asqldb.NewConsultationRoomRepo(client)
	orRepo := asqldb.NewOperatingRoomRepo(client)
	surgeryRepo := asqldb.NewSurgeryRepo(client)
	admRepo := asqldb.NewAdmissionRepo(client)
	wardRepo := asqldb.NewWardRepo(client)
	bedRepo := asqldb.NewBedRepo(client)
	mealRepo := asqldb.NewMealOrderRepo(client)
	careRepo := asqldb.NewCareNoteRepo(client)
	medRepo := asqldb.NewMedicationRepo(client)
	rxRepo := asqldb.NewPrescriptionRepo(client)
	dispRepo := asqldb.NewPharmacyDispenseRepo(client)
	invoiceRepo := asqldb.NewInvoiceRepo(client)
	guardShiftRepo := asqldb.NewGuardShiftRepo(client)
	planRepo := asqldb.NewRehabPlanRepo(client)
	sessionRepo := asqldb.NewRehabSessionRepo(client)
	msgRepo := asqldb.NewMessageRepo(client)
	commRepo := asqldb.NewPatientCommunicationRepo(client)
	docRepo := asqldb.NewDocumentRepo(client)

	// ASQL-specific feature repos (time travel, audit, cross-domain)
	timeTravelRepo := asqldb.NewTimeTravelRepo(client)
	auditRepo := asqldb.NewAuditRepo(client)
	crossDomainRepo := asqldb.NewCrossDomainReadRepo(client)

	// ── Application use cases (orchestration layer) ─────────────
	authUC := usecase.NewAuthUseCase(userRepo, cfg.JWT.Secret)
	staffUC := usecase.NewStaffUseCase(staffRepo, deptRepo)
	patientUC := usecase.NewPatientUseCase(patientRepo)
	appointmentUC := usecase.NewAppointmentUseCase(apptRepo, roomRepo)
	surgeryUC := usecase.NewSurgeryUseCase(orRepo, surgeryRepo)
	pharmacyUC := usecase.NewPharmacyUseCase(medRepo, rxRepo, dispRepo)
	admissionUC := usecase.NewAdmissionUseCase(admRepo, wardRepo, bedRepo, mealRepo, careRepo)
	guardShiftUC := usecase.NewGuardShiftUseCase(guardShiftRepo)
	rehabUC := usecase.NewRehabUseCase(planRepo, sessionRepo)
	billingUC := usecase.NewBillingUseCase(invoiceRepo)
	messagingUC := usecase.NewMessagingUseCase(msgRepo, commRepo)
	documentUC := usecase.NewDocumentUseCase(docRepo)
	asqlFeaturesUC := usecase.NewASQLFeaturesUseCase(timeTravelRepo, auditRepo, crossDomainRepo)

	// ── HTTP server (inbound adapter) ───────────────────────────
	srv := httpAdapter.NewServer(
		authUC,
		staffUC,
		patientUC,
		appointmentUC,
		surgeryUC,
		pharmacyUC,
		admissionUC,
		guardShiftUC,
		rehabUC,
		billingUC,
		messagingUC,
		documentUC,
		asqlFeaturesUC,
		cfg.JWT.Secret,
	)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	httpSrv := &http.Server{
		Addr:         addr,
		Handler:      srv.Router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ── Graceful shutdown ───────────────────────────────────────
	go func() {
		log.Printf("Hospital MiKS API listening on %s", addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down server...")
	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("server forced to shutdown: %v", err)
	}
	log.Println("server stopped")
}
