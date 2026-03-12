package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/hospital-miks/backend/internal/application/usecase"
)

// Server holds all HTTP handlers and the router.
type Server struct {
	Router *chi.Mux
}

// NewServer creates a new HTTP server with all routes registered.
func NewServer(
	authSvc *usecase.AuthUseCase,
	staffSvc *usecase.StaffUseCase,
	patientSvc *usecase.PatientUseCase,
	appointmentSvc *usecase.AppointmentUseCase,
	surgerySvc *usecase.SurgeryUseCase,
	pharmacySvc *usecase.PharmacyUseCase,
	admissionSvc *usecase.AdmissionUseCase,
	guardShiftSvc *usecase.GuardShiftUseCase,
	rehabSvc *usecase.RehabUseCase,
	billingSvc *usecase.BillingUseCase,
	messagingSvc *usecase.MessagingUseCase,
	documentSvc *usecase.DocumentUseCase,
	asqlFeatures *usecase.ASQLFeaturesUseCase,
	jwtSecret string,
) *Server {
	r := chi.NewRouter()

	// Middleware
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"http://localhost:5173", "http://localhost:3000"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	// Health check
	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		respondJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "hospital-miks"})
	})

	// API v1 routes
	r.Route("/api/v1", func(r chi.Router) {
		// Public — no auth required
		r.Mount("/auth", NewAuthHandler(authSvc).Routes())

		// Protected — require valid Bearer token
		r.Group(func(r chi.Router) {
			r.Use(AuthMiddleware(jwtSecret))
			r.Mount("/staff", NewStaffHandler(staffSvc).Routes())
			r.Mount("/patients", NewPatientHandler(patientSvc).Routes())
			r.Mount("/appointments", NewAppointmentHandler(appointmentSvc).Routes())
			r.Mount("/pharmacy", NewPharmacyHandler(pharmacySvc).Routes())
			r.Mount("/surgery", NewSurgeryHandler(surgerySvc).Routes())
			r.Mount("/admissions", NewAdmissionHandler(admissionSvc).Routes())
			r.Mount("/guard-shifts", NewGuardShiftHandler(guardShiftSvc).Routes())
			r.Mount("/rehab", NewRehabHandler(rehabSvc).Routes())
			r.Mount("/billing", NewBillingHandler(billingSvc).Routes())
			r.Mount("/messaging", NewMessagingHandler(messagingSvc).Routes())
			r.Mount("/documents", NewDocumentHandler(documentSvc).Routes())
			r.Mount("/asql", NewASQLHandler(asqlFeatures).Routes())
		})
	})

	return &Server{Router: r}
}
