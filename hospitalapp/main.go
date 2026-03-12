package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	ListenAddr string
	PGWireAddr string
	User       string
	Password   string
	Database   string
}

type application struct {
	logger *slog.Logger
	pool   *pgxpool.Pool
}

type sqlStatement struct {
	query string
	args  []any
}

type txPlan struct {
	name       string
	mode       string
	domains    []string
	statements []sqlStatement
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.ListenAddr, "listen", ":8095", "HTTP listen address")
	flag.StringVar(&cfg.PGWireAddr, "pgwire", "127.0.0.1:5433", "ASQL pgwire endpoint")
	flag.StringVar(&cfg.User, "user", "asql", "pgwire user")
	flag.StringVar(&cfg.Password, "password", "", "pgwire password")
	flag.StringVar(&cfg.Database, "database", "asql", "pgwire database")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := openPool(ctx, cfg)
	if err != nil {
		logger.Error("failed to connect to ASQL", "error", err.Error())
		os.Exit(1)
	}
	defer pool.Close()

	app := &application{logger: logger, pool: pool}
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info("hospital app listening", "listen", cfg.ListenAddr, "pgwire", cfg.PGWireAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http server failed", "error", err.Error())
		os.Exit(1)
	}
}

func openPool(ctx context.Context, cfg config) (*pgxpool.Pool, error) {
	u := &url.URL{
		Scheme: "postgres",
		Host:   cfg.PGWireAddr,
		Path:   cfg.Database,
	}
	if cfg.Password == "" {
		u.User = url.User(cfg.User)
	} else {
		u.User = url.UserPassword(cfg.User, cfg.Password)
	}
	query := u.Query()
	query.Set("sslmode", "disable")
	u.RawQuery = query.Encode()

	poolConfig, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping ASQL: %w", err)
	}
	return pool, nil
}

func (a *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.handleHealthz)
	mux.HandleFunc("POST /bootstrap", a.handleBootstrap)
	mux.HandleFunc("POST /seed/demo", a.handleSeedDemo)
	mux.HandleFunc("POST /patients/register", a.handleRegisterPatient)
	mux.HandleFunc("POST /patients/{patientID}/admissions", a.handleAdmitPatient)
	mux.HandleFunc("POST /patients/{patientID}/lab-orders", a.handleCreateLabOrder)
	mux.HandleFunc("POST /patients/{patientID}/medication-orders", a.handleCreateMedicationOrder)
	mux.HandleFunc("POST /patients/{patientID}/surgeries", a.handleBookSurgery)
	mux.HandleFunc("POST /patients/{patientID}/discharge", a.handleDischargePatient)
	mux.HandleFunc("GET /patients/{patientID}/snapshot", a.handlePatientSnapshot)
	mux.HandleFunc("GET /patients/{patientID}/history", a.handlePatientHistory)
	mux.HandleFunc("GET /patients/{patientID}/audit", a.handlePatientAudit)
	return mux
}
