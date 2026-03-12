package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// RunDomainTx executes one explicit ASQL domain-scoped transaction.
func RunDomainTx(ctx context.Context, conn *pgx.Conn, domain string, fn func(*pgx.Conn) error) error {
	return runScopedTx(ctx, conn, txModeDomain, []string{domain}, fn)
}

// RunCrossDomainTx executes one explicit ASQL cross-domain transaction.
func RunCrossDomainTx(ctx context.Context, conn *pgx.Conn, domains []string, fn func(*pgx.Conn) error) error {
	return runScopedTx(ctx, conn, txModeCross, domains, fn)
}

func runScopedTx(ctx context.Context, conn *pgx.Conn, mode txMode, domains []string, fn func(*pgx.Conn) error) error {
	beginSQL, err := beginScopeSQL(mode, domains)
	if err != nil {
		return err
	}

	if _, err := conn.Exec(ctx, beginSQL); err != nil {
		return fmt.Errorf("begin scoped tx: %w", err)
	}

	if err := fn(conn); err != nil {
		rollbackErr := rollback(ctx, conn)
		if rollbackErr != nil {
			return fmt.Errorf("run scoped tx: %w (rollback error: %v)", err, rollbackErr)
		}
		return err
	}

	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit scoped tx: %w", err)
	}

	return nil
}

func beginScopeSQL(mode txMode, domains []string) (string, error) {
	step := scenarioStep{Name: "transaction helper", Mode: mode, Domains: domains}
	return step.BeginSQL()
}
