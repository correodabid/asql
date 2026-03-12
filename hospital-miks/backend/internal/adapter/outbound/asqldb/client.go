// Package asqldb implements outbound adapters for the ASQL database engine.
//
// ASQL speaks the PostgreSQL wire protocol (pgwire) so we use the standard
// pgx/v5 driver.  The key differences from vanilla PostgreSQL are:
//
//   - Transactions are domain-scoped: BEGIN DOMAIN <name>; ... COMMIT;
//   - Cross-domain transactions: BEGIN CROSS DOMAIN <a>, <b>; ... COMMIT;
//   - Versioned Foreign Keys (VFK) capture cross-domain references at a point-in-time LSN.
//   - IMPORT <domain>.<table> enables cross-domain reads inside a single query.
//   - AS OF LSN <n> enables time-travel queries.
//   - FOR HISTORY returns the complete change log of a table.
//   - CREATE ENTITY defines aggregate roots whose version auto-increments.
//
// Each repository targets a single ASQL domain and executes its work inside
// a domain-scoped transaction obtained via Client.BeginDomain().
package asqldb

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Client wraps a pgx connection pool connected to an ASQL instance via pgwire.
type Client struct {
	Pool *pgxpool.Pool
}

// NewClient creates a new ASQL client.
// dsn should point to the ASQL pgwire endpoint, e.g.
// "postgres://asql:asql@127.0.0.1:5432/hospital_miks?sslmode=disable"
func NewClient(ctx context.Context, dsn string) (*Client, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("asql: connecting: %w", err)
	}
	// Note: we skip pool.Ping() because ASQL requires an active domain
	// transaction for any SQL operation. Connectivity is verified on first use.
	return &Client{Pool: pool}, nil
}

// Close shuts down the connection pool.
func (c *Client) Close() { c.Pool.Close() }

// Ping checks connectivity to the ASQL instance.
func (c *Client) Ping(ctx context.Context) error { return c.Pool.Ping(ctx) }

// ── Domain Transactions ─────────────────────────────────────────────────────

// DomainTx represents a transaction scoped to one or more ASQL domains.
type DomainTx struct {
	conn    *pgxpool.Conn
	ctx     context.Context
	domains []string
}

// BeginDomain starts a single-domain transaction.
func (c *Client) BeginDomain(ctx context.Context, domain string) (*DomainTx, error) {
	conn, err := c.Pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("asql: acquire conn: %w", err)
	}

	sql := fmt.Sprintf("BEGIN DOMAIN %s", domain)
	if _, err := conn.Exec(ctx, sql); err != nil {
		conn.Release()
		return nil, fmt.Errorf("asql: begin domain %s: %w", domain, err)
	}
	return &DomainTx{conn: conn, ctx: ctx, domains: []string{domain}}, nil
}

// BeginCrossDomain starts a cross-domain transaction spanning multiple domains.
func (c *Client) BeginCrossDomain(ctx context.Context, domains []string) (*DomainTx, error) {
	conn, err := c.Pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("asql: acquire conn: %w", err)
	}

	sql := fmt.Sprintf("BEGIN CROSS DOMAIN %s", strings.Join(domains, ", "))
	if _, err := conn.Exec(ctx, sql); err != nil {
		conn.Release()
		return nil, fmt.Errorf("asql: begin cross domain: %w", err)
	}
	return &DomainTx{conn: conn, ctx: ctx, domains: domains}, nil
}

// Exec executes a statement inside the domain transaction.
func (tx *DomainTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.conn.Exec(ctx, sql, args...)
}

// Query executes a query inside the domain transaction.
func (tx *DomainTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.conn.Query(ctx, sql, args...)
}

// QueryRow executes a single-row query inside the domain transaction.
func (tx *DomainTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.conn.QueryRow(ctx, sql, args...)
}

// Commit commits the domain transaction.
func (tx *DomainTx) Commit(ctx context.Context) error {
	_, err := tx.conn.Exec(ctx, "COMMIT")
	tx.conn.Release()
	return err
}

// Rollback rolls back the domain transaction.
func (tx *DomainTx) Rollback(ctx context.Context) error {
	_, err := tx.conn.Exec(ctx, "ROLLBACK")
	tx.conn.Release()
	return err
}

// ── Time Travel ─────────────────────────────────────────────────────────────

// TimeTravelQuery executes a SELECT at a historical LSN.
// It appends a magic comment that ASQL's pgwire server interprets.
func (c *Client) TimeTravelQuery(ctx context.Context, sql string, lsn uint64) (pgx.Rows, error) {
	annotated := fmt.Sprintf("%s /* as-of-lsn: %d */", strings.TrimRight(sql, "; \t\n"), lsn)
	return c.Pool.Query(ctx, annotated)
}

// TimeTravelQueryByTimestamp executes a SELECT at a historical logical timestamp.
func (c *Client) TimeTravelQueryByTimestamp(ctx context.Context, sql string, ts uint64) (pgx.Rows, error) {
	annotated := fmt.Sprintf("%s /* as-of-ts: %d */", strings.TrimRight(sql, "; \t\n"), ts)
	return c.Pool.Query(ctx, annotated)
}

// ── FOR HISTORY (audit) ─────────────────────────────────────────────────────

// HistoryRow represents a single change record from FOR HISTORY.
type HistoryRow struct {
	Operation string            // INSERT, UPDATE, DELETE
	CommitLSN uint64            // WAL position of the change
	Columns   map[string]string // column name → value (as string)
}

// ForHistory returns the complete change log of a table.
// The caller must set up a domain session first (e.g. via BeginDomain + COMMIT
// or using the /* domain: X */ comment annotation).
func (c *Client) ForHistory(ctx context.Context, domain, table string) ([]HistoryRow, error) {
	// Use a short-lived domain transaction for the read.
	tx, err := c.BeginDomain(ctx, domain)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	sql := fmt.Sprintf("SELECT * FROM %s FOR HISTORY", table)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("asql: for history %s.%s: %w", domain, table, err)
	}
	defer rows.Close()

	var history []HistoryRow
	cols := rows.FieldDescriptions()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		hr := HistoryRow{Columns: make(map[string]string, len(cols))}
		for i, fd := range cols {
			name := string(fd.Name)
			val := fmt.Sprintf("%v", vals[i])
			switch name {
			case "__operation":
				hr.Operation = val
			case "__commit_lsn":
				if n, ok := vals[i].(int64); ok {
					hr.CommitLSN = uint64(n)
				}
			default:
				hr.Columns[name] = val
			}
		}
		history = append(history, hr)
	}
	return history, rows.Err()
}

// ── IMPORT (cross-domain read) ──────────────────────────────────────────────

// ImportQuery executes a read query that uses IMPORT to access another domain's tables.
// Example: ImportQuery(ctx, "billing", "IMPORT patients.patients AS p; SELECT i.*, p.first_name FROM invoices i JOIN p ON i.patient_id = p.id")
func (c *Client) ImportQuery(ctx context.Context, domain, sql string) (pgx.Rows, error) {
	tx, err := c.BeginDomain(ctx, domain)
	if err != nil {
		return nil, err
	}
	// Note: caller must handle rows.Close() and then we do a deferred rollback.
	// For simplicity, we execute within the domain tx and let the caller close rows.
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return nil, fmt.Errorf("asql: import query: %w", err)
	}
	// We can't easily rollback after returning rows, so we commit read-only tx.
	// The tx will be committed when rows are fully consumed by the caller.
	return &importRows{Rows: rows, tx: tx}, nil
}

// importRows wraps pgx.Rows to auto-commit the domain transaction on Close.
type importRows struct {
	pgx.Rows
	tx *DomainTx
}

func (r *importRows) Close() {
	r.Rows.Close()
	r.tx.Commit(r.tx.ctx) //nolint:errcheck
}
