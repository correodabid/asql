package main

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// conn wraps a pgx pool and exposes a narrow, pgwire-centric surface
// that the MCP tool handlers need.  All tool methods are safe to call
// concurrently.
type conn struct {
	pool *pgxpool.Pool

	mu             sync.RWMutex
	domainCache    []string
	domainCacheLen int
}

func newConn(ctx context.Context, cfg *config) (*conn, error) {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   cfg.PgwireAddr,
		Path:   "/" + cfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()

	// ASQL's pgwire supports the extended query protocol, but several
	// catalog intercepts (information_schema, asql_admin.*) return fixed
	// tuple shapes that don't cleanly re-encode in binary for every type
	// the client might request. Using the simple query protocol keeps
	// everything in text format, which bypasses the edge cases and is
	// perfectly adequate for an agent-facing query surface.
	pcfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("pgx config: %w", err)
	}
	pcfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	return &conn{pool: pool}, nil
}

func (c *conn) Close() { c.pool.Close() }

// Ping runs `SELECT current_lsn()` rather than pgx's default empty-
// statement ping or a plain `SELECT 1`. ASQL's pgwire requires an
// explicit BEGIN DOMAIN around user-table queries, but system-function
// calls bypass that check and are a cleaner liveness probe anyway — a
// successful ping returns the engine's current commit position.
func (c *conn) Ping(ctx context.Context) error {
	var lsn int64
	return c.pool.QueryRow(ctx, "SELECT current_lsn()").Scan(&lsn)
}

// isReadOnlySQL returns true when the SQL can be treated as a safe
// read-only query — SELECT, WITH (CTE), TABLE, EXPLAIN, or SHOW — and
// does not contain transaction control that might modify state.
//
// This is a conservative lexical check; it rejects anything ambiguous.
// It is not a full SQL parser.
func isReadOnlySQL(sql string) bool {
	s := strings.TrimSpace(sql)
	// Strip leading SQL comments (block + line, keep it simple: one pass).
	for {
		if strings.HasPrefix(s, "--") {
			if nl := strings.IndexByte(s, '\n'); nl >= 0 {
				s = strings.TrimSpace(s[nl+1:])
				continue
			}
			return false
		}
		if strings.HasPrefix(s, "/*") {
			if end := strings.Index(s, "*/"); end >= 0 {
				s = strings.TrimSpace(s[end+2:])
				continue
			}
			return false
		}
		break
	}
	upper := strings.ToUpper(s)
	switch {
	case strings.HasPrefix(upper, "SELECT "),
		strings.HasPrefix(upper, "SELECT\n"),
		strings.HasPrefix(upper, "SELECT\t"),
		strings.HasPrefix(upper, "WITH "),
		strings.HasPrefix(upper, "TABLE "),
		strings.HasPrefix(upper, "EXPLAIN "),
		strings.HasPrefix(upper, "SHOW "),
		strings.HasPrefix(upper, "TAIL "),
		upper == "SELECT CURRENT_LSN()":
		// Disallow any semicolon-chained additional statements — we
		// only want one statement per tool call.
		if strings.Contains(strings.TrimRight(s, ";"), ";") {
			return false
		}
		return true
	}
	return false
}
