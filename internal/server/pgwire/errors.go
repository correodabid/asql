package pgwire

// errors.go — SQLSTATE code mapping for engine errors.
//
// Reference: https://www.postgresql.org/docs/current/errcodes-appendix.html

import (
	"fmt"
	"strings"

	"asql/internal/engine/executor"

	"github.com/jackc/pgx/v5/pgproto3"
)

// mapErrorToSQLState converts an engine error message to the most appropriate
// PostgreSQL SQLSTATE code.  The mapping is intentionally coarse — we look for
// well-known substrings rather than typed sentinel errors.  Callers should
// prefer typed error checks as the engine error hierarchy matures.
func mapErrorToSQLState(err error) string {
	if err == nil {
		return "00000"
	}
	return sqlStateFromMessage(err.Error())
}

func sqlStateFromMessage(msg string) string {
	lower := strings.ToLower(msg)

	// ── Syntax / parse errors ── Class 42 ────────────────────────────────
	if containsAny(lower, "syntax error", "parse error", "unexpected token") ||
		strings.HasPrefix(lower, "expected ") || strings.Contains(lower, " expected ") {
		return "42601" // syntax_error
	}
	if containsAny(lower, "undefined function", "function does not exist") {
		return "42883" // undefined_function
	}
	if containsAll(lower, "column", "does not exist") || containsAll(lower, "column", "not found") {
		return "42703" // undefined_column
	}
	if containsAll(lower, "table", "does not exist") || containsAll(lower, "table", "not found") {
		return "42P01" // undefined_table
	}
	if containsAny(lower, "already exists") {
		return "42P07" // duplicate_table (used generically for duplicate objects)
	}

	// ── Integrity / constraint violations ── Class 23 ────────────────────
	if containsAny(lower, "unique constraint", "duplicate key") ||
		containsAll(lower, "primary key", "duplicate") ||
		containsAll(lower, "unique", "duplicate") {
		return "23505" // unique_violation
	}
	if containsAll(lower, "foreign key", "references") || containsAny(lower, "foreign key violation") {
		return "23503" // foreign_key_violation
	}
	if containsAny(lower, "not null", "null value") || containsAll(lower, "cannot be null") {
		return "23502" // not_null_violation
	}
	if containsAny(lower, "check constraint") {
		return "23514" // check_violation
	}

	// ── Transaction state errors ── Class 25 ─────────────────────────────
	if containsAny(lower, "no active transaction", "not in a transaction", "active transaction required") {
		return "25P01" // no_active_sql_transaction
	}
	if containsAll(lower, "transaction", "already begun") || containsAll(lower, "transaction", "already in") || containsAll(lower, "transaction", "already active") {
		return "25001" // active_sql_transaction
	}
	if containsAny(lower, "domain is required", "domain not set") {
		return "25000" // invalid_transaction_state
	}

	// ── Write conflict / serialisation ── Class 40 ───────────────────────
	if containsAny(lower, "write conflict", "serialization") {
		return "40001" // serialization_failure
	}
	if containsAny(lower, "deadlock") {
		return "40P01" // deadlock_detected
	}

	// ── Invalid input / type ── Class 22 ─────────────────────────────────
	if containsAny(lower, "invalid input", "type mismatch", "cannot cast") {
		return "22P02" // invalid_text_representation
	}
	if containsAny(lower, "out of range") {
		return "22003" // numeric_value_out_of_range
	}
	if containsAny(lower, "division by zero") {
		return "22012" // division_by_zero
	}

	// ── Not found / object does not exist ── Class 42 / 02 ───────────────
	if containsAny(lower, "not found", "does not exist") {
		return "42000" // syntax_error_or_access_rule_violation (generic)
	}

	// ── Default ──────────────────────────────────────────────────────────
	return "XX000" // internal_error
}

// sendFollowerRedirectError sends SQLSTATE 25006 (read_only_sql_transaction)
// with an asql_leader hint so SDK clients can reconnect to the correct leader.
func sendFollowerRedirectError(backend *pgproto3.Backend, leaderAddr string, session *executor.Session) error {
	msg := "not the leader: write must be directed to the current leader"
	hint := ""
	if leaderAddr != "" {
		msg = fmt.Sprintf("not the leader: redirect writes to %s", leaderAddr)
		hint = fmt.Sprintf("asql_leader=%s", leaderAddr)
	}
	return sendMessages(
		backend,
		&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "25006",
			Message:  msg,
			Hint:     hint,
		},
		&pgproto3.ReadyForQuery{TxStatus: txStatus(session)},
	)
}

// containsAny reports whether s contains any of the provided substrings.
func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// containsAll reports whether s contains all of the provided substrings.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
