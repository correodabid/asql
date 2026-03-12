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
	if contains(lower, "syntax", "parse error", "unexpected token", "expected") {
		return "42601" // syntax_error
	}
	if contains(lower, "undefined function", "function does not exist") {
		return "42883" // undefined_function
	}
	if contains(lower, "column", "does not exist", "not found") && contains(lower, "column") {
		return "42703" // undefined_column
	}
	if contains(lower, "table", "does not exist", "not found") && contains(lower, "table") {
		return "42P01" // undefined_table
	}
	if contains(lower, "already exists") {
		return "42P07" // duplicate_table (used generically for duplicate objects)
	}

	// ── Integrity / constraint violations ── Class 23 ────────────────────
	if contains(lower, "unique constraint", "duplicate key") {
		return "23505" // unique_violation
	}
	if contains(lower, "foreign key", "references") {
		return "23503" // foreign_key_violation
	}
	if contains(lower, "not null", "null value") {
		return "23502" // not_null_violation
	}
	if contains(lower, "check constraint") {
		return "23514" // check_violation
	}

	// ── Transaction state errors ── Class 25 ─────────────────────────────
	if contains(lower, "no active transaction", "not in a transaction") {
		return "25P01" // no_active_sql_transaction
	}
	if contains(lower, "transaction", "already begun", "already in") {
		return "25001" // active_sql_transaction
	}
	if contains(lower, "domain is required", "domain not set") {
		return "25000" // invalid_transaction_state
	}

	// ── Write conflict / serialisation ── Class 40 ───────────────────────
	if contains(lower, "write conflict", "serialization", "conflict") {
		return "40001" // serialization_failure
	}
	if contains(lower, "deadlock") {
		return "40P01" // deadlock_detected
	}

	// ── Invalid input / type ── Class 22 ─────────────────────────────────
	if contains(lower, "invalid input", "type mismatch", "cannot cast") {
		return "22P02" // invalid_text_representation
	}
	if contains(lower, "out of range") {
		return "22003" // numeric_value_out_of_range
	}
	if contains(lower, "division by zero") {
		return "22012" // division_by_zero
	}

	// ── Not found / object does not exist ── Class 42 / 02 ───────────────
	if contains(lower, "not found", "does not exist") {
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

// contains reports whether s contains any of the provided substrings.
func contains(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
