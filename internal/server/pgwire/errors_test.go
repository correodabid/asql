package pgwire

import (
	"errors"
	"fmt"
	"testing"

	"asql/internal/engine/sqlerr"
)

func TestSQLStateFromMessageMappings(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want string
	}{
		{name: "syntax error", msg: "syntax error near FROM", want: "42601"},
		{name: "undefined function", msg: "function does not exist: nowz()", want: "42883"},
		{name: "undefined column", msg: "column emailz does not exist", want: "42703"},
		{name: "undefined table", msg: "table users_archive does not exist", want: "42P01"},
		{name: "duplicate object", msg: "table users already exists", want: "42P07"},
		{name: "unique violation", msg: "duplicate key value violates unique constraint users_pkey", want: "23505"},
		{name: "primary key duplicate", msg: "constraint violation: primary key id duplicate value 1", want: "23505"},
		{name: "unique duplicate", msg: "constraint violation: unique email duplicate value one@asql.dev", want: "23505"},
		{name: "foreign key violation", msg: "foreign key references missing parent", want: "23503"},
		{name: "not null violation", msg: "null value violates not null constraint", want: "23502"},
		{name: "cannot be null violation", msg: "constraint violation: column email cannot be null", want: "23502"},
		{name: "check violation", msg: "check constraint age_positive failed", want: "23514"},
		{name: "no active transaction", msg: "no active transaction", want: "25P01"},
		{name: "active transaction required", msg: "active transaction required", want: "25P01"},
		{name: "already in transaction", msg: "transaction already begun", want: "25001"},
		{name: "transaction already active", msg: "transaction already active", want: "25001"},
		{name: "domain required", msg: "domain is required before executing statement", want: "25000"},
		{name: "explicit privilege required", msg: "SELECT_HISTORY privilege required", want: "42501"},
		{name: "serialization failure", msg: "write conflict detected during commit", want: "40001"},
		{name: "deadlock detected", msg: "deadlock detected while waiting for lock", want: "40P01"},
		{name: "invalid input", msg: "invalid input syntax for integer", want: "22P02"},
		{name: "out of range", msg: "value out of range for int4", want: "22003"},
		{name: "division by zero", msg: "division by zero", want: "22012"},
		{name: "generic not found", msg: "object does not exist", want: "42000"},
		{name: "default internal", msg: "unexpected executor panic wrapper", want: "XX000"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sqlStateFromMessage(tc.msg); got != tc.want {
				t.Fatalf("sqlStateFromMessage(%q) = %s, want %s", tc.msg, got, tc.want)
			}
		})
	}
}

func TestMapErrorToSQLState(t *testing.T) {
	if got := mapErrorToSQLState(nil); got != "00000" {
		t.Fatalf("mapErrorToSQLState(nil) = %s, want 00000", got)
	}

	err := errors.New("duplicate key value violates unique constraint users_pkey")
	if got := mapErrorToSQLState(err); got != "23505" {
		t.Fatalf("mapErrorToSQLState(err) = %s, want 23505", got)
	}
}

func TestMapErrorToSQLState_TypedError(t *testing.T) {
	sentinel := sqlerr.New("42P01", "table not found")
	if got := mapErrorToSQLState(sentinel); got != "42P01" {
		t.Fatalf("mapErrorToSQLState(typed) = %s, want 42P01", got)
	}
}

func TestMapErrorToSQLState_WrappedTypedError(t *testing.T) {
	sentinel := sqlerr.New("23505", "constraint violation")
	wrapped := fmt.Errorf("%w: unique index on users.email", sentinel)
	if got := mapErrorToSQLState(wrapped); got != "23505" {
		t.Fatalf("mapErrorToSQLState(wrapped) = %s, want 23505", got)
	}
}

func TestMapErrorToSQLState_DoubleWrappedTypedError(t *testing.T) {
	sentinel := sqlerr.New("40001", "write conflict detected")
	inner := fmt.Errorf("%w: table orders changed at ts=5", sentinel)
	outer := fmt.Errorf("execute mutation: %w", inner)
	if got := mapErrorToSQLState(outer); got != "40001" {
		t.Fatalf("mapErrorToSQLState(double-wrapped) = %s, want 40001", got)
	}
}

func TestMapErrorToSQLState_PlainErrorFallsBackToStringMatch(t *testing.T) {
	plain := errors.New("division by zero")
	if got := mapErrorToSQLState(plain); got != "22012" {
		t.Fatalf("mapErrorToSQLState(plain) = %s, want 22012", got)
	}
}
