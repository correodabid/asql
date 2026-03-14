package pgwire

import (
	"errors"
	"testing"
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
		{name: "foreign key violation", msg: "foreign key references missing parent", want: "23503"},
		{name: "not null violation", msg: "null value violates not null constraint", want: "23502"},
		{name: "check violation", msg: "check constraint age_positive failed", want: "23514"},
		{name: "no active transaction", msg: "no active transaction", want: "25P01"},
		{name: "already in transaction", msg: "transaction already begun", want: "25001"},
		{name: "domain required", msg: "domain is required before executing statement", want: "25000"},
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
