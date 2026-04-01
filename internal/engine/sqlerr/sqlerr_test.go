package sqlerr_test

import (
	"errors"
	"fmt"
	"testing"

	"asql/internal/engine/sqlerr"
)

func TestSQLError_Error(t *testing.T) {
	err := sqlerr.New("42P01", "table not found")
	if err.Error() != "table not found" {
		t.Fatalf("got %q, want %q", err.Error(), "table not found")
	}
}

func TestErrorsAs_Bare(t *testing.T) {
	sentinel := sqlerr.New("42P01", "table not found")
	var target *sqlerr.SQLError
	if !errors.As(sentinel, &target) {
		t.Fatal("errors.As failed on bare *SQLError")
	}
	if target.Code != "42P01" {
		t.Fatalf("got code %q, want %q", target.Code, "42P01")
	}
}

func TestErrorsAs_Wrapped(t *testing.T) {
	sentinel := sqlerr.New("23505", "constraint violation")
	wrapped := fmt.Errorf("%w: unique index on users.email", sentinel)

	var target *sqlerr.SQLError
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As failed on wrapped *SQLError")
	}
	if target.Code != "23505" {
		t.Fatalf("got code %q, want %q", target.Code, "23505")
	}
}

func TestErrorsAs_DoubleWrapped(t *testing.T) {
	sentinel := sqlerr.New("40001", "write conflict detected")
	inner := fmt.Errorf("%w: table orders changed at ts=5", sentinel)
	outer := fmt.Errorf("execute mutation: %w", inner)

	var target *sqlerr.SQLError
	if !errors.As(outer, &target) {
		t.Fatal("errors.As failed on double-wrapped *SQLError")
	}
	if target.Code != "40001" {
		t.Fatalf("got code %q, want %q", target.Code, "40001")
	}
}

func TestErrorsIs_Sentinel(t *testing.T) {
	sentinel := sqlerr.New("42P01", "table not found")
	wrapped := fmt.Errorf("%w: users", sentinel)
	if !errors.Is(wrapped, sentinel) {
		t.Fatal("errors.Is failed for wrapped sentinel")
	}
}

func TestErrorsAs_NonSQLError(t *testing.T) {
	plain := errors.New("some plain error")
	var target *sqlerr.SQLError
	if errors.As(plain, &target) {
		t.Fatal("errors.As should not match plain error")
	}
}
