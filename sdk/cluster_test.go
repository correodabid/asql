package asqlsdk

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestParseRedirectHint(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantAddr string
	}{
		{
			name:     "not a pg error",
			err:      errors.New("connection refused"),
			wantAddr: "",
		},
		{
			name:     "pg error wrong code",
			err:      &pgconn.PgError{Code: "42601", Message: "syntax error"},
			wantAddr: "",
		},
		{
			name:     "pg error 25006 without hint",
			err:      &pgconn.PgError{Code: "25006", Message: "not the leader"},
			wantAddr: "",
		},
		{
			name:     "pg error 25006 with hint",
			err:      &pgconn.PgError{Code: "25006", Message: "not the leader: redirect writes to 127.0.0.1:5434", Hint: "asql_leader=127.0.0.1:5434"},
			wantAddr: "127.0.0.1:5434",
		},
		{
			name:     "hint with extra words before leader key",
			err:      &pgconn.PgError{Code: "25006", Message: "not the leader", Hint: "connect to the leader node asql_leader=10.0.0.2:5433"},
			wantAddr: "10.0.0.2:5433",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseRedirectHint(tc.err)
			if got != tc.wantAddr {
				t.Errorf("parseRedirectHint() = %q, want %q", got, tc.wantAddr)
			}
		})
	}
}

func TestDedupe(t *testing.T) {
	in := []string{"a", "b", "a", "c", "b", "d"}
	got := dedupe(in)
	want := []string{"a", "b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("dedupe() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("dedupe()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestConnString(t *testing.T) {
	cfg := Config{User: "alice", Password: "secret", Database: "mydb"}
	dsn := connString(cfg, "10.1.2.3:5435")
	for _, want := range []string{"host=10.1.2.3", "port=5435", "user=alice", "dbname=mydb", "password=secret", "sslmode=disable"} {
		if !containsStr(dsn, want) {
			t.Errorf("connString() = %q, missing %q", dsn, want)
		}
	}
}

func TestConnStringDefaultsNoPort(t *testing.T) {
	cfg := Config{}
	dsn := connString(cfg, "myhost")
	for _, want := range []string{"host=myhost", "port=5433", "user=asql", "dbname=asql"} {
		if !containsStr(dsn, want) {
			t.Errorf("connString() = %q, missing %q", dsn, want)
		}
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
