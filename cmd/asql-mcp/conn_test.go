package main

import "testing"

func TestIsReadOnlySQL(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		// Read-only shapes we accept.
		{"SELECT 1", true},
		{"select * from foo", true},
		{"  SELECT\t*\nFROM foo  ", true},
		{"WITH x AS (SELECT 1) SELECT * FROM x", true},
		{"TABLE foo", true},
		{"EXPLAIN SELECT 1", true},
		{"SHOW search_path", true},
		{"TAIL ENTITY CHANGES billing.invoice FOR '1' LIMIT 10", true},
		{"SELECT 1;", true}, // single trailing semicolon is fine
		{"-- comment\nSELECT 1", true},
		{"/* block */ SELECT 1", true},

		// Things that must NOT slip through.
		{"INSERT INTO foo VALUES (1)", false},
		{"UPDATE foo SET x = 1", false},
		{"DELETE FROM foo", false},
		{"CREATE TABLE foo (id INT)", false},
		{"DROP TABLE foo", false},
		{"ALTER TABLE foo ADD COLUMN x INT", false},
		{"TRUNCATE foo", false},
		{"BEGIN", false},
		{"COMMIT", false},
		{"ROLLBACK", false},
		{"", false},
		{"  ", false},

		// Chained statements are rejected to prevent "SELECT 1; DELETE FROM ..." injection.
		{"SELECT 1; DELETE FROM foo", false},
		{"SELECT 1; SELECT 2", false},

		// Unterminated comment is rejected rather than guessed at.
		{"/* unterminated comment SELECT 1", false},
		{"-- unterminated comment", false},

		// Case-insensitive keyword check.
		{"select 1", true},
		{"UPDATE foo", false},
	}
	for _, tc := range cases {
		got := isReadOnlySQL(tc.sql)
		if got != tc.want {
			t.Errorf("isReadOnlySQL(%q) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}
