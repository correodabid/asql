package studioapp

import "testing"

func TestNormalizeExplainSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "plain select", sql: "SELECT * FROM sensors LIMIT 100", want: "EXPLAIN SELECT * FROM sensors LIMIT 100"},
		{name: "single explain", sql: "EXPLAIN SELECT * FROM sensors LIMIT 100", want: "EXPLAIN SELECT * FROM sensors LIMIT 100"},
		{name: "repeated explain", sql: "EXPLAIN explain SELECT * FROM process_orders LIMIT 100", want: "EXPLAIN SELECT * FROM process_orders LIMIT 100"},
		{name: "mixed whitespace", sql: "  EXPLAIN\n\tEXPLAIN   SELECT id FROM users  ", want: "EXPLAIN SELECT id FROM users"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeExplainSQL(tt.sql); got != tt.want {
				t.Fatalf("normalizeExplainSQL(%q) = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
}
