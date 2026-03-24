package studioapp

import (
	"reflect"
	"testing"
)

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

func TestPreprocessImportedReadSQLRewritesImportToCTE(t *testing.T) {
	domains, sql, err := preprocessImportedReadSQL(
		[]string{"sales"},
		"IMPORT customers.customers AS customer; SELECT customer.id FROM orders JOIN customer ON orders.customer_id = customer.id",
	)
	if err != nil {
		t.Fatalf("preprocessImportedReadSQL: %v", err)
	}
	if !reflect.DeepEqual(domains, []string{"sales", "customers"}) {
		t.Fatalf("unexpected domains: %#v", domains)
	}
	want := "WITH customer AS (SELECT * FROM customers.customers) SELECT customer.id FROM orders JOIN customer ON orders.customer_id = customer.id"
	if sql != want {
		t.Fatalf("unexpected rewritten sql:\n got: %s\nwant: %s", sql, want)
	}
}

func TestPreprocessImportedReadSQLMergesWithExistingWithClause(t *testing.T) {
	domains, sql, err := preprocessImportedReadSQL(
		[]string{"sales"},
		"IMPORT customers.customers AS customer; WITH top_orders AS (SELECT * FROM orders) SELECT * FROM top_orders JOIN customer ON top_orders.customer_id = customer.id",
	)
	if err != nil {
		t.Fatalf("preprocessImportedReadSQL: %v", err)
	}
	if !reflect.DeepEqual(domains, []string{"sales", "customers"}) {
		t.Fatalf("unexpected domains: %#v", domains)
	}
	want := "WITH customer AS (SELECT * FROM customers.customers), top_orders AS (SELECT * FROM orders) SELECT * FROM top_orders JOIN customer ON top_orders.customer_id = customer.id"
	if sql != want {
		t.Fatalf("unexpected rewritten sql:\n got: %s\nwant: %s", sql, want)
	}
}
