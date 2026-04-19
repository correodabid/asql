package planner

import (
	"encoding/json"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser"
)

func TestBuildCreateTablePlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("CREATE TABLE users (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("parse create table: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"create_table","table_name":"users","schema":[{"name":"id","type":"INT"},{"name":"email","type":"TEXT"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildCreateTableWithConstraintsPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("parse create table: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"create_table","table_name":"users","schema":[{"name":"id","type":"INT","primary_key":true,"unique":true,"not_null":true},{"name":"email","type":"TEXT","unique":true}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildCreateTableWithForeignKeyPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("CREATE TABLE payments (id INT PRIMARY KEY, user_id INT REFERENCES users(id))")
	if err != nil {
		t.Fatalf("parse create table: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"create_table","table_name":"payments","schema":[{"name":"id","type":"INT","primary_key":true,"unique":true,"not_null":true},{"name":"user_id","type":"INT","references_table":"users","references_column":"id"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildCreateTableWithCheckPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("CREATE TABLE payments (id INT PRIMARY KEY, amount INT CHECK (amount >= 0))")
	if err != nil {
		t.Fatalf("parse create table: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"create_table","table_name":"payments","schema":[{"name":"id","type":"INT","primary_key":true,"unique":true,"not_null":true},{"name":"amount","type":"INT","check":{"column":"amount","operator":"\u003e=","value":{"kind":"number"}}}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildAlterTableAddColumnPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("ALTER TABLE users ADD COLUMN email TEXT")
	if err != nil {
		t.Fatalf("parse alter table add column: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"alter_table_add_column","table_name":"users","alter_column":{"name":"email","type":"TEXT"}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildInsertPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("INSERT INTO users (id, email) VALUES (10, 'x@y.com')")
	if err != nil {
		t.Fatalf("parse insert: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"insert","table_name":"users","columns":["id","email"],"values":[{"kind":"number","number_value":10},{"kind":"string","string_value":"x@y.com"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildUpdatePlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("UPDATE users SET email = 'updated@asql.dev' WHERE id = 10")
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"update","table_name":"users","columns":["email"],"values":[{"kind":"string","string_value":"updated@asql.dev"}],"filter":{"column":"id","operator":"=","value":{"kind":"number","number_value":10}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildDeletePlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("DELETE FROM users WHERE id = 10")
	if err != nil {
		t.Fatalf("parse delete: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"delete","table_name":"users","filter":{"column":"id","operator":"=","value":{"kind":"number","number_value":10}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildCreateIndexPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("CREATE INDEX idx_users_id ON users (id)")
	if err != nil {
		t.Fatalf("parse create index: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"create_index","table_name":"users","index_name":"idx_users_id","index_column":"id"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildCreateIndexBTreePlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("CREATE INDEX idx_users_id_btree ON users (id) USING BTREE")
	if err != nil {
		t.Fatalf("parse create index btree: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"create_index","table_name":"users","index_name":"idx_users_id_btree","index_column":"id","index_method":"btree"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildCreateCompositeIndexBTreePlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("CREATE INDEX idx_users_id_email_btree ON users (id, email) USING BTREE")
	if err != nil {
		t.Fatalf("parse create composite index btree: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"create_index","table_name":"users","index_name":"idx_users_id_email_btree","index_column":"id","index_columns":["id","email"],"index_method":"btree"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM users WHERE id = 10")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"users","columns":["id"],"filter":{"column":"id","operator":"=","value":{"kind":"number","number_value":10}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectRangePlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM users WHERE id >= 10")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"users","columns":["id"],"filter":{"column":"id","operator":"\u003e=","value":{"kind":"number","number_value":10}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectIsNullPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM users WHERE email IS NULL")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"users","columns":["id"],"filter":{"column":"email","operator":"IS NULL","value":{"kind":"null"}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectBooleanPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM users WHERE NOT (id = 1 OR id = 2) AND email IS NOT NULL")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"users","columns":["id"],"filter":{"operator":"AND","value":{"kind":""},"left":{"operator":"NOT","value":{"kind":""},"left":{"operator":"OR","value":{"kind":""},"left":{"column":"id","operator":"=","value":{"kind":"number","number_value":1}},"right":{"column":"id","operator":"=","value":{"kind":"number","number_value":2}}}},"right":{"column":"email","operator":"IS NOT NULL","value":{"kind":"null"}}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectArithmeticPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM payments WHERE amount + fee >= 10")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"payments","columns":["id"],"filter":{"column":"amount + fee","operator":"\u003e=","value":{"kind":"number","number_value":10}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectOrderByLimitPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM users WHERE id = 10 ORDER BY id DESC LIMIT 3")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"users","columns":["id"],"filter":{"column":"id","operator":"=","value":{"kind":"number","number_value":10}},"order_by":[{"column":"id","direction":"DESC"}],"limit":3}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectMultiOrderByPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM users ORDER BY email ASC, id DESC LIMIT 3")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"users","columns":["id"],"order_by":[{"column":"email","direction":"ASC"},{"column":"id","direction":"DESC"}],"limit":3}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectInnerJoinPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT users.id, loans.id FROM users INNER JOIN loans ON users.id = loans.user_id ORDER BY users.id ASC")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"users","columns":["users.id","loans.id"],"joins":[{"join_type":"INNER","table_name":"loans","left_column":"users.id","right_column":"loans.user_id"}],"order_by":[{"column":"users.id","direction":"ASC"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectGroupByHavingPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT user_id, COUNT(*), SUM(amount), AVG(amount) FROM payments GROUP BY user_id HAVING COUNT(*) >= 2 ORDER BY user_id ASC")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"payments","columns":["user_id","count(*)","sum(amount)","avg(amount)"],"group_by":["user_id"],"having":{"column":"count(*)","operator":"\u003e=","value":{"kind":"number","number_value":2}},"order_by":[{"column":"user_id","direction":"ASC"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildSelectAggregateAliasPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("SELECT COUNT(*) AS total FROM batch_orders")
	if err != nil {
		t.Fatalf("parse select aggregate alias: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"select","table_name":"batch_orders","columns":["count(*) as total"]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildForDomainRejectsCrossDomainTable(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM loans.users WHERE id = 10")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	_, err = BuildForDomain(statement, "accounts")
	if err == nil {
		t.Fatal("expected cross-domain access error")
	}
}

func TestBuildForDomainAcceptsQualifiedSameDomain(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM accounts.users WHERE id = 10")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := BuildForDomain(statement, "accounts")
	if err != nil {
		t.Fatalf("build for domain: %v", err)
	}

	if plan.DomainName != "accounts" || plan.TableName != "users" {
		t.Fatalf("unexpected domain/table resolution: domain=%s table=%s", plan.DomainName, plan.TableName)
	}
}

func TestBuildForDomainsResolvesUnqualifiedTableToFirstDomain(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM users WHERE id = 10")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := BuildForDomains(statement, []string{"accounts", "loans"})
	if err != nil {
		t.Fatalf("build for domains: %v", err)
	}

	if plan.DomainName != "accounts" || plan.TableName != "users" {
		t.Fatalf("expected domain=accounts table=users, got domain=%s table=%s", plan.DomainName, plan.TableName)
	}
}

func TestBuildForDomainsAcceptsQualifiedDomainWithinScope(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM loans.users WHERE id = 10")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	plan, err := BuildForDomains(statement, []string{"accounts", "loans"})
	if err != nil {
		t.Fatalf("build for domains: %v", err)
	}

	if plan.DomainName != "loans" || plan.TableName != "users" {
		t.Fatalf("unexpected domain/table resolution: domain=%s table=%s", plan.DomainName, plan.TableName)
	}
}

func TestBuildPlanSnapshotsRemainDeterministicAcrossRepeatedRuns(t *testing.T) {
	testCases := []struct {
		name    string
		sql     string
		domains []string
	}{
		{
			name: "select",
			sql:  "SELECT id FROM users WHERE id = 10 ORDER BY id DESC LIMIT 3",
		},
		{
			name: "join",
			sql:  "SELECT users.id, loans.id FROM users INNER JOIN loans ON users.id = loans.user_id ORDER BY users.id ASC",
		},
		{
			name: "group-by-having",
			sql:  "SELECT user_id, COUNT(*), SUM(amount), AVG(amount) FROM payments GROUP BY user_id HAVING COUNT(*) >= 2 ORDER BY user_id ASC",
		},
		{
			name:    "scoped-qualified-domain",
			sql:     "SELECT id FROM loans.users WHERE id = 10",
			domains: []string{" ACCOUNTS ", "loans", "Loans", "accounts"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			baseline := ""
			for i := 0; i < 32; i++ {
				statement, err := parser.Parse(tc.sql)
				if err != nil {
					t.Fatalf("parse sql: %v", err)
				}

				var plan Plan
				if len(tc.domains) == 0 {
					plan, err = Build(statement)
				} else {
					plan, err = BuildForDomains(statement, tc.domains)
				}
				if err != nil {
					t.Fatalf("build plan: %v", err)
				}

				bytes, err := json.Marshal(plan)
				if err != nil {
					t.Fatalf("marshal plan: %v", err)
				}

				snapshot := string(bytes)
				if i == 0 {
					baseline = snapshot
					continue
				}

				if snapshot != baseline {
					t.Fatalf("planner snapshot drift detected at iteration %d\n got: %s\nwant: %s", i, snapshot, baseline)
				}
			}
		})
	}
}

func TestBuildForDomainsSnapshotStableAcrossDomainOrder(t *testing.T) {
	statement, err := parser.Parse("SELECT id FROM loans.users WHERE id = 10")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	domainPermutations := [][]string{
		{"accounts", "loans"},
		{"LOANS", "ACCOUNTS"},
		{"loans", "accounts", "accounts"},
	}

	baseline := ""
	for i, domains := range domainPermutations {
		plan, err := BuildForDomains(statement, domains)
		if err != nil {
			t.Fatalf("build for domains %v: %v", domains, err)
		}

		bytes, err := json.Marshal(plan)
		if err != nil {
			t.Fatalf("marshal plan: %v", err)
		}

		snapshot := string(bytes)
		if i == 0 {
			baseline = snapshot
			continue
		}

		if snapshot != baseline {
			t.Fatalf("domain-order snapshot drift\n got: %s\nwant: %s", snapshot, baseline)
		}
	}
}

func TestBuildCreateEntityPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("CREATE ENTITY order_aggregate (ROOT orders, INCLUDES order_lines, order_approvals)")
	if err != nil {
		t.Fatalf("parse create entity: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	bytes, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal plan: %v", err)
	}

	expected := `{"operation":"create_entity","table_name":"","entity_name":"order_aggregate","entity_root_table":"orders","entity_tables":["orders","order_lines","order_approvals"]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestBuildCreateEntityIfNotExistsPlanSnapshot(t *testing.T) {
	statement, err := parser.Parse("CREATE ENTITY IF NOT EXISTS order_aggregate (ROOT orders, INCLUDES order_lines)")
	if err != nil {
		t.Fatalf("parse create entity: %v", err)
	}

	plan, err := Build(statement)
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}

	if plan.Operation != OperationCreateEntity {
		t.Fatalf("expected operation %q, got %q", OperationCreateEntity, plan.Operation)
	}
	if !plan.IfNotExists {
		t.Fatal("expected IfNotExists=true")
	}
	if plan.EntityName != "order_aggregate" {
		t.Fatalf("expected entity name 'order_aggregate', got %q", plan.EntityName)
	}
}

func TestBuildCreateEntityPlanForDomain(t *testing.T) {
	statement, err := parser.Parse("CREATE ENTITY order_aggregate (ROOT orders, INCLUDES order_lines)")
	if err != nil {
		t.Fatalf("parse create entity: %v", err)
	}

	plan, err := BuildForDomain(statement, "accounts")
	if err != nil {
		t.Fatalf("build for domain: %v", err)
	}

	if plan.DomainName != "accounts" {
		t.Fatalf("expected domain 'accounts', got %q", plan.DomainName)
	}
	if plan.EntityName != "order_aggregate" {
		t.Fatalf("expected entity name 'order_aggregate', got %q", plan.EntityName)
	}
}
