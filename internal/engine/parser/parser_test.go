package parser

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

func TestParseCreateTableSnapshot(t *testing.T) {
	statement, err := Parse("CREATE TABLE Users (ID INT, Email TEXT, Meta JSON);")
	if err != nil {
		t.Fatalf("parse create table: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","columns":[{"name":"id","type":"INT"},{"name":"email","type":"TEXT"},{"name":"meta","type":"JSON"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseCreateTableWithConstraintsSnapshot(t *testing.T) {
	statement, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, nickname TEXT);")
	if err != nil {
		t.Fatalf("parse create table with constraints: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","columns":[{"name":"id","type":"INT","primary_key":true,"unique":true,"not_null":true},{"name":"email","type":"TEXT","unique":true},{"name":"nickname","type":"TEXT"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseCreateTableWithForeignKeySnapshot(t *testing.T) {
	statement, err := Parse("CREATE TABLE payments (id INT PRIMARY KEY, user_id INT REFERENCES users(id), amount INT);")
	if err != nil {
		t.Fatalf("parse create table with foreign key: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"payments","columns":[{"name":"id","type":"INT","primary_key":true,"unique":true,"not_null":true},{"name":"user_id","type":"INT","references_table":"users","references_column":"id"},{"name":"amount","type":"INT"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseCreateTableWithCheckSnapshot(t *testing.T) {
	statement, err := Parse("CREATE TABLE payments (id INT PRIMARY KEY, amount INT CHECK (amount >= 0));")
	if err != nil {
		t.Fatalf("parse create table with check: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"payments","columns":[{"name":"id","type":"INT","primary_key":true,"unique":true,"not_null":true},{"name":"amount","type":"INT","check":{"column":"amount","operator":"\u003e=","value":{"kind":"number"}}}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseAlterTableAddColumnSnapshot(t *testing.T) {
	statement, err := Parse("ALTER TABLE users ADD COLUMN email TEXT;")
	if err != nil {
		t.Fatalf("parse alter table add column: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","column":{"name":"email","type":"TEXT"}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseAlterTableAddColumnWithDefaultNotNullSnapshot(t *testing.T) {
	statement, err := Parse("ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'planned' NOT NULL;")
	if err != nil {
		t.Fatalf("parse alter table add column with default/not null: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","column":{"name":"status","type":"TEXT","not_null":true,"default_value":{"kind":"literal","value":{"kind":"string","string_value":"planned"}}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseAlterTableAddColumnWithReferencesSnapshot(t *testing.T) {
	statement, err := Parse("ALTER TABLE orders ADD COLUMN user_id INT NOT NULL REFERENCES users(id);")
	if err != nil {
		t.Fatalf("parse alter table add column with references: %v", err)
	}
	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}
	expected := `{"table_name":"orders","column":{"name":"user_id","type":"INT","not_null":true,"references_table":"users","references_column":"id"}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseAlterTableAddColumnRejectsUnsupportedConstraints(t *testing.T) {
	_, err := Parse("ALTER TABLE users ADD COLUMN email TEXT UNIQUE;")
	if err == nil {
		t.Fatal("expected add column constraints to be rejected")
	}
}

func TestParseAlterTableAddColumnRejectsNonLiteralDefault(t *testing.T) {
	_, err := Parse("ALTER TABLE users ADD COLUMN id TEXT DEFAULT UUID_V7;")
	if err == nil {
		t.Fatal("expected non-literal add column default to be rejected")
	}
}

func TestParseInsertSnapshot(t *testing.T) {
	statement, err := Parse("INSERT INTO users (id, email, age) VALUES (1, 'a@b.com', 42);")
	if err != nil {
		t.Fatalf("parse insert: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","columns":["id","email","age"],"values":[{"kind":"number","number_value":1},{"kind":"string","string_value":"a@b.com"},{"kind":"number","number_value":42}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseUpdateSnapshot(t *testing.T) {
	statement, err := Parse("UPDATE users SET email = 'new@asql.dev', age = 43 WHERE id = 1;")
	if err != nil {
		t.Fatalf("parse update: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","columns":["email","age"],"values":[{"kind":"string","string_value":"new@asql.dev"},{"kind":"number","number_value":43}],"where":{"column":"id","operator":"=","value":{"kind":"number","number_value":1}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseDeleteSnapshot(t *testing.T) {
	statement, err := Parse("DELETE FROM users WHERE id = 1;")
	if err != nil {
		t.Fatalf("parse delete: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","where":{"column":"id","operator":"=","value":{"kind":"number","number_value":1}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseCreateIndexSnapshot(t *testing.T) {
	statement, err := Parse("CREATE INDEX idx_users_id ON users (id);")
	if err != nil {
		t.Fatalf("parse create index: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"index_name":"idx_users_id","table_name":"users","column":"id"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseCreateIndexBTreeSnapshot(t *testing.T) {
	statement, err := Parse("CREATE INDEX idx_users_id_btree ON users (id) USING BTREE;")
	if err != nil {
		t.Fatalf("parse create index btree: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"index_name":"idx_users_id_btree","table_name":"users","column":"id","method":"btree"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseCreateCompositeIndexBTreeSnapshot(t *testing.T) {
	statement, err := Parse("CREATE INDEX idx_users_id_email_btree ON users (id, email) USING BTREE;")
	if err != nil {
		t.Fatalf("parse create composite index btree: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"index_name":"idx_users_id_email_btree","table_name":"users","column":"id","columns":["id","email"],"method":"btree"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id, email FROM users WHERE id = 7;")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["id","email"],"table_name":"users","where":{"column":"id","operator":"=","value":{"kind":"number","number_value":7}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectRangeWhereSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id FROM users WHERE id >= 7;")
	if err != nil {
		t.Fatalf("parse select range where: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["id"],"table_name":"users","where":{"column":"id","operator":"\u003e=","value":{"kind":"number","number_value":7}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectIsNullWhereSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id FROM users WHERE email IS NULL;")
	if err != nil {
		t.Fatalf("parse select IS NULL where: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["id"],"table_name":"users","where":{"column":"email","operator":"IS NULL","value":{"kind":"null"}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectIsNotNullWhereSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id FROM users WHERE email IS NOT NULL;")
	if err != nil {
		t.Fatalf("parse select IS NOT NULL where: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["id"],"table_name":"users","where":{"column":"email","operator":"IS NOT NULL","value":{"kind":"null"}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectBooleanWhereSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id FROM users WHERE NOT (id = 1 OR id = 2) AND email IS NOT NULL;")
	if err != nil {
		t.Fatalf("parse select boolean where: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["id"],"table_name":"users","where":{"operator":"AND","value":{"kind":""},"left":{"operator":"NOT","value":{"kind":""},"left":{"operator":"OR","value":{"kind":""},"left":{"column":"id","operator":"=","value":{"kind":"number","number_value":1}},"right":{"column":"id","operator":"=","value":{"kind":"number","number_value":2}}}},"right":{"column":"email","operator":"IS NOT NULL","value":{"kind":"null"}}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectArithmeticWhereSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id FROM payments WHERE amount + fee >= 10;")
	if err != nil {
		t.Fatalf("parse select arithmetic where: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["id"],"table_name":"payments","where":{"column":"amount + fee","operator":"\u003e=","value":{"kind":"number","number_value":10}}}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectOrderByLimitSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id, email FROM users WHERE id = 7 ORDER BY email DESC LIMIT 5;")
	if err != nil {
		t.Fatalf("parse select with order by limit: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["id","email"],"table_name":"users","where":{"column":"id","operator":"=","value":{"kind":"number","number_value":7}},"order_by":[{"column":"email","direction":"DESC"}],"limit":5}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectMultiOrderBySnapshot(t *testing.T) {
	statement, err := Parse("SELECT id, email FROM users ORDER BY email ASC, id DESC LIMIT 5;")
	if err != nil {
		t.Fatalf("parse select with multi order by: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["id","email"],"table_name":"users","order_by":[{"column":"email","direction":"ASC"},{"column":"id","direction":"DESC"}],"limit":5}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectInnerJoinSnapshot(t *testing.T) {
	statement, err := Parse("SELECT users.id, loans.id FROM users INNER JOIN loans ON users.id = loans.user_id ORDER BY users.id ASC;")
	if err != nil {
		t.Fatalf("parse select with inner join: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["users.id","loans.id"],"table_name":"users","joins":[{"join_type":"INNER","table_name":"loans","left_column":"users.id","right_column":"loans.user_id"}],"order_by":[{"column":"users.id","direction":"ASC"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectGroupByHavingSnapshot(t *testing.T) {
	statement, err := Parse("SELECT user_id, COUNT(*), SUM(amount), AVG(amount) FROM payments GROUP BY user_id HAVING COUNT(*) >= 2 ORDER BY user_id ASC;")
	if err != nil {
		t.Fatalf("parse select group by having: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["user_id","count(*)","sum(amount)","avg(amount)"],"table_name":"payments","group_by":["user_id"],"having":{"column":"count(*)","operator":"\u003e=","value":{"kind":"number","number_value":2}},"order_by":[{"column":"user_id","direction":"ASC"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseSelectAggregateAliasSnapshot(t *testing.T) {
	statement, err := Parse("SELECT COUNT(*) AS total FROM batch_orders;")
	if err != nil {
		t.Fatalf("parse select aggregate alias: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"columns":["count(*) as total"],"table_name":"batch_orders"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseBeginCrossDomainSnapshot(t *testing.T) {
	statement, err := Parse("BEGIN CROSS DOMAIN loans, accounts, accounts;")
	if err != nil {
		t.Fatalf("parse begin cross domain: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"domains":["accounts","loans"]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseUnsupportedStatement(t *testing.T) {
	_, err := Parse("UPSERT users SET email = 'x' WHERE id = 1")
	if err == nil {
		t.Fatal("expected unsupported statement error")
	}
}

func TestParseUnsupportedStatementProvidesGuidanceForAnyPredicate(t *testing.T) {
	_, err := Parse("SELECT id FROM users WHERE id = ANY(ARRAY[1,2])")
	if err == nil {
		t.Fatal("expected unsupported statement error")
	}
	if !strings.Contains(err.Error(), "ANY(...)") {
		t.Fatalf("expected ANY guidance, got %v", err)
	}
}

func TestParseUnsupportedStatementProvidesGuidanceForBareBegin(t *testing.T) {
	_, err := Parse("BEGIN")
	if err == nil {
		t.Fatal("expected unsupported statement error")
	}
	if !strings.Contains(err.Error(), "BEGIN DOMAIN") {
		t.Fatalf("expected BEGIN DOMAIN guidance, got %v", err)
	}
}

func TestParseCreateTableNewDataTypes(t *testing.T) {
	statement, err := Parse("CREATE TABLE metrics (id INT, active BOOL, score FLOAT, created TIMESTAMP);")
	if err != nil {
		t.Fatalf("parse create table with new types: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"metrics","columns":[{"name":"id","type":"INT"},{"name":"active","type":"BOOL"},{"name":"score","type":"FLOAT"},{"name":"created","type":"TIMESTAMP"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseBoolLiteral(t *testing.T) {
	statement, err := Parse("INSERT INTO flags (id, active) VALUES (1, TRUE);")
	if err != nil {
		t.Fatalf("parse insert with bool: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"flags","columns":["id","active"],"values":[{"kind":"number","number_value":1},{"kind":"boolean","bool_value":true}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseBoolFalseLiteral(t *testing.T) {
	statement, err := Parse("INSERT INTO flags (id, active) VALUES (2, FALSE);")
	if err != nil {
		t.Fatalf("parse insert with false: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"flags","columns":["id","active"],"values":[{"kind":"number","number_value":2},{"kind":"boolean"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseFloatLiteral(t *testing.T) {
	statement, err := Parse("INSERT INTO prices (id, amount) VALUES (1, 19.99);")
	if err != nil {
		t.Fatalf("parse insert with float: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"prices","columns":["id","amount"],"values":[{"kind":"number","number_value":1},{"kind":"float","float_value":19.99}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseTimestampLiteral(t *testing.T) {
	statement, err := Parse("INSERT INTO events (id, created) VALUES (1, '2024-01-15T10:30:00Z');")
	if err != nil {
		t.Fatalf("parse insert with timestamp: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	// 2024-01-15T10:30:00Z in microseconds since epoch
	expected := `{"table_name":"events","columns":["id","created"],"values":[{"kind":"number","number_value":1},{"kind":"timestamp","number_value":1705314600000000}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseStringNotTimestamp(t *testing.T) {
	statement, err := Parse("INSERT INTO users (name) VALUES ('hello');")
	if err != nil {
		t.Fatalf("parse insert with string: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","columns":["name"],"values":[{"kind":"string","string_value":"hello"}]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseFloatFormats(t *testing.T) {
	tests := []struct {
		input string
		value float64
	}{
		{"1.0", 1.0},
		{"-0.5", -0.5},
		{"3.14", 3.14},
	}

	for _, tt := range tests {
		stmt, err := Parse("INSERT INTO t (v) VALUES (" + tt.input + ");")
		if err != nil {
			t.Fatalf("parse float %s: %v", tt.input, err)
		}
		bytes, _ := json.Marshal(stmt)
		t.Logf("float %s -> %s", tt.input, string(bytes))
	}
}

func TestParseSubqueryIN(t *testing.T) {
	stmt, err := Parse("SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers)")
	if err != nil {
		t.Fatalf("parse IN subquery: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where == nil || sel.Where.Operator != "IN" || sel.Where.Column != "customer_id" {
		t.Fatalf("expected IN predicate on customer_id, got %+v", sel.Where)
	}
	if sel.Where.Subquery == nil {
		t.Fatal("expected subquery on IN predicate")
	}
	if sel.Where.Subquery.Statement.TableName != "customers" {
		t.Fatalf("subquery table: got %q, want %q", sel.Where.Subquery.Statement.TableName, "customers")
	}
}

func TestParseSubqueryNOTIN(t *testing.T) {
	stmt, err := Parse("SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM blocked_users)")
	if err != nil {
		t.Fatalf("parse NOT IN subquery: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where == nil || sel.Where.Operator != "NOT IN" || sel.Where.Column != "customer_id" {
		t.Fatalf("expected NOT IN predicate on customer_id, got %+v", sel.Where)
	}
	if sel.Where.Subquery == nil {
		t.Fatal("expected subquery on NOT IN predicate")
	}
	if sel.Where.Subquery.Statement.TableName != "blocked_users" {
		t.Fatalf("subquery table: got %q, want %q", sel.Where.Subquery.Statement.TableName, "blocked_users")
	}
}

func TestParseSubqueryScalar(t *testing.T) {
	stmt, err := Parse("SELECT * FROM orders WHERE total = (SELECT max_total FROM limits LIMIT 1)")
	if err != nil {
		t.Fatalf("parse scalar subquery: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where == nil || sel.Where.Operator != "=" || sel.Where.Column != "total" {
		t.Fatalf("expected = predicate on total, got %+v", sel.Where)
	}
	if sel.Where.Subquery == nil {
		t.Fatal("expected subquery on scalar predicate")
	}
	sub := sel.Where.Subquery.Statement
	if sub.TableName != "limits" || sub.Limit == nil || *sub.Limit != 1 {
		t.Fatalf("subquery: got table=%q limit=%v, want limits/1", sub.TableName, sub.Limit)
	}
}

func TestParseSubqueryEXISTS(t *testing.T) {
	stmt, err := Parse("SELECT * FROM orders WHERE EXISTS (SELECT id FROM payments WHERE amount > 0)")
	if err != nil {
		t.Fatalf("parse EXISTS subquery: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where == nil || sel.Where.Operator != "EXISTS" {
		t.Fatalf("expected EXISTS predicate, got %+v", sel.Where)
	}
	if sel.Where.Subquery == nil {
		t.Fatal("expected subquery on EXISTS predicate")
	}
	if sel.Where.Subquery.Statement.TableName != "payments" {
		t.Fatalf("subquery table: got %q, want %q", sel.Where.Subquery.Statement.TableName, "payments")
	}
}

func TestParseSubqueryNOTEXISTS(t *testing.T) {
	stmt, err := Parse("SELECT * FROM orders WHERE NOT EXISTS (SELECT id FROM refunds)")
	if err != nil {
		t.Fatalf("parse NOT EXISTS subquery: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where == nil || sel.Where.Operator != "NOT" {
		t.Fatalf("expected NOT predicate wrapping EXISTS, got %+v", sel.Where)
	}
	inner := sel.Where.Left
	if inner == nil || inner.Operator != "EXISTS" {
		t.Fatalf("expected inner EXISTS predicate, got %+v", inner)
	}
	if inner.Subquery == nil {
		t.Fatal("expected subquery on EXISTS predicate")
	}
	if inner.Subquery.Statement.TableName != "refunds" {
		t.Fatalf("subquery table: got %q, want %q", inner.Subquery.Statement.TableName, "refunds")
	}
}

func TestParseSubqueryINWithOuterOrderBy(t *testing.T) {
	// Ensures depth-aware firstClauseIndex doesn't match ORDER BY inside subquery
	stmt, err := Parse("SELECT * FROM orders WHERE id IN (SELECT id FROM t2 ORDER BY id LIMIT 5) ORDER BY name")
	if err != nil {
		t.Fatalf("parse IN subquery with outer ORDER BY: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where == nil || sel.Where.Operator != "IN" {
		t.Fatalf("expected IN predicate, got %+v", sel.Where)
	}
	if len(sel.OrderBy) != 1 || sel.OrderBy[0].Column != "name" {
		t.Fatalf("expected outer ORDER BY name, got %+v", sel.OrderBy)
	}
	sub := sel.Where.Subquery.Statement
	if len(sub.OrderBy) != 1 || sub.OrderBy[0].Column != "id" {
		t.Fatalf("expected subquery ORDER BY id, got %+v", sub.OrderBy)
	}
	if sub.Limit == nil || *sub.Limit != 5 {
		t.Fatalf("expected subquery LIMIT 5, got %v", sub.Limit)
	}
}

func TestParseSubqueryWithAND(t *testing.T) {
	stmt, err := Parse("SELECT * FROM orders WHERE status = 'active' AND id IN (SELECT order_id FROM items)")
	if err != nil {
		t.Fatalf("parse subquery with AND: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where == nil || sel.Where.Operator != "AND" {
		t.Fatalf("expected AND predicate, got %+v", sel.Where)
	}
	left := sel.Where.Left
	if left.Column != "status" || left.Operator != "=" {
		t.Fatalf("expected left = status, got %+v", left)
	}
	right := sel.Where.Right
	if right.Column != "id" || right.Operator != "IN" || right.Subquery == nil {
		t.Fatalf("expected right IN subquery on id, got %+v", right)
	}
}

func TestParseColumnToColumnPredicate(t *testing.T) {
	stmt, err := Parse("SELECT * FROM items WHERE block_code = bc.block_id")
	if err != nil {
		t.Fatalf("parse column-to-column: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if sel.Where.Column != "block_code" {
		t.Fatalf("expected column block_code, got %q", sel.Where.Column)
	}
	if sel.Where.RightColumn != "bc.block_id" {
		t.Fatalf("expected right_column bc.block_id, got %q", sel.Where.RightColumn)
	}
	if sel.Where.Operator != "=" {
		t.Fatalf("expected operator =, got %q", sel.Where.Operator)
	}
}

func TestParseCorrelatedSubqueryINWithColumnRef(t *testing.T) {
	sql := "SELECT * FROM phases WHERE phase_code IN (SELECT phase_code FROM phase_blocks WHERE block_code = outer_ref.id)"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse correlated IN subquery: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where == nil || sel.Where.Subquery == nil {
		t.Fatal("expected IN subquery")
	}
	inner := sel.Where.Subquery.Statement
	if inner.Where == nil {
		t.Fatal("expected inner WHERE")
	}
	if inner.Where.RightColumn != "outer_ref.id" {
		t.Fatalf("expected inner right_column outer_ref.id, got %q", inner.Where.RightColumn)
	}
}

func TestParseColumnRefDoesNotMatchNumericLiterals(t *testing.T) {
	// Numeric values should still parse as literals, not column refs.
	stmt, err := Parse("SELECT * FROM items WHERE id = 42")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.Where.RightColumn != "" {
		t.Fatalf("expected no right_column for numeric literal, got %q", sel.Where.RightColumn)
	}
	if sel.Where.Value.Kind != ast.LiteralNumber || sel.Where.Value.NumberValue != 42 {
		t.Fatalf("expected literal number 42, got %+v", sel.Where.Value)
	}
}

func TestParseLeftJoin(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
	if err != nil {
		t.Fatalf("parse left join: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.Joins) == 0 {
		t.Fatal("expected join clause")
	}
	if sel.Joins[0].JoinType != ast.JoinLeft {
		t.Fatalf("expected LEFT join, got %q", sel.Joins[0].JoinType)
	}
	if sel.Joins[0].TableName != "orders" {
		t.Fatalf("expected right table orders, got %q", sel.Joins[0].TableName)
	}
	if sel.Joins[0].LeftColumn != "users.id" {
		t.Fatalf("expected left column users.id, got %q", sel.Joins[0].LeftColumn)
	}
}

func TestParseRightJoin(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id")
	if err != nil {
		t.Fatalf("parse right join: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.Joins) == 0 {
		t.Fatal("expected join clause")
	}
	if sel.Joins[0].JoinType != ast.JoinRight {
		t.Fatalf("expected RIGHT join, got %q", sel.Joins[0].JoinType)
	}
}

func TestParseCrossJoin(t *testing.T) {
	stmt, err := Parse("SELECT * FROM colors CROSS JOIN sizes")
	if err != nil {
		t.Fatalf("parse cross join: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.Joins) == 0 {
		t.Fatal("expected join clause")
	}
	if sel.Joins[0].JoinType != ast.JoinCross {
		t.Fatalf("expected CROSS join, got %q", sel.Joins[0].JoinType)
	}
	if sel.Joins[0].TableName != "sizes" {
		t.Fatalf("expected right table sizes, got %q", sel.Joins[0].TableName)
	}
	if sel.Joins[0].LeftColumn != "" || sel.Joins[0].RightColumn != "" {
		t.Fatalf("CROSS JOIN should have no ON columns, got left=%q right=%q", sel.Joins[0].LeftColumn, sel.Joins[0].RightColumn)
	}
}

func TestParseBareJoinDefaultsInner(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users JOIN orders ON users.id = orders.user_id")
	if err != nil {
		t.Fatalf("parse bare join: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.Joins) == 0 {
		t.Fatal("expected join clause")
	}
	if sel.Joins[0].JoinType != ast.JoinInner {
		t.Fatalf("expected INNER join for bare JOIN, got %q", sel.Joins[0].JoinType)
	}
}

func TestParseMultiJoin(t *testing.T) {
	stmt, err := Parse("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id")
	if err != nil {
		t.Fatalf("parse multi join: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "a" {
		t.Fatalf("expected table a, got %q", sel.TableName)
	}
	if len(sel.Joins) != 2 {
		t.Fatalf("expected 2 joins, got %d", len(sel.Joins))
	}
	if sel.Joins[0].TableName != "b" || sel.Joins[0].JoinType != ast.JoinLeft {
		t.Fatalf("expected first join LEFT on b, got %+v", sel.Joins[0])
	}
	if sel.Joins[0].LeftColumn != "a.id" || sel.Joins[0].RightColumn != "b.a_id" {
		t.Fatalf("first join columns wrong: left=%q right=%q", sel.Joins[0].LeftColumn, sel.Joins[0].RightColumn)
	}
	if sel.Joins[1].TableName != "c" || sel.Joins[1].JoinType != ast.JoinLeft {
		t.Fatalf("expected second join LEFT on c, got %+v", sel.Joins[1])
	}
	if sel.Joins[1].LeftColumn != "b.id" || sel.Joins[1].RightColumn != "c.b_id" {
		t.Fatalf("second join columns wrong: left=%q right=%q", sel.Joins[1].LeftColumn, sel.Joins[1].RightColumn)
	}
}

func TestParseMultiJoinWithAliases(t *testing.T) {
	stmt, err := Parse("SELECT u.id, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id LEFT JOIN items i ON o.id = i.order_id")
	if err != nil {
		t.Fatalf("parse multi join with aliases: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "users" {
		t.Fatalf("expected table users, got %q", sel.TableName)
	}
	if sel.TableAlias != "u" {
		t.Fatalf("expected table alias u, got %q", sel.TableAlias)
	}
	if len(sel.Joins) != 2 {
		t.Fatalf("expected 2 joins, got %d", len(sel.Joins))
	}
	if sel.Joins[0].TableName != "orders" || sel.Joins[0].Alias != "o" {
		t.Fatalf("expected first join on orders alias o, got table=%q alias=%q", sel.Joins[0].TableName, sel.Joins[0].Alias)
	}
	if sel.Joins[1].TableName != "items" || sel.Joins[1].Alias != "i" {
		t.Fatalf("expected second join on items alias i, got table=%q alias=%q", sel.Joins[1].TableName, sel.Joins[1].Alias)
	}
}

func TestParseMultiLineSQL(t *testing.T) {
	sql := "SELECT r.name, up.label, op.label\n" +
		"FROM recipes r\n" +
		"JOIN unit_procedures up ON r.id = up.recipe_id\n" +
		"JOIN operations op ON up.id = op.unit_procedure_id\n" +
		"WHERE r.id = 1;"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse multi-line SQL: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "recipes" {
		t.Fatalf("expected table recipes, got %q", sel.TableName)
	}
	if sel.TableAlias != "r" {
		t.Fatalf("expected alias r, got %q", sel.TableAlias)
	}
	if len(sel.Joins) != 2 {
		t.Fatalf("expected 2 joins, got %d", len(sel.Joins))
	}
	if sel.Joins[0].TableName != "unit_procedures" {
		t.Fatalf("expected first join on unit_procedures, got %q", sel.Joins[0].TableName)
	}
	if sel.Joins[1].TableName != "operations" {
		t.Fatalf("expected second join on operations, got %q", sel.Joins[1].TableName)
	}
}

func TestParseMultiLineSQLWithTabs(t *testing.T) {
	sql := "SELECT id, name\n\tFROM users\n\tWHERE id = 1"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse SQL with tabs: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "users" {
		t.Fatalf("expected table users, got %q", sel.TableName)
	}
}

func TestParseMultiLinePreservesStringLiterals(t *testing.T) {
	sql := "SELECT id FROM users\nWHERE name = 'hello\nworld'"
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse SQL with newline in string literal: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "users" {
		t.Fatalf("expected table users, got %q", sel.TableName)
	}
	// The string literal should preserve its internal newline
	cond := sel.Where
	if cond == nil {
		t.Fatal("expected WHERE clause")
	}
	if cond.Value.Kind != ast.LiteralString || cond.Value.StringValue != "hello\nworld" {
		t.Fatalf("expected string literal 'hello\\nworld', got %+v", cond.Value)
	}
}

func TestParseTableAlias(t *testing.T) {
	stmt, err := Parse("SELECT u.id FROM users u")
	if err != nil {
		t.Fatalf("parse table alias: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "users" {
		t.Fatalf("expected table users, got %q", sel.TableName)
	}
	if sel.TableAlias != "u" {
		t.Fatalf("expected alias u, got %q", sel.TableAlias)
	}
}

func TestParseSingleCTE(t *testing.T) {
	stmt, err := Parse("WITH active AS (SELECT id, name FROM users WHERE status = 'active') SELECT * FROM active")
	if err != nil {
		t.Fatalf("parse single CTE: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(sel.CTEs))
	}
	if sel.CTEs[0].Name != "active" {
		t.Fatalf("expected CTE name 'active', got %q", sel.CTEs[0].Name)
	}
	if sel.CTEs[0].Statement.TableName != "users" {
		t.Fatalf("expected CTE table 'users', got %q", sel.CTEs[0].Statement.TableName)
	}
	if sel.TableName != "active" {
		t.Fatalf("expected main query table 'active', got %q", sel.TableName)
	}
}

func TestParseMultipleCTEs(t *testing.T) {
	stmt, err := Parse("WITH a AS (SELECT id FROM t1), b AS (SELECT id FROM t2) SELECT * FROM a")
	if err != nil {
		t.Fatalf("parse multiple CTEs: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.CTEs) != 2 {
		t.Fatalf("expected 2 CTEs, got %d", len(sel.CTEs))
	}
	if sel.CTEs[0].Name != "a" || sel.CTEs[1].Name != "b" {
		t.Fatalf("expected CTE names a,b got %q,%q", sel.CTEs[0].Name, sel.CTEs[1].Name)
	}
}

func TestParseDerivedTableInFrom(t *testing.T) {
	stmt, err := Parse("SELECT * FROM (SELECT id, name FROM users) AS u")
	if err != nil {
		t.Fatalf("parse derived table: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "u" {
		t.Fatalf("expected main table u, got %q", sel.TableName)
	}
	if sel.TableAlias != "" {
		t.Fatalf("expected no separate table alias, got %q", sel.TableAlias)
	}
	if len(sel.CTEs) != 1 {
		t.Fatalf("expected 1 synthetic CTE, got %d", len(sel.CTEs))
	}
	if sel.CTEs[0].Name != "u" {
		t.Fatalf("expected derived table CTE name u, got %q", sel.CTEs[0].Name)
	}
	if sel.CTEs[0].Statement.TableName != "users" {
		t.Fatalf("expected derived table inner source users, got %q", sel.CTEs[0].Statement.TableName)
	}
}

func TestParseDerivedTableJoin(t *testing.T) {
	stmt, err := Parse("SELECT u.id, o.id FROM (SELECT id FROM users) u JOIN orders o ON u.id = o.user_id")
	if err != nil {
		t.Fatalf("parse derived table join: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "u" {
		t.Fatalf("expected main table u, got %q", sel.TableName)
	}
	if len(sel.CTEs) != 1 || sel.CTEs[0].Name != "u" {
		t.Fatalf("expected one derived-table CTE named u, got %+v", sel.CTEs)
	}
	if len(sel.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(sel.Joins))
	}
	if sel.Joins[0].TableName != "orders" || sel.Joins[0].Alias != "o" {
		t.Fatalf("expected join orders alias o, got %+v", sel.Joins[0])
	}
	if sel.Joins[0].LeftColumn != "u.id" || sel.Joins[0].RightColumn != "o.user_id" {
		t.Fatalf("unexpected join columns: %+v", sel.Joins[0])
	}
}

func TestParseJoinWithDerivedRightTable(t *testing.T) {
	stmt, err := Parse("SELECT users.id, x.id FROM users JOIN (SELECT id, user_id FROM orders) x ON users.id = x.user_id")
	if err != nil {
		t.Fatalf("parse join with derived right table: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "users" {
		t.Fatalf("expected base table users, got %q", sel.TableName)
	}
	if len(sel.CTEs) != 1 || sel.CTEs[0].Name != "x" {
		t.Fatalf("expected one derived-table CTE named x, got %+v", sel.CTEs)
	}
	if len(sel.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(sel.Joins))
	}
	if sel.Joins[0].TableName != "x" || sel.Joins[0].Alias != "" {
		t.Fatalf("expected derived right table x with no extra alias, got %+v", sel.Joins[0])
	}
}

func TestParseMultipleDerivedTableJoins(t *testing.T) {
	stmt, err := Parse("SELECT profile.name, ranked.amount FROM (SELECT id, name FROM users WHERE id <= 2) profile JOIN (SELECT user_id, amount, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn FROM orders) ranked ON profile.id = ranked.user_id JOIN (SELECT id FROM sizes WHERE id <= 2) allowed ON profile.id = allowed.id WHERE ranked.rn = 1")
	if err != nil {
		t.Fatalf("parse multiple derived table joins: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if sel.TableName != "profile" {
		t.Fatalf("expected main table profile, got %q", sel.TableName)
	}
	if len(sel.CTEs) != 3 {
		t.Fatalf("expected 3 synthetic CTEs, got %d", len(sel.CTEs))
	}
	if sel.CTEs[0].Name != "profile" || sel.CTEs[1].Name != "ranked" || sel.CTEs[2].Name != "allowed" {
		t.Fatalf("expected derived CTE names profile, ranked, allowed, got %+v", sel.CTEs)
	}
	if len(sel.Joins) != 2 {
		t.Fatalf("expected 2 joins, got %d", len(sel.Joins))
	}
	if sel.Joins[0].TableName != "ranked" || sel.Joins[0].LeftColumn != "profile.id" || sel.Joins[0].RightColumn != "ranked.user_id" {
		t.Fatalf("unexpected first join: %+v", sel.Joins[0])
	}
	if sel.Joins[1].TableName != "allowed" || sel.Joins[1].LeftColumn != "profile.id" || sel.Joins[1].RightColumn != "allowed.id" {
		t.Fatalf("unexpected second join: %+v", sel.Joins[1])
	}
}

func TestParseDerivedTableRequiresAlias(t *testing.T) {
	_, err := Parse("SELECT * FROM (SELECT id FROM users)")
	if err == nil {
		t.Fatal("expected derived table without alias to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "derived table requires alias") {
		t.Fatalf("expected alias error, got %v", err)
	}
}

func TestParseRejectsLateralDerivedTable(t *testing.T) {
	_, err := Parse("SELECT * FROM users u JOIN LATERAL (SELECT id FROM orders) o ON u.id = o.id")
	if err == nil {
		t.Fatal("expected LATERAL derived table to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "lateral derived tables are not supported") {
		t.Fatalf("expected lateral unsupported error, got %v", err)
	}
}

func TestParseRejectsCorrelatedDerivedTable(t *testing.T) {
	_, err := Parse("SELECT * FROM users u JOIN (SELECT u.id AS outer_user_id FROM orders) o ON u.id = o.outer_user_id")
	if err == nil {
		t.Fatal("expected correlated derived table to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "correlated derived tables in from are not supported") {
		t.Fatalf("expected correlated derived table error, got %v", err)
	}
}

func TestParseRejectsCorrelatedDerivedTableInWindowClause(t *testing.T) {
	_, err := Parse("SELECT * FROM users u JOIN (SELECT ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY id) AS rn FROM orders) o ON u.id = o.rn")
	if err == nil {
		t.Fatal("expected correlated derived table via window clause to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "correlated derived tables in from are not supported") {
		t.Fatalf("expected correlated derived table error, got %v", err)
	}
}

func TestParseCTEWithWhere(t *testing.T) {
	stmt, err := Parse("WITH big AS (SELECT id, amount FROM orders WHERE amount > 100) SELECT * FROM big WHERE id = 1")
	if err != nil {
		t.Fatalf("parse CTE with WHERE: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(sel.CTEs))
	}
	if sel.CTEs[0].Statement.Where == nil {
		t.Fatal("expected CTE WHERE clause")
	}
	if sel.Where == nil {
		t.Fatal("expected main query WHERE clause")
	}
}

func TestParseWindowFunctionRowNumber(t *testing.T) {
	stmt, err := Parse("SELECT name, ROW_NUMBER() OVER (ORDER BY name ASC) AS rn FROM users")
	if err != nil {
		t.Fatalf("parse window function: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.WindowFunctions) != 1 {
		t.Fatalf("expected 1 window function, got %d", len(sel.WindowFunctions))
	}
	wf := sel.WindowFunctions[0]
	if wf.Function != "ROW_NUMBER" {
		t.Fatalf("expected function ROW_NUMBER, got %q", wf.Function)
	}
	if wf.Alias != "rn" {
		t.Fatalf("expected alias 'rn', got %q", wf.Alias)
	}
	if len(wf.OrderBy) != 1 || wf.OrderBy[0].Column != "name" || wf.OrderBy[0].Direction != ast.SortAsc {
		t.Fatalf("expected ORDER BY name ASC, got %+v", wf.OrderBy)
	}
	if len(wf.Partition) != 0 {
		t.Fatalf("expected no PARTITION BY, got %+v", wf.Partition)
	}
	// rn should be in columns list
	found := false
	for _, c := range sel.Columns {
		if c == "rn" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'rn' in columns, got %v", sel.Columns)
	}
}

func TestParseWindowFunctionWithPartition(t *testing.T) {
	stmt, err := Parse("SELECT user_id, amount, RANK() OVER (PARTITION BY user_id ORDER BY amount DESC) AS rnk FROM orders")
	if err != nil {
		t.Fatalf("parse window function with partition: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.WindowFunctions) != 1 {
		t.Fatalf("expected 1 window function, got %d", len(sel.WindowFunctions))
	}
	wf := sel.WindowFunctions[0]
	if wf.Function != "RANK" {
		t.Fatalf("expected function RANK, got %q", wf.Function)
	}
	if wf.Alias != "rnk" {
		t.Fatalf("expected alias 'rnk', got %q", wf.Alias)
	}
	if len(wf.Partition) != 1 || wf.Partition[0] != "user_id" {
		t.Fatalf("expected PARTITION BY user_id, got %+v", wf.Partition)
	}
	if len(wf.OrderBy) != 1 || wf.OrderBy[0].Column != "amount" || wf.OrderBy[0].Direction != ast.SortDesc {
		t.Fatalf("expected ORDER BY amount DESC, got %+v", wf.OrderBy)
	}
}

func TestParseWindowFunctionLag(t *testing.T) {
	stmt, err := Parse("SELECT id, amount, LAG(amount) OVER (ORDER BY id ASC) AS prev_amount FROM orders")
	if err != nil {
		t.Fatalf("parse LAG window function: %v", err)
	}
	sel := stmt.(ast.SelectStatement)
	if len(sel.WindowFunctions) != 1 {
		t.Fatalf("expected 1 window function, got %d", len(sel.WindowFunctions))
	}
	wf := sel.WindowFunctions[0]
	if wf.Function != "LAG" {
		t.Fatalf("expected function LAG, got %q", wf.Function)
	}
	if len(wf.Args) != 1 || wf.Args[0] != "amount" {
		t.Fatalf("expected args [amount], got %+v", wf.Args)
	}
	if wf.Alias != "prev_amount" {
		t.Fatalf("expected alias 'prev_amount', got %q", wf.Alias)
	}
}

func TestParseSelectForHistory(t *testing.T) {
	statement, err := Parse("SELECT * FROM users FOR HISTORY WHERE id = 1")
	if err != nil {
		t.Fatalf("parse FOR HISTORY: %v", err)
	}

	sel, ok := statement.(ast.SelectStatement)
	if !ok {
		t.Fatalf("expected SelectStatement, got %T", statement)
	}

	if !sel.ForHistory {
		t.Fatal("expected ForHistory to be true")
	}
	if sel.TableName != "users" {
		t.Fatalf("expected table 'users', got %q", sel.TableName)
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE clause to be preserved")
	}
}

func TestParseSelectForHistoryNoWhere(t *testing.T) {
	statement, err := Parse("SELECT * FROM users FOR HISTORY")
	if err != nil {
		t.Fatalf("parse FOR HISTORY no where: %v", err)
	}

	sel := statement.(ast.SelectStatement)
	if !sel.ForHistory {
		t.Fatal("expected ForHistory to be true")
	}
	if sel.TableName != "users" {
		t.Fatalf("expected table 'users', got %q", sel.TableName)
	}
}

func TestParseSelectWithoutForHistory(t *testing.T) {
	statement, err := Parse("SELECT * FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("parse normal select: %v", err)
	}

	sel := statement.(ast.SelectStatement)
	if sel.ForHistory {
		t.Fatal("expected ForHistory to be false for normal SELECT")
	}
}

func TestParseCreateTableDefaultAutoIncrement(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY DEFAULT AUTOINCREMENT, name TEXT);")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	col := ct.Columns[0]
	if col.DefaultValue == nil {
		t.Fatal("expected DefaultValue to be set")
	}
	if col.DefaultValue.Kind != ast.DefaultAutoIncrement {
		t.Fatalf("expected DefaultAutoIncrement, got %q", col.DefaultValue.Kind)
	}
}

func TestParseCreateTableDefaultUUIDv7(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, uid TEXT DEFAULT UUID_V7);")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	col := ct.Columns[1]
	if col.DefaultValue == nil {
		t.Fatal("expected DefaultValue to be set")
	}
	if col.DefaultValue.Kind != ast.DefaultUUIDv7 {
		t.Fatalf("expected DefaultUUIDv7, got %q", col.DefaultValue.Kind)
	}
}

func TestParseCreateTableDefaultLiteralInt(t *testing.T) {
	stmt, err := Parse("CREATE TABLE scores (id INT, score INT DEFAULT 0);")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	col := ct.Columns[1]
	if col.DefaultValue == nil {
		t.Fatal("expected DefaultValue to be set")
	}
	if col.DefaultValue.Kind != ast.DefaultLiteral {
		t.Fatalf("expected DefaultLiteral, got %q", col.DefaultValue.Kind)
	}
	if col.DefaultValue.Value.Kind != ast.LiteralNumber || col.DefaultValue.Value.NumberValue != 0 {
		t.Fatalf("expected literal int 0, got %+v", col.DefaultValue.Value)
	}
}

func TestParseCreateTableDefaultLiteralString(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT, name TEXT DEFAULT 'anonymous');")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	col := ct.Columns[1]
	if col.DefaultValue == nil {
		t.Fatal("expected DefaultValue to be set")
	}
	if col.DefaultValue.Kind != ast.DefaultLiteral {
		t.Fatalf("expected DefaultLiteral, got %q", col.DefaultValue.Kind)
	}
	if col.DefaultValue.Value.Kind != ast.LiteralString || col.DefaultValue.Value.StringValue != "anonymous" {
		t.Fatalf("expected literal string 'anonymous', got %+v", col.DefaultValue.Value)
	}
}

func TestParseCreateTableDefaultLiteralBool(t *testing.T) {
	stmt, err := Parse("CREATE TABLE flags (id INT, active BOOL DEFAULT TRUE);")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	col := ct.Columns[1]
	if col.DefaultValue == nil {
		t.Fatal("expected DefaultValue to be set")
	}
	if col.DefaultValue.Kind != ast.DefaultLiteral {
		t.Fatalf("expected DefaultLiteral, got %q", col.DefaultValue.Kind)
	}
	if col.DefaultValue.Value.Kind != ast.LiteralBoolean || !col.DefaultValue.Value.BoolValue {
		t.Fatalf("expected literal bool true, got %+v", col.DefaultValue.Value)
	}
}

func TestParseCreateTableDefaultNull(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT, notes TEXT DEFAULT NULL);")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	col := ct.Columns[1]
	if col.DefaultValue == nil {
		t.Fatal("expected DefaultValue to be set")
	}
	if col.DefaultValue.Kind != ast.DefaultLiteral {
		t.Fatalf("expected DefaultLiteral, got %q", col.DefaultValue.Kind)
	}
	if col.DefaultValue.Value.Kind != ast.LiteralNull {
		t.Fatalf("expected literal null, got %+v", col.DefaultValue.Value)
	}
}

func TestParseCreateTableDefaultLiteralFloat(t *testing.T) {
	stmt, err := Parse("CREATE TABLE metrics (id INT, rate FLOAT DEFAULT 1.5);")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	col := ct.Columns[1]
	if col.DefaultValue == nil {
		t.Fatal("expected DefaultValue to be set")
	}
	if col.DefaultValue.Kind != ast.DefaultLiteral {
		t.Fatalf("expected DefaultLiteral, got %q", col.DefaultValue.Kind)
	}
	if col.DefaultValue.Value.Kind != ast.LiteralFloat || col.DefaultValue.Value.FloatValue != 1.5 {
		t.Fatalf("expected literal float 1.5, got %+v", col.DefaultValue.Value)
	}
}

func TestParseCreateTableDefaultMissingValue(t *testing.T) {
	_, err := Parse("CREATE TABLE users (id INT DEFAULT);")
	if err == nil {
		t.Fatal("expected error for DEFAULT without value")
	}
}

func TestParseCreateTableDefaultWithOtherConstraints(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY DEFAULT AUTOINCREMENT, email TEXT UNIQUE DEFAULT 'none');")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	if ct.Columns[0].PrimaryKey != true {
		t.Fatal("expected id to be primary key")
	}
	if ct.Columns[0].DefaultValue == nil || ct.Columns[0].DefaultValue.Kind != ast.DefaultAutoIncrement {
		t.Fatal("expected id DEFAULT AUTOINCREMENT")
	}
	if ct.Columns[1].Unique != true {
		t.Fatal("expected email to be unique")
	}
	if ct.Columns[1].DefaultValue == nil || ct.Columns[1].DefaultValue.Kind != ast.DefaultLiteral {
		t.Fatal("expected email DEFAULT literal")
	}
	if ct.Columns[1].DefaultValue.Value.StringValue != "none" {
		t.Fatalf("expected email default 'none', got %q", ct.Columns[1].DefaultValue.Value.StringValue)
	}
}

func TestParseCreateTableNoDefault(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT, name TEXT);")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	for _, col := range ct.Columns {
		if col.DefaultValue != nil {
			t.Fatalf("expected no DefaultValue for column %q, got %+v", col.Name, col.DefaultValue)
		}
	}
}

// ---------- Versioned Foreign Key tests ----------

func TestParseVersionedForeignKeyCrossDomain(t *testing.T) {
	sql := `CREATE TABLE process_orders (
		id INT PRIMARY KEY DEFAULT AUTOINCREMENT,
		recipe_id INT,
		recipe_lsn INT,
		VERSIONED FOREIGN KEY (recipe_id) REFERENCES recipes.master_recipes(id) AS OF recipe_lsn
	)`
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	if ct.TableName != "process_orders" {
		t.Fatalf("expected table process_orders, got %q", ct.TableName)
	}
	if len(ct.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(ct.Columns))
	}
	if len(ct.VersionedForeignKeys) != 1 {
		t.Fatalf("expected 1 versioned FK, got %d", len(ct.VersionedForeignKeys))
	}
	vfk := ct.VersionedForeignKeys[0]
	if vfk.Column != "recipe_id" {
		t.Fatalf("expected VFK column recipe_id, got %q", vfk.Column)
	}
	if vfk.LSNColumn != "recipe_lsn" {
		t.Fatalf("expected VFK lsn_column recipe_lsn, got %q", vfk.LSNColumn)
	}
	if vfk.ReferencesDomain != "recipes" {
		t.Fatalf("expected VFK domain recipes, got %q", vfk.ReferencesDomain)
	}
	if vfk.ReferencesTable != "master_recipes" {
		t.Fatalf("expected VFK table master_recipes, got %q", vfk.ReferencesTable)
	}
	if vfk.ReferencesColumn != "id" {
		t.Fatalf("expected VFK column id, got %q", vfk.ReferencesColumn)
	}
}

func TestParseVersionedForeignKeySameDomain(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INT PRIMARY KEY,
		product_id INT,
		product_lsn INT,
		VERSIONED FOREIGN KEY (product_id) REFERENCES products(id) AS OF product_lsn
	)`
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	if len(ct.VersionedForeignKeys) != 1 {
		t.Fatalf("expected 1 versioned FK, got %d", len(ct.VersionedForeignKeys))
	}
	vfk := ct.VersionedForeignKeys[0]
	if vfk.ReferencesDomain != "" {
		t.Fatalf("expected empty domain for same-domain VFK, got %q", vfk.ReferencesDomain)
	}
	if vfk.ReferencesTable != "products" {
		t.Fatalf("expected VFK table products, got %q", vfk.ReferencesTable)
	}
	if vfk.ReferencesColumn != "id" {
		t.Fatalf("expected VFK column id, got %q", vfk.ReferencesColumn)
	}
}

func TestParseVersionedForeignKeyMultiple(t *testing.T) {
	sql := `CREATE TABLE batch (
		id INT PRIMARY KEY,
		recipe_id INT,
		recipe_lsn INT,
		equipment_id INT,
		equipment_lsn INT,
		VERSIONED FOREIGN KEY (recipe_id) REFERENCES recipes.master_recipes(id) AS OF recipe_lsn,
		VERSIONED FOREIGN KEY (equipment_id) REFERENCES equipment.machines(id) AS OF equipment_lsn
	)`
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	if len(ct.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(ct.Columns))
	}
	if len(ct.VersionedForeignKeys) != 2 {
		t.Fatalf("expected 2 versioned FKs, got %d", len(ct.VersionedForeignKeys))
	}
	if ct.VersionedForeignKeys[0].Column != "recipe_id" {
		t.Fatalf("expected first VFK column recipe_id, got %q", ct.VersionedForeignKeys[0].Column)
	}
	if ct.VersionedForeignKeys[1].Column != "equipment_id" {
		t.Fatalf("expected second VFK column equipment_id, got %q", ct.VersionedForeignKeys[1].Column)
	}
}

func TestParseVersionedForeignKeyMissingAsOf(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INT PRIMARY KEY,
		recipe_id INT,
		VERSIONED FOREIGN KEY (recipe_id) REFERENCES recipes.master_recipes(id)
	)`
	_, err := Parse(sql)
	if err == nil {
		t.Fatal("expected error for VFK without AS OF")
	}
	if !strings.Contains(err.Error(), "AS OF") {
		t.Fatalf("expected 'AS OF' in error, got %q", err.Error())
	}
}

func TestParseVersionedForeignKeyMissingReferences(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INT PRIMARY KEY,
		recipe_id INT,
		recipe_lsn INT,
		VERSIONED FOREIGN KEY (recipe_id) AS OF recipe_lsn
	)`
	_, err := Parse(sql)
	if err == nil {
		t.Fatal("expected error for VFK without REFERENCES")
	}
	if !strings.Contains(err.Error(), "REFERENCES") {
		t.Fatalf("expected 'REFERENCES' in error, got %q", err.Error())
	}
}

func TestParseVersionedForeignKeyMixedWithRegularColumns(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INT PRIMARY KEY DEFAULT AUTOINCREMENT,
		name TEXT UNIQUE,
		recipe_id INT,
		recipe_lsn INT,
		VERSIONED FOREIGN KEY (recipe_id) REFERENCES recipes.master_recipes(id) AS OF recipe_lsn
	)`
	stmt, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	if len(ct.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(ct.Columns))
	}
	if ct.Columns[0].PrimaryKey != true {
		t.Fatal("expected id to be primary key")
	}
	if ct.Columns[1].Unique != true {
		t.Fatal("expected name to be unique")
	}
	if len(ct.VersionedForeignKeys) != 1 {
		t.Fatalf("expected 1 VFK, got %d", len(ct.VersionedForeignKeys))
	}
}

func TestParseCreateTableIfNotExists(t *testing.T) {
	stmt, err := Parse("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	if !ct.IfNotExists {
		t.Fatal("expected IfNotExists=true")
	}
	if ct.TableName != "users" {
		t.Fatalf("expected table name 'users', got %q", ct.TableName)
	}
	if len(ct.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ct.Columns))
	}
}

func TestParseCreateTableWithoutIfNotExists(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := stmt.(ast.CreateTableStatement)
	if ct.IfNotExists {
		t.Fatal("expected IfNotExists=false for regular CREATE TABLE")
	}
}

func TestParseCreateIndexIfNotExists(t *testing.T) {
	stmt, err := Parse("CREATE INDEX IF NOT EXISTS idx_users_name ON users (name)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ci := stmt.(ast.CreateIndexStatement)
	if !ci.IfNotExists {
		t.Fatal("expected IfNotExists=true")
	}
	if ci.IndexName != "idx_users_name" {
		t.Fatalf("expected index name 'idx_users_name', got %q", ci.IndexName)
	}
	if ci.TableName != "users" {
		t.Fatalf("expected table name 'users', got %q", ci.TableName)
	}
	if ci.Column != "name" {
		t.Fatalf("expected column 'name', got %q", ci.Column)
	}
}

func TestParseCreateIndexWithoutIfNotExists(t *testing.T) {
	stmt, err := Parse("CREATE INDEX idx_users_name ON users (name)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ci := stmt.(ast.CreateIndexStatement)
	if ci.IfNotExists {
		t.Fatal("expected IfNotExists=false for regular CREATE INDEX")
	}
}

func TestParseInsertReturning(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (name) VALUES ('alice') RETURNING id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins := stmt.(ast.InsertStatement)
	if ins.TableName != "users" {
		t.Fatalf("expected table 'users', got %q", ins.TableName)
	}
	if len(ins.ReturningColumns) != 1 || ins.ReturningColumns[0] != "id" {
		t.Fatalf("expected ReturningColumns=[id], got %v", ins.ReturningColumns)
	}
}

func TestParseInsertReturningMultiple(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (name) VALUES ('alice') RETURNING id, name, email;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins := stmt.(ast.InsertStatement)
	if len(ins.ReturningColumns) != 3 {
		t.Fatalf("expected 3 returning columns, got %d: %v", len(ins.ReturningColumns), ins.ReturningColumns)
	}
	expected := []string{"id", "name", "email"}
	for i, col := range expected {
		if ins.ReturningColumns[i] != col {
			t.Fatalf("returning[%d]: expected %q, got %q", i, col, ins.ReturningColumns[i])
		}
	}
}

func TestParseInsertReturningStar(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (name) VALUES ('alice') RETURNING *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins := stmt.(ast.InsertStatement)
	if len(ins.ReturningColumns) != 1 || ins.ReturningColumns[0] != "*" {
		t.Fatalf("expected ReturningColumns=[*], got %v", ins.ReturningColumns)
	}
}

func TestParseInsertWithoutReturning(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (name) VALUES ('alice');")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins := stmt.(ast.InsertStatement)
	if len(ins.ReturningColumns) != 0 {
		t.Fatalf("expected no ReturningColumns, got %v", ins.ReturningColumns)
	}
}

func TestExtractImports(t *testing.T) {
	t.Run("no import passthrough", func(t *testing.T) {
		imports, remaining, err := ExtractImports("SELECT * FROM orders")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(imports) != 0 {
			t.Fatalf("expected no imports, got %d", len(imports))
		}
		if remaining != "SELECT * FROM orders" {
			t.Fatalf("expected unchanged SQL, got %q", remaining)
		}
	})

	t.Run("single import", func(t *testing.T) {
		imports, remaining, err := ExtractImports("IMPORT master_recipe.recipes; SELECT * FROM orders")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(imports) != 1 {
			t.Fatalf("expected 1 import, got %d", len(imports))
		}
		if imports[0].SourceDomain != "master_recipe" || imports[0].SourceTable != "recipes" {
			t.Fatalf("expected master_recipe.recipes, got %s.%s", imports[0].SourceDomain, imports[0].SourceTable)
		}
		if imports[0].Alias != "" {
			t.Fatalf("expected no alias, got %q", imports[0].Alias)
		}
		if remaining != "SELECT * FROM orders" {
			t.Fatalf("expected remaining SQL 'SELECT * FROM orders', got %q", remaining)
		}
	})

	t.Run("import with alias", func(t *testing.T) {
		imports, remaining, err := ExtractImports("IMPORT master_recipe.recipes AS mr; SELECT * FROM orders")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(imports) != 1 {
			t.Fatalf("expected 1 import, got %d", len(imports))
		}
		if imports[0].Alias != "mr" {
			t.Fatalf("expected alias 'mr', got %q", imports[0].Alias)
		}
		if remaining != "SELECT * FROM orders" {
			t.Fatalf("expected remaining SQL 'SELECT * FROM orders', got %q", remaining)
		}
	})

	t.Run("multiple imports", func(t *testing.T) {
		imports, remaining, err := ExtractImports("IMPORT alpha.users; IMPORT beta.products; SELECT * FROM orders")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(imports) != 2 {
			t.Fatalf("expected 2 imports, got %d", len(imports))
		}
		if imports[0].SourceDomain != "alpha" || imports[0].SourceTable != "users" {
			t.Fatalf("first import: expected alpha.users, got %s.%s", imports[0].SourceDomain, imports[0].SourceTable)
		}
		if imports[1].SourceDomain != "beta" || imports[1].SourceTable != "products" {
			t.Fatalf("second import: expected beta.products, got %s.%s", imports[1].SourceDomain, imports[1].SourceTable)
		}
		if remaining != "SELECT * FROM orders" {
			t.Fatalf("expected remaining SQL 'SELECT * FROM orders', got %q", remaining)
		}
	})

	t.Run("import without select fails", func(t *testing.T) {
		_, _, err := ExtractImports("IMPORT master_recipe.recipes")
		if err == nil {
			t.Fatal("expected error for import without select")
		}
	})

	t.Run("import without qualified name fails", func(t *testing.T) {
		_, _, err := ExtractImports("IMPORT recipes; SELECT * FROM orders")
		if err == nil {
			t.Fatal("expected error for unqualified import")
		}
	})
}

func TestParseCreateEntitySnapshot(t *testing.T) {
	statement, err := Parse("CREATE ENTITY order_aggregate (ROOT orders, INCLUDES order_lines, order_approvals);")
	if err != nil {
		t.Fatalf("parse create entity: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"entity_name":"order_aggregate","root_table":"orders","tables":["orders","order_lines","order_approvals"]}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseCreateEntityIfNotExists(t *testing.T) {
	stmt, err := Parse("CREATE ENTITY IF NOT EXISTS order_aggregate (ROOT orders, INCLUDES order_lines)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ce := stmt.(ast.CreateEntityStatement)
	if !ce.IfNotExists {
		t.Fatal("expected IfNotExists=true")
	}
	if ce.EntityName != "order_aggregate" {
		t.Fatalf("expected entity name 'order_aggregate', got %q", ce.EntityName)
	}
	if ce.RootTable != "orders" {
		t.Fatalf("expected root table 'orders', got %q", ce.RootTable)
	}
	if len(ce.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(ce.Tables))
	}
}

func TestParseCreateEntitySingleTable(t *testing.T) {
	stmt, err := Parse("CREATE ENTITY product_aggregate (ROOT products)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ce := stmt.(ast.CreateEntityStatement)
	if ce.EntityName != "product_aggregate" {
		t.Fatalf("expected entity name 'product_aggregate', got %q", ce.EntityName)
	}
	if ce.RootTable != "products" {
		t.Fatalf("expected root table 'products', got %q", ce.RootTable)
	}
	if len(ce.Tables) != 1 || ce.Tables[0] != "products" {
		t.Fatalf("expected tables=[products], got %v", ce.Tables)
	}
}

func TestParseCreateEntityMultipleIncludes(t *testing.T) {
	stmt, err := Parse("CREATE ENTITY recipe_agg (ROOT recipes, INCLUDES steps, ingredients, approvals)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ce := stmt.(ast.CreateEntityStatement)
	if len(ce.Tables) != 4 {
		t.Fatalf("expected 4 tables, got %d: %v", len(ce.Tables), ce.Tables)
	}
	expected := []string{"recipes", "steps", "ingredients", "approvals"}
	for i, want := range expected {
		if ce.Tables[i] != want {
			t.Fatalf("tables[%d]: expected %q, got %q", i, want, ce.Tables[i])
		}
	}
}

func TestParseJSONLiteral(t *testing.T) {
	statement, err := Parse(`INSERT INTO events (id, data) VALUES ('e1', '{"type":"click","x":100}');`)
	if err != nil {
		t.Fatalf("parse insert with JSON: %v", err)
	}

	insert := statement.(ast.InsertStatement)
	if len(insert.Values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(insert.Values))
	}
	if insert.Values[1].Kind != ast.LiteralJSON {
		t.Fatalf("expected LiteralJSON, got %s", insert.Values[1].Kind)
	}
	if insert.Values[1].StringValue != `{"type":"click","x":100}` {
		t.Fatalf("unexpected JSON value: %s", insert.Values[1].StringValue)
	}
}

func TestParseJSONAccessSelectTextMode(t *testing.T) {
	statement, err := Parse(`SELECT data->>'type' AS event_type FROM events;`)
	if err != nil {
		t.Fatalf("parse JSON access select: %v", err)
	}

	sel := statement.(ast.SelectStatement)
	if len(sel.JsonAccessColumns) != 1 {
		t.Fatalf("expected 1 json access column, got %d", len(sel.JsonAccessColumns))
	}
	ja := sel.JsonAccessColumns[0]
	if ja.Column != "data" {
		t.Fatalf("expected column 'data', got %q", ja.Column)
	}
	if len(ja.Path) != 1 || ja.Path[0] != "type" {
		t.Fatalf("expected path ['type'], got %v", ja.Path)
	}
	if !ja.TextMode {
		t.Fatal("expected TextMode=true for ->>")
	}
	if ja.Alias != "event_type" {
		t.Fatalf("expected alias 'event_type', got %q", ja.Alias)
	}
}

func TestParseJSONAccessSelectJSONMode(t *testing.T) {
	statement, err := Parse(`SELECT data->'meta' AS meta FROM events;`)
	if err != nil {
		t.Fatalf("parse JSON access select json mode: %v", err)
	}

	sel := statement.(ast.SelectStatement)
	if len(sel.JsonAccessColumns) != 1 {
		t.Fatalf("expected 1 json access column, got %d", len(sel.JsonAccessColumns))
	}
	ja := sel.JsonAccessColumns[0]
	if ja.Column != "data" {
		t.Fatalf("expected column 'data', got %q", ja.Column)
	}
	if len(ja.Path) != 1 || ja.Path[0] != "meta" {
		t.Fatalf("expected path ['meta'], got %v", ja.Path)
	}
	if ja.TextMode {
		t.Fatal("expected TextMode=false for ->")
	}
}

func TestParseJSONAccessChained(t *testing.T) {
	statement, err := Parse(`SELECT data->'meta'->>'browser' FROM events;`)
	if err != nil {
		t.Fatalf("parse chained JSON access: %v", err)
	}

	sel := statement.(ast.SelectStatement)
	if len(sel.JsonAccessColumns) != 1 {
		t.Fatalf("expected 1 json access column, got %d", len(sel.JsonAccessColumns))
	}
	ja := sel.JsonAccessColumns[0]
	if ja.Column != "data" {
		t.Fatalf("expected column 'data', got %q", ja.Column)
	}
	if len(ja.Path) != 2 || ja.Path[0] != "meta" || ja.Path[1] != "browser" {
		t.Fatalf("expected path ['meta','browser'], got %v", ja.Path)
	}
	if !ja.TextMode {
		t.Fatal("expected TextMode=true for final ->>")
	}
}

func TestParseJSONAccessWhere(t *testing.T) {
	statement, err := Parse(`SELECT * FROM events WHERE data->>'type' = 'click';`)
	if err != nil {
		t.Fatalf("parse JSON access where: %v", err)
	}

	sel := statement.(ast.SelectStatement)
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if sel.Where.JsonAccess == nil {
		t.Fatal("expected JsonAccess in predicate")
	}
	ja := sel.Where.JsonAccess
	if ja.Column != "data" {
		t.Fatalf("expected column 'data', got %q", ja.Column)
	}
	if len(ja.Path) != 1 || ja.Path[0] != "type" {
		t.Fatalf("expected path ['type'], got %v", ja.Path)
	}
	if !ja.TextMode {
		t.Fatal("expected TextMode=true for ->>")
	}
	if sel.Where.Operator != "=" {
		t.Fatalf("expected operator '=', got %q", sel.Where.Operator)
	}
	if sel.Where.Value.StringValue != "click" {
		t.Fatalf("expected value 'click', got %q", sel.Where.Value.StringValue)
	}
}

func TestParseDropTableSnapshot(t *testing.T) {
	statement, err := Parse("DROP TABLE users;")
	if err != nil {
		t.Fatalf("parse drop table: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseDropTableIfExistsSnapshot(t *testing.T) {
	statement, err := Parse("DROP TABLE IF EXISTS users;")
	if err != nil {
		t.Fatalf("parse drop table if exists: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","if_exists":true}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseDropTableCascadeSnapshot(t *testing.T) {
	statement, err := Parse("DROP TABLE users CASCADE;")
	if err != nil {
		t.Fatalf("parse drop table cascade: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users","cascade":true}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseDropIndexSnapshot(t *testing.T) {
	statement, err := Parse("DROP INDEX idx_users_email;")
	if err != nil {
		t.Fatalf("parse drop index: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"index_name":"idx_users_email"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseDropIndexIfExistsOnTableSnapshot(t *testing.T) {
	statement, err := Parse("DROP INDEX IF EXISTS idx_users_email ON users;")
	if err != nil {
		t.Fatalf("parse drop index if exists on table: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"index_name":"idx_users_email","table_name":"users","if_exists":true}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseTruncateTableSnapshot(t *testing.T) {
	statement, err := Parse("TRUNCATE TABLE users;")
	if err != nil {
		t.Fatalf("parse truncate table: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseTruncateShortFormSnapshot(t *testing.T) {
	statement, err := Parse("TRUNCATE users;")
	if err != nil {
		t.Fatalf("parse truncate short form: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	expected := `{"table_name":"users"}`
	if string(bytes) != expected {
		t.Fatalf("snapshot mismatch\n got: %s\nwant: %s", string(bytes), expected)
	}
}

func TestParseMultiRowInsertSnapshot(t *testing.T) {
	statement, err := Parse("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');")
	if err != nil {
		t.Fatalf("parse multi-row insert: %v", err)
	}

	bytes, err := json.Marshal(statement)
	if err != nil {
		t.Fatalf("marshal ast: %v", err)
	}

	ins := statement.(ast.InsertStatement)
	if len(ins.Values) != 2 {
		t.Fatalf("expected 2 values in first row, got %d", len(ins.Values))
	}
	if len(ins.MultiValues) != 2 {
		t.Fatalf("expected 2 extra value rows, got %d", len(ins.MultiValues))
	}
	if ins.Values[0].NumberValue != 1 {
		t.Fatalf("expected first row id=1, got %d", ins.Values[0].NumberValue)
	}
	if ins.MultiValues[0][0].NumberValue != 2 {
		t.Fatalf("expected second row id=2, got %d", ins.MultiValues[0][0].NumberValue)
	}
	if ins.MultiValues[1][0].NumberValue != 3 {
		t.Fatalf("expected third row id=3, got %d", ins.MultiValues[1][0].NumberValue)
	}
	_ = bytes // we verified structure, snapshot check not needed
}

func TestParseSingleRowInsertNoMultiValues(t *testing.T) {
	statement, err := Parse("INSERT INTO users (id, name) VALUES (1, 'alice');")
	if err != nil {
		t.Fatalf("parse single-row insert: %v", err)
	}
	ins := statement.(ast.InsertStatement)
	if len(ins.MultiValues) != 0 {
		t.Fatalf("expected no multi_values for single-row insert, got %d", len(ins.MultiValues))
	}
}

func TestParseUpdateArithmeticExprSnapshot(t *testing.T) {
	statement, err := Parse("UPDATE counters SET count = count + 1 WHERE id = 1;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	upd := statement.(ast.UpdateStatement)
	if len(upd.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(upd.Expressions))
	}
	expr := upd.Expressions[0]
	if expr.Kind != ast.UpdateExprArithmetic {
		t.Fatalf("expected arithmetic, got %s", expr.Kind)
	}
	if expr.Column != "count" {
		t.Errorf("expected column 'count', got %q", expr.Column)
	}
	if expr.Operator != "+" {
		t.Errorf("expected operator '+', got %q", expr.Operator)
	}
	if expr.Operand.NumberValue != 1 {
		t.Errorf("expected operand 1, got %d", expr.Operand.NumberValue)
	}
}

func TestParseUpdateMixedArithmeticAndLiteral(t *testing.T) {
	statement, err := Parse("UPDATE products SET price = price * 0.9, name = 'sale' WHERE active = TRUE;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	upd := statement.(ast.UpdateStatement)
	if len(upd.Expressions) != 2 {
		t.Fatalf("expected 2 expressions, got %d", len(upd.Expressions))
	}
	// First: arithmetic (price * 0.9)
	if upd.Expressions[0].Kind != ast.UpdateExprArithmetic {
		t.Errorf("expected arithmetic for expr 0, got %s", upd.Expressions[0].Kind)
	}
	if upd.Expressions[0].Column != "price" {
		t.Errorf("expected column 'price', got %q", upd.Expressions[0].Column)
	}
	if upd.Expressions[0].Operator != "*" {
		t.Errorf("expected operator '*', got %q", upd.Expressions[0].Operator)
	}
	// Second: literal
	if upd.Expressions[1].Kind != ast.UpdateExprLiteral {
		t.Errorf("expected literal for expr 1, got %s", upd.Expressions[1].Kind)
	}
	if upd.Expressions[1].Value.StringValue != "sale" {
		t.Errorf("expected value 'sale', got %q", upd.Expressions[1].Value.StringValue)
	}
}

func TestParseUpdatePlainLiteralNoExpressions(t *testing.T) {
	statement, err := Parse("UPDATE users SET name = 'bob' WHERE id = 1;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	upd := statement.(ast.UpdateStatement)
	if upd.Expressions != nil {
		t.Fatalf("expected nil Expressions for plain literal UPDATE, got length %d", len(upd.Expressions))
	}
}

func TestParseUpdateSubtractExpression(t *testing.T) {
	statement, err := Parse("UPDATE inventory SET stock = stock - 5 WHERE product_id = 42;")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	upd := statement.(ast.UpdateStatement)
	if len(upd.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(upd.Expressions))
	}
	expr := upd.Expressions[0]
	if expr.Kind != ast.UpdateExprArithmetic || expr.Operator != "-" || expr.Operand.NumberValue != 5 {
		t.Errorf("unexpected expression: %+v", expr)
	}
}

func TestParseInListSnapshot(t *testing.T) {
	statement, err := Parse("SELECT * FROM users WHERE id IN (1, 2, 3)")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := statement.(ast.SelectStatement)
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if sel.Where.Operator != "IN" {
		t.Fatalf("expected IN operator, got %q", sel.Where.Operator)
	}
	if sel.Where.Subquery != nil {
		t.Fatal("expected no subquery for IN list")
	}
	if len(sel.Where.InValues) != 3 {
		t.Fatalf("expected 3 in_values, got %d", len(sel.Where.InValues))
	}
	for i, expected := range []int64{1, 2, 3} {
		if sel.Where.InValues[i].NumberValue != expected {
			t.Errorf("in_values[%d]: expected %d, got %d", i, expected, sel.Where.InValues[i].NumberValue)
		}
	}
}

func TestParseNotInListSnapshot(t *testing.T) {
	statement, err := Parse("SELECT * FROM users WHERE status NOT IN ('active', 'pending')")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := statement.(ast.SelectStatement)
	if sel.Where.Operator != "NOT IN" {
		t.Fatalf("expected NOT IN operator, got %q", sel.Where.Operator)
	}
	if len(sel.Where.InValues) != 2 {
		t.Fatalf("expected 2 in_values, got %d", len(sel.Where.InValues))
	}
	if sel.Where.InValues[0].StringValue != "active" {
		t.Errorf("expected 'active', got %q", sel.Where.InValues[0].StringValue)
	}
}

func TestParseBetweenSnapshot(t *testing.T) {
	statement, err := Parse("SELECT * FROM orders WHERE amount BETWEEN 10 AND 100")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := statement.(ast.SelectStatement)
	if sel.Where.Operator != "BETWEEN" {
		t.Fatalf("expected BETWEEN operator, got %q", sel.Where.Operator)
	}
	if sel.Where.Value.NumberValue != 10 {
		t.Errorf("expected lower bound 10, got %d", sel.Where.Value.NumberValue)
	}
	if sel.Where.Value2 == nil || sel.Where.Value2.NumberValue != 100 {
		t.Errorf("expected upper bound 100, got %v", sel.Where.Value2)
	}
}

func TestParseNotBetweenSnapshot(t *testing.T) {
	statement, err := Parse("SELECT * FROM orders WHERE price NOT BETWEEN 5.0 AND 50.0")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sel := statement.(ast.SelectStatement)
	if sel.Where.Operator != "NOT BETWEEN" {
		t.Fatalf("expected NOT BETWEEN, got %q", sel.Where.Operator)
	}
	if sel.Where.Value.FloatValue != 5.0 {
		t.Errorf("expected lower 5.0, got %f", sel.Where.Value.FloatValue)
	}
	if sel.Where.Value2 == nil || sel.Where.Value2.FloatValue != 50.0 {
		t.Errorf("expected upper 50.0, got %v", sel.Where.Value2)
	}
}

func TestParseInsertOnConflictDoNothingSnapshot(t *testing.T) {
	statement, err := Parse("INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins := statement.(ast.InsertStatement)
	if ins.OnConflict == nil {
		t.Fatal("expected OnConflict clause")
	}
	if ins.OnConflict.Action != ast.OnConflictDoNothing {
		t.Fatalf("expected DO NOTHING, got %q", ins.OnConflict.Action)
	}
	if len(ins.OnConflict.ConflictColumns) != 1 || ins.OnConflict.ConflictColumns[0] != "id" {
		t.Fatalf("expected conflict column [id], got %v", ins.OnConflict.ConflictColumns)
	}
}

func TestParseInsertOnConflictDoUpdateSnapshot(t *testing.T) {
	statement, err := Parse("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'a@b.com') ON CONFLICT (id) DO UPDATE SET name = 'Bob', email = 'b@b.com'")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins := statement.(ast.InsertStatement)
	if ins.OnConflict == nil {
		t.Fatal("expected OnConflict clause")
	}
	if ins.OnConflict.Action != ast.OnConflictDoUpdate {
		t.Fatalf("expected DO UPDATE, got %q", ins.OnConflict.Action)
	}
	if len(ins.OnConflict.UpdateColumns) != 2 {
		t.Fatalf("expected 2 update columns, got %d", len(ins.OnConflict.UpdateColumns))
	}
	if ins.OnConflict.UpdateColumns[0] != "name" || ins.OnConflict.UpdateColumns[1] != "email" {
		t.Errorf("unexpected update columns: %v", ins.OnConflict.UpdateColumns)
	}
	if ins.OnConflict.UpdateValues[0].StringValue != "Bob" {
		t.Errorf("expected 'Bob', got %q", ins.OnConflict.UpdateValues[0].StringValue)
	}
}

func TestParseInsertOnConflictExcludedSnapshot(t *testing.T) {
	statement, err := Parse("INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ins := statement.(ast.InsertStatement)
	if ins.OnConflict == nil {
		t.Fatal("expected OnConflict clause")
	}
	if ins.OnConflict.Action != ast.OnConflictDoUpdate {
		t.Fatalf("expected DO UPDATE, got %q", ins.OnConflict.Action)
	}
	if len(ins.OnConflict.UpdateColumns) != 1 || ins.OnConflict.UpdateColumns[0] != "name" {
		t.Fatalf("expected update column [name], got %v", ins.OnConflict.UpdateColumns)
	}
	if len(ins.OnConflict.UpdateExcluded) != 1 || ins.OnConflict.UpdateExcluded[0] != "name" {
		t.Fatalf("expected excluded [name], got %v", ins.OnConflict.UpdateExcluded)
	}
}

func TestParseUnionSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id FROM users UNION SELECT id FROM admins")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	setOp := statement.(ast.SetOperationStatement)
	if setOp.Op != ast.SetOpUnion {
		t.Fatalf("expected UNION, got %q", setOp.Op)
	}
	if setOp.Left.TableName != "users" {
		t.Errorf("expected left table 'users', got %q", setOp.Left.TableName)
	}
	if setOp.Right.TableName != "admins" {
		t.Errorf("expected right table 'admins', got %q", setOp.Right.TableName)
	}
}

func TestParseUnionAllSnapshot(t *testing.T) {
	statement, err := Parse("SELECT name FROM a UNION ALL SELECT name FROM b")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	setOp := statement.(ast.SetOperationStatement)
	if setOp.Op != ast.SetOpUnionAll {
		t.Fatalf("expected UNION ALL, got %q", setOp.Op)
	}
}

func TestParseIntersectSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id FROM users INTERSECT SELECT id FROM premium")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	setOp := statement.(ast.SetOperationStatement)
	if setOp.Op != ast.SetOpIntersect {
		t.Fatalf("expected INTERSECT, got %q", setOp.Op)
	}
}

func TestParseExceptSnapshot(t *testing.T) {
	statement, err := Parse("SELECT id FROM all_users EXCEPT SELECT id FROM banned")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	setOp := statement.(ast.SetOperationStatement)
	if setOp.Op != ast.SetOpExcept {
		t.Fatalf("expected EXCEPT, got %q", setOp.Op)
	}
}
