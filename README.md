# ASQL

**A deterministic SQL engine built in Go.** Domain isolation, append-only WAL, time-travel queries, entity versioning, and optional clustered operation through pgwire + Raft.

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
```

---

## Why ASQL?

Every database forces you to choose: **simple** (SQLite) or **powerful** (PostgreSQL). ASQL refuses that tradeoff.

| Problem | ASQL's answer |
|---|---|
| You need multi-tenant data isolation but embedded simplicity | **Domain isolation** -- each domain has its own schema, constraints, and rules inside a single engine |
| You need to audit who changed what and when | **Time-travel queries** -- read historical state by commit LSN |
| You need cross-service consistency without distributed transactions | **Cross-domain transactions** -- atomic commits across domain boundaries |
| You need to debug production issues by reproducing exact state | **Deterministic replay** -- same WAL input always produces identical state |
| You need to track aggregate versions across related tables | **Entity versioning** -- automatic version tracking with versioned foreign keys |
| You need a database you can reason about | **Append-only WAL** -- the log is truth, materialized state is derived |

---

## Quickstart

The primary onboarding path is [docs/getting-started/README.md](docs/getting-started/README.md).

Short local path:

### 1. Start ASQL locally

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
```

### 2. Open the interactive shell

```bash
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

### 3. Run a first transaction

```sql
BEGIN DOMAIN app;
CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, status TEXT);
INSERT INTO users (id, email, status) VALUES (1, 'alice@example.com', 'active');
COMMIT;
SELECT * FROM users;
```

### 4. Launch ASQL Studio

```bash
go run ./asqlstudio -pgwire-endpoint 127.0.0.1:5433 -data-dir .asql
```

### 5. Validate and load a deterministic fixture

```bash
go run ./cmd/asqlctl -command fixture-validate \
  -fixture-file fixtures/healthcare-billing-demo-v1.json

go run ./cmd/asqlctl -command fixture-load \
  -pgwire 127.0.0.1:5433 \
  -fixture-file fixtures/healthcare-billing-demo-v1.json
```

For time-travel, entities, fixtures, Studio, and integration patterns, continue with [docs/getting-started/README.md](docs/getting-started/README.md).

---

## Core features

### Domain isolation

Every transaction declares its scope. Domains are isolated data boundaries with independent schemas, constraints, and indexes.

```sql
-- Single domain
BEGIN DOMAIN billing;
CREATE TABLE invoices (id INT PRIMARY KEY, amount FLOAT, status TEXT);
INSERT INTO invoices (id, amount, status) VALUES (1, 250.00, 'pending');
COMMIT;

-- Cross-domain atomic transaction
BEGIN CROSS DOMAIN billing, inventory;
INSERT INTO invoices (id, amount, status) VALUES (2, 100.00, 'paid');
UPDATE stock SET quantity = quantity - 1 WHERE product_id = 42;
COMMIT;
```

Domains are created implicitly on first `BEGIN DOMAIN`. No configuration needed.

### Time-travel queries

Read data as it existed at any point in history. Every committed transaction gets a monotonic LSN (Log Sequence Number).

```sql
-- Read state at a specific LSN
SELECT * FROM users AS OF LSN 42;

-- Inspect the current visible head LSN
SELECT current_lsn();

-- Inspect the current visible row-head LSN for a specific row
SELECT row_lsn('billing.invoices', '42');

-- Inspect the latest entity version and its head commit LSN
SELECT entity_version('recipes', 'recipe_aggregate', 'recipe-1');
SELECT entity_head_lsn('recipes', 'recipe_aggregate', 'recipe-1');
SELECT entity_version_lsn('recipes', 'recipe_aggregate', 'recipe-1', 3);

-- Inspect the exact token a versioned foreign key would capture right now
SELECT resolve_reference('recipes.master_recipes', '1');

-- Row-level change history with a stable metadata contract
SELECT * FROM invoices FOR HISTORY;
-- Returns: __operation, __commit_lsn, and the row image at that commit
-- INSERT => inserted row image
-- UPDATE => post-update row image
-- DELETE => pre-delete row image
```

`resolve_reference(...)` returns the latest entity version for entity root
tables, and the current row-head `_lsn` for non-entity tables.
`entity_version_lsn(...)` lets clients translate a business-facing entity
version into the exact replay-safe `LSN` needed for `AS OF LSN` reads.

### Entity versioning

Track aggregate versions across related tables. When any table in an entity changes, the entity version increments automatically.

```sql
-- Define an entity spanning multiple tables
CREATE ENTITY recipe (
  ROOT ingredients,
  INCLUDES steps, quality_checks
);

-- Versioned foreign keys capture the entity version at insert time
CREATE TABLE process_orders (
  id TEXT PRIMARY KEY DEFAULT UUID_V7,
  recipe_id TEXT,
  recipe_version INT,
  VERSIONED FOREIGN KEY (recipe_id)
    REFERENCES master.ingredients(id)
    AS OF recipe_version
);

-- Explicit override remains available when you need precise historical control
INSERT INTO process_orders (id, recipe_id, recipe_version)
VALUES ('po-1', 'recipe-1', 3);

-- JOINs across versioned references resolve to the captured version
IMPORT master.ingredients AS ingredients;
SELECT o.id, i.name
FROM process_orders o
JOIN ingredients i ON o.recipe_id = i.id;
-- Automatically reads ingredients at the version captured in recipe_version
```

Auto-capture resolves against the transaction-visible snapshot. Later
statements in the same transaction can reference rows or entity versions
created earlier in that transaction, and replay reconstructs the same captured
tokens deterministically from WAL order.

### SQL support

**DDL**
```sql
CREATE TABLE products (
  id TEXT PRIMARY KEY DEFAULT UUID_V7,
  name TEXT NOT NULL,
  price FLOAT CHECK (price > 0),
  category_id INT REFERENCES categories(id),
  created_at TIMESTAMP,
  counter INT DEFAULT AUTO_INCREMENT
);

CREATE INDEX idx_products_name ON products USING btree (name);
CREATE INDEX idx_products_category ON products USING hash (category_id);
ALTER TABLE products ADD COLUMN description TEXT;
```

**DML**
```sql
INSERT INTO products (name, price) VALUES ('Widget', 29.99) RETURNING id, name, counter;
UPDATE products SET price = 24.99 WHERE name = 'Widget';
DELETE FROM products WHERE price < 10;
```

**Queries**
```sql
-- JOINs
SELECT o.id, u.email, p.name
FROM orders o
INNER JOIN users u ON o.user_id = u.id
LEFT JOIN products p ON o.product_id = p.id;

-- Aggregations
SELECT category_id, COUNT(*), AVG(price), SUM(price)
FROM products
GROUP BY category_id
HAVING COUNT(*) > 5;

-- Window functions
SELECT name, price,
  ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY price DESC),
  RANK() OVER (ORDER BY price DESC),
  LAG(price) OVER (ORDER BY created_at),
  LEAD(price) OVER (ORDER BY created_at)
FROM products;

-- CTEs
WITH expensive AS (
  SELECT * FROM products WHERE price > 100
)
SELECT category_id, COUNT(*) FROM expensive GROUP BY category_id;

-- Subqueries
SELECT * FROM users
WHERE id IN (SELECT user_id FROM orders WHERE total > 500);

SELECT * FROM products
WHERE EXISTS (SELECT 1 FROM order_items WHERE product_id = products.id);

-- Cross-domain reads
IMPORT billing.invoices AS invoices;
SELECT * FROM local_table JOIN invoices ON local_table.invoice_id = invoices.id;
```

**Transactions**
```sql
BEGIN DOMAIN app;
SAVEPOINT before_update;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
ROLLBACK TO SAVEPOINT before_update;  -- undo the update
COMMIT;
```

### Data types

| Type | Aliases | Description |
|------|---------|-------------|
| `INT` | `INTEGER`, `BIGINT` | 64-bit signed integer |
| `TEXT` | `VARCHAR`, `STRING` | UTF-8 string |
| `FLOAT` | `REAL`, `DOUBLE` | 64-bit floating point |
| `BOOLEAN` | `BOOL` | true/false |
| `TIMESTAMP` | - | Date-time with microsecond precision |
| `UUID` | - | UUID with `UUID_V7` default generation |

### Indexes

```sql
-- Hash index: O(1) equality lookups
CREATE INDEX idx_email ON users USING hash (email);

-- BTree index: range queries, ordering, prefix scans
CREATE INDEX idx_price ON products USING btree (price);

-- Composite index
CREATE INDEX idx_order_date ON orders USING btree (customer_id, created_at);
```

The query planner automatically selects the optimal scan strategy:

| Strategy | When used |
|----------|-----------|
| `hash` | Equality predicate on hash-indexed column |
| `btree-lookup` | Range predicate on btree-indexed column |
| `btree-order` | ORDER BY matches btree column order |
| `btree-prefix` | Equality + range on composite btree |
| `join-right-index` / `join-left-index` | Index-accelerated JOINs |
| `full-scan` | No applicable index |

### Constraints

```sql
CREATE TABLE accounts (
  id INT PRIMARY KEY,
  email TEXT UNIQUE,
  balance FLOAT CHECK (balance >= 0),
  owner_id INT REFERENCES users(id),
  recipe_id TEXT,
  recipe_version INT,
  VERSIONED FOREIGN KEY (recipe_id)
    REFERENCES master.ingredients(id)
    AS OF recipe_version
);
```

- **PRIMARY KEY** -- unique row identity
- **UNIQUE** -- enforced via index
- **FOREIGN KEY** -- referential integrity within domain
- **CHECK** -- expression validation on write
- **VERSIONED FOREIGN KEY** -- cross-domain reference with entity version capture

---

## Architecture

```
                        Clients
                    gRPC  |  pgwire
                          v
                +---------+---------+
                |    Server Layer   |
                |  grpc  | pgwire* |
                +---------+---------+
                          |
              +-----------+-----------+
              |       Engine          |
              |  parser -> planner -> |
              |  executor -> tx mgr  |
              +-----------+-----------+
                          |
              +-----------+-----------+
              |    Storage Layer      |
              |   WAL   | Snapshots  |
              +-----------+-----------+
                          |
              +-----------+-----------+
              |  Cluster (optional)   |
              |  Raft + sidecar RPCs  |
              +-------------------------+
```

`pgwire*` is the primary runtime. In clustered deployments, the production
path is pgwire + Raft, with a gRPC sidecar for cluster communication.
Standalone gRPC remains useful for APIs and transitional flows, but it is not
the canonical production cluster runtime.

### Design principles

- **Hexagonal architecture** -- pure engine core with ports/adapters for storage, transport, and time
- **Determinism first** -- same WAL input always produces same state. No wall-clock dependencies in the execution path
- **Append-only truth** -- the WAL is the source of truth; materialized state is rebuilt from it
- **Lock-free reads** -- readers access immutable state snapshots via atomic pointer; never block writers
- **COW mutations** -- writers clone affected state, apply mutations, then atomically swap the pointer

### Performance

The engine is optimized for high-throughput concurrent workloads:

- **Commit coalescing** -- concurrent commits are batched into a single lock acquisition, WAL write, state swap, and fsync. Under N concurrent writers, overhead drops from O(N) to O(1)
- **Group commit** -- fsync calls are batched across transactions, reducing disk I/O
- **Overlay indexes** -- INSERT operations create O(1) index overlays instead of copying entire index structures. Chains are flattened when depth exceeds 128
- **Binary snapshots** -- gzip-compressed binary format with delta encoding. Snapshots accelerate recovery and enable WAL truncation

### WAL and recovery

```
[WAL records] --> [Replay] --> [Materialized state]
                                      |
                            [Periodic snapshots]
                                      |
                       [Snapshot + partial WAL replay]
```

Every mutation is written to the append-only WAL before becoming visible. On restart, the engine loads the latest snapshot and replays only the WAL records after that point. Snapshots are taken automatically every 500 mutations.

### Replication

```bash
# Node A
go run ./cmd/asqld -addr :5433 -data-dir .asql-node-a \
  -node-id node-a -grpc-addr :6433 \
  -peers node-b@127.0.0.1:6434,node-c@127.0.0.1:6435 \
  -groups default

# Node B
go run ./cmd/asqld -addr :5434 -data-dir .asql-node-b \
  -node-id node-b -grpc-addr :6434 \
  -peers node-a@127.0.0.1:6433,node-c@127.0.0.1:6435 \
  -groups default
```

- **WAL-based streaming** -- followers replicate by streaming the leader's WAL
- **Read routing** -- `strong` reads go to leader; `bounded-stale` reads go to follower when lag is within threshold, with automatic leader fallback
- **Catch-up sync** -- followers read from their last LSN and apply records incrementally

Cluster mode extends the single-node runtime. It should not replace the local standalone onboarding path.

---

## Tools

### ASQL Studio

Desktop Studio for managing ASQL interactively over pgwire.

Current first-run path and surfaces:

- guided `Start Here` overview,
- `Workspace` for SQL queries and transaction controls (`Begin`, `Run`, `Commit`, `Rollback`),
- `Time Explorer` for temporal history and diffs,
- `Fixtures` for validate/load/export flows,
- `Dashboard`, `Cluster`, and `Recovery` panels for engine and cluster visibility,
- query-plan / scan-strategy inspection and replication-lag monitoring when follower or peer endpoints are configured.

```bash
go run ./asqlstudio -pgwire-endpoint 127.0.0.1:5433 -data-dir .asql
```

### CLI (asqlctl)

```bash
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
go run ./cmd/asqlctl -command fixture-validate -fixture-file fixtures/healthcare-billing-demo-v1.json
go run ./cmd/asqlctl -command fixture-load -pgwire 127.0.0.1:5433 -fixture-file fixtures/healthcare-billing-demo-v1.json
```

The lower-level `begin` / `execute` / `commit` and `time-travel` commands still exist for engine-oriented workflows, but the normal developer path should be pgwire SQL, fixtures, and Studio.

### PostgreSQL wire protocol

ASQL implements a narrow PostgreSQL wire-compatibility wedge:

- simple query protocol
- extended query protocol for the current ASQL SQL subset
- narrow `COPY FROM STDIN` / `COPY TO STDOUT` support for table ingest/export flows
- optional password challenge when `-auth-token` is configured
- text result rows with a limited PostgreSQL type surface

Connect with PostgreSQL clients such as `pgx` against the documented compatibility surface:

```go
conn, _ := pgx.Connect(ctx, "postgres://localhost:5433/asql")
conn.Exec(ctx, "BEGIN DOMAIN myapp")
conn.Exec(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
conn.Exec(ctx, "COMMIT")
```

See [docs/reference/sql-pgwire-compatibility-policy-v1.md](docs/reference/sql-pgwire-compatibility-policy-v1.md) for the policy stance and [docs/reference/postgres-compatibility-surface-v1.md](docs/reference/postgres-compatibility-surface-v1.md) for the exact supported and unsupported behavior.

### gRPC API

Native gRPC with JSON codec. Full API at `api/proto/asql/v1/service.proto`.

For most services, this is a secondary integration surface. Prefer pgwire for normal application reads and writes.

```protobuf
service ASQLService {
  rpc BeginTx(BeginTxRequest) returns (BeginTxResponse);
  rpc Execute(ExecuteRequest) returns (ExecuteResponse);
  rpc CommitTx(CommitTxRequest) returns (CommitTxResponse);
  rpc RollbackTx(RollbackTxRequest) returns (RollbackTxResponse);
  rpc Query(QueryRequest) returns (QueryResponse);
  rpc TimeTravelQuery(TimeTravelQueryRequest) returns (TimeTravelQueryResponse);
  rpc ExplainQuery(ExplainQueryRequest) returns (ExplainQueryResponse);
  rpc RowHistory(RowHistoryRequest) returns (RowHistoryResponse);
  rpc EntityVersionHistory(...) returns (...);
  rpc SchemaSnapshot(...) returns (...);
  rpc EngineStats(...) returns (...);
  // ... and more
}
```

---

## Security

```bash
# Shared pgwire password / bearer token authentication
go run ./cmd/asqld -addr :5433 -data-dir .asql -auth-token my-secret
```

TLS transport is not part of the current local pgwire runtime surface.
See [docs/reference/postgres-compatibility-surface-v1.md](docs/reference/postgres-compatibility-surface-v1.md) for the current compatibility stance.

---

## Docker

```bash
docker build -t asql:local .
docker run -p 5433:5433 -v $(pwd)/.data:/data asql:local
```

Uses `gcr.io/distroless/static-debian12` as runtime base for minimal attack surface.

---

## Use cases

**Event-driven backends** -- Every state change is a WAL record. Replay the log to reconstruct state at any point. Debug production issues by reproducing exact sequences.

**Multi-tenant SaaS** -- Each tenant gets a domain with isolated schema and constraints. Cross-tenant operations use explicit cross-domain transactions.

**Audit and compliance** -- `FOR HISTORY` queries provide row-level change tracking with old/new values and commit LSNs. Deterministic replay proves data integrity.

**Edge and offline-first** -- Single-binary deployment with embedded WAL. No external dependencies. Sync via WAL replication when connectivity returns.

**Pharmaceutical manufacturing (eBR)** -- Entity versioning with versioned foreign keys tracks recipe versions. Process orders reference the exact recipe version used, with full audit trail.

---

## Development

```bash
make test              # All tests
make test-race         # Tests with race detector
make bench             # Benchmark suite
make ci                # Full CI pipeline (fmt + vet + tests + race)
make security-scan     # govulncheck
make sbom              # Generate SPDX SBOM

make dev               # Start dev environment (asqld + studio + vite HMR)
make dev-cluster       # Start 3-node cluster dev environment

make seed-billing      # Seed data: 100 recipes with ingredients/steps
make seed-domains      # Seed data: recipes + process orders
make seed-domains-10x  # 10x scale: 1K recipes, 3K orders
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [Getting started (10 min)](docs/getting-started/10-min.md) | Hands-on guide from zero to time-travel |
| [Go SDK cookbook](docs/reference/cookbook-go-sdk.md) | Code recipes for common operations |
| [Architecture one-pager](docs/architecture/architecture-one-pager-v1.md) | System design overview |
| [Benchmark one-pager](docs/product/benchmark-one-pager-v1.md) | Performance characteristics |
| [Fixture format and lifecycle](docs/reference/fixture-format-and-lifecycle-v1.md) | Deterministic scenario file contract and loader workflow |
| [SQLite migration path](docs/migration/sqlite-quick-path.md) | Migrate from SQLite to ASQL |
| [PostgreSQL compatibility](docs/reference/postgres-compatibility-surface-v1.md) | pgwire protocol support matrix |
| [SLO definitions](docs/operations/slo-v1.md) | Service level objectives |
| [Runbook](docs/operations/runbook.md) | Executable demo commands |
| [Incident runbook](docs/operations/incident-runbook-v1.md) | Operational procedures |
| [Security disclosure](docs/operations/security-disclosure-policy-v1.md) | Vulnerability reporting |

---

## Project structure

```
cmd/
  asqld/              Server binary
  asqlctl/            CLI tool
  asqlstudio/         Web UI (React + Go HTTP bridge)
internal/
  engine/
    parser/           SQL parser and AST
    planner/          Query planner
    executor/         Query execution, transactions, snapshots
    domains/          Domain catalog
    ports/            Interface definitions (hexagonal)
  storage/
    wal/              Write-ahead log implementation
  cluster/
    coordinator/      Leadership and coordination
    replication/      WAL streaming and catch-up
    heartbeat/        Node health monitoring
  server/
    grpc/             gRPC transport layer
    pgwire/           PostgreSQL wire protocol
  platform/
    clock/            Deterministic time abstraction
api/
  proto/              Protocol buffer definitions
examples/
  go-client/          Reference Go client
docs/                 Documentation
test/
  integration/        End-to-end tests
```

---

## License

See [LICENSE](LICENSE) for details.
