# ASQL

**A deterministic SQL engine built in Go.** Domain isolation, append-only WAL, time-travel queries, entity versioning, and optional distributed replication -- all in a single binary.

```
go run ./cmd/asqld -addr :9042 -data-dir .asql
```

---

## Why ASQL?

Every database forces you to choose: **simple** (SQLite) or **powerful** (PostgreSQL). ASQL refuses that tradeoff.

| Problem | ASQL's answer |
|---|---|
| You need multi-tenant data isolation but embedded simplicity | **Domain isolation** -- each domain has its own schema, constraints, and rules inside a single engine |
| You need to audit who changed what and when | **Time-travel queries** -- read any historical state by LSN or timestamp |
| You need cross-service consistency without distributed transactions | **Cross-domain transactions** -- atomic commits across domain boundaries |
| You need to debug production issues by reproducing exact state | **Deterministic replay** -- same WAL input always produces identical state |
| You need to track aggregate versions across related tables | **Entity versioning** -- automatic version tracking with versioned foreign keys |
| You need a database you can reason about | **Append-only WAL** -- the log is truth, materialized state is derived |

---

## Quickstart

### 1. Start the server

```bash
go run ./cmd/asqld -addr :9042 -data-dir .asql
```

### 2. Create a table and insert data

```bash
# Open a transaction in the "app" domain
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command begin -mode domain -domains app

# Use the returned tx_id for subsequent commands
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command execute -tx-id <tx_id> \
  -sql "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, created_at TIMESTAMP DEFAULT UUID_V7())"

go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command execute -tx-id <tx_id> \
  -sql "INSERT INTO users (id, email) VALUES (1, 'alice@example.com')"

go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command commit -tx-id <tx_id>
```

### 3. Query historical state

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command time-travel -domains app -lsn 4 \
  -sql "SELECT id, email FROM users"
```

### 4. Launch the Studio UI

```bash
go run ./cmd/asqlstudio -http-addr :9080 -grpc-endpoint 127.0.0.1:9042
# Open http://localhost:9080
```

Or use `make dev` to start everything at once.

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

-- Row-level change history with old/new values
SELECT * FROM invoices FOR HISTORY;
-- Returns: operation (INSERT/UPDATE/DELETE), commit_lsn, old values, new values
```

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
  id TEXT PRIMARY KEY DEFAULT UUID_V7(),
  recipe_id TEXT REFERENCES VERSIONED master.ingredients(id) AS OF recipe_version,
  recipe_version INT
);

-- JOINs across versioned references resolve to the captured version
IMPORT master.ingredients AS ingredients;
SELECT o.id, i.name
FROM process_orders o
JOIN ingredients i ON o.recipe_id = i.id;
-- Automatically reads ingredients at the version captured in recipe_version
```

### SQL support

**DDL**
```sql
CREATE TABLE products (
  id TEXT PRIMARY KEY DEFAULT UUID_V7(),
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
| `UUID` | - | UUID with `UUID_V7()` default generation |

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
  recipe_id TEXT REFERENCES VERSIONED master.ingredients(id) AS OF recipe_version,
  recipe_version INT
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
# Leader
go run ./cmd/asqld -addr :9042 -data-dir .asql-leader \
  -peers follower1@127.0.0.1:9043 -groups default

# Follower
go run ./cmd/asqld -addr :9043 -data-dir .asql-follower \
  -peers leader@127.0.0.1:9042 -groups default
```

- **WAL-based streaming** -- followers replicate by streaming the leader's WAL
- **Read routing** -- `strong` reads go to leader; `bounded-stale` reads go to follower when lag is within threshold, with automatic leader fallback
- **Catch-up sync** -- followers read from their last LSN and apply records incrementally

---

## Tools

### ASQL Studio

Web UI for managing ASQL interactively at `http://localhost:9080`.

- SQL query editor with domain selection
- Schema browser with ER diagram visualization
- Transaction controls (BEGIN / EXECUTE / COMMIT / ROLLBACK)
- Time-travel query panel
- Scan strategy statistics
- Replication lag monitoring (with `-follower-grpc-endpoint`)

```bash
make dev  # Starts asqld + asqlstudio + vite HMR
```

### CLI (asqlctl)

```bash
asqlctl -endpoint 127.0.0.1:9042 -command begin -mode domain -domains app
asqlctl -endpoint 127.0.0.1:9042 -command execute -tx-id <id> -sql "..."
asqlctl -endpoint 127.0.0.1:9042 -command commit -tx-id <id>
asqlctl -endpoint 127.0.0.1:9042 -command time-travel -domains app -lsn 10 -sql "SELECT ..."
```

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

See [docs/postgres-compatibility-surface-v1.md](docs/postgres-compatibility-surface-v1.md) for the exact supported and unsupported behavior.

### gRPC API

Native gRPC with JSON codec. Full API at `api/proto/asql/v1/service.proto`.

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
go run ./cmd/asqld -addr :9042 -data-dir .asql -auth-token my-secret

# mTLS (mutual TLS)
go run ./cmd/asqld -addr :9042 -data-dir .asql \
  -tls-cert ./certs/server.pem \
  -tls-key ./certs/server-key.pem \
  -tls-client-ca ./certs/ca.pem
```

---

## Docker

```bash
make docker-build   # Build image (asql:local)
make docker-run     # Run on :9042 with persistent volume

# Or directly:
docker build -t asql:local .
docker run -p 9042:9042 -v $(pwd)/.data:/data asql:local
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
| [Getting started (10 min)](docs/getting-started-10-min.md) | Hands-on guide from zero to time-travel |
| [Go SDK cookbook](docs/cookbook-go-sdk.md) | Code recipes for common operations |
| [Architecture one-pager](docs/architecture-one-pager-v1.md) | System design overview |
| [Benchmark one-pager](docs/benchmark-one-pager-v1.md) | Performance characteristics |
| [SQLite migration path](docs/migration-sqlite-quick-path.md) | Migrate from SQLite to ASQL |
| [PostgreSQL compatibility](docs/postgres-compatibility-surface-v1.md) | pgwire protocol support matrix |
| [SLO definitions](docs/slo-v1.md) | Service level objectives |
| [Runbook](docs/runbook.md) | Executable demo commands |
| [Incident runbook](docs/incident-runbook-v1.md) | Operational procedures |
| [Security disclosure](docs/security-disclosure-policy-v1.md) | Vulnerability reporting |

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
