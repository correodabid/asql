# 03. First Database

This guide creates a small schema, writes data, and reads it back.

## Option A: use `asqlctl`

### 1. Open a domain transaction

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command begin -mode domain -domains app
```

Save the returned `tx_id`.

### 2. Create a table

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command execute -tx-id <tx_id> \
  -sql "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, status TEXT)"
```

### 3. Insert data

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command execute -tx-id <tx_id> \
  -sql "INSERT INTO users (id, email, status) VALUES (1, 'alice@example.com', 'active')"
```

### 4. Commit

```bash
go run ./cmd/asqlctl -endpoint 127.0.0.1:9042 \
  -command commit -tx-id <tx_id>
```

## Option B: use the example client

```bash
go run ./examples/go-client -endpoint 127.0.0.1:9042 -domain app -table users -id 1 -email bootstrap@example.com -init-schema
```

## Read the data back

Use Studio, or a pgwire-compatible client, or the Go example path.

Example with Studio:

- open `Workspace`,
- select domain `app`,
- run `SELECT * FROM users`.

## What you learned

At this point you have already used the most important ASQL building block:

- explicit transaction scope before schema or DML work.

## Next step

Continue with [04-domains-and-transactions.md](04-domains-and-transactions.md).
