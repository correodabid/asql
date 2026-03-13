# ASQL Getting Started (10 Minutes)

This is the shortest path into ASQL.

For the full adoption path, use [docs/getting-started/README.md](README.md).

## 1) Optional: validate project health

```bash
go test ./...
```

If you only want the fastest local walkthrough, skip straight to starting `asqld`.

## 2) Start ASQL locally

```bash
go run ./cmd/asqld -addr :5433 -data-dir .asql
```

Keep this terminal open.

## 3) Open the interactive shell

```bash
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

## 4) Run a first transaction

In the shell:

```sql
BEGIN DOMAIN app;
CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, status TEXT);
INSERT INTO users (id, email, status) VALUES (1, 'alice@example.com', 'active');
COMMIT;
SELECT * FROM users;
```

## 5) Optional Studio path

```bash
go run ./asqlstudio -pgwire-endpoint 127.0.0.1:5433 -data-dir .asql
```

## 6) Optional fixture path

```bash
go run ./cmd/asqlctl -command fixture-validate \
	-fixture-file fixtures/healthcare-billing-demo-v1.json
```

## Next reading

- [docs/getting-started/03-first-database.md](03-first-database.md)
- [docs/getting-started/04-domains-and-transactions.md](04-domains-and-transactions.md)
- [docs/getting-started/05-time-travel-and-history.md](05-time-travel-and-history.md)
- [docs/getting-started/07-fixtures-and-seeding.md](07-fixtures-and-seeding.md)