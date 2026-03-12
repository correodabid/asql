# 03. First Database

This guide creates a small schema, writes data, and reads it back using the pgwire shell.

## 1. Open the shell

```bash
go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
```

## 2. Create a table and insert data

Run this in the shell:

```sql
BEGIN DOMAIN app;
CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, status TEXT);
INSERT INTO users (id, email, status) VALUES (1, 'alice@example.com', 'active');
COMMIT;
```

## 3. Read the data back

Still in the shell:

```sql
SELECT * FROM users;
```

## 4. Inspect the same data in Studio

If Studio is running:

- open `Workspace`
- select domain `app`
- run `SELECT * FROM users`

## What you learned

At this point you have already used the most important ASQL building block:

- explicit transaction scope before schema or DML work.

You also used the current canonical local interface:

- pgwire through the built-in shell.

## Next step

Continue with [04-domains-and-transactions.md](04-domains-and-transactions.md).
