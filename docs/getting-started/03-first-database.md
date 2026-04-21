# 03. First Database

This guide creates a small schema, writes data, and reads it back using the pgwire shell.

:::steps
1. **Open the shell**

   ```bash
   go run ./cmd/asqlctl -command shell -pgwire 127.0.0.1:5433
   ```

2. **Create a table and insert data**

   ```sql
   BEGIN DOMAIN app;
   CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, status TEXT);
   INSERT INTO users (id, email, status) VALUES (1, 'alice@example.com', 'active');
   COMMIT;
   ```

3. **Read the data back**

   ```sql
   SELECT * FROM users;
   ```

4. **Inspect the same data in Studio**

   If Studio is running: open `Workspace`, select domain `app`, run `SELECT * FROM users`.
:::

## What you learned

:::info[The two ASQL essentials]
You already used the most important building block: **explicit transaction scope** before any schema or DML work (`BEGIN DOMAIN app` / `COMMIT`).

You also used the canonical local interface: **pgwire** through the built-in shell. That same pgwire endpoint is what Studio, `pgx`, and any PostgreSQL-compatible tool will connect to.
:::

## Next step

Continue with [04-domains-and-transactions.md](04-domains-and-transactions.md).
