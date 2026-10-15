package determinism_test

import (
	"fmt"
	"math/rand"
)

// The workload covers what replay must reproduce: multi-domain and cross-
// domain transactions, indexes, entities, foreign and versioned foreign keys,
// literal, autoincrement and volatile (UUID_V7, TX_TIMESTAMP) defaults,
// multi-row inserts, predicate UPDATE and DELETE, schema evolution,
// savepoints, and rolled-back transactions. Phases are meant to be run with
// restarts or crashes in between.

var workloadProbes = probes{entities: []entityProbe{
	{"inventory", "item_entity", "1"},
	{"inventory", "item_entity", "7"},
	{"inventory", "item_entity", "12"},
	{"quality", "result_entity", "r-3"},
}}

// schemaPhase creates the schema. With stable, stock moves have literal
// defaults instead of UUID_V7 and TX_TIMESTAMP, so two runs of the workload
// write identical WALs.
func schemaPhase(stable bool) []tx {
	moves := "CREATE TABLE inventory.item_moves (id INT PRIMARY KEY DEFAULT AUTOINCREMENT, item_id INT NOT NULL REFERENCES items(id), delta INT NOT NULL, uid TEXT DEFAULT UUID_V7, created_at TEXT DEFAULT TX_TIMESTAMP)"
	if stable {
		moves = "CREATE TABLE inventory.item_moves (id INT PRIMARY KEY DEFAULT AUTOINCREMENT, item_id INT NOT NULL REFERENCES items(id), delta INT NOT NULL, uid TEXT DEFAULT 'none', created_at TEXT DEFAULT 'never')"
	}
	return []tx{
		{domains: []string{"inventory"}, statements: []string{
			"CREATE TABLE inventory.items (id INT PRIMARY KEY, sku TEXT UNIQUE, qty INT NOT NULL DEFAULT 0, status TEXT DEFAULT 'ACTIVE')",
			moves,
			"CREATE INDEX idx_items_status ON items (status) USING HASH",
			"CREATE INDEX idx_items_qty ON items (qty) USING BTREE",
			"CREATE INDEX idx_moves_item ON item_moves (item_id) USING HASH",
			"CREATE ENTITY item_entity (ROOT items, INCLUDES item_moves)",
		}},
		{domains: []string{"quality", "inventory"}, statements: []string{
			"CREATE TABLE quality.results (id TEXT PRIMARY KEY, item_id INT NOT NULL, item_lsn INT, result TEXT NOT NULL, VERSIONED FOREIGN KEY (item_id) REFERENCES inventory.items(id) AS OF item_lsn)",
		}},
		{domains: []string{"quality"}, statements: []string{
			"CREATE ENTITY result_entity (ROOT results)",
		}},
	}
}

// loadPhase inserts items and stock moves, with a seeded mix of statement
// shapes so the workload is the same on every run.
func loadPhase() []tx {
	rng := rand.New(rand.NewSource(42))
	var txs []tx
	for id := 1; id <= 24; id += 2 {
		txs = append(txs, tx{domains: []string{"inventory"}, statements: []string{
			fmt.Sprintf("INSERT INTO items (id, sku, qty) VALUES (%d, 'SKU-%03d', %d), (%d, 'SKU-%03d', %d)",
				id, id, rng.Intn(50), id+1, id+1, rng.Intn(50)),
			fmt.Sprintf("INSERT INTO item_moves (item_id, delta) VALUES (%d, %d)", id, rng.Intn(20)-10),
		}})
	}
	for i := 0; i < 10; i++ {
		item := 1 + rng.Intn(24)
		txs = append(txs, tx{domains: []string{"inventory"}, statements: []string{
			fmt.Sprintf("UPDATE items SET qty = %d WHERE id = %d", rng.Intn(100), item),
			fmt.Sprintf("INSERT INTO item_moves (item_id, delta) VALUES (%d, %d)", item, rng.Intn(20)-10),
		}})
	}
	return txs
}

func evolvePhase() []tx {
	return []tx{
		{domains: []string{"inventory"}, statements: []string{
			"UPDATE items SET status = 'LOW' WHERE qty < 10",
			"DELETE FROM item_moves WHERE delta = 0",
		}},
		{domains: []string{"inventory"}, statements: []string{
			"ALTER TABLE items ADD COLUMN location TEXT",
			"UPDATE items SET location = 'A1' WHERE qty >= 25",
		}},
		// Savepoint: only the statements before ROLLBACK TO survive.
		{domains: []string{"inventory"}, statements: []string{
			"UPDATE items SET location = 'B2' WHERE id = 3",
			"SAVEPOINT before_delete",
			"DELETE FROM items WHERE id = 24",
			"ROLLBACK TO SAVEPOINT before_delete",
			"INSERT INTO item_moves (item_id, delta) VALUES (3, 5)",
		}},
		// Rolled back: never reaches the WAL.
		{domains: []string{"inventory"}, rollback: true, statements: []string{
			"DELETE FROM items WHERE status = 'LOW'",
		}},
	}
}

func crossDomainPhase() []tx {
	var txs []tx
	for i := 1; i <= 8; i++ {
		txs = append(txs, tx{domains: []string{"quality", "inventory"}, statements: []string{
			fmt.Sprintf("INSERT INTO quality.results (id, item_id, result) VALUES ('r-%d', %d, '%s')", i, i, []string{"WITHIN_SPEC", "OUT_OF_SPEC"}[i%2]),
			fmt.Sprintf("UPDATE inventory.items SET qty = qty + 1 WHERE id = %d", i),
		}})
	}
	txs = append(txs,
		tx{domains: []string{"quality"}, statements: []string{"UPDATE results SET result = 'RETESTED' WHERE result = 'OUT_OF_SPEC'"}},
		tx{domains: []string{"quality"}, statements: []string{"DELETE FROM results WHERE id = 'r-8'"}},
	)
	return txs
}

func workloadPhases() [][]tx {
	return [][]tx{schemaPhase(false), loadPhase(), evolvePhase(), crossDomainPhase()}
}

func stableWorkloadPhases() [][]tx {
	return [][]tx{schemaPhase(true), loadPhase(), evolvePhase(), crossDomainPhase()}
}
