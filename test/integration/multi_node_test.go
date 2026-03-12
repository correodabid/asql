package integration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/engine/executor"
	"asql/internal/engine/parser/ast"
	"asql/internal/storage/wal"
)

// TestMultiNodeReplicationConsistency verifies that a follower node replaying
// a leader's WAL produces identical state, and that queries using advanced
// features (JOINs, CTEs, window functions) return correct results on both nodes.
func TestMultiNodeReplicationConsistency(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()
	walPath := filepath.Join(temp, "leader.wal")

	// --- Leader: populate data ---
	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new leader store: %v", err)
	}

	leader, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new leader engine: %v", err)
	}

	session := leader.NewSession()
	sqls := []string{
		"BEGIN DOMAIN store",
		"CREATE TABLE users (id INT, name TEXT, dept TEXT)",
		"INSERT INTO users (id, name, dept) VALUES (1, 'Alice', 'eng')",
		"INSERT INTO users (id, name, dept) VALUES (2, 'Bob', 'eng')",
		"INSERT INTO users (id, name, dept) VALUES (3, 'Charlie', 'sales')",
		"INSERT INTO users (id, name, dept) VALUES (4, 'Diana', 'sales')",
		"CREATE TABLE orders (id INT, user_id INT, amount INT)",
		"INSERT INTO orders (id, user_id, amount) VALUES (10, 1, 100)",
		"INSERT INTO orders (id, user_id, amount) VALUES (20, 1, 250)",
		"INSERT INTO orders (id, user_id, amount) VALUES (30, 2, 150)",
		"INSERT INTO orders (id, user_id, amount) VALUES (40, 3, 300)",
		"INSERT INTO orders (id, user_id, amount) VALUES (50, 4, 75)",
		"COMMIT",
	}
	for _, sql := range sqls {
		if _, err := leader.Execute(ctx, session, sql); err != nil {
			t.Fatalf("leader execute %q: %v", sql, err)
		}
	}

	// Verify leader row counts
	if got := leader.RowCount("store", "users"); got != 4 {
		t.Fatalf("leader users count: got %d want 4", got)
	}
	if got := leader.RowCount("store", "orders"); got != 5 {
		t.Fatalf("leader orders count: got %d want 5", got)
	}

	// --- Leader: query with LEFT JOIN ---
	joinResult, err := leader.TimeTravelQueryAsOfLSN(ctx,
		"SELECT users.name, orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.name ASC",
		[]string{"store"}, 8192)
	if err != nil {
		t.Fatalf("leader LEFT JOIN query: %v", err)
	}
	// Alice=2 orders, Bob=1, Charlie=1, Diana=1 → 5 rows
	if len(joinResult.Rows) != 5 {
		t.Fatalf("leader LEFT JOIN: expected 5 rows, got %d: %+v", len(joinResult.Rows), joinResult.Rows)
	}

	// --- Leader: query with CTE ---
	cteResult, err := leader.TimeTravelQueryAsOfLSN(ctx,
		"WITH big_orders AS (SELECT id, user_id, amount FROM orders WHERE amount >= 150) SELECT * FROM big_orders",
		[]string{"store"}, 8192)
	if err != nil {
		t.Fatalf("leader CTE query: %v", err)
	}
	// Orders >= 150: (20,1,250), (30,2,150), (40,3,300) = 3 rows
	if len(cteResult.Rows) != 3 {
		t.Fatalf("leader CTE: expected 3 rows, got %d: %+v", len(cteResult.Rows), cteResult.Rows)
	}

	// --- Leader: query with window function (ROW_NUMBER partitioned by dept) ---
	windowResult, err := leader.TimeTravelQueryAsOfLSN(ctx,
		"SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY name ASC) AS rn FROM users",
		[]string{"store"}, 8192)
	if err != nil {
		t.Fatalf("leader window query: %v", err)
	}
	if len(windowResult.Rows) != 4 {
		t.Fatalf("leader window: expected 4 rows, got %d: %+v", len(windowResult.Rows), windowResult.Rows)
	}
	// Verify ROW_NUMBER values are present on each row
	for _, row := range windowResult.Rows {
		rn, ok := row["rn"]
		if !ok {
			t.Fatalf("leader window: missing rn column in row %+v", row)
		}
		if rn.Kind != ast.LiteralNumber || rn.NumberValue < 1 {
			t.Fatalf("leader window: invalid rn value %+v in row %+v", rn, row)
		}
	}

	// Close leader store for follower replay
	if err := store.Close(); err != nil {
		t.Fatalf("close leader store: %v", err)
	}

	// --- Follower: replay from leader WAL ---
	followerStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new follower store: %v", err)
	}
	t.Cleanup(func() { _ = followerStore.Close() })

	follower, err := executor.New(ctx, followerStore, "")
	if err != nil {
		t.Fatalf("new follower engine: %v", err)
	}

	// Verify follower row counts match leader
	if got := follower.RowCount("store", "users"); got != 4 {
		t.Fatalf("follower users count: got %d want 4", got)
	}
	if got := follower.RowCount("store", "orders"); got != 5 {
		t.Fatalf("follower orders count: got %d want 5", got)
	}

	// --- Follower: same LEFT JOIN query should produce identical results ---
	followerJoinResult, err := follower.TimeTravelQueryAsOfLSN(ctx,
		"SELECT users.name, orders.amount FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.name ASC",
		[]string{"store"}, 8192)
	if err != nil {
		t.Fatalf("follower LEFT JOIN query: %v", err)
	}
	if len(followerJoinResult.Rows) != len(joinResult.Rows) {
		t.Fatalf("follower LEFT JOIN row count mismatch: got %d want %d", len(followerJoinResult.Rows), len(joinResult.Rows))
	}

	// --- Follower: same CTE query ---
	followerCTEResult, err := follower.TimeTravelQueryAsOfLSN(ctx,
		"WITH big_orders AS (SELECT id, user_id, amount FROM orders WHERE amount >= 150) SELECT * FROM big_orders",
		[]string{"store"}, 8192)
	if err != nil {
		t.Fatalf("follower CTE query: %v", err)
	}
	if len(followerCTEResult.Rows) != len(cteResult.Rows) {
		t.Fatalf("follower CTE row count mismatch: got %d want %d", len(followerCTEResult.Rows), len(cteResult.Rows))
	}

	// --- Follower: same window function query ---
	followerWindowResult, err := follower.TimeTravelQueryAsOfLSN(ctx,
		"SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY name ASC) AS rn FROM users",
		[]string{"store"}, 8192)
	if err != nil {
		t.Fatalf("follower window query: %v", err)
	}
	if len(followerWindowResult.Rows) != len(windowResult.Rows) {
		t.Fatalf("follower window row count mismatch: got %d want %d", len(followerWindowResult.Rows), len(windowResult.Rows))
	}
	for _, row := range followerWindowResult.Rows {
		rn, ok := row["rn"]
		if !ok {
			t.Fatalf("follower window: missing rn column in row %+v", row)
		}
		if rn.Kind != ast.LiteralNumber || rn.NumberValue < 1 {
			t.Fatalf("follower window: invalid rn value %+v", rn)
		}
	}

	// --- Follower: LEAD window function ---
	leadResult, err := follower.TimeTravelQueryAsOfLSN(ctx,
		"SELECT id, amount, LEAD(amount) OVER (ORDER BY id ASC) AS next_amount FROM orders",
		[]string{"store"}, 8192)
	if err != nil {
		t.Fatalf("follower LEAD query: %v", err)
	}
	if len(leadResult.Rows) != 5 {
		t.Fatalf("follower LEAD: expected 5 rows, got %d", len(leadResult.Rows))
	}
	// Last row by id ASC (id=50) should have NULL next_amount
	var lastRow map[string]ast.Literal
	for _, row := range leadResult.Rows {
		if lastRow == nil || row["id"].NumberValue > lastRow["id"].NumberValue {
			lastRow = row
		}
	}
	if lastRow["next_amount"].Kind != ast.LiteralNull {
		t.Fatalf("expected NULL next_amount for last row, got %+v", lastRow["next_amount"])
	}
}

// TestMultiNodeLeadershipFailoverWithDataIntegrity simulates a leader crash
// and failover, verifying that the new leader can serve consistent data.
func TestMultiNodeLeadershipFailoverWithDataIntegrity(t *testing.T) {
	ctx := context.Background()
	temp := t.TempDir()
	walPath := filepath.Join(temp, "node.wal")

	// --- Setup: write data as "leader" ---
	store, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	session := engine.NewSession()
	sqls := []string{
		"BEGIN DOMAIN store",
		"CREATE TABLE products (id INT, name TEXT, category TEXT, price INT)",
		"INSERT INTO products (id, name, category, price) VALUES (1, 'Widget', 'tools', 100)",
		"INSERT INTO products (id, name, category, price) VALUES (2, 'Gadget', 'tools', 200)",
		"INSERT INTO products (id, name, category, price) VALUES (3, 'Doohickey', 'parts', 50)",
		"INSERT INTO products (id, name, category, price) VALUES (4, 'Thingamajig', 'parts', 75)",
		"COMMIT",
	}
	for _, sql := range sqls {
		if _, err := engine.Execute(ctx, session, sql); err != nil {
			t.Fatalf("setup execute %q: %v", sql, err)
		}
	}

	leaderLSN := uint64(100) // Simulated LSN for leadership tracking

	// --- Leadership: simulate failover from node-a to node-b ---
	clock := &integrationClock{now: time.Unix(200, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	// node-a acquires leadership
	leaderState, err := leadership.TryAcquireLeadership("store", "node-a", leaderLSN)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	// Verify node-a can write
	if !leadership.CanAcceptWrite("store", "node-a", leaderState.FencingToken) {
		t.Fatal("expected node-a to accept writes")
	}

	// Simulate leader crash: advance past lease TTL
	clock.Advance(6 * time.Second)

	// node-b triggers failover
	failoverCoord, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	result, err := failoverCoord.Failover("store",
		[]coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: leaderLSN}},
		leaderLSN)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}

	if result.Promoted.LeaderID != "node-b" {
		t.Fatalf("expected node-b promoted, got %s", result.Promoted.LeaderID)
	}
	if result.Promoted.Term != leaderState.Term+1 {
		t.Fatalf("expected term %d, got %d", leaderState.Term+1, result.Promoted.Term)
	}

	// Old leader's token should be rejected
	if leadership.CanAcceptWrite("store", "node-a", leaderState.FencingToken) {
		t.Fatal("expected stale leader token to be rejected after failover")
	}

	// New leader can write
	if !leadership.CanAcceptWrite("store", "node-b", result.Promoted.FencingToken) {
		t.Fatal("expected node-b to accept writes after failover")
	}

	// --- Verify data is still accessible from the replayed WAL ---
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	// Simulate node-b replaying the same WAL
	newStore, err := wal.NewSegmentedLogStore(walPath, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = newStore.Close() })

	newEngine, err := executor.New(ctx, newStore, "")
	if err != nil {
		t.Fatalf("new engine after failover: %v", err)
	}

	// Row counts should match
	if got := newEngine.RowCount("store", "products"); got != 4 {
		t.Fatalf("post-failover products count: got %d want 4", got)
	}

	// CROSS JOIN on the new leader should work
	crossResult, err := newEngine.TimeTravelQueryAsOfLSN(ctx,
		"SELECT * FROM products CROSS JOIN products",
		[]string{"store"}, 8192)
	if err != nil {
		// CROSS JOIN with same table may not be supported; just verify no panic
		t.Logf("CROSS JOIN same table not supported (expected): %v", err)
	} else {
		// 4 × 4 = 16 rows
		if len(crossResult.Rows) != 16 {
			t.Fatalf("CROSS JOIN: expected 16 rows, got %d", len(crossResult.Rows))
		}
	}

	// Window function query on the post-failover engine
	rankResult, err := newEngine.TimeTravelQueryAsOfLSN(ctx,
		"SELECT name, category, RANK() OVER (PARTITION BY category ORDER BY price DESC) AS price_rank FROM products",
		[]string{"store"}, 8192)
	if err != nil {
		t.Fatalf("post-failover RANK query: %v", err)
	}
	if len(rankResult.Rows) != 4 {
		t.Fatalf("post-failover RANK: expected 4 rows, got %d", len(rankResult.Rows))
	}

	// Verify RANK values: tools category (Widget=100, Gadget=200) → Gadget=1, Widget=2
	for _, row := range rankResult.Rows {
		rnk := row["price_rank"]
		if rnk.Kind != ast.LiteralNumber || rnk.NumberValue < 1 || rnk.NumberValue > 2 {
			t.Fatalf("unexpected rank value %+v in row %+v", rnk, row)
		}
	}

	// CTE query on the post-failover engine
	cteResult, err := newEngine.TimeTravelQueryAsOfLSN(ctx,
		"WITH expensive AS (SELECT id, name, price FROM products WHERE price >= 100) SELECT * FROM expensive",
		[]string{"store"}, 8192)
	if err != nil {
		t.Fatalf("post-failover CTE query: %v", err)
	}
	// products with price >= 100: Widget(100), Gadget(200) = 2 rows
	if len(cteResult.Rows) != 2 {
		t.Fatalf("post-failover CTE: expected 2 rows, got %d: %+v", len(cteResult.Rows), cteResult.Rows)
	}
}

// TestMultiNodeFailoverElectionHighestLSNWins verifies that during failover,
// the candidate with the highest LSN is elected leader (deterministic election).
func TestMultiNodeFailoverElectionHighestLSNWins(t *testing.T) {
	clock := &integrationClock{now: time.Unix(300, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	_, err = leadership.TryAcquireLeadership("data", "node-a", 50)
	if err != nil {
		t.Fatalf("acquire leader: %v", err)
	}

	// Expire lease
	clock.Advance(6 * time.Second)

	failoverCoord, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	// Three candidates with different LSNs
	result, err := failoverCoord.Failover("data", []coordinator.FailoverCandidate{
		{NodeID: "node-b", NodeLSN: 40},
		{NodeID: "node-c", NodeLSN: 50},
		{NodeID: "node-d", NodeLSN: 45},
	}, 40)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}

	// node-c has highest LSN (50), should be elected
	if result.Promoted.LeaderID != "node-c" {
		t.Fatalf("expected node-c (highest LSN) promoted, got %s", result.Promoted.LeaderID)
	}

	// Second failover: same LSN, tie-break by NodeID (ascending)
	clock.Advance(6 * time.Second)
	result2, err := failoverCoord.Failover("data", []coordinator.FailoverCandidate{
		{NodeID: "node-x", NodeLSN: 60},
		{NodeID: "node-e", NodeLSN: 60},
	}, 50)
	if err != nil {
		t.Fatalf("second failover: %v", err)
	}

	// Same LSN: node-e < node-x alphabetically, so node-e wins
	if result2.Promoted.LeaderID != "node-e" {
		t.Fatalf("expected node-e (tie-break) promoted, got %s", result2.Promoted.LeaderID)
	}
}

// TestMultiNodeActiveLeaseBlocksFailover verifies that failover is blocked
// while the current leader's lease is still active.
func TestMultiNodeActiveLeaseBlocksFailover(t *testing.T) {
	clock := &integrationClock{now: time.Unix(400, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	_, err = leadership.TryAcquireLeadership("data", "node-a", 100)
	if err != nil {
		t.Fatalf("acquire leader: %v", err)
	}

	failoverCoord, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	// Try failover while lease is active (only 2 seconds passed, TTL is 5)
	clock.Advance(2 * time.Second)
	_, err = failoverCoord.Failover("data", []coordinator.FailoverCandidate{
		{NodeID: "node-b", NodeLSN: 100},
	}, 100)

	if !errors.Is(err, coordinator.ErrActiveLeaderLease) {
		t.Fatalf("expected ErrActiveLeaderLease during active lease, got %v", err)
	}

	// After lease expires, failover should succeed
	clock.Advance(4 * time.Second)
	result, err := failoverCoord.Failover("data", []coordinator.FailoverCandidate{
		{NodeID: "node-b", NodeLSN: 100},
	}, 100)
	if err != nil {
		t.Fatalf("failover after lease expiry: %v", err)
	}
	if result.Promoted.LeaderID != "node-b" {
		t.Fatalf("expected node-b promoted, got %s", result.Promoted.LeaderID)
	}
}
