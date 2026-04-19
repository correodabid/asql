package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/cluster/coordinator"
	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/wal"
)

func TestFailoverPromotionPreservesReplayStateHashContinuity(t *testing.T) {
	ctx := context.Background()

	failoverHash, failoverLSN := runFailoverScenarioAndStateHash(t, ctx)
	baselineHash, baselineLSN := runBaselineScenarioAndStateHash(t, ctx)

	if failoverLSN != baselineLSN {
		t.Fatalf("unexpected WAL continuity mismatch: failover_lsn=%d baseline_lsn=%d", failoverLSN, baselineLSN)
	}
	if failoverHash != baselineHash {
		t.Fatalf("state hash mismatch across promotion: failover=%s baseline=%s", failoverHash, baselineHash)
	}
}

func runFailoverScenarioAndStateHash(t *testing.T, ctx context.Context) (string, uint64) {
	t.Helper()

	clock := &integrationClock{now: time.Unix(200, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}
	state, err := leadership.TryAcquireLeadership("orders", "node-a", 0)
	if err != nil {
		t.Fatalf("acquire leader node-a: %v", err)
	}

	path := filepath.Join(t.TempDir(), "failover-continuity.wal")
	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new failover wal store: %v", err)
	}
	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new failover engine: %v", err)
	}

	execTransaction(t, ctx, engine,
		"BEGIN DOMAIN orders",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	)

	if _, err := leadership.RenewLeadership("orders", "node-a", state.FencingToken, store.LastLSN()); err != nil {
		t.Fatalf("renew leader node-a: %v", err)
	}

	clock.Advance(6 * time.Second)
	flow, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}
	promoted, err := flow.Failover("orders", []coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: store.LastLSN()}}, store.LastLSN())
	if err != nil {
		t.Fatalf("failover promote node-b: %v", err)
	}
	if promoted.Promoted.LeaderID != "node-b" {
		t.Fatalf("expected node-b leader after promotion, got %s", promoted.Promoted.LeaderID)
	}

	execTransaction(t, ctx, engine,
		"BEGIN DOMAIN orders",
		"INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')",
		"COMMIT",
	)

	if err := store.Close(); err != nil {
		t.Fatalf("close failover wal store: %v", err)
	}

	replayStore, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen failover wal store: %v", err)
	}
	t.Cleanup(func() { _ = replayStore.Close() })

	replayed, err := executor.New(ctx, replayStore, "")
	if err != nil {
		t.Fatalf("new replayed failover engine: %v", err)
	}

	hash := deterministicUsersStateHash(t, ctx, replayed, replayStore.LastLSN())
	return hash, replayStore.LastLSN()
}

func runBaselineScenarioAndStateHash(t *testing.T, ctx context.Context) (string, uint64) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "baseline-continuity.wal")
	store, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("new baseline wal store: %v", err)
	}
	engine, err := executor.New(ctx, store, "")
	if err != nil {
		t.Fatalf("new baseline engine: %v", err)
	}

	execTransaction(t, ctx, engine,
		"BEGIN DOMAIN orders",
		"CREATE TABLE users (id INT, email TEXT)",
		"INSERT INTO users (id, email) VALUES (1, 'one@asql.dev')",
		"COMMIT",
	)
	execTransaction(t, ctx, engine,
		"BEGIN DOMAIN orders",
		"INSERT INTO users (id, email) VALUES (2, 'two@asql.dev')",
		"COMMIT",
	)

	if err := store.Close(); err != nil {
		t.Fatalf("close baseline wal store: %v", err)
	}

	replayStore, err := wal.NewSegmentedLogStore(path, wal.AlwaysSync{})
	if err != nil {
		t.Fatalf("reopen baseline wal store: %v", err)
	}
	t.Cleanup(func() { _ = replayStore.Close() })

	replayed, err := executor.New(ctx, replayStore, "")
	if err != nil {
		t.Fatalf("new replayed baseline engine: %v", err)
	}

	hash := deterministicUsersStateHash(t, ctx, replayed, replayStore.LastLSN())
	return hash, replayStore.LastLSN()
}

func execTransaction(t *testing.T, ctx context.Context, engine *executor.Engine, statements ...string) {
	t.Helper()

	session := engine.NewSession()
	for _, statement := range statements {
		if _, err := engine.Execute(ctx, session, statement); err != nil {
			t.Fatalf("execute %q: %v", statement, err)
		}
	}
}

func deterministicUsersStateHash(t *testing.T, ctx context.Context, engine *executor.Engine, lsn uint64) string {
	t.Helper()

	result, err := engine.TimeTravelQueryAsOfLSN(ctx, "SELECT id, email FROM users ORDER BY id ASC", []string{"orders"}, lsn)
	if err != nil {
		t.Fatalf("time travel query for state hash: %v", err)
	}

	rows := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, deterministicRowString(row))
	}
	sort.Strings(rows)

	sum := sha256.Sum256([]byte(strings.Join(rows, "\n")))
	return hex.EncodeToString(sum[:])
}

func deterministicRowString(row map[string]ast.Literal) string {
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		value := row[key]
		parts = append(parts, fmt.Sprintf("%s=%s", key, literalToHashString(value)))
	}

	return strings.Join(parts, "|")
}

func literalToHashString(value ast.Literal) string {
	switch value.Kind {
	case ast.LiteralString:
		return "s:" + value.StringValue
	case ast.LiteralNumber:
		return fmt.Sprintf("n:%d", value.NumberValue)
	case ast.LiteralNull:
		return "null"
	default:
		return "unknown"
	}
}
