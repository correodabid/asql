package integration

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/cluster/coordinator"
)

type integrationClock struct {
	now time.Time
}

func (clock *integrationClock) Now() time.Time {
	return clock.now
}

func (clock *integrationClock) Advance(delta time.Duration) {
	clock.now = clock.now.Add(delta)
}

func TestFailoverSimulationLeaderCrashPromotesCandidate(t *testing.T) {
	clock := &integrationClock{now: time.Unix(100, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	initial, err := leadership.TryAcquireLeadership("orders", "node-a", 100)
	if err != nil {
		t.Fatalf("acquire initial leader: %v", err)
	}

	clock.Advance(6 * time.Second)
	failover, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	result, err := failover.Failover("orders", []coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: 100}}, 100)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}

	if result.Promoted.LeaderID != "node-b" {
		t.Fatalf("expected node-b promoted, got %s", result.Promoted.LeaderID)
	}
	if result.Promoted.Term != initial.Term+1 {
		t.Fatalf("expected term increment from %d to %d, got %d", initial.Term, initial.Term+1, result.Promoted.Term)
	}
}

func TestFailoverSimulationDelayedHeartbeatBlocksThenAllowsFailover(t *testing.T) {
	clock := &integrationClock{now: time.Unix(100, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	state, err := leadership.TryAcquireLeadership("payments", "node-a", 50)
	if err != nil {
		t.Fatalf("acquire initial leader: %v", err)
	}

	clock.Advance(4 * time.Second)
	state, err = leadership.RenewLeadership("payments", "node-a", state.FencingToken, 55)
	if err != nil {
		t.Fatalf("renew leader heartbeat: %v", err)
	}

	coordinatorFlow, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	_, err = coordinatorFlow.Failover("payments", []coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: 55}}, 55)
	if !errors.Is(err, coordinator.ErrActiveLeaderLease) {
		t.Fatalf("expected ErrActiveLeaderLease while heartbeat lease is active, got %v", err)
	}

	clock.Advance(6 * time.Second)
	result, err := coordinatorFlow.Failover("payments", []coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: 55}}, 55)
	if err != nil {
		t.Fatalf("failover after lease expiry: %v", err)
	}
	if result.Promoted.LeaderID != "node-b" {
		t.Fatalf("expected node-b promoted after delayed heartbeat expiry, got %s", result.Promoted.LeaderID)
	}
}

func TestFailoverSimulationDualCandidateContentionSingleWinner(t *testing.T) {
	clock := &integrationClock{now: time.Unix(100, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	_, err = leadership.TryAcquireLeadership("ledger", "node-a", 200)
	if err != nil {
		t.Fatalf("acquire initial leader: %v", err)
	}
	clock.Advance(6 * time.Second)

	coordinatorFlow, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	type outcome struct {
		err error
	}

	start := make(chan struct{})
	results := make(chan outcome, 2)
	var wg sync.WaitGroup

	run := func(node string) {
		defer wg.Done()
		<-start
		_, runErr := coordinatorFlow.Failover("ledger", []coordinator.FailoverCandidate{{NodeID: node, NodeLSN: 200}}, 200)
		results <- outcome{err: runErr}
	}

	wg.Add(2)
	go run("node-b")
	go run("node-c")
	close(start)
	wg.Wait()
	close(results)

	success := 0
	rejected := 0
	for result := range results {
		if result.err == nil {
			success++
			continue
		}
		if errors.Is(result.err, coordinator.ErrActiveLeaderLease) {
			rejected++
			continue
		}
		t.Fatalf("unexpected contention error: %v", result.err)
	}

	if success != 1 || rejected != 1 {
		t.Fatalf("expected exactly one success and one rejected contender, got success=%d rejected=%d", success, rejected)
	}
}

func TestFailoverSimulationStaleLeaderRecoveryRejectsOldToken(t *testing.T) {
	clock := &integrationClock{now: time.Unix(100, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	oldState, err := leadership.TryAcquireLeadership("accounts", "node-a", 90)
	if err != nil {
		t.Fatalf("acquire initial leader: %v", err)
	}

	clock.Advance(6 * time.Second)
	coordinatorFlow, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	result, err := coordinatorFlow.Failover("accounts", []coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: 90}}, 90)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}

	if leadership.CanAcceptWrite("accounts", "node-a", oldState.FencingToken) {
		t.Fatal("expected stale leader token rejection after promotion")
	}
	if !leadership.CanAcceptWrite("accounts", "node-b", result.Promoted.FencingToken) {
		t.Fatal("expected promoted leader token to be writable")
	}
}

func TestFailoverSimulationRepeatedSeededTimelineProducesIdenticalSequence(t *testing.T) {
	const rounds = 12
	baseline := runSeededFailoverTimeline(t)
	for iteration := 1; iteration < rounds; iteration++ {
		current := runSeededFailoverTimeline(t)
		if len(current) != len(baseline) {
			t.Fatalf("sequence length mismatch at iteration %d: got=%d want=%d", iteration, len(current), len(baseline))
		}
		for i := range baseline {
			if current[i] != baseline[i] {
				t.Fatalf("sequence mismatch at iteration %d step %d: got=%s want=%s", iteration, i, current[i], baseline[i])
			}
		}
	}
}

func runSeededFailoverTimeline(t *testing.T) []string {
	t.Helper()

	clock := &integrationClock{now: time.Unix(500, 0).UTC()}
	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	state, err := leadership.TryAcquireLeadership("timeline", "node-a", 100)
	if err != nil {
		t.Fatalf("acquire initial leader: %v", err)
	}

	clock.Advance(2 * time.Second)
	state, err = leadership.RenewLeadership("timeline", "node-a", state.FencingToken, 120)
	if err != nil {
		t.Fatalf("renew initial leader: %v", err)
	}

	flow, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	clock.Advance(6 * time.Second)
	first, err := flow.Failover("timeline", []coordinator.FailoverCandidate{{NodeID: "node-b", NodeLSN: 120}, {NodeID: "node-c", NodeLSN: 120}}, 120)
	if err != nil {
		t.Fatalf("first failover: %v", err)
	}

	clock.Advance(1 * time.Second)
	updated, err := leadership.RenewLeadership("timeline", first.Promoted.LeaderID, first.Promoted.FencingToken, 140)
	if err != nil {
		t.Fatalf("renew promoted leader: %v", err)
	}

	clock.Advance(6 * time.Second)
	second, err := flow.Failover("timeline", []coordinator.FailoverCandidate{{NodeID: "node-c", NodeLSN: 140}, {NodeID: "node-d", NodeLSN: 140}}, 140)
	if err != nil {
		t.Fatalf("second failover: %v", err)
	}

	sequence := []string{
		fmt.Sprintf("%s:%d", first.Promoted.LeaderID, first.Promoted.Term),
		fmt.Sprintf("%s:%d", second.Promoted.LeaderID, second.Promoted.Term),
		fmt.Sprintf("tail_lsn:%d", updated.LastLeaderLSN),
	}

	return sequence
}
