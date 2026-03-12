package coordinator

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type transitionRecorder struct {
	mu          sync.Mutex
	transitions []FailoverTransition
}

func (recorder *transitionRecorder) OnFailoverTransition(transition FailoverTransition) {
	recorder.mu.Lock()
	recorder.transitions = append(recorder.transitions, transition)
	recorder.mu.Unlock()
}

func (recorder *transitionRecorder) Snapshot() []FailoverTransition {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	out := make([]FailoverTransition, len(recorder.transitions))
	copy(out, recorder.transitions)
	return out
}

func TestNewFailoverCoordinatorRequiresLeadershipManager(t *testing.T) {
	_, err := NewFailoverCoordinator(nil)
	if !errors.Is(err, ErrLeadershipManagerRequired) {
		t.Fatalf("expected ErrLeadershipManagerRequired, got %v", err)
	}
}

func TestFailoverPromotesDeterministicCandidateWithSerializedTransitions(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0).UTC()}
	leadership, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	seed, err := leadership.TryAcquireLeadership("orders", "node-a", 100)
	if err != nil {
		t.Fatalf("acquire initial leader: %v", err)
	}

	clock.Advance(6 * time.Second)
	coordinator, err := NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	result, err := coordinator.Failover("orders", []FailoverCandidate{
		{NodeID: "node-c", NodeLSN: 100},
		{NodeID: "node-b", NodeLSN: 100},
		{NodeID: "node-d", NodeLSN: 99},
	}, 100)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}

	if result.Previous.Term != seed.Term {
		t.Fatalf("unexpected previous term: got=%d want=%d", result.Previous.Term, seed.Term)
	}
	if result.Elected.NodeID != "node-b" {
		t.Fatalf("expected lexicographically first tied candidate node-b, got %s", result.Elected.NodeID)
	}
	if result.Promoted.LeaderID != "node-b" {
		t.Fatalf("expected promoted leader node-b, got %s", result.Promoted.LeaderID)
	}
	if result.Promoted.Term != seed.Term+1 {
		t.Fatalf("expected term increment to %d, got %d", seed.Term+1, result.Promoted.Term)
	}
	if len(result.Transitions) != 3 {
		t.Fatalf("expected 3 transitions, got %d", len(result.Transitions))
	}
	if result.Transitions[0].Phase != FailoverPhaseLeaderDown {
		t.Fatalf("unexpected first phase: %s", result.Transitions[0].Phase)
	}
	if result.Transitions[1].Phase != FailoverPhaseCandidateElected {
		t.Fatalf("unexpected second phase: %s", result.Transitions[1].Phase)
	}
	if result.Transitions[2].Phase != FailoverPhasePromotedLeader {
		t.Fatalf("unexpected third phase: %s", result.Transitions[2].Phase)
	}
}

func TestFailoverRejectsWhenLeaderLeaseStillActive(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0).UTC()}
	leadership, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	_, err = leadership.TryAcquireLeadership("billing", "node-a", 50)
	if err != nil {
		t.Fatalf("acquire initial leader: %v", err)
	}

	coordinator, err := NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	_, err = coordinator.Failover("billing", []FailoverCandidate{{NodeID: "node-b", NodeLSN: 50}}, 50)
	if !errors.Is(err, ErrActiveLeaderLease) {
		t.Fatalf("expected ErrActiveLeaderLease, got %v", err)
	}
}

func TestFailoverIsSerializedUnderConcurrentAttempts(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0).UTC()}
	leadership, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	_, err = leadership.TryAcquireLeadership("payments", "node-a", 10)
	if err != nil {
		t.Fatalf("acquire initial leader: %v", err)
	}

	clock.Advance(6 * time.Second)
	coordinator, err := NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	type outcome struct {
		result FailoverResult
		err    error
	}

	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup

	runAttempt := func(candidates []FailoverCandidate) {
		defer wg.Done()
		<-start
		result, runErr := coordinator.Failover("payments", candidates, 10)
		outcomes <- outcome{result: result, err: runErr}
	}

	wg.Add(2)
	go runAttempt([]FailoverCandidate{{NodeID: "node-b", NodeLSN: 10}})
	go runAttempt([]FailoverCandidate{{NodeID: "node-c", NodeLSN: 10}})
	close(start)
	wg.Wait()
	close(outcomes)

	success := 0
	fail := 0
	for outcome := range outcomes {
		if outcome.err == nil {
			success++
			if outcome.result.Promoted.LeaderID == "" {
				t.Fatal("expected promoted leader id in successful result")
			}
			continue
		}

		if errors.Is(outcome.err, ErrActiveLeaderLease) {
			fail++
			continue
		}
		t.Fatalf("unexpected failover error: %v", outcome.err)
	}

	if success != 1 || fail != 1 {
		t.Fatalf("expected one success and one ErrActiveLeaderLease, got success=%d fail=%d", success, fail)
	}
}

func TestFailoverEmitsObservableTransitionsInOrder(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0).UTC()}
	leadership, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	_, err = leadership.TryAcquireLeadership("inventory", "node-a", 30)
	if err != nil {
		t.Fatalf("acquire initial leader: %v", err)
	}

	clock.Advance(6 * time.Second)
	recorder := &transitionRecorder{}
	coordinator, err := NewFailoverCoordinatorWithObserver(leadership, recorder)
	if err != nil {
		t.Fatalf("new failover coordinator with observer: %v", err)
	}

	_, err = coordinator.Failover("inventory", []FailoverCandidate{{NodeID: "node-b", NodeLSN: 30}}, 30)
	if err != nil {
		t.Fatalf("failover: %v", err)
	}

	transitions := recorder.Snapshot()
	if len(transitions) != 3 {
		t.Fatalf("expected 3 observed transitions, got %d", len(transitions))
	}
	if transitions[0].Phase != FailoverPhaseLeaderDown {
		t.Fatalf("unexpected first observed phase: %s", transitions[0].Phase)
	}
	if transitions[1].Phase != FailoverPhaseCandidateElected {
		t.Fatalf("unexpected second observed phase: %s", transitions[1].Phase)
	}
	if transitions[2].Phase != FailoverPhasePromotedLeader {
		t.Fatalf("unexpected third observed phase: %s", transitions[2].Phase)
	}
}
