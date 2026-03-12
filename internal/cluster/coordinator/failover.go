package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	// ErrLeadershipManagerRequired is returned when failover coordinator is initialized without leadership manager.
	ErrLeadershipManagerRequired = errors.New("leadership manager is required")
	// ErrLeadershipStateNotFound is returned when failover is requested for unknown group.
	ErrLeadershipStateNotFound = errors.New("leadership state not found")
	// ErrNoEligibleFailoverCandidate is returned when no candidate satisfies failover preconditions.
	ErrNoEligibleFailoverCandidate = errors.New("no eligible failover candidate")
)

// FailoverPhase identifies serialized failover transition phases.
type FailoverPhase string

const (
	// FailoverPhaseLeaderDown marks detection of a down/expired leader.
	FailoverPhaseLeaderDown FailoverPhase = "leader_down"
	// FailoverPhaseCandidateElected marks deterministic candidate selection.
	FailoverPhaseCandidateElected FailoverPhase = "candidate_elected"
	// FailoverPhasePromotedLeader marks successful leader promotion.
	FailoverPhasePromotedLeader FailoverPhase = "promoted_leader"
)

// FailoverCandidate captures candidate readiness for deterministic election.
type FailoverCandidate struct {
	NodeID  string
	NodeLSN uint64
}

// FailoverTransition captures a single serialized failover transition.
type FailoverTransition struct {
	Phase  FailoverPhase
	Group  string
	Term   uint64
	NodeID string
}

// FailoverEventObserver receives deterministic failover transition events.
type FailoverEventObserver interface {
	OnFailoverTransition(FailoverTransition)
}

// FailoverResult contains deterministic failover selection and promotion outputs.
type FailoverResult struct {
	Previous    GroupLeadershipState
	Elected     FailoverCandidate
	Promoted    GroupLeadershipState
	Transitions []FailoverTransition
}

// FailoverCoordinator executes deterministic serialized failover transitions.
type FailoverCoordinator struct {
	mu         sync.Mutex
	leadership *LeadershipManager
	observer   FailoverEventObserver
}

// NewFailoverCoordinator creates a failover coordinator for leadership transitions.
func NewFailoverCoordinator(leadership *LeadershipManager) (*FailoverCoordinator, error) {
	return NewFailoverCoordinatorWithObserver(leadership, nil)
}

// NewFailoverCoordinatorWithObserver creates a failover coordinator with optional event observer.
func NewFailoverCoordinatorWithObserver(leadership *LeadershipManager, observer FailoverEventObserver) (*FailoverCoordinator, error) {
	if leadership == nil {
		return nil, ErrLeadershipManagerRequired
	}

	return &FailoverCoordinator{leadership: leadership, observer: observer}, nil
}

// Failover performs deterministic failover for one group with serialized transitions.
//
// Flow:
// 1) Verify leader lease is down/expired for the target group.
// 2) Elect deterministic candidate (highest LSN, tie-break by NodeID asc).
// 3) Promote elected candidate through LeadershipManager term/fencing rules.
func (coordinator *FailoverCoordinator) Failover(group string, candidates []FailoverCandidate, requiredLSN uint64) (FailoverResult, error) {
	if group == "" {
		return FailoverResult{}, ErrGroupRequired
	}

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()

	current, exists, leaseActive := coordinator.leadership.SnapshotWithLeaseStatus(group)
	if !exists {
		return FailoverResult{}, fmt.Errorf("%w: group=%s", ErrLeadershipStateNotFound, group)
	}
	if leaseActive {
		return FailoverResult{}, fmt.Errorf("%w: group=%s leader=%s", ErrActiveLeaderLease, group, current.LeaderID)
	}

	transitions := []FailoverTransition{{
		Phase:  FailoverPhaseLeaderDown,
		Group:  group,
		Term:   current.Term,
		NodeID: current.LeaderID,
	}}
	coordinator.notifyTransition(transitions[0])

	minRequiredLSN := requiredLSN
	if current.LastLeaderLSN > minRequiredLSN {
		minRequiredLSN = current.LastLeaderLSN
	}

	eligible := make([]FailoverCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.NodeID == "" {
			continue
		}
		if candidate.NodeLSN < minRequiredLSN {
			continue
		}
		eligible = append(eligible, candidate)
	}

	if len(eligible) == 0 {
		return FailoverResult{}, fmt.Errorf("%w: group=%s required_lsn=%d", ErrNoEligibleFailoverCandidate, group, minRequiredLSN)
	}

	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].NodeLSN == eligible[j].NodeLSN {
			return eligible[i].NodeID < eligible[j].NodeID
		}
		return eligible[i].NodeLSN > eligible[j].NodeLSN
	})

	elected := eligible[0]
	transitions = append(transitions, FailoverTransition{
		Phase:  FailoverPhaseCandidateElected,
		Group:  group,
		Term:   current.Term,
		NodeID: elected.NodeID,
	})
	coordinator.notifyTransition(transitions[len(transitions)-1])

	promoted, err := coordinator.leadership.TryPromoteCandidate(group, elected.NodeID, current.Term, elected.NodeLSN, minRequiredLSN)
	if err != nil {
		return FailoverResult{}, err
	}

	transitions = append(transitions, FailoverTransition{
		Phase:  FailoverPhasePromotedLeader,
		Group:  group,
		Term:   promoted.Term,
		NodeID: promoted.LeaderID,
	})
	coordinator.notifyTransition(transitions[len(transitions)-1])

	return FailoverResult{
		Previous:    current,
		Elected:     elected,
		Promoted:    promoted,
		Transitions: transitions,
	}, nil
}

func (coordinator *FailoverCoordinator) notifyTransition(transition FailoverTransition) {
	if coordinator == nil || coordinator.observer == nil {
		return
	}

	coordinator.observer.OnFailoverTransition(transition)
}
