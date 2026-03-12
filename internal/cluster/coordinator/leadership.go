package coordinator

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"asql/internal/engine/ports"
)

var (
	// ErrClockRequired is returned when leadership manager is initialized without clock.
	ErrClockRequired = errors.New("clock is required")
	// ErrLeaseTTLInvalid is returned when lease ttl is non-positive.
	ErrLeaseTTLInvalid = errors.New("lease ttl must be positive")
	// ErrGroupRequired is returned when group is empty.
	ErrGroupRequired = errors.New("group is required")
	// ErrNodeRequired is returned when node id is empty.
	ErrNodeRequired = errors.New("node id is required")
	// ErrActiveLeaderLease is returned when another leader still holds a non-expired lease.
	ErrActiveLeaderLease = errors.New("active leader lease exists")
	// ErrInvalidFencingToken is returned when provided fencing token does not match active token.
	ErrInvalidFencingToken = errors.New("invalid fencing token")
	// ErrTermMismatch is returned when expected term does not match current term.
	ErrTermMismatch = errors.New("term mismatch")
	// ErrPromotionLSNBehind is returned when promotion candidate is behind required lsn.
	ErrPromotionLSNBehind = errors.New("candidate lsn is behind required lsn")
)

// GroupLeadershipState captures deterministic leadership state for a domain-group.
//
// Invariants:
// - Term is monotonic and only increases when leadership changes.
// - At most one active leader lease exists for a group at any time.
// - FencingToken uniquely identifies the active writable lease for the current term.
// - LastLeaderLSN is monotonic per accepted leader updates.
type GroupLeadershipState struct {
	Group          string
	Term           uint64
	LeaderID       string
	FencingToken   string
	LeaseExpiresAt time.Time
	LastLeaderLSN  uint64
}

// LeadershipManager maintains deterministic leader lease state across domain-groups.
type LeadershipManager struct {
	mu       sync.Mutex
	clock    ports.Clock
	leaseTTL time.Duration
	groups   map[string]GroupLeadershipState
}

// NewLeadershipManager creates a deterministic leadership manager.
func NewLeadershipManager(clock ports.Clock, leaseTTL time.Duration) (*LeadershipManager, error) {
	if clock == nil {
		return nil, ErrClockRequired
	}
	if leaseTTL <= 0 {
		return nil, ErrLeaseTTLInvalid
	}

	return &LeadershipManager{
		clock:    clock,
		leaseTTL: leaseTTL,
		groups:   map[string]GroupLeadershipState{},
	}, nil
}

// TryAcquireLeadership grants leadership lease for group to node when invariants allow it.
func (manager *LeadershipManager) TryAcquireLeadership(group, nodeID string, nodeLSN uint64) (GroupLeadershipState, error) {
	if group == "" {
		return GroupLeadershipState{}, ErrGroupRequired
	}
	if nodeID == "" {
		return GroupLeadershipState{}, ErrNodeRequired
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	now := manager.clock.Now()
	current, exists := manager.groups[group]
	if exists && current.LeaderID != "" && now.Before(current.LeaseExpiresAt) && current.LeaderID != nodeID {
		return GroupLeadershipState{}, fmt.Errorf("%w: group=%s leader=%s lease_expires_at=%s", ErrActiveLeaderLease, group, current.LeaderID, current.LeaseExpiresAt.UTC().Format(time.RFC3339Nano))
	}

	term := uint64(1)
	if exists {
		term = current.Term
		if current.LeaderID != nodeID || !now.Before(current.LeaseExpiresAt) {
			term = current.Term + 1
		}
	}

	if exists && current.LastLeaderLSN > nodeLSN {
		nodeLSN = current.LastLeaderLSN
	}

	next := GroupLeadershipState{
		Group:          group,
		Term:           term,
		LeaderID:       nodeID,
		FencingToken:   fencingToken(group, term, nodeID),
		LeaseExpiresAt: now.Add(manager.leaseTTL),
		LastLeaderLSN:  nodeLSN,
	}
	manager.groups[group] = next
	return next, nil
}

// RenewLeadership updates the active leader lease and optional lsn watermark.
func (manager *LeadershipManager) RenewLeadership(group, nodeID, fencing string, leaderLSN uint64) (GroupLeadershipState, error) {
	if group == "" {
		return GroupLeadershipState{}, ErrGroupRequired
	}
	if nodeID == "" {
		return GroupLeadershipState{}, ErrNodeRequired
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.groups[group]
	if !exists || current.LeaderID == "" {
		return GroupLeadershipState{}, fmt.Errorf("%w: no active leader for group=%s", ErrInvalidFencingToken, group)
	}
	if current.LeaderID != nodeID || current.FencingToken != fencing {
		return GroupLeadershipState{}, fmt.Errorf("%w: group=%s node=%s", ErrInvalidFencingToken, group, nodeID)
	}

	now := manager.clock.Now()
	if leaderLSN < current.LastLeaderLSN {
		leaderLSN = current.LastLeaderLSN
	}

	current.LeaseExpiresAt = now.Add(manager.leaseTTL)
	current.LastLeaderLSN = leaderLSN
	manager.groups[group] = current
	return current, nil
}

// TryPromoteCandidate attempts follower promotion when term and lsn preconditions hold.
func (manager *LeadershipManager) TryPromoteCandidate(group, nodeID string, expectedTerm, candidateLSN, requiredLSN uint64) (GroupLeadershipState, error) {
	if group == "" {
		return GroupLeadershipState{}, ErrGroupRequired
	}
	if nodeID == "" {
		return GroupLeadershipState{}, ErrNodeRequired
	}
	if candidateLSN < requiredLSN {
		return GroupLeadershipState{}, fmt.Errorf("%w: candidate_lsn=%d required_lsn=%d", ErrPromotionLSNBehind, candidateLSN, requiredLSN)
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	now := manager.clock.Now()
	current := manager.groups[group]
	if current.Term != expectedTerm {
		return GroupLeadershipState{}, fmt.Errorf("%w: expected=%d got=%d", ErrTermMismatch, expectedTerm, current.Term)
	}
	if current.LeaderID != "" && now.Before(current.LeaseExpiresAt) && current.LeaderID != nodeID {
		return GroupLeadershipState{}, fmt.Errorf("%w: group=%s leader=%s lease_expires_at=%s", ErrActiveLeaderLease, group, current.LeaderID, current.LeaseExpiresAt.UTC().Format(time.RFC3339Nano))
	}

	nextTerm := current.Term + 1
	next := GroupLeadershipState{
		Group:          group,
		Term:           nextTerm,
		LeaderID:       nodeID,
		FencingToken:   fencingToken(group, nextTerm, nodeID),
		LeaseExpiresAt: now.Add(manager.leaseTTL),
		LastLeaderLSN:  candidateLSN,
	}
	manager.groups[group] = next
	return next, nil
}

// CanAcceptWrite returns true when node and fencing token match active non-expired lease.
func (manager *LeadershipManager) CanAcceptWrite(group, nodeID, fencing string) bool {
	if group == "" || nodeID == "" || fencing == "" {
		return false
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.groups[group]
	if !exists {
		return false
	}
	now := manager.clock.Now()
	if !now.Before(current.LeaseExpiresAt) {
		return false
	}
	return current.LeaderID == nodeID && current.FencingToken == fencing
}

// Snapshot returns current leadership state for group, if present.
func (manager *LeadershipManager) Snapshot(group string) (GroupLeadershipState, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	state, exists := manager.groups[group]
	return state, exists
}

// SnapshotWithLeaseStatus returns state and whether the current lease is active.
func (manager *LeadershipManager) SnapshotWithLeaseStatus(group string) (GroupLeadershipState, bool, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	state, exists := manager.groups[group]
	if !exists {
		return GroupLeadershipState{}, false, false
	}

	now := manager.clock.Now()
	leaseActive := now.Before(state.LeaseExpiresAt)
	return state, true, leaseActive
}

// Groups returns a sorted list of all group names with registered leadership state.
func (manager *LeadershipManager) Groups() []string {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	groups := make([]string, 0, len(manager.groups))
	for g := range manager.groups {
		groups = append(groups, g)
	}
	sort.Strings(groups)
	return groups
}

// SyncExternalLeaderState records a remote leader's state locally so that
// FailoverCoordinator can track lease expiry via SnapshotWithLeaseStatus.
// Term monotonicity is preserved: a state with a lower term than the
// already-registered one is silently ignored.
func (manager *LeadershipManager) SyncExternalLeaderState(state GroupLeadershipState) {
	if state.Group == "" {
		return
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.groups[state.Group]
	if exists && current.Term > state.Term {
		return
	}

	manager.groups[state.Group] = state
}

// ForceExpireLease clears the lease expiry for a group so that the next
// FailoverCoordinator.Failover call is not blocked by a stale active-lease
// guard. This is called after enough consecutive probe failures to ensure
// failover proceeds even when discoverLeader has refreshed the lease from
// stale peer gossip.
func (manager *LeadershipManager) ForceExpireLease(group string) {
	if group == "" {
		return
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	state, exists := manager.groups[group]
	if !exists {
		return
	}
	state.LeaseExpiresAt = time.Time{} // zero value — always in the past
	manager.groups[group] = state
}

func fencingToken(group string, term uint64, nodeID string) string {
	return fmt.Sprintf("%s:%d:%s", group, term, nodeID)
}
