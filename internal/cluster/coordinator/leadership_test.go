package coordinator

import (
	"errors"
	"testing"
	"time"
)

type manualClock struct {
	now time.Time
}

func (clock *manualClock) Now() time.Time {
	return clock.now
}

func (clock *manualClock) Advance(delta time.Duration) {
	clock.now = clock.now.Add(delta)
}

func TestNewLeadershipManagerValidatesInputs(t *testing.T) {
	_, err := NewLeadershipManager(nil, time.Second)
	if !errors.Is(err, ErrClockRequired) {
		t.Fatalf("expected ErrClockRequired, got %v", err)
	}

	clock := &manualClock{now: time.Unix(0, 0)}
	_, err = NewLeadershipManager(clock, 0)
	if !errors.Is(err, ErrLeaseTTLInvalid) {
		t.Fatalf("expected ErrLeaseTTLInvalid, got %v", err)
	}
}

func TestTryAcquireLeadershipEnforcesSingleActiveLeader(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	state, err := manager.TryAcquireLeadership("payments", "node-a", 10)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}
	if state.Term != 1 {
		t.Fatalf("expected term=1, got %d", state.Term)
	}
	if state.FencingToken == "" {
		t.Fatal("expected non-empty fencing token")
	}

	_, err = manager.TryAcquireLeadership("payments", "node-b", 11)
	if !errors.Is(err, ErrActiveLeaderLease) {
		t.Fatalf("expected ErrActiveLeaderLease, got %v", err)
	}
}

func TestTryAcquireLeadershipAfterLeaseExpiryPromotesNewLeaderAndTerm(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	first, err := manager.TryAcquireLeadership("inventory", "node-a", 5)
	if err != nil {
		t.Fatalf("acquire first leader: %v", err)
	}

	clock.Advance(6 * time.Second)
	second, err := manager.TryAcquireLeadership("inventory", "node-b", 7)
	if err != nil {
		t.Fatalf("acquire second leader: %v", err)
	}
	if second.Term != first.Term+1 {
		t.Fatalf("expected term increment, first=%d second=%d", first.Term, second.Term)
	}
	if second.LeaderID != "node-b" {
		t.Fatalf("expected node-b as leader, got %s", second.LeaderID)
	}
}

func TestRenewLeadershipRequiresMatchingFencingToken(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	state, err := manager.TryAcquireLeadership("accounts", "node-a", 20)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	_, err = manager.RenewLeadership("accounts", "node-a", "bad-token", 21)
	if !errors.Is(err, ErrInvalidFencingToken) {
		t.Fatalf("expected ErrInvalidFencingToken, got %v", err)
	}

	clock.Advance(2 * time.Second)
	renewed, err := manager.RenewLeadership("accounts", "node-a", state.FencingToken, 21)
	if err != nil {
		t.Fatalf("renew leadership: %v", err)
	}
	if renewed.LastLeaderLSN != 21 {
		t.Fatalf("expected LastLeaderLSN=21, got %d", renewed.LastLeaderLSN)
	}
}

func TestTryPromoteCandidateValidatesTermAndLSN(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	leader, err := manager.TryAcquireLeadership("ledger", "node-a", 100)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	clock.Advance(6 * time.Second)
	_, err = manager.TryPromoteCandidate("ledger", "node-b", leader.Term+1, 100, 100)
	if !errors.Is(err, ErrTermMismatch) {
		t.Fatalf("expected ErrTermMismatch, got %v", err)
	}

	_, err = manager.TryPromoteCandidate("ledger", "node-b", leader.Term, 99, 100)
	if !errors.Is(err, ErrPromotionLSNBehind) {
		t.Fatalf("expected ErrPromotionLSNBehind, got %v", err)
	}

	promoted, err := manager.TryPromoteCandidate("ledger", "node-b", leader.Term, 100, 100)
	if err != nil {
		t.Fatalf("promote candidate: %v", err)
	}
	if promoted.Term != leader.Term+1 {
		t.Fatalf("expected promoted term=%d, got %d", leader.Term+1, promoted.Term)
	}
	if promoted.LeaderID != "node-b" {
		t.Fatalf("expected node-b leader, got %s", promoted.LeaderID)
	}
}

func TestCanAcceptWriteRequiresLiveLeaseAndValidToken(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	state, err := manager.TryAcquireLeadership("orders", "node-a", 1)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	if !manager.CanAcceptWrite("orders", "node-a", state.FencingToken) {
		t.Fatal("expected write accepted for active leader/token")
	}
	if manager.CanAcceptWrite("orders", "node-a", "wrong") {
		t.Fatal("expected write rejection for wrong token")
	}

	clock.Advance(6 * time.Second)
	if manager.CanAcceptWrite("orders", "node-a", state.FencingToken) {
		t.Fatal("expected write rejection after lease expiry")
	}
}

func TestSnapshotWithLeaseStatusTracksExpiry(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	_, err = manager.TryAcquireLeadership("catalog", "node-a", 2)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	_, exists, leaseActive := manager.SnapshotWithLeaseStatus("catalog")
	if !exists || !leaseActive {
		t.Fatalf("expected existing active lease, exists=%v leaseActive=%v", exists, leaseActive)
	}

	clock.Advance(6 * time.Second)
	_, exists, leaseActive = manager.SnapshotWithLeaseStatus("catalog")
	if !exists || leaseActive {
		t.Fatalf("expected existing expired lease, exists=%v leaseActive=%v", exists, leaseActive)
	}
}

func TestSyncExternalLeaderStateSetsGroupState(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	// Sync external leader state for a group that doesn't exist locally yet.
	external := GroupLeadershipState{
		Group:          "orders",
		Term:           3,
		LeaderID:       "node-a",
		FencingToken:   "orders:3:node-a",
		LeaseExpiresAt: time.Unix(108, 0).UTC(),
		LastLeaderLSN:  50,
	}
	manager.SyncExternalLeaderState(external)

	state, exists, leaseActive := manager.SnapshotWithLeaseStatus("orders")
	if !exists {
		t.Fatal("expected synced group to exist")
	}
	if !leaseActive {
		t.Fatal("expected synced lease to be active")
	}
	if state.Term != 3 {
		t.Fatalf("expected term=3, got %d", state.Term)
	}
	if state.LeaderID != "node-a" {
		t.Fatalf("expected leader node-a, got %s", state.LeaderID)
	}
	if state.LastLeaderLSN != 50 {
		t.Fatalf("expected LastLeaderLSN=50, got %d", state.LastLeaderLSN)
	}
}

func TestSyncExternalLeaderStateDoesNotRegressTerm(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	// Sync term 5 first.
	manager.SyncExternalLeaderState(GroupLeadershipState{
		Group:          "billing",
		Term:           5,
		LeaderID:       "node-b",
		FencingToken:   "billing:5:node-b",
		LeaseExpiresAt: time.Unix(110, 0).UTC(),
		LastLeaderLSN:  100,
	})

	// Attempt to sync an older term — should be ignored.
	manager.SyncExternalLeaderState(GroupLeadershipState{
		Group:          "billing",
		Term:           3,
		LeaderID:       "node-a",
		FencingToken:   "billing:3:node-a",
		LeaseExpiresAt: time.Unix(110, 0).UTC(),
		LastLeaderLSN:  80,
	})

	state, exists := manager.Snapshot("billing")
	if !exists {
		t.Fatal("expected group to exist")
	}
	if state.Term != 5 {
		t.Fatalf("expected term=5 (not regressed), got %d", state.Term)
	}
	if state.LeaderID != "node-b" {
		t.Fatalf("expected leader node-b (not regressed), got %s", state.LeaderID)
	}
}

func TestSyncExternalLeaderStateIgnoresEmptyGroup(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	manager.SyncExternalLeaderState(GroupLeadershipState{Group: "", Term: 1, LeaderID: "node-a"})

	_, exists := manager.Snapshot("")
	if exists {
		t.Fatal("expected empty group not to be registered")
	}
}

func TestSyncExternalLeaderStateLeaseExpiresNaturally(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	// Sync a leader whose lease expires at t=105.
	manager.SyncExternalLeaderState(GroupLeadershipState{
		Group:          "shipping",
		Term:           2,
		LeaderID:       "node-a",
		FencingToken:   "shipping:2:node-a",
		LeaseExpiresAt: time.Unix(105, 0).UTC(),
		LastLeaderLSN:  30,
	})

	// Lease should be active at t=100.
	_, _, leaseActive := manager.SnapshotWithLeaseStatus("shipping")
	if !leaseActive {
		t.Fatal("expected lease active before expiry")
	}

	// Advance past expiry.
	clock.Advance(6 * time.Second)
	_, _, leaseActive = manager.SnapshotWithLeaseStatus("shipping")
	if leaseActive {
		t.Fatal("expected lease expired after advancing clock past TTL")
	}
}

func TestForceExpireLeaseClears(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 30*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	// Sync a long-lived lease that would normally remain active.
	manager.SyncExternalLeaderState(GroupLeadershipState{
		Group:          "default",
		Term:           1,
		LeaderID:       "node-a",
		FencingToken:   "default:1:node-a",
		LeaseExpiresAt: time.Unix(130, 0).UTC(), // 30 s in the future
		LastLeaderLSN:  10,
	})

	_, _, leaseActive := manager.SnapshotWithLeaseStatus("default")
	if !leaseActive {
		t.Fatal("expected lease active before force-expire")
	}

	// Force-expire simulates the heartbeat loop having seen FailoverAfter
	// consecutive probe failures and wanting to unblock failover.
	manager.ForceExpireLease("default")

	_, exists, leaseActive := manager.SnapshotWithLeaseStatus("default")
	if !exists {
		t.Fatal("expected group to still exist")
	}
	if leaseActive {
		t.Fatal("expected lease inactive after ForceExpireLease")
	}
}

func TestForceExpireLeaseOnMissingGroupIsNoop(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	manager, err := NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}
	// Must not panic for an unknown group.
	manager.ForceExpireLease("nonexistent")
}
