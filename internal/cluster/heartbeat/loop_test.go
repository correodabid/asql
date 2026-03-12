package heartbeat

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"asql/internal/cluster/coordinator"
)

// ---------- test helpers ----------

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// mockProber is a deterministic PeerProber for testing.
type mockProber struct {
	mu             sync.Mutex
	leadershipResp map[string]LeadershipProbe // addr+group -> response
	leadershipErr  map[string]error           // addr+group -> error
	lsnResp        map[string]uint64          // addr -> LSN
	lsnErr         map[string]error           // addr -> error
}

func newMockProber() *mockProber {
	return &mockProber{
		leadershipResp: make(map[string]LeadershipProbe),
		leadershipErr:  make(map[string]error),
		lsnResp:        make(map[string]uint64),
		lsnErr:         make(map[string]error),
	}
}

func (m *mockProber) SetLeadership(addr, group string, probe LeadershipProbe, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := addr + "|" + group
	m.leadershipResp[key] = probe
	if err != nil {
		m.leadershipErr[key] = err
	} else {
		delete(m.leadershipErr, key)
	}
}

func (m *mockProber) SetLSN(addr string, lsn uint64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lsnResp[addr] = lsn
	if err != nil {
		m.lsnErr[addr] = err
	} else {
		delete(m.lsnErr, addr)
	}
}

func (m *mockProber) ProbeLeadership(_ context.Context, addr, group string) (LeadershipProbe, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := addr + "|" + group
	if err, ok := m.leadershipErr[key]; ok {
		return LeadershipProbe{}, err
	}
	probe, ok := m.leadershipResp[key]
	if !ok {
		return LeadershipProbe{}, errors.New("peer unreachable")
	}
	return probe, nil
}

func (m *mockProber) ProbeLSN(_ context.Context, addr string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err, ok := m.lsnErr[addr]; ok {
		return 0, err
	}
	lsn, ok := m.lsnResp[addr]
	if !ok {
		return 0, errors.New("peer unreachable")
	}
	return lsn, nil
}

func newTestLoop(t *testing.T, nodeID string, peers []Peer, groups []string, clock *manualClock, prober *mockProber, localLSN uint64) *Loop {
	t.Helper()

	leadership, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}
	failoverCoord, err := coordinator.NewFailoverCoordinator(leadership)
	if err != nil {
		t.Fatalf("new failover coordinator: %v", err)
	}

	return New(
		Config{
			NodeID:            nodeID,
			Peers:             peers,
			Groups:            groups,
			HeartbeatInterval: 2 * time.Second,
			FailoverAfter:     3,
		},
		clock,
		leadership,
		failoverCoord,
		prober,
		func() uint64 { return localLSN },
		nil, // no logger in tests
	)
}

// ---------- tests ----------

func TestLeaderRenewsLeaseOnTick(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	prober := newMockProber()

	loop := newTestLoop(t, "node-a", nil, []string{"orders"}, clock, prober, 50)

	// Acquire leadership first.
	state, err := loop.leadership.TryAcquireLeadership("orders", "node-a", 50)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	// Set role to leader with metadata.
	loop.mu.Lock()
	loop.role["orders"] = roleLeader
	loop.leaderMeta["orders"] = &leaderState{fencingToken: state.FencingToken}
	loop.mu.Unlock()

	// Tick — should renew the lease.
	clock.Advance(2 * time.Second)
	loop.Tick(context.Background())

	if loop.Role("orders") != roleLeader {
		t.Fatalf("expected role=leader, got %s", loop.Role("orders"))
	}

	// Verify lease was renewed (still active after advancing past original TTL).
	clock.Advance(3 * time.Second) // total 5s from original, but renewed at 2s
	_, _, active := loop.leadership.SnapshotWithLeaseStatus("orders")
	if !active {
		t.Fatal("expected lease still active after renewal")
	}
}

func TestLeaderStepsDownOnRenewalFailure(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	prober := newMockProber()

	loop := newTestLoop(t, "node-a", nil, []string{"orders"}, clock, prober, 50)

	// Set role to leader but with a bad fencing token.
	loop.mu.Lock()
	loop.role["orders"] = roleLeader
	loop.leaderMeta["orders"] = &leaderState{fencingToken: "bad-token"}
	loop.mu.Unlock()

	loop.Tick(context.Background())

	if loop.Role("orders") != roleFollower {
		t.Fatalf("expected role=follower after renewal failure, got %s", loop.Role("orders"))
	}
}

func TestFollowerSyncsLeaderState(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	prober := newMockProber()

	peers := []Peer{{NodeID: "node-a", Address: "10.0.0.1:9042"}}
	loop := newTestLoop(t, "node-b", peers, []string{"payments"}, clock, prober, 40)

	// Set follower role with known leader address.
	loop.mu.Lock()
	loop.role["payments"] = roleFollower
	loop.leaderAddr["payments"] = "10.0.0.1:9042"
	loop.mu.Unlock()

	// Mock leader responding healthy.
	prober.SetLeadership("10.0.0.1:9042", "payments", LeadershipProbe{
		LeaderID:     "node-a",
		Term:         3,
		FencingToken: "payments:3:node-a",
		LeaseActive:  true,
		LSN:          100,
	}, nil)

	loop.Tick(context.Background())

	// Verify leader state was synced into local leadership manager.
	state, exists, _ := loop.leadership.SnapshotWithLeaseStatus("payments")
	if !exists {
		t.Fatal("expected synced state to exist")
	}
	if state.Term != 3 {
		t.Fatalf("expected synced term=3, got %d", state.Term)
	}
	if state.LeaderID != "node-a" {
		t.Fatalf("expected synced leader=node-a, got %s", state.LeaderID)
	}
}

func TestFollowerDetectsLeaderFailureAndTriggersFailover(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	prober := newMockProber()

	peers := []Peer{{NodeID: "node-a", Address: "10.0.0.1:9042"}}
	loop := newTestLoop(t, "node-b", peers, []string{"billing"}, clock, prober, 100)

	// Seed leadership state so FailoverCoordinator has something to work with.
	loop.leadership.SyncExternalLeaderState(coordinator.GroupLeadershipState{
		Group:          "billing",
		Term:           2,
		LeaderID:       "node-a",
		FencingToken:   "billing:2:node-a",
		LeaseExpiresAt: time.Unix(103, 0).UTC(), // will expire shortly
		LastLeaderLSN:  90,
	})

	loop.mu.Lock()
	loop.role["billing"] = roleFollower
	loop.leaderAddr["billing"] = "10.0.0.1:9042"
	loop.mu.Unlock()

	// Make leader unreachable.
	prober.SetLeadership("10.0.0.1:9042", "billing", LeadershipProbe{}, errors.New("connection refused"))

	// Also make LSN probes available for self and the dead leader.
	prober.SetLSN("10.0.0.1:9042", 0, errors.New("connection refused"))

	// Tick 3 times to reach failover threshold.
	for i := 0; i < 2; i++ {
		clock.Advance(2 * time.Second)
		loop.Tick(context.Background())
	}

	// At this point failCount=2, no failover yet.
	if loop.Role("billing") != roleFollower {
		t.Fatalf("expected still follower after 2 failures, got %s", loop.Role("billing"))
	}

	// Third tick — should trigger failover. Advance clock past lease expiry.
	clock.Advance(2 * time.Second)
	loop.Tick(context.Background())

	// node-b (self, LSN=100) should be promoted since node-a is unreachable.
	if loop.Role("billing") != roleLeader {
		t.Fatalf("expected promoted to leader, got %s", loop.Role("billing"))
	}
}

func TestFailoverElectsHighestLSNCandidate(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	prober := newMockProber()

	peers := []Peer{
		{NodeID: "node-a", Address: "10.0.0.1:9042"},
		{NodeID: "node-c", Address: "10.0.0.3:9042"},
	}
	// node-b has LSN 80, node-c has LSN 120.
	loop := newTestLoop(t, "node-b", peers, []string{"orders"}, clock, prober, 80)

	// Seed expired leadership.
	loop.leadership.SyncExternalLeaderState(coordinator.GroupLeadershipState{
		Group:          "orders",
		Term:           1,
		LeaderID:       "node-a",
		FencingToken:   "orders:1:node-a",
		LeaseExpiresAt: time.Unix(99, 0).UTC(), // already expired
		LastLeaderLSN:  80,
	})

	loop.mu.Lock()
	loop.role["orders"] = roleFollower
	loop.leaderAddr["orders"] = "10.0.0.1:9042"
	loop.mu.Unlock()

	// Leader unreachable, node-c reachable with higher LSN.
	prober.SetLeadership("10.0.0.1:9042", "orders", LeadershipProbe{}, errors.New("down"))
	prober.SetLSN("10.0.0.1:9042", 0, errors.New("down"))
	prober.SetLSN("10.0.0.3:9042", 120, nil)

	// Trigger failover with 3 ticks.
	for i := 0; i < 3; i++ {
		clock.Advance(2 * time.Second)
		loop.Tick(context.Background())
	}

	// node-c has highest LSN (120), so it should be elected.
	// node-b should remain follower and track node-c as leader.
	if loop.Role("orders") != roleFollower {
		t.Fatalf("expected node-b to remain follower (node-c has higher LSN), got %s", loop.Role("orders"))
	}
	if loop.LeaderAddress("orders") != "10.0.0.3:9042" {
		t.Fatalf("expected leader address 10.0.0.3:9042, got %s", loop.LeaderAddress("orders"))
	}
}

func TestNewLeaderStartsRenewalAfterPromotion(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	prober := newMockProber()

	peers := []Peer{{NodeID: "node-a", Address: "10.0.0.1:9042"}}
	loop := newTestLoop(t, "node-b", peers, []string{"accounts"}, clock, prober, 100)

	// Seed expired leadership.
	loop.leadership.SyncExternalLeaderState(coordinator.GroupLeadershipState{
		Group:          "accounts",
		Term:           1,
		LeaderID:       "node-a",
		FencingToken:   "accounts:1:node-a",
		LeaseExpiresAt: time.Unix(99, 0).UTC(),
		LastLeaderLSN:  90,
	})

	loop.mu.Lock()
	loop.role["accounts"] = roleFollower
	loop.leaderAddr["accounts"] = "10.0.0.1:9042"
	loop.mu.Unlock()

	prober.SetLeadership("10.0.0.1:9042", "accounts", LeadershipProbe{}, errors.New("down"))
	prober.SetLSN("10.0.0.1:9042", 0, errors.New("down"))

	// Trigger failover.
	for i := 0; i < 3; i++ {
		clock.Advance(2 * time.Second)
		loop.Tick(context.Background())
	}

	if loop.Role("accounts") != roleLeader {
		t.Fatalf("expected role=leader after promotion, got %s", loop.Role("accounts"))
	}

	// Next tick should renew the lease as leader (not crash).
	clock.Advance(2 * time.Second)
	loop.Tick(context.Background())

	if loop.Role("accounts") != roleLeader {
		t.Fatalf("expected still leader after renewal tick, got %s", loop.Role("accounts"))
	}

	// Verify lease is active.
	_, _, active := loop.leadership.SnapshotWithLeaseStatus("accounts")
	if !active {
		t.Fatal("expected active lease after leader renewal tick")
	}
}

func TestFollowerDiscoversNewLeaderAfterFailover(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	prober := newMockProber()

	peers := []Peer{
		{NodeID: "node-a", Address: "10.0.0.1:9042"},
		{NodeID: "node-c", Address: "10.0.0.3:9042"},
	}
	loop := newTestLoop(t, "node-b", peers, []string{"catalog"}, clock, prober, 50)

	// No role set — should discover leader.
	prober.SetLeadership("10.0.0.1:9042", "catalog", LeadershipProbe{}, errors.New("down"))
	prober.SetLeadership("10.0.0.3:9042", "catalog", LeadershipProbe{
		LeaderID:     "node-c",
		Term:         5,
		FencingToken: "catalog:5:node-c",
		LeaseActive:  true,
		LSN:          200,
	}, nil)

	loop.Tick(context.Background())

	if loop.Role("catalog") != roleFollower {
		t.Fatalf("expected role=follower after discovering node-c as leader, got %s", loop.Role("catalog"))
	}
	if loop.LeaderAddress("catalog") != "10.0.0.3:9042" {
		t.Fatalf("expected leader address 10.0.0.3:9042, got %s", loop.LeaderAddress("catalog"))
	}
}

func TestDiscoverLeaderSetsRoleToLeaderWhenSelfIsLeader(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	prober := newMockProber()

	peers := []Peer{{NodeID: "node-a", Address: "10.0.0.1:9042"}}
	loop := newTestLoop(t, "node-b", peers, []string{"shipping"}, clock, prober, 100)

	// Peer reports that node-b is the leader.
	prober.SetLeadership("10.0.0.1:9042", "shipping", LeadershipProbe{
		LeaderID:     "node-b",
		Term:         4,
		FencingToken: "shipping:4:node-b",
		LeaseActive:  true,
		LSN:          100,
	}, nil)

	loop.Tick(context.Background())

	if loop.Role("shipping") != roleLeader {
		t.Fatalf("expected role=leader when self is leader, got %s", loop.Role("shipping"))
	}
}

// TestFailoverSucceedsWhenLeaseActiveFromStalePeerGossip covers the scenario
// where probe failures happen fast enough (e.g. "connection refused") that the
// failure threshold is reached before the synced lease expires.
//
// Previously discoverLeader would refresh the lease from stale peer gossip and
// reset failCount, creating an infinite loop where failover never fired.
// The fix is to call ForceExpireLease before triggerFailover so that
// FailoverCoordinator.Failover always sees leaseActive=false.
func TestFailoverSucceedsWhenLeaseActiveFromStalePeerGossip(t *testing.T) {
	clock := &manualClock{now: time.Unix(100, 0)}
	prober := newMockProber()

	peers := []Peer{{NodeID: "node-a", Address: "10.0.0.1:9042"}}
	// node-b has the highest LSN so it will self-promote.
	loop := newTestLoop(t, "node-b", peers, []string{"orders"}, clock, prober, 200)

	// Seed a long-lived lease — simulates what discoverLeader refreshes from
	// stale gossip: the lease would not expire for 60 s.
	loop.leadership.SyncExternalLeaderState(coordinator.GroupLeadershipState{
		Group:          "orders",
		Term:           1,
		LeaderID:       "node-a",
		FencingToken:   "orders:1:node-a",
		LeaseExpiresAt: time.Unix(160, 0).UTC(), // 60 s in the future
		LastLeaderLSN:  100,
	})

	loop.mu.Lock()
	loop.role["orders"] = roleFollower
	loop.leaderAddr["orders"] = "10.0.0.1:9042"
	loop.mu.Unlock()

	// Leader is down — probe returns immediately with an error (fast reject).
	prober.SetLeadership("10.0.0.1:9042", "orders", LeadershipProbe{}, errors.New("connection refused"))
	prober.SetLSN("10.0.0.1:9042", 0, errors.New("connection refused"))

	// Tick 3 times WITHOUT advancing the clock — the lease is still "active"
	// according to raw SnapshotWithLeaseStatus, but ForceExpireLease must
	// ensure Failover() is not blocked.
	for i := 0; i < 3; i++ {
		loop.Tick(context.Background())
	}

	// Despite the lease appearing active from the synced state, node-b must
	// have self-promoted because ForceExpireLease clears the guard.
	if role := loop.Role("orders"); role != roleLeader {
		t.Fatalf("expected node-b promoted to leader, but role is %s", role)
	}
}
