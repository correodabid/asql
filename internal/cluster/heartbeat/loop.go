package heartbeat

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"asql/internal/cluster/coordinator"
	"asql/internal/engine/ports"
)

const (
	// DefaultHeartbeatInterval is the default period between heartbeat ticks.
	DefaultHeartbeatInterval = 2 * time.Second

	// DefaultFailoverAfter is the default number of consecutive probe failures
	// before triggering automatic failover.
	DefaultFailoverAfter = 3

	roleLeader   = "leader"
	roleFollower = "follower"
)

// Config controls heartbeat loop behaviour.
type Config struct {
	NodeID            string
	Peers             []Peer
	Groups            []string
	HeartbeatInterval time.Duration
	FailoverAfter     int
}

// leaderState tracks per-group leader information held by this node.
type leaderState struct {
	fencingToken string
}

// ReplicateFunc is called by the heartbeat loop when this node is a follower
// and the leader probe succeeds. It should pull WAL records from the leader.
type ReplicateFunc func(ctx context.Context, leaderAddr string) error

// Loop monitors cluster leader health and triggers automatic failover
// when the leader becomes unreachable.
type Loop struct {
	config     Config
	clock      ports.Clock
	leadership *coordinator.LeadershipManager
	failover   *coordinator.FailoverCoordinator
	prober     PeerProber
	localLSN   func() uint64
	replicate  ReplicateFunc
	logger     *slog.Logger
	// raftRoleSource, when non-nil, overrides the internal lease-based role
	// tracking.  Used when Raft is active so the heartbeat loop's tick
	// behaviour (leader vs follower) always reflects the elected Raft leader
	// rather than the heartbeat lease table.
	raftRoleSource func() string

	mu             sync.Mutex
	peers          []Peer                  // mutable peer list; protected by mu
	leaderAddr     map[string]string       // group -> leader peer address
	failCount      map[string]int          // group -> consecutive probe failures
	role           map[string]string       // group -> roleLeader | roleFollower
	leaderMeta     map[string]*leaderState // group -> leader fencing metadata (when this node is leader)
	tickReplicated bool                    // dedup replication across groups within one tick
}

// New creates a new heartbeat loop. It does not start it; call Run().
func New(
	config Config,
	clock ports.Clock,
	leadership *coordinator.LeadershipManager,
	failover *coordinator.FailoverCoordinator,
	prober PeerProber,
	localLSN func() uint64,
	logger *slog.Logger,
	options ...Option,
) *Loop {
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if config.FailoverAfter <= 0 {
		config.FailoverAfter = DefaultFailoverAfter
	}

	loop := &Loop{
		config:     config,
		clock:      clock,
		leadership: leadership,
		failover:   failover,
		prober:     prober,
		localLSN:   localLSN,
		logger:     logger,
		peers:      append([]Peer{}, config.Peers...), // mutable copy
		leaderAddr: make(map[string]string),
		failCount:  make(map[string]int),
		role:       make(map[string]string),
		leaderMeta: make(map[string]*leaderState),
	}

	for _, opt := range options {
		opt(loop)
	}

	return loop
}

// Option configures optional Loop behaviour.
type Option func(*Loop)

// AddPeer registers a new peer at runtime (hot join). Idempotent by NodeID.
func (loop *Loop) AddPeer(peer Peer) {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	for _, p := range loop.peers {
		if p.NodeID == peer.NodeID {
			return // already known
		}
	}
	loop.peers = append(loop.peers, peer)
}

// Peers returns a snapshot of the current peer list.
func (loop *Loop) Peers() []Peer {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	result := make([]Peer, len(loop.peers))
	copy(result, loop.peers)
	return result
}

// WithReplicateFunc sets the replication callback for follower catch-up.
func WithReplicateFunc(fn ReplicateFunc) Option {
	return func(loop *Loop) {
		loop.replicate = fn
	}
}

// WithRaftRoleSource injects a Raft-authoritative role function into the
// heartbeat loop.  When set, tickGroup delegates role determination to fn
// instead of the internal lease-based role map.  This ensures that nodes
// that are followers in Raft always run tickFollower (and therefore receive
// gossip with peer pgwire addresses) even if the heartbeat lease still lists
// them as the lease-holder.
// fn must return "leader" or "follower" (never empty).
func WithRaftRoleSource(fn func() string) Option {
	return func(loop *Loop) {
		loop.raftRoleSource = fn
	}
}

// SetRaftRoleSource wires a Raft-authoritative role function after construction.
// Useful when the Raft node is created after the heartbeat loop.
// Safe to call concurrently; the assignment is not under loop.mu because
// raftRoleSource is only read in tickGroup which runs single-threaded.
func (loop *Loop) SetRaftRoleSource(fn func() string) {
	loop.raftRoleSource = fn
}

// Run starts the heartbeat loop blocking until ctx is cancelled.
func (loop *Loop) Run(ctx context.Context) {
	ticker := time.NewTicker(loop.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			loop.Tick(ctx)
		}
	}
}

// Tick performs one heartbeat cycle across all configured groups.
// Exposed for deterministic testing.
func (loop *Loop) Tick(ctx context.Context) {
	loop.mu.Lock()
	loop.tickReplicated = false
	loop.mu.Unlock()

	for _, group := range loop.config.Groups {
		loop.tickGroup(ctx, group)
	}
}

// Role returns the current role for a group (for observability/testing).
func (loop *Loop) Role(group string) string {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	return loop.role[group]
}

// LeaderAddress returns the tracked leader address for a group (for observability/testing).
func (loop *Loop) LeaderAddress(group string) string {
	loop.mu.Lock()
	defer loop.mu.Unlock()
	return loop.leaderAddr[group]
}

func (loop *Loop) tickGroup(ctx context.Context, group string) {
	// When a Raft role source is wired in, always use it — it is authoritative.
	// This keeps the heartbeat tick (leader vs follower) in sync with the
	// actual Raft election result, independent of the heartbeat lease state.
	if loop.raftRoleSource != nil {
		raftRole := loop.raftRoleSource()
		if raftRole == roleLeader {
			// Ensure we have lease metadata before running tickLeader so that
			// RenewLeadership can succeed.  On the first tick after a Raft
			// election the leaderMeta may be nil if this node was a follower
			// before and never acquired the heartbeat lease.
			loop.mu.Lock()
			needsAcquire := loop.leaderMeta[group] == nil
			loop.mu.Unlock()
			if needsAcquire {
				lsn := loop.localLSN()
				state, err := loop.leadership.TryAcquireLeadership(group, loop.config.NodeID, lsn)
				if err == nil {
					loop.mu.Lock()
					loop.leaderMeta[group] = &leaderState{fencingToken: state.FencingToken}
					loop.mu.Unlock()
				}
				// Even if acquire fails (e.g. another node still holds the lease),
				// skip this tick — we'll retry next interval.
				return
			}
			loop.tickLeader(ctx, group)
		} else {
			loop.tickFollower(ctx, group)
		}
		return
	}

	loop.mu.Lock()
	currentRole := loop.role[group]
	loop.mu.Unlock()

	switch currentRole {
	case roleLeader:
		loop.tickLeader(ctx, group)
	case roleFollower:
		loop.tickFollower(ctx, group)
	default:
		// No role yet — discover leader from peers.
		loop.discoverLeader(ctx, group)
	}
}

// tickLeader renews the leadership lease and gossip-probes all peers so
// PgwireAddress entries propagate cluster-wide within two heartbeat cycles.
// Without this, followers would only learn the heartbeat-leader's pgwire
// address, leaving the Raft leader's address unknown when they differ.
func (loop *Loop) tickLeader(ctx context.Context, group string) {
	loop.mu.Lock()
	meta := loop.leaderMeta[group]
	loop.mu.Unlock()

	if meta == nil {
		loop.logWarn("heartbeat.leader_no_meta", group, "no leader metadata, stepping down")
		loop.stepDown(group)
		return
	}

	lsn := loop.localLSN()
	_, err := loop.leadership.RenewLeadership(group, loop.config.NodeID, meta.fencingToken, lsn)
	if err != nil {
		loop.logWarn("heartbeat.renew_failed", group, err.Error())
		loop.stepDown(group)
		return
	}

	loop.logInfo("heartbeat.renewed", group)

	// Gossip: probe each peer to learn their PgwireAddress and propagate
	// ours.  This closes the gap where followers only probe the heartbeat
	// leader and never exchange addresses with other followers or the Raft
	// leader (which may be a different node).
	loop.mu.Lock()
	snapshot := make([]Peer, len(loop.peers))
	copy(snapshot, loop.peers)
	loop.mu.Unlock()

	for _, peer := range snapshot {
		if peer.NodeID == loop.config.NodeID || peer.Address == "" {
			continue
		}
		probe, probeErr := loop.prober.ProbeLeadership(ctx, peer.Address, group)
		if probeErr != nil {
			continue // non-fatal; we'll pick it up next tick
		}
		loop.mergePeers(probe.Peers)
	}
}

// tickFollower probes the current leader and triggers failover if unreachable.
func (loop *Loop) tickFollower(ctx context.Context, group string) {
	loop.mu.Lock()
	addr := loop.leaderAddr[group]
	loop.mu.Unlock()

	if addr == "" {
		// Lost track of leader — try to discover.
		loop.discoverLeader(ctx, group)
		return
	}

	probe, err := loop.prober.ProbeLeadership(ctx, addr, group)
	if err != nil {
		loop.handleProbeFailure(ctx, group)
		return
	}

	if !probe.LeaseActive {
		loop.handleProbeFailure(ctx, group)
		return
	}

	// Leader is healthy — sync state and reset failures.
	loop.leadership.SyncExternalLeaderState(coordinator.GroupLeadershipState{
		Group:          group,
		Term:           probe.Term,
		LeaderID:       probe.LeaderID,
		FencingToken:   probe.FencingToken,
		LeaseExpiresAt: loop.clock.Now().Add(loop.config.HeartbeatInterval * time.Duration(loop.config.FailoverAfter)),
		LastLeaderLSN:  probe.LSN,
	})

	loop.mu.Lock()
	loop.failCount[group] = 0
	alreadyReplicated := loop.tickReplicated
	loop.tickReplicated = true
	loop.mu.Unlock()

	// Replicate WAL from leader (once per tick, not per group).
	if loop.replicate != nil && !alreadyReplicated {
		if err := loop.replicate(ctx, addr); err != nil {
			loop.logWarn("heartbeat.replicate_failed", group, err.Error())
		} else {
			loop.logInfo("heartbeat.replicated", group)
		}
	}

	loop.logInfo("heartbeat.follower_ok", group)

	// Gossip: propagate PgwireAddress for any peer the responder knows that
	// we don't have (or don't have a pgwire address for) yet.
	loop.mergePeers(probe.Peers)
}

// handleProbeFailure increments the failure counter and triggers failover
// when the threshold is reached.
func (loop *Loop) handleProbeFailure(ctx context.Context, group string) {
	loop.mu.Lock()
	loop.failCount[group]++
	count := loop.failCount[group]
	loop.mu.Unlock()

	loop.logWarn("heartbeat.probe_failed", group, "consecutive failures", slog.Int("count", count))

	if count < loop.config.FailoverAfter {
		return
	}

	// Force-expire the local lease before attempting failover. discoverLeader
	// may have refreshed it from stale peer gossip (peers that haven't yet
	// detected the leader failure), which would cause Failover() to return
	// ErrActiveLeaderLease. Clearing it here ensures the guard is bypassed —
	// the Tick goroutine is single-threaded so the lease stays cleared until
	// Failover() runs.
	loop.leadership.ForceExpireLease(group)

	loop.triggerFailover(ctx, group)
}

// triggerFailover gathers peer LSNs and executes deterministic failover.
func (loop *Loop) triggerFailover(ctx context.Context, group string) {
	loop.logInfo("heartbeat.failover_start", group)

	// Snapshot peers under lock before network probes.
	loop.mu.Lock()
	peersSnap := make([]Peer, len(loop.peers))
	copy(peersSnap, loop.peers)
	loop.mu.Unlock()

	// Gather candidates: self + all reachable peers.
	candidates := []coordinator.FailoverCandidate{
		{NodeID: loop.config.NodeID, NodeLSN: loop.localLSN()},
	}

	for _, peer := range peersSnap {
		lsn, err := loop.prober.ProbeLSN(ctx, peer.Address)
		if err != nil {
			loop.logWarn("heartbeat.peer_lsn_failed", group, peer.NodeID)
			continue
		}
		candidates = append(candidates, coordinator.FailoverCandidate{
			NodeID:  peer.NodeID,
			NodeLSN: lsn,
		})
	}

	result, err := loop.failover.Failover(group, candidates, 0)
	if err != nil {
		loop.logWarn("heartbeat.failover_failed", group, err.Error())

		// Failover failed (e.g. lease still active, no eligible candidate).
		// Try discovering the current leader from peers instead.
		loop.discoverLeader(ctx, group)
		return
	}

	loop.mu.Lock()
	loop.failCount[group] = 0

	if result.Promoted.LeaderID == loop.config.NodeID {
		loop.role[group] = roleLeader
		loop.leaderMeta[group] = &leaderState{fencingToken: result.Promoted.FencingToken}
		delete(loop.leaderAddr, group)
		loop.mu.Unlock()
		loop.logInfo("heartbeat.promoted_self", group)
	} else {
		loop.role[group] = roleFollower
		loop.leaderAddr[group] = loop.peerAddress(result.Promoted.LeaderID)
		delete(loop.leaderMeta, group)
		loop.mu.Unlock()
		loop.logInfo("heartbeat.promoted_peer", group, slog.String("new_leader", result.Promoted.LeaderID))
	}
}

// discoverLeader polls all peers for leadership state to find the current leader.
func (loop *Loop) discoverLeader(ctx context.Context, group string) {
	loop.mu.Lock()
	peersSnap := make([]Peer, len(loop.peers))
	copy(peersSnap, loop.peers)
	loop.mu.Unlock()

	for _, peer := range peersSnap {
		probe, err := loop.prober.ProbeLeadership(ctx, peer.Address, group)
		if err != nil {
			continue
		}
		if probe.LeaseActive && probe.LeaderID != "" {
			loop.leadership.SyncExternalLeaderState(coordinator.GroupLeadershipState{
				Group:          group,
				Term:           probe.Term,
				LeaderID:       probe.LeaderID,
				FencingToken:   probe.FencingToken,
				LeaseExpiresAt: loop.clock.Now().Add(loop.config.HeartbeatInterval * time.Duration(loop.config.FailoverAfter)),
				LastLeaderLSN:  probe.LSN,
			})

			loop.mu.Lock()
			if probe.LeaderID == loop.config.NodeID {
				// We are the leader according to the cluster.
				loop.role[group] = roleLeader
				loop.leaderMeta[group] = &leaderState{fencingToken: probe.FencingToken}
				delete(loop.leaderAddr, group)
			} else {
				loop.role[group] = roleFollower
				loop.leaderAddr[group] = peer.Address
				delete(loop.leaderMeta, group)
			}
			loop.failCount[group] = 0
			loop.mu.Unlock()

			loop.logInfo("heartbeat.discovered_leader", group, slog.String("leader", probe.LeaderID))
			// Gossip: merge peer addresses from the responding node.
			loop.mergePeers(probe.Peers)
			return
		}
	}

	// No peer has an active leader — attempt to acquire leadership ourselves (bootstrap path).
	lsn := loop.localLSN()
	state, err := loop.leadership.TryAcquireLeadership(group, loop.config.NodeID, lsn)
	if err != nil {
		loop.logWarn("heartbeat.no_leader_found", group, "no active leader discovered from peers")
		return
	}

	loop.mu.Lock()
	loop.role[group] = roleLeader
	loop.leaderMeta[group] = &leaderState{fencingToken: state.FencingToken}
	delete(loop.leaderAddr, group)
	loop.failCount[group] = 0
	loop.mu.Unlock()

	loop.logInfo("heartbeat.bootstrap_leader", group, slog.String("fencing_token", state.FencingToken))
}

// stepDown transitions this node from leader to follower for the given group.
func (loop *Loop) stepDown(group string) {
	loop.mu.Lock()
	loop.role[group] = roleFollower
	delete(loop.leaderMeta, group)
	delete(loop.leaderAddr, group)
	loop.failCount[group] = 0
	loop.mu.Unlock()

	loop.logInfo("heartbeat.stepped_down", group)
}

// peerAddress returns the address for a given node ID.
// Caller must hold loop.mu.
func (loop *Loop) peerAddress(nodeID string) string {
	for _, peer := range loop.peers {
		if peer.NodeID == nodeID {
			return peer.Address
		}
	}
	return ""
}

// mergePeers incorporates gossip peers received in a probe response.
// It adds entirely new peers and backfills PgwireAddress on existing peers
// that were registered with only a gRPC address (static -peers flag).
// Safe to call without holding loop.mu.
func (loop *Loop) mergePeers(gossip []Peer) {
	if len(gossip) == 0 {
		return
	}
	loop.mu.Lock()
	defer loop.mu.Unlock()

	// Build a nodeID → slice-index map for O(1) look-up.
	idx := make(map[string]int, len(loop.peers))
	for i, p := range loop.peers {
		idx[p.NodeID] = i
	}

	for _, gp := range gossip {
		if gp.NodeID == "" || gp.Address == "" {
			continue
		}
		if i, exists := idx[gp.NodeID]; exists {
			// Backfill PgwireAddress if it was previously missing.
			if loop.peers[i].PgwireAddress == "" && gp.PgwireAddress != "" {
				loop.peers[i].PgwireAddress = gp.PgwireAddress
			}
		} else {
			loop.peers = append(loop.peers, gp)
			idx[gp.NodeID] = len(loop.peers) - 1
		}
	}
}

// ---------- logging helpers ----------

func (loop *Loop) logInfo(event, group string, attrs ...any) {
	if loop.logger == nil {
		return
	}
	args := append([]any{slog.String("event", event), slog.String("group", group), slog.String("node", loop.config.NodeID)}, attrs...)
	loop.logger.Info("heartbeat", args...)
}

func (loop *Loop) logWarn(event, group string, attrs ...any) {
	if loop.logger == nil {
		return
	}
	args := append([]any{slog.String("event", event), slog.String("group", group), slog.String("node", loop.config.NodeID)}, attrs...)
	loop.logger.Warn("heartbeat", args...)
}
