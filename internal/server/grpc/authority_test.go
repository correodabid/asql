package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/cluster/coordinator"
	"github.com/correodabid/asql/internal/cluster/heartbeat"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeClusterAuthority struct {
	leaderID       string
	canAcceptWrite bool
	seenGroups     []string
}

func (authority *fakeClusterAuthority) LeaderID(groups []string) string {
	authority.seenGroups = append([]string(nil), groups...)
	return authority.leaderID
}

func (authority *fakeClusterAuthority) CanAcceptWrite(group, nodeID, fencing string) bool {
	return authority.canAcceptWrite
}

type fakePeerRegistry struct {
	peers []heartbeat.Peer
}

func (registry *fakePeerRegistry) AddPeer(peer heartbeat.Peer) {
	registry.peers = append(registry.peers, peer)
}

func (registry *fakePeerRegistry) Peers() []heartbeat.Peer {
	return append([]heartbeat.Peer(nil), registry.peers...)
}

func TestValidateFencingForCommitUsesAuthority(t *testing.T) {
	service := newService(nil, nil, nil)
	service.authority = &fakeClusterAuthority{canAcceptWrite: false}

	err := service.validateFencingForCommit(nil, &CommitTxRequest{
		TxID:         "tx-1",
		Group:        "orders",
		NodeID:       "node-a",
		FencingToken: "orders-1-node-a",
	})
	if err == nil {
		t.Fatal("expected permission denied error")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", status.Code(err))
	}
}

func TestJoinClusterUsesAuthorityLeaderID(t *testing.T) {
	registry := &fakePeerRegistry{peers: []heartbeat.Peer{
		{NodeID: "node-a", Address: "127.0.0.1:7001", PgwireAddress: "127.0.0.1:5432"},
		{NodeID: "node-b", Address: "127.0.0.1:7002", PgwireAddress: "127.0.0.1:5433"},
	}}
	authority := &fakeClusterAuthority{leaderID: "node-b"}
	service := newService(nil, nil, nil)
	service.peerRegistry = registry
	service.authority = authority

	response, err := service.JoinCluster(context.Background(), &JoinClusterRequest{
		NodeID:        "node-c",
		Address:       "127.0.0.1:7003",
		PgwireAddress: "127.0.0.1:5434",
		Groups:        []string{"orders"},
	})
	if err != nil {
		t.Fatalf("join cluster: %v", err)
	}
	if response.LeaderID != "node-b" {
		t.Fatalf("unexpected leader id: got %q", response.LeaderID)
	}
	if response.LeaderAddress != "127.0.0.1:7002" {
		t.Fatalf("unexpected leader address: got %q", response.LeaderAddress)
	}
	if len(authority.seenGroups) != 1 || authority.seenGroups[0] != "orders" {
		t.Fatalf("authority did not receive join groups: %#v", authority.seenGroups)
	}
	if len(response.KnownPeers) != 2 {
		t.Fatalf("unexpected known peers count: got %d want 2", len(response.KnownPeers))
	}
	if len(registry.peers) != 3 {
		t.Fatalf("expected joining peer to be registered, got %d peers", len(registry.peers))
	}
}

type fakeRaftLeaderState struct {
	isLeader bool
	leaderID string
}

func (state fakeRaftLeaderState) IsLeader() bool   { return state.isLeader }
func (state fakeRaftLeaderState) LeaderID() string { return state.leaderID }

func TestRaftClusterAuthorityRequiresLocalLeader(t *testing.T) {
	clock := &leadershipTestClock{now: time.Unix(100, 0).UTC()}
	manager, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}
	state, err := manager.TryAcquireLeadership("orders", "node-a", 42)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	authority := &raftClusterAuthority{
		localNodeID: "node-a",
		raftNode:    fakeRaftLeaderState{isLeader: false, leaderID: "node-b"},
		leadership:  manager,
	}
	if authority.CanAcceptWrite("orders", "node-a", state.FencingToken) {
		t.Fatal("expected follower authority to reject writes")
	}

	authority.raftNode = fakeRaftLeaderState{isLeader: true, leaderID: "node-a"}
	if !authority.CanAcceptWrite("orders", "node-a", state.FencingToken) {
		t.Fatal("expected local leader authority to accept writes")
	}
	if authority.CanAcceptWrite("orders", "node-b", state.FencingToken) {
		t.Fatal("expected authority to reject writes for non-local node id")
	}
	if authority.LeaderID([]string{"orders"}) != "node-a" {
		t.Fatalf("unexpected leader id when local leader: got %q", authority.LeaderID([]string{"orders"}))
	}
	authority.raftNode = fakeRaftLeaderState{isLeader: false, leaderID: "node-b"}
	if authority.LeaderID([]string{"orders"}) != "node-b" {
		t.Fatalf("unexpected leader id when follower: got %q", authority.LeaderID([]string{"orders"}))
	}
}
