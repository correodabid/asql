package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/correodabid/asql/internal/cluster/coordinator"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type leadershipTestClock struct {
	now time.Time
}

func (clock *leadershipTestClock) Now() time.Time {
	return clock.now
}

func TestLeadershipStateRejectsEmptyGroup(t *testing.T) {
	service := newService(nil, nil, nil)

	_, err := service.LeadershipState(context.Background(), &LeadershipStateRequest{Group: "   "})
	if err == nil {
		t.Fatal("expected invalid argument error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}
}

func TestLeadershipStateReturnsNotFoundForUnknownGroup(t *testing.T) {
	clock := &leadershipTestClock{now: time.Unix(100, 0).UTC()}
	manager, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	service := newService(nil, nil, manager)

	_, err = service.LeadershipState(context.Background(), &LeadershipStateRequest{Group: "missing"})
	if err == nil {
		t.Fatal("expected not found error")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", status.Code(err))
	}
}

func TestLeadershipStateReturnsSnapshotFields(t *testing.T) {
	clock := &leadershipTestClock{now: time.Unix(100, 0).UTC()}
	manager, err := coordinator.NewLeadershipManager(clock, 5*time.Second)
	if err != nil {
		t.Fatalf("new leadership manager: %v", err)
	}

	state, err := manager.TryAcquireLeadership("orders", "node-a", 42)
	if err != nil {
		t.Fatalf("acquire leadership: %v", err)
	}

	service := newService(nil, nil, manager)

	response, err := service.LeadershipState(context.Background(), &LeadershipStateRequest{Group: "orders"})
	if err != nil {
		t.Fatalf("leadership state: %v", err)
	}

	if response.Group != "orders" {
		t.Fatalf("unexpected group: got %q", response.Group)
	}
	if response.Term != state.Term {
		t.Fatalf("unexpected term: got %d want %d", response.Term, state.Term)
	}
	if response.LeaderID != "node-a" {
		t.Fatalf("unexpected leader id: got %q", response.LeaderID)
	}
	if response.FencingToken == "" {
		t.Fatal("expected non-empty fencing token")
	}
	if response.LeaseExpiresAtUnix != state.LeaseExpiresAt.Unix() {
		t.Fatalf("unexpected lease expiry: got %d want %d", response.LeaseExpiresAtUnix, state.LeaseExpiresAt.Unix())
	}
	if !response.LeaseActive {
		t.Fatal("expected active lease")
	}
	if response.LastLeaderLSN != 42 {
		t.Fatalf("unexpected last leader lsn: got %d", response.LastLeaderLSN)
	}
}
