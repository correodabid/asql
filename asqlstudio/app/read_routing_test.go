package studioapp

import "testing"

func TestNormalizeReadConsistency(t *testing.T) {
	if got := normalizeReadConsistency("bounded-stale"); got != ReadConsistencyBoundedStale {
		t.Fatalf("expected bounded-stale, got %q", got)
	}

	if got := normalizeReadConsistency(""); got != ReadConsistencyStrong {
		t.Fatalf("expected strong for empty consistency, got %q", got)
	}

	if got := normalizeReadConsistency("unknown"); got != ReadConsistencyStrong {
		t.Fatalf("expected strong for unknown consistency, got %q", got)
	}
}

func TestDecideReadRouteStrongUsesLeader(t *testing.T) {
	decision := DecideReadRoute(ReadRouteInput{
		Consistency: ReadConsistencyStrong,
		LeaderLSN:   10,
		FollowerLSN: 9,
		HasFollower: true,
		MaxLag:      5,
	})

	if decision.Route != ReadRouteLeader {
		t.Fatalf("expected leader route, got %q", decision.Route)
	}
	if decision.Lag != 1 {
		t.Fatalf("expected lag=1, got %d", decision.Lag)
	}
}

func TestDecideReadRouteBoundedStaleWithinThresholdUsesFollower(t *testing.T) {
	decision := DecideReadRoute(ReadRouteInput{
		Consistency: ReadConsistencyBoundedStale,
		LeaderLSN:   20,
		FollowerLSN: 18,
		HasFollower: true,
		MaxLag:      2,
	})

	if decision.Route != ReadRouteFollower {
		t.Fatalf("expected follower route, got %q", decision.Route)
	}
	if decision.Lag != 2 {
		t.Fatalf("expected lag=2, got %d", decision.Lag)
	}
}

func TestDecideReadRouteBoundedStaleAboveThresholdFallsBackToLeader(t *testing.T) {
	decision := DecideReadRoute(ReadRouteInput{
		Consistency: ReadConsistencyBoundedStale,
		LeaderLSN:   20,
		FollowerLSN: 10,
		HasFollower: true,
		MaxLag:      5,
	})

	if decision.Route != ReadRouteLeader {
		t.Fatalf("expected leader fallback route, got %q", decision.Route)
	}
	if decision.Lag != 10 {
		t.Fatalf("expected lag=10, got %d", decision.Lag)
	}
}

func TestDecideReadRouteWithoutFollowerUsesLeader(t *testing.T) {
	decision := DecideReadRoute(ReadRouteInput{
		Consistency: ReadConsistencyBoundedStale,
		LeaderLSN:   7,
		FollowerLSN: 0,
		HasFollower: false,
		MaxLag:      10,
	})

	if decision.Route != ReadRouteLeader {
		t.Fatalf("expected leader route without follower, got %q", decision.Route)
	}
	if decision.Lag != 7 {
		t.Fatalf("expected lag=7, got %d", decision.Lag)
	}
}
