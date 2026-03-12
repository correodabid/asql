package studioapp

import "testing"

func TestReadRoutingStatsRecordStrongLeader(t *testing.T) {
	stats := newReadRoutingStats()
	stats.record(readRoutingMetricInput{
		Consistency: ReadConsistencyStrong,
		Decision:    ReadRouteDecision{Route: ReadRouteLeader, Lag: 3},
		HasFollower: true,
		MaxLag:      1,
	})

	counts := stats.snapshot()
	if counts["requests_total"] != 1 {
		t.Fatalf("expected requests_total=1, got %d", counts["requests_total"])
	}
	if counts["consistency_strong"] != 1 {
		t.Fatalf("expected consistency_strong=1, got %d", counts["consistency_strong"])
	}
	if counts["route_leader"] != 1 {
		t.Fatalf("expected route_leader=1, got %d", counts["route_leader"])
	}
}

func TestReadRoutingStatsRecordBoundedStaleFollowerPath(t *testing.T) {
	stats := newReadRoutingStats()
	stats.record(readRoutingMetricInput{
		Consistency: ReadConsistencyBoundedStale,
		Decision:    ReadRouteDecision{Route: ReadRouteFollower, Lag: 2},
		HasFollower: true,
		MaxLag:      2,
	})

	counts := stats.snapshot()
	if counts["consistency_bounded_stale"] != 1 {
		t.Fatalf("expected consistency_bounded_stale=1, got %d", counts["consistency_bounded_stale"])
	}
	if counts["route_follower"] != 1 {
		t.Fatalf("expected route_follower=1, got %d", counts["route_follower"])
	}
	if counts["lag_within_threshold"] != 1 {
		t.Fatalf("expected lag_within_threshold=1, got %d", counts["lag_within_threshold"])
	}
	if counts["served_within_threshold"] != 1 {
		t.Fatalf("expected served_within_threshold=1, got %d", counts["served_within_threshold"])
	}
}

func TestReadRoutingStatsRecordBoundedStaleFallbacks(t *testing.T) {
	stats := newReadRoutingStats()

	stats.record(readRoutingMetricInput{
		Consistency:         ReadConsistencyBoundedStale,
		Decision:            ReadRouteDecision{Route: ReadRouteLeader, Lag: 0},
		HasFollower:         false,
		FollowerUnavailable: true,
		MaxLag:              0,
	})

	stats.record(readRoutingMetricInput{
		Consistency: ReadConsistencyBoundedStale,
		Decision:    ReadRouteDecision{Route: ReadRouteLeader, Lag: 10},
		HasFollower: true,
		MaxLag:      5,
	})

	counts := stats.snapshot()
	if counts["fallback_follower_unavailable"] != 1 {
		t.Fatalf("expected fallback_follower_unavailable=1, got %d", counts["fallback_follower_unavailable"])
	}
	if counts["fallback_no_follower"] != 1 {
		t.Fatalf("expected fallback_no_follower=1, got %d", counts["fallback_no_follower"])
	}
	if counts["lag_exceeded_threshold"] != 1 {
		t.Fatalf("expected lag_exceeded_threshold=1, got %d", counts["lag_exceeded_threshold"])
	}
	if counts["fallback_lag_exceeded"] != 1 {
		t.Fatalf("expected fallback_lag_exceeded=1, got %d", counts["fallback_lag_exceeded"])
	}
}
