package grpc

import "testing"

func TestNormalizeReadConsistency(t *testing.T) {
	if got := normalizeReadConsistency("bounded-stale"); got != readConsistencyBoundedStale {
		t.Fatalf("expected bounded-stale, got %q", got)
	}

	if got := normalizeReadConsistency(""); got != readConsistencyStrong {
		t.Fatalf("expected strong for empty consistency, got %q", got)
	}

	if got := normalizeReadConsistency("unknown"); got != readConsistencyStrong {
		t.Fatalf("expected strong for unknown consistency, got %q", got)
	}
}

func TestDecideReadRouteBoundedStaleWithinThresholdUsesFollower(t *testing.T) {
	decision := decideReadRoute(readRouteInput{
		Consistency: readConsistencyBoundedStale,
		LeaderLSN:   20,
		FollowerLSN: 18,
		HasFollower: true,
		MaxLag:      2,
	})

	if decision.Route != readRouteFollower {
		t.Fatalf("expected follower route, got %q", decision.Route)
	}
	if decision.Lag != 2 {
		t.Fatalf("expected lag=2, got %d", decision.Lag)
	}
	if decision.FallbackReason != fallbackReasonNone {
		t.Fatalf("expected empty fallback reason, got %q", decision.FallbackReason)
	}
}

func TestDecideReadRouteBoundedStaleFallbackReasons(t *testing.T) {
	cases := []struct {
		name   string
		input  readRouteInput
		reason string
	}{
		{name: "strong", input: readRouteInput{Consistency: readConsistencyStrong, LeaderLSN: 10, FollowerLSN: 5, HasFollower: true, MaxLag: 5}, reason: fallbackReasonStrongConsistency},
		{name: "unavailable", input: readRouteInput{Consistency: readConsistencyBoundedStale, LeaderLSN: 10, FollowerLSN: 9, HasFollower: true, FollowerUnavailable: true, MaxLag: 5}, reason: fallbackReasonFollowerUnavailable},
		{name: "no_follower", input: readRouteInput{Consistency: readConsistencyBoundedStale, LeaderLSN: 10, FollowerLSN: 0, HasFollower: false, MaxLag: 5}, reason: fallbackReasonNoFollowerConfigured},
		{name: "lag_exceeded", input: readRouteInput{Consistency: readConsistencyBoundedStale, LeaderLSN: 20, FollowerLSN: 10, HasFollower: true, MaxLag: 5}, reason: fallbackReasonLagExceeded},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decision := decideReadRoute(tc.input)
			if decision.Route != readRouteLeader {
				t.Fatalf("expected leader route, got %q", decision.Route)
			}
			if decision.FallbackReason != tc.reason {
				t.Fatalf("expected fallback reason %q, got %q", tc.reason, decision.FallbackReason)
			}
		})
	}
}

func TestReadRoutingStatsRecord(t *testing.T) {
	stats := newReadRoutingStats()
	stats.record(readRoutingMetricInput{
		Consistency: readConsistencyBoundedStale,
		Decision: readRouteDecision{
			Route:          readRouteLeader,
			Lag:            10,
			FallbackReason: fallbackReasonLagExceeded,
		},
		HasFollower: true,
		MaxLag:      5,
		LeaderLSN:   20,
		FollowerLSN: 10,
	})

	counts := stats.snapshot()
	if counts["requests_total"] != 1 {
		t.Fatalf("expected requests_total=1, got %d", counts["requests_total"])
	}
	if counts["consistency_bounded_stale"] != 1 {
		t.Fatalf("expected consistency_bounded_stale=1, got %d", counts["consistency_bounded_stale"])
	}
	if counts["route_leader"] != 1 {
		t.Fatalf("expected route_leader=1, got %d", counts["route_leader"])
	}
	if counts["fallback_lag_exceeded"] != 1 {
		t.Fatalf("expected fallback_lag_exceeded=1, got %d", counts["fallback_lag_exceeded"])
	}
}

func TestReadRouteDeterministicAcrossRepeatedSeededTimeline(t *testing.T) {
	seeded := []readRouteInput{
		{Consistency: readConsistencyBoundedStale, LeaderLSN: 100, FollowerLSN: 100, HasFollower: true, MaxLag: 2},
		{Consistency: readConsistencyBoundedStale, LeaderLSN: 101, FollowerLSN: 100, HasFollower: true, MaxLag: 2},
		{Consistency: readConsistencyBoundedStale, LeaderLSN: 105, FollowerLSN: 100, HasFollower: true, MaxLag: 2},
		{Consistency: readConsistencyBoundedStale, LeaderLSN: 106, FollowerLSN: 100, HasFollower: true, FollowerUnavailable: true, MaxLag: 2},
		{Consistency: readConsistencyStrong, LeaderLSN: 106, FollowerLSN: 100, HasFollower: true, MaxLag: 2},
	}

	baseline := make([]readRouteDecision, 0, len(seeded))
	for _, input := range seeded {
		baseline = append(baseline, decideReadRoute(input))
	}

	for round := 0; round < 20; round++ {
		for i, input := range seeded {
			current := decideReadRoute(input)
			if current.Route != baseline[i].Route || current.Lag != baseline[i].Lag || current.FallbackReason != baseline[i].FallbackReason {
				t.Fatalf("non-deterministic decision at round=%d step=%d: got=%+v want=%+v", round, i, current, baseline[i])
			}
		}
	}
}
