package main

import "sync"

type readRoutingMetricInput struct {
	Consistency         ReadConsistency
	Decision            ReadRouteDecision
	HasFollower         bool
	MaxLag              uint64
	FollowerUnavailable bool
}

type readRoutingStats struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func newReadRoutingStats() *readRoutingStats {
	return &readRoutingStats{counts: map[string]uint64{}}
}

func (stats *readRoutingStats) inc(name string) {
	if stats == nil {
		return
	}
	stats.counts[name] = stats.counts[name] + 1
}

func (stats *readRoutingStats) record(input readRoutingMetricInput) {
	if stats == nil {
		return
	}

	stats.mu.Lock()
	defer stats.mu.Unlock()

	stats.inc("requests_total")

	switch input.Consistency {
	case ReadConsistencyBoundedStale:
		stats.inc("consistency_bounded_stale")
	default:
		stats.inc("consistency_strong")
	}

	switch input.Decision.Route {
	case ReadRouteFollower:
		stats.inc("route_follower")
	default:
		stats.inc("route_leader")
	}

	if input.Consistency != ReadConsistencyBoundedStale {
		return
	}

	if input.FollowerUnavailable {
		stats.inc("fallback_follower_unavailable")
	}

	if !input.HasFollower {
		stats.inc("fallback_no_follower")
		return
	}

	if input.Decision.Lag <= input.MaxLag {
		stats.inc("lag_within_threshold")
		if input.Decision.Route == ReadRouteFollower {
			stats.inc("served_within_threshold")
		}
		return
	}

	stats.inc("lag_exceeded_threshold")
	if input.Decision.Route == ReadRouteLeader {
		stats.inc("fallback_lag_exceeded")
	}
}

func (stats *readRoutingStats) snapshot() map[string]uint64 {
	if stats == nil {
		return map[string]uint64{}
	}

	stats.mu.Lock()
	defer stats.mu.Unlock()

	result := make(map[string]uint64, len(stats.counts))
	for key, value := range stats.counts {
		result[key] = value
	}
	return result
}
