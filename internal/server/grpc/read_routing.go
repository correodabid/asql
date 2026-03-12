package grpc

import "sync"

type readConsistency string

const (
	readConsistencyStrong       readConsistency = "strong"
	readConsistencyBoundedStale readConsistency = "bounded-stale"
)

type readRoute string

const (
	readRouteLeader   readRoute = "leader"
	readRouteFollower readRoute = "follower"
)

const (
	fallbackReasonNone                 = ""
	fallbackReasonStrongConsistency    = "consistency_strong"
	fallbackReasonFollowerUnavailable  = "follower_unavailable"
	fallbackReasonNoFollowerConfigured = "no_follower"
	fallbackReasonLagExceeded          = "lag_exceeded"
)

type readRouteInput struct {
	Consistency         readConsistency
	LeaderLSN           uint64
	FollowerLSN         uint64
	HasFollower         bool
	FollowerUnavailable bool
	MaxLag              uint64
}

type readRouteDecision struct {
	Route          readRoute
	Lag            uint64
	FallbackReason string
}

func normalizeReadConsistency(value string) readConsistency {
	switch value {
	case string(readConsistencyBoundedStale):
		return readConsistencyBoundedStale
	default:
		return readConsistencyStrong
	}
}

func decideReadRoute(input readRouteInput) readRouteDecision {
	lag := uint64(0)
	if input.LeaderLSN > input.FollowerLSN {
		lag = input.LeaderLSN - input.FollowerLSN
	}

	if input.Consistency != readConsistencyBoundedStale {
		return readRouteDecision{Route: readRouteLeader, Lag: lag, FallbackReason: fallbackReasonStrongConsistency}
	}

	if input.FollowerUnavailable {
		return readRouteDecision{Route: readRouteLeader, Lag: lag, FallbackReason: fallbackReasonFollowerUnavailable}
	}

	if !input.HasFollower {
		return readRouteDecision{Route: readRouteLeader, Lag: lag, FallbackReason: fallbackReasonNoFollowerConfigured}
	}

	if lag <= input.MaxLag {
		return readRouteDecision{Route: readRouteFollower, Lag: lag, FallbackReason: fallbackReasonNone}
	}

	return readRouteDecision{Route: readRouteLeader, Lag: lag, FallbackReason: fallbackReasonLagExceeded}
}

type readRoutingMetricInput struct {
	Consistency  readConsistency
	Decision     readRouteDecision
	HasFollower  bool
	MaxLag       uint64
	LeaderLSN    uint64
	FollowerLSN  uint64
	RequestRoute string
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
	case readConsistencyBoundedStale:
		stats.inc("consistency_bounded_stale")
	default:
		stats.inc("consistency_strong")
	}

	switch input.Decision.Route {
	case readRouteFollower:
		stats.inc("route_follower")
	default:
		stats.inc("route_leader")
	}

	if input.Decision.Lag <= input.MaxLag {
		stats.inc("lag_within_threshold")
	} else {
		stats.inc("lag_exceeded_threshold")
	}

	switch input.Decision.FallbackReason {
	case fallbackReasonStrongConsistency:
		stats.inc("fallback_strong_consistency")
	case fallbackReasonFollowerUnavailable:
		stats.inc("fallback_follower_unavailable")
	case fallbackReasonNoFollowerConfigured:
		stats.inc("fallback_no_follower")
	case fallbackReasonLagExceeded:
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
