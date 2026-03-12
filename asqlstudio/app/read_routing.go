package studioapp

type ReadConsistency string

const (
	ReadConsistencyStrong       ReadConsistency = "strong"
	ReadConsistencyBoundedStale ReadConsistency = "bounded-stale"
)

type ReadRoute string

const (
	ReadRouteLeader   ReadRoute = "leader"
	ReadRouteFollower ReadRoute = "follower"
)

type ReadRouteInput struct {
	Consistency ReadConsistency
	LeaderLSN   uint64
	FollowerLSN uint64
	HasFollower bool
	MaxLag      uint64
}

type ReadRouteDecision struct {
	Route ReadRoute
	Lag   uint64
}

func normalizeReadConsistency(value string) ReadConsistency {
	switch value {
	case string(ReadConsistencyBoundedStale):
		return ReadConsistencyBoundedStale
	default:
		return ReadConsistencyStrong
	}
}

func DecideReadRoute(input ReadRouteInput) ReadRouteDecision {
	lag := uint64(0)
	if input.LeaderLSN > input.FollowerLSN {
		lag = input.LeaderLSN - input.FollowerLSN
	}

	if input.Consistency != ReadConsistencyBoundedStale {
		return ReadRouteDecision{Route: ReadRouteLeader, Lag: lag}
	}

	if !input.HasFollower {
		return ReadRouteDecision{Route: ReadRouteLeader, Lag: lag}
	}

	if lag <= input.MaxLag {
		return ReadRouteDecision{Route: ReadRouteFollower, Lag: lag}
	}

	return ReadRouteDecision{Route: ReadRouteLeader, Lag: lag}
}
