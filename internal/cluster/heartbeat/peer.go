package heartbeat

import "context"

// Peer identifies a cluster node by its unique ID and network address.
type Peer struct {
	NodeID        string
	Address       string // cluster gRPC address (used for heartbeat / replication)
	PgwireAddress string // pgwire (SQL client) address; may be empty for static peers
}

// LeadershipProbe contains leadership state returned by a remote peer.
type LeadershipProbe struct {
	LeaderID     string
	Term         uint64
	FencingToken string
	LeaseActive  bool
	LSN          uint64
	// Peers carries the responding node's peer list for address gossip.
	// Receivers should call AddPeer / update PgwireAddress for entries they
	// don't already know.
	Peers []Peer
}

// PeerProber is the port for querying cluster peers' health and state.
// Implementations are transport-specific (e.g. gRPC adapter).
type PeerProber interface {
	// ProbeLeadership queries a peer's leadership state for a domain group.
	// Returns an error if the peer is unreachable.
	ProbeLeadership(ctx context.Context, addr, group string) (LeadershipProbe, error)

	// ProbeLSN queries a peer's last committed WAL LSN.
	// Returns an error if the peer is unreachable.
	ProbeLSN(ctx context.Context, addr string) (uint64, error)
}
