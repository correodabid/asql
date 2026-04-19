package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/correodabid/asql/internal/cluster/heartbeat"

	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const defaultProbeTimeout = time.Second

// GRPCPeerProber implements heartbeat.PeerProber using a per-peer persistent
// connection pool. Connections are established lazily, kept alive with gRPC
// keepalives, and evicted on error so the next probe triggers a fresh dial.
type GRPCPeerProber struct {
	authToken    string
	probeTimeout time.Duration

	mu   sync.Mutex
	pool map[string]*grpcgo.ClientConn
}

// NewGRPCPeerProber creates a prober with a persistent connection pool per peer.
// probeTimeout controls the per-RPC deadline; 0 applies the default (1s).
func NewGRPCPeerProber(authToken string, probeTimeout time.Duration) *GRPCPeerProber {
	if probeTimeout <= 0 {
		probeTimeout = defaultProbeTimeout
	}
	return &GRPCPeerProber{
		authToken:    authToken,
		probeTimeout: probeTimeout,
		pool:         make(map[string]*grpcgo.ClientConn),
	}
}

// ProbeLeadership queries a peer's leadership state for a domain group.
func (p *GRPCPeerProber) ProbeLeadership(ctx context.Context, addr, group string) (heartbeat.LeadershipProbe, error) {
	ctx, cancel := context.WithTimeout(ctx, p.probeTimeout)
	defer cancel()

	conn, err := p.conn(addr)
	if err != nil {
		return heartbeat.LeadershipProbe{}, fmt.Errorf("peer conn %s: %w", addr, err)
	}

	resp := new(LeadershipStateResponse)
	if err := conn.Invoke(ctx, "/asql.v1.ASQLService/LeadershipState",
		&LeadershipStateRequest{Group: group}, resp, grpcgo.ForceCodec(peerCodec{})); err != nil {
		p.evict(addr) // drop conn on RPC failure; next probe re-dials
		return heartbeat.LeadershipProbe{}, fmt.Errorf("leadership state rpc: %w", err)
	}

	gossipPeers := make([]heartbeat.Peer, 0, len(resp.Peers))
	for _, p := range resp.Peers {
		if peer, ok := PeerFromInfo(p); ok {
			gossipPeers = append(gossipPeers, peer)
		}
	}

	return heartbeat.LeadershipProbe{
		LeaderID:     resp.LeaderID,
		Term:         resp.Term,
		FencingToken: resp.FencingToken,
		LeaseActive:  resp.LeaseActive,
		LSN:          resp.LastLeaderLSN,
		Peers:        gossipPeers,
	}, nil
}

// ProbeLSN queries a peer's last committed WAL LSN.
func (p *GRPCPeerProber) ProbeLSN(ctx context.Context, addr string) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, p.probeTimeout)
	defer cancel()

	conn, err := p.conn(addr)
	if err != nil {
		return 0, fmt.Errorf("peer conn %s: %w", addr, err)
	}

	resp := new(LastLSNResponse)
	if err := conn.Invoke(ctx, "/asql.v1.ReplicationService/LastLSN",
		&LastLSNRequest{}, resp, grpcgo.ForceCodec(peerCodec{})); err != nil {
		p.evict(addr)
		return 0, fmt.Errorf("last lsn rpc: %w", err)
	}

	return resp.LSN, nil
}

// JoinCluster calls the JoinCluster RPC on the node at addr, announcing nodeID
// and grpcAddress as the new peer. Returns the seed's known peers + leader info.
func (p *GRPCPeerProber) JoinCluster(ctx context.Context, addr string, req *JoinClusterRequest) (*JoinClusterResponse, error) {
	conn, err := p.conn(addr)
	if err != nil {
		return nil, fmt.Errorf("peer conn %s: %w", addr, err)
	}

	resp := new(JoinClusterResponse)
	if err := conn.Invoke(ctx, "/asql.v1.ASQLService/JoinCluster",
		req, resp, grpcgo.ForceCodec(peerCodec{})); err != nil {
		p.evict(addr)
		return nil, fmt.Errorf("join cluster rpc: %w", err)
	}
	return resp, nil
}

// PeerFromInfo converts a PeerInfo wire type to a heartbeat.Peer.
// Returns (peer, false) when either field is empty.
func PeerFromInfo(p PeerInfo) (heartbeat.Peer, bool) {
	if p.NodeID == "" || p.Address == "" {
		return heartbeat.Peer{}, false
	}
	return heartbeat.Peer{NodeID: p.NodeID, Address: p.Address, PgwireAddress: p.PgwireAddress}, true
}

// Close closes all pooled connections. Must be called when the prober is no longer needed.
func (p *GRPCPeerProber) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, conn := range p.pool {
		_ = conn.Close()
		delete(p.pool, addr)
	}
}

// conn returns a healthy pooled connection for addr, dialing lazily when absent
// or evicting and re-dialing when the connection is in a terminal failure state.
func (p *GRPCPeerProber) conn(addr string) (*grpcgo.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, ok := p.pool[addr]; ok {
		switch existing.GetState() {
		case connectivity.Shutdown, connectivity.TransientFailure:
			_ = existing.Close()
			delete(p.pool, addr)
		default:
			return existing, nil
		}
	}

	conn, err := p.dial(addr)
	if err != nil {
		return nil, err
	}
	p.pool[addr] = conn
	return conn, nil
}

// evict removes and closes the pooled connection for addr.
func (p *GRPCPeerProber) evict(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.pool[addr]; ok {
		_ = conn.Close()
		delete(p.pool, addr)
	}
}

// dial creates a new gRPC connection. The call is non-blocking; the TCP
// handshake happens lazily on first use.
func (p *GRPCPeerProber) dial(addr string) (*grpcgo.ClientConn, error) {
	opts := []grpcgo.DialOption{
		grpcgo.WithTransportCredentials(insecure.NewCredentials()),
		grpcgo.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second, // ping if idle >10 s
			Timeout:             3 * time.Second,  // abort if no pong within 3 s
			PermitWithoutStream: true,             // keepalives even without active RPCs
		}),
	}
	if p.authToken != "" {
		opts = append(opts, grpcgo.WithPerRPCCredentials(tokenCredentials{token: p.authToken}))
	}
	//nolint:staticcheck
	return grpcgo.Dial(addr, opts...)
}

// peerCodec is a JSON codec for peer probing RPCs.
type peerCodec struct{}

func (peerCodec) Name() string                          { return "json" }
func (peerCodec) Marshal(v interface{}) ([]byte, error) { return json.Marshal(v) }
func (peerCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// tokenCredentials attaches a bearer token to RPCs.
type tokenCredentials struct {
	token string
}

func (t tokenCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + t.token}, nil
}

func (t tokenCredentials) RequireTransportSecurity() bool {
	return false
}
