package pgwire

import (
	"errors"
	"log/slog"
	"time"
)

// Config holds all configuration for the pgwire (PostgreSQL-wire) server.
type Config struct {
	Address       string
	AdminHTTPAddr string // optional admin HTTP listen address for /metrics, /readyz, /livez
	DataDirPath   string // path to .asql/ data directory
	Logger        *slog.Logger
	AuthToken     string // optional pgwire password and cluster/admin bearer token

	// Cluster fields — optional. When NodeID, Peers, and ClusterGRPCAddr are
	// all set the server starts the production cluster runtime: pgwire + Raft,
	// with a gRPC sidecar on ClusterGRPCAddr for Raft and cluster RPCs.
	//
	// This is the only production cluster path. Legacy heartbeat-led cluster
	// flows in other packages are transitional/non-production.
	NodeID           string        // unique node identifier (e.g. "node-a")
	Peers            []string      // peer cluster gRPC addresses in "nodeID@host:port" format
	Groups           []string      // domain groups for heartbeat monitoring
	ClusterGRPCAddr  string        // TCP address for the cluster gRPC sidecar (e.g. ":6433")
	PeerProbeTimeout time.Duration // per-RPC deadline for heartbeat probes; 0 → default (1s)
	JoinAddr         string        // gRPC address of an existing cluster peer to join at startup (hot join)
}

func (config Config) Validate() error {
	if config.Address == "" {
		return errors.New("address is required")
	}
	if config.DataDirPath == "" {
		return errors.New("data directory path is required")
	}
	if config.Logger == nil {
		return errors.New("logger is required")
	}
	return nil
}
