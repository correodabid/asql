package grpc

import (
	"errors"
	"log/slog"
	"time"
)

type Config struct {
	Address         string
	DataDirPath     string // path to .asql/ data directory
	Logger          *slog.Logger
	AuthToken       string
	TLSCertPath     string
	TLSKeyPath      string
	TLSClientCAPath string

	// Cluster fields — legacy/transitional path. They remain in the config so
	// old internal call sites fail explicitly instead of silently enabling a
	// non-production cluster mode.
	//
	// Standalone gRPC cluster mode is disabled. Production clustering is driven
	// by the pgwire + Raft server path.
	NodeID string   // unique node identifier (e.g. "node-a")
	Peers  []string // peer addresses in "nodeID@host:port" format
	Groups []string // domain groups for heartbeat monitoring

	// PeerProbeTimeout is the per-RPC deadline used by the heartbeat prober
	// when querying peer leadership state and LSN. Defaults to 1 s when zero.
	PeerProbeTimeout time.Duration
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

	mtlsInputs := 0
	if config.TLSCertPath != "" {
		mtlsInputs++
	}
	if config.TLSKeyPath != "" {
		mtlsInputs++
	}
	if config.TLSClientCAPath != "" {
		mtlsInputs++
	}

	if mtlsInputs > 0 && mtlsInputs < 3 {
		return errors.New("mtls requires tls cert path, tls key path, and tls client ca path")
	}

	return nil
}
