package pgwire

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/correodabid/asql/internal/storage/wal"
)

// Config holds all configuration for the pgwire (PostgreSQL-wire) server.
type Config struct {
	Address         string
	AdminHTTPAddr   string // optional admin HTTP listen address for /metrics, /readyz, /livez
	DataDirPath     string // path to .asql/ data directory
	Logger          *slog.Logger
	AuthToken       string // optional pgwire password and cluster/admin bearer token
	AdminReadToken  string // optional bearer token for read-only admin API endpoints; falls back to AuthToken
	AdminWriteToken string // optional bearer token for mutating admin API endpoints; falls back to AuthToken
	LedgerMode      string // WAL ledger mode (ADR 0005): "off", "chain" or "signed"; empty means "chain"
	// LedgerKeyFile is the node's Ed25519 private key (PEM, PKCS #8) used to
	// sign ledger digests. Required with LedgerMode "signed".
	LedgerKeyFile string
	// LedgerDigestInterval and LedgerDigestEveryLSNs set how often the
	// committed chain head is signed: at most every interval, or sooner after
	// that many LSNs. Zero means the defaults (1s and 10,000).
	LedgerDigestInterval  time.Duration
	LedgerDigestEveryLSNs uint64
	// Compliance starts the server only when every retained WAL record is
	// vouched for by the ledger (ADR 0005): the ledger is on, every segment
	// is anchored and the chain reaches back to genesis.
	Compliance bool

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
	if _, _, err := config.ledgerSettings(); err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	if mode, _, _ := config.ledgerSettings(); config.Compliance && mode == wal.LedgerOff {
		return errors.New("compliance: the ledger cannot be off")
	}
	return nil
}

// checkCompliance refuses a WAL whose history the ledger does not fully
// vouch for. Records in an unanchored segment are still chained, but only
// by a chain recomputed from those same records, so editing them consistently
// goes unnoticed.
func checkCompliance(store *wal.SegmentedLogStore) error {
	head := store.LedgerHead()
	if !head.Valid {
		return errors.New("compliance: the ledger is off")
	}
	if seqs := store.UnanchoredSegments(); len(seqs) > 0 {
		return fmt.Errorf("compliance: %d WAL segments have no ledger anchor (first: %06d), because they were written with the ledger off or before it existed; start without compliance to keep serving them", len(seqs), seqs[0])
	}
	if !head.Rooted {
		return errors.New("compliance: the ledger chain does not reach back to genesis, because WAL history was removed before it was anchored")
	}
	return nil
}

// ledgerSettings resolves the WAL ledger mode. "signed" maintains the chain
// like "chain" and also signs checkpointed digests of the committed head.
func (config Config) ledgerSettings() (mode wal.LedgerMode, signed bool, err error) {
	if config.LedgerMode == "signed" {
		if config.LedgerKeyFile == "" {
			return 0, false, errors.New("ledger mode signed requires a key file")
		}
		return wal.LedgerChain, true, nil
	}
	mode, err = wal.ParseLedgerMode(config.LedgerMode)
	if err != nil {
		return 0, false, fmt.Errorf("invalid ledger mode %q (want off, chain or signed)", config.LedgerMode)
	}
	if config.LedgerKeyFile != "" {
		return 0, false, errors.New("a ledger key file is only used with ledger mode signed")
	}
	return mode, false, nil
}
