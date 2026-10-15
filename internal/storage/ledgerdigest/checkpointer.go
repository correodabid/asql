package ledgerdigest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/correodabid/asql/internal/engine/ports"
)

const (
	// DefaultInterval is the longest a committed head waits to be signed.
	DefaultInterval = time.Second
	// DefaultEveryLSNs signs early once this many LSNs are committed after
	// the last digest.
	DefaultEveryLSNs = 10_000

	// pollInterval bounds how late the count-based trigger fires.
	pollInterval = 50 * time.Millisecond
)

// Source exposes the committed ledger state to sign.
type Source struct {
	// CommittedLSN returns the newest LSN that is committed, and so can no
	// longer be replaced by Raft. On a node that is the engine's applied
	// head.
	CommittedLSN func() uint64
	// ChainAt returns the ledger chain value at lsn while the WAL store
	// still holds it in memory.
	ChainAt func(lsn uint64) ([32]byte, bool)
	// RecordAt returns the term and logical timestamp of the WAL record at
	// lsn.
	RecordAt func(ctx context.Context, lsn uint64) (term, timestamp uint64, err error)
}

// CheckpointerConfig configures a Checkpointer.
type CheckpointerConfig struct {
	NodeID    string
	Signer    ports.LedgerSigner
	Log       *Log
	Source    Source
	Interval  time.Duration // 0 means DefaultInterval
	EveryLSNs uint64        // 0 means DefaultEveryLSNs
	Now       func() time.Time
	Logger    *slog.Logger
}

// Checkpointer signs the committed chain head at checkpoints: at most every
// Interval, or sooner once EveryLSNs LSNs were committed, and only when the
// head has advanced. It never runs on the commit path; signing per commit is
// ruled out by ADR 0005 § Performance.
type Checkpointer struct {
	cfg CheckpointerConfig

	mu         sync.Mutex
	lastLSN    uint64
	lastSigned time.Time
	warned     bool
}

// NewCheckpointer returns a checkpointer that continues after the latest
// digest in cfg.Log.
func NewCheckpointer(cfg CheckpointerConfig) (*Checkpointer, error) {
	if cfg.Signer == nil || cfg.Log == nil || cfg.Source.CommittedLSN == nil || cfg.Source.ChainAt == nil || cfg.Source.RecordAt == nil {
		return nil, errors.New("ledger checkpointer: signer, log and source are required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultInterval
	}
	if cfg.EveryLSNs == 0 {
		cfg.EveryLSNs = DefaultEveryLSNs
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &Checkpointer{cfg: cfg}
	if latest, ok := cfg.Log.Latest(); ok {
		c.lastLSN = latest.LSN
	}
	return c, nil
}

// Run signs checkpoints until ctx is done, then signs the final committed
// head once more so that a clean shutdown leaves its state signed.
func (c *Checkpointer) Run(ctx context.Context) {
	ticker := time.NewTicker(min(pollInterval, c.cfg.Interval))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if _, _, err := c.Checkpoint(context.Background()); err != nil {
				c.cfg.Logger.Warn("ledger: final digest failed", "error", err.Error())
			}
			return
		case <-ticker.C:
			if !c.due() {
				continue
			}
			if _, _, err := c.Checkpoint(ctx); err != nil && ctx.Err() == nil {
				c.cfg.Logger.Warn("ledger: digest failed", "error", err.Error())
			}
		}
	}
}

func (c *Checkpointer) due() bool {
	head := c.cfg.Source.CommittedLSN()
	c.mu.Lock()
	defer c.mu.Unlock()
	if head <= c.lastLSN {
		return false
	}
	return head-c.lastLSN >= c.cfg.EveryLSNs || c.cfg.Now().Sub(c.lastSigned) >= c.cfg.Interval
}

// Checkpoint signs the committed head now if it advanced since the last
// digest, and returns the new digest.
func (c *Checkpointer) Checkpoint(ctx context.Context) (Digest, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lsn := c.cfg.Source.CommittedLSN()
	if lsn == 0 || lsn <= c.lastLSN {
		return Digest{}, false, nil
	}
	chain, ok := c.cfg.Source.ChainAt(lsn)
	if !ok {
		// The WAL's uncommitted tail is longer than the chain values kept
		// in memory. Retry at the next checkpoint.
		if !c.warned {
			c.cfg.Logger.Warn("ledger: chain value at committed LSN unavailable; digest deferred", "lsn", lsn)
			c.warned = true
		}
		return Digest{}, false, nil
	}
	c.warned = false
	term, timestamp, err := c.cfg.Source.RecordAt(ctx, lsn)
	if err != nil {
		return Digest{}, false, fmt.Errorf("read record at LSN %d: %w", lsn, err)
	}
	d, err := Sign(c.cfg.Signer, Digest{
		LSN:       lsn,
		Chain:     chain,
		Term:      term,
		Timestamp: timestamp,
		NodeID:    c.cfg.NodeID,
		SignedAt:  c.cfg.Now().UTC(),
	})
	if err != nil {
		return Digest{}, false, err
	}
	if err := c.cfg.Log.Append(d); err != nil {
		return Digest{}, false, err
	}
	c.lastLSN, c.lastSigned = lsn, c.cfg.Now()
	return d, true, nil
}
