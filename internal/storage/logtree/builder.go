package logtree

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	// DefaultPollInterval is how often the builder looks for new committed
	// records.
	DefaultPollInterval = 50 * time.Millisecond
	// batchSize bounds the leaves read and appended in one step.
	batchSize = 8192
)

// ErrUnavailable reports a tree the builder cannot build: the WAL does not
// hold every record from where the tree ends (ADR 0008: the tree starts at
// LSN 1), or its records disagree with the tree.
var ErrUnavailable = errors.New("log tree: unavailable")

// Source exposes the committed WAL to the builder.
type Source struct {
	// CommittedLSN returns the newest LSN that can no longer be replaced.
	CommittedLSN func() uint64
	// Leaves returns the ledger leaves at LSNs from..to in order, fewer if
	// the WAL ends first. A first leaf that is not from's is an error.
	Leaves func(ctx context.Context, from, to uint64) ([]Hash, error)
}

// Builder follows the committed WAL and appends its leaves to the tree, off
// the commit path. Once the tree is unavailable it stops for good and says
// why; the node keeps working without it.
type Builder struct {
	tree   *Tree
	source Source
	poll   time.Duration
	logger *slog.Logger

	mu      sync.Mutex
	err     error // set once the tree is unavailable
	warned  bool  // a transient error was logged
	checked bool  // the tree's tail was compared with the WAL
}

// NewBuilder returns a builder that extends tree from source. poll 0 means
// DefaultPollInterval.
func NewBuilder(tree *Tree, source Source, poll time.Duration, logger *slog.Logger) (*Builder, error) {
	if tree == nil || source.CommittedLSN == nil || source.Leaves == nil {
		return nil, errors.New("log tree builder: tree and source are required")
	}
	if poll <= 0 {
		poll = DefaultPollInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Builder{tree: tree, source: source, poll: poll, logger: logger}, nil
}

// Err returns why the tree is unavailable, or nil.
func (b *Builder) Err() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

// Run extends the tree until ctx is done or the tree becomes unavailable.
func (b *Builder) Run(ctx context.Context) {
	ticker := time.NewTicker(b.poll)
	defer ticker.Stop()
	for {
		if err := b.CatchUp(ctx); err != nil {
			if b.Err() != nil || ctx.Err() != nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// CatchUp extends the tree to the committed LSN. A transient error (a
// segment compacted while it was read) is logged once and retried on the
// next call; one that makes the tree unavailable stops the builder.
func (b *Builder) CatchUp(ctx context.Context) error {
	if err := b.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	checked := b.checked
	b.mu.Unlock()
	if !checked {
		if err := b.checkTail(ctx); err != nil {
			return err
		}
		b.mu.Lock()
		b.checked = true
		b.mu.Unlock()
	}
	for {
		size := uint64(b.tree.Size())
		committed := b.source.CommittedLSN()
		if committed <= size {
			b.mu.Lock()
			b.warned = false
			b.mu.Unlock()
			return nil
		}
		to := min(committed, size+batchSize)
		leaves, err := b.source.Leaves(ctx, size+1, to)
		if err != nil {
			return b.transient(err)
		}
		if len(leaves) == 0 {
			return b.unavailable(fmt.Errorf("the WAL does not hold LSN %d, which the tree needs next", size+1))
		}
		if err := b.tree.Append(leaves); err != nil {
			return b.transient(err)
		}
	}
}

// checkTail compares the tree's last leaf with the WAL's record at that LSN
// when the WAL still holds it, so a tree left from another WAL is refused.
func (b *Builder) checkTail(ctx context.Context) error {
	size := uint64(b.tree.Size())
	if size == 0 {
		return nil
	}
	leaves, err := b.source.Leaves(ctx, size, size)
	if err != nil || len(leaves) == 0 {
		// Not retained (GC holds back to the tree, so it was cut before
		// the tree was built) or not readable now: nothing to compare.
		return nil
	}
	stored, err := b.tree.Leaf(size)
	if err != nil {
		return b.transient(err)
	}
	if stored != leaves[0] {
		return b.unavailable(fmt.Errorf("the tree's leaf at LSN %d does not match the WAL record", size))
	}
	return nil
}

func (b *Builder) transient(err error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.warned {
		b.logger.Warn("ledger: log tree builder will retry", "error", err.Error())
		b.warned = true
	}
	return err
}

func (b *Builder) unavailable(err error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = fmt.Errorf("%w: %v", ErrUnavailable, err)
	b.logger.Warn("ledger: log tree disabled; digests stay v1 and proofs are refused", "error", err.Error())
	return b.err
}
