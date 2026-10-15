package pgwire

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/platform/datadir"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/wal"
)

// ledgerSigning runs ledger=signed (ADR 0005): a checkpointer signs the
// committed chain head into the sidecar digest log, off the commit path.
type ledgerSigning struct {
	log          *ledgerdigest.Log
	checkpointer *ledgerdigest.Checkpointer
	logger       *slog.Logger

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	stopped bool
}

func newLedgerSigning(config Config, dd *datadir.DataDir, engine *executor.Engine, store *wal.SegmentedLogStore, signer *ledgerdigest.Ed25519Signer) (*ledgerSigning, error) {
	digestLog, err := ledgerdigest.OpenLog(dd.LedgerDir(), 0)
	if err != nil {
		return nil, err
	}
	checkpointer, err := ledgerdigest.NewCheckpointer(ledgerdigest.CheckpointerConfig{
		NodeID: config.NodeID,
		Signer: signer,
		Log:    digestLog,
		Source: ledgerdigest.Source{
			// The engine applies only committed entries (Raft delivers them
			// once a quorum holds them), so its head never covers a WAL tail
			// that a new leader could still replace.
			CommittedLSN: engine.HeadLSN,
			ChainAt:      store.LedgerChainAt,
			RecordAt: func(ctx context.Context, lsn uint64) (uint64, uint64, error) {
				records, err := store.ReadFrom(ctx, lsn, 1)
				if err != nil {
					return 0, 0, err
				}
				if len(records) == 0 || records[0].LSN != lsn {
					return 0, 0, fmt.Errorf("WAL record %d not found", lsn)
				}
				return records[0].Term, records[0].Timestamp, nil
			},
		},
		Interval:  config.LedgerDigestInterval,
		EveryLSNs: config.LedgerDigestEveryLSNs,
		Logger:    config.Logger,
	})
	if err != nil {
		_ = digestLog.Close()
		return nil, err
	}
	config.Logger.Info("ledger: signing digests", "key_id", signer.KeyID(), "dir", dd.LedgerDir())
	return &ledgerSigning{log: digestLog, checkpointer: checkpointer, logger: config.Logger}, nil
}

func (ls *ledgerSigning) start() {
	if ls == nil {
		return
	}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.cancel != nil || ls.stopped {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	ls.cancel, ls.done = cancel, make(chan struct{})
	go func() {
		defer close(ls.done)
		ls.checkpointer.Run(ctx)
	}()
}

// stop signs the final committed head and closes the digest log. It must run
// before the WAL store is closed.
func (ls *ledgerSigning) stop() {
	if ls == nil {
		return
	}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.stopped {
		return
	}
	ls.stopped = true
	if ls.cancel != nil {
		ls.cancel()
		<-ls.done
	} else if _, _, err := ls.checkpointer.Checkpoint(context.Background()); err != nil {
		ls.logger.Warn("ledger: final digest failed", "error", err.Error())
	}
	_ = ls.log.Close()
}
