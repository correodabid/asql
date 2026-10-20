package pgwire

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/ports"
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

	// For result attestations (ADR 0006).
	store    *wal.SegmentedLogStore
	signer   ports.LedgerSigner
	nodeID   string
	recordAt func(ctx context.Context, lsn uint64) (term, timestamp uint64, err error)
	now      func() time.Time

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
	recordAt := func(ctx context.Context, lsn uint64) (uint64, uint64, error) {
		records, err := store.ReadFrom(ctx, lsn, 1)
		if err != nil {
			return 0, 0, err
		}
		if len(records) == 0 || records[0].LSN != lsn {
			return 0, 0, fmt.Errorf("WAL record %d not found", lsn)
		}
		return records[0].Term, records[0].Timestamp, nil
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
			RecordAt:     recordAt,
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
	return &ledgerSigning{
		log: digestLog, checkpointer: checkpointer, logger: config.Logger,
		store: store, signer: signer, nodeID: config.NodeID, recordAt: recordAt, now: time.Now,
	}, nil
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

// digest returns the latest digest, or with lsn > 0 the oldest one covering
// lsn.
func (ls *ledgerSigning) digest(lsn uint64) (ledgerdigest.Digest, bool, error) {
	if lsn == 0 {
		d, ok := ls.log.Latest()
		return d, ok, nil
	}
	return ls.log.Covering(lsn)
}

// handleAdminLedgerDigest serves GET /api/v1/ledger/digest[?lsn=N]: the
// latest signed digest, or the oldest one covering LSN N.
func (server *Server) handleAdminLedgerDigest(w http.ResponseWriter, r *http.Request) {
	if server.ledger == nil {
		writeAdminJSON(w, http.StatusNotFound, map[string]string{"error": "ledger digests are not enabled (ledger mode is not signed)"})
		return
	}
	var lsn uint64
	if raw := r.URL.Query().Get("lsn"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 {
			writeAdminJSON(w, http.StatusBadRequest, map[string]string{"error": "lsn must be a positive integer"})
			return
		}
		lsn = parsed
	}
	d, ok, err := server.ledger.digest(lsn)
	if err != nil {
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		msg := "no digest signed yet"
		if lsn > 0 {
			msg = fmt.Sprintf("no digest covers LSN %d yet", lsn)
		}
		writeAdminJSON(w, http.StatusNotFound, map[string]string{"error": msg})
		return
	}
	writeAdminJSON(w, http.StatusOK, d)
}

var reLedgerDigestLSN = regexp.MustCompile(`(?i)\blsn\s*>=\s*(\d+)`)

var ledgerDigestColumns = []string{"version", "lsn", "chain", "term", "timestamp", "node_id", "key_id", "signed_at", "sig"}

// adminLedgerDigest serves SELECT * FROM asql_admin.ledger_digest: one row
// with the latest digest, or with WHERE lsn >= N the oldest one covering N.
// It is answered here, outside the engine, so digests never reach the WAL.
func (server *Server) adminLedgerDigest(sql string) (interceptResult, bool) {
	empty := interceptResult{result: executor.Result{Status: "OK"}, columns: ledgerDigestColumns}
	if server.ledger == nil {
		return empty, true
	}
	var lsn uint64
	if m := reLedgerDigestLSN.FindStringSubmatch(sql); m != nil {
		lsn, _ = strconv.ParseUint(m[1], 10, 64)
	}
	d, ok, err := server.ledger.digest(lsn)
	if err != nil {
		return interceptResult{result: executor.Result{Status: fmt.Sprintf("ERROR: %v", err)}}, true
	}
	if !ok {
		return empty, true
	}
	row := map[string]ast.Literal{
		"version":   litU(uint64(d.Version)),
		"lsn":       litU(d.LSN),
		"chain":     litS(hex.EncodeToString(d.Chain[:])),
		"term":      litU(d.Term),
		"timestamp": litU(d.Timestamp),
		"node_id":   litS(d.NodeID),
		"key_id":    litS(d.KeyID),
		"signed_at": litS(d.SignedAt.UTC().Format(time.RFC3339Nano)),
		"sig":       litS(base64.StdEncoding.EncodeToString(d.Sig)),
	}
	return interceptResult{result: executor.Result{Status: "OK", Rows: []map[string]ast.Literal{row}}, columns: ledgerDigestColumns}, true
}
