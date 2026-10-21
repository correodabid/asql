package pgwire

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/correodabid/asql/internal/engine/executor"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/ports"
	"github.com/correodabid/asql/internal/engine/sqlerr"
	"github.com/correodabid/asql/internal/platform/datadir"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
	"github.com/correodabid/asql/internal/storage/logtree"
	"github.com/correodabid/asql/internal/storage/wal"
)

// ledgerSigning runs ledger=signed (ADR 0005): a checkpointer signs the
// committed chain head into the sidecar digest log, off the commit path.
// Each checkpoint first extends the log tree (ADR 0008) and signs its root
// too, as a version 2 digest.
type ledgerSigning struct {
	log          *ledgerdigest.Log
	checkpointer *ledgerdigest.Checkpointer
	logger       *slog.Logger
	tree         *logtree.Tree
	treeBuilder  *logtree.Builder
	// stateKey salts the state tree's leaves (ADR 0008). It is nil when the
	// node has none; VERIFIABLE tables then cannot be proven.
	stateKey *ledgerdigest.StateKey
	// stateTree keeps the state tree when there is a state key.
	stateTree *stateTreeBuilder

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

// loadLedgerStateKey loads the state key (ADR 0008). A file named in the
// config must load; the default one next to the signing key may be missing,
// as on nodes set up before ADR 0008, and the node then starts without it.
func loadLedgerStateKey(config Config) (*ledgerdigest.StateKey, error) {
	path := config.LedgerStateKeyFile
	if path == "" {
		path = ledgerdigest.DefaultStateKeyPath(config.LedgerKeyFile)
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			config.Logger.Warn("ledger: no state key; rows of VERIFIABLE tables cannot be proven until one exists (asqlctl -command ledger-keygen -output <the -ledger-key file> writes it)",
				"path", path)
			return nil, nil
		}
	}
	key, err := ledgerdigest.LoadStateKey(path)
	if err != nil {
		return nil, err
	}
	return &key, nil
}

func newLedgerSigning(config Config, dd *datadir.DataDir, engine *executor.Engine, store *wal.SegmentedLogStore, signer *ledgerdigest.Ed25519Signer, stateKey *ledgerdigest.StateKey) (*ledgerSigning, error) {
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
	tree, err := logtree.Open(dd.LedgerDir())
	if err != nil {
		_ = digestLog.Close()
		return nil, err
	}
	treeBuilder, err := logtree.NewBuilder(tree, logtree.Source{
		CommittedLSN: engine.HeadLSN,
		Leaves: func(ctx context.Context, from, to uint64) ([]logtree.Hash, error) {
			leaves, err := store.LedgerLeaves(ctx, from, to)
			out := make([]logtree.Hash, len(leaves))
			for i, leaf := range leaves {
				out[i] = logtree.Hash(leaf)
			}
			return out, err
		},
	}, 0, config.Logger)
	if err != nil {
		_ = tree.Close()
		_ = digestLog.Close()
		return nil, err
	}
	// WAL GC never removes records the tree has not added: the tree starts
	// at LSN 1 and is never rebuilt from a truncated WAL.
	engine.SetWALGCFloor(func() uint64 {
		if treeBuilder.Err() != nil {
			return math.MaxUint64
		}
		return uint64(tree.Size()) + 1
	})
	// With a state key, digests also sign the state tree of the
	// VERIFIABLE tables (version 3).
	var stateTree *stateTreeBuilder
	var stateSource func(ctx context.Context) (uint64, [32]byte, bool, error)
	if stateKey != nil {
		stateTree = newStateTreeBuilder(engine, *stateKey)
		stateSource = stateTree.State
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
			TreeRoot: func(ctx context.Context, lsn uint64) ([32]byte, bool, error) {
				root, ok, err := treeBuilder.RootAt(ctx, lsn)
				return [32]byte(root), ok, err
			},
			State: stateSource,
		},
		Interval:  config.LedgerDigestInterval,
		EveryLSNs: config.LedgerDigestEveryLSNs,
		Logger:    config.Logger,
	})
	if err != nil {
		_ = tree.Close()
		_ = digestLog.Close()
		return nil, err
	}
	config.Logger.Info("ledger: signing digests", "key_id", signer.KeyID(), "dir", dd.LedgerDir(), "state_key", stateKey != nil)
	return &ledgerSigning{
		log: digestLog, checkpointer: checkpointer, logger: config.Logger, tree: tree, treeBuilder: treeBuilder, stateKey: stateKey, stateTree: stateTree,
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
	_ = ls.tree.Close()
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

var ledgerDigestColumns = []string{"version", "lsn", "chain", "term", "timestamp", "node_id", "key_id", "signed_at", "sig", "tree_root", "state_root"}

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
	treeRoot, stateRoot := ast.Literal{Kind: ast.LiteralNull}, ast.Literal{Kind: ast.LiteralNull}
	if d.HasTree() {
		treeRoot = litS(hex.EncodeToString(d.TreeRoot[:]))
	}
	if d.HasState() {
		stateRoot = litS(hex.EncodeToString(d.StateRoot[:]))
	}
	row := map[string]ast.Literal{
		"version":    litU(uint64(d.Version)),
		"lsn":        litU(d.LSN),
		"chain":      litS(hex.EncodeToString(d.Chain[:])),
		"tree_root":  treeRoot,
		"state_root": stateRoot,
		"term":       litU(d.Term),
		"timestamp":  litU(d.Timestamp),
		"node_id":    litS(d.NodeID),
		"key_id":     litS(d.KeyID),
		"signed_at":  litS(d.SignedAt.UTC().Format(time.RFC3339Nano)),
		"sig":        litS(base64.StdEncoding.EncodeToString(d.Sig)),
	}
	return interceptResult{result: executor.Result{Status: "OK", Rows: []map[string]ast.Literal{row}}, columns: ledgerDigestColumns}, true
}

// Consistency proofs (ADR 0008): SELECT * FROM asql_admin.prove_consistency
// WHERE from_lsn = A [AND to_lsn = B] proves that the log signed at B
// extends the log signed at A. Proofs exist between signed checkpoints only.
var (
	errConsistency    = sqlerr.New("22023", "prove_consistency")
	errConsistencyOff = sqlerr.New("55000", "prove_consistency: consistency proofs need ledger mode signed")

	reConsistencyFromLSN = regexp.MustCompile(`(?i)\bfrom_lsn\s*=\s*(\d+)`)
	reConsistencyToLSN   = regexp.MustCompile(`(?i)\bto_lsn\s*=\s*(\d+)`)
)

// proveConsistencyColumns are named as the JSON fields of
// ledgerdigest.ConsistencyProof; each holds that field's JSON.
var proveConsistencyColumns = []string{"from_digest", "to_digest", "proof"}

func (server *Server) adminProveConsistency(sql string) (interceptResult, bool) {
	row, err := server.proveConsistency(sql)
	if err != nil {
		return interceptResult{err: err}, true
	}
	return interceptResult{result: executor.Result{Status: "OK", Rows: []map[string]ast.Literal{row}}, columns: proveConsistencyColumns}, true
}

func (server *Server) proveConsistency(sql string) (map[string]ast.Literal, error) {
	if server.ledger == nil {
		return nil, errConsistencyOff
	}
	m := reConsistencyFromLSN.FindStringSubmatch(sql)
	if m == nil {
		return nil, fmt.Errorf("%w: WHERE from_lsn = <LSN of a signed digest> is required", errConsistency)
	}
	fromLSN, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: from_lsn: %v", errConsistency, err)
	}
	var toLSN uint64
	if m := reConsistencyToLSN.FindStringSubmatch(sql); m != nil {
		if toLSN, err = strconv.ParseUint(m[1], 10, 64); err != nil {
			return nil, fmt.Errorf("%w: to_lsn: %v", errConsistency, err)
		}
	}
	p, err := server.ledger.consistency(fromLSN, toLSN)
	if err != nil {
		return nil, err
	}
	from, err := json.Marshal(p.From)
	if err != nil {
		return nil, err
	}
	to, err := json.Marshal(p.To)
	if err != nil {
		return nil, err
	}
	proof, err := json.Marshal(ledgerdigest.EncodeHashes(p.Proof))
	if err != nil {
		return nil, err
	}
	return map[string]ast.Literal{"from_digest": litS(string(from)), "to_digest": litS(string(to)), "proof": litS(string(proof))}, nil
}

// consistency proves that the digest signed at toLSN (0: the latest)
// extends the one signed at fromLSN.
func (ls *ledgerSigning) consistency(fromLSN, toLSN uint64) (ledgerdigest.ConsistencyProof, error) {
	from, err := ls.digestAt(fromLSN)
	if err != nil {
		return ledgerdigest.ConsistencyProof{}, err
	}
	var to ledgerdigest.Digest
	if toLSN == 0 {
		latest, ok := ls.log.Latest()
		if !ok {
			return ledgerdigest.ConsistencyProof{}, fmt.Errorf("%w: no digest signed yet", errConsistency)
		}
		to = latest
	} else if to, err = ls.digestAt(toLSN); err != nil {
		return ledgerdigest.ConsistencyProof{}, err
	}
	if from.LSN > to.LSN {
		return ledgerdigest.ConsistencyProof{}, fmt.Errorf("%w: from_lsn %d is after to_lsn %d", errConsistency, from.LSN, to.LSN)
	}
	for _, d := range []ledgerdigest.Digest{from, to} {
		if !d.HasTree() {
			return ledgerdigest.ConsistencyProof{}, fmt.Errorf("%w: the digest at LSN %d is version 1 and signs no log tree root", errConsistency, d.LSN)
		}
	}
	proof, err := ls.tree.ConsistencyProof(int64(from.LSN), int64(to.LSN))
	if err != nil {
		return ledgerdigest.ConsistencyProof{}, err
	}
	return ledgerdigest.ConsistencyProof{From: from, To: to, Proof: proof}, nil
}

// digestAt returns the digest signed at exactly lsn.
func (ls *ledgerSigning) digestAt(lsn uint64) (ledgerdigest.Digest, error) {
	if lsn == 0 {
		return ledgerdigest.Digest{}, fmt.Errorf("%w: LSN must be positive", errConsistency)
	}
	d, ok, err := ls.log.Covering(lsn)
	if err != nil {
		return ledgerdigest.Digest{}, err
	}
	if !ok {
		return ledgerdigest.Digest{}, fmt.Errorf("%w: no digest was signed at LSN %d or after it", errConsistency, lsn)
	}
	if d.LSN != lsn {
		return ledgerdigest.Digest{}, fmt.Errorf("%w: no digest was signed at LSN %d; proofs exist between signed digests only, and the next one is at LSN %d", errConsistency, lsn, d.LSN)
	}
	return d, nil
}
