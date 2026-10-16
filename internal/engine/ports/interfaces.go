package ports

import (
	"context"
	"time"

	"github.com/correodabid/asql/internal/engine/parser/ast"
)

// Clock provides deterministic-safe time access for the engine core.
type Clock interface {
	Now() time.Time
}

// Entropy provides deterministic-safe random values via injected source.
type Entropy interface {
	Uint64() uint64
	Bytes(n int) []byte
}

// WALRecord represents an append-only write-ahead log record.
type WALRecord struct {
	LSN       uint64
	Term      uint64 // Raft term in which this record was written; 0 for pre-Raft records.
	TxID      string
	Type      string
	Timestamp uint64
	Payload   []byte
}

// LogStore appends and reads WAL records in deterministic LSN order.
type LogStore interface {
	Append(ctx context.Context, record WALRecord) (uint64, error)
	ReadFrom(ctx context.Context, fromLSN uint64, limit int) ([]WALRecord, error)
}

// BatchAppender is an optional interface that LogStore implementations can
// provide to support batching multiple WAL records into a single write+sync.
// This dramatically reduces fsync overhead when a transaction contains
// multiple mutations (e.g. BEGIN + N mutations + COMMIT = 1 fsync instead of N+2).
type BatchAppender interface {
	AppendBatch(ctx context.Context, records []WALRecord) ([]uint64, error)
}

// NoSyncBatchAppender writes WAL records without calling fsync.
// Used by the group commit path to separate WAL writes (fast, under lock)
// from fsync (slow, batched outside the lock).
type NoSyncBatchAppender interface {
	AppendBatchNoSync(ctx context.Context, records []WALRecord) ([]uint64, error)
}

// Syncer provides explicit fsync control over the WAL.
// Used by the group commit syncer goroutine to batch multiple commits
// into a single fsync.
type Syncer interface {
	Sync() error
}

// Truncator provides WAL compaction after snapshot persistence.
// Removes all records with LSN < beforeLSN. The caller is responsible
// for ensuring a snapshot covering those LSNs has been persisted.
type Truncator interface {
	TruncateBefore(ctx context.Context, beforeLSN uint64) error
}

// Sizer reports the total on-disk byte size of the store.
type Sizer interface {
	TotalSize() (int64, error)
}

// LedgerReader is implemented by log stores that maintain the tamper-evident
// hash chain over the WAL (ADR 0005). LedgerChainAt returns the chain value at
// lsn while the store still holds it in memory. The chain is metadata about
// the log: the engine only records it in snapshots as an anchor and never
// lets it influence state.
type LedgerReader interface {
	LedgerChainAt(lsn uint64) ([32]byte, bool)
}

// LedgerSigner signs WAL ledger digests (ADR 0005, ledger=signed). The
// built-in implementation holds a node's Ed25519 key; KMS or HSM backends can
// implement it later. Signatures are metadata about the log, never engine
// state.
type LedgerSigner interface {
	// KeyID identifies the verification key; verifiers use it to pick the
	// trusted public key.
	KeyID() string
	// Sign returns an Ed25519 signature over message.
	Sign(message []byte) ([]byte, error)
}

// RaftCommitter is the narrow interface the executor uses to route each WAL
// record through the Raft consensus layer before committing it locally.
// Implementations block until a quorum of nodes has durably stored the entry.
//
// When nil (standalone / non-cluster mode) the executor writes directly to the
// WAL via AppendBatchNoSync + group-fsync.  When set, the Raft path replaces
// both the WAL write and the fsync — durability is guaranteed by the quorum.
//
// Apply returns the LSN assigned to the entry by the Raft log (= WAL LSN), or
// ErrNotLeader if this node has been demoted mid-transaction.
//
// ApplyBatch appends all records locally and performs a single Raft round-trip
// (one broadcastAndCommit) for the entire batch, amortising network latency
// across all records in the batch.  Prefer ApplyBatch whenever multiple
// records must be committed atomically or with maximum throughput.
type RaftCommitter interface {
	Apply(ctx context.Context, typ, txID string, payload []byte) (lsn uint64, err error)
	ApplyBatch(ctx context.Context, records []RaftRecord) (lsns []uint64, err error)
}

// LeaderReadinessWaiter is an optional RaftCommitter extension. The engine
// calls WaitLeaderReady before taking its write lock for a commit, because a
// new leader's catch-up needs that lock and Apply/ApplyBatch fail fast until
// the catch-up is done. An error means the commit should proceed and let
// Apply/ApplyBatch report the failure.
type LeaderReadinessWaiter interface {
	WaitLeaderReady(ctx context.Context) error
}

// RaftRecord is a single entry to be proposed to the Raft log as part of a
// batch operation via RaftCommitter.ApplyBatch.
type RaftRecord struct {
	Type    string
	TxID    string
	Payload []byte
}

// AuditEntry records a single row-level change committed to the engine.
// It is written to the AuditStore during the commit path and is never
// truncated, providing permanent history independent of the WAL lifecycle.
type AuditEntry struct {
	CommitLSN uint64
	Domain    string
	Table     string
	Operation string                 // "INSERT", "UPDATE", "DELETE"
	OldRow    map[string]ast.Literal // nil for INSERT
	NewRow    map[string]ast.Literal // nil for DELETE
}

// AuditStore holds row-level change history: the row changes of every
// committed transaction, in commit order, as an index of the WAL that
// outlives WAL GC. FOR HISTORY reads it.
type AuditStore interface {
	// AppendCommitted appends the row changes of the commits up to through
	// and records that the store covers every commit up to through.
	AppendCommitted(ctx context.Context, entries []AuditEntry, through uint64) error
	// Covered returns the LSN through which the store holds every commit's
	// row changes. tracked is false for a store written before coverage was
	// recorded, whose history may have gaps.
	Covered() (lsn uint64, tracked bool)
	// TruncateAfter drops the row changes of commits after lsn.
	TruncateAfter(ctx context.Context, lsn uint64) error
	ReadAll(ctx context.Context) ([]AuditEntry, error)
	ReadFromLSN(ctx context.Context, fromLSN uint64, limit int) ([]AuditEntry, error)
	TotalSize() (int64, error)
}

// KVStore is the domain-partitioned key-value persistence interface.
type KVStore interface {
	Get(ctx context.Context, domain string, key []byte) ([]byte, bool, error)
	Put(ctx context.Context, domain string, key []byte, value []byte) error
	Delete(ctx context.Context, domain string, key []byte) error
}

// Telemetry captures metrics and traces through a stable abstraction.
type Telemetry interface {
	Counter(name string, delta int64, attrs map[string]string)
	Histogram(name string, value float64, attrs map[string]string)
	Trace(event string, attrs map[string]string)
}
