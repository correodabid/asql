package wal

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"sync"

	"github.com/correodabid/asql/internal/engine/ports"
)

// Tamper-evident ledger (ADR 0005).
//
// Every WAL record contributes a leaf hash computed over its logical fields
// with the uncompressed payload, never over on-disk bytes, so the chain is
// invariant under s2 payload compression, WALZ segment compaction and the
// v4 -> v5 record migration:
//
//	leaf_n  = SHA-256( 0x00 || Term || Timestamp || u16(len TxID) || TxID
//	                        || TypeEnum || u32(len Payload) || Payload )
//	chain_n = SHA-256( 0x01 || chain_{n-1} || LSN_n || leaf_n )
//	chain_0 = SHA-256( 0x02 || "asql-ledger-v1" )
//
// The LSN enters in the chain step, not the leaf, so leaves can be computed
// before LSN assignment and outside the store lock. The chain is a pure
// function of the WAL contents: it reads no clock, randomness or process state,
// and neither queries nor replay observe it.

const (
	ledgerLeafPrefix    byte = 0x00
	ledgerChainPrefix   byte = 0x01
	ledgerGenesisPrefix byte = 0x02

	ledgerDomain = "asql-ledger-v1"

	// ledgerChainStepSize is prefix(1) + chain(32) + LSN(8) + leaf(32): two
	// SHA-256 blocks regardless of payload size.
	ledgerChainStepSize = 1 + 32 + 8 + 32

	// ledgerRingSize bounds how many recent (LSN, chain) points a store keeps
	// in memory so snapshots can anchor the chain at their LSN.
	ledgerRingSize = 4096

	// ledgerContiguousMax is the largest leaf input hashed in one Sum256 call
	// over a contiguous scratch buffer; larger payloads are streamed.
	ledgerContiguousMax = 16 << 10

	// ledgerStackLeafMax is the largest leaf input hashed from a stack buffer.
	ledgerStackLeafMax = 256
)

// LedgerMode selects whether a log store maintains the hash chain.
type LedgerMode uint8

const (
	// LedgerChain maintains the hash chain. It is the zero value, so every
	// store is tamper-evident unless configured otherwise.
	LedgerChain LedgerMode = iota
	// LedgerOff skips all ledger work and writes v3 segment headers, which
	// restores the pre-ledger append path exactly.
	LedgerOff
)

// String returns the configuration spelling of the mode.
func (m LedgerMode) String() string {
	switch m {
	case LedgerChain:
		return "chain"
	case LedgerOff:
		return "off"
	default:
		return fmt.Sprintf("LedgerMode(%d)", uint8(m))
	}
}

// ParseLedgerMode parses the configuration spelling of a ledger mode.
func ParseLedgerMode(s string) (LedgerMode, error) {
	switch s {
	case "", "chain":
		return LedgerChain, nil
	case "off":
		return LedgerOff, nil
	default:
		return 0, fmt.Errorf("invalid ledger mode %q (want off or chain)", s)
	}
}

// ChainHash is a ledger leaf or chain value.
type ChainHash [32]byte

// String returns the lowercase hex encoding.
func (h ChainHash) String() string { return hex.EncodeToString(h[:]) }

// GenesisChain is chain_0, the predecessor of LSN 1.
var GenesisChain = func() ChainHash {
	buf := make([]byte, 0, 1+len(ledgerDomain))
	buf = append(buf, ledgerGenesisPrefix)
	buf = append(buf, ledgerDomain...)
	return sha256.Sum256(buf)
}()

type leafHasher struct {
	h       hash.Hash
	scratch []byte
	sum     [sha256.Size]byte
}

var leafHasherPool = sync.Pool{New: func() any {
	return &leafHasher{h: sha256.New(), scratch: make([]byte, 0, 64)}
}}

// LedgerLeaf computes the leaf hash of a WAL record. The record's LSN is not
// part of the leaf.
func LedgerLeaf(record ports.WALRecord) (ChainHash, error) {
	typeEnum, err := typeStringToEnum(record.Type)
	if err != nil {
		return ChainHash{}, err
	}
	return ledgerLeaf(record.Term, record.Timestamp, record.TxID, typeEnum, record.Payload), nil
}

func ledgerLeaf(term, timestamp uint64, txID string, typeEnum byte, payload []byte) ChainHash {
	// Small leaves (BEGIN/COMMIT, short mutations) hash a stack buffer.
	if leafHeaderSize+len(txID)+len(payload) <= ledgerStackLeafMax {
		var buf [ledgerStackLeafMax]byte
		b := appendLeafHeader(buf[:0], term, timestamp, txID, typeEnum, len(payload))
		b = append(b, payload...)
		return sha256.Sum256(b)
	}
	lh := leafHasherPool.Get().(*leafHasher)
	b := appendLeafHeader(lh.scratch[:0], term, timestamp, txID, typeEnum, len(payload))
	var leaf ChainHash
	if len(b)+len(payload) <= ledgerContiguousMax {
		b = append(b, payload...)
		leaf = sha256.Sum256(b)
	} else {
		lh.h.Reset()
		lh.h.Write(b)
		lh.h.Write(payload)
		lh.h.Sum(lh.sum[:0])
		leaf = ChainHash(lh.sum)
	}
	lh.scratch = b
	leafHasherPool.Put(lh)
	return leaf
}

// leafHeaderSize is the fixed part of a leaf input: prefix(1) + Term(8) +
// Timestamp(8) + u16 TxID length(2) + TypeEnum(1) + u32 payload length(4).
const leafHeaderSize = 1 + 8 + 8 + 2 + 1 + 4

func appendLeafHeader(b []byte, term, timestamp uint64, txID string, typeEnum byte, payloadLen int) []byte {
	b = append(b, ledgerLeafPrefix)
	b = binary.BigEndian.AppendUint64(b, term)
	b = binary.BigEndian.AppendUint64(b, timestamp)
	b = binary.BigEndian.AppendUint16(b, uint16(len(txID)))
	b = append(b, txID...)
	b = append(b, typeEnum)
	return binary.BigEndian.AppendUint32(b, uint32(payloadLen))
}

// ChainStep advances the chain from prev by the record at lsn with the given leaf.
func ChainStep(prev ChainHash, lsn uint64, leaf ChainHash) ChainHash {
	var buf [ledgerChainStepSize]byte
	buf[0] = ledgerChainPrefix
	copy(buf[1:33], prev[:])
	binary.BigEndian.PutUint64(buf[33:41], lsn)
	copy(buf[41:], leaf[:])
	return sha256.Sum256(buf[:])
}

// leafBuf is a pooled scratch slice for batch leaves. The leaves are
// overwritten in place by the chain values once LSNs are known, so a batch
// append needs no per-call allocation.
type leafBuf struct{ hashes []ChainHash }

var leafBufPool = sync.Pool{New: func() any { return &leafBuf{hashes: make([]ChainHash, 0, 16)} }}

// computeLeaves hashes every record's leaf. It runs before the store lock is
// taken. It returns nil when the ledger is off; the caller must release a
// non-nil buffer.
func computeLeaves(mode LedgerMode, records []ports.WALRecord) (*leafBuf, error) {
	if mode == LedgerOff {
		return nil, nil
	}
	lb := leafBufPool.Get().(*leafBuf)
	lb.hashes = lb.hashes[:0]
	for _, record := range records {
		leaf, err := LedgerLeaf(record)
		if err != nil {
			releaseLeaves(lb)
			return nil, err
		}
		lb.hashes = append(lb.hashes, leaf)
	}
	return lb, nil
}

func releaseLeaves(lb *leafBuf) {
	if lb != nil {
		leafBufPool.Put(lb)
	}
}

type ledgerPoint struct {
	lsn   uint64
	chain ChainHash
}

// ledgerState is the chain head of a store plus a ring of recent points.
// All methods must be called under the owning store's lock.
type ledgerState struct {
	mode LedgerMode
	// valid is false while the chain head is unknown (ledger off).
	valid bool
	// rooted is false when the chain was re-anchored because history before
	// the first retained record was unavailable and unanchored.
	rooted  bool
	headLSN uint64
	head    ChainHash
	ring    []ledgerPoint
	ringPos int
}

func newLedgerState(mode LedgerMode) ledgerState {
	ls := ledgerState{mode: mode}
	if mode == LedgerChain {
		ls.valid = true
		ls.rooted = true
		ls.head = GenesisChain
		ls.ring = make([]ledgerPoint, 0, ledgerRingSize)
	}
	return ls
}

func (ls *ledgerState) active() bool { return ls.mode == LedgerChain && ls.valid }

// reset positions the head at (lsn, chain) and clears the ring.
func (ls *ledgerState) reset(lsn uint64, chain ChainHash, rooted bool) {
	ls.valid = ls.mode == LedgerChain
	ls.rooted = rooted
	ls.headLSN = lsn
	ls.head = chain
	ls.ring = ls.ring[:0]
	ls.ringPos = 0
}

// advance turns lb's leaves into chain values for consecutive LSNs starting
// at firstLSN. It does not publish the new head; see commit.
func (ls *ledgerState) advance(firstLSN uint64, lb *leafBuf) {
	chain := ls.head
	for i := range lb.hashes {
		chain = ChainStep(chain, firstLSN+uint64(i), lb.hashes[i])
		lb.hashes[i] = chain
	}
}

// commit publishes the chain values produced by advance.
func (ls *ledgerState) commit(firstLSN uint64, lb *leafBuf) {
	if len(lb.hashes) == 0 {
		return
	}
	for i := range lb.hashes {
		ls.push(firstLSN+uint64(i), lb.hashes[i])
	}
	ls.headLSN = firstLSN + uint64(len(lb.hashes)) - 1
	ls.head = lb.hashes[len(lb.hashes)-1]
}

// step advances and publishes a single record.
func (ls *ledgerState) step(lsn uint64, leaf ChainHash) {
	ls.head = ChainStep(ls.head, lsn, leaf)
	ls.headLSN = lsn
	ls.push(lsn, ls.head)
}

func (ls *ledgerState) push(lsn uint64, chain ChainHash) {
	if len(ls.ring) < ledgerRingSize {
		ls.ring = append(ls.ring, ledgerPoint{lsn: lsn, chain: chain})
		return
	}
	ls.ring[ls.ringPos] = ledgerPoint{lsn: lsn, chain: chain}
	ls.ringPos = (ls.ringPos + 1) % ledgerRingSize
}

// chainAt returns the chain value at lsn if it is the head or still in the ring.
func (ls *ledgerState) chainAt(lsn uint64) (ChainHash, bool) {
	if !ls.active() {
		return ChainHash{}, false
	}
	if lsn == ls.headLSN {
		return ls.head, true
	}
	// Newest entries first, so a re-used LSN after a failed write resolves to
	// the value that matches the file.
	for i := 1; i <= len(ls.ring); i++ {
		idx := (ls.ringPos - i + len(ls.ring)) % len(ls.ring)
		if ls.ring[idx].lsn == lsn {
			return ls.ring[idx].chain, true
		}
	}
	return ChainHash{}, false
}

// LedgerHead describes a store's chain head.
type LedgerHead struct {
	Mode LedgerMode
	// Valid is false when the ledger is off.
	Valid bool
	// Rooted is false when the chain was re-anchored after an unanchored,
	// truncated prefix and therefore does not extend back to genesis.
	Rooted bool
	LSN    uint64
	Chain  ChainHash
}

func (ls *ledgerState) headInfo() LedgerHead {
	return LedgerHead{Mode: ls.mode, Valid: ls.active(), Rooted: ls.rooted, LSN: ls.headLSN, Chain: ls.head}
}
