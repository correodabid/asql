// Package statetree implements the state tree of ADR 0008: a compact sparse
// Merkle tree over the live rows of VERIFIABLE tables, keyed by primary key,
// with salted leaves and batch proofs for a set of rows under one root.
//
// Hashing (all SHA-256):
//
//	row key   = H(0x01 || str(domain) || str(table) || u32 n || n canonical values)
//	row leaf  = H(0x00 || 0x01 || salt || u32 n || n × (str(column) || canonical value))
//	salt      = HMAC-SHA-256(state key, row key || u64 commit LSN)
//	empty     = 32 zero bytes
//	one key   = H(0x02 || key || leaf), at the highest level where it is alone
//	otherwise = H(0x01 || left || right)
//
// str is a u32 length and the bytes; integers are big-endian; canonical
// values are ADR 0006's typed encoding (ledgerdigest.AppendValue). Row
// columns are sorted by name, so a leaf needs no schema to be read.
//
// Like the log tree, the state tree is metadata about the database: nothing
// the engine computes reads it.
package statetree

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
)

// Hash is a key, leaf, salt or node hash.
type Hash = [32]byte

// Type bytes of keys and leaves. Group leaves (ADR 0008, completeness
// upwards) will take 0x02.
const (
	typeRow byte = 0x01

	leafPrefix  byte = 0x00
	nodePrefix  byte = 0x01
	alonePrefix byte = 0x02
)

// RowKey returns the key of the row of domain.table with the given primary
// key values, in key order.
func RowKey(domain, table string, primaryKey []ast.Literal) (Hash, error) {
	b := []byte{typeRow}
	b = appendString(b, domain)
	b = appendString(b, table)
	b = binary.BigEndian.AppendUint32(b, uint32(len(primaryKey)))
	for i, value := range primaryKey {
		var err error
		if b, err = ledgerdigest.AppendValue(b, value); err != nil {
			return Hash{}, fmt.Errorf("state tree: primary key value %d: %w", i, err)
		}
	}
	return sha256.Sum256(b), nil
}

// RowLeaf returns the leaf of a row: every column, sorted by name, salted.
func RowLeaf(salt Hash, row map[string]ast.Literal) (Hash, error) {
	columns := make([]string, 0, len(row))
	for column := range row {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	b := make([]byte, 0, 2+len(salt)+4+len(columns)*16)
	b = append(b, leafPrefix, typeRow)
	b = append(b, salt[:]...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(columns)))
	for _, column := range columns {
		b = appendString(b, column)
		var err error
		if b, err = ledgerdigest.AppendValue(b, row[column]); err != nil {
			return Hash{}, fmt.Errorf("state tree: column %s: %w", column, err)
		}
	}
	return sha256.Sum256(b), nil
}

// Salt returns the salt of a row version: it depends on the row's key, the
// LSN of the commit that wrote that version, and the node's state key.
func Salt(stateKey [32]byte, key Hash, commitLSN uint64) Hash {
	mac := hmac.New(sha256.New, stateKey[:])
	mac.Write(key[:])
	var lsn [8]byte
	binary.BigEndian.PutUint64(lsn[:], commitLSN)
	mac.Write(lsn[:])
	var out Hash
	mac.Sum(out[:0])
	return out
}

func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func nodeHash(left, right Hash) Hash {
	var b [1 + 64]byte
	b[0] = nodePrefix
	copy(b[1:], left[:])
	copy(b[33:], right[:])
	return sha256.Sum256(b[:])
}

func aloneHash(key, leaf Hash) Hash {
	var b [1 + 64]byte
	b[0] = alonePrefix
	copy(b[1:], key[:])
	copy(b[33:], leaf[:])
	return sha256.Sum256(b[:])
}

// bit returns bit i of key, most significant first.
func bit(key Hash, i int) int {
	return int(key[i/8]>>(7-uint(i%8))) & 1
}
