package statetree

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/storage/ledgerdigest"
)

// Group leaves (ADR 0008, completeness upwards). A verifiable group of a
// table names, for every row of the referenced table, the rows of this
// table whose group columns equal that row's primary key: for
// artifact_parents GROUP (child_id) REFERENCES artifacts (id), all the
// parent edges of each artifact. Every referenced row has a group leaf, an
// empty one when it has no members, so "X has no parents" is proven like
// any other group, without proofs of absence.
//
//	group key  = H(0x02 || str(domain) || str(table) || u32 n || n × str(column)
//	               || str(referenced table) || n canonical values)
//	group leaf = H(0x00 || 0x02 || u32 m || m × (row key || row leaf)), by row key

const typeGroup byte = 0x02

// GroupKey returns the key of the group of domain.table whose columns
// equal values, under the group that references refTable.
func GroupKey(domain, table string, columns []string, refTable string, values []ast.Literal) (Hash, error) {
	if len(columns) != len(values) || len(columns) == 0 {
		return Hash{}, fmt.Errorf("state tree: group of %s.%s: %d columns for %d values", domain, table, len(columns), len(values))
	}
	b := []byte{typeGroup}
	b = appendString(b, domain)
	b = appendString(b, table)
	b = binary.BigEndian.AppendUint32(b, uint32(len(columns)))
	for _, column := range columns {
		b = appendString(b, column)
	}
	b = appendString(b, refTable)
	for i, value := range values {
		var err error
		if b, err = ledgerdigest.AppendValue(b, value); err != nil {
			return Hash{}, fmt.Errorf("state tree: group value %d: %w", i, err)
		}
	}
	return sha256.Sum256(b), nil
}

// GroupLeaf returns the leaf of a group whose members have these keys and
// leaves, in any order.
func GroupLeaf(members []Entry) Hash {
	sorted := append([]Entry(nil), members...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i].Key[:], sorted[j].Key[:]) < 0 })
	h := sha256.New()
	h.Write([]byte{leafPrefix, typeGroup})
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(sorted)))
	h.Write(n[:])
	for _, m := range sorted {
		h.Write(m.Key[:])
		h.Write(m.Leaf[:])
	}
	var out Hash
	h.Sum(out[:0])
	return out
}

// InGroup reports whether a row belongs to the group whose columns equal
// values.
func InGroup(row map[string]ast.Literal, columns []string, values []ast.Literal) bool {
	if len(columns) != len(values) {
		return false
	}
	for i, column := range columns {
		if v, ok := row[column]; !ok || v != values[i] {
			return false
		}
	}
	return true
}
