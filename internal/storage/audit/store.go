// Package audit implements the append-only row-level audit log.
//
// Separate from the WAL so WAL segments can be truncated after snapshots
// while the full audit trail is retained indefinitely (or per policy).
//
// Storage: SegmentedLogStore under <datadir>/audit/ (same framing + zstd).
//
// Entry binary format (WALRecord.Payload):
//
//	[1B ver=0x01][uv domain][uv table][8B commitLSN LE]
//	[1B op: 0x01=INSERT 0x02=UPDATE 0x03=DELETE]
//	[rowMap oldRow]  0x00=nil | 0x01 [uv N] [[str col][literal] x N]
//	[rowMap newRow]  same
package audit

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"asql/internal/engine/parser/ast"
	"asql/internal/engine/ports"
	"asql/internal/storage/wal"
)

const auditPayloadVersion byte = 0x01

const (
	opInsert byte = 0x01
	opUpdate byte = 0x02
	opDelete byte = 0x03
)

const (
	litNull      byte = 0x00
	litString    byte = 0x01
	litInt64     byte = 0x02
	litFloat64   byte = 0x03
	litBoolTrue  byte = 0x04
	litBoolFalse byte = 0x05
	litJSON      byte = 0x06
	litTimestamp byte = 0x08
)

// Store is an append-only audit log backed by SegmentedLogStore.
// Implements ports.AuditStore.
type Store struct {
	inner *wal.SegmentedLogStore
}

// New opens (or creates) an audit store at basePath (e.g. "<datadir>/audit/audit").
func New(basePath string) (*Store, error) {
	seg, err := wal.NewSegmentedLogStore(basePath, wal.AlwaysSync{})
	if err != nil {
		return nil, fmt.Errorf("audit: open segment store: %w", err)
	}
	return &Store{inner: seg}, nil
}

// AppendBatch persists a slice of audit entries.
func (s *Store) AppendBatch(ctx context.Context, entries []ports.AuditEntry) error {
	if len(entries) == 0 {
		return nil
	}
	records := make([]ports.WALRecord, len(entries))
	for i, e := range entries {
		payload, err := encodeEntry(e)
		if err != nil {
			return fmt.Errorf("audit: encode entry[%d]: %w", i, err)
		}
		records[i] = ports.WALRecord{Type: "AUDIT", Payload: payload}
	}
	_, err := s.inner.AppendBatch(ctx, records)
	return err
}

// ReadAll returns all audit entries in insertion order.
func (s *Store) ReadAll(ctx context.Context) ([]ports.AuditEntry, error) {
	return s.ReadFromLSN(ctx, 0, 0)
}

// ReadFromLSN returns entries with CommitLSN >= fromLSN. limit=0 means unlimited.
func (s *Store) ReadFromLSN(ctx context.Context, fromLSN uint64, limit int) ([]ports.AuditEntry, error) {
	records, err := s.inner.ReadFrom(ctx, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("audit: read records: %w", err)
	}
	var out []ports.AuditEntry
	for _, r := range records {
		if r.Type != "AUDIT" {
			continue
		}
		e, err := decodeEntry(r.Payload)
		if err != nil {
			return nil, fmt.Errorf("audit: decode record: %w", err)
		}
		if e.CommitLSN < fromLSN {
			continue
		}
		out = append(out, e)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// TotalSize returns the on-disk byte size of the audit store.
func (s *Store) TotalSize() (int64, error) { return s.inner.TotalSize() }

// Close flushes and closes the underlying store.
func (s *Store) Close() error { return s.inner.Close() }

func encodeEntry(e ports.AuditEntry) ([]byte, error) {
	w := &abuf{b: make([]byte, 0, 64)}
	w.byte1(auditPayloadVersion)
	w.str(e.Domain)
	w.str(e.Table)
	w.u64(e.CommitLSN)
	switch e.Operation {
	case "INSERT":
		w.byte1(opInsert)
	case "UPDATE":
		w.byte1(opUpdate)
	case "DELETE":
		w.byte1(opDelete)
	default:
		return nil, fmt.Errorf("audit: unknown operation %q", e.Operation)
	}
	encodeRowMap(w, e.OldRow)
	encodeRowMap(w, e.NewRow)
	return w.b, nil
}

func encodeRowMap(w *abuf, row map[string]ast.Literal) {
	if row == nil {
		w.byte1(0x00)
		return
	}
	w.byte1(0x01)
	w.uv(uint64(len(row)))
	for col, lit := range row {
		w.str(col)
		encodeLit(w, lit)
	}
}

func encodeLit(w *abuf, lit ast.Literal) {
	switch lit.Kind {
	case ast.LiteralNull, "":
		w.byte1(litNull)
	case ast.LiteralString:
		w.byte1(litString)
		w.str(lit.StringValue)
	case ast.LiteralNumber:
		w.byte1(litInt64)
		w.u64(uint64(lit.NumberValue))
	case ast.LiteralFloat:
		w.byte1(litFloat64)
		w.u64(math.Float64bits(lit.FloatValue))
	case ast.LiteralBoolean:
		if lit.BoolValue {
			w.byte1(litBoolTrue)
		} else {
			w.byte1(litBoolFalse)
		}
	case ast.LiteralJSON:
		w.byte1(litJSON)
		w.str(lit.StringValue)
	case ast.LiteralTimestamp:
		w.byte1(litTimestamp)
		w.str(lit.StringValue)
	default:
		w.byte1(litNull)
	}
}

func decodeEntry(data []byte) (ports.AuditEntry, error) {
	r := &areader{data: data}
	ver, err := r.byte1()
	if err != nil || ver != auditPayloadVersion {
		return ports.AuditEntry{}, fmt.Errorf("audit: bad version 0x%02x", firstOrZero(data))
	}
	domain, err := r.str()
	if err != nil {
		return ports.AuditEntry{}, fmt.Errorf("audit: domain: %w", err)
	}
	table, err := r.str()
	if err != nil {
		return ports.AuditEntry{}, fmt.Errorf("audit: table: %w", err)
	}
	lsn, err := r.u64()
	if err != nil {
		return ports.AuditEntry{}, fmt.Errorf("audit: lsn: %w", err)
	}
	op, err := r.byte1()
	if err != nil {
		return ports.AuditEntry{}, fmt.Errorf("audit: op: %w", err)
	}
	var opStr string
	switch op {
	case opInsert:
		opStr = "INSERT"
	case opUpdate:
		opStr = "UPDATE"
	case opDelete:
		opStr = "DELETE"
	default:
		return ports.AuditEntry{}, fmt.Errorf("audit: unknown op 0x%02x", op)
	}
	oldRow, err := decodeRowMap(r)
	if err != nil {
		return ports.AuditEntry{}, fmt.Errorf("audit: oldRow: %w", err)
	}
	newRow, err := decodeRowMap(r)
	if err != nil {
		return ports.AuditEntry{}, fmt.Errorf("audit: newRow: %w", err)
	}
	return ports.AuditEntry{
		CommitLSN: lsn, Domain: domain, Table: table,
		Operation: opStr, OldRow: oldRow, NewRow: newRow,
	}, nil
}

func decodeRowMap(r *areader) (map[string]ast.Literal, error) {
	tag, err := r.byte1()
	if err != nil {
		return nil, err
	}
	if tag == 0x00 {
		return nil, nil
	}
	n, err := r.uv()
	if err != nil {
		return nil, err
	}
	row := make(map[string]ast.Literal, n)
	for i := uint64(0); i < n; i++ {
		col, err := r.str()
		if err != nil {
			return nil, fmt.Errorf("col[%d]: %w", i, err)
		}
		lit, err := decodeLit(r)
		if err != nil {
			return nil, fmt.Errorf("lit[%d]: %w", i, err)
		}
		row[col] = lit
	}
	return row, nil
}

func decodeLit(r *areader) (ast.Literal, error) {
	kind, err := r.byte1()
	if err != nil {
		return ast.Literal{}, err
	}
	switch kind {
	case litNull:
		return ast.Literal{Kind: ast.LiteralNull}, nil
	case litString:
		s, err := r.str()
		return ast.Literal{Kind: ast.LiteralString, StringValue: s}, err
	case litInt64:
		v, err := r.u64()
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(v)}, err
	case litFloat64:
		bits, err := r.u64()
		return ast.Literal{Kind: ast.LiteralFloat, FloatValue: math.Float64frombits(bits)}, err
	case litBoolTrue:
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: true}, nil
	case litBoolFalse:
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: false}, nil
	case litJSON:
		s, err := r.str()
		return ast.Literal{Kind: ast.LiteralJSON, StringValue: s}, err
	case litTimestamp:
		s, err := r.str()
		return ast.Literal{Kind: ast.LiteralTimestamp, StringValue: s}, err
	default:
		return ast.Literal{}, fmt.Errorf("audit: unknown literal kind 0x%02x", kind)
	}
}

type abuf struct{ b []byte }

func (w *abuf) byte1(b byte) { w.b = append(w.b, b) }
func (w *abuf) uv(v uint64)  { w.b = binary.AppendUvarint(w.b, v) }
func (w *abuf) str(s string) { w.uv(uint64(len(s))); w.b = append(w.b, s...) }
func (w *abuf) u64(v uint64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}

type areader struct {
	data []byte
	off  int
}

func (r *areader) byte1() (byte, error) {
	if r.off >= len(r.data) {
		return 0, errors.New("audit: unexpected EOF")
	}
	b := r.data[r.off]
	r.off++
	return b, nil
}

func (r *areader) uv() (uint64, error) {
	v, n := binary.Uvarint(r.data[r.off:])
	if n <= 0 {
		return 0, errors.New("audit: truncated uvarint")
	}
	r.off += n
	return v, nil
}

func (r *areader) str() (string, error) {
	n, err := r.uv()
	if err != nil {
		return "", err
	}
	if r.off+int(n) > len(r.data) {
		return "", fmt.Errorf("audit: string truncated (need %d)", n)
	}
	s := string(r.data[r.off : r.off+int(n)])
	r.off += int(n)
	return s, nil
}

func (r *areader) u64() (uint64, error) {
	if r.off+8 > len(r.data) {
		return 0, errors.New("audit: truncated u64")
	}
	v := binary.LittleEndian.Uint64(r.data[r.off:])
	r.off += 8
	return v, nil
}

func firstOrZero(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}
