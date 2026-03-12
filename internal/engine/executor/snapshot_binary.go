package executor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sort"

	"asql/internal/engine/parser/ast"
)

// Binary snapshot format (version 10).
//
// All multi-byte integers are big-endian. The file is zstd-compressed on disk.
//
// Changes vs v9:
//   - CRC32C (Castagnoli) replaces CRC32 IEEE; hardware-accelerated on x86/ARM.
//
// Version 9 adds a string dictionary for deduplication. All string values are
// stored once in a dictionary table and referenced by 4-byte index elsewhere.
//
// Top-level:
//   [4B] magic "ASNP"
//   [1B] version = 10
//   [string dictionary]             <- added in v9
//   [4B] num_snapshots
//   [snapshot]...
//   [4B] CRC32C (over everything before this)  <- changed in v10
//
// String dictionary:
//   [4B] num_strings
//   For each: [2B len][bytes...]
//
// All places that previously wrote [2B len][bytes...] now write [4B dictIndex].
//
// Each snapshot:
//   [8B] LSN
//   [8B] LogicalTS
//   [1B] IsFull flag
//   [catalog]
//   [domains]
//
// Catalog:
//   [2B] num_domains
//   For each: [2B name_len][name...][2B num_tables][for each: [2B name_len][name...]]
//
// Domains:
//   [2B] num_domains
//   For each: [2B name_len][name...][2B num_tables][table...][entities][entity_versions]
//
// Table:
//   [2B] name_len][name...]
//   [columns][column_definitions]
//   [rows: column-indexed format]
//   [indexes_meta][indexed_columns][indexed_column_sets]
//   [2B pk_len][pk...]
//   [unique_columns][foreign_keys][check_constraints][versioned_foreign_keys]
//   [8B] lastMutationTS
//   [changelog: column-indexed format]
//
// Rows (column-indexed, v9):
//   [4B] num_rows
//   [2B] num_columns [4B dictIdx col0]...[4B dictIdx colN]   <- written once
//   for each row:
//     [bitmap bytes (ceil(numCols/8))]          <- which columns present
//     for each set bit: [literal value]
//
// Changelog (column-indexed, v9):
//   [4B] num_entries
//   for each entry:
//     [8B commitLSN][4B dictIdx operation]
//     [1B hasOldRow]
//     if hasOldRow: [2B numCols][col names as dictIdx...][bitmap][values...]
//     [1B hasNewRow]
//     if hasNewRow: [2B numCols][col names as dictIdx...][bitmap][values...]
//
// See encode/decode functions below for field-level details.

var (
	snapMagic   = [4]byte{'A', 'S', 'N', 'P'}
	snapVersion = byte(11)

	// snapCRC32C is the CRC32C (Castagnoli) table, hardware-accelerated on x86 (SSE4.2) and ARM.
	snapCRC32C = crc32.MakeTable(crc32.Castagnoli)
)

// ---------- binary write helpers ----------

type binWriter struct {
	buf  bytes.Buffer
	dict *stringDict // non-nil in v9 dictionary mode
}

// stringDict maps unique strings to sequential indices for deduplication.
type stringDict struct {
	strings []string          // ordered list (index → string)
	index   map[string]uint32 // string → index
}

func newStringDict() *stringDict {
	return &stringDict{index: make(map[string]uint32)}
}

// intern adds a string to the dictionary (if absent) and returns its index.
func (d *stringDict) intern(s string) uint32 {
	if idx, ok := d.index[s]; ok {
		return idx
	}
	idx := uint32(len(d.strings))
	d.strings = append(d.strings, s)
	d.index[s] = idx
	return idx
}

func (w *binWriter) u8(v byte) { w.buf.WriteByte(v) }
func (w *binWriter) u16(v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	w.buf.Write(b[:])
}
func (w *binWriter) u32(v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	w.buf.Write(b[:])
}
func (w *binWriter) u64(v uint64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	w.buf.Write(b[:])
}
func (w *binWriter) i64(v int64)   { w.u64(uint64(v)) }
func (w *binWriter) f64(v float64) { w.u64(math.Float64bits(v)) }
func (w *binWriter) boolean(v bool) {
	if v {
		w.u8(1)
	} else {
		w.u8(0)
	}
}

// str writes a string as a 4-byte dictionary index (v9 format).
func (w *binWriter) str(s string) {
	w.u32(w.dict.intern(s))
}
func (w *binWriter) blob(b []byte) {
	w.u32(uint32(len(b)))
	w.buf.Write(b)
}
func (w *binWriter) bytes() []byte { return w.buf.Bytes() }

// writeRowColumnIndexed writes a single nullable row in column-indexed format:
// [1B hasRow] if hasRow: [2B numCols][col names...][bitmap][values...]
func (w *binWriter) writeRowColumnIndexed(row map[string]ast.Literal) {
	if row == nil {
		w.u8(0)
		return
	}
	w.u8(1)
	cols := make([]string, 0, len(row))
	for k := range row {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	w.u16(uint16(len(cols)))
	for _, c := range cols {
		w.str(c)
	}
	// All columns are present (it's a single row, not sparse), but we write
	// the bitmap for format consistency.
	bitmapLen := (len(cols) + 7) / 8
	bitmap := make([]byte, bitmapLen)
	for i := range cols {
		bitmap[i/8] |= 1 << (uint(i) % 8)
	}
	w.buf.Write(bitmap)
	for _, c := range cols {
		w.literal(row[c])
	}
}

// ---------- binary read helpers ----------

type binReader struct {
	data      []byte
	off       int
	err       error
	version   byte
	dictTable []string // non-nil for v9 dictionary mode
}

func (r *binReader) remaining() int { return len(r.data) - r.off }

func (r *binReader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if r.off+n > len(r.data) {
		r.err = fmt.Errorf("snapshot binary: unexpected EOF at offset %d, need %d", r.off, n)
		return false
	}
	return true
}

func (r *binReader) u8() byte {
	if !r.need(1) {
		return 0
	}
	v := r.data[r.off]
	r.off++
	return v
}
func (r *binReader) u16() uint16 {
	if !r.need(2) {
		return 0
	}
	v := binary.BigEndian.Uint16(r.data[r.off:])
	r.off += 2
	return v
}
func (r *binReader) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(r.data[r.off:])
	r.off += 4
	return v
}
func (r *binReader) u64() uint64 {
	if !r.need(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(r.data[r.off:])
	r.off += 8
	return v
}
func (r *binReader) i64() int64    { return int64(r.u64()) }
func (r *binReader) f64() float64  { return math.Float64frombits(r.u64()) }
func (r *binReader) boolean() bool { return r.u8() != 0 }

// str reads a string as a 4-byte dictionary index (v9 format).
func (r *binReader) str() string {
	idx := r.u32()
	if r.err != nil {
		return ""
	}
	if int(idx) >= len(r.dictTable) {
		r.err = fmt.Errorf("snapshot binary: dict index %d out of range (dict size %d)", idx, len(r.dictTable))
		return ""
	}
	return r.dictTable[idx]
}
func (r *binReader) blob() []byte {
	n := int(r.u32())
	if !r.need(n) {
		return nil
	}
	b := make([]byte, n)
	copy(b, r.data[r.off:r.off+n])
	r.off += n
	return b
}

// readRowColumnIndexed reads a single nullable row in column-indexed format.
func (r *binReader) readRowColumnIndexed() map[string]ast.Literal {
	hasRow := r.u8()
	if hasRow == 0 || r.err != nil {
		return nil
	}
	numCols := int(r.u16())
	cols := make([]string, numCols)
	for i := 0; i < numCols; i++ {
		cols[i] = r.str()
	}
	bitmapLen := (numCols + 7) / 8
	if !r.need(bitmapLen) {
		return nil
	}
	bitmap := r.data[r.off : r.off+bitmapLen]
	r.off += bitmapLen
	row := make(map[string]ast.Literal, numCols)
	for ci, col := range cols {
		if bitmap[ci/8]&(1<<(uint(ci)%8)) != 0 {
			row[col] = r.literal()
		}
	}
	return row
}

// ---------- Literal encode/decode ----------

// Literal kind tags for binary encoding.
const (
	litTagNull      byte = 0
	litTagString    byte = 1
	litTagNumber    byte = 2
	litTagBoolean   byte = 3
	litTagFloat     byte = 4
	litTagTimestamp byte = 5
	litTagJSON      byte = 6
)

func (w *binWriter) literal(lit ast.Literal) {
	switch lit.Kind {
	case ast.LiteralNull:
		w.u8(litTagNull)
	case ast.LiteralString:
		w.u8(litTagString)
		w.str(lit.StringValue)
	case ast.LiteralNumber:
		w.u8(litTagNumber)
		w.i64(lit.NumberValue)
	case ast.LiteralBoolean:
		w.u8(litTagBoolean)
		w.boolean(lit.BoolValue)
	case ast.LiteralFloat:
		w.u8(litTagFloat)
		w.f64(lit.FloatValue)
	case ast.LiteralTimestamp:
		w.u8(litTagTimestamp)
		w.str(lit.StringValue)
	case ast.LiteralJSON:
		w.u8(litTagJSON)
		w.str(lit.StringValue)
	default:
		w.u8(litTagNull) // fallback
	}
}

func (r *binReader) literal() ast.Literal {
	tag := r.u8()
	switch tag {
	case litTagNull:
		return ast.Literal{Kind: ast.LiteralNull}
	case litTagString:
		return ast.Literal{Kind: ast.LiteralString, StringValue: r.str()}
	case litTagNumber:
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: r.i64()}
	case litTagBoolean:
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: r.boolean()}
	case litTagFloat:
		return ast.Literal{Kind: ast.LiteralFloat, FloatValue: r.f64()}
	case litTagTimestamp:
		return ast.Literal{Kind: ast.LiteralTimestamp, StringValue: r.str()}
	case litTagJSON:
		return ast.Literal{Kind: ast.LiteralJSON, StringValue: r.str()}
	default:
		r.err = fmt.Errorf("snapshot binary: unknown literal tag %d", tag)
		return ast.Literal{Kind: ast.LiteralNull}
	}
}

// ---------- Predicate encode/decode (recursive) ----------

const (
	predTagNil  byte = 0
	predTagNode byte = 1
)

func (w *binWriter) predicate(p *ast.Predicate) {
	if p == nil {
		w.u8(predTagNil)
		return
	}
	w.u8(predTagNode)
	w.str(p.Column)
	w.str(p.Operator)
	w.literal(p.Value)
	w.predicate(p.Left)
	w.predicate(p.Right)
	// Subquery omitted — not used in CHECK constraints.
}

func (r *binReader) readPredicate() *ast.Predicate {
	tag := r.u8()
	if tag == predTagNil || r.err != nil {
		return nil
	}
	p := &ast.Predicate{}
	p.Column = r.str()
	p.Operator = r.str()
	p.Value = r.literal()
	p.Left = r.readPredicate()
	p.Right = r.readPredicate()
	return p
}

// ---------- ColumnDefinition encode/decode ----------

// DefaultValue tags for binary encoding.
const (
	defaultTagNil           byte = 0x00
	defaultTagLiteral       byte = 0x01
	defaultTagAutoIncrement byte = 0x02
	defaultTagUUIDv7        byte = 0x03
)

func (w *binWriter) columnDef(cd ast.ColumnDefinition) {
	w.str(cd.Name)
	w.str(string(cd.Type))
	// Pack flags into a single byte.
	var flags byte
	if cd.PrimaryKey {
		flags |= 0x01
	}
	if cd.Unique {
		flags |= 0x02
	}
	if cd.NotNull {
		flags |= 0x04
	}
	w.u8(flags)
	w.str(cd.ReferencesTable)
	w.str(cd.ReferencesColumn)
	w.predicate(cd.Check)
	// DefaultValue
	if cd.DefaultValue == nil {
		w.u8(defaultTagNil)
	} else {
		switch cd.DefaultValue.Kind {
		case ast.DefaultLiteral:
			w.u8(defaultTagLiteral)
			w.literal(cd.DefaultValue.Value)
		case ast.DefaultAutoIncrement:
			w.u8(defaultTagAutoIncrement)
		case ast.DefaultUUIDv7:
			w.u8(defaultTagUUIDv7)
		default:
			w.u8(defaultTagNil)
		}
	}
}

func (r *binReader) columnDef() ast.ColumnDefinition {
	cd := ast.ColumnDefinition{}
	cd.Name = r.str()
	cd.Type = ast.DataType(r.str())
	flags := r.u8()
	cd.PrimaryKey = flags&0x01 != 0
	cd.Unique = flags&0x02 != 0
	cd.NotNull = flags&0x04 != 0
	cd.ReferencesTable = r.str()
	cd.ReferencesColumn = r.str()
	cd.Check = r.readPredicate()
	// DefaultValue
	defTag := r.u8()
	switch defTag {
	case defaultTagLiteral:
		cd.DefaultValue = &ast.DefaultExpr{Kind: ast.DefaultLiteral, Value: r.literal()}
	case defaultTagAutoIncrement:
		cd.DefaultValue = &ast.DefaultExpr{Kind: ast.DefaultAutoIncrement}
	case defaultTagUUIDv7:
		cd.DefaultValue = &ast.DefaultExpr{Kind: ast.DefaultUUIDv7}
	}
	return cd
}

// ---------- Table encode/decode ----------

func (w *binWriter) table(name string, pt *persistedTable) {
	w.str(name)

	// Columns
	w.u16(uint16(len(pt.Columns)))
	for _, col := range pt.Columns {
		w.str(col)
	}

	// Column definitions
	w.u16(uint16(len(pt.ColumnDefinitions)))
	for colName, cd := range pt.ColumnDefinitions {
		// Ensure the name field in cd matches the map key.
		cd.Name = colName
		w.columnDef(cd)
	}

	// Rows (column-indexed: column header once, then bitmap + values per row)
	w.u32(uint32(len(pt.Rows)))
	if len(pt.Rows) > 0 {
		// Collect all column names across all rows (stable order).
		colSet := make(map[string]struct{})
		for _, row := range pt.Rows {
			for key := range row {
				colSet[key] = struct{}{}
			}
		}
		rowCols := make([]string, 0, len(colSet))
		for col := range colSet {
			rowCols = append(rowCols, col)
		}
		sort.Strings(rowCols)

		// Write column header once.
		w.u16(uint16(len(rowCols)))
		for _, col := range rowCols {
			w.str(col)
		}

		// Write each row as bitmap + values.
		bitmapLen := (len(rowCols) + 7) / 8
		for _, row := range pt.Rows {
			bitmap := make([]byte, bitmapLen)
			for ci, col := range rowCols {
				if _, ok := row[col]; ok {
					bitmap[ci/8] |= 1 << (uint(ci) % 8)
				}
			}
			w.buf.Write(bitmap)
			for ci, col := range rowCols {
				if bitmap[ci/8]&(1<<(uint(ci)%8)) != 0 {
					w.literal(row[col])
				}
			}
		}
	}

	// Indexes (metadata + bucket/entry data for v11+)
	w.u16(uint16(len(pt.Indexes)))
	for idxName, idx := range pt.Indexes {
		w.str(idxName)
		w.str(idx.Name)
		w.str(idx.Column)
		w.u16(uint16(len(idx.Columns)))
		for _, col := range idx.Columns {
			w.str(col)
		}
		w.str(idx.Kind)
		// v11+: write index data (1 = data follows, 0 = metadata only)
		if idx.DataLoaded {
			w.u8(1)
			switch idx.Kind {
			case "hash":
				w.u32(uint32(len(idx.Buckets)))
				for key, rowIDs := range idx.Buckets {
					w.str(key)
					w.u32(uint32(len(rowIDs)))
					for _, rid := range rowIDs {
						w.u32(uint32(rid))
					}
				}
			case "btree":
				w.u32(uint32(len(idx.Entries)))
				for _, e := range idx.Entries {
					w.literal(e.Value)
					w.u16(uint16(len(e.Values)))
					for _, v := range e.Values {
						w.literal(v)
					}
					w.u32(uint32(e.RowID))
				}
			}
		} else {
			w.u8(0) // no data; reader should rebuild from rows
		}
	}

	// IndexedColumns
	w.u16(uint16(len(pt.IndexedColumns)))
	for col, idxName := range pt.IndexedColumns {
		w.str(col)
		w.str(idxName)
	}

	// IndexedColumnSets
	w.u16(uint16(len(pt.IndexedColumnSets)))
	for cols, idxName := range pt.IndexedColumnSets {
		w.str(cols)
		w.str(idxName)
	}

	// Primary key
	w.str(pt.PrimaryKey)

	// Unique columns
	w.u16(uint16(len(pt.UniqueColumns)))
	for _, col := range pt.UniqueColumns {
		w.str(col)
	}

	// Foreign keys
	w.u16(uint16(len(pt.ForeignKeys)))
	for _, fk := range pt.ForeignKeys {
		w.str(fk.Column)
		w.str(fk.ReferencesTable)
		w.str(fk.ReferencesColumn)
	}

	// Check constraints
	w.u16(uint16(len(pt.CheckConstraints)))
	for _, cc := range pt.CheckConstraints {
		w.str(cc.Column)
		w.predicate(cc.Predicate)
	}

	// Versioned foreign keys
	w.u16(uint16(len(pt.VersionedForeignKeys)))
	for _, vfk := range pt.VersionedForeignKeys {
		w.str(vfk.Column)
		w.str(vfk.LSNColumn)
		w.str(vfk.ReferencesDomain)
		w.str(vfk.ReferencesTable)
		w.str(vfk.ReferencesColumn)
	}

	// LastMutationTS
	w.u64(pt.LastMutationTS)

	// Change log (column-indexed per entry)
	w.u32(uint32(len(pt.ChangeLog)))
	for _, entry := range pt.ChangeLog {
		w.u64(entry.CommitLSN)
		w.str(entry.Operation)
		// OldRow (column-indexed)
		w.writeRowColumnIndexed(entry.OldRow)
		// NewRow (column-indexed)
		w.writeRowColumnIndexed(entry.NewRow)
	}
}

func (r *binReader) readTable() (string, *persistedTable) {
	name := r.str()

	pt := &persistedTable{}

	// Columns
	numCols := int(r.u16())
	pt.Columns = make([]string, numCols)
	for i := 0; i < numCols; i++ {
		pt.Columns[i] = r.str()
	}

	// Column definitions
	numDefs := int(r.u16())
	pt.ColumnDefinitions = make(map[string]ast.ColumnDefinition, numDefs)
	for i := 0; i < numDefs; i++ {
		cd := r.columnDef()
		pt.ColumnDefinitions[cd.Name] = cd
	}

	// Rows (column-indexed)
	numRows := int(r.u32())
	pt.Rows = make([]map[string]ast.Literal, numRows)
	if numRows > 0 {
		// Read column header.
		numRowCols := int(r.u16())
		rowCols := make([]string, numRowCols)
		for i := 0; i < numRowCols; i++ {
			rowCols[i] = r.str()
		}
		bitmapLen := (numRowCols + 7) / 8
		for i := 0; i < numRows; i++ {
			if !r.need(bitmapLen) {
				break
			}
			bitmap := r.data[r.off : r.off+bitmapLen]
			r.off += bitmapLen
			row := make(map[string]ast.Literal, numRowCols)
			for ci, col := range rowCols {
				if bitmap[ci/8]&(1<<(uint(ci)%8)) != 0 {
					row[col] = r.literal()
				}
			}
			pt.Rows[i] = row
		}
	}

	// Indexes (metadata + optional bucket/entry data for v11+)
	numIdx := int(r.u16())
	if numIdx > 0 {
		pt.Indexes = make(map[string]*persistedIndex, numIdx)
		for i := 0; i < numIdx; i++ {
			idxKey := r.str()
			pi := &persistedIndex{}
			pi.Name = r.str()
			pi.Column = r.str()
			numIdxCols := int(r.u16())
			pi.Columns = make([]string, numIdxCols)
			for j := 0; j < numIdxCols; j++ {
				pi.Columns[j] = r.str()
			}
			pi.Kind = r.str()
			// v11+: read data flag and payload
			if r.version >= 11 {
				hasData := r.u8()
				if hasData != 0 {
					pi.DataLoaded = true
					switch pi.Kind {
					case "hash":
						numBuckets := int(r.u32())
						pi.Buckets = make(map[string][]int, numBuckets)
						for b := 0; b < numBuckets; b++ {
							key := r.str()
							numRIDs := int(r.u32())
							rids := make([]int, numRIDs)
							for k := 0; k < numRIDs; k++ {
								rids[k] = int(r.u32())
							}
							pi.Buckets[key] = rids
						}
					case "btree":
						numEntries := int(r.u32())
						pi.Entries = make([]persistedIndexEntry, numEntries)
						for e := 0; e < numEntries; e++ {
							entry := persistedIndexEntry{}
							entry.Value = r.literal()
							numVals := int(r.u16())
							if numVals > 0 {
								entry.Values = make([]ast.Literal, numVals)
								for v := 0; v < numVals; v++ {
									entry.Values[v] = r.literal()
								}
							}
							entry.RowID = int(r.u32())
							pi.Entries[e] = entry
						}
					}
				}
			}
			pt.Indexes[idxKey] = pi
		}
	}

	// IndexedColumns
	numIC := int(r.u16())
	if numIC > 0 {
		pt.IndexedColumns = make(map[string]string, numIC)
		for i := 0; i < numIC; i++ {
			col := r.str()
			idxName := r.str()
			pt.IndexedColumns[col] = idxName
		}
	}

	// IndexedColumnSets
	numICS := int(r.u16())
	if numICS > 0 {
		pt.IndexedColumnSets = make(map[string]string, numICS)
		for i := 0; i < numICS; i++ {
			cols := r.str()
			idxName := r.str()
			pt.IndexedColumnSets[cols] = idxName
		}
	}

	// Primary key
	pt.PrimaryKey = r.str()

	// Unique columns
	numUC := int(r.u16())
	if numUC > 0 {
		pt.UniqueColumns = make([]string, numUC)
		for i := 0; i < numUC; i++ {
			pt.UniqueColumns[i] = r.str()
		}
	}

	// Foreign keys
	numFK := int(r.u16())
	if numFK > 0 {
		pt.ForeignKeys = make([]persistedFK, numFK)
		for i := 0; i < numFK; i++ {
			pt.ForeignKeys[i] = persistedFK{
				Column:           r.str(),
				ReferencesTable:  r.str(),
				ReferencesColumn: r.str(),
			}
		}
	}

	// Check constraints
	numCC := int(r.u16())
	if numCC > 0 {
		pt.CheckConstraints = make([]persistedCheck, numCC)
		for i := 0; i < numCC; i++ {
			pt.CheckConstraints[i] = persistedCheck{
				Column:    r.str(),
				Predicate: r.readPredicate(),
			}
		}
	}

	// Versioned foreign keys
	numVFK := int(r.u16())
	if numVFK > 0 {
		pt.VersionedForeignKeys = make([]persistedVersionedFK, numVFK)
		for i := 0; i < numVFK; i++ {
			pt.VersionedForeignKeys[i] = persistedVersionedFK{
				Column:           r.str(),
				LSNColumn:        r.str(),
				ReferencesDomain: r.str(),
				ReferencesTable:  r.str(),
				ReferencesColumn: r.str(),
			}
		}
	}

	// LastMutationTS
	pt.LastMutationTS = r.u64()

	// Change log (column-indexed, v9)
	numEntries := int(r.u32())
	if numEntries > 0 {
		pt.ChangeLog = make([]persistedChangeLogEntry, numEntries)
		for i := 0; i < numEntries; i++ {
			entry := persistedChangeLogEntry{
				CommitLSN: r.u64(),
				Operation: r.str(),
			}
			entry.OldRow = r.readRowColumnIndexed()
			entry.NewRow = r.readRowColumnIndexed()
			pt.ChangeLog[i] = entry
		}
	}

	return name, pt
}

// ---------- Entity encode/decode ----------

func (w *binWriter) entityDefs(entities map[string]*persistedEntity) {
	w.u16(uint16(len(entities)))
	for name, e := range entities {
		w.str(name)
		w.str(e.Name)
		w.str(e.RootTable)
		w.u16(uint16(len(e.Tables)))
		for _, t := range e.Tables {
			w.str(t)
		}
		w.u16(uint16(len(e.FKPaths)))
		for tableName, hops := range e.FKPaths {
			w.str(tableName)
			w.u16(uint16(len(hops)))
			for _, hop := range hops {
				w.str(hop.FromTable)
				w.str(hop.FromColumn)
				w.str(hop.ToTable)
				w.str(hop.ToColumn)
			}
		}
	}
}

func (r *binReader) readEntityDefs() map[string]*persistedEntity {
	n := int(r.u16())
	if n == 0 {
		return nil
	}
	result := make(map[string]*persistedEntity, n)
	for i := 0; i < n; i++ {
		key := r.str()
		pe := &persistedEntity{}
		pe.Name = r.str()
		pe.RootTable = r.str()
		numTables := int(r.u16())
		pe.Tables = make([]string, numTables)
		for j := 0; j < numTables; j++ {
			pe.Tables[j] = r.str()
		}
		numPaths := int(r.u16())
		if numPaths > 0 {
			pe.FKPaths = make(map[string][]persistedFKHop, numPaths)
			for j := 0; j < numPaths; j++ {
				tableName := r.str()
				numHops := int(r.u16())
				hops := make([]persistedFKHop, numHops)
				for k := 0; k < numHops; k++ {
					hops[k] = persistedFKHop{
						FromTable:  r.str(),
						FromColumn: r.str(),
						ToTable:    r.str(),
						ToColumn:   r.str(),
					}
				}
				pe.FKPaths[tableName] = hops
			}
		}
		result[key] = pe
	}
	return result
}

func (w *binWriter) entityVersionIndexes(indexes map[string]*persistedEntityVersionIndex) {
	w.u16(uint16(len(indexes)))
	for name, idx := range indexes {
		w.str(name)
		w.u32(uint32(len(idx.Versions)))
		for pk, vers := range idx.Versions {
			w.str(pk)
			w.u32(uint32(len(vers)))
			for _, v := range vers {
				w.u64(v.Version)
				w.u64(v.CommitLSN)
				w.u16(uint16(len(v.Tables)))
				for _, t := range v.Tables {
					w.str(t)
				}
			}
		}
	}
}

func (r *binReader) readEntityVersionIndexes() map[string]*persistedEntityVersionIndex {
	n := int(r.u16())
	if n == 0 {
		return nil
	}
	result := make(map[string]*persistedEntityVersionIndex, n)
	for i := 0; i < n; i++ {
		name := r.str()
		numPKs := int(r.u32())
		versions := make(map[string][]persistedEntityVersion, numPKs)
		for j := 0; j < numPKs; j++ {
			pk := r.str()
			numVers := int(r.u32())
			vers := make([]persistedEntityVersion, numVers)
			for k := 0; k < numVers; k++ {
				vers[k] = persistedEntityVersion{
					Version:   r.u64(),
					CommitLSN: r.u64(),
				}
				numTables := int(r.u16())
				vers[k].Tables = make([]string, numTables)
				for l := 0; l < numTables; l++ {
					vers[k].Tables[l] = r.str()
				}
			}
			versions[pk] = vers
		}
		result[name] = &persistedEntityVersionIndex{Versions: versions}
	}
	return result
}

// ---------- Snapshot encode/decode ----------

func (w *binWriter) catalog(snap *engineSnapshot) {
	catDomains := snap.catalog.Domains()
	w.u16(uint16(len(catDomains)))
	for domain, tables := range catDomains {
		w.str(domain)
		w.u16(uint16(len(tables)))
		for table := range tables {
			w.str(table)
		}
	}
}

func (w *binWriter) snapshot(snap *engineSnapshot, isFull bool, prevLogicalTS uint64) {
	w.u64(snap.lsn)
	w.u64(snap.logicalTS)
	w.boolean(isFull)
	w.catalog(snap)

	if isFull {
		// All domains/tables.
		w.u16(uint16(len(snap.state.domains)))
		for domainName, domain := range snap.state.domains {
			w.str(domainName)
			w.u16(uint16(len(domain.tables)))
			for tableName, table := range domain.tables {
				pt := tableStateToMarshalable(table)
				w.table(tableName, pt)
			}
			// Entities
			w.entityDefs(domainEntitiesToMarshalable(domain.entities))
			w.entityVersionIndexes(domainEntityVersionsToMarshalable(domain.entityVersions))
		}
	} else {
		// Delta: only changed tables.
		changed := buildChangedDomains(snap, prevLogicalTS)
		w.u16(uint16(len(changed)))
		for domainName, pd := range changed {
			w.str(domainName)
			w.u16(uint16(len(pd.Tables)))
			for tableName, pt := range pd.Tables {
				w.table(tableName, pt)
			}
			// Entities
			w.entityDefs(pd.Entities)
			w.entityVersionIndexes(pd.EntityVersions)
		}
	}
}

// encodeSnapshotFileBinary encodes a single snapshot into a binary file.
// isFull=true writes all domains; isFull=false writes only tables mutated after
// prevLogicalTS (delta mode for cross-file snapshot chains).
// Callers should use seq%fullSnapshotFrequency==0 to determine isFull.
func encodeSnapshotFileBinary(snap *engineSnapshot, isFull bool, prevLogicalTS uint64) ([]byte, error) {
	if snap == nil {
		return nil, nil
	}
	// Pass 1: collect strings for dictionary.
	dict := newStringDict()
	pass1 := &binWriter{dict: dict}
	pass1.snapshot(snap, isFull, prevLogicalTS)

	// Pass 2: write full payload.
	w := &binWriter{dict: dict}
	w.buf.Write(snapMagic[:])
	w.u8(snapVersion)

	// String dictionary.
	w.u32(uint32(len(dict.strings)))
	for _, s := range dict.strings {
		b := []byte(s)
		w.u16(uint16(len(b)))
		w.buf.Write(b)
	}

	w.u32(1) // exactly one snapshot per file
	w.snapshot(snap, isFull, prevLogicalTS)

	data := w.bytes()
	checksum := crc32.Checksum(data, snapCRC32C)
	w.u32(checksum)
	return w.bytes(), nil
}

// rawSnapshotFileEntry is the unresolved decoded content of a single disk file.
// isFull=false means domains contains only the changed tables (delta file).
type rawSnapshotFileEntry struct {
	lsn       uint64
	logicalTS uint64
	isFull    bool
	catalog   persistedCatalog
	domains   map[string]*persistedDomain
}

// decodeSnapshotFileBinaryRaw decodes a binary snapshot file and returns the raw
// entries without applying cross-file delta resolution. This is used by
// readAllSnapshotsFromDir to merge delta files with their base snapshots.
func decodeSnapshotFileBinaryRaw(data []byte) ([]rawSnapshotFileEntry, error) {
	if len(data) < 13 {
		return nil, fmt.Errorf("snapshot binary: data too short")
	}
	if data[0] != snapMagic[0] || data[1] != snapMagic[1] || data[2] != snapMagic[2] || data[3] != snapMagic[3] {
		return nil, fmt.Errorf("snapshot binary: invalid magic %x", data[:4])
	}

	payloadEnd := len(data) - 4
	storedCRC := binary.BigEndian.Uint32(data[payloadEnd:])
	computedCRC := crc32.Checksum(data[:payloadEnd], snapCRC32C)
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("snapshot binary: CRC mismatch stored=%08x computed=%08x", storedCRC, computedCRC)
	}

	r := &binReader{data: data[:payloadEnd]}
	r.off = 4

	version := r.u8()
	if version != snapVersion {
		return nil, fmt.Errorf("snapshot binary: unsupported version %d (expected %d)", version, snapVersion)
	}
	r.version = version

	numStrings := int(r.u32())
	r.dictTable = make([]string, numStrings)
	for i := 0; i < numStrings; i++ {
		n := int(r.u16())
		if !r.need(n) {
			return nil, fmt.Errorf("snapshot binary: dictionary string %d truncated", i)
		}
		r.dictTable[i] = string(r.data[r.off : r.off+n])
		r.off += n
	}

	numSnapshots := int(r.u32())
	if numSnapshots == 0 {
		return nil, nil
	}

	entries := make([]rawSnapshotFileEntry, 0, numSnapshots)
	for i := 0; i < numSnapshots; i++ {
		lsn := r.u64()
		logicalTS := r.u64()
		isFull := r.boolean()
		catalog := r.readCatalog()
		domains := r.readDomains()
		if r.err != nil {
			return nil, r.err
		}
		entries = append(entries, rawSnapshotFileEntry{
			lsn:       lsn,
			logicalTS: logicalTS,
			isFull:    isFull,
			catalog:   catalog,
			domains:   domains,
		})
	}
	return entries, nil
}

// encodeSnapshotsBinary encodes all snapshots into binary format.
// Version 9: two-pass encoding — first collects all unique strings into a
// dictionary, then serializes snapshots using dictionary references.
func encodeSnapshotsBinary(store *snapshotStore) ([]byte, error) {
	if store == nil || len(store.snapshots) == 0 {
		return nil, nil
	}

	// --- Pass 1: collect all unique strings into a dictionary ---
	dict := newStringDict()
	collectW := &binWriter{dict: dict}
	for i := range store.snapshots {
		snap := &store.snapshots[i]
		isFull := (i%fullSnapshotFrequency == 0)
		var prevTS uint64
		if !isFull && i > 0 {
			prevTS = store.snapshots[i-1].logicalTS
		}
		collectW.snapshot(snap, isFull, prevTS)
	}

	// --- Pass 2: encode with dictionary ---
	w := &binWriter{dict: dict}

	// Header
	w.buf.Write(snapMagic[:])
	w.u8(snapVersion)

	// String dictionary table
	w.u32(uint32(len(dict.strings)))
	for _, s := range dict.strings {
		b := []byte(s)
		w.u16(uint16(len(b)))
		w.buf.Write(b)
	}

	// Snapshots
	w.u32(uint32(len(store.snapshots)))
	for i := range store.snapshots {
		snap := &store.snapshots[i]
		isFull := (i%fullSnapshotFrequency == 0)

		var prevTS uint64
		if !isFull && i > 0 {
			prevTS = store.snapshots[i-1].logicalTS
		}

		w.snapshot(snap, isFull, prevTS)
	}

	// CRC32C over everything written so far.
	data := w.bytes()
	checksum := crc32.Checksum(data, snapCRC32C)
	w.u32(checksum)

	return w.bytes(), nil
}

// ---------- Decode ----------

func (r *binReader) readCatalog() persistedCatalog {
	pc := persistedCatalog{Domains: make(map[string][]string)}
	numDomains := int(r.u16())
	for i := 0; i < numDomains; i++ {
		domain := r.str()
		numTables := int(r.u16())
		tables := make([]string, numTables)
		for j := 0; j < numTables; j++ {
			tables[j] = r.str()
		}
		pc.Domains[domain] = tables
	}
	return pc
}

func (r *binReader) readDomains() map[string]*persistedDomain {
	numDomains := int(r.u16())
	result := make(map[string]*persistedDomain, numDomains)
	for i := 0; i < numDomains; i++ {
		domainName := r.str()
		numTables := int(r.u16())
		pd := &persistedDomain{Tables: make(map[string]*persistedTable, numTables)}
		for j := 0; j < numTables; j++ {
			tableName, pt := r.readTable()
			pd.Tables[tableName] = pt
		}
		// Entities
		pd.Entities = r.readEntityDefs()
		pd.EntityVersions = r.readEntityVersionIndexes()
		result[domainName] = pd
	}
	return result
}

// decodeSnapshotsBinary decodes binary snapshot data (v11 format with string dictionary and CRC32C).
func decodeSnapshotsBinary(data []byte) ([]engineSnapshot, error) {
	if len(data) < 13 { // magic(4) + version(1) + count(4) + crc(4)
		return nil, errors.New("snapshot binary: data too short")
	}

	// Verify magic.
	if data[0] != snapMagic[0] || data[1] != snapMagic[1] || data[2] != snapMagic[2] || data[3] != snapMagic[3] {
		return nil, fmt.Errorf("snapshot binary: invalid magic %x", data[:4])
	}

	// Verify CRC32C: last 4 bytes are checksum over everything before.
	payloadEnd := len(data) - 4
	storedCRC := binary.BigEndian.Uint32(data[payloadEnd:])
	computedCRC := crc32.Checksum(data[:payloadEnd], snapCRC32C)
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("snapshot binary: CRC mismatch stored=%08x computed=%08x", storedCRC, computedCRC)
	}

	r := &binReader{data: data[:payloadEnd]}
	r.off = 4 // skip magic

	version := r.u8()
	if version != snapVersion {
		return nil, fmt.Errorf("snapshot binary: unsupported version %d (expected %d)", version, snapVersion)
	}
	r.version = version

	// Read string dictionary.
	numStrings := int(r.u32())
	r.dictTable = make([]string, numStrings)
	for i := 0; i < numStrings; i++ {
		n := int(r.u16())
		if !r.need(n) {
			return nil, fmt.Errorf("snapshot binary: dictionary string %d truncated", i)
		}
		r.dictTable[i] = string(r.data[r.off : r.off+n])
		r.off += n
	}
	if r.err != nil {
		return nil, r.err
	}

	numSnapshots := int(r.u32())
	if numSnapshots == 0 {
		return nil, nil
	}

	// Accumulated state for delta reconstruction (same logic as V2 JSON).
	var accumulated map[string]*persistedDomain
	result := make([]engineSnapshot, numSnapshots)

	for i := 0; i < numSnapshots; i++ {
		lsn := r.u64()
		logicalTS := r.u64()
		isFull := r.boolean()
		catalog := r.readCatalog()
		domains := r.readDomains()

		if r.err != nil {
			return nil, r.err
		}

		if isFull {
			accumulated = domains
		} else {
			accumulated = applyDeltaBinary(accumulated, domains, catalog)
		}

		full := persistedSnapshot{
			LSN:       lsn,
			LogicalTS: logicalTS,
			Catalog:   catalog,
			Domains:   deepCopyPersistedDomains(accumulated),
		}

		result[i] = marshalableToSnapshot(full)
		rebuildAllIndexes(&result[i])
	}

	return result, nil
}

// applyDeltaBinary merges delta domains into accumulated state, removing
// domains/tables absent from the catalog. Entities are carried forward and
// overlaid from deltas.
func applyDeltaBinary(
	accumulated map[string]*persistedDomain,
	delta map[string]*persistedDomain,
	catalog persistedCatalog,
) map[string]*persistedDomain {
	// Start with a copy of accumulated.
	result := make(map[string]*persistedDomain, len(accumulated))
	for name, domain := range accumulated {
		pd := &persistedDomain{Tables: make(map[string]*persistedTable, len(domain.Tables))}
		for tName, tbl := range domain.Tables {
			pd.Tables[tName] = tbl // shallow ref — immutable at this point
		}
		// Carry forward entities
		pd.Entities = domain.Entities
		pd.EntityVersions = domain.EntityVersions
		result[name] = pd
	}

	// Overlay delta.
	for domainName, deltaDomain := range delta {
		existing, exists := result[domainName]
		if !exists {
			existing = &persistedDomain{Tables: make(map[string]*persistedTable)}
			result[domainName] = existing
		}
		for tableName, dt := range deltaDomain.Tables {
			existing.Tables[tableName] = dt
		}
		// Overlay entities from delta if present
		if deltaDomain.Entities != nil {
			existing.Entities = deltaDomain.Entities
		}
		if deltaDomain.EntityVersions != nil {
			existing.EntityVersions = deltaDomain.EntityVersions
		}
	}

	// Prune domains/tables not in catalog.
	for domainName := range result {
		if _, inCatalog := catalog.Domains[domainName]; !inCatalog {
			delete(result, domainName)
		}
	}
	for domainName, domain := range result {
		catalogTables := make(map[string]struct{})
		for _, t := range catalog.Domains[domainName] {
			catalogTables[t] = struct{}{}
		}
		for tableName := range domain.Tables {
			if _, inCatalog := catalogTables[tableName]; !inCatalog {
				delete(domain.Tables, tableName)
			}
		}
	}

	return result
}
