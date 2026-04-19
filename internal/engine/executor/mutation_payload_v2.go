package executor

// mutation_payload_v2.go — compact binary mutation payload (sole format).
//
// All WAL MUTATION records use this format. There is no v1 format in this
// codebase; v1 (SQL text) has been removed.
//
// Wire format:
//   [0x02]                      — version marker
//   [uvarint domainLen][domain] — domain name
//   [1B op]                     — 1=INSERT, 2=UPDATE, 3=DELETE, 4=DDL-SQL(legacy), 5=DDL-struct
//   op-specific body (see inline docs below)
//
// INSERT body:
//   [uvarint numCols][col0 … colN-1]
//   [uvarint numRows][for each row: numCols literals]
//   [onConflict]
//
// UPDATE body:
//   [uvarint numSetCols][for each: col + updateExpr]
//   [predicate]
//
// DELETE body:
//   [predicate]
//
// DDL-SQL body (op=0x04, legacy — backward compat only, never written by new code):
//   [uvarint sqlLen][sql bytes]
//   (decoded by parsing SQL into a Plan at replay time)
//
// DDL-struct body (op=0x05, current format — parser-free at replay):
//   [1B ddl_sub_op]  — identifies the DDL operation
//   sub-op-specific binary fields (see inline docs and encodeDDLStructPayload)
//
// DDL sub-op codes:
//   0x01 CreateTable          0x02 AlterTableAddColumn
//   0x03 AlterTableDropColumn 0x04 AlterTableRenameColumn
//   0x05 CreateIndex          0x06 CreateEntity
//   0x07 DropTable            0x08 DropIndex
//   0x09 TruncateTable
//
// ColumnDefinition flags byte (bitmask in encodeColumnDef):
//   bit0 PrimaryKey  bit1 Unique  bit2 NotNull
//   bit3 HasRefs     bit4 HasCheck  bit5 HasDefault
//
// DefaultExpr kind byte:
//   0x01 literal  0x02 autoincrement  0x03 uuid_v7
//
// UpdateExpr encoding — 1B kind tag (used in UPDATE SET body):
//   0x01 literal     — [literal]
//   0x02 arithmetic  — [uvarint srcColLen][srcCol][1B arithOp][literal operand]
//        arithOp: 0x01=+ 0x02=- 0x03=* 0x04=/
//
// OnConflict encoding — 1B action tag:
//   0x00 none
//   0x01 DO NOTHING  — [uvarint numConflictCols][cols…]
//   0x02 DO UPDATE   — [uvarint numConflictCols][cols…]
//                      [uvarint numUpdateCols]
//                      for each: [col][1B kind: 0x01=literal 0x02=excluded]
//                                if literal:  [literal]
//                                if excluded: [excludedColStr]
//   UpdateColumns/UpdateValues/UpdateExcluded are parallel arrays.
//   UpdateExcluded[i]=="" means literal; UpdateExcluded[i]!="" means EXCLUDED ref.
//
// Literal encoding — 1B kind tag:
//   0x00 null       — no payload
//   0x01 string     — [uvarint len][bytes]
//   0x02 int64      — [8B LE]
//   0x03 float64    — [8B LE]
//   0x04 bool true  — no payload
//   0x05 bool false — no payload
//   0x06 JSON       — [uvarint len][bytes]
//   0x07 UUID       — [16B binary] (decoded from 36-char UUID string)
//   0x08 bytes      — [uvarint len][bytes]
//
// Predicate encoding — 1B kind tag:
//   0x00 nil
//   0x10 leaf CMP   [1B cmpOp][uvarint colLen][col][literal]
//        cmpOp: 0x01= 0x02!= 0x03< 0x04<= 0x05> 0x06>=
//   0x11 IS NULL    [uvarint colLen][col]
//   0x12 IS NOT NULL[uvarint colLen][col]
//   0x13 IN         [uvarint colLen][col][uvarint n][literals]
//   0x14 NOT IN     [uvarint colLen][col][uvarint n][literals]
//   0x20 AND        [pred][pred]
//   0x21 OR         [pred][pred]
//   0x22 NOT        [pred]

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/correodabid/asql/internal/engine/parser"
	"github.com/correodabid/asql/internal/engine/parser/ast"
	"github.com/correodabid/asql/internal/engine/planner"
)

// payloadVersion is the leading byte of every v2 payload.
const payloadVersion byte = 0x02

// op codes
const (
	v2OpInsert    byte = 0x01
	v2OpUpdate    byte = 0x02
	v2OpDelete    byte = 0x03
	v2OpDDL       byte = 0x04 // legacy: SQL text payload (backward compat, never written by new code)
	v2OpDDLStruct byte = 0x05 // structured DDL: parser-free binary encoding
)

// DDL sub-operation codes (written after v2OpDDLStruct)
const (
	ddlSubCreateTable       byte = 0x01
	ddlSubAlterAddColumn    byte = 0x02
	ddlSubAlterDropColumn   byte = 0x03
	ddlSubAlterRenameColumn byte = 0x04
	ddlSubCreateIndex       byte = 0x05
	ddlSubCreateEntity      byte = 0x06
	ddlSubDropTable         byte = 0x07
	ddlSubDropIndex         byte = 0x08
	ddlSubTruncateTable     byte = 0x09
)

// ColumnDefinition flag bits (used in encodeColumnDef)
const (
	colFlagPrimaryKey byte = 0x01
	colFlagUnique     byte = 0x02
	colFlagNotNull    byte = 0x04
	colFlagHasRefs    byte = 0x08
	colFlagHasCheck   byte = 0x10
	colFlagHasDefault byte = 0x20
)

// DefaultExpr kind bytes (used in encodeDefaultExpr)
const (
	defaultKindLiteral       byte = 0x01
	defaultKindAutoIncrement byte = 0x02
	defaultKindUUIDv7        byte = 0x03
	defaultKindTxTimestamp   byte = 0x04
)

// literal kind bytes
const (
	litNull      byte = 0x00
	litString    byte = 0x01
	litInt64     byte = 0x02
	litFloat64   byte = 0x03
	litBoolTrue  byte = 0x04
	litBoolFalse byte = 0x05
	litJSON      byte = 0x06
	litUUID      byte = 0x07
	litTimestamp byte = 0x08 // stored as string (ISO-8601 / epoch-millis string)
)

// predicate kind bytes
const (
	predNil       byte = 0x00
	predCMP       byte = 0x10
	predIsNull    byte = 0x11
	predIsNotNull byte = 0x12
	predIn        byte = 0x13
	predNotIn     byte = 0x14
	predAnd       byte = 0x20
	predOr        byte = 0x21
	predNot       byte = 0x22
)

// cmpOp sub-codes inside predCMP
const (
	cmpEq  byte = 0x01
	cmpNeq byte = 0x02
	cmpLt  byte = 0x03
	cmpLte byte = 0x04
	cmpGt  byte = 0x05
	cmpGte byte = 0x06
)

// updateExpr kind bytes
const (
	updExprLiteral    byte = 0x01
	updExprArithmetic byte = 0x02
)

// arithmetic operator bytes inside updExprArithmetic
const (
	arithAdd byte = 0x01
	arithSub byte = 0x02
	arithMul byte = 0x03
	arithDiv byte = 0x04
)

// onConflict action bytes
const (
	ocNone      byte = 0x00
	ocDoNothing byte = 0x01
	ocDoUpdate  byte = 0x02
)

// ─── write buffer ─────────────────────────────────────────────────────────────

type v2Buf struct{ b []byte }

func (w *v2Buf) writeByte(b byte)  { w.b = append(w.b, b) }
func (w *v2Buf) writeU8(v byte)    { w.b = append(w.b, v) }
func (w *v2Buf) writeUv(v uint64)  { w.b = binary.AppendUvarint(w.b, v) }
func (w *v2Buf) writeStr(s string) { w.writeUv(uint64(len(s))); w.b = append(w.b, s...) }
func (w *v2Buf) writeRaw(p []byte) { w.writeUv(uint64(len(p))); w.b = append(w.b, p...) }
func (w *v2Buf) writeU64(v uint64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	w.b = append(w.b, tmp[:]...)
}

// ─── literal encode ───────────────────────────────────────────────────────────

func isUUIDStr(s string) bool {
	return len(s) == 36 && s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-'
}

func uuidTo16(s string) ([]byte, bool) {
	plain := s[:8] + s[9:13] + s[14:18] + s[19:23] + s[24:]
	b, err := hex.DecodeString(plain)
	if err != nil || len(b) != 16 {
		return nil, false
	}
	return b, true
}

func uuid16ToStr(b []byte) string {
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func encodeLit(w *v2Buf, lit ast.Literal) {
	switch lit.Kind {
	case ast.LiteralNull, "":
		w.writeByte(litNull)
	case ast.LiteralString:
		if isUUIDStr(lit.StringValue) {
			if raw, ok := uuidTo16(lit.StringValue); ok {
				w.writeByte(litUUID)
				w.b = append(w.b, raw...)
				return
			}
		}
		w.writeByte(litString)
		w.writeStr(lit.StringValue)
	case ast.LiteralNumber:
		w.writeByte(litInt64)
		w.writeU64(uint64(lit.NumberValue))
	case ast.LiteralFloat:
		w.writeByte(litFloat64)
		w.writeU64(math.Float64bits(lit.FloatValue))
	case ast.LiteralBoolean:
		if lit.BoolValue {
			w.writeByte(litBoolTrue)
		} else {
			w.writeByte(litBoolFalse)
		}
	case ast.LiteralJSON:
		w.writeByte(litJSON)
		w.writeStr(lit.StringValue)
	case ast.LiteralTimestamp:
		w.writeByte(litTimestamp)
		w.writeStr(lit.StringValue)
	default:
		w.writeByte(litNull) // unknown kind → null; must not happen
	}
}

// ─── updateExpr encode/decode ────────────────────────────────────────────────

func encodeUpdateExpr(w *v2Buf, e ast.UpdateExpr) {
	switch e.Kind {
	case ast.UpdateExprArithmetic:
		w.writeByte(updExprArithmetic)
		w.writeStr(e.Column)
		var op byte
		switch e.Operator {
		case "+":
			op = arithAdd
		case "-":
			op = arithSub
		case "*":
			op = arithMul
		case "/":
			op = arithDiv
		default:
			op = arithAdd // safe default; unknown ops should not reach here
		}
		w.writeByte(op)
		encodeLit(w, e.Operand)
	default: // UpdateExprLiteral + catch-all
		w.writeByte(updExprLiteral)
		encodeLit(w, e.Value)
	}
}

func decodeUpdateExpr(r *v2Reader) (ast.UpdateExpr, error) {
	kind, err := r.readByte()
	if err != nil {
		return ast.UpdateExpr{}, err
	}
	switch kind {
	case updExprArithmetic:
		col, err := r.readStr()
		if err != nil {
			return ast.UpdateExpr{}, err
		}
		op, err := r.readByte()
		if err != nil {
			return ast.UpdateExpr{}, err
		}
		var opStr string
		switch op {
		case arithAdd:
			opStr = "+"
		case arithSub:
			opStr = "-"
		case arithMul:
			opStr = "*"
		case arithDiv:
			opStr = "/"
		default:
			return ast.UpdateExpr{}, fmt.Errorf("payloadv2: unknown arithOp 0x%02x", op)
		}
		operand, err := decodeLit(r)
		if err != nil {
			return ast.UpdateExpr{}, err
		}
		return ast.UpdateExpr{Kind: ast.UpdateExprArithmetic, Column: col, Operator: opStr, Operand: operand}, nil
	case updExprLiteral:
		val, err := decodeLit(r)
		if err != nil {
			return ast.UpdateExpr{}, err
		}
		return ast.UpdateExpr{Kind: ast.UpdateExprLiteral, Value: val}, nil
	default:
		return ast.UpdateExpr{}, fmt.Errorf("payloadv2: unknown updateExpr kind 0x%02x", kind)
	}
}

// ─── onConflict encode/decode ─────────────────────────────────────────────────

// Per-column kind tags used inside DO UPDATE encoding.
// UpdateColumns, UpdateValues, and UpdateExcluded are parallel arrays:
//
//	UpdateExcluded[i] != "" → EXCLUDED.col reference for column i
//	UpdateExcluded[i] == "" → literal value UpdateValues[i] for column i
const (
	ocColLiteral  byte = 0x01
	ocColExcluded byte = 0x02
)

func encodeOnConflict(w *v2Buf, oc *ast.OnConflictClause) {
	if oc == nil {
		w.writeByte(ocNone)
		return
	}
	switch oc.Action {
	case ast.OnConflictDoNothing:
		w.writeByte(ocDoNothing)
		w.writeUv(uint64(len(oc.ConflictColumns)))
		for _, c := range oc.ConflictColumns {
			w.writeStr(c)
		}
	case ast.OnConflictDoUpdate:
		w.writeByte(ocDoUpdate)
		w.writeUv(uint64(len(oc.ConflictColumns)))
		for _, c := range oc.ConflictColumns {
			w.writeStr(c)
		}
		w.writeUv(uint64(len(oc.UpdateColumns)))
		for i, col := range oc.UpdateColumns {
			w.writeStr(col)
			// UpdateExcluded[i] != "" means EXCLUDED.col reference; else literal.
			if i < len(oc.UpdateExcluded) && oc.UpdateExcluded[i] != "" {
				w.writeByte(ocColExcluded)
				w.writeStr(oc.UpdateExcluded[i])
			} else {
				w.writeByte(ocColLiteral)
				if i < len(oc.UpdateValues) {
					encodeLit(w, oc.UpdateValues[i])
				} else {
					encodeLit(w, ast.Literal{Kind: ast.LiteralNull})
				}
			}
		}
	default:
		w.writeByte(ocNone)
	}
}

func decodeOnConflict(r *v2Reader) (*ast.OnConflictClause, error) {
	action, err := r.readByte()
	if err != nil {
		return nil, err
	}
	if action == ocNone {
		return nil, nil
	}
	numConflict, err := r.readUv()
	if err != nil {
		return nil, err
	}
	conflictCols := make([]string, numConflict)
	for i := range conflictCols {
		conflictCols[i], err = r.readStr()
		if err != nil {
			return nil, err
		}
	}
	oc := &ast.OnConflictClause{ConflictColumns: conflictCols}
	switch action {
	case ocDoNothing:
		oc.Action = ast.OnConflictDoNothing
	case ocDoUpdate:
		oc.Action = ast.OnConflictDoUpdate
		numUpdate, err := r.readUv()
		if err != nil {
			return nil, err
		}
		// All three slices are parallel to UpdateColumns.
		oc.UpdateColumns = make([]string, numUpdate)
		oc.UpdateValues = make([]ast.Literal, numUpdate)
		oc.UpdateExcluded = make([]string, numUpdate) // "" means literal for that index
		for i := range oc.UpdateColumns {
			oc.UpdateColumns[i], err = r.readStr()
			if err != nil {
				return nil, fmt.Errorf("payloadv2: onConflict updateCol[%d]: %w", i, err)
			}
			kind, err := r.readByte()
			if err != nil {
				return nil, fmt.Errorf("payloadv2: onConflict colKind[%d]: %w", i, err)
			}
			switch kind {
			case ocColLiteral:
				oc.UpdateValues[i], err = decodeLit(r)
				if err != nil {
					return nil, fmt.Errorf("payloadv2: onConflict lit[%d]: %w", i, err)
				}
				// oc.UpdateExcluded[i] stays ""
			case ocColExcluded:
				oc.UpdateExcluded[i], err = r.readStr()
				if err != nil {
					return nil, fmt.Errorf("payloadv2: onConflict excluded[%d]: %w", i, err)
				}
			default:
				return nil, fmt.Errorf("payloadv2: unknown onConflict colKind 0x%02x at index %d", kind, i)
			}
		}
	default:
		return nil, fmt.Errorf("payloadv2: unknown onConflict action 0x%02x", action)
	}
	return oc, nil
}

// ─── predicate encode ─────────────────────────────────────────────────────────

// encodePred returns false for constructs not expressible in the binary format
// (BETWEEN, LIKE, JsonAccess column expressions, subqueries). Caller promotes
// the whole payload to v2OpDDL SQL-text fallback.
func encodePred(w *v2Buf, p *ast.Predicate) bool {
	if p == nil {
		w.writeByte(predNil)
		return true
	}
	if p.JsonAccess != nil || p.Subquery != nil {
		return false
	}

	op := strings.ToUpper(strings.TrimSpace(p.Operator))
	switch op {
	case "AND":
		w.writeByte(predAnd)
		return encodePred(w, p.Left) && encodePred(w, p.Right)
	case "OR":
		w.writeByte(predOr)
		return encodePred(w, p.Left) && encodePred(w, p.Right)
	case "NOT":
		w.writeByte(predNot)
		return encodePred(w, p.Left)
	case "IS NULL":
		w.writeByte(predIsNull)
		w.writeStr(p.Column)
		return true
	case "IS NOT NULL":
		w.writeByte(predIsNotNull)
		w.writeStr(p.Column)
		return true
	case "IN", "NOT IN":
		if len(p.InValues) == 0 {
			return false
		}
		if op == "IN" {
			w.writeByte(predIn)
		} else {
			w.writeByte(predNotIn)
		}
		w.writeStr(p.Column)
		w.writeUv(uint64(len(p.InValues)))
		for _, v := range p.InValues {
			encodeLit(w, v)
		}
		return true
	}

	// leaf comparison
	var cmpCode byte
	switch op {
	case "=":
		cmpCode = cmpEq
	case "!=", "<>":
		cmpCode = cmpNeq
	case "<":
		cmpCode = cmpLt
	case "<=":
		cmpCode = cmpLte
	case ">":
		cmpCode = cmpGt
	case ">=":
		cmpCode = cmpGte
	default:
		return false // BETWEEN, LIKE, ILIKE, etc.
	}
	w.writeByte(predCMP)
	w.writeU8(cmpCode)
	w.writeStr(p.Column)
	encodeLit(w, p.Value)
	return true
}

// ─── public API ───────────────────────────────────────────────────────────────

// encodeMutationPayload serialises a mutation into a binary v2 payload.
// DML (INSERT/UPDATE/DELETE) is fully binary-encoded; rare predicate constructs
// (JsonAccess, subqueries, LIKE, BETWEEN) fall back to legacy SQL text.
// DDL is always structurally binary-encoded (v2OpDDLStruct); no SQL text stored.
//
// sql is only consumed for the rare DML predicate fallback; it is ignored for DDL.
func encodeMutationPayloadV2(domain string, plan planner.Plan, sql string) []byte {
	switch plan.Operation {
	case planner.OperationInsert, planner.OperationUpdate, planner.OperationDelete:
		if p, ok := encodeDMLPayload(domain, plan); ok {
			return p
		}
		// Predicate contained JsonAccess/subquery/LIKE/BETWEEN — store as SQL.
		return encodeDDLPayload(domain, sql)
	case planner.OperationCreateTable,
		planner.OperationAlterTableAddColumn,
		planner.OperationAlterTableDropColumn,
		planner.OperationAlterTableRenameColumn,
		planner.OperationCreateIndex,
		planner.OperationCreateEntity,
		planner.OperationDropTable,
		planner.OperationDropIndex,
		planner.OperationTruncateTable:
		return encodeDDLStructPayload(domain, plan)
	default:
		// SELECT, SetOp, or future ops not yet handled — fall back to SQL.
		return encodeDDLPayload(domain, sql)
	}
}

func encodeDMLPayload(domain string, plan planner.Plan) ([]byte, bool) {
	w := &v2Buf{b: make([]byte, 0, 128)}
	w.writeByte(payloadVersion)
	w.writeStr(domain)

	switch plan.Operation {
	case planner.OperationInsert:
		w.writeByte(v2OpInsert)
		w.writeStr(plan.TableName)
		w.writeUv(uint64(len(plan.Columns)))
		for _, c := range plan.Columns {
			w.writeStr(c)
		}
		numRows := 1 + len(plan.MultiValues)
		w.writeUv(uint64(numRows))
		for _, v := range plan.Values {
			encodeLit(w, v)
		}
		for _, row := range plan.MultiValues {
			for _, v := range row {
				encodeLit(w, v)
			}
		}
		encodeOnConflict(w, plan.OnConflict)

	case planner.OperationUpdate:
		w.writeByte(v2OpUpdate)
		w.writeStr(plan.TableName)
		w.writeUv(uint64(len(plan.Columns)))
		for i, c := range plan.Columns {
			w.writeStr(c)
			// Use UpdateExprs when present (arithmetic), else plain literal.
			if i < len(plan.UpdateExprs) && plan.UpdateExprs[i].Kind == ast.UpdateExprArithmetic {
				encodeUpdateExpr(w, plan.UpdateExprs[i])
			} else if i < len(plan.Values) {
				w.writeByte(updExprLiteral)
				encodeLit(w, plan.Values[i])
			} else {
				w.writeByte(updExprLiteral)
				encodeLit(w, ast.Literal{Kind: ast.LiteralNull})
			}
		}
		if !encodePred(w, plan.Filter) {
			return nil, false
		}

	case planner.OperationDelete:
		w.writeByte(v2OpDelete)
		w.writeStr(plan.TableName)
		if !encodePred(w, plan.Filter) {
			return nil, false
		}
	}

	return w.b, true
}

// encodeDDLStructPayload serialises a DDL plan into a structured binary payload
// (op code v2OpDDLStruct). No SQL text is stored; the plan is reconstructed
// directly from the binary data at replay — no parser involvement.
func encodeDDLStructPayload(domain string, plan planner.Plan) []byte {
	w := &v2Buf{b: make([]byte, 0, 64)}
	w.writeByte(payloadVersion)
	w.writeStr(domain)
	w.writeByte(v2OpDDLStruct)

	switch plan.Operation {
	case planner.OperationCreateTable:
		w.writeByte(ddlSubCreateTable)
		var flags byte
		if plan.IfNotExists {
			flags |= 0x01
		}
		w.writeByte(flags)
		w.writeStr(plan.TableName)
		w.writeUv(uint64(len(plan.Schema)))
		for _, col := range plan.Schema {
			encodeColumnDef(w, col)
		}
		w.writeUv(uint64(len(plan.VersionedForeignKeys)))
		for _, vfk := range plan.VersionedForeignKeys {
			encodeVersionedFK(w, vfk)
		}

	case planner.OperationAlterTableAddColumn:
		w.writeByte(ddlSubAlterAddColumn)
		w.writeStr(plan.TableName)
		if plan.AlterColumn != nil {
			encodeColumnDef(w, *plan.AlterColumn)
		} else {
			encodeColumnDef(w, ast.ColumnDefinition{})
		}

	case planner.OperationAlterTableDropColumn:
		w.writeByte(ddlSubAlterDropColumn)
		w.writeStr(plan.TableName)
		w.writeStr(plan.DropColumnName)

	case planner.OperationAlterTableRenameColumn:
		w.writeByte(ddlSubAlterRenameColumn)
		w.writeStr(plan.TableName)
		w.writeStr(plan.RenameOldColumn)
		w.writeStr(plan.RenameNewColumn)

	case planner.OperationCreateIndex:
		w.writeByte(ddlSubCreateIndex)
		var flags byte
		if plan.IfNotExists {
			flags |= 0x01
		}
		w.writeByte(flags)
		w.writeStr(plan.TableName)
		w.writeStr(plan.IndexName)
		w.writeStr(plan.IndexColumn)
		w.writeUv(uint64(len(plan.IndexColumns)))
		for _, col := range plan.IndexColumns {
			w.writeStr(col)
		}
		w.writeStr(plan.IndexMethod)

	case planner.OperationCreateEntity:
		w.writeByte(ddlSubCreateEntity)
		var flags byte
		if plan.IfNotExists {
			flags |= 0x01
		}
		w.writeByte(flags)
		w.writeStr(plan.EntityName)
		w.writeStr(plan.EntityRootTable)
		w.writeUv(uint64(len(plan.EntityTables)))
		for _, t := range plan.EntityTables {
			w.writeStr(t)
		}

	case planner.OperationDropTable:
		w.writeByte(ddlSubDropTable)
		var flags byte
		if plan.IfExists {
			flags |= 0x01
		}
		if plan.Cascade {
			flags |= 0x02
		}
		w.writeByte(flags)
		w.writeStr(plan.TableName)

	case planner.OperationDropIndex:
		w.writeByte(ddlSubDropIndex)
		var flags byte
		if plan.IfExists {
			flags |= 0x01
		}
		w.writeByte(flags)
		w.writeStr(plan.IndexName)
		w.writeStr(plan.TableName)

	case planner.OperationTruncateTable:
		w.writeByte(ddlSubTruncateTable)
		w.writeStr(plan.TableName)

	default:
		// should not happen: caller must only pass DDL operations
		panic(fmt.Sprintf("encodeDDLStructPayload: unhandled DDL operation %q", plan.Operation))
	}

	return w.b
}

// encodeColumnDef serialises an ast.ColumnDefinition into w.
func encodeColumnDef(w *v2Buf, col ast.ColumnDefinition) {
	w.writeStr(col.Name)
	w.writeStr(string(col.Type))
	var flags byte
	if col.PrimaryKey {
		flags |= colFlagPrimaryKey
	}
	if col.Unique {
		flags |= colFlagUnique
	}
	if col.NotNull {
		flags |= colFlagNotNull
	}
	if col.ReferencesTable != "" {
		flags |= colFlagHasRefs
	}
	if col.Check != nil {
		flags |= colFlagHasCheck
	}
	if col.DefaultValue != nil {
		flags |= colFlagHasDefault
	}
	w.writeByte(flags)
	if flags&colFlagHasRefs != 0 {
		w.writeStr(col.ReferencesTable)
		w.writeStr(col.ReferencesColumn)
	}
	if flags&colFlagHasCheck != 0 {
		// CHECK predicates in column definitions are always simple comparisons;
		// encodePred is always able to encode them (no JsonAccess/subquery/LIKE).
		encodePred(w, col.Check)
	}
	if flags&colFlagHasDefault != 0 {
		encodeDefaultExpr(w, col.DefaultValue)
	}
}

// encodeDefaultExpr serialises an ast.DefaultExpr into w.
func encodeDefaultExpr(w *v2Buf, d *ast.DefaultExpr) {
	switch d.Kind {
	case ast.DefaultLiteral:
		w.writeByte(defaultKindLiteral)
		encodeLit(w, d.Value)
	case ast.DefaultAutoIncrement:
		w.writeByte(defaultKindAutoIncrement)
	case ast.DefaultUUIDv7:
		w.writeByte(defaultKindUUIDv7)
	case ast.DefaultTxTimestamp:
		w.writeByte(defaultKindTxTimestamp)
	default:
		w.writeByte(defaultKindLiteral)
		encodeLit(w, ast.Literal{Kind: ast.LiteralNull})
	}
}

// encodeVersionedFK serialises an ast.VersionedForeignKey into w.
func encodeVersionedFK(w *v2Buf, vfk ast.VersionedForeignKey) {
	w.writeStr(vfk.Column)
	w.writeStr(vfk.LSNColumn)
	w.writeStr(vfk.ReferencesDomain)
	w.writeStr(vfk.ReferencesTable)
	w.writeStr(vfk.ReferencesColumn)
}

// encodeDDLPayload stores raw SQL text under the legacy op code v2OpDDL.
// Only used as a fallback for DML statements whose predicates contain
// non-binary-encodable constructs (LIKE, BETWEEN, JsonAccess, subqueries).
// DDL operations always use encodeDDLStructPayload instead.
func encodeDDLPayload(domain, sql string) []byte {
	w := &v2Buf{b: make([]byte, 0, 8+len(domain)+len(sql))}
	w.writeByte(payloadVersion)
	w.writeStr(domain)
	w.writeByte(v2OpDDL)
	w.writeStr(sql)
	return w.b
}

// decodeMutationPayloadV2 deserialises a binary v2 payload into a domain and Plan.
// For DDL payloads the SQL is parsed into a Plan at decode time (DDL is rare).
func decodeMutationPayloadV2(data []byte) (domain string, plan planner.Plan, err error) {
	if len(data) == 0 || data[0] != payloadVersion {
		return "", planner.Plan{}, fmt.Errorf("payloadv2: bad version byte 0x%02x (expected 0x%02x)", firstOrZero(data), payloadVersion)
	}
	r := &v2Reader{data: data, off: 1}

	domain, err = r.readStr()
	if err != nil {
		return "", planner.Plan{}, fmt.Errorf("payloadv2: read domain: %w", err)
	}

	op, err := r.readByte()
	if err != nil {
		return "", planner.Plan{}, fmt.Errorf("payloadv2: read op: %w", err)
	}

	// Legacy SQL-text DDL (backward compat — old WAL records only).
	if op == v2OpDDL {
		sql, err := r.readStr()
		if err != nil {
			return "", planner.Plan{}, fmt.Errorf("payloadv2: read DDL sql: %w", err)
		}
		stmt, err := parser.Parse(sql)
		if err != nil {
			return "", planner.Plan{}, fmt.Errorf("payloadv2: parse DDL sql: %w", err)
		}
		plan, err = planner.BuildForDomains(stmt, []string{domain})
		if err != nil {
			return "", planner.Plan{}, fmt.Errorf("payloadv2: build DDL plan: %w", err)
		}
		return domain, plan, nil
	}

	// Structured DDL (current format — parser-free).
	if op == v2OpDDLStruct {
		plan.DomainName = domain
		if err := decodeDDLStructPayload(r, domain, &plan); err != nil {
			return "", planner.Plan{}, err
		}
		return domain, plan, nil
	}

	tableName, err := r.readStr()
	if err != nil {
		return "", planner.Plan{}, fmt.Errorf("payloadv2: read table: %w", err)
	}
	plan.DomainName = domain
	plan.TableName = tableName

	switch op {
	case v2OpInsert:
		plan.Operation = planner.OperationInsert
		numCols, err := r.readUv()
		if err != nil {
			return "", planner.Plan{}, fmt.Errorf("payloadv2: INSERT numCols: %w", err)
		}
		plan.Columns = make([]string, numCols)
		for i := range plan.Columns {
			plan.Columns[i], err = r.readStr()
			if err != nil {
				return "", planner.Plan{}, fmt.Errorf("payloadv2: INSERT col[%d]: %w", i, err)
			}
		}
		numRows, err := r.readUv()
		if err != nil {
			return "", planner.Plan{}, fmt.Errorf("payloadv2: INSERT numRows: %w", err)
		}
		if numRows == 0 {
			return "", planner.Plan{}, errors.New("payloadv2: INSERT with 0 rows")
		}
		plan.Values = make([]ast.Literal, numCols)
		for i := range plan.Values {
			plan.Values[i], err = decodeLit(r)
			if err != nil {
				return "", planner.Plan{}, fmt.Errorf("payloadv2: INSERT row0 col[%d]: %w", i, err)
			}
		}
		if numRows > 1 {
			plan.MultiValues = make([][]ast.Literal, numRows-1)
			for ri := range plan.MultiValues {
				row := make([]ast.Literal, numCols)
				for i := range row {
					row[i], err = decodeLit(r)
					if err != nil {
						return "", planner.Plan{}, fmt.Errorf("payloadv2: INSERT row%d col[%d]: %w", ri+1, i, err)
					}
				}
				plan.MultiValues[ri] = row
			}
		}
		plan.OnConflict, err = decodeOnConflict(r)
		if err != nil {
			return "", planner.Plan{}, fmt.Errorf("payloadv2: INSERT onConflict: %w", err)
		}

	case v2OpUpdate:
		plan.Operation = planner.OperationUpdate
		numSet, err := r.readUv()
		if err != nil {
			return "", planner.Plan{}, fmt.Errorf("payloadv2: UPDATE numSetCols: %w", err)
		}
		plan.Columns = make([]string, numSet)
		plan.Values = make([]ast.Literal, numSet)
		hasArith := false
		plan.UpdateExprs = make([]ast.UpdateExpr, numSet)
		for i := range plan.Columns {
			plan.Columns[i], err = r.readStr()
			if err != nil {
				return "", planner.Plan{}, fmt.Errorf("payloadv2: UPDATE setCol[%d]: %w", i, err)
			}
			expr, err := decodeUpdateExpr(r)
			if err != nil {
				return "", planner.Plan{}, fmt.Errorf("payloadv2: UPDATE setExpr[%d]: %w", i, err)
			}
			plan.UpdateExprs[i] = expr
			if expr.Kind == ast.UpdateExprArithmetic {
				hasArith = true
			} else {
				plan.Values[i] = expr.Value
			}
		}
		if !hasArith {
			plan.UpdateExprs = nil // no arithmetic exprs — keep Values-only path
		}
		plan.Filter, err = decodePred(r)
		if err != nil {
			return "", planner.Plan{}, fmt.Errorf("payloadv2: UPDATE filter: %w", err)
		}

	case v2OpDelete:
		plan.Operation = planner.OperationDelete
		plan.Filter, err = decodePred(r)
		if err != nil {
			return "", planner.Plan{}, fmt.Errorf("payloadv2: DELETE filter: %w", err)
		}

	default:
		return "", planner.Plan{}, fmt.Errorf("payloadv2: unknown op byte 0x%02x", op)
	}

	return domain, plan, nil
}

// ─── read buffer ──────────────────────────────────────────────────────────────

type v2Reader struct {
	data []byte
	off  int
}

func (r *v2Reader) remaining() int { return len(r.data) - r.off }

func (r *v2Reader) readByte() (byte, error) {
	if r.off >= len(r.data) {
		return 0, errors.New("payloadv2: unexpected EOF")
	}
	b := r.data[r.off]
	r.off++
	return b, nil
}

func (r *v2Reader) readUv() (uint64, error) {
	v, n := binary.Uvarint(r.data[r.off:])
	if n <= 0 {
		return 0, errors.New("payloadv2: truncated uvarint")
	}
	r.off += n
	return v, nil
}

func (r *v2Reader) readStr() (string, error) {
	n, err := r.readUv()
	if err != nil {
		return "", err
	}
	if r.off+int(n) > len(r.data) {
		return "", fmt.Errorf("payloadv2: string truncated (need %d, have %d)", n, r.remaining())
	}
	s := string(r.data[r.off : r.off+int(n)])
	r.off += int(n)
	return s, nil
}

func (r *v2Reader) readU64() (uint64, error) {
	if r.off+8 > len(r.data) {
		return 0, errors.New("payloadv2: truncated u64")
	}
	v := binary.LittleEndian.Uint64(r.data[r.off:])
	r.off += 8
	return v, nil
}

func (r *v2Reader) readRawN(n int) ([]byte, error) {
	if r.off+n > len(r.data) {
		return nil, fmt.Errorf("payloadv2: raw truncated (need %d, have %d)", n, r.remaining())
	}
	b := make([]byte, n)
	copy(b, r.data[r.off:])
	r.off += n
	return b, nil
}

// ─── literal decode ───────────────────────────────────────────────────────────

func decodeLit(r *v2Reader) (ast.Literal, error) {
	kind, err := r.readByte()
	if err != nil {
		return ast.Literal{}, err
	}
	switch kind {
	case litNull:
		return ast.Literal{Kind: ast.LiteralNull}, nil
	case litString:
		s, err := r.readStr()
		return ast.Literal{Kind: ast.LiteralString, StringValue: s}, err
	case litInt64:
		v, err := r.readU64()
		return ast.Literal{Kind: ast.LiteralNumber, NumberValue: int64(v)}, err
	case litFloat64:
		bits, err := r.readU64()
		return ast.Literal{Kind: ast.LiteralFloat, FloatValue: math.Float64frombits(bits)}, err
	case litBoolTrue:
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: true}, nil
	case litBoolFalse:
		return ast.Literal{Kind: ast.LiteralBoolean, BoolValue: false}, nil
	case litJSON:
		s, err := r.readStr()
		return ast.Literal{Kind: ast.LiteralJSON, StringValue: s}, err
	case litUUID:
		raw, err := r.readRawN(16)
		if err != nil {
			return ast.Literal{}, err
		}
		return ast.Literal{Kind: ast.LiteralString, StringValue: uuid16ToStr(raw)}, nil
	case litTimestamp:
		s, err := r.readStr()
		return ast.Literal{Kind: ast.LiteralTimestamp, StringValue: s}, err
	default:
		return ast.Literal{}, fmt.Errorf("payloadv2: unknown literal kind 0x%02x", kind)
	}
}

// ─── predicate decode ─────────────────────────────────────────────────────────

func decodePred(r *v2Reader) (*ast.Predicate, error) {
	kind, err := r.readByte()
	if err != nil {
		return nil, err
	}
	switch kind {
	case predNil:
		return nil, nil
	case predAnd:
		left, err := decodePred(r)
		if err != nil {
			return nil, err
		}
		right, err := decodePred(r)
		if err != nil {
			return nil, err
		}
		return &ast.Predicate{Operator: "AND", Left: left, Right: right}, nil
	case predOr:
		left, err := decodePred(r)
		if err != nil {
			return nil, err
		}
		right, err := decodePred(r)
		if err != nil {
			return nil, err
		}
		return &ast.Predicate{Operator: "OR", Left: left, Right: right}, nil
	case predNot:
		sub, err := decodePred(r)
		if err != nil {
			return nil, err
		}
		return &ast.Predicate{Operator: "NOT", Left: sub}, nil
	case predIsNull:
		col, err := r.readStr()
		return &ast.Predicate{Column: col, Operator: "IS NULL"}, err
	case predIsNotNull:
		col, err := r.readStr()
		return &ast.Predicate{Column: col, Operator: "IS NOT NULL"}, err
	case predIn, predNotIn:
		col, err := r.readStr()
		if err != nil {
			return nil, err
		}
		n, err := r.readUv()
		if err != nil {
			return nil, err
		}
		vals := make([]ast.Literal, n)
		for i := range vals {
			vals[i], err = decodeLit(r)
			if err != nil {
				return nil, err
			}
		}
		op := "IN"
		if kind == predNotIn {
			op = "NOT IN"
		}
		return &ast.Predicate{Column: col, Operator: op, InValues: vals}, nil
	case predCMP:
		cmpCode, err := r.readByte()
		if err != nil {
			return nil, err
		}
		col, err := r.readStr()
		if err != nil {
			return nil, err
		}
		val, err := decodeLit(r)
		if err != nil {
			return nil, err
		}
		var opStr string
		switch cmpCode {
		case cmpEq:
			opStr = "="
		case cmpNeq:
			opStr = "!="
		case cmpLt:
			opStr = "<"
		case cmpLte:
			opStr = "<="
		case cmpGt:
			opStr = ">"
		case cmpGte:
			opStr = ">="
		default:
			return nil, fmt.Errorf("payloadv2: unknown cmpOp 0x%02x", cmpCode)
		}
		return &ast.Predicate{Column: col, Operator: opStr, Value: val}, nil
	default:
		return nil, fmt.Errorf("payloadv2: unknown predicate kind 0x%02x", kind)
	}
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func firstOrZero(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

// ─── DDL struct decode ────────────────────────────────────────────────────────

// decodeDDLStructPayload reads the structured DDL body (after v2OpDDLStruct has
// been consumed) and fills plan. domain must already be set on plan.DomainName
// before this call.
func decodeDDLStructPayload(r *v2Reader, domain string, plan *planner.Plan) error {
	subOp, err := r.readByte()
	if err != nil {
		return fmt.Errorf("payloadv2: ddl_struct sub-op: %w", err)
	}

	switch subOp {
	case ddlSubCreateTable:
		flags, err := r.readByte()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE TABLE flags: %w", err)
		}
		plan.Operation = planner.OperationCreateTable
		plan.IfNotExists = flags&0x01 != 0
		plan.TableName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE TABLE tableName: %w", err)
		}
		numCols, err := r.readUv()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE TABLE numCols: %w", err)
		}
		plan.Schema = make([]ast.ColumnDefinition, numCols)
		for i := range plan.Schema {
			plan.Schema[i], err = decodeColumnDef(r)
			if err != nil {
				return fmt.Errorf("payloadv2: CREATE TABLE col[%d]: %w", i, err)
			}
		}
		numVFKs, err := r.readUv()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE TABLE numVFKs: %w", err)
		}
		plan.VersionedForeignKeys = make([]ast.VersionedForeignKey, numVFKs)
		for i := range plan.VersionedForeignKeys {
			plan.VersionedForeignKeys[i], err = decodeVersionedFK(r)
			if err != nil {
				return fmt.Errorf("payloadv2: CREATE TABLE vfk[%d]: %w", i, err)
			}
		}

	case ddlSubAlterAddColumn:
		plan.Operation = planner.OperationAlterTableAddColumn
		plan.TableName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: ALTER ADD COLUMN tableName: %w", err)
		}
		col, err := decodeColumnDef(r)
		if err != nil {
			return fmt.Errorf("payloadv2: ALTER ADD COLUMN col: %w", err)
		}
		plan.AlterColumn = &col

	case ddlSubAlterDropColumn:
		plan.Operation = planner.OperationAlterTableDropColumn
		plan.TableName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: ALTER DROP COLUMN tableName: %w", err)
		}
		plan.DropColumnName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: ALTER DROP COLUMN columnName: %w", err)
		}

	case ddlSubAlterRenameColumn:
		plan.Operation = planner.OperationAlterTableRenameColumn
		plan.TableName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: ALTER RENAME COLUMN tableName: %w", err)
		}
		plan.RenameOldColumn, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: ALTER RENAME COLUMN old: %w", err)
		}
		plan.RenameNewColumn, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: ALTER RENAME COLUMN new: %w", err)
		}

	case ddlSubCreateIndex:
		flags, err := r.readByte()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE INDEX flags: %w", err)
		}
		plan.Operation = planner.OperationCreateIndex
		plan.IfNotExists = flags&0x01 != 0
		plan.TableName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE INDEX tableName: %w", err)
		}
		plan.IndexName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE INDEX indexName: %w", err)
		}
		plan.IndexColumn, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE INDEX indexColumn: %w", err)
		}
		numCols, err := r.readUv()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE INDEX numCols: %w", err)
		}
		if numCols > 0 {
			plan.IndexColumns = make([]string, numCols)
			for i := range plan.IndexColumns {
				plan.IndexColumns[i], err = r.readStr()
				if err != nil {
					return fmt.Errorf("payloadv2: CREATE INDEX col[%d]: %w", i, err)
				}
			}
		}
		plan.IndexMethod, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE INDEX indexMethod: %w", err)
		}

	case ddlSubCreateEntity:
		flags, err := r.readByte()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE ENTITY flags: %w", err)
		}
		plan.Operation = planner.OperationCreateEntity
		plan.IfNotExists = flags&0x01 != 0
		plan.EntityName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE ENTITY entityName: %w", err)
		}
		plan.EntityRootTable, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE ENTITY entityRootTable: %w", err)
		}
		numTables, err := r.readUv()
		if err != nil {
			return fmt.Errorf("payloadv2: CREATE ENTITY numTables: %w", err)
		}
		plan.EntityTables = make([]string, numTables)
		for i := range plan.EntityTables {
			plan.EntityTables[i], err = r.readStr()
			if err != nil {
				return fmt.Errorf("payloadv2: CREATE ENTITY table[%d]: %w", i, err)
			}
		}

	case ddlSubDropTable:
		flags, err := r.readByte()
		if err != nil {
			return fmt.Errorf("payloadv2: DROP TABLE flags: %w", err)
		}
		plan.Operation = planner.OperationDropTable
		plan.IfExists = flags&0x01 != 0
		plan.Cascade = flags&0x02 != 0
		plan.TableName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: DROP TABLE tableName: %w", err)
		}

	case ddlSubDropIndex:
		flags, err := r.readByte()
		if err != nil {
			return fmt.Errorf("payloadv2: DROP INDEX flags: %w", err)
		}
		plan.Operation = planner.OperationDropIndex
		plan.IfExists = flags&0x01 != 0
		plan.IndexName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: DROP INDEX indexName: %w", err)
		}
		plan.TableName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: DROP INDEX tableName: %w", err)
		}

	case ddlSubTruncateTable:
		plan.Operation = planner.OperationTruncateTable
		plan.TableName, err = r.readStr()
		if err != nil {
			return fmt.Errorf("payloadv2: TRUNCATE TABLE tableName: %w", err)
		}

	default:
		return fmt.Errorf("payloadv2: unknown DDL struct sub-op 0x%02x", subOp)
	}

	return nil
}

// decodeColumnDef deserialises a ColumnDefinition written by encodeColumnDef.
func decodeColumnDef(r *v2Reader) (ast.ColumnDefinition, error) {
	var col ast.ColumnDefinition
	var err error
	col.Name, err = r.readStr()
	if err != nil {
		return col, fmt.Errorf("col name: %w", err)
	}
	typStr, err := r.readStr()
	if err != nil {
		return col, fmt.Errorf("col type: %w", err)
	}
	col.Type = ast.DataType(typStr)
	flags, err := r.readByte()
	if err != nil {
		return col, fmt.Errorf("col flags: %w", err)
	}
	col.PrimaryKey = flags&colFlagPrimaryKey != 0
	col.Unique = flags&colFlagUnique != 0
	col.NotNull = flags&colFlagNotNull != 0
	if flags&colFlagHasRefs != 0 {
		col.ReferencesTable, err = r.readStr()
		if err != nil {
			return col, fmt.Errorf("col referencesTable: %w", err)
		}
		col.ReferencesColumn, err = r.readStr()
		if err != nil {
			return col, fmt.Errorf("col referencesColumn: %w", err)
		}
	}
	if flags&colFlagHasCheck != 0 {
		col.Check, err = decodePred(r)
		if err != nil {
			return col, fmt.Errorf("col check: %w", err)
		}
	}
	if flags&colFlagHasDefault != 0 {
		col.DefaultValue, err = decodeDefaultExpr(r)
		if err != nil {
			return col, fmt.Errorf("col default: %w", err)
		}
	}
	return col, nil
}

// decodeDefaultExpr deserialises a DefaultExpr written by encodeDefaultExpr.
func decodeDefaultExpr(r *v2Reader) (*ast.DefaultExpr, error) {
	kind, err := r.readByte()
	if err != nil {
		return nil, fmt.Errorf("default kind: %w", err)
	}
	switch kind {
	case defaultKindLiteral:
		lit, err := decodeLit(r)
		if err != nil {
			return nil, fmt.Errorf("default literal: %w", err)
		}
		return &ast.DefaultExpr{Kind: ast.DefaultLiteral, Value: lit}, nil
	case defaultKindAutoIncrement:
		return &ast.DefaultExpr{Kind: ast.DefaultAutoIncrement}, nil
	case defaultKindUUIDv7:
		return &ast.DefaultExpr{Kind: ast.DefaultUUIDv7}, nil
	case defaultKindTxTimestamp:
		return &ast.DefaultExpr{Kind: ast.DefaultTxTimestamp}, nil
	default:
		return nil, fmt.Errorf("payloadv2: unknown default kind 0x%02x", kind)
	}
}

// decodeVersionedFK deserialises a VersionedForeignKey written by encodeVersionedFK.
func decodeVersionedFK(r *v2Reader) (ast.VersionedForeignKey, error) {
	var vfk ast.VersionedForeignKey
	var err error
	vfk.Column, err = r.readStr()
	if err != nil {
		return vfk, fmt.Errorf("vfk column: %w", err)
	}
	vfk.LSNColumn, err = r.readStr()
	if err != nil {
		return vfk, fmt.Errorf("vfk lsn_column: %w", err)
	}
	vfk.ReferencesDomain, err = r.readStr()
	if err != nil {
		return vfk, fmt.Errorf("vfk references_domain: %w", err)
	}
	vfk.ReferencesTable, err = r.readStr()
	if err != nil {
		return vfk, fmt.Errorf("vfk references_table: %w", err)
	}
	vfk.ReferencesColumn, err = r.readStr()
	if err != nil {
		return vfk, fmt.Errorf("vfk references_column: %w", err)
	}
	return vfk, nil
}
