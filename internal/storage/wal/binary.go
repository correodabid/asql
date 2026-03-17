package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/klauspost/compress/s2"
)

// Binary WAL record format (version 5).
//
// All multi-byte integers are big-endian. The frame is:
//
//	[4 bytes] body length (uint32, standard length-prefix frame)
//	[body]    version(1) + LSN(8) + Term(8) + Timestamp(8) + TxIDLen(2) + TxID(N)
//	          + TypeEnum(1) + Payload(P, s2-compressed) + CRC32C(4)
//
// Changes vs v4:
//   - Term(8) added after LSN — the Raft term in which the record was written.
//
// Backward compatibility:
//   - v4 records are decoded with Term=0.
//   - v4 encoding path is no longer written; all new records use v5.
//
// CRC32C is computed over all body bytes preceding the 4-byte checksum.
// Type is stored as a 1-byte enum instead of a variable-length string.
// Mutation payloads are s2-compressed (snappy-compatible, very fast).

const (
	binaryVersion       byte = 5
	binaryVersionLegacy byte = 4

	// binaryFixedOverhead is the fixed byte count in a v5 body:
	// version(1) + LSN(8) + Term(8) + Timestamp(8) + TxIDLen(2) + TypeEnum(1) + CRC32C(4) = 32
	binaryFixedOverhead = 32

	// binaryFixedOverheadV4 is the fixed overhead for legacy v4 records:
	// version(1) + LSN(8) + Timestamp(8) + TxIDLen(2) + TypeEnum(1) + CRC32C(4) = 24
	binaryFixedOverheadV4 = 24

	// Type enum values.
)

// crc32cTable is the CRC32C (Castagnoli) table, hardware-accelerated on x86 (SSE4.2) and ARM.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

const (
	typeEnumBegin    byte = 0x01
	typeEnumMutation byte = 0x02
	typeEnumCommit   byte = 0x03
	typeEnumAudit    byte = 0x04 // persistent audit log entry (payload-only, no wall time)
	typeEnumSecurity byte = 0x05 // durable principal/authorization catalog mutation
)

// typeStringToEnum converts a WAL type string to its 1-byte enum.
func typeStringToEnum(typ string) (byte, error) {
	switch typ {
	case "BEGIN":
		return typeEnumBegin, nil
	case "MUTATION":
		return typeEnumMutation, nil
	case "COMMIT":
		return typeEnumCommit, nil
	case "AUDIT":
		return typeEnumAudit, nil
	case "SECURITY":
		return typeEnumSecurity, nil
	default:
		return 0, fmt.Errorf("unknown wal type: %q", typ)
	}
}

// typeEnumToString converts a 1-byte type enum back to its string.
func typeEnumToString(b byte) (string, error) {
	switch b {
	case typeEnumBegin:
		return "BEGIN", nil
	case typeEnumMutation:
		return "MUTATION", nil
	case typeEnumCommit:
		return "COMMIT", nil
	case typeEnumAudit:
		return "AUDIT", nil
	case typeEnumSecurity:
		return "SECURITY", nil
	default:
		return "", fmt.Errorf("unknown wal type enum: 0x%02x", b)
	}
}

// encodeBinaryDiskRecord encodes a disk record using compact binary format v5.
// Returns the framed bytes: [4-byte length prefix][binary body].
func encodeBinaryDiskRecord(r diskRecord) []byte {
	txID := []byte(r.TxID)

	typeEnum, err := typeStringToEnum(r.Type)
	if err != nil {
		// Should never happen with well-formed records.
		panic(err)
	}

	// Compress payload with s2 (no-op for empty payloads).
	payload := r.Payload
	if len(payload) > 0 {
		payload = s2.Encode(nil, payload)
	}

	bodyLen := binaryFixedOverhead + len(txID) + len(payload)
	frame := make([]byte, 4+bodyLen)

	// Length prefix.
	binary.BigEndian.PutUint32(frame[0:4], uint32(bodyLen))

	body := frame[4:]
	off := 0

	body[off] = binaryVersion
	off++
	binary.BigEndian.PutUint64(body[off:], r.LSN)
	off += 8
	binary.BigEndian.PutUint64(body[off:], r.Term)
	off += 8
	binary.BigEndian.PutUint64(body[off:], r.Timestamp)
	off += 8
	binary.BigEndian.PutUint16(body[off:], uint16(len(txID)))
	off += 2
	copy(body[off:], txID)
	off += len(txID)
	body[off] = typeEnum
	off++
	copy(body[off:], payload)
	off += len(payload)

	// CRC32C over everything before the checksum slot.
	checksum := crc32.Checksum(body[:off], crc32cTable)
	binary.BigEndian.PutUint32(body[off:], checksum)

	return frame
}

// decodeBinaryDiskRecord decodes a binary WAL record body.
// Supports both v4 (Term=0, legacy) and v5 (with Term) formats.
func decodeBinaryDiskRecord(body []byte) (diskRecord, error) {
	if len(body) < binaryFixedOverheadV4 {
		return diskRecord{}, errors.New("binary wal record too short")
	}

	off := 0

	version := body[off]
	off++
	if version != binaryVersion && version != binaryVersionLegacy {
		return diskRecord{}, fmt.Errorf("%w: binary got=%d want=%d or %d", errInvalidVersion, version, binaryVersionLegacy, binaryVersion)
	}

	lsn := binary.BigEndian.Uint64(body[off:])
	off += 8

	// v5 adds Term after LSN; v4 has no Term field (Term=0).
	var term uint64
	if version == binaryVersion {
		if len(body) < binaryFixedOverhead {
			return diskRecord{}, errors.New("binary wal v5 record too short")
		}
		term = binary.BigEndian.Uint64(body[off:])
		off += 8
	}

	timestamp := binary.BigEndian.Uint64(body[off:])
	off += 8

	txIDLen := int(binary.BigEndian.Uint16(body[off:]))
	off += 2
	if off+txIDLen > len(body) {
		return diskRecord{}, errors.New("binary wal record truncated at txid")
	}
	txID := string(body[off : off+txIDLen])
	off += txIDLen

	if off >= len(body) {
		return diskRecord{}, errors.New("binary wal record truncated at type")
	}
	typeEnum := body[off]
	off++

	typ, err := typeEnumToString(typeEnum)
	if err != nil {
		return diskRecord{}, fmt.Errorf("binary wal record invalid type: %w", err)
	}

	// PayloadLen is derived: remaining bytes minus the trailing 4-byte CRC32C.
	payloadLen := len(body) - off - 4
	if payloadLen < 0 {
		return diskRecord{}, errors.New("binary wal record truncated at payload")
	}
	compressed := body[off : off+payloadLen]
	off += payloadLen

	// Verify CRC32C before decompression.
	expected := crc32.Checksum(body[:off], crc32cTable)
	actual := binary.BigEndian.Uint32(body[off:])
	if actual != expected {
		return diskRecord{}, errChecksum
	}

	// Decompress payload with s2.
	var payload []byte
	if payloadLen > 0 {
		payload, err = s2.Decode(nil, compressed)
		if err != nil {
			return diskRecord{}, fmt.Errorf("s2 decompress payload: %w", err)
		}
	}

	return diskRecord{
		Version:   uint16(version),
		LSN:       lsn,
		Term:      term,
		TxID:      txID,
		Type:      typ,
		Timestamp: timestamp,
		Payload:   payload,
		Checksum:  actual,
	}, nil
}
