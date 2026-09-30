package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"google.golang.org/protobuf/proto"

	raftv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/raft/v1"
)

// Record framing, as it sits on disk:
//
//	[u32 length][u32 crc32c][u8 type][payload]
//
// length is the payload length alone. The CRC covers length, type and payload,
// in that order, so a corrupted length is caught by the checksum rather than
// only by whatever happens to lie at the offset it points to. Integers are
// little-endian.
//
// Protobuf supplies neither framing nor corruption detection, which is why this
// layer exists at all: without it a torn tail is indistinguishable from a short
// but valid message.

const (
	headerSize = 4 + 4 + 1

	// maxPayloadBytes bounds a single record. Anything claiming to be larger is
	// treated as corruption rather than trusted, so a flipped high bit in a
	// length field cannot make recovery try to allocate gigabytes. It is well
	// above anything a Ready batch produces; Sync refuses a batch that would
	// exceed it rather than writing a record recovery would then reject.
	maxPayloadBytes = 64 << 20
)

// recordType tags the payload. The values are written to disk, so they are
// frozen: never renumber one, only add.
//
// Zero is deliberately not a type. Some filesystems leave a zero-filled region
// behind after a crash that extended a file, and an all-zero header must never
// read as a record. (It is also rejected by the CRC: the CRC32C of five zero
// bytes is not zero.)
type recordType uint8

const (
	// recordEntryBatch carries a WalEntryBatch: one Ready's entries and hard
	// state.
	recordEntryBatch recordType = 1

	// recordSnapshotPointer carries a WalSnapshotPointer: it names a durable
	// snapshot file and carries the log tail and hard state, and it is always
	// the first record of its segment. Recovery starts from the last one.
	recordSnapshotPointer recordType = 2
)

func (t recordType) String() string {
	switch t {
	case recordEntryBatch:
		return "EntryBatch"
	case recordSnapshotPointer:
		return "SnapshotPointer"
	default:
		return fmt.Sprintf("recordType(%d)", uint8(t))
	}
}

// record is one decoded WAL record. Exactly one of the payload fields is set,
// matching typ.
type record struct {
	typ     recordType
	batch   *raftv1.WalEntryBatch
	pointer *raftv1.WalSnapshotPointer
}

var (
	// errShortRecord means the buffer ends before the record does. At the end
	// of the last segment this is a torn write from a crash, and recovery
	// truncates it. Anywhere else it is corruption.
	errShortRecord = errors.New("storage: record extends past end of data")

	// errCorruptRecord means the bytes present are not a record at all: a bad
	// CRC or an impossible length. At the end of the last segment that is a
	// torn write and recovery truncates it.
	errCorruptRecord = errors.New("storage: corrupt record")

	// errUnreadableRecord means the CRC matched -- so these exact bytes were
	// deliberately written -- but this binary cannot interpret them: an
	// unknown type, or a payload that does not parse. That is never a torn
	// write. It is a newer binary's record or a bug, and truncating it would
	// silently discard data that was durable, so recovery refuses to start.
	errUnreadableRecord = errors.New("storage: unreadable record")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// crc is the CRC32C of b. Snapshot files are checked with it against the
// pointer that names them.
func crc(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// encodeRecord appends the framed record to dst.
func encodeRecord(dst []byte, r record) ([]byte, error) {
	var msg proto.Message
	switch r.typ {
	case recordEntryBatch:
		msg = r.batch
	case recordSnapshotPointer:
		msg = r.pointer
	default:
		return nil, fmt.Errorf("storage: cannot encode record of type %s", r.typ)
	}

	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("storage: marshal %s: %w", r.typ, err)
	}
	if len(payload) > maxPayloadBytes {
		return nil, fmt.Errorf("storage: %s record is %d bytes, over the %d-byte limit; "+
			"lower MaxEntriesPerAppend or the proposal size so a single Ready fits",
			r.typ, len(payload), maxPayloadBytes)
	}

	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(payload)))
	dst = binary.LittleEndian.AppendUint32(dst, 0) // CRC, filled in below
	dst = append(dst, byte(r.typ))
	dst = append(dst, payload...)

	binary.LittleEndian.PutUint32(dst[start+4:], recordCRC(dst[start:]))
	return dst, nil
}

// recordCRC computes the checksum of a complete framed record, skipping the CRC
// field itself.
func recordCRC(frame []byte) uint32 {
	c := crc32.Update(0, castagnoli, frame[0:4])
	return crc32.Update(c, castagnoli, frame[8:])
}

// decodeRecord reads one record from the front of buf and reports how many
// bytes it consumed.
//
// It never returns a record it did not read in full, and never returns one
// whose CRC does not match: on any error, n is zero and the record is empty.
// FuzzWALDecode holds it to that.
func decodeRecord(buf []byte) (r record, n int, err error) {
	if len(buf) < headerSize {
		return record{}, 0, errShortRecord
	}

	length := binary.LittleEndian.Uint32(buf[0:4])
	if length > maxPayloadBytes {
		return record{}, 0, fmt.Errorf("%w: length %d exceeds the %d-byte limit",
			errCorruptRecord, length, maxPayloadBytes)
	}
	total := headerSize + int(length)
	if len(buf) < total {
		return record{}, 0, errShortRecord
	}

	frame := buf[:total]
	if got, want := recordCRC(frame), binary.LittleEndian.Uint32(frame[4:8]); got != want {
		return record{}, 0, fmt.Errorf("%w: crc %08x, header says %08x", errCorruptRecord, got, want)
	}

	typ := recordType(frame[8])
	payload := frame[headerSize:]
	switch typ {
	case recordEntryBatch:
		b := &raftv1.WalEntryBatch{}
		if err := proto.Unmarshal(payload, b); err != nil {
			return record{}, 0, fmt.Errorf("%w: %s payload: %w", errUnreadableRecord, typ, err)
		}
		return record{typ: typ, batch: b}, total, nil
	case recordSnapshotPointer:
		p := &raftv1.WalSnapshotPointer{}
		if err := proto.Unmarshal(payload, p); err != nil {
			return record{}, 0, fmt.Errorf("%w: %s payload: %w", errUnreadableRecord, typ, err)
		}
		return record{typ: typ, pointer: p}, total, nil
	default:
		return record{}, 0, fmt.Errorf("%w: unknown type %s", errUnreadableRecord, typ)
	}
}
