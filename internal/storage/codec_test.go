package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	raftv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/raft/v1"
)

func sampleRecords() []record {
	big := bytes.Repeat([]byte("x"), 70_000) // forces a multi-byte varint length
	return []record{
		{typ: recordEntryBatch, batch: &raftv1.WalEntryBatch{}},
		{typ: recordEntryBatch, batch: &raftv1.WalEntryBatch{
			HardState: &raftv1.HardState{Term: 3, VotedFor: 2, Commit: 0},
		}},
		{typ: recordEntryBatch, batch: &raftv1.WalEntryBatch{
			Entries: []*raftv1.Entry{
				{Term: 1, Index: 1, Type: raftv1.EntryType_ENTRY_TYPE_NOOP},
				{Term: 1, Index: 2, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: []byte("set a=1")},
				{Term: 2, Index: 3, Type: raftv1.EntryType_ENTRY_TYPE_SESSION, Data: []byte{0, 1, 2}},
			},
			HardState: &raftv1.HardState{Term: 2, VotedFor: 1, Commit: 2},
		}},
		{typ: recordEntryBatch, batch: &raftv1.WalEntryBatch{
			Entries: []*raftv1.Entry{{Term: 9, Index: 4, Type: raftv1.EntryType_ENTRY_TYPE_NORMAL, Data: big}},
		}},
		{typ: recordSnapshotPointer, pointer: &raftv1.WalSnapshotPointer{
			Metadata:  &raftv1.SnapshotMetadata{LastIncludedIndex: 40, LastIncludedTerm: 7},
			Filename:  "40-7.snap",
			Crc32C:    0xdeadbeef,
			SizeBytes: 1 << 20,
		}},
	}
}

func payloadOf(r record) proto.Message {
	if r.typ == recordSnapshotPointer {
		return r.pointer
	}
	return r.batch
}

// TestWALCodecRoundTrip is phase 3 acceptance criterion 1 (first half): every
// record type survives encode then decode unchanged, alone and back to back in
// one stream, and the decoder consumes exactly the bytes it was given.
func TestWALCodecRoundTrip(t *testing.T) {
	var stream []byte
	for _, r := range sampleRecords() {
		one, err := encodeRecord(nil, r)
		require.NoError(t, err)

		got, n, err := decodeRecord(one)
		require.NoError(t, err)
		require.Equal(t, len(one), n, "decoder must consume the whole record and no more")
		require.Equal(t, r.typ, got.typ)
		require.True(t, proto.Equal(payloadOf(r), payloadOf(got)), "%s did not round-trip", r.typ)

		stream = append(stream, one...)
	}

	off := 0
	for i, want := range sampleRecords() {
		got, n, err := decodeRecord(stream[off:])
		require.NoError(t, err, "record %d", i)
		require.True(t, proto.Equal(payloadOf(want), payloadOf(got)), "record %d", i)
		off += n
	}
	require.Equal(t, len(stream), off)
}

// TestWALCodecRejectsEveryBitFlip is the deterministic half of "never accept a
// record whose CRC does not match": flip each bit of a real record in turn,
// and every single variant must be refused.
func TestWALCodecRejectsEveryBitFlip(t *testing.T) {
	for _, r := range sampleRecords()[:3] { // the 70 kB one would be 560k cases
		good, err := encodeRecord(nil, r)
		require.NoError(t, err)
		for bit := range len(good) * 8 {
			bad := bytes.Clone(good)
			bad[bit/8] ^= 1 << (bit % 8)
			_, n, err := decodeRecord(bad)
			require.Errorf(t, err, "%s: flipping bit %d was accepted", r.typ, bit)
			require.Zero(t, n)
		}
	}
}

// TestWALCodecClassifiesDamage pins down which errors recovery may treat as a
// torn tail (short, corrupt) and which it must refuse to truncate (unreadable:
// the CRC matched, so the bytes were deliberately written).
func TestWALCodecClassifiesDamage(t *testing.T) {
	good, err := encodeRecord(nil, sampleRecords()[2])
	require.NoError(t, err)

	// frame builds a record with a valid CRC around arbitrary type and payload.
	frame := func(typ byte, payload []byte) []byte {
		b := binary.LittleEndian.AppendUint32(nil, uint32(len(payload)))
		b = binary.LittleEndian.AppendUint32(b, 0)
		b = append(b, typ)
		b = append(b, payload...)
		binary.LittleEndian.PutUint32(b[4:], recordCRC(b))
		return b
	}
	huge := binary.LittleEndian.AppendUint32(nil, maxPayloadBytes+1)
	huge = append(huge, make([]byte, 5)...)

	cases := []struct {
		name string
		data []byte
		want error
	}{
		{"empty", nil, errShortRecord},
		{"partial header", good[:headerSize-1], errShortRecord},
		{"partial payload", good[:len(good)-1], errShortRecord},
		{"zero-filled", make([]byte, 64), errCorruptRecord},
		{"flipped payload byte", func() []byte { b := bytes.Clone(good); b[len(b)-1] ^= 0xff; return b }(), errCorruptRecord},
		{"impossible length", huge, errCorruptRecord},
		{"unknown type, valid crc", frame(77, nil), errUnreadableRecord},
		{"zero type, valid crc", frame(0, nil), errUnreadableRecord},
		{"garbage payload, valid crc", frame(byte(recordEntryBatch), []byte{0xff, 0xff, 0xff}), errUnreadableRecord},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, n, err := decodeRecord(tc.data)
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, n)
		})
	}
}

// decodeFunc is the decoder's signature, so the contract check can be pointed
// at a deliberately broken decoder as well as the real one.
type decodeFunc func([]byte) (record, int, error)

// checkDecodeContract states what FuzzWALDecode holds the decoder to, for one
// input. It returns an error describing the first broken promise.
func checkDecodeContract(decode decodeFunc, data []byte) error {
	r, n, err := decode(data)
	if err != nil {
		switch {
		case n != 0:
			return fmt.Errorf("error %w returned alongside n=%d", err, n)
		case !errors.Is(err, errShortRecord) && !errors.Is(err, errCorruptRecord) &&
			!errors.Is(err, errUnreadableRecord):
			return fmt.Errorf("error %w is not one of the three classified errors", err)
		}
		return nil
	}

	// Never a record it did not fully read.
	if n < headerSize || n > len(data) {
		return fmt.Errorf("consumed %d bytes of %d", n, len(data))
	}
	if length := int(binary.LittleEndian.Uint32(data[0:4])); headerSize+length != n {
		return fmt.Errorf("consumed %d bytes but the header declares %d", n, headerSize+length)
	}
	if _, _, err := decode(data[:n-1]); !errors.Is(err, errShortRecord) {
		return fmt.Errorf("one byte short of the record decoded with err=%w, want short", err)
	}

	// Never a record whose CRC does not match -- recomputed independently.
	if got, want := recordCRC(data[:n]), binary.LittleEndian.Uint32(data[4:8]); got != want {
		return fmt.Errorf("accepted a record with crc %08x, header says %08x", got, want)
	}
	if r.typ != recordEntryBatch && r.typ != recordSnapshotPointer {
		return fmt.Errorf("returned record of unknown type %s", r.typ)
	}
	if (r.batch == nil) == (r.pointer == nil) {
		return fmt.Errorf("record must carry exactly one payload")
	}
	return nil
}

// FuzzWALDecode is phase 3 acceptance criterion 1 (second half). Under plain
// `go test` it runs the seed corpus; `go test -fuzz=FuzzWALDecode
// ./internal/storage` searches further.
func FuzzWALDecode(f *testing.F) {
	var stream []byte
	for _, r := range sampleRecords() {
		b, err := encodeRecord(nil, r)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
		f.Add(b[:len(b)/2])
		stream = append(stream, b...)
	}
	f.Add(stream)
	f.Add([]byte{})
	f.Add(make([]byte, headerSize))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Walk the whole input the way recovery does, checking the contract at
		// every record boundary, not only the first.
		for off := 0; off < len(data); {
			if err := checkDecodeContract(decodeRecord, data[off:]); err != nil {
				t.Fatalf("offset %d: %v", off, err)
			}
			_, n, err := decodeRecord(data[off:])
			if err != nil {
				return
			}
			off += n
		}
	})
}

// TestDecodeContractCatchesBrokenDecoders is the negative control for
// FuzzWALDecode: the contract check must actually fail against decoders that
// break each promise, or the fuzz target's silence proves nothing.
func TestDecodeContractCatchesBrokenDecoders(t *testing.T) {
	good, err := encodeRecord(nil, sampleRecords()[2])
	require.NoError(t, err)
	corrupted := bytes.Clone(good)
	corrupted[len(corrupted)-1] ^= 0x01

	// Skips the CRC check entirely.
	noCRC := func(b []byte) (record, int, error) {
		if len(b) < headerSize {
			return record{}, 0, errShortRecord
		}
		n := headerSize + int(binary.LittleEndian.Uint32(b[0:4]))
		if n > len(b) {
			return record{}, 0, errShortRecord
		}
		return record{typ: recordEntryBatch, batch: &raftv1.WalEntryBatch{}}, n, nil
	}

	// Accepts whatever is there when the record is cut short.
	acceptsShort := func(b []byte) (record, int, error) {
		r, n, err := decodeRecord(b)
		if errors.Is(err, errShortRecord) && len(b) > 0 {
			return record{typ: recordEntryBatch, batch: &raftv1.WalEntryBatch{}}, len(b), nil
		}
		return r, n, err
	}

	// Reports an error but still claims to have consumed bytes.
	leaksN := func(b []byte) (record, int, error) {
		r, n, err := decodeRecord(b)
		if err != nil {
			return record{}, 1, err
		}
		return r, n, nil
	}

	require.NoError(t, checkDecodeContract(decodeRecord, good))
	require.NoError(t, checkDecodeContract(decodeRecord, corrupted))
	require.Error(t, checkDecodeContract(noCRC, corrupted), "missed an accepted bad CRC")
	require.Error(t, checkDecodeContract(acceptsShort, good[:len(good)-3]), "missed a partial record")
	require.Error(t, checkDecodeContract(leaksN, corrupted), "missed n>0 on error")
}
