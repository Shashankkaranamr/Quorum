package statemachine

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/kv/v1"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// log builds a sequence of entries from payloads, numbered from 1.
type logBuilder struct{ entries []raft.Entry }

func (b *logBuilder) add(typ raft.EntryType, data []byte) raft.Index {
	i := raft.Index(len(b.entries) + 1)
	b.entries = append(b.entries, raft.Entry{Index: i, Term: 1, Type: typ, Data: data})
	return i
}

func applyAll(t *testing.T, m *KV, ents []raft.Entry) []Result {
	t.Helper()
	var out []Result
	for _, e := range ents {
		r, err := m.ApplyEntry(e)
		require.NoError(t, err)
		out = append(out, r)
	}
	return out
}

func TestWritesApplyAndRetriesAreDeduplicated(t *testing.T) {
	var b logBuilder
	b.add(raft.EntryNoOp, nil)
	client := uint64(b.add(raft.EntrySession, EncodeRegister([]byte("n"))))
	b.add(raft.EntryNormal, EncodePut(client, 1, "k", []byte("v1")))
	b.add(raft.EntryNormal, EncodePut(client, 1, "k", []byte("v1"))) // retry of seq 1
	b.add(raft.EntryNormal, EncodeDelete(client, 2, "k"))
	b.add(raft.EntryNormal, EncodeDelete(client, 2, "k")) // retry of seq 2
	b.add(raft.EntryNormal, EncodePut(99, 1, "x", []byte("no session")))

	m := New()
	res := applyAll(t, m, b.entries)

	require.Equal(t, uint64(2), res[1].ClientID, "the client id is the session entry's index")
	require.False(t, res[2].Duplicate)
	require.Equal(t, raft.Index(3), res[2].AppliedIndex)

	require.True(t, res[3].Duplicate, "a retried seq must come from the session cache")
	require.Equal(t, raft.Index(3), res[3].AppliedIndex, "and report where the write originally applied")

	require.True(t, res[4].Existed)
	require.True(t, res[5].Duplicate)
	require.True(t, res[5].Existed, "a retried delete must replay the original answer, not re-evaluate it")

	require.Equal(t, kvv1.Status_STATUS_SESSION_EXPIRED, res[6].Status)
	_, found := m.Get("x")
	require.False(t, found, "a command with no session must not be applied")
	_, found = m.Get("k")
	require.False(t, found)
}

// TestSnapshotRoundTripIsExact: restoring a snapshot into a fresh machine must
// give identical state -- same hash, same data, same sessions -- and applying
// the same further entries to both must keep them identical.
func TestSnapshotRoundTripIsExact(t *testing.T) {
	var b logBuilder
	c1 := uint64(b.add(raft.EntrySession, EncodeRegister(nil)))
	c2 := uint64(b.add(raft.EntrySession, EncodeRegister(nil)))
	for i := range uint64(20) {
		b.add(raft.EntryNormal, EncodePut(c1, i+1, string(rune('a'+i%7)), []byte{byte(i)}))
		b.add(raft.EntryNormal, EncodeDelete(c2, i+1, string(rune('a'+i%5))))
	}
	head, tail := b.entries[:30], b.entries[30:]

	orig := New()
	applyAll(t, orig, head)
	data, at, err := orig.Snapshot()
	require.NoError(t, err)
	require.Equal(t, raft.Index(30), at)

	restored := New()
	require.NoError(t, restored.Restore(raft.Snapshot{Meta: raft.SnapshotMeta{Index: at, Term: 1}, Data: data}))
	h1, err := orig.Hash()
	require.NoError(t, err)
	h2, err := restored.Hash()
	require.NoError(t, err)
	require.Equal(t, h1, h2)

	applyAll(t, orig, tail)
	applyAll(t, restored, tail)
	h1, err = orig.Hash()
	require.NoError(t, err)
	h2, err = restored.Hash()
	require.NoError(t, err)
	require.Equal(t, h1, h2, "a restored machine must evolve identically to the original")

	again, _, err := restored.Snapshot()
	require.NoError(t, err)
	final, _, err := orig.Snapshot()
	require.NoError(t, err)
	require.Equal(t, final, again, "equal states must encode to identical bytes")
}

// TestRestoredSessionsStillDeduplicate is the unit-level form of phase 4
// criterion 4, with its negative control: a snapshot with the session table
// stripped out restores a machine that applies the retry again. That is what
// the session table in the snapshot is for.
func TestRestoredSessionsStillDeduplicate(t *testing.T) {
	var b logBuilder
	client := uint64(b.add(raft.EntrySession, EncodeRegister(nil)))
	b.add(raft.EntryNormal, EncodePut(client, 1, "counter", []byte("1")))

	orig := New()
	applyAll(t, orig, b.entries)
	data, at, err := orig.Snapshot()
	require.NoError(t, err)

	retry := raft.Entry{Index: at + 1, Term: 1, Type: raft.EntryNormal,
		Data: EncodePut(client, 1, "counter", []byte("1"))}

	t.Run("with sessions", func(t *testing.T) {
		m := New()
		require.NoError(t, m.Restore(raft.Snapshot{Meta: raft.SnapshotMeta{Index: at, Term: 1}, Data: data}))
		r, err := m.ApplyEntry(retry)
		require.NoError(t, err)
		require.True(t, r.Duplicate)
		require.Equal(t, raft.Index(2), r.AppliedIndex)
	})

	t.Run("negative control: sessions stripped from the snapshot", func(t *testing.T) {
		var img kvv1.StateMachineSnapshot
		require.NoError(t, proto.Unmarshal(data, &img))
		img.Sessions = nil
		stripped, err := proto.Marshal(&img)
		require.NoError(t, err)

		m := New()
		require.NoError(t, m.Restore(raft.Snapshot{Meta: raft.SnapshotMeta{Index: at, Term: 1}, Data: stripped}))
		r, err := m.ApplyEntry(retry)
		require.NoError(t, err)
		require.False(t, r.Duplicate, "without sessions the retry cannot be recognized")
	})
}

func TestApplyRefusesWhatItCannotApplyDeterministically(t *testing.T) {
	m := New()
	_, err := m.ApplyEntry(raft.Entry{Index: 2, Term: 1, Type: raft.EntryNoOp})
	require.ErrorContains(t, err, "in order", "a gap in applied entries must be refused")

	_, err = m.ApplyEntry(raft.Entry{Index: 1, Term: 1, Type: raft.EntryNormal, Data: []byte{0xff, 0xff}})
	require.ErrorContains(t, err, "does not decode")

	_, err = m.ApplyEntry(raft.Entry{Index: 1, Term: 1, Type: raft.EntryConfig})
	require.ErrorContains(t, err, "does not apply")

	data, _, err := New().Snapshot()
	require.NoError(t, err)
	require.ErrorContains(t, m.Restore(raft.Snapshot{Meta: raft.SnapshotMeta{Index: 7}, Data: data}),
		"describes the state at 0", "an image filed under the wrong index must be refused")
}
