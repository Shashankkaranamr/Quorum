package pbconv_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	raftv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/raft/v1"
	"github.com/Shashankkaranamr/Quorum/internal/pbconv"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// TestEntryTypeValuesMatch is the reason this package converts explicitly
// instead of casting.
//
// These values are written to disk. If the core's EntryType and the proto's
// EntryType ever drift apart numerically, every write-ahead log produced before
// the drift becomes unreadable, silently and in a way that looks like
// corruption rather than a schema change.
func TestEntryTypeValuesMatch(t *testing.T) {
	for _, tc := range []struct {
		core raft.EntryType
		wire raftv1.EntryType
	}{
		{raft.EntryUnspecified, raftv1.EntryType_ENTRY_TYPE_UNSPECIFIED},
		{raft.EntryNormal, raftv1.EntryType_ENTRY_TYPE_NORMAL},
		{raft.EntryNoOp, raftv1.EntryType_ENTRY_TYPE_NOOP},
		{raft.EntrySession, raftv1.EntryType_ENTRY_TYPE_SESSION},
		{raft.EntryConfig, raftv1.EntryType_ENTRY_TYPE_CONFIG},
	} {
		require.Equalf(t, int32(tc.core), int32(tc.wire),
			"raft.%s is %d but the wire value is %d; a WAL written with one cannot be read with the other",
			tc.core, int32(tc.core), int32(tc.wire))
	}
}

func TestEntryRoundTrip(t *testing.T) {
	for _, e := range []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNoOp},
		{Index: 2, Term: 3, Type: raft.EntryNormal, Data: []byte("hello")},
		{Index: 3, Term: 3, Type: raft.EntrySession, Data: []byte{0x00, 0xff}},
		{Index: 4, Term: 9, Type: raft.EntryConfig},
	} {
		p, err := pbconv.EntryToProto(e)
		require.NoError(t, err)

		// Through the actual wire encoding, not just the struct conversion.
		buf, err := proto.Marshal(p)
		require.NoError(t, err)
		var decoded raftv1.Entry
		require.NoError(t, proto.Unmarshal(buf, &decoded))

		got, err := pbconv.EntryFromProto(&decoded)
		require.NoError(t, err)
		require.Equal(t, e.Index, got.Index)
		require.Equal(t, e.Term, got.Term)
		require.Equal(t, e.Type, got.Type)
		require.Equal(t, e.Data, got.Data)
	}
}

func TestEntryRejectsUnspecifiedType(t *testing.T) {
	_, err := pbconv.EntryToProto(raft.Entry{Index: 1, Term: 1})
	require.Error(t, err, "an unspecified entry type is a bug, not a default")
	require.Contains(t, err.Error(), "unspecified")
}

func TestHardStateRoundTrip(t *testing.T) {
	for _, hs := range []raft.HardState{
		{},
		{Term: 7, VotedFor: 3, Commit: 42},
		{Term: 1},
	} {
		got := pbconv.HardStateFromProto(pbconv.HardStateToProto(hs))
		require.Equal(t, hs, got)
	}
	require.Equal(t, raft.HardState{}, pbconv.HardStateFromProto(nil))
}

// TestMessageRoundTrip covers every message type the core can produce, through
// a real protobuf marshal and unmarshal.
func TestMessageRoundTrip(t *testing.T) {
	entries := []raft.Entry{
		{Index: 5, Term: 2, Type: raft.EntryNormal, Data: []byte("x")},
		{Index: 6, Term: 2, Type: raft.EntryNoOp},
	}

	for _, m := range []raft.Message{
		{Type: raft.MsgRequestVote, From: 1, To: 2, Term: 4, LastLogIndex: 9, LastLogTerm: 3},
		{Type: raft.MsgRequestVoteResp, From: 2, To: 1, Term: 4, VoteGranted: true},
		{Type: raft.MsgRequestVoteResp, From: 2, To: 1, Term: 4, VoteGranted: false},
		{Type: raft.MsgAppendEntries, From: 1, To: 3, Term: 4,
			PrevLogIndex: 4, PrevLogTerm: 2, Entries: entries, LeaderCommit: 4},
		{Type: raft.MsgAppendEntries, From: 1, To: 3, Term: 4, PrevLogIndex: 4, PrevLogTerm: 2},
		{Type: raft.MsgAppendEntriesResp, From: 3, To: 1, Term: 4, Success: true, MatchIndex: 6},
		{Type: raft.MsgAppendEntriesResp, From: 3, To: 1, Term: 4,
			Success: false, ConflictIndex: 3, ConflictTerm: 1},
	} {
		t.Run(m.Type.String()+"/"+m.String(), func(t *testing.T) {
			p, err := pbconv.MessageToProto(m)
			require.NoError(t, err)

			buf, err := proto.Marshal(p)
			require.NoError(t, err)
			var decoded raftv1.Message
			require.NoError(t, proto.Unmarshal(buf, &decoded))

			got, err := pbconv.MessageFromProto(&decoded)
			require.NoError(t, err)
			require.Equal(t, m.Type, got.Type)
			require.Equal(t, m.From, got.From)
			require.Equal(t, m.To, got.To)
			require.Equal(t, m.Term, got.Term)
			require.Equal(t, m.LastLogIndex, got.LastLogIndex)
			require.Equal(t, m.LastLogTerm, got.LastLogTerm)
			require.Equal(t, m.VoteGranted, got.VoteGranted)
			require.Equal(t, m.PrevLogIndex, got.PrevLogIndex)
			require.Equal(t, m.PrevLogTerm, got.PrevLogTerm)
			require.Equal(t, m.LeaderCommit, got.LeaderCommit)
			require.Equal(t, m.Success, got.Success)
			require.Equal(t, m.MatchIndex, got.MatchIndex)
			require.Equal(t, m.ConflictTerm, got.ConflictTerm)
			require.Equal(t, m.ConflictIndex, got.ConflictIndex)
			require.Equal(t, len(m.Entries), len(got.Entries))
			for i := range m.Entries {
				require.Equal(t, m.Entries[i], got.Entries[i])
			}
		})
	}
}

// TestSnapshotMessagesAreRejectedUntilPhase4 keeps the unimplemented path loud.
// A silent zero value here would be a message that converts to nothing and is
// dropped without anyone noticing.
func TestSnapshotMessagesAreRejectedUntilPhase4(t *testing.T) {
	for _, typ := range []raft.MessageType{raft.MsgInstallSnapshot, raft.MsgInstallSnapshotResp} {
		_, err := pbconv.MessageToProto(raft.Message{Type: typ, From: 1, To: 2, Term: 1})
		require.Error(t, err)
		require.Contains(t, err.Error(), "phase 4")
	}
}

func TestMalformedInputIsRejected(t *testing.T) {
	_, err := pbconv.MessageFromProto(nil)
	require.Error(t, err)

	_, err = pbconv.MessageFromProto(&raftv1.Message{From: 1, To: 2, Term: 3})
	require.Error(t, err, "a message with no body must not decode to a silent zero value")
	require.Contains(t, err.Error(), "no recognized body")

	_, err = pbconv.EntryFromProto(nil)
	require.Error(t, err)

	_, err = pbconv.EntryFromProto(&raftv1.Entry{Index: 1, Term: 1, Type: raftv1.EntryType(99)})
	require.Error(t, err)

	_, err = pbconv.MessageToProto(raft.Message{Type: raft.MessageType(200)})
	require.Error(t, err)
}
