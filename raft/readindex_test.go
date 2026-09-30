package raft_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// newLeader makes node 1 of 3 the leader of term 1. If commit is set, its
// no-op is also committed, which is when a new leader first knows its commit
// index.
func newLeader(t *testing.T, mut raft.Mutation, commit bool) *raft.Node {
	t.Helper()
	cfg := baseConfig(1, 3)
	cfg.UnsafeMutation = mut
	n := newNode(t, cfg)
	n.Campaign()
	drain(n)
	require.NoError(t, n.Step(raft.Message{Type: raft.MsgRequestVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true}))
	drain(n)
	require.Equal(t, raft.Leader, n.Status().Role)
	if commit {
		require.NoError(t, n.Step(appendResp(2, 1, 0)))
		drain(n)
		require.Equal(t, raft.Index(1), n.Status().CommitIndex)
	}
	return n
}

func appendResp(from raft.NodeID, match raft.Index, readSeq uint64) raft.Message {
	return raft.Message{Type: raft.MsgAppendEntriesResp, From: from, To: 1, Term: 1,
		Success: true, MatchIndex: match, ReadSeq: readSeq}
}

// roundOf is the read sequence number the leader stamped on its AppendEntries.
func roundOf(t *testing.T, rd raft.Ready) uint64 {
	t.Helper()
	var seq uint64
	for _, m := range rd.Messages {
		if m.Type == raft.MsgAppendEntries {
			seq = max(seq, m.ReadSeq)
		}
	}
	require.NotZero(t, seq, "no AppendEntries carried a read round")
	return seq
}

// TestReadIndexWaitsForAQuorum: a read is confirmed only once a quorum -- the
// leader and one follower, of three -- has echoed a round sent after the read
// was made. Until then there is no ReadState at all.
func TestReadIndexWaitsForAQuorum(t *testing.T) {
	n := newLeader(t, raft.MutationNone, true)
	require.NoError(t, n.ReadIndex(7))
	rd := drain(n)
	require.Empty(t, rd.ReadStates, "a read must not be confirmed by the leader alone")
	seq := roundOf(t, rd)

	// An echo of an EARLIER round proves nothing about now.
	require.NoError(t, n.Step(appendResp(2, 1, seq-1)))
	require.Empty(t, drain(n).ReadStates)

	// A rejection still counts: the follower recognized us as leader.
	rej := appendResp(3, 0, seq)
	rej.Success = false
	require.NoError(t, n.Step(rej))
	rd = drain(n)
	require.Equal(t, []raft.ReadState{{ID: 7, Index: 1}}, rd.ReadStates)
}

// TestReadIndexWaitsForTheTermsFirstCommit: before a new leader commits its
// no-op, its commit index may be behind what earlier leaders committed
// (§5.4.2), so a read made then must not start its round until the no-op
// commits -- and its read index must then be at least the no-op.
func TestReadIndexWaitsForTheTermsFirstCommit(t *testing.T) {
	n := newLeader(t, raft.MutationNone, false)
	require.NoError(t, n.ReadIndex(1))
	rd := drain(n)
	for _, m := range rd.Messages {
		require.Zero(t, m.ReadSeq, "a round started before the leader knew its commit index")
	}

	require.NoError(t, n.Step(appendResp(2, 1, 0))) // commits the no-op; the round starts
	rd = drain(n)
	seq := roundOf(t, rd)
	require.Empty(t, rd.ReadStates)

	require.NoError(t, n.Step(appendResp(2, 1, seq)))
	rd = drain(n)
	require.Equal(t, []raft.ReadState{{ID: 1, Index: 1}}, rd.ReadStates)
}

// TestReadsAreAbandonedOnStepDown: a leader that learns of a higher term can
// no longer confirm anything in its own, so every pending read is reported
// lost rather than left hanging or, worse, confirmed later.
func TestReadsAreAbandonedOnStepDown(t *testing.T) {
	n := newLeader(t, raft.MutationNone, true)
	require.NoError(t, n.ReadIndex(1))
	drain(n)

	require.NoError(t, n.Step(raft.Message{Type: raft.MsgAppendEntries, From: 2, To: 1, Term: 2}))
	rd := drain(n)
	require.Equal(t, []raft.ReadState{{ID: 1, Lost: true}}, rd.ReadStates)
	require.ErrorIs(t, n.ReadIndex(2), raft.ErrNotLeader)
}

// TestReadWithoutQuorumMutationConfirmsAlone pins down what the negative
// control breaks: with it, a read confirms with no follower's help at all.
func TestReadWithoutQuorumMutationConfirmsAlone(t *testing.T) {
	n := newLeader(t, raft.MutationReadWithoutQuorum, true)
	require.NoError(t, n.ReadIndex(3))
	require.Equal(t, []raft.ReadState{{ID: 3, Index: 1}}, drain(n).ReadStates)
}

func TestReadIDsMustBeUnique(t *testing.T) {
	n := newLeader(t, raft.MutationNone, true)
	require.NoError(t, n.ReadIndex(5))
	require.ErrorIs(t, n.ReadIndex(5), raft.ErrReadIDInUse)
}
