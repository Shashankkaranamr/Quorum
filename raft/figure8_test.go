package raft_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// driver drains a node's Ready and acknowledges it, standing in for
// internal/server so that this test can stay inside package raft with no
// import cycle. It does no I/O, so the ordering the real driver enforces is
// not in question here.
type driver struct{ n *raft.Node }

func (d driver) settle() {
	for range 100 {
		if !d.n.HasReady() {
			return
		}
		d.n.Ready()
		d.n.Advance()
	}
	panic("raft_test: node never settled")
}

func fig8Config(t *testing.T, mut raft.Mutation) raft.Config {
	t.Helper()
	return raft.Config{
		ID:                      1,
		Peers:                   []raft.NodeID{1, 2, 3, 4, 5},
		ElectionTimeoutMinTicks: 10,
		ElectionTimeoutMaxTicks: 20,
		HeartbeatTimeoutTicks:   3,
		MaxEntriesPerAppend:     64,
		Rand:                    func(n int) int { return n / 2 },

		// The state at the start of Figure 8(c): S1 holds an entry from term 2
		// at index 2 that a majority does not yet have, and the cluster has
		// since moved on to term 3. Index 1 is committed and common to all.
		HardState: raft.HardState{Term: 3, Commit: 1},
		Entries: []raft.Entry{
			{Index: 1, Term: 1, Type: raft.EntryNormal, Data: []byte("a")},
			{Index: 2, Term: 2, Type: raft.EntryNormal, Data: []byte("b")},
		},
		Applied:        1,
		UnsafeMutation: mut,
	}
}

// electS1 drives node 1 to leadership of term 4, as Figure 8(c) does.
func electS1(t *testing.T, mut raft.Mutation) (*raft.Node, driver) {
	t.Helper()
	n, err := raft.New(fig8Config(t, mut))
	require.NoError(t, err)
	d := driver{n}

	n.Campaign()
	require.Equal(t, raft.Term(4), n.Status().Term)

	// Two peers vote yes, which with its own vote gives node 1 the quorum of
	// three. Their logs are shorter, so a correct voter grants.
	for _, from := range []raft.NodeID{2, 3} {
		require.NoError(t, n.Step(raft.Message{
			Type: raft.MsgRequestVoteResp, From: from, To: 1, Term: 4, VoteGranted: true,
		}))
	}
	require.Equal(t, raft.Leader, n.Status().Role)

	// Settle so the term-4 no-op at index 3 becomes durable and the leader's
	// own match index catches up to it.
	d.settle()
	require.Equal(t, raft.Index(3), n.Status().LastLogIndex)
	require.Equal(t, raft.Index(3), n.Status().StableIndex)
	return n, d
}

// TestFigure8CommitRule is the test the Figure 8 rule exists for.
//
// The scenario, from the Raft paper:
//
//	(a) S1 leads term 2 and partially replicates an entry at index 2.
//	(b) S1 crashes; S5 wins term 3 with a different entry at index 2.
//	(c) S5 crashes; S1 wins term 4 and replicates its OLD term-2 entry to a
//	    majority. It is now on three of five nodes.
//	(d) If S1 commits it on that majority count alone, S5 can still win term 5
//	    -- its last term is 3, which beats 2 -- and overwrite index 2.
//
// The entry was on a majority and was still lost. That is why a leader may
// only commit an entry from its OWN term by counting replicas.
//
// This test drives node 1 directly rather than through the simulator. With a
// no-op appended on election, index 2 and index 3 normally replicate in the
// same message, so the dangerous window closes almost immediately. It is
// reached deliberately here by having two followers acknowledge index 2 while
// their acknowledgement of index 3 is still in flight -- which is an ordinary
// thing for a network to do, and exactly the case the rule must survive.
func TestFigure8CommitRule(t *testing.T) {
	t.Run("correct implementation refuses to commit an old-term entry", func(t *testing.T) {
		n, _ := electS1(t, raft.MutationNone)

		// Two followers acknowledge only up to index 2. Together with the
		// leader itself that is a majority holding the term-2 entry.
		for _, from := range []raft.NodeID{2, 3} {
			require.NoError(t, n.Step(raft.Message{
				Type: raft.MsgAppendEntriesResp, From: from, To: 1, Term: 4,
				Success: true, MatchIndex: 2,
			}))
		}

		s := n.Status()
		require.Equal(t, raft.Index(2), s.Progress[2].MatchIndex)
		require.Equal(t, raft.Index(2), s.Progress[3].MatchIndex)
		require.Equal(t, raft.Index(3), s.Progress[1].MatchIndex,
			"the leader should count its own durable no-op")

		require.Lessf(t, s.CommitIndex, raft.Index(2),
			"committed index 2 (term 2) on a majority count while leading term 4; "+
				"that entry can still be overwritten by a legitimate term-5 leader")
	})

	t.Run("removing the rule commits it", func(t *testing.T) {
		n, _ := electS1(t, raft.MutationCommitAnyTerm)

		for _, from := range []raft.NodeID{2, 3} {
			require.NoError(t, n.Step(raft.Message{
				Type: raft.MsgAppendEntriesResp, From: from, To: 1, Term: 4,
				Success: true, MatchIndex: 2,
			}))
		}

		require.Equal(t, raft.Index(2), n.Status().CommitIndex,
			"the mutation is supposed to remove the rule; if this fails the "+
				"negative control is not testing what it claims to")
	})

	// The consequence. A legitimate term-5 leader arrives holding a different
	// entry at index 2, whose last term (3) beats ours (2), so it genuinely
	// won the election and genuinely gets to overwrite us.
	//
	// Under the correct rule that is harmless: we never told anyone index 2
	// was committed. Under the mutation it is data loss, and the node says so.
	overwrite := raft.Message{
		Type: raft.MsgAppendEntries, From: 5, To: 1, Term: 5,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries:      []raft.Entry{{Index: 2, Term: 3, Type: raft.EntryNormal, Data: []byte("c")}},
		LeaderCommit: 1,
	}

	t.Run("correct implementation accepts the overwrite without complaint", func(t *testing.T) {
		n, _ := electS1(t, raft.MutationNone)
		for _, from := range []raft.NodeID{2, 3} {
			require.NoError(t, n.Step(raft.Message{
				Type: raft.MsgAppendEntriesResp, From: from, To: 1, Term: 4,
				Success: true, MatchIndex: 2,
			}))
		}

		require.NotPanics(t, func() { require.NoError(t, n.Step(overwrite)) })

		log := n.LogEntries()
		require.Len(t, log, 2)
		require.Equal(t, raft.Term(3), log[1].Term, "index 2 should now hold the term-3 entry")
		require.Equal(t, raft.Follower, n.Status().Role)
	})

	t.Run("without the rule the overwrite destroys a committed entry", func(t *testing.T) {
		n, _ := electS1(t, raft.MutationCommitAnyTerm)
		for _, from := range []raft.NodeID{2, 3} {
			require.NoError(t, n.Step(raft.Message{
				Type: raft.MsgAppendEntriesResp, From: from, To: 1, Term: 4,
				Success: true, MatchIndex: 2,
			}))
		}
		require.Equal(t, raft.Index(2), n.Status().CommitIndex)

		require.PanicsWithValue(t, raft.SafetyViolation{
			Property: raft.PropertyCommittedEntriesAreStable,
			Detail:   "node 1 asked to overwrite committed index 2 (commit=2) by leader 5 term 5",
		}, func() { _ = n.Step(overwrite) })
	})
}
