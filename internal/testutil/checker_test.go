package testutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/internal/testutil"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// This file is the second layer of negative control.
//
// TestMutatedRaftTripsInvariant proves that a broken Raft produces violations
// the checkers notice. It cannot cover every checker, because some violations
// are not reachable from any single surgical mutation -- Log Matching in
// particular is protected by several rules at once.
//
// So each checker is also fed a hand-built history that violates exactly one
// property, and is required to report exactly that property. Between the two
// layers, every checker has been observed to fail for the right reason.

func view(id raft.NodeID, role raft.Role, term raft.Term, commit raft.Index, entries ...raft.Entry) testutil.NodeView {
	last := raft.Index(0)
	lastTerm := raft.Term(0)
	if n := len(entries); n > 0 {
		last, lastTerm = entries[n-1].Index, entries[n-1].Term
	}
	return testutil.NodeView{
		Status: raft.Status{
			ID: id, Role: role, Term: term,
			CommitIndex: commit, LastApplied: commit,
			LastLogIndex: last, LastLogTerm: lastTerm,
			// A distinct revision per observation, so the checker's
			// change-detection never skips a hand-built view.
			LogRevision: uint64(len(entries))*1000 + uint64(term),
		},
		Log: entries,
	}
}

func e(index raft.Index, term raft.Term) raft.Entry {
	return raft.Entry{Index: index, Term: term, Type: raft.EntryNormal}
}

func observe(c *testutil.Checker, tick uint64, views ...testutil.NodeView) {
	m := make(map[raft.NodeID]testutil.NodeView, len(views))
	order := make([]raft.NodeID, 0, len(views))
	for _, v := range views {
		m[v.Status.ID] = v
		order = append(order, v.Status.ID)
	}
	c.Observe(tick, m, order)
}

func TestCheckerDetectsHandBuiltViolations(t *testing.T) {
	t.Run("ElectionSafety: two leaders in one term", func(t *testing.T) {
		c := testutil.NewChecker()
		observe(c, 1,
			view(1, raft.Leader, 5, 1, e(1, 5)),
			view(2, raft.Leader, 5, 1, e(1, 5)),
		)
		require.True(t, c.Violated(testutil.ElectionSafety), "%v", c.Violations())
	})

	t.Run("LeaderAppendOnly: a leader's log shrinks", func(t *testing.T) {
		c := testutil.NewChecker()
		observe(c, 1, view(1, raft.Leader, 5, 0, e(1, 5), e(2, 5), e(3, 5)))
		observe(c, 2, view(1, raft.Leader, 5, 0, e(1, 5), e(2, 5)))
		require.True(t, c.Violated(testutil.LeaderAppendOnly), "%v", c.Violations())
	})

	t.Run("LeaderAppendOnly: a leader rewrites its own entry", func(t *testing.T) {
		c := testutil.NewChecker()
		observe(c, 1, view(1, raft.Leader, 5, 0, e(1, 5), e(2, 5)))
		observe(c, 2, view(1, raft.Leader, 5, 0, e(1, 5), e(2, 4)))
		require.True(t, c.Violated(testutil.LeaderAppendOnly), "%v", c.Violations())
	})

	t.Run("LogMatching: same index and term, different prefix", func(t *testing.T) {
		c := testutil.NewChecker()
		// Both hold index 3 at term 7, so every earlier entry must be
		// identical. Index 2 differs, which is exactly the violation.
		observe(c, 1,
			view(1, raft.Follower, 7, 0, e(1, 1), e(2, 2), e(3, 7)),
			view(2, raft.Follower, 7, 0, e(1, 1), e(2, 5), e(3, 7)),
		)
		require.True(t, c.Violated(testutil.LogMatching), "%v", c.Violations())
	})

	t.Run("LogMatching: identical logs are fine", func(t *testing.T) {
		c := testutil.NewChecker()
		observe(c, 1,
			view(1, raft.Follower, 7, 0, e(1, 1), e(2, 2), e(3, 7)),
			view(2, raft.Follower, 7, 0, e(1, 1), e(2, 2), e(3, 7)),
		)
		require.Empty(t, c.Violations(), "a correct history must not be flagged")
	})

	t.Run("LogMatching: divergent suffixes with different terms are fine", func(t *testing.T) {
		c := testutil.NewChecker()
		// Two nodes on different branches. No shared index carries the same
		// term, so nothing is violated -- this is ordinary mid-election state.
		observe(c, 1,
			view(1, raft.Follower, 7, 0, e(1, 1), e(2, 2)),
			view(2, raft.Follower, 7, 0, e(1, 1), e(2, 3)),
		)
		require.Empty(t, c.Violations(), "divergent uncommitted suffixes are legal")
	})

	t.Run("LeaderCompleteness: a new leader is missing a committed entry", func(t *testing.T) {
		c := testutil.NewChecker()
		// Node 1 commits index 2 in term 3.
		observe(c, 1, view(1, raft.Leader, 3, 2, e(1, 1), e(2, 3)))
		// Node 2 then leads term 9 without it.
		observe(c, 2, view(2, raft.Leader, 9, 0, e(1, 1)))
		require.True(t, c.Violated(testutil.LeaderCompleteness), "%v", c.Violations())
	})

	t.Run("LeaderCompleteness: a new leader holding it is fine", func(t *testing.T) {
		c := testutil.NewChecker()
		observe(c, 1, view(1, raft.Leader, 3, 2, e(1, 1), e(2, 3)))
		observe(c, 2, view(2, raft.Leader, 9, 0, e(1, 1), e(2, 3), e(3, 9)))
		require.False(t, c.Violated(testutil.LeaderCompleteness), "%v", c.Violations())
	})

	t.Run("StateMachineSafety: two nodes apply different entries at one index", func(t *testing.T) {
		c := testutil.NewChecker()
		c.RecordApply(1, 1, raft.Entry{Index: 4, Term: 2, Data: []byte("a")})
		c.RecordApply(2, 2, raft.Entry{Index: 4, Term: 3, Data: []byte("b")})
		require.True(t, c.Violated(testutil.StateMachineSafety), "%v", c.Violations())
	})

	t.Run("StateMachineSafety: applying the same entry twice is fine", func(t *testing.T) {
		c := testutil.NewChecker()
		ent := raft.Entry{Index: 4, Term: 2, Data: []byte("a")}
		c.RecordApply(1, 1, ent)
		c.RecordApply(2, 2, ent)
		c.RecordApply(3, 3, ent)
		require.Empty(t, c.Violations())
	})

	t.Run("CommittedEntriesAreStable: committed index changes term", func(t *testing.T) {
		c := testutil.NewChecker()
		observe(c, 1, view(1, raft.Follower, 4, 2, e(1, 1), e(2, 2)))
		observe(c, 2, view(2, raft.Follower, 5, 2, e(1, 1), e(2, 3)))
		require.True(t, c.Violated(testutil.CommittedEntriesAreStable), "%v", c.Violations())
	})

	t.Run("WellFormed: applied past committed", func(t *testing.T) {
		c := testutil.NewChecker()
		v := view(1, raft.Follower, 4, 1, e(1, 1), e(2, 2))
		v.Status.LastApplied = 2
		observe(c, 1, v)
		require.True(t, c.Violated(testutil.WellFormed), "%v", c.Violations())
	})

	t.Run("WellFormed: committed past the end of the log", func(t *testing.T) {
		c := testutil.NewChecker()
		v := view(1, raft.Follower, 4, 9, e(1, 1))
		v.Status.LastApplied = 0
		observe(c, 1, v)
		require.True(t, c.Violated(testutil.WellFormed), "%v", c.Violations())
	})
}

// TestCheckerAcceptsAHealthyHistory is the other half of the control: a checker
// that fires on everything is as useless as one that fires on nothing.
func TestCheckerAcceptsAHealthyHistory(t *testing.T) {
	c := testutil.NewChecker()

	log := []raft.Entry{e(1, 1), e(2, 1), e(3, 2)}
	for tick := uint64(1); tick <= 20; tick++ {
		observe(c, tick,
			view(1, raft.Leader, 2, 3, log...),
			view(2, raft.Follower, 2, 3, log...),
			view(3, raft.Follower, 2, 2, log[:2]...),
		)
		for _, id := range []raft.NodeID{1, 2} {
			for _, ent := range log {
				c.RecordApply(tick, id, ent)
			}
		}
	}
	require.NoError(t, c.Err(), "a healthy history was flagged")
	require.Equal(t, raft.Index(3), c.MaxCommitted)

	id, ok := c.LeaderOfTerm(2)
	require.True(t, ok)
	require.Equal(t, raft.NodeID(1), id)
	require.Equal(t, 1, c.TermsWithLeaders())
}
