package testutil_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/internal/testutil"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// electionBoundTicks is the liveness bound: absent a permanent partition, a
// leader must emerge within this many logical ticks.
//
// Justification, with the election timeout range at 10-20 ticks:
//
//   - One election attempt costs at most ElectionTimeoutMax (20) ticks to
//     start, plus a round trip to collect votes, which in the simulator is 1-2
//     ticks on a clean network and up to MaxDelayTicks on a lossy one.
//   - A split vote costs one more attempt. Randomized timeouts make repeated
//     splits geometrically unlikely: two nodes must draw from a 10-tick range
//     closely enough to campaign in the same window.
//   - 200 ticks is 10 x ElectionTimeoutMax, which leaves room for roughly ten
//     consecutive failed attempts.
//
// TestElectionConverges reports the worst case actually observed across 1000
// seeds, so this number is checked against measurement rather than asserted.
// It is also small enough that a hang fails in milliseconds of simulated time
// rather than stalling CI.
const electionBoundTicks = 200

func electLeader(t *testing.T, c *testutil.Cluster) raft.NodeID {
	t.Helper()
	ok, ticks := c.RunUntil(electionBoundTicks, func() bool {
		id, ok := c.Leader()
		return ok && c.Status(id).CommitIndex > 0
	})
	require.True(t, ok, "no leader committed its no-op within %d ticks:\n%s",
		electionBoundTicks, c)
	id, _ := c.Leader()
	t.Logf("node %d leads after %d ticks", id, ticks)
	return id
}

// TestElectionConverges is PLAN.md phase 2 acceptance criterion 2: over 1000
// seeds and both cluster sizes, a cold-start cluster elects exactly one leader
// within the bound.
//
// It also measures the worst case, which is what justifies the bound rather
// than leaving it a guess.
func TestElectionConverges(t *testing.T) {
	seeds := 1000
	if testing.Short() {
		seeds = 50
	}

	for _, n := range []int{3, 5} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			worst, worstSeed := 0, uint64(0)
			for seed := uint64(1); seed <= uint64(seeds); seed++ {
				c, err := testutil.NewCluster(testutil.DefaultOptions(n, seed))
				require.NoError(t, err)

				ok, ticks := c.RunUntil(electionBoundTicks, func() bool {
					_, ok := c.Leader()
					return ok
				})
				require.Truef(t, ok,
					"seed %d (n=%d): no leader within %d ticks. Reproduce with that seed.\n%s",
					seed, n, electionBoundTicks, c)
				require.NoErrorf(t, c.Err(), "seed %d (n=%d)", seed, n)

				if ticks > worst {
					worst, worstSeed = ticks, seed
				}
			}
			t.Logf("n=%d: %d seeds, worst election took %d ticks (seed %d), bound is %d",
				n, seeds, worst, worstSeed, electionBoundTicks)
			require.Less(t, worst, electionBoundTicks,
				"the bound is not comfortably above the observed worst case")
		})
	}
}

// TestProposalsCommitAndApply is acceptance criterion 5: entries proposed on
// the leader are committed and applied in identical order on every live node.
func TestProposalsCommitAndApply(t *testing.T) {
	c := newCluster(t, testutil.DefaultOptions(5, 7))
	leader := electLeader(t, c)

	const n = 25
	for i := range n {
		_, _, err := c.Propose(leader, []byte(fmt.Sprintf("cmd-%d", i)))
		require.NoError(t, err)
		c.RunTicks(1)
	}

	ok, _ := c.RunUntil(electionBoundTicks, func() bool {
		for _, id := range c.IDs {
			if countNormal(c.Applied(id)) < n {
				return false
			}
		}
		return true
	})
	require.True(t, ok, "not every node applied all %d commands:\n%s", n, c)
	require.NoError(t, c.Err())

	// Identical order everywhere, not merely identical contents.
	want := c.Applied(c.IDs[0])
	for _, id := range c.IDs[1:] {
		got := c.Applied(id)
		require.Equal(t, len(want), len(got), "node %d applied a different number of entries", id)
		for i := range want {
			require.Equalf(t, want[i].Index, got[i].Index,
				"node %d applied a different index at position %d", id, i)
			require.Equalf(t, want[i].Term, got[i].Term,
				"node %d applied a different term at position %d", id, i)
			require.Equalf(t, want[i].Data, got[i].Data,
				"node %d applied different data at index %d", id, want[i].Index)
		}
	}
}

func countNormal(ents []raft.Entry) int {
	n := 0
	for _, e := range ents {
		if e.Type == raft.EntryNormal {
			n++
		}
	}
	return n
}

// TestLeaderFailoverPreservesCommitted is acceptance criterion 6: kill the
// leader, and every entry committed before the kill survives into the new
// leader's log.
func TestLeaderFailoverPreservesCommitted(t *testing.T) {
	c := newCluster(t, testutil.DefaultOptions(5, 11))
	leader := electLeader(t, c)

	for i := range 10 {
		_, _, err := c.Propose(leader, []byte(fmt.Sprintf("before-%d", i)))
		require.NoError(t, err)
		c.RunTicks(2)
	}
	c.RunTicks(20)

	committed := c.Status(leader).CommitIndex
	require.Greater(t, committed, raft.Index(5), "expected real progress before the kill")

	// Snapshot what was committed, so the assertion afterwards is against the
	// history rather than against whatever survives.
	before := make(map[raft.Index]raft.Entry)
	for _, e := range c.Log(leader) {
		if e.Index <= committed {
			before[e.Index] = e
		}
	}

	c.Crash(leader)
	t.Logf("killed leader %d with commit index %d", leader, committed)

	ok, ticks := c.RunUntil(electionBoundTicks, func() bool {
		id, ok := c.Leader()
		return ok && id != leader
	})
	require.True(t, ok, "no new leader within %d ticks:\n%s", electionBoundTicks, c)
	require.NoError(t, c.Err())

	newLeader, _ := c.Leader()
	t.Logf("node %d took over after %d ticks", newLeader, ticks)

	// Leader Completeness, asserted directly rather than trusting the checker.
	got := make(map[raft.Index]raft.Entry)
	for _, e := range c.Log(newLeader) {
		got[e.Index] = e
	}
	for idx, want := range before {
		have, ok := got[idx]
		require.Truef(t, ok, "new leader %d is missing committed entry %d (%s)", newLeader, idx, want)
		require.Equalf(t, want.Term, have.Term,
			"new leader %d has term %d at index %d, but term %d was committed there",
			newLeader, have.Term, idx, want.Term)
		require.Equalf(t, want.Data, have.Data, "data changed at committed index %d", idx)
	}

	// And the cluster must still make progress.
	_, _, err := c.Propose(newLeader, []byte("after"))
	require.NoError(t, err)
	progressed, _ := c.RunUntil(electionBoundTicks, func() bool {
		return c.Status(newLeader).CommitIndex > committed+1
	})
	require.True(t, progressed, "cluster did not commit after failover:\n%s", c)
	require.NoError(t, c.Err())
}

// TestMinorityPartitionCannotCommit is acceptance criterion 7 and the explicit
// partition scenario the phase brief calls for.
//
// The interesting arrangement is the one where the OLD LEADER is stranded in
// the minority. It does not know it has lost quorum, so it keeps accepting
// proposals -- and must never commit one. A test that partitions the leader
// into the majority instead proves much less.
func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := newCluster(t, testutil.DefaultOptions(5, 23))
	leader := electLeader(t, c)

	for i := range 5 {
		_, _, err := c.Propose(leader, []byte(fmt.Sprintf("pre-%d", i)))
		require.NoError(t, err)
		c.RunTicks(2)
	}
	c.RunTicks(20)
	require.NoError(t, c.Err())

	commonCommit := c.Status(leader).CommitIndex
	committedBefore := map[raft.Index]raft.Entry{}
	for _, e := range c.Log(leader) {
		if e.Index <= commonCommit {
			committedBefore[e.Index] = e
		}
	}
	require.Greater(t, commonCommit, raft.Index(3))

	// Strand the leader with one companion: 2 against 3.
	minority := []raft.NodeID{leader}
	var majority []raft.NodeID
	for _, id := range c.IDs {
		switch {
		case id == leader:
		case len(minority) < 2:
			minority = append(minority, id)
		default:
			majority = append(majority, id)
		}
	}
	require.Len(t, minority, 2)
	require.Len(t, majority, 3)
	c.Partition(minority, majority)
	t.Logf("partitioned: minority %v (holding old leader %d) | majority %v", minority, leader, majority)

	// The old leader still believes it leads, so proposals are accepted.
	for i := range 5 {
		if _, _, err := c.Propose(leader, []byte(fmt.Sprintf("stranded-%d", i))); err != nil {
			require.ErrorIs(t, err, raft.ErrNotLeader)
		}
		c.RunTicks(2)
	}
	c.RunTicks(electionBoundTicks)
	require.NoError(t, c.Err())

	// 1. The minority elects nobody.
	minLeader, hasMinLeader := c.LeaderIn(minority)
	if hasMinLeader {
		require.Equalf(t, leader, minLeader,
			"minority elected a NEW leader (%d); only the stale pre-partition leader may still think it leads",
			minLeader)
	}

	// 2. The minority commits nothing new.
	for _, id := range minority {
		require.LessOrEqualf(t, c.Status(id).CommitIndex, commonCommit,
			"minority node %d advanced its commit index from %d to %d without a quorum",
			id, commonCommit, c.Status(id).CommitIndex)
		require.LessOrEqualf(t, raft.Index(len(c.Applied(id))), commonCommit,
			"minority node %d applied an entry it could not have committed", id)
	}

	// 3. The majority elects and commits normally.
	majLeader, ok := c.LeaderIn(majority)
	require.True(t, ok, "majority failed to elect a leader:\n%s", c)
	require.NotEqual(t, leader, majLeader)

	_, _, err := c.Propose(majLeader, []byte("majority-write"))
	require.NoError(t, err)
	progressed, _ := c.RunUntil(electionBoundTicks, func() bool {
		return c.Status(majLeader).CommitIndex > commonCommit+1
	})
	require.True(t, progressed, "majority could not commit:\n%s", c)
	majCommit := c.Status(majLeader).CommitIndex

	// 4. Heal and require full reconvergence.
	//
	// First confirm the minority is genuinely behind. A reconvergence test run
	// against a cluster that never diverged proves nothing, and "converged in
	// one tick" is exactly what that would look like.
	majTerm := c.Status(majLeader).Term
	diverged := false
	for _, id := range minority {
		s := c.Status(id)
		if s.CommitIndex != majCommit || s.Term != majTerm {
			diverged = true
			t.Logf("minority node %d is behind at heal time: term %d commit %d vs majority term %d commit %d",
				id, s.Term, s.CommitIndex, majTerm, majCommit)
		}
	}
	require.True(t, diverged,
		"the minority was not actually behind when the partition healed, so reconvergence proves nothing")

	c.Heal()
	t.Logf("healed; majority leader %d at commit %d", majLeader, majCommit)

	converged, ticks := c.RunUntil(4*electionBoundTicks, func() bool {
		lead, ok := c.Leader()
		if !ok {
			return false
		}
		target := c.Status(lead).CommitIndex
		for _, id := range c.IDs {
			s := c.Status(id)
			if s.CommitIndex != target || s.Term != c.Status(lead).Term {
				return false
			}
		}
		return true
	})
	require.True(t, converged, "cluster did not reconverge after healing:\n%s", c)
	require.NoError(t, c.Err())
	t.Logf("reconverged %d ticks after heal", ticks)

	// Every node's log must be identical up to the global commit index.
	final, _ := c.Leader()
	target := c.Status(final).CommitIndex
	want := c.Log(final)
	for _, id := range c.IDs {
		got := c.Log(id)
		for i := range want {
			if want[i].Index > target {
				break
			}
			require.Greaterf(t, len(got), i, "node %d is missing committed index %d", id, want[i].Index)
			require.Equalf(t, want[i].Index, got[i].Index, "node %d diverges at position %d", id, i)
			require.Equalf(t, want[i].Term, got[i].Term,
				"node %d has term %d at index %d, leader has %d",
				id, got[i].Term, got[i].Index, want[i].Term)
		}
	}

	// And nothing committed before the partition was altered by any of it.
	// The checker verified this after every single tick; this re-states it
	// against the entries recorded up front.
	for idx, e := range committedBefore {
		have, ok := c.Checker.CommittedEntry(idx)
		require.Truef(t, ok, "index %d stopped being committed anywhere", idx)
		require.Equalf(t, e.Term, have.Term,
			"entry committed at index %d changed from term %d to term %d during the partition",
			idx, e.Term, have.Term)
	}
	require.NotZero(t, c.Net.Stats.Partitioned,
		"the partition never actually dropped a message, so this test proved nothing")
}
