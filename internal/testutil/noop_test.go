package testutil_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/internal/testutil"
	"github.com/Shashankkaranamr/Quorum/internal/transport/inmem"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// checkTermsStartWithNoop verifies one log: wherever the term changes, the
// first entry of the new term is a no-op. prevTerm is the term of the entry
// just before the log begins -- the snapshot's -- so a term that starts
// exactly at the compaction boundary is still checked.
func checkTermsStartWithNoop(id raft.NodeID, prevTerm raft.Term, log []raft.Entry) error {
	for _, e := range log {
		if e.Term != prevTerm && e.Type != raft.EntryNoOp {
			return fmt.Errorf("node %d: term %d begins at index %d with a %s entry, not a no-op",
				id, e.Term, e.Index, e.Type)
		}
		prevTerm = e.Term
	}
	return nil
}

// TestNoopCommittedOnElection is phase 5 acceptance criterion 2. Across
// randomized schedules with crashes, partitions and message loss -- so many
// elections, some won by nodes that then lose their leadership at once -- the
// first entry of every term in every log is a no-op, and once the cluster
// settles, the current leader's no-op is committed. ReadIndex depends on it:
// until a leader commits something of its own term it does not know its
// commit index.
func TestNoopCommittedOnElection(t *testing.T) {
	seeds := 30
	if testing.Short() {
		seeds = 6
	}
	terms := 0
	for _, n := range []int{3, 5} {
		for seed := range uint64(seeds) {
			opts := testutil.DefaultOptions(n, seed+500)
			opts.Fault = inmem.Fault{DropRate: 0.05, DuplicateRate: 0.05, MaxDelayTicks: 2, ReorderDueBatch: true}
			opts.SnapshotThreshold = 25
			c := newCluster(t, opts)
			rng := rand.New(rand.NewPCG(seed, 9))
			for i := range 400 {
				if rng.IntN(100) < 3 {
					id := c.IDs[rng.IntN(n)]
					c.Crash(id)
					c.RunTicks(rng.IntN(20))
					require.NoError(t, c.Restart(id))
				}
				if rng.IntN(100) < 2 {
					c.Isolate(c.IDs[rng.IntN(n)])
				} else if rng.IntN(100) < 5 {
					c.Heal()
				}
				if id, ok := c.Leader(); ok && rng.IntN(100) < 30 {
					_, _, _ = c.Propose(id, []byte(fmt.Sprintf("v%d", i)))
				}
				c.Tick()
			}
			c.Heal()
			electLeader(t, c)
			c.RunTicks(50)
			// Loss is still on, so leadership may have moved while settling.
			lead := electLeader(t, c)

			for _, id := range c.LiveIDs() {
				st := c.Status(id)
				require.NoError(t, checkTermsStartWithNoop(id, st.SnapshotTerm, c.Log(id)), "seed %d n=%d", seed, n)
			}
			st := c.Status(lead)
			elected, _ := c.Checker.LeaderOfTerm(st.Term)
			require.Equal(t, lead, elected)
			require.Equal(t, st.Term, committedTermAt(c, lead, st.CommitIndex),
				"seed %d: leader %d has not committed anything in its own term %d", seed, lead, st.Term)
			require.NoError(t, c.Err())
			terms += c.Checker.TermsWithLeaders()
		}
	}
	t.Logf("%d terms with an elected leader checked", terms)
}

// committedTermAt is the term of a node's entry at index i.
func committedTermAt(c *testutil.Cluster, id raft.NodeID, i raft.Index) raft.Term {
	st := c.Status(id)
	if i == st.SnapshotIndex {
		return st.SnapshotTerm
	}
	for _, e := range c.Log(id) {
		if e.Index == i {
			return e.Term
		}
	}
	return 0
}

// TestNoopCheckCatchesATermWithoutOne is the negative control for criterion 2:
// a log in which a term begins with a client command must be rejected,
// including when the term starts right at the snapshot boundary.
func TestNoopCheckCatchesATermWithoutOne(t *testing.T) {
	good := []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNoOp}, {Index: 2, Term: 1, Type: raft.EntryNormal},
		{Index: 3, Term: 3, Type: raft.EntryNoOp},
	}
	require.NoError(t, checkTermsStartWithNoop(1, 0, good))

	bad := []raft.Entry{
		{Index: 1, Term: 1, Type: raft.EntryNoOp}, {Index: 2, Term: 2, Type: raft.EntryNormal},
	}
	require.Error(t, checkTermsStartWithNoop(1, 0, bad))

	afterSnapshot := []raft.Entry{{Index: 6, Term: 4, Type: raft.EntryNormal}}
	require.Error(t, checkTermsStartWithNoop(1, 3, afterSnapshot))
	require.NoError(t, checkTermsStartWithNoop(1, 4, afterSnapshot), "a term continuing across the boundary is fine")
}
