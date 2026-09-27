package testutil_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/internal/storage"
	"github.com/Shashankkaranamr/Quorum/internal/testutil"
	"github.com/Shashankkaranamr/Quorum/internal/transport/inmem"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// newDiskCluster builds a simulated cluster whose nodes run on real
// write-ahead logs in a temporary directory.
func newDiskCluster(t *testing.T, opts testutil.Options) *testutil.Cluster {
	t.Helper()
	if opts.Storage == nil {
		opts.Storage = testutil.WALStorageFactory(t.TempDir(), storage.WALOptions{SegmentBytes: 32 << 10})
	}
	c, err := testutil.NewCluster(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
		if t.Failed() {
			t.Logf("final state:\n%s", c)
		}
	})
	return c
}

// committedHistory is every entry the cluster had committed, taken from the
// node with the highest commit index. Log Matching makes any node that has
// committed an index agree with it there, which the checkers verify every tick.
func committedHistory(c *testutil.Cluster) []raft.Entry {
	var best raft.NodeID
	var bestCommit raft.Index
	for _, id := range c.LiveIDs() {
		if ci := c.Status(id).CommitIndex; ci > bestCommit {
			best, bestCommit = id, ci
		}
	}
	if best == raft.None {
		return nil
	}
	return append([]raft.Entry(nil), c.Log(best)[:bestCommit]...)
}

// checkCommittedSurvived is the verdict: every node has re-applied exactly the
// committed history, in order, with identical content.
func checkCommittedSurvived(c *testutil.Cluster, want []raft.Entry) error {
	for _, id := range c.IDs {
		got := c.Applied(id)
		if len(got) < len(want) {
			return fmt.Errorf("node %d re-applied %d entries, but %d were committed before the crash",
				id, len(got), len(want))
		}
		for i, w := range want {
			g := got[i]
			if g.Index != w.Index || g.Term != w.Term || g.Type != w.Type || !bytes.Equal(g.Data, w.Data) {
				return fmt.Errorf("node %d applied %s %q at index %d, but %s %q was committed there",
					id, g, g.Data, w.Index, w, w.Data)
			}
		}
	}
	return nil
}

// restartAllAndConverge crashes every node at once, brings them all back from
// disk, and runs until every node has re-applied at least want entries or the
// bound runs out.
func restartAllAndConverge(t *testing.T, c *testutil.Cluster, want int) {
	t.Helper()
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		require.NoError(t, c.Restart(id))
	}
	c.RunUntil(4*electionBoundTicks, func() bool {
		for _, id := range c.IDs {
			if len(c.Applied(id)) < want {
				return false
			}
		}
		return true
	})
}

// buildHistory runs a faulty but live cluster with a steady stream of
// proposals, so that at the moment of the crash there are committed entries,
// uncommitted entries and proposals still in flight.
func buildHistory(t *testing.T, c *testutil.Cluster, seed uint64) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 3))
	electLeader(t, c)
	for i := range 250 {
		if id, ok := c.Leader(); ok && rng.IntN(100) < 40 {
			_, _, _ = c.Propose(id, []byte(fmt.Sprintf("s%d-%d", seed, i)))
		}
		if rng.IntN(100) < 2 {
			id := c.IDs[rng.IntN(len(c.IDs))]
			c.Crash(id)
			c.RunTicks(rng.IntN(30))
			require.NoError(t, c.Restart(id))
		}
		c.Tick()
	}
}

// TestClusterRestartRecoversCommitted is phase 3 acceptance criterion 6.
//
// Every node runs on a real write-ahead log. After a history with faults and
// individual crashes, EVERY node is crashed at once -- nothing survives in any
// process's memory -- and all are restarted from disk. Every entry committed
// before the crash must be committed and re-applied afterwards, identically on
// every node, and the cluster must go on to commit new entries.
func TestClusterRestartRecoversCommitted(t *testing.T) {
	seeds := 6
	if testing.Short() {
		seeds = 2
	}
	for _, n := range []int{3, 5} {
		for seed := range uint64(seeds) {
			t.Run(fmt.Sprintf("n=%d/seed=%d", n, seed), func(t *testing.T) {
				opts := testutil.DefaultOptions(n, seed)
				opts.Fault = inmem.Fault{DropRate: 0.05, MaxDelayTicks: 2, ReorderDueBatch: true}
				c := newDiskCluster(t, opts)
				buildHistory(t, c, seed)

				before := committedHistory(c)
				require.Greater(t, len(before), 20, "too little was committed for the test to mean anything")

				restartAllAndConverge(t, c, len(before))
				require.NoError(t, checkCommittedSurvived(c, before))
				require.NoError(t, c.Err())
				t.Logf("%d committed entries survived a full-cluster crash", len(before))

				// And it is a working cluster again, not merely a readable one.
				lead := electLeader(t, c)
				idx, _, err := c.Propose(lead, []byte("after-restart"))
				require.NoError(t, err)
				ok, _ := c.RunUntil(electionBoundTicks, func() bool {
					return c.MinCommitIndex(c.IDs) >= idx
				})
				require.True(t, ok, "the restarted cluster did not commit a new entry:\n%s", c)
				require.NoError(t, c.Err())
			})
		}
	}
}

// losesLastCommitted wraps the write-ahead log so that recovery forgets the
// newest committed entry and everything after it -- the shape of a storage
// layer that acknowledged a write before it was really durable.
type losesLastCommitted struct{ *storage.WAL }

func (l losesLastCommitted) InitialState() (raft.HardState, []raft.Entry, raft.Index, error) {
	hs, ents, applied, err := l.WAL.InitialState()
	if err != nil || hs.Commit == 0 {
		return hs, ents, applied, err
	}
	hs.Commit--
	return hs, ents[:hs.Commit], applied, nil
}

// TestRestartCheckCatchesLostCommittedEntries is the negative control for
// TestClusterRestartRecoversCommitted: the same scenario on storage that loses
// a committed entry in recovery must be caught. Without it, a check that
// compared the wrong thing -- say, each node's applied log against itself --
// would pass forever.
func TestRestartCheckCatchesLostCommittedEntries(t *testing.T) {
	opts := testutil.DefaultOptions(3, 1)
	root := t.TempDir()
	walFactory := testutil.WALStorageFactory(root, storage.WALOptions{})
	started := map[raft.NodeID]bool{}
	opts.Storage = func(id raft.NodeID) (storage.Storage, error) {
		s, err := walFactory(id)
		if err != nil || !started[id] {
			started[id] = true
			return s, err
		}
		return losesLastCommitted{s.(*storage.WAL)}, nil
	}
	c := newDiskCluster(t, opts)
	buildHistory(t, c, 1)
	before := committedHistory(c)
	require.NotEmpty(t, before)

	restartAllAndConverge(t, c, len(before))
	err := checkCommittedSurvived(c, before)
	require.Error(t, err, "every node lost its newest committed entry and the check did not notice")
	t.Logf("caught, as it should be: %v", err)
}

// TestRandomizedTrialsUpholdSafetyOnDisk runs phase 2's randomized safety
// trials with every node on a real write-ahead log, so each simulated crash is
// followed by a real recovery from disk. The trial is the same; only the
// storage underneath changed, which is the point: the invariants must not care.
func TestRandomizedTrialsUpholdSafetyOnDisk(t *testing.T) {
	trials := 10
	if testing.Short() {
		trials = 2
	}
	for _, n := range []int{3, 5} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			var committed raft.Index
			for i := range trials {
				seed := uint64(i)*2654435761 + uint64(n) + 1_000_000
				st := runRandomizedTrial(t, n, seed, func(o *testutil.Options) {
					o.Storage = testutil.WALStorageFactory(t.TempDir(), storage.WALOptions{SegmentBytes: 16 << 10})
				})
				committed += st.committed
			}
			t.Logf("n=%d: %d trials on disk, %d committed entries", n, trials, committed)
			require.NotZero(t, committed)
		})
	}
}
