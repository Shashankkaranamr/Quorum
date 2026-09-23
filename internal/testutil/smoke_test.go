package testutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/internal/testutil"
)

func newCluster(t *testing.T, opts testutil.Options) *testutil.Cluster {
	t.Helper()
	c, err := testutil.NewCluster(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("final state:\n%s", c)
		}
	})
	return c
}

// TestClusterElectsALeader is the smoke test: three nodes on a clean network
// converge on exactly one leader and hold it.
func TestClusterElectsALeader(t *testing.T) {
	c := newCluster(t, testutil.DefaultOptions(3, 1))

	elected, ticks := c.RunUntil(200, func() bool {
		_, ok := c.Leader()
		return ok
	})
	require.True(t, elected, "no leader within 200 ticks:\n%s", c)
	t.Logf("leader elected after %d ticks", ticks)

	leader, _ := c.Leader()

	// Leadership must be stable: a healthy cluster should not keep re-electing.
	termAtElection := c.Status(leader).Term
	c.RunTicks(200)
	require.NoError(t, c.Err())

	still, ok := c.Leader()
	require.True(t, ok, "lost the leader on a healthy network")
	require.Equal(t, leader, still, "leadership changed on a healthy network")
	require.Equal(t, termAtElection, c.Status(leader).Term,
		"term advanced on a healthy network, so heartbeats are not holding followers off")

	// Every follower must agree who leads.
	for _, id := range c.IDs {
		s := c.Status(id)
		require.Equal(t, leader, s.Leader, "node %d disagrees about the leader", id)
		require.Equal(t, termAtElection, s.Term, "node %d is on a different term", id)
	}
}
