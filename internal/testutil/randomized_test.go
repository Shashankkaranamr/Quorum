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

// trialTicks is how long a randomized trial runs before healing. Long enough
// for several elections and a few hundred proposals; short enough that a few
// hundred trials finish in seconds under the race detector.
const trialTicks = 500

// TestRandomizedTrialsUpholdSafety is the core deliverable of phase 2.
//
// The mechanics -- elections, replication, backoff -- only matter because they
// make the five safety properties true. This runs many seeded trials with
// randomized message loss, delay, duplication, reordering, partitions and node
// crashes, and checks all five after every single tick, not merely at the end.
//
// Every trial reproduces from its seed alone. A failure prints the seed.
func TestRandomizedTrialsUpholdSafety(t *testing.T) {
	trials := 200
	if testing.Short() {
		trials = 20
	}

	for _, n := range []int{3, 5} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			var (
				totalCommitted   raft.Index
				totalDropped     uint64
				totalPartition   uint64
				trialsWithLeader int
			)
			for i := range trials {
				seed := uint64(i)*2654435761 + uint64(n)
				st := runRandomizedTrial(t, n, seed)
				totalCommitted += st.committed
				totalDropped += st.dropped
				totalPartition += st.partitioned
				if st.hadLeader {
					trialsWithLeader++
				}
			}

			// A suite that injected no faults, or never committed anything,
			// would pass every safety check and prove nothing at all.
			t.Logf("n=%d: %d trials, %d committed entries, %d messages dropped, "+
				"%d blocked by partitions, %d trials reached a leader",
				n, trials, totalCommitted, totalDropped, totalPartition, trialsWithLeader)
			require.NotZero(t, totalCommitted, "no trial committed anything")
			require.NotZero(t, totalDropped, "no trial dropped a message")
			require.NotZero(t, totalPartition, "no trial partitioned the network")
			require.Equal(t, trials, trialsWithLeader,
				"some trials never elected a leader at all, so they exercised nothing")
		})
	}
}

type trialStats struct {
	committed   raft.Index
	dropped     uint64
	partitioned uint64
	hadLeader   bool
}

func runRandomizedTrial(t *testing.T, n int, seed uint64) trialStats {
	t.Helper()

	rng := rand.New(rand.NewPCG(seed, 0x5deece66d))

	opts := testutil.DefaultOptions(n, seed)
	opts.Fault = inmem.Fault{
		DropRate:        rng.Float64() * 0.15,
		DuplicateRate:   rng.Float64() * 0.10,
		MaxDelayTicks:   rng.IntN(5),
		ReorderDueBatch: true,
	}

	c, err := testutil.NewCluster(opts)
	require.NoError(t, err)

	quorum := n/2 + 1
	down := map[raft.NodeID]bool{}
	partitioned := false
	stats := trialStats{}
	payload := 0

	for range trialTicks {
		// Partition or heal, occasionally.
		if rng.IntN(1000) < 15 {
			if partitioned {
				c.Heal()
				partitioned = false
			} else {
				a, b := splitRandomly(rng, c.IDs)
				if len(a) > 0 && len(b) > 0 {
					c.Partition(a, b)
					partitioned = true
				}
			}
		}

		// Crash or restart, keeping at least a quorum alive so the trial does
		// not spend all its time in a cluster that cannot possibly progress.
		if rng.IntN(1000) < 10 {
			id := c.IDs[rng.IntN(len(c.IDs))]
			switch {
			case down[id]:
				require.NoError(t, c.Restart(id))
				delete(down, id)
			case n-len(down) > quorum:
				c.Crash(id)
				down[id] = true
			}
		}

		// Propose to whoever currently believes it leads. A stale leader
		// accepting a proposal it can never commit is a case worth exercising.
		if id, ok := c.Leader(); ok {
			stats.hadLeader = true
			if rng.IntN(100) < 25 {
				payload++
				_, _, _ = c.Propose(id, []byte(fmt.Sprintf("s%d-%d", seed, payload)))
			}
		}

		c.Tick()
	}

	// Heal everything and give the cluster room to reconverge, so the trial
	// also covers recovery rather than only the damage.
	c.Heal()
	for id := range down {
		require.NoError(t, c.Restart(id))
	}
	c.RunTicks(4 * electionBoundTicks)

	require.NoErrorf(t, c.Err(),
		"seed %d (n=%d): reproduce with testutil.DefaultOptions(%d, %d)\n%s",
		seed, n, n, seed, c)

	stats.dropped = c.Net.Stats.Dropped
	stats.partitioned = c.Net.Stats.Partitioned
	stats.committed = c.Checker.MaxCommitted

	// After healing, every live node must agree on the committed prefix.
	if lead, ok := c.Leader(); ok {
		target := c.Status(lead).CommitIndex
		want := c.Log(lead)
		for _, id := range c.LiveIDs() {
			got := c.Log(id)
			for i := range want {
				if want[i].Index > target || i >= len(got) {
					break
				}
				require.Equalf(t, want[i].Term, got[i].Term,
					"seed %d: node %d disagrees with leader %d at index %d after healing\n%s",
					seed, id, lead, want[i].Index, c)
			}
		}
	}
	return stats
}

func splitRandomly(rng *rand.Rand, ids []raft.NodeID) (a, b []raft.NodeID) {
	for _, id := range ids {
		if rng.IntN(2) == 0 {
			a = append(a, id)
		} else {
			b = append(b, id)
		}
	}
	return a, b
}

// livenessBoundTicks is the bound for a lossy but unpartitioned network.
//
// With messages dropped and delayed, a candidate can lose a whole round to a
// vote request that never arrives and must wait out another randomized timeout.
// Each attempt costs 10-20 ticks, so 150 ticks allows roughly seven consecutive
// failures.
//
// The number is set from measurement, not taste. Across 200 seeds at each
// cluster size, with 10%% drops and delays up to 3 ticks, the worst observed
// election took 51 ticks -- so this leaves about 3x headroom. A looser bound
// (the 20x that was here first) would pass even if elections got five times
// slower, which defeats the point of having a bound at all. The test logs the
// observed worst case on every run, so the margin stays visible.
const livenessBoundTicks = 150

// TestLivenessBoundUnderTransientFaults checks the liveness property the phase
// brief asks for: absent a permanent partition, a leader is elected within a
// bounded number of ticks.
//
// Liveness, unlike safety, is conditional. Under arbitrary message loss Raft
// guarantees nothing (this is FLP, not a shortcut), so the network here drops
// and delays but never partitions, and never permanently silences a node.
func TestLivenessBoundUnderTransientFaults(t *testing.T) {
	seeds := 200
	if testing.Short() {
		seeds = 25
	}

	for _, n := range []int{3, 5} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			worst, worstSeed := 0, uint64(0)
			for seed := uint64(1); seed <= uint64(seeds); seed++ {
				opts := testutil.DefaultOptions(n, seed)
				opts.Fault = inmem.Fault{
					DropRate:        0.10,
					DuplicateRate:   0.05,
					MaxDelayTicks:   3,
					ReorderDueBatch: true,
				}
				c, err := testutil.NewCluster(opts)
				require.NoError(t, err)

				ok, ticks := c.RunUntil(livenessBoundTicks, func() bool {
					_, ok := c.Leader()
					return ok
				})
				require.Truef(t, ok,
					"seed %d (n=%d): no leader within %d ticks on a lossy but connected network\n%s",
					seed, n, livenessBoundTicks, c)
				require.NoErrorf(t, c.Err(), "seed %d", seed)

				if ticks > worst {
					worst, worstSeed = ticks, seed
				}
			}
			t.Logf("n=%d: 10%% drops, delays up to 3 ticks: worst election %d ticks (seed %d), bound %d",
				n, worst, worstSeed, livenessBoundTicks)
			require.Lessf(t, worst, livenessBoundTicks,
				"observed worst case %d is not comfortably below the bound", worst)
		})
	}
}

// TestTickLagIsMeasured exercises the instrumentation the phase brief asks for
// and that phase 6 will need.
//
// One goroutine owns ticks, messages and durability, so a slow fsync delays
// ticks. That is the head-of-line-blocking hazard DESIGN.md names. Here the
// simulator makes a Sync cost logical ticks, and the metric must notice.
func TestTickLagIsMeasured(t *testing.T) {
	fast := newCluster(t, testutil.DefaultOptions(3, 5))
	fast.RunTicks(100)
	for _, id := range fast.IDs {
		m := fast.Replicas[id].Driver.Metrics()
		require.Zerof(t, m.MaxTickLagTicks,
			"node %d reported tick lag on instant storage", id)
		require.Zerof(t, m.SendBeforeSyncViolations,
			"node %d sent a message before its state was durable", id)
	}

	slowOpts := testutil.DefaultOptions(3, 5)
	slowOpts.SyncCostTicks = 4
	slow := newCluster(t, slowOpts)
	slow.RunTicks(200)

	var worst uint64
	for _, id := range slow.IDs {
		m := slow.Replicas[id].Driver.Metrics()
		require.NotZerof(t, m.Syncs, "node %d never synced", id)
		require.Zerof(t, m.SendBeforeSyncViolations,
			"node %d sent a message before its state was durable", id)
		worst = max(worst, m.MaxTickLagTicks)
		t.Logf("node %d: syncs=%d maxTickLag=%d totalTickLag=%d ticksProcessed=%d",
			id, m.Syncs, m.MaxTickLagTicks, m.TotalTickLagTicks, m.TicksProcessed)
	}
	require.NotZero(t, worst,
		"storage that costs 4 ticks per fsync produced no measurable tick lag, "+
			"so the metric phase 6 depends on is not actually wired up")
	require.GreaterOrEqual(t, worst, uint64(2),
		"tick lag should approach the fsync cost")
}
