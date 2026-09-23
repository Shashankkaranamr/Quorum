package testutil_test

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/internal/testutil"
	"github.com/Shashankkaranamr/Quorum/internal/transport/inmem"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// TestFailureMessagesAreLazy guards a bug this phase actually hit.
//
// Go evaluates function-call arguments eagerly, so passing c.Describe() as a
// testify failure message renders the entire cluster on every PASSING
// assertion. Inside the per-index loop of a randomized trial that was 48% of
// the suite's CPU time: the trials ran in 33s instead of 7s.
//
// The fix is structural -- Cluster implements fmt.Stringer, and tests pass c
// rather than c.Describe() -- and this is what stops it coming back.
func TestFailureMessagesAreLazy(t *testing.T) {
	c := newCluster(t, testutil.DefaultOptions(3, 99))
	electLeader(t, c)
	c.RunTicks(50)
	require.NoError(t, c.Err())

	require.Zerof(t, c.DescribeCalls(),
		"the cluster was rendered %d times during a passing run. A failure message "+
			"argument is being evaluated eagerly; pass the Cluster itself, not Describe().",
		c.DescribeCalls())
}

// findViolatingSeed searches for a seed where running the cluster with mut
// produces a violation of want. It returns the seed and the violations.
//
// Searching rather than hard-coding a seed keeps the negative control honest:
// if a future change makes a mutation stop producing its violation, the search
// exhausts and the test fails, rather than a stale seed quietly passing for the
// wrong reason.
func findViolatingSeed(t *testing.T, n int, mut raft.Mutation, want testutil.Invariant, seeds int) (uint64, []testutil.Violation) {
	t.Helper()

	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		rng := rand.New(rand.NewPCG(seed, 0x1234))
		opts := testutil.DefaultOptions(n, seed)
		opts.UnsafeMutation = mut
		opts.Fault = inmem.Fault{
			DropRate:        rng.Float64() * 0.2,
			DuplicateRate:   rng.Float64() * 0.1,
			MaxDelayTicks:   1 + rng.IntN(6),
			ReorderDueBatch: true,
		}

		c, err := testutil.NewCluster(opts)
		require.NoError(t, err)

		down := map[raft.NodeID]bool{}
		partitioned := false
		for range 400 {
			if rng.IntN(1000) < 25 {
				if partitioned {
					c.Heal()
					partitioned = false
				} else if a, b := splitRandomly(rng, c.IDs); len(a) > 0 && len(b) > 0 {
					c.Partition(a, b)
					partitioned = true
				}
			}
			if rng.IntN(1000) < 12 {
				id := c.IDs[rng.IntN(len(c.IDs))]
				if down[id] {
					require.NoError(t, c.Restart(id))
					delete(down, id)
				} else if n-len(down) > n/2+1 {
					c.Crash(id)
					down[id] = true
				}
			}
			if id, ok := c.Leader(); ok && rng.IntN(100) < 30 {
				_, _, _ = c.Propose(id, []byte("x"))
			}
			c.Tick()

			if c.Checker.Violated(want) {
				return seed, c.Checker.Violations()
			}
		}

		c.Heal()
		for id := range down {
			require.NoError(t, c.Restart(id))
		}
		c.RunTicks(2 * electionBoundTicks)
		if c.Checker.Violated(want) {
			return seed, c.Checker.Violations()
		}
	}
	return 0, nil
}

// TestMutatedRaftTripsInvariant is PLAN.md phase 2 acceptance criterion 4.
//
// The invariant checkers are the primary deliverable of this phase, and a
// checker that has never been observed to fail is not evidence of anything.
// Each subtest deliberately breaks ONE Raft rule and requires the corresponding
// checker to catch it.
//
// Each mutation is surgical: it removes one rule and nothing else, so a
// violation attributed to a specific invariant really does mean that invariant
// was the one broken.
func TestMutatedRaftTripsInvariant(t *testing.T) {
	seeds := 60
	if testing.Short() {
		seeds = 15
	}

	for _, tc := range []struct {
		name string
		n    int
		mut  raft.Mutation
		want testutil.Invariant
		why  string
	}{
		{
			name: "voting twice in a term breaks Election Safety",
			n:    5,
			mut:  raft.MutationVoteTwicePerTerm,
			want: testutil.ElectionSafety,
			why: "one vote per term is what stops two candidates assembling a " +
				"majority from the same set of single votes",
		},
		{
			name: "skipping the up-to-date check breaks Leader Completeness",
			n:    5,
			mut:  raft.MutationSkipUpToDateCheck,
			want: testutil.LeaderCompleteness,
			why: "a candidate missing a committed entry must not be able to win; " +
				"the §5.4.1 log comparison is the only thing preventing it",
		},
		{
			name: "a leader truncating its own log breaks Leader Append-Only",
			n:    5,
			mut:  raft.MutationLeaderTruncatesOwnLog,
			want: testutil.LeaderAppendOnly,
			why:  "a leader appends and never rewrites; a stale message must be ignored",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seed, violations := findViolatingSeed(t, tc.n, tc.mut, tc.want, seeds)
			require.NotZerof(t, seed,
				"mutation %q ran %d seeds without the %s checker ever firing.\n"+
					"Either the checker is broken, or the mutation no longer breaks the rule.\n"+
					"Why this should fail: %s",
				tc.mut, seeds, tc.want, tc.why)

			t.Logf("mutation %q tripped %s at seed %d: %s",
				tc.mut, tc.want, seed, violations[0])
		})
	}

	// MutationCommitAnyTerm is covered by raft.TestFigure8CommitRule instead of
	// here, and the reason is worth writing down.
	//
	// Because this implementation appends a no-op on election, index N and the
	// new leader's no-op at N+1 almost always replicate in the same
	// AppendEntries, so the old-term entry commits legitimately a moment later
	// and the dangerous window closes before a random trial can catch it. That
	// is the no-op doing its job. The window is real but narrow, so the Figure
	// 8 test constructs it exactly -- two followers acknowledging index N while
	// their acknowledgement of N+1 is still in flight -- and asserts both that
	// the correct rule refuses to commit and that removing it loses a committed
	// entry.
	t.Run("commit-any-term is covered by TestFigure8CommitRule", func(t *testing.T) {
		t.Skip("see raft/figure8_test.go: the window is constructed there rather than sampled")
	})
}

// TestZeroConfigIsUnmutated is the fence around the mutation machinery: a
// Config built without thinking about it must be correct Raft.
func TestZeroConfigIsUnmutated(t *testing.T) {
	var cfg raft.Config
	require.Equal(t, raft.MutationNone, cfg.UnsafeMutation,
		"the zero value of Config must be a correct Raft, or the mutation knob is a hazard")

	var opts testutil.Options
	require.Equal(t, raft.MutationNone, opts.UnsafeMutation)
	require.Equal(t, raft.MutationNone, testutil.DefaultOptions(3, 1).UnsafeMutation)
	require.Equal(t, "none", raft.MutationNone.String())
}

// TestEveryMutationIsDistinct guards against two mutation constants collapsing
// to the same value, which would make a negative control silently test the
// wrong rule.
func TestEveryMutationIsDistinct(t *testing.T) {
	all := []raft.Mutation{
		raft.MutationNone,
		raft.MutationCommitAnyTerm,
		raft.MutationVoteTwicePerTerm,
		raft.MutationSkipUpToDateCheck,
		raft.MutationLeaderTruncatesOwnLog,
	}
	seen := map[raft.Mutation]bool{}
	names := map[string]bool{}
	for _, m := range all {
		require.Falsef(t, seen[m], "duplicate mutation value %d", uint8(m))
		seen[m] = true
		name := m.String()
		require.NotContainsf(t, name, "mutation(", "mutation %d has no name", uint8(m))
		require.Falsef(t, names[name], "duplicate mutation name %q", name)
		names[name] = true
	}
	require.Len(t, seen, 5)
}
