package node_test

import (
	"math"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"

	"github.com/Shashankkaranamr/Quorum/internal/client"
	"github.com/Shashankkaranamr/Quorum/internal/node"
	"github.com/Shashankkaranamr/Quorum/internal/testutil/lincheck"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// workload runs concurrent clients against a fresh cluster while injecting
// faults, and returns Porcupine's verdict on the recorded history. killLeaders
// chooses whether faults include killing the leader or only partitioning it;
// isolation is how long a partitioned leader stays cut off.
func workload(t *testing.T, duration time.Duration, mutation raft.Mutation, killLeaders bool,
	isolation func(*rand.Rand) time.Duration) porcupine.CheckResult {
	c := newCluster(t, 3, func(o *node.Options) { o.UnsafeMutation = mutation })
	c.leader()

	h := lincheck.NewHistory()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 5 {
		cl := c.client(ctxFor(t, 10*time.Second), client.Config{AttemptTimeout: time.Second})
		wg.Add(1)
		go func() {
			defer wg.Done()
			lincheck.RunClient(cl, h, i, 42, stop)
		}()
	}

	// Faults, from this goroutine only: it is the only one touching the
	// cluster's node table while the clients run.
	rng := rand.New(rand.NewPCG(7, 7))
	kills, partitions := 0, 0
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(300+rng.IntN(400)) * time.Millisecond)
		lead := c.leaderNow()
		if lead == raft.None {
			continue
		}
		if killLeaders && rng.IntN(2) == 0 {
			c.kill(lead)
			kills++
			time.Sleep(time.Duration(200+rng.IntN(400)) * time.Millisecond)
			c.restart(lead)
		} else {
			c.isolate(lead)
			partitions++
			time.Sleep(isolation(rng))
			c.heal()
		}
	}
	close(stop)
	wg.Wait()

	ops, unknown := h.Ops()
	t.Logf("%d operations (%d writes of unknown outcome), %d leader kills, %d leader partitions",
		len(ops), unknown, kills, partitions)
	if killLeaders {
		require.Positive(t, kills, "no leader was killed")
	}
	require.Positive(t, partitions, "no leader was partitioned")
	require.Greater(t, len(ops), 100, "too few operations completed to say anything")

	res, info := h.Check(60 * time.Second)
	if res != porcupine.Ok {
		path := filepath.Join(t.TempDir(), "linearizability.html")
		if err := porcupine.VisualizePath(lincheck.Model, info, path); err == nil {
			t.Logf("visualization written to %s", path)
		}
	}
	return res
}

// TestLinearizabilityUnderFaults is phase 5 acceptance criterion 6. Concurrent
// clients read and write a few keys while the leader is repeatedly killed and
// restarted, or cut off from the others and healed. The whole recorded
// history must be linearizable, as checked by Porcupine against a sequential
// map.
func TestLinearizabilityUnderFaults(t *testing.T) {
	duration := 8 * time.Second
	if testing.Short() {
		duration = 3 * time.Second
	}
	isolation := func(rng *rand.Rand) time.Duration { return time.Duration(200+rng.IntN(1300)) * time.Millisecond }
	require.Equal(t, porcupine.Ok, workload(t, duration, raft.MutationNone, true, isolation),
		"the history is not linearizable")
}

// TestLinearizabilityCatchesQuorumlessReads is the end-to-end negative control
// for criterion 6: the same workload, recorder and checker, against a cluster
// whose ReadIndex skips the quorum confirmation, with the leader repeatedly
// partitioned. Clients still talking to the cut-off leader read values the
// majority has already overwritten, and Porcupine must call the history
// illegal. The hand-built control below proves the checker; this one proves
// the whole pipeline can see a real violation.
//
// Its first version was NOT caught, which is how the workload's blind spot
// was found; see lincheck.RunClient and BUGS.md, 2026-09-30.
func TestLinearizabilityCatchesQuorumlessReads(t *testing.T) {
	isolation := func(rng *rand.Rand) time.Duration { return time.Duration(200+rng.IntN(1300)) * time.Millisecond }
	res := workload(t, 8*time.Second, raft.MutationReadWithoutQuorum, false, isolation)
	require.Equal(t, porcupine.Illegal, res,
		"stale reads from a partitioned leader went undetected")
}

// TestLinearizabilityCheckerRejectsAStaleRead is the negative control for the
// model: fed a history with a read that returns a value already overwritten
// before the read began, it must reject it -- and must accept the same
// history with the read corrected.
func TestLinearizabilityCheckerRejectsAStaleRead(t *testing.T) {
	put := func(v string, call, ret int64) porcupine.Operation {
		return porcupine.Operation{ClientId: 0, Input: lincheck.Input{Op: "put", Key: "a", Value: v}, Call: call, Return: ret}
	}
	get := func(v string, call, ret int64) porcupine.Operation {
		return porcupine.Operation{ClientId: 1, Input: lincheck.Input{Op: "get", Key: "a"},
			Output: lincheck.Output{Found: v != "", Value: v}, Call: call, Return: ret}
	}
	check := func(ops ...porcupine.Operation) porcupine.CheckResult {
		return porcupine.CheckOperationsTimeout(lincheck.Model, ops, 5*time.Second)
	}
	require.Equal(t, porcupine.Illegal, check(put("1", 0, 10), put("2", 20, 30), get("1", 40, 50)))
	require.Equal(t, porcupine.Ok, check(put("1", 0, 10), put("2", 20, 30), get("2", 40, 50)))

	// An unanswered write may take effect at any time after it began, so a
	// later read may see it or not.
	require.Equal(t, porcupine.Ok, check(put("1", 0, 10), put("2", 20, math.MaxInt64), get("1", 40, 50), get("2", 60, 70)))
}
