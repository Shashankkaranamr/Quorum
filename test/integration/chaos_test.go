package integration

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	"github.com/Shashankkaranamr/Quorum/internal/client"
	"github.com/Shashankkaranamr/Quorum/internal/testutil/lincheck"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// chaosSeeds are the seeds TestChaosSeeded runs. QUORUM_CHAOS_SEED runs one
// seed instead -- the way to replay a failure -- and QUORUM_CHAOS_ITERATIONS
// runs that many consecutive seeds starting from 1, for a longer soak.
func chaosSeeds(t *testing.T) []uint64 {
	if s := os.Getenv("QUORUM_CHAOS_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		require.NoError(t, err, "QUORUM_CHAOS_SEED")
		return []uint64{seed}
	}
	n := 3
	if s := os.Getenv("QUORUM_CHAOS_ITERATIONS"); s != "" {
		var err error
		n, err = strconv.Atoi(s)
		require.NoError(t, err, "QUORUM_CHAOS_ITERATIONS")
	}
	var seeds []uint64
	for i := range n {
		seeds = append(seeds, uint64(i+1))
	}
	return seeds
}

// TestChaosSeeded is phase 6 acceptance criterion 5. Each iteration runs a
// randomized fault schedule against a real three-process cluster while five
// clients (one read-only) read and write, then checks the whole recorded
// history for linearizability with Porcupine -- the same model and workload
// whose negative controls live in internal/node.
//
// The faults: kill a node and restart it, isolate a node in both directions,
// cut one directed link, freeze a node and let it thaw, and isolate whoever
// currently leads. One fault is in force at a time, so a majority is always
// up and connected; the claim under test is safety, and progress with a live
// majority.
//
// What "reproducible from its seed" means, precisely: the seed fixes the
// schedule -- which fault, on which node, for how long, in what order -- and
// every client's sequence of operations. It cannot fix how real processes
// interleave on a real clock, so a failing seed replays the same attack, not
// the same microsecond-level history. The schedule is logged so a failure can
// be read without rerunning it.
func TestChaosSeeded(t *testing.T) {
	skipShort(t)
	for _, seed := range chaosSeeds(t) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { chaos(t, seed) })
	}
}

func chaos(t *testing.T, seed uint64) {
	c := startCluster(t, 3)
	c.waitLeader(electionBound)
	ids := c.sup.IDs()

	h := lincheck.NewHistory()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 5 {
		cl := c.client(client.Config{AttemptTimeout: time.Second})
		wg.Add(1)
		go func() {
			defer wg.Done()
			lincheck.RunClient(cl, h, i, seed, stop)
		}()
	}

	rng := rand.New(rand.NewPCG(seed, 0xc4a05))
	pick := func() raft.NodeID { return ids[rng.IntN(len(ids))] }
	hold := func(lo, hi int) time.Duration { return time.Duration(lo+rng.IntN(hi-lo)) * time.Millisecond }
	var schedule []string
	counts := map[string]int{}
	note := func(kind, what string) {
		counts[kind]++
		schedule = append(schedule, what)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed %d schedule:\n  %s", seed, strings.Join(schedule, "\n  "))
		}
	})

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(hold(150, 500))
		switch rng.IntN(5) {
		case 0:
			id, d := pick(), hold(200, 800)
			note("kill", fmt.Sprintf("kill %d, restart after %s", id, d))
			c.kill(id)
			time.Sleep(d)
			c.start(id)
		case 1:
			id, d := pick(), hold(300, 1500)
			note("isolate", fmt.Sprintf("isolate %d for %s", id, d))
			c.partition([]raft.NodeID{id}, c.others(id))
			time.Sleep(d)
			c.heal()
		case 2:
			from, d := pick(), hold(300, 1500)
			to := c.others(from)[rng.IntN(len(ids)-1)]
			note("oneway", fmt.Sprintf("cut %d -> %d for %s", from, to, d))
			c.block(from, []raft.NodeID{to}, false, true)
			c.block(to, []raft.NodeID{from}, true, false)
			time.Sleep(d)
			c.heal()
		case 3:
			id, d := pick(), hold(200, 1200)
			note("freeze", fmt.Sprintf("freeze %d, auto-thaw after %s", id, d))
			_, err := c.admin(id).Freeze(context.Background(), &adminv1.FreezeRequest{AutoThawAfterMs: uint32(d.Milliseconds())})
			require.NoError(t, err)
			time.Sleep(d + 50*time.Millisecond)
		default:
			// The leader is chosen at random only in the sense that the
			// schedule chose this fault; which node leads is up to the
			// cluster, so this one fault's target is not seed-determined.
			lead := c.leaderAmong()
			if lead == raft.None {
				note("leader-isolate", "isolate leader: none right now, skipped")
				continue
			}
			d := hold(300, 1500)
			note("leader-isolate", fmt.Sprintf("isolate leader %d for %s", lead, d))
			c.partition([]raft.NodeID{lead}, c.others(lead))
			time.Sleep(d)
			c.heal()
		}
	}
	close(stop)
	wg.Wait()

	ops, unknown := h.Ops()
	t.Logf("seed %d: %d operations (%d writes of unknown outcome); faults %v", seed, len(ops), unknown, counts)
	require.Greater(t, len(ops), 100, "too few operations completed to say anything")
	require.GreaterOrEqual(t, len(counts), 3, "the schedule exercised fewer than three kinds of fault")

	res, info := h.Check(2 * time.Minute)
	if res != porcupine.Ok {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("chaos-seed-%d.html", seed))
		if err := porcupine.VisualizePath(lincheck.Model, info, path); err == nil {
			t.Logf("visualization written to %s", path)
		}
	}
	require.Equal(t, porcupine.Ok, res, "seed %d produced a non-linearizable history", seed)
}
