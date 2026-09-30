package node_test

import (
	"context"
	"fmt"
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
	"github.com/Shashankkaranamr/Quorum/raft"
)

// kvInput and kvOutput are one operation as Porcupine sees it.
type kvInput struct {
	op    string // "get", "put", "delete"
	key   string
	value string
}

type kvOutput struct {
	found bool
	value string
}

// kvModel is the sequential specification a linearizable history must match:
// a map, checked one key at a time. The state is the key's value, with ""
// meaning absent; every value the workload writes is non-empty and unique.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(kvInput).key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		st, in := state.(string), input.(kvInput)
		switch in.op {
		case "get":
			out := output.(kvOutput)
			return out.found == (st != "") && out.value == st, st
		case "put":
			return true, in.value
		default:
			return true, ""
		}
	},
	DescribeOperation: func(input, output any) string {
		in := input.(kvInput)
		if in.op == "get" {
			out := output.(kvOutput)
			return fmt.Sprintf("get(%s) -> %q found=%v", in.key, out.value, out.found)
		}
		return fmt.Sprintf("%s(%s, %q)", in.op, in.key, in.value)
	},
}

// history records operations from many goroutines.
type history struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation

	unknown int
}

func (h *history) now() int64 { return time.Since(h.start).Nanoseconds() }

func (h *history) add(op porcupine.Operation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, op)
}

// runClient issues random operations until stop closes. A write whose outcome
// is unknown is recorded as never having returned: it may have taken effect at
// any point after it was invoked, which is exactly what an unanswered write
// means. A failed read is left out, because a read that returned nothing
// constrains nothing.
//
// Client 0 only reads. That matters: a client that writes is dragged off a
// cut-off leader within one request timeout, and every writer is dragged off
// at about the same moment, so none is left to read from the old leader after
// the majority has written. A reader keeps talking to whoever it believes
// leads for as long as that node answers, as a real read-mostly client would.
// Without it, this workload cannot produce a stale read from a partitioned
// leader at all (BUGS.md, 2026-09-30).
func runClient(t *testing.T, c *cluster, h *history, id int, seed uint64, stop <-chan struct{}) {
	ctx := context.Background()
	cl := c.client(ctxFor(t, 10*time.Second), client.Config{AttemptTimeout: time.Second})
	rng := rand.New(rand.NewPCG(seed, uint64(id)))
	keys := []string{"a", "b", "c"}
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		default:
		}
		in := kvInput{key: keys[rng.IntN(len(keys))]}
		switch r := rng.IntN(10); {
		case id == 0 || r < 5:
			in.op = "get"
		case r < 9:
			in.op, in.value = "put", fmt.Sprintf("c%d-%d", id, n)
		default:
			in.op = "delete"
		}

		opCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		call := h.now()
		var out kvOutput
		var err error
		switch in.op {
		case "get":
			var g client.GetResult
			g, err = cl.Get(opCtx, in.key)
			out = kvOutput{found: g.Found, value: string(g.Value)}
		case "put":
			_, err = cl.Put(opCtx, in.key, []byte(in.value))
		default:
			_, err = cl.Delete(opCtx, in.key)
		}
		ret := h.now()
		cancel()

		switch {
		case err == nil:
			h.add(porcupine.Operation{ClientId: id, Input: in, Call: call, Output: out, Return: ret})
		case in.op != "get":
			// Unknown outcome: never returned, as far as the model knows.
			h.add(porcupine.Operation{ClientId: id, Input: in, Call: call, Output: out, Return: math.MaxInt64})
			h.mu.Lock()
			h.unknown++
			h.mu.Unlock()
		}
	}
}

// workload runs concurrent clients against a fresh cluster while injecting
// faults, and returns Porcupine's verdict on the recorded history. killLeaders
// chooses whether faults include killing the leader or only partitioning it;
// isolation is how long a partitioned leader stays cut off.
func workload(t *testing.T, duration time.Duration, mutation raft.Mutation, killLeaders bool,
	isolation func(*rand.Rand) time.Duration) porcupine.CheckResult {
	c := newCluster(t, 3, func(o *node.Options) { o.UnsafeMutation = mutation })
	c.leader()

	h := &history{start: time.Now()}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runClient(t, c, h, i, 42, stop)
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

	h.mu.Lock()
	ops := append([]porcupine.Operation(nil), h.ops...)
	unknown := h.unknown
	h.mu.Unlock()
	t.Logf("%d operations (%d writes of unknown outcome), %d leader kills, %d leader partitions",
		len(ops), unknown, kills, partitions)
	if killLeaders {
		require.Positive(t, kills, "no leader was killed")
	}
	require.Positive(t, partitions, "no leader was partitioned")
	require.Greater(t, len(ops), 100, "too few operations completed to say anything")

	res, info := porcupine.CheckOperationsVerbose(kvModel, ops, 60*time.Second)
	if res != porcupine.Ok {
		path := filepath.Join(t.TempDir(), "linearizability.html")
		if err := porcupine.VisualizePath(kvModel, info, path); err == nil {
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
// was found; see runClient and BUGS.md, 2026-09-30.
func TestLinearizabilityCatchesQuorumlessReads(t *testing.T) {
	isolation := func(rng *rand.Rand) time.Duration { return time.Duration(200+rng.IntN(1300)) * time.Millisecond }
	res := workload(t, 8*time.Second, raft.MutationReadWithoutQuorum, false, isolation)
	require.Equal(t, porcupine.Illegal, res,
		"stale reads from a partitioned leader went undetected")
}

// TestLinearizabilityCheckerRejectsAStaleRead is the negative control for
// criterion 6: the model and checker, fed a history with a read that returns a
// value already overwritten before the read began, must reject it -- and must
// accept the same history with the read corrected.
func TestLinearizabilityCheckerRejectsAStaleRead(t *testing.T) {
	put := func(v string, call, ret int64) porcupine.Operation {
		return porcupine.Operation{ClientId: 0, Input: kvInput{op: "put", key: "a", value: v}, Call: call, Return: ret}
	}
	get := func(v string, call, ret int64) porcupine.Operation {
		return porcupine.Operation{ClientId: 1, Input: kvInput{op: "get", key: "a"},
			Output: kvOutput{found: v != "", value: v}, Call: call, Return: ret}
	}
	stale := []porcupine.Operation{put("1", 0, 10), put("2", 20, 30), get("1", 40, 50)}
	require.Equal(t, porcupine.Illegal, porcupine.CheckOperationsTimeout(kvModel, stale, 5*time.Second))

	fresh := []porcupine.Operation{put("1", 0, 10), put("2", 20, 30), get("2", 40, 50)}
	require.Equal(t, porcupine.Ok, porcupine.CheckOperationsTimeout(kvModel, fresh, 5*time.Second))

	// An unanswered write may take effect at any time after it began, so a
	// later read may see it or not.
	pending := []porcupine.Operation{put("1", 0, 10), put("2", 20, math.MaxInt64), get("1", 40, 50), get("2", 60, 70)}
	require.Equal(t, porcupine.Ok, porcupine.CheckOperationsTimeout(kvModel, pending, 5*time.Second))
}
