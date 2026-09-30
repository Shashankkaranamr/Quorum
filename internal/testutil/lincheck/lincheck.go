// Package lincheck records client histories against a Quorum cluster and
// checks them for linearizability with Porcupine.
//
// It is shared by the in-process tests (internal/node) and the real-process
// chaos suite (test/integration), so that both are judged by one model and
// one workload. Its negative controls live with the in-process tests: a
// hand-built stale history the model must reject, and the whole workload run
// against a ReadIndex with its quorum check removed, which must be caught.
package lincheck

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/Shashankkaranamr/Quorum/internal/client"
)

// Input is one operation as Porcupine sees it.
type Input struct {
	Op    string // "get", "put", "delete"
	Key   string
	Value string
}

// Output is a read's result. Writes have none.
type Output struct {
	Found bool
	Value string
}

// Model is the sequential specification a linearizable history must match: a
// map, checked one key at a time. The state is the key's value, with ""
// meaning absent; every value the workload writes is non-empty and unique.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(Input).Key
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
		st, in := state.(string), input.(Input)
		switch in.Op {
		case "get":
			out := output.(Output)
			return out.Found == (st != "") && out.Value == st, st
		case "put":
			return true, in.Value
		default:
			return true, ""
		}
	},
	DescribeOperation: func(input, output any) string {
		in := input.(Input)
		if in.Op == "get" {
			out := output.(Output)
			return fmt.Sprintf("get(%s) -> %q found=%v", in.Key, out.Value, out.Found)
		}
		return fmt.Sprintf("%s(%s, %q)", in.Op, in.Key, in.Value)
	},
}

// History records operations from many goroutines.
type History struct {
	mu      sync.Mutex
	start   time.Time
	ops     []porcupine.Operation
	unknown int
}

// NewHistory starts a history now.
func NewHistory() *History { return &History{start: time.Now()} }

func (h *History) now() int64 { return time.Since(h.start).Nanoseconds() }

func (h *History) add(op porcupine.Operation, unknown bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, op)
	if unknown {
		h.unknown++
	}
}

// Ops returns the recorded operations and how many were writes of unknown
// outcome.
func (h *History) Ops() ([]porcupine.Operation, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]porcupine.Operation(nil), h.ops...), h.unknown
}

// Check runs Porcupine over the history.
func (h *History) Check(timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	ops, _ := h.Ops()
	return porcupine.CheckOperationsVerbose(Model, ops, timeout)
}

// Keys are the keys the workload touches. Few, so that operations collide.
var Keys = []string{"a", "b", "c"}

// RunClient issues random operations through cl until stop closes, recording
// each in h. A write whose outcome is unknown is recorded as never having
// returned: it may have taken effect at any point after it was invoked, which
// is exactly what an unanswered write means. A failed read is left out,
// because a read that returned nothing constrains nothing.
//
// Client 0 only reads. That matters: a client that writes is dragged off a
// cut-off leader within one request timeout, and every writer is dragged off
// at about the same moment, so none is left to read from the old leader after
// the majority has written. A reader keeps talking to whoever it believes
// leads for as long as that node answers, as a real read-mostly client would.
// Without it, this workload cannot produce a stale read from a partitioned
// leader at all (BUGS.md, 2026-09-30).
func RunClient(cl *client.Client, h *History, id int, seed uint64, stop <-chan struct{}) {
	rng := rand.New(rand.NewPCG(seed, uint64(id)))
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		default:
		}
		in := Input{Key: Keys[rng.IntN(len(Keys))]}
		switch r := rng.IntN(10); {
		case id == 0 || r < 5:
			in.Op = "get"
		case r < 9:
			in.Op, in.Value = "put", fmt.Sprintf("c%d-%d", id, n)
		default:
			in.Op = "delete"
		}

		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		call := h.now()
		var out Output
		var err error
		switch in.Op {
		case "get":
			var g client.GetResult
			g, err = cl.Get(ctx, in.Key)
			out = Output{Found: g.Found, Value: string(g.Value)}
		case "put":
			_, err = cl.Put(ctx, in.Key, []byte(in.Value))
		default:
			_, err = cl.Delete(ctx, in.Key)
		}
		ret := h.now()
		cancel()

		switch {
		case err == nil:
			h.add(porcupine.Operation{ClientId: id, Input: in, Call: call, Output: out, Return: ret}, false)
		case in.Op != "get":
			h.add(porcupine.Operation{ClientId: id, Input: in, Call: call, Output: out, Return: math.MaxInt64}, true)
		}
	}
}
