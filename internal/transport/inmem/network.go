package inmem

import (
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/Shashankkaranamr/Quorum/internal/transport"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// Fault describes how unreliable the network should be.
//
// Every probability is applied per message, drawn from the network's seeded
// source, so a trial that fails replays exactly from its seed.
type Fault struct {
	// DropRate is the probability a message is discarded on send.
	DropRate float64

	// DuplicateRate is the probability a message is delivered twice, at
	// independently drawn delays. Duplicates are what catch code that
	// truncates a log on a stale AppendEntries.
	DuplicateRate float64

	// MaxDelayTicks bounds the extra latency applied to a message. A delay of
	// zero means delivery on the next Deliver. Varying delays are what produce
	// reordering, which is the other half of the same class of bug.
	MaxDelayTicks int

	// ReorderDueBatch shuffles messages that come due in the same tick, so
	// that even zero-delay traffic does not arrive in send order.
	ReorderDueBatch bool
}

type link struct{ from, to raft.NodeID }

type pending struct {
	msg       raft.Message
	deliverAt uint64
	seq       uint64 // tie-breaker, keeps ordering deterministic
}

// Network is a deterministic in-memory message bus.
//
// It holds no goroutines, no locks and no wall clock. Messages are queued
// against logical tick deadlines, so a scenario that would take sixty seconds
// of real elections runs in microseconds, and every scheduling decision comes
// from a seeded source.
type Network struct {
	rng   *rand.Rand
	now   uint64
	seq   uint64
	ids   []raft.NodeID
	fault Fault

	// blocked holds directed links that are cut. Directed rather than
	// symmetric on purpose: a one-way partition, where a leader can still send
	// heartbeats but cannot hear the acknowledgements, is where the subtler
	// Raft bugs live.
	blocked map[link]bool

	inflight []pending

	Stats Stats
}

// Stats counts what the network did, so a test can assert it actually injected
// the faults it asked for rather than silently running on a clean link.
type Stats struct {
	Sent        uint64
	Delivered   uint64
	Dropped     uint64
	Duplicated  uint64
	Partitioned uint64
}

// New creates a network over ids, seeded for reproducibility.
func New(seed uint64, ids []raft.NodeID, f Fault) *Network {
	sorted := append([]raft.NodeID(nil), ids...)
	slices.Sort(sorted)
	return &Network{
		rng:     rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		ids:     sorted,
		fault:   f,
		blocked: make(map[link]bool),
	}
}

// Now is the current logical tick.
func (n *Network) Now() uint64 { return n.now }

// Tick advances logical time by one.
func (n *Network) Tick() { n.now++ }

// TransportFor returns the Transport handed to node id.
func (n *Network) TransportFor(id raft.NodeID) transport.Transport {
	return transport.SendFunc(func(msgs []raft.Message) {
		for _, m := range msgs {
			if m.From != id {
				panic(fmt.Sprintf("inmem: node %d sent a message stamped from %d", id, m.From))
			}
			n.enqueue(m)
		}
	})
}

func (n *Network) enqueue(m raft.Message) {
	n.Stats.Sent++

	if n.fault.DropRate > 0 && n.rng.Float64() < n.fault.DropRate {
		n.Stats.Dropped++
		return
	}

	n.push(m)
	if n.fault.DuplicateRate > 0 && n.rng.Float64() < n.fault.DuplicateRate {
		n.Stats.Duplicated++
		n.push(m)
	}
}

func (n *Network) push(m raft.Message) {
	delay := 0
	if n.fault.MaxDelayTicks > 0 {
		delay = n.rng.IntN(n.fault.MaxDelayTicks + 1)
	}
	n.seq++
	n.inflight = append(n.inflight, pending{
		msg:       m,
		deliverAt: n.now + uint64(delay),
		seq:       n.seq,
	})
}

// Deliver returns every message that has come due, removing it from flight.
//
// A partitioned link is checked HERE, at delivery, not at send. A real
// partition drops packets that are already on the wire, and checking only at
// send time would let a message sent a moment before the cut arrive after it --
// which is precisely the case a partition test is trying to rule out.
func (n *Network) Deliver() []raft.Message {
	if len(n.inflight) == 0 {
		return nil
	}

	var due []pending
	kept := n.inflight[:0]
	for _, p := range n.inflight {
		if p.deliverAt > n.now {
			kept = append(kept, p)
			continue
		}
		if n.blocked[link{p.msg.From, p.msg.To}] {
			n.Stats.Partitioned++
			continue
		}
		due = append(due, p)
	}
	n.inflight = kept

	slices.SortFunc(due, func(a, b pending) int {
		if a.deliverAt != b.deliverAt {
			return int(a.deliverAt) - int(b.deliverAt)
		}
		return int(a.seq) - int(b.seq)
	})

	if n.fault.ReorderDueBatch && len(due) > 1 {
		n.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	}

	out := make([]raft.Message, len(due))
	for i, p := range due {
		out[i] = p.msg
	}
	n.Stats.Delivered += uint64(len(out))
	return out
}

// InFlight is the number of messages queued but not yet delivered.
func (n *Network) InFlight() int { return len(n.inflight) }

// Block cuts the directed link from -> to.
func (n *Network) Block(from, to raft.NodeID) { n.blocked[link{from, to}] = true }

// Unblock restores the directed link from -> to.
func (n *Network) Unblock(from, to raft.NodeID) { delete(n.blocked, link{from, to}) }

// Isolate cuts every link into and out of id.
func (n *Network) Isolate(id raft.NodeID) {
	for _, other := range n.ids {
		if other == id {
			continue
		}
		n.Block(id, other)
		n.Block(other, id)
	}
}

// Partition splits the cluster into groups. Messages between different groups
// are dropped in both directions until Heal; messages within a group are
// unaffected.
//
// Every node must appear in exactly one group, because a node missing from the
// partition specification is almost always a test bug rather than an intent to
// leave it fully connected.
func (n *Network) Partition(groups ...[]raft.NodeID) {
	seen := make(map[raft.NodeID]int, len(n.ids))
	for gi, g := range groups {
		for _, id := range g {
			if prev, dup := seen[id]; dup {
				panic(fmt.Sprintf("inmem: node %d appears in partition groups %d and %d", id, prev, gi))
			}
			seen[id] = gi
		}
	}
	for _, id := range n.ids {
		if _, ok := seen[id]; !ok {
			panic(fmt.Sprintf("inmem: node %d is in no partition group", id))
		}
	}

	n.blocked = make(map[link]bool)
	for _, a := range n.ids {
		for _, b := range n.ids {
			if a != b && seen[a] != seen[b] {
				n.blocked[link{a, b}] = true
			}
		}
	}
}

// Heal removes every injected partition. Messages already dropped stay dropped;
// Raft is expected to recover from that on its own, which is the point.
func (n *Network) Heal() { n.blocked = make(map[link]bool) }

// IsBlocked reports whether the directed link from -> to is cut.
func (n *Network) IsBlocked(from, to raft.NodeID) bool { return n.blocked[link{from, to}] }

// Rand exposes the seeded source so a harness can make its own scheduling
// decisions from the same deterministic stream.
func (n *Network) Rand() *rand.Rand { return n.rng }
