package server

import (
	"fmt"

	"github.com/Shashankkaranamr/Quorum/internal/storage"
	"github.com/Shashankkaranamr/Quorum/internal/transport"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// StateMachine consumes committed entries.
//
// Declared here rather than imported from internal/statemachine because this is
// the consumer, and a consumer-side interface keeps the driver testable without
// dragging in the real key-value store. Phase 5's state machine satisfies it.
type StateMachine interface {
	// Apply must be deterministic. Every replica applying the same entry at
	// the same index must reach the same state, so nothing here may consult
	// the clock, the network or a random source.
	Apply(entries []raft.Entry) error
}

// Metrics are the numbers that make the known hazards measurable rather than
// theoretical.
//
// They exist from the first version of the loop, not bolted on after something
// goes wrong, because the hazard they measure is structural: one goroutine owns
// ticks, messages and durability, so a slow fsync delays ticks and can stall an
// election. Phase 6 tests that behaviour; this is the instrumentation it needs.
type Metrics struct {
	ReadyBatches    uint64
	EntriesAppended uint64
	EntriesApplied  uint64
	MessagesSent    uint64
	HardStateWrites uint64

	// Syncs is the fsync count. Phase 3 asserts exactly one per Ready batch.
	Syncs uint64

	TicksProcessed uint64

	// MaxTickLagTicks is the largest backlog of ticks observed waiting at the
	// top of a Run. One tick waiting is the steady state; anything above that
	// is the loop running behind, which is the head-of-line-blocking symptom.
	MaxTickLagTicks uint64

	// TotalTickLagTicks accumulates the same measure, so an average can be
	// taken over a run rather than only the worst case.
	TotalTickLagTicks uint64

	// SendBeforeSyncViolations must always be zero. It counts the one thing
	// this loop exists to prevent: a message leaving the process before the
	// state it promises is durable. A non-zero value is a safety bug, not a
	// performance note.
	SendBeforeSyncViolations uint64
}

// TickLagTicks is the current backlog, in ticks, waiting to reach the core.
func (d *Driver) TickLagTicks() uint64 { return d.pendingTicks }

// Driver owns a raft.Node and is the only place in the system where I/O
// ordering is decided.
//
// In phase 2 it is stepped synchronously by the simulator. In phase 3 and
// beyond the same struct is driven by one goroutine selecting on a ticker and
// an inbound channel. The body -- ProcessReady below -- is identical in both,
// which is what makes the fast tests cover the real ordering.
type Driver struct {
	node  *raft.Node
	store storage.Storage
	trans transport.Transport
	sm    StateMachine

	metrics      Metrics
	pendingTicks uint64

	// syncedThisReady guards the ordering rule within one batch.
	syncedThisReady bool
}

// New wires a driver together.
func New(node *raft.Node, store storage.Storage, trans transport.Transport, sm StateMachine) *Driver {
	return &Driver{node: node, store: store, trans: trans, sm: sm}
}

// Node exposes the core for inspection. Callers must not step it directly.
func (d *Driver) Node() *raft.Node { return d.node }

// Metrics returns a copy of the current counters.
func (d *Driver) Metrics() Metrics { return d.metrics }

// Tick records that logical time advanced.
//
// It does not touch the core. Ticks accumulate here and are handed over in Run,
// which is what makes a backlog visible: if Run is delayed -- by a slow fsync,
// or in the simulator by a storage configured to cost ticks -- pendingTicks
// grows and MaxTickLagTicks records it.
func (d *Driver) Tick() { d.pendingTicks++ }

// Step delivers an inbound message to the core.
func (d *Driver) Step(m raft.Message) error { return d.node.Step(m) }

// Propose appends a command. Only a leader may propose.
func (d *Driver) Propose(typ raft.EntryType, data []byte) (raft.Index, raft.Term, error) {
	return d.node.Propose(typ, data)
}

// Run hands over any accumulated ticks and processes one Ready batch.
func (d *Driver) Run() error {
	if lag := d.pendingTicks; lag > 1 {
		if excess := lag - 1; excess > d.metrics.MaxTickLagTicks {
			d.metrics.MaxTickLagTicks = excess
		}
		d.metrics.TotalTickLagTicks += lag - 1
	}

	for d.pendingTicks > 0 {
		d.node.Tick()
		d.pendingTicks--
		d.metrics.TicksProcessed++
	}

	return d.ProcessReady()
}

// ProcessReady carries out one Ready batch in the order correctness requires.
//
// This ordering is the single most load-bearing sequence in the project:
//
//  1. Append entries          buffered
//  2. Set hard state          buffered
//  3. Sync                    the fsync -- exactly one per batch
//  4. Send messages           never before step 3 returns
//  5. Apply committed         never before step 3 returns
//  6. Advance                 acknowledge the batch
//
// Steps 4 and 5 come after 3 because a granted vote and an accepted
// AppendEntries are durable promises. Sending either before it is on disk lets
// a crash make the node contradict itself, and two leaders in one term follows.
// etcd relaxes this for follower appends as a throughput optimization; Quorum
// does not, and pays the latency.
func (d *Driver) ProcessReady() error {
	if !d.node.HasReady() {
		return nil
	}

	rd := d.node.Ready()
	d.metrics.ReadyBatches++
	d.syncedThisReady = false

	// 1. Entries.
	if len(rd.Entries) > 0 {
		if err := d.store.Append(rd.Entries); err != nil {
			return fmt.Errorf("append %d entries: %w", len(rd.Entries), err)
		}
		d.metrics.EntriesAppended += uint64(len(rd.Entries))
	}

	// 2. Hard state.
	if rd.HardState != nil {
		if err := d.store.SetHardState(*rd.HardState); err != nil {
			return fmt.Errorf("set hard state: %w", err)
		}
		d.metrics.HardStateWrites++
	}

	// 3. The fsync. Everything above is now durable; nothing below may run
	//    before this returns.
	if err := d.store.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	d.metrics.Syncs++
	d.syncedThisReady = true

	// 4. Messages.
	if len(rd.Messages) > 0 {
		if !d.syncedThisReady {
			// Unreachable as written. It is here so that a future refactor
			// that moves the send above the sync is caught by a counter a test
			// asserts on, rather than by a corrupted cluster in phase 6.
			d.metrics.SendBeforeSyncViolations++
		}
		d.trans.Send(rd.Messages)
		d.metrics.MessagesSent += uint64(len(rd.Messages))
	}

	// 5. Apply.
	if len(rd.CommittedEntries) > 0 {
		if d.sm != nil {
			if err := d.sm.Apply(rd.CommittedEntries); err != nil {
				return fmt.Errorf("apply %d entries: %w", len(rd.CommittedEntries), err)
			}
		}
		d.metrics.EntriesApplied += uint64(len(rd.CommittedEntries))
	}

	// 6. Done.
	d.node.Advance()
	return nil
}
