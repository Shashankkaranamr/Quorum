package server

import (
	"fmt"

	"github.com/Shashankkaranamr/Quorum/internal/storage"
	"github.com/Shashankkaranamr/Quorum/internal/transport"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// StateMachine consumes committed entries, and can be captured into and
// rebuilt from a snapshot.
//
// Declared here rather than imported from internal/statemachine because this is
// the consumer, and a consumer-side interface keeps the driver testable without
// dragging in the real key-value store. statemachine.KV satisfies it.
type StateMachine interface {
	// Apply must be deterministic. Every replica applying the same entry at
	// the same index must reach the same state, so nothing here may consult
	// the clock, the network or a random source.
	Apply(entries []raft.Entry) error

	// Snapshot captures the complete state as of the last applied entry, and
	// reports that entry's index. The driver checks it against the core's
	// applied index: a snapshot filed under the wrong index would restore a
	// replica to a state no other replica ever passed through.
	Snapshot() ([]byte, raft.Index, error)

	// Restore replaces the complete state with a snapshot's.
	Restore(snap raft.Snapshot) error
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

	// Syncs counts Sync calls: exactly one per Ready batch, which
	// TestOneFsyncPerReady asserts. Whether a Sync costs a real fsync is the
	// storage's business -- the write-ahead log skips it when the batch has
	// nothing durable in it -- and WAL.Stats().Fsyncs counts those.
	Syncs uint64

	TicksProcessed uint64

	// MaxTickLagTicks is the largest backlog of ticks observed waiting at the
	// top of a Run. One tick waiting is the steady state; anything above that
	// is the loop running behind, which is the head-of-line-blocking symptom.
	MaxTickLagTicks uint64

	// TotalTickLagTicks accumulates the same measure, so an average can be
	// taken over a run rather than only the worst case.
	TotalTickLagTicks uint64

	// SnapshotsTaken counts compactions this node made of its own log, and
	// SnapshotsInstalled counts snapshots it accepted from a leader.
	SnapshotsTaken     uint64
	SnapshotsInstalled uint64

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
// The simulator steps it synchronously. From phase 5, when a transport exists
// that can deliver inbound messages, the same struct is driven by one
// goroutine selecting on a ticker and an inbound channel. The body --
// ProcessReady below -- is identical in both, which is what makes the fast
// tests cover the real ordering.
type Driver struct {
	node  *raft.Node
	store storage.Storage
	trans transport.Transport
	sm    StateMachine

	metrics      Metrics
	pendingTicks uint64

	// snapshotThreshold is how many applied entries may accumulate past the
	// last snapshot before the log is compacted. Zero disables compaction.
	snapshotThreshold uint64

	// syncedThisReady guards the ordering rule within one batch.
	syncedThisReady bool
}

// New wires a driver together.
func New(node *raft.Node, store storage.Storage, trans transport.Transport, sm StateMachine) *Driver {
	return &Driver{node: node, store: store, trans: trans, sm: sm}
}

// SetSnapshotThreshold turns on log compaction: once this many entries have
// been applied past the last snapshot, the next Run captures the state machine
// and compacts the log up to it. Zero, the default, never compacts.
func (d *Driver) SetSnapshotThreshold(entries uint64) { d.snapshotThreshold = entries }

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

// Run hands over any accumulated ticks, processes one Ready batch, and compacts
// the log if it has grown past the snapshot threshold.
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

	if err := d.ProcessReady(); err != nil {
		return err
	}
	return d.MaybeSnapshot()
}

// MaybeSnapshot compacts the log if enough has been applied since the last
// snapshot.
//
// It runs between Readies, never inside one, so nothing is buffered in storage
// and the state machine is exactly at the core's applied index. The order is
// capture, compact the core, persist:
//
//  1. The state machine renders itself as of its last applied entry.
//  2. The core drops the entries that image covers and keeps the image, so it
//     can send it to a follower that needs one.
//  3. Storage writes the snapshot file, then the pointer, then deletes the
//     segments it supersedes.
//
// A crash between 2 and 3 costs nothing: the core's compaction lived only in
// memory, and the node comes back with its full log on disk. Nothing is sent
// in between, so no other node can have acted on the compacted state.
func (d *Driver) MaybeSnapshot() error {
	if d.snapshotThreshold == 0 || d.sm == nil {
		return nil
	}
	applied := d.node.Applied()
	if uint64(applied-d.node.SnapshotIndex()) < d.snapshotThreshold {
		return nil
	}

	data, at, err := d.sm.Snapshot()
	if err != nil {
		return fmt.Errorf("capture snapshot: %w", err)
	}
	if at != applied {
		return fmt.Errorf("state machine snapshot is at index %d but the core has applied %d; "+
			"the two have diverged", at, applied)
	}
	snap, err := d.node.Compact(at, data)
	if err != nil {
		return fmt.Errorf("compact log to %d: %w", at, err)
	}
	if err := d.store.SaveSnapshot(snap); err != nil {
		return fmt.Errorf("save snapshot at %d: %w", at, err)
	}
	d.metrics.SnapshotsTaken++
	return nil
}

// ProcessReady carries out one Ready batch in the order correctness requires.
//
// This ordering is the single most load-bearing sequence in the project:
//
//  0. Save snapshot           durable on return, only when one was received
//  1. Append entries          buffered
//  2. Set hard state          buffered
//  3. Sync                    the fsync -- exactly one per batch
//  4. Send messages           never before step 3 returns
//  5. Restore, then apply     never before step 3 returns
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

	// 0. A snapshot from the leader. The follower's acknowledgement of it is
	//    in rd.Messages, and says "I hold everything up to its index", so it
	//    must be durable before step 4 exactly as appended entries must be.
	//    It goes first because the entries below continue from it.
	if rd.Snapshot != nil {
		if err := d.store.SaveSnapshot(*rd.Snapshot); err != nil {
			return fmt.Errorf("save received snapshot at %d: %w", rd.Snapshot.Meta.Index, err)
		}
		d.metrics.SnapshotsInstalled++
	}

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

	// 5. Restore, then apply. The committed entries continue from the
	//    snapshot, so the state machine must be at its index first.
	if rd.Snapshot != nil && d.sm != nil {
		if err := d.sm.Restore(*rd.Snapshot); err != nil {
			return fmt.Errorf("restore snapshot at %d: %w", rd.Snapshot.Meta.Index, err)
		}
	}
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
